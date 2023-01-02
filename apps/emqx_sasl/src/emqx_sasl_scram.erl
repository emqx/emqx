%%--------------------------------------------------------------------
%% Copyright (c) 2020-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(emqx_sasl_scram).

-include("emqx_sasl.hrl").

-export([ init/0
        , add/3
        , add/4
        , update/3
        , update/4
        , delete/1
        , lookup/1
        , check/2
        , make_client_first/1]).

-record(?SCRAM_AUTH_TAB, {
            username,
            stored_key,
            server_key,
            salt,
            iteration_count :: integer()
        }).

-ifdef(TEST).
-compile(export_all).
-compile(nowarn_export_all).
-endif.

init() ->
    ok = ekka_mnesia:create_table(?SCRAM_AUTH_TAB, [
            {disc_copies, [node()]},
            {attributes, record_info(fields, ?SCRAM_AUTH_TAB)},
            {storage_properties, [{ets, [{read_concurrency, true}]}]}]),
    ok = ekka_mnesia:copy_table(?SCRAM_AUTH_TAB, disc_copies).

add(Username, Password, Salt) ->
    add(Username, Password, Salt, 4096).

add(Username, Password, Salt, IterationCount) ->
    case lookup(Username) of
        {error, not_found} ->
            do_add(Username, Password, Salt, IterationCount);
        _ ->
            {error, already_existed}
    end.

update(Username, Password, Salt) ->
    update(Username, Password, Salt, 4096).

update(Username, Password, Salt, IterationCount) ->
    case lookup(Username) of
        {error, not_found} ->
            {error, not_found};
        _ ->
            do_add(Username, Password, Salt, IterationCount)
    end.

delete(Username) ->
    ret(mnesia:transaction(fun mnesia:delete/3, [?SCRAM_AUTH_TAB, Username, write])).

lookup(Username) ->
    case mnesia:dirty_read(?SCRAM_AUTH_TAB, Username) of
        [#scram_auth{username = Username,
                     stored_key = StoredKey,
                     server_key = ServerKey,
                     salt = Salt,
                     iteration_count = IterationCount}] ->
            {ok, #{username => Username,
                   stored_key => StoredKey,
                   server_key => ServerKey,
                   salt => Salt,
                   iteration_count => IterationCount}};
        [] ->
            {error, not_found}
    end.

do_add(Username, Password, Salt, IterationCount) ->
    SaltedPassword = pbkdf2_sha_1(Password, Salt, IterationCount),
    ClientKey = client_key(SaltedPassword),
    ServerKey = server_key(SaltedPassword),
    StoredKey = crypto:hash(sha, ClientKey),
    AuthInfo = #scram_auth{username = Username,
                           stored_key = base64:encode(StoredKey),
                           server_key = base64:encode(ServerKey),
                           salt = base64:encode(Salt),
                           iteration_count = IterationCount},
    ret(mnesia:transaction(fun mnesia:write/3, [?SCRAM_AUTH_TAB, AuthInfo, write])).

ret({atomic, ok})     -> ok;
ret({aborted, Error}) -> {error, Error}.

check(Data, Cache) when map_size(Cache) =:= 0 ->
    check_client_first(Data);
check(Data, Cache) ->
    case maps:get(next_step, Cache, undefined) of
        undefined -> check_server_first(Data, Cache);
        check_client_final -> check_client_final(Data, Cache);
        check_server_final -> check_server_final(Data, Cache)
    end.

check_client_first(ClientFirst) ->
    ClientFirstWithoutHeader = without_header(ClientFirst),
    Attributes = parse(ClientFirstWithoutHeader),
    Username = proplists:get_value(username, Attributes),
    ClientNonce = proplists:get_value(nonce, Attributes),
    case lookup(Username) of
        {error, not_found} ->
            {error, not_found};
        {ok, #{stored_key := StoredKey0,
               server_key := ServerKey0,
               salt := Salt0,
               iteration_count := IterationCount}} ->
            StoredKey = base64:decode(StoredKey0), 
            ServerKey = base64:decode(ServerKey0),
            Salt = base64:decode(Salt0),
            ServerNonce = nonce(),
            Nonce = list_to_binary(binary_to_list(ClientNonce) ++ binary_to_list(ServerNonce)),
            ServerFirst = make_server_first(Nonce, Salt, IterationCount),
            {continue, ServerFirst, #{next_step => check_client_final,
                                      client_first_without_header => ClientFirstWithoutHeader,
                                      server_first => ServerFirst,
                                      stored_key => StoredKey,
                                      server_key => ServerKey,
                                      nonce => Nonce}}
    end.

check_client_final(ClientFinal, #{client_first_without_header := ClientFirstWithoutHeader,
                                  server_first := ServerFirst,
                                  server_key := ServerKey,
                                  stored_key := StoredKey,
                                  nonce := OldNonce}) ->
    ClientFinalWithoutProof = without_proof(ClientFinal),
    Attributes = parse(ClientFinal),
    ClientProof = base64:decode(proplists:get_value(proof, Attributes)),
    NewNonce = proplists:get_value(nonce, Attributes),
    Auth0 = io_lib:format("~s,~s,~s", [ClientFirstWithoutHeader, ServerFirst, ClientFinalWithoutProof]),
    Auth = iolist_to_binary(Auth0),
    ClientSignature = hmac(StoredKey, Auth),
    ClientKey = crypto:exor(ClientProof, ClientSignature),
    case NewNonce =:= OldNonce andalso crypto:hash(sha, ClientKey) =:= StoredKey of
        true ->
            ServerSignature = hmac(ServerKey, Auth),
            ServerFinal = make_server_final(ServerSignature),
            {ok, ServerFinal, #{}};
        false ->
            {error, invalid_client_final}
    end.

check_server_first(ServerFirst, #{password := Password,
                                  client_first := ClientFirst}) ->
    Attributes = parse(ServerFirst),
    Nonce = proplists:get_value(nonce, Attributes),
    ClientFirstWithoutHeader = without_header(ClientFirst),
    ClientFinalWithoutProof = serialize([{channel_binding, <<"biws">>}, {nonce, Nonce}]),
    Auth = list_to_binary(io_lib:format("~s,~s,~s", [ClientFirstWithoutHeader, ServerFirst, ClientFinalWithoutProof])),
    Salt = base64:decode(proplists:get_value(salt, Attributes)),
    IterationCount = binary_to_integer(proplists:get_value(iteration_count, Attributes)),
    SaltedPassword = pbkdf2_sha_1(Password, Salt, IterationCount),
    ClientKey = client_key(SaltedPassword),
    StoredKey = crypto:hash(sha, ClientKey),
    ClientSignature = hmac(StoredKey, Auth),
    ClientProof = base64:encode(crypto:exor(ClientKey, ClientSignature)),
    ClientFinal = serialize([{channel_binding, <<"biws">>},
                             {nonce, Nonce},
                             {proof, ClientProof}]),
    {continue, ClientFinal, #{next_step => check_server_final,
                              password => Password,
                              client_first => ClientFirst,
                              server_first => ServerFirst}}.

check_server_final(ServerFinal, #{password := Password,
                                  client_first := ClientFirst,
                                  server_first := ServerFirst}) ->
    NewAttributes = parse(ServerFinal),
    Attributes = parse(ServerFirst),
    Nonce = proplists:get_value(nonce, Attributes),
    ClientFirstWithoutHeader = without_header(ClientFirst),
    ClientFinalWithoutProof = serialize([{channel_binding, <<"biws">>}, {nonce, Nonce}]),
    Auth = list_to_binary(io_lib:format("~s,~s,~s", [ClientFirstWithoutHeader, ServerFirst, ClientFinalWithoutProof])),
    Salt = base64:decode(proplists:get_value(salt, Attributes)),
    IterationCount = binary_to_integer(proplists:get_value(iteration_count, Attributes)),
    SaltedPassword = pbkdf2_sha_1(Password, Salt, IterationCount),
    ServerKey = server_key(SaltedPassword),
    ServerSignature = hmac(ServerKey, Auth),
    case base64:encode(ServerSignature) =:= proplists:get_value(verifier, NewAttributes) of
        true ->
            {ok, <<>>, #{}};
        false -> 
            {stop, invalid_server_final}
    end.

make_client_first(Username) ->
    list_to_binary("n,," ++ binary_to_list(serialize([{username, Username}, {nonce, nonce()}]))).

make_server_first(Nonce, Salt, IterationCount) ->
    serialize([{nonce, Nonce}, {salt, base64:encode(Salt)}, {iteration_count, IterationCount}]).

make_server_final(ServerSignature) ->
    serialize([{verifier, base64:encode(ServerSignature)}]).

nonce() ->
    base64:encode([$a + rand:uniform(26) || _ <- lists:seq(1, 10)]).

pbkdf2_sha_1(Password, Salt, IterationCount) ->
    {ok, Bin} = pbkdf2:pbkdf2(sha, Password, Salt, IterationCount),
    pbkdf2:to_hex(Bin).

-if(?OTP_RELEASE >= 23).
hmac(Key, Data) ->
    HMAC = crypto:mac_init(hmac, sha, Key),
    HMAC1 = crypto:mac_update(HMAC, Data),
    crypto:mac_final(HMAC1).
-else.
hmac(Key, Data) ->
    HMAC = crypto:hmac_init(sha, Key),
    HMAC1 = crypto:hmac_update(HMAC, Data),
    crypto:hmac_final(HMAC1).
-endif.

client_key(SaltedPassword) ->
    hmac(<<"Client Key">>, SaltedPassword).

server_key(SaltedPassword) ->
    hmac(<<"Server Key">>, SaltedPassword).

without_header(<<"n,,", ClientFirstWithoutHeader/binary>>) ->
    ClientFirstWithoutHeader;
without_header(<<GS2CbindFlag:1/binary, _/binary>>) ->
    error({unsupported_gs2_cbind_flag, binary_to_atom(GS2CbindFlag, utf8)}).

without_proof(ClientFinal) ->
    [ClientFinalWithoutProof | _] = binary:split(ClientFinal, <<",p=">>, [global, trim_all]),
    ClientFinalWithoutProof.

parse(Message) ->
    Attributes = binary:split(Message, <<$,>>, [global, trim_all]),
    lists:foldl(fun(<<Key:1/binary, "=", Value/binary>>, Acc) ->
                    [{to_long(Key), Value} | Acc]
                end, [], Attributes).

serialize(Attributes) ->
    iolist_to_binary(
        lists:foldl(fun({Key, Value}, []) ->
                        [to_short(Key), "=", to_list(Value)];
                       ({Key, Value}, Acc) ->
                        Acc ++ [",", to_short(Key), "=", to_list(Value)]
                     end, [], Attributes)).

to_long(<<"a">>) ->
    authzid;
to_long(<<"c">>) ->
    channel_binding;
to_long(<<"n">>) ->
    username;
to_long(<<"p">>) ->
    proof;
to_long(<<"r">>) ->
    nonce;
to_long(<<"s">>) ->
    salt;
to_long(<<"v">>) ->
    verifier;
to_long(<<"i">>) ->
    iteration_count;
to_long(_) ->
    error(test).

to_short(authzid) ->
    "a";
to_short(channel_binding) ->
    "c";
to_short(username) ->
    "n";
to_short(proof) ->
    "p";
to_short(nonce) ->
    "r";
to_short(salt) ->
    "s";
to_short(verifier) ->
    "v";
to_short(iteration_count) ->
    "i";
to_short(_) ->
    error(test).

to_list(V) when is_binary(V) ->
    binary_to_list(V);
to_list(V) when is_list(V) ->
    V;
to_list(V) when is_integer(V) ->
    integer_to_list(V);
to_list(_) ->
    error(bad_type).

