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

-module(emqx_auth_mnesia).

-include("emqx_auth_mnesia.hrl").

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("emqx/include/types.hrl").

-include_lib("stdlib/include/ms_transform.hrl").

-define(TABLE, emqx_user).
%% Auth callbacks
-export([ init/1
        , check/3
        , description/0
        ]).

-export([ match_password/3
        , hash_type/0
        ]).

init(#{clientid_list := ClientidList, username_list := UsernameList}) ->
    ok = ekka_mnesia:create_table(?TABLE, [
            {disc_copies, [node()]},
            {attributes, record_info(fields, emqx_user)},
            {storage_properties, [{ets, [{read_concurrency, true}]}]}]),
    lists:foreach(fun({Clientid, Password}) ->
        emqx_auth_mnesia_cli:add_default_user(clientid, iolist_to_binary(Clientid), iolist_to_binary(Password))
    end, ClientidList),

    lists:foreach(fun({Username, Password}) ->
        emqx_auth_mnesia_cli:add_default_user(username, iolist_to_binary(Username), iolist_to_binary(Password))
    end, UsernameList),

    ok = ekka_mnesia:copy_table(?TABLE, disc_copies).

hash_type() ->
    application:get_env(emqx_auth_mnesia, password_hash, sha256).

check(ClientInfo = #{ clientid := Clientid
                    , password := NPassword
                    }, AuthResult, #{hash_type := HashType}) ->
    Username = maps:get(username, ClientInfo, undefined),
    MatchSpec = ets:fun2ms(fun({?TABLE, {clientid, X}, Password, InterTime}) when X =:= Clientid-> Password;
                              ({?TABLE, {username, X}, Password, InterTime}) when X =:= Username andalso X =/= undefined -> Password
                           end),
    Info = maps:without([password, ws_cookie], ClientInfo),
    case ets:select(?TABLE, MatchSpec) of
        [] ->
            ?LOG(debug, "[Mnesia] Auth ignored, Client: ~p", [Info]);
        List ->
            case match_password(NPassword, HashType, List)  of
                false ->
                    ?LOG(info, "[Mnesia] Auth from mnesia failed: ~p", [Info]),
                    {stop, AuthResult#{anonymous => false, auth_result => password_error}};
                _ ->
                    ?LOG(debug,"[Mnesia] Auth from mnesia succeeded, Client: ~p", [Info]),
                    {stop, AuthResult#{anonymous => false, auth_result => success}}
            end
    end.

description() -> "Authentication with Mnesia".

match_password(Password, HashType, HashList) ->
    lists:any(
      fun(Secret) ->
        case is_salt_hash(Secret, HashType) of
            true ->
                <<Salt:4/binary, Hash/binary>> = Secret,
                Hash =:= hash(Password, Salt, HashType);
            _ ->
                Secret =:= hash(Password, HashType)
        end
      end, HashList).

hash(undefined, HashType) ->
    hash(<<>>, HashType);
hash(Password, HashType) ->
    emqx_passwd:hash(HashType, Password).

hash(undefined, SaltBin, HashType) ->
    hash(<<>>, SaltBin, HashType);
hash(Password, SaltBin, HashType) ->
    emqx_passwd:hash(HashType, <<SaltBin/binary, Password/binary>>).

is_salt_hash(_, plain) ->
    true;
is_salt_hash(Secret, HashType) ->
    not (byte_size(Secret) == len(HashType)).

len(md5) -> 32;
len(sha) -> 40;
len(sha256) -> 64;
len(sha512) -> 128.
