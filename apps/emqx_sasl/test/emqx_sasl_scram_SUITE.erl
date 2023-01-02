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

-module(emqx_sasl_scram_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

init_per_suite(Config) ->
    emqx_ct_helpers:start_apps([emqx_sasl]),
    Config.

end_per_suite(_Config) ->
    emqx_ct_helpers:stop_apps([]).

all() -> emqx_ct:all(?MODULE).

t_crud(_) ->
    Username = <<"test">>,
    Password = <<"public">>,
    Salt = <<"emqx">>,
    IterationCount = 4096,
    EncodedSalt = base64:encode(Salt),
    SaltedPassword = emqx_sasl_scram:pbkdf2_sha_1(Password, Salt, IterationCount),
    ClientKey = emqx_sasl_scram:client_key(SaltedPassword),
    ServerKey = base64:encode(emqx_sasl_scram:server_key(SaltedPassword)),
    StoredKey = base64:encode(crypto:hash(sha, ClientKey)),

    {error, not_found} = emqx_sasl_scram:lookup(Username),
    ok = emqx_sasl_scram:add(Username, Password, Salt),
    {error, already_existed} = emqx_sasl_scram:add(Username, Password, Salt),

    {ok, #{username := Username,
           stored_key := StoredKey,
           server_key := ServerKey,
           salt := EncodedSalt,
           iteration_count := IterationCount}} = emqx_sasl_scram:lookup(Username),

    NewSalt = <<"new salt">>,
    NewEncodedSalt = base64:encode(NewSalt),
    emqx_sasl_scram:update(Username, Password, NewSalt),
    {ok, #{username := Username,
           salt := NewEncodedSalt}} = emqx_sasl_scram:lookup(Username),
    emqx_sasl_scram:delete(Username),
    {error, not_found} = emqx_sasl_scram:lookup(Username).

t_scram(_) ->
    AuthMethod = <<"SCRAM-SHA-1">>,
    [AuthMethod] = emqx_sasl:supported(),

    Username = <<"test">>,
    Password = <<"public">>,
    Salt = <<"emqx">>,
    ok = emqx_sasl_scram:add(Username, Password, Salt),
    ClientFirst = emqx_sasl_scram:make_client_first(Username),

    {ok, {continue, ServerFirst, Cache}} = emqx_sasl:check(AuthMethod, ClientFirst, #{}),

    {ok, {continue, ClientFinal, ClientCache}} = emqx_sasl:check(AuthMethod, ServerFirst, #{password => Password, client_first => ClientFirst}),

    {ok, {ok, ServerFinal, #{}}} = emqx_sasl:check(AuthMethod, ClientFinal, Cache),

    {ok, _} = emqx_sasl:check(AuthMethod, ServerFinal, ClientCache).

%t_proto(_) ->
%    process_flag(trap_exit, true),
%
%    Username = <<"username">>,
%    Password = <<"password">>,
%    Salt = <<"emqx">>,
%    AuthMethod = <<"SCRAM-SHA-1">>,
%
%    {ok, Client0} = emqtt:start_link([{clean_start, true},
%                                     {proto_ver, v5},
%                                     {enhanced_auth, #{method => AuthMethod,
%                                                       params => #{username => Username,
%                                                                   password => Password,
%                                                                   salt => Salt}}},
%                                     {connect_timeout, 6000}]),
%    {error,{not_authorized,#{}}} = emqtt:connect(Client0),
%
%    ok = emqx_sasl_scram:add(Username, Password, Salt),
%    {ok, Client1} = emqtt:start_link([{clean_start, true},
%                                     {proto_ver, v5},
%                                     {enhanced_auth, #{method => AuthMethod,
%                                                       params => #{username => Username,
%                                                                   password => Password,
%                                                                   salt => Salt}}},
%                                     {connect_timeout, 6000}]),
%    {ok, _} = emqtt:connect(Client1),
%
%    timer:sleep(200),
%    ok = emqtt:reauthentication(Client1, #{params => #{username => Username,
%                                                       password => Password,
%                                                       salt => Salt}}),
%
%    timer:sleep(200),
%    ErrorFun = fun (_State) -> {ok, <<>>, #{}} end,
%    ok = emqtt:reauthentication(Client1, #{params => #{},function => ErrorFun}),
%    receive
%        {disconnected,ReasonCode2,#{}} ->
%            ?assertEqual(ReasonCode2, 135)
%    after 500 ->
%        error("emqx re-authentication failed")
%    end,
%
%    {ok, Client2} = emqtt:start_link([{clean_start, true},
%                                     {proto_ver, v5},
%                                     {enhanced_auth, #{method => AuthMethod,
%                                                       params => #{},
%                                                       function =>fun (_State) -> {ok, <<>>, #{}} end}},
%                                     {connect_timeout, 6000}]),
%    {error,{not_authorized,#{}}} = emqtt:connect(Client2),
%
%    receive_msg(),
%    process_flag(trap_exit, false).

receive_msg() ->
    receive
        {'EXIT', Msg} -> 
            ct:print("==========+~p~n", [Msg]), 
            receive_msg()
    after 200 -> ok
    end.
