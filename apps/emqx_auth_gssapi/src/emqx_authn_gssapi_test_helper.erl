%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(emqx_authn_gssapi_test_helper).
%% GSSAPI (Generic Security Service Application Interface)
%% TIP: All Tokens are from KDC api.
%% client connect mqtt v5 -> server  KDC

%% 1.1 Client auth_init(Keytab, Principal = "rig", Host) -> CliSaslConn
%% 1.1. client client_start(CliSaslConn) -> {ok, {sasl_continue, ClientToken1}}
%% 1.2. Client send connect({Props = ClientToken1})-> Server

%% 2.1 Server server_new("emqx" = Service, Principal = "emqx/erlang.emqx.net@KDC.EMQX.NET")) -> SrvSaslConn
%% 2.2 Server server_start(SrvSaslConn, ClientToken1) -> {ok, {sasl_continue, ServerToken1}}
%% 2.3 Server send auth({Props = ServerToken1}) -> Client

%% 3.1 Client client_step(CliSaslConn, ServerToken1) -> {ok, {sasl_continue, ClientToken2}}
%% 3.2 Client send auth({Props = ClientToken2}) -> Server

%% 4.1 Server server_step(SrvSaslConn, ClientToken2) -> {ok, {sasl_continue, ServerToken2}}
%% 4.2 Server send auth({Props = ServerToken2}) -> Client

%% 5.1 Client client_step(CliSaslConn, ServerToken2) -> {ok, {sasl_ok, ClientToken3}}
%% 5.2. Client send auth({Props = ClientToken3}) -> Server

%% 6.1 Server server_step(SrvSaslConn, ClientToken3) -> {ok, {sasl_ok, ServerToken3}}
%% 6.2 Server send auth({Props = ServerToken3}) -> Client
%% 6.3. Server send connack(0) -> Client

%% API
-export([test/3, test/0, test/1, test_invalid_bad_token/3, test_invalid_bad_token/0]).

test() ->
    test("127.0.0.1", 1883, <<"gssapi-client">>).

test_invalid_bad_token() ->
    test_invalid_bad_token("127.0.0.1", 1883, <<"gssapi-bad-token">>).

test(ClientId) ->
    test("127.0.0.1", 1883, ClientId).

test(Host, Port, ClientId) ->
    %% init client with keytab and principal
    {SaslConn, C} = init_connection(Host, Port, ClientId),
    {auth, #{'Authentication-Data' := ServerToken} = Props1} = emqtt:connect(C),
    %% client step to kdc
    {ok, {sasl_continue, ClientToken}} = sasl_auth:client_step(SaslConn, ServerToken),
    %% send auth packet to server
    ok = emqtt:auth(C, Props1#{'Authentication-Data' => ClientToken}),
    case receive_msg(auth_timeout1) of
        {_,
            {auth, #{
                'Authentication-Data' := ServerToken1,
                'Authentication-Method' := <<"GSSAPI">>
            }}} ->
            {ok, {sasl_ok, ClientToken1}} = sasl_auth:client_step(SaslConn, ServerToken1),
            %% send auth packet to server
            ok = emqtt:auth(C, Props1#{'Authentication-Data' => ClientToken1});
        Other1 ->
            erlang:error({auth_timeout1, Other1})
    end,
    case receive_msg(auth_timeout2) of
        {_,
            {auth, #{
                'Authentication-Data' := ServerToken2,
                'Authentication-Method' := <<"GSSAPI">>
            }}} ->
            io:format("sasl_ok: ~p~n", [ServerToken2]),
            ok;
        Other2 ->
            erlang:error({auth_timeout2, Other2})
    end,
    %% receive connack(0) from server
    case receive_msg(connack_ok) of
        {_,
            {ok, #{
                'Authentication-Method' := <<"GSSAPI">>,
                'Authentication-Data' := <<>>
            }}} ->
            ok;
        Other3 ->
            erlang:error({connack_ok, Other3})
    end,
    emqtt:subscribe(C, <<"test">>, 0).

test_invalid_bad_token(Host, Port, ClientId) ->
    %% init client with keytab and principal
    {SaslConn, C} = init_connection(Host, Port, ClientId),
    {auth, #{'Authentication-Data' := ServerToken} = Props1} = emqtt:connect(C),
    %% client step to kdc
    {ok, {sasl_continue, _ClientToken}} = sasl_auth:client_step(SaslConn, ServerToken),
    %% use the ServerToken (bad)
    ok = emqtt:auth(C, Props1#{'Authentication-Data' => ServerToken}),
    receive_msg(auth_timeout).

init_connection(Host, Port, ClientId) ->
    {ok, SaslConn} = auth_init(<<"/var/lib/secret/rig.keytab">>, <<"rig">>, "erlang.emqx.net"),
    {ok, {sasl_continue, Challenge}} = sasl_auth:client_start(SaslConn),
    Props = #{
        'Session-Expiry-Interval' => 7200,
        'Authentication-Method' => <<"GSSAPI">>,
        'Authentication-Data' => Challenge
    },
    {ok, C} = emqtt:start_link([
        {host, Host},
        {port, Port},
        {clientid, ClientId},
        {proto_ver, v5},
        {properties, Props}
    ]),
    {SaslConn, C}.

receive_msg(Reason) ->
    receive
        Msg -> Msg
    after 1000 ->
        erlang:error(Reason)
    end.

auth_init(Keytab, Principal, Host) ->
    case sasl_auth:kinit(Keytab, Principal) of
        ok -> sasl_auth:client_new(<<"emqx">>, Host, Principal);
        Error -> Error
    end.
