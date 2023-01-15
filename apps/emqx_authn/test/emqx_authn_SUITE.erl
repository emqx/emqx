%%--------------------------------------------------------------------
%% Copyright (c) 2022-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_authn_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").

%%=================================================================================
%% CT boilerplate
%%=================================================================================

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(Case, Config) ->
    ?MODULE:Case({init, Config}).

end_per_testcase(Case, Config) ->
    ?MODULE:Case({'end', Config}).

%%=================================================================================
%% Helpers fns
%%=================================================================================

%%=================================================================================
%% Testcases
%%=================================================================================

t_will_message_connection_denied({init, Config}) ->
    emqx_common_test_helpers:start_apps([emqx_conf, emqx_authn]),
    mria:clear_table(emqx_authn_mnesia),
    AuthnConfig = #{
        <<"mechanism">> => <<"password_based">>,
        <<"backend">> => <<"built_in_database">>,
        <<"user_id_type">> => <<"clientid">>
    },
    Chain = 'mqtt:global',
    emqx:update_config(
        [authentication],
        {create_authenticator, Chain, AuthnConfig}
    ),
    User = #{user_id => <<"subscriber">>, password => <<"p">>},
    AuthenticatorID = <<"password_based:built_in_database">>,
    {ok, _} = emqx_authentication:add_user(
        Chain,
        AuthenticatorID,
        User
    ),
    Config;
t_will_message_connection_denied({'end', _Config}) ->
    emqx:update_config(
        [authentication],
        {delete_authenticator, 'mqtt:global', <<"password_based:built_in_database">>}
    ),
    emqx_common_test_helpers:stop_apps([emqx_authn, emqx_conf]),
    mria:clear_table(emqx_authn_mnesia),
    ok;
t_will_message_connection_denied(Config) when is_list(Config) ->
    {ok, Subscriber} = emqtt:start_link([
        {clientid, <<"subscriber">>},
        {password, <<"p">>}
    ]),
    {ok, _} = emqtt:connect(Subscriber),
    {ok, _, [?RC_SUCCESS]} = emqtt:subscribe(Subscriber, <<"lwt">>),

    process_flag(trap_exit, true),

    {ok, Publisher} = emqtt:start_link([
        {clientid, <<"publisher">>},
        {will_topic, <<"lwt">>},
        {will_payload, <<"should not be published">>}
    ]),
    Ref = monitor(process, Publisher),
    _ = unlink(Publisher),
    {error, _} = emqtt:connect(Publisher),
    receive
        {'DOWN', Ref, process, Publisher, Reason} ->
            ?assertEqual({shutdown, unauthorized_client}, Reason)
    after 2000 ->
        error(timeout)
    end,
    receive
        {publish, #{
            topic := <<"lwt">>,
            payload := <<"should not be published">>
        }} ->
            ct:fail("should not publish will message")
    after 1000 ->
        ok
    end,
    ok.

%% With auth enabled, send CONNECT without password field,
%% expect CONNACK with reason_code=5 and socket close
t_password_undefined({init, Config}) ->
    emqx_common_test_helpers:start_apps([emqx_conf, emqx_authn]),
    AuthnConfig = #{
        <<"mechanism">> => <<"password_based">>,
        <<"backend">> => <<"built_in_database">>,
        <<"user_id_type">> => <<"clientid">>
    },
    Chain = 'mqtt:global',
    emqx:update_config(
        [authentication],
        {create_authenticator, Chain, AuthnConfig}
    ),
    Config;
t_password_undefined({'end', _Config}) ->
    emqx:update_config(
        [authentication],
        {delete_authenticator, 'mqtt:global', <<"password_based:built_in_database">>}
    ),
    emqx_common_test_helpers:stop_apps([emqx_authn, emqx_conf]),
    ok;
t_password_undefined(Config) when is_list(Config) ->
    Payload = <<16, 19, 0, 4, 77, 81, 84, 84, 4, 130, 0, 60, 0, 2, 97, 49, 0, 3, 97, 97, 97>>,
    {ok, Sock} = gen_tcp:connect("localhost", 1883, [binary, {active, true}]),
    gen_tcp:send(Sock, Payload),
    receive
        {tcp, Sock, Bytes} ->
            Resp = parse(iolist_to_binary(Bytes)),
            ?assertMatch(
                #mqtt_packet{
                    header = #mqtt_packet_header{type = ?CONNACK},
                    variable = #mqtt_packet_connack{
                        ack_flags = 0,
                        reason_code = ?CONNACK_AUTH
                    },
                    payload = undefined
                },
                Resp
            )
    after 2000 ->
        error(timeout)
    end,
    receive
        {tcp_closed, Sock} ->
            ok
    after 2000 ->
        error(timeout)
    end,
    ok.

parse(Bytes) ->
    {ok, Frame, <<>>, {none, _}} = emqx_frame:parse(Bytes),
    Frame.
