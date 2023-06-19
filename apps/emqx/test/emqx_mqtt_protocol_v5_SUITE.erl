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

-module(emqx_mqtt_protocol_v5_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("common_test/include/ct.hrl").

-import(lists, [nth/2]).

-define(TOPICS, [
    <<"TopicA">>,
    <<"TopicA/B">>,
    <<"Topic/C">>,
    <<"TopicA/C">>,
    <<"/TopicA">>
]).

-define(WILD_TOPICS, [
    <<"TopicA/+">>,
    <<"+/C">>,
    <<"#">>,
    <<"/#">>,
    <<"/+">>,
    <<"+/+">>,
    <<"TopicA/#">>
]).

all() ->
    [
        {group, tcp},
        {group, quic}
    ].

groups() ->
    TCs = emqx_common_test_helpers:all(?MODULE),
    [
        {tcp, [], TCs},
        {quic, [], TCs}
    ].

init_per_group(tcp, Config) ->
    emqx_common_test_helpers:start_apps([]),
    [{port, 1883}, {conn_fun, connect} | Config];
init_per_group(quic, Config) ->
    UdpPort = 1884,
    emqx_common_test_helpers:start_apps([]),
    emqx_common_test_helpers:ensure_quic_listener(?MODULE, UdpPort),
    [{port, UdpPort}, {conn_fun, quic_connect} | Config];
init_per_group(_, Config) ->
    emqx_common_test_helpers:stop_apps([]),
    Config.

end_per_group(quic, _Config) ->
    emqx_config:put([listeners, quic], #{}),
    ok;
end_per_group(_Group, _Config) ->
    ok.

init_per_suite(Config) ->
    %% Start Apps
    emqx_common_test_helpers:boot_modules(all),
    emqx_common_test_helpers:start_apps([]),
    Config.

end_per_suite(_Config) ->
    emqx_common_test_helpers:stop_apps([]).

init_per_testcase(TestCase, Config) ->
    case erlang:function_exported(?MODULE, TestCase, 2) of
        true -> ?MODULE:TestCase(init, Config);
        _ -> Config
    end.

end_per_testcase(TestCase, Config) ->
    case erlang:function_exported(?MODULE, TestCase, 2) of
        true -> ?MODULE:TestCase('end', Config);
        false -> ok
    end,
    Config.

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

client_info(Key, Client) ->
    maps:get(Key, maps:from_list(emqtt:info(Client)), undefined).

receive_messages(Count) ->
    receive_messages(Count, []).

receive_messages(0, Msgs) ->
    Msgs;
receive_messages(Count, Msgs) ->
    receive
        {publish, Msg} ->
            receive_messages(Count - 1, [Msg | Msgs]);
        _Other ->
            receive_messages(Count, Msgs)
    after 1000 ->
        Msgs
    end.

receive_disconnect_reasoncode() ->
    receive
        {disconnected, ReasonCode, _} -> ReasonCode;
        _Other -> receive_disconnect_reasoncode()
    after 100 ->
        error("no disconnect packet")
    end.

waiting_client_process_exit(C) ->
    receive
        {'EXIT', C, Reason} -> Reason;
        _Oth -> error({got_another_message, _Oth})
    after 1000 -> error({waiting_timeout, C})
    end.

clean_retained(Topic, Config) ->
    ConnFun = ?config(conn_fun, Config),
    {ok, Clean} = emqtt:start_link([{clean_start, true} | Config]),
    {ok, _} = emqtt:ConnFun(Clean),
    {ok, _} = emqtt:publish(Clean, Topic, #{}, <<"">>, [{qos, ?QOS_1}, {retain, true}]),
    ok = emqtt:disconnect(Clean).

%%--------------------------------------------------------------------
%% Test Cases
%%--------------------------------------------------------------------

t_basic_test(Config) ->
    ConnFun = ?config(conn_fun, Config),
    Topic = nth(1, ?TOPICS),
    {ok, C} = emqtt:start_link([{proto_ver, v5} | Config]),
    {ok, _} = emqtt:ConnFun(C),
    {ok, _, [1]} = emqtt:subscribe(C, Topic, qos1),
    {ok, _, [2]} = emqtt:subscribe(C, Topic, qos2),
    {ok, _} = emqtt:publish(C, Topic, <<"qos 2">>, 2),
    {ok, _} = emqtt:publish(C, Topic, <<"qos 2">>, 2),
    {ok, _} = emqtt:publish(C, Topic, <<"qos 2">>, 2),
    ?assertEqual(3, length(receive_messages(3))),
    ok = emqtt:disconnect(C).

%%--------------------------------------------------------------------
%% Connection
%%--------------------------------------------------------------------

t_connect_clean_start(Config) ->
    ConnFun = ?config(conn_fun, Config),
    process_flag(trap_exit, true),
    {ok, Client1} = emqtt:start_link([
        {clientid, <<"t_connect_clean_start">>},
        {proto_ver, v5},
        {clean_start, true}
        | Config
    ]),
    {ok, _} = emqtt:ConnFun(Client1),
    %% [MQTT-3.1.2-4]
    ?assertEqual(0, client_info(session_present, Client1)),
    ok = emqtt:pause(Client1),
    {ok, Client2} = emqtt:start_link([
        {clientid, <<"t_connect_clean_start">>},
        {proto_ver, v5},
        {clean_start, false}
        | Config
    ]),
    {ok, _} = emqtt:ConnFun(Client2),
    %% [MQTT-3.1.2-5]
    ?assertEqual(1, client_info(session_present, Client2)),
    ?assertEqual(142, receive_disconnect_reasoncode()),
    waiting_client_process_exit(Client1),

    ok = emqtt:disconnect(Client2),
    waiting_client_process_exit(Client2),

    {ok, Client3} = emqtt:start_link([
        {clientid, <<"new_client">>},
        {proto_ver, v5},
        {clean_start, false}
        | Config
    ]),
    {ok, _} = emqtt:ConnFun(Client3),
    %% [MQTT-3.1.2-6]
    ?assertEqual(0, client_info(session_present, Client3)),
    ok = emqtt:disconnect(Client3),
    waiting_client_process_exit(Client3),

    process_flag(trap_exit, false).

t_connect_will_message(Config) ->
    ConnFun = ?config(conn_fun, Config),
    Topic = nth(1, ?TOPICS),
    Payload = "will message",

    {ok, Client1} = emqtt:start_link([
        {proto_ver, v5},
        {clean_start, true},
        {will_flag, true},
        {will_topic, Topic},
        {will_payload, Payload}
        | Config
    ]),
    {ok, _} = emqtt:ConnFun(Client1),
    [ClientPid] = emqx_cm:lookup_channels(client_info(clientid, Client1)),
    Info = emqx_connection:info(sys:get_state(ClientPid)),
    %% [MQTT-3.1.2-7]
    ?assertNotEqual(undefined, maps:find(will_msg, Info)),

    {ok, Client2} = emqtt:start_link([{proto_ver, v5} | Config]),
    {ok, _} = emqtt:ConnFun(Client2),
    {ok, _, [2]} = emqtt:subscribe(Client2, Topic, qos2),

    %% [MQTT-3.14.2-1]
    ok = emqtt:disconnect(Client1, 4),
    [Msg | _] = receive_messages(1),
    %% [MQTT-3.1.2-8]
    ?assertEqual({ok, iolist_to_binary(Topic)}, maps:find(topic, Msg)),
    ?assertEqual({ok, iolist_to_binary(Payload)}, maps:find(payload, Msg)),
    ?assertEqual({ok, 0}, maps:find(qos, Msg)),
    ok = emqtt:disconnect(Client2),

    {ok, Client3} = emqtt:start_link([
        {proto_ver, v5},
        {clean_start, true},
        {will_flag, true},
        {will_topic, Topic},
        {will_payload, Payload}
        | Config
    ]),
    {ok, _} = emqtt:ConnFun(Client3),

    {ok, Client4} = emqtt:start_link([{proto_ver, v5} | Config]),
    {ok, _} = emqtt:ConnFun(Client4),
    {ok, _, [2]} = emqtt:subscribe(Client4, Topic, qos2),
    ok = emqtt:disconnect(Client3),
    %% [MQTT-3.1.2-10]
    MsgRecv = receive_messages(1),
    ?assertEqual([], MsgRecv),
    ok = emqtt:disconnect(Client4).

t_batch_subscribe(init, Config) ->
    emqx_config:put_zone_conf(default, [authorization, enable], true),
    ok = meck:new(emqx_access_control, [non_strict, passthrough, no_history, no_link]),
    meck:expect(emqx_access_control, authorize, fun(_, _, _) -> deny end),
    Config;
t_batch_subscribe('end', _Config) ->
    emqx_config:put_zone_conf(default, [authorization, enable], false),
    meck:unload(emqx_access_control).

t_batch_subscribe(Config) ->
    ConnFun = ?config(conn_fun, Config),
    {ok, Client} = emqtt:start_link([{proto_ver, v5}, {clientid, <<"batch_test">>} | Config]),
    {ok, _} = emqtt:ConnFun(Client),
    {ok, _, [
        ?RC_NOT_AUTHORIZED,
        ?RC_NOT_AUTHORIZED,
        ?RC_NOT_AUTHORIZED
    ]} = emqtt:subscribe(Client, [
        {<<"t1">>, qos1},
        {<<"t2">>, qos2},
        {<<"t3">>, qos0}
    ]),
    {ok, _, [
        ?RC_NO_SUBSCRIPTION_EXISTED,
        ?RC_NO_SUBSCRIPTION_EXISTED,
        ?RC_NO_SUBSCRIPTION_EXISTED
    ]} = emqtt:unsubscribe(Client, [
        <<"t1">>,
        <<"t2">>,
        <<"t3">>
    ]),
    emqtt:disconnect(Client).

t_connect_will_retain(Config) ->
    ConnFun = ?config(conn_fun, Config),
    process_flag(trap_exit, true),
    Topic = nth(1, ?TOPICS),
    Payload = "will message",

    {ok, Client1} = emqtt:start_link([
        {proto_ver, v5},
        {clean_start, true},
        {will_flag, true},
        {will_topic, Topic},
        {will_payload, Payload},
        {will_retain, false}
        | Config
    ]),
    {ok, _} = emqtt:ConnFun(Client1),

    {ok, Client2} = emqtt:start_link([{proto_ver, v5} | Config]),
    {ok, _} = emqtt:ConnFun(Client2),
    {ok, _, [2]} = emqtt:subscribe(Client2, #{}, [{Topic, [{rap, true}, {qos, 2}]}]),

    ok = emqtt:disconnect(Client1, 4),
    [Msg1 | _] = receive_messages(1),
    %% [MQTT-3.1.2-14]
    ?assertEqual({ok, false}, maps:find(retain, Msg1)),
    ok = emqtt:disconnect(Client2),

    {ok, Client3} = emqtt:start_link([
        {proto_ver, v5},
        {clean_start, true},
        {will_flag, true},
        {will_topic, Topic},
        {will_payload, Payload},
        {will_retain, true}
        | Config
    ]),
    {ok, _} = emqtt:ConnFun(Client3),

    {ok, Client4} = emqtt:start_link([{proto_ver, v5} | Config]),
    {ok, _} = emqtt:ConnFun(Client4),
    {ok, _, [2]} = emqtt:subscribe(Client4, #{}, [{Topic, [{rap, true}, {qos, 2}]}]),

    ok = emqtt:disconnect(Client3, 4),
    [Msg2 | _] = receive_messages(1),
    %% [MQTT-3.1.2-15]
    ?assertEqual({ok, true}, maps:find(retain, Msg2)),
    ok = emqtt:disconnect(Client4),
    clean_retained(Topic, Config).

t_connect_idle_timeout(_Config) ->
    IdleTimeout = 2000,
    emqx_config:put_zone_conf(default, [mqtt, idle_timeout], IdleTimeout),
    emqx_config:put_zone_conf(default, [mqtt, idle_timeout], IdleTimeout),
    {ok, Sock} = emqtt_sock:connect({127, 0, 0, 1}, 1883, [], 60000),
    timer:sleep(IdleTimeout),
    ?assertMatch({error, closed}, emqtt_sock:recv(Sock, 1024)).

t_connect_emit_stats_timeout(init, Config) ->
    NewIdleTimeout = 1000,
    emqx_config:put_zone_conf(default, [mqtt, idle_timeout], NewIdleTimeout),
    emqx_config:put_zone_conf(default, [mqtt, idle_timeout], NewIdleTimeout),
    ok = snabbkaffe:start_trace(),
    [{idle_timeout, NewIdleTimeout} | Config];
t_connect_emit_stats_timeout('end', _Config) ->
    snabbkaffe:stop(),
    emqx_config:put_zone_conf(default, [mqtt, idle_timeout], 15000),
    emqx_config:put_zone_conf(default, [mqtt, idle_timeout], 15000),
    ok.

t_connect_emit_stats_timeout(Config) ->
    ConnFun = ?config(conn_fun, Config),
    {_, IdleTimeout} = lists:keyfind(idle_timeout, 1, Config),
    {ok, Client} = emqtt:start_link([{proto_ver, v5}, {keepalive, 60} | Config]),
    {ok, _} = emqtt:ConnFun(Client),
    [ClientPid] = emqx_cm:lookup_channels(client_info(clientid, Client)),
    ?assert(is_reference(emqx_connection:info(stats_timer, sys:get_state(ClientPid)))),
    ?block_until(#{?snk_kind := cancel_stats_timer}, IdleTimeout * 2, _BackInTime = 0),
    ?assertEqual(undefined, emqx_connection:info(stats_timer, sys:get_state(ClientPid))),
    ok = emqtt:disconnect(Client).

%% [MQTT-3.1.2-22]
t_connect_keepalive_timeout(Config) ->
    ConnFun = ?config(conn_fun, Config),
    %% Prevent the emqtt client bringing us down on the disconnect.
    process_flag(trap_exit, true),

    Keepalive = 2,

    {ok, Client} = emqtt:start_link([
        {proto_ver, v5},
        {keepalive, Keepalive}
        | Config
    ]),
    {ok, _} = emqtt:ConnFun(Client),
    emqtt:pause(Client),
    receive
        {disconnected, ReasonCode, _Channel} -> ?assertEqual(141, ReasonCode)
    after round(timer:seconds(Keepalive) * 2 * 1.5) ->
        error("keepalive timeout")
    end.

%% [MQTT-3.1.3-9]
%% !!!REFACTOR NEED:
%t_connect_will_delay_interval(Config) ->
%    process_flag(trap_exit, true),
%    Topic = nth(1, ?TOPICS),
%    Payload = "will message",
%
%    {ok, Client1} = emqtt:start_link([{proto_ver, v5} | Config]),
%    {ok, _} = emqtt:ConnFun(Client1),
%    {ok, _, [2]} = emqtt:subscribe(Client1, Topic, qos2),
%
%    {ok, Client2} = emqtt:start_link([
%                                        {clientid, <<"t_connect_will_delay_interval">>},
%                                        {proto_ver, v5},
%                                        {clean_start, true},
%                                        {will_flag, true},
%                                        {will_qos, 2},
%                                        {will_topic, Topic},
%                                        {will_payload, Payload},
%                                        {will_props, #{'Will-Delay-Interval' => 3}},
%                                        {properties, #{'Session-Expiry-Interval' => 7200}},
%                                        {keepalive, 2} | Config
%                                        ]),
%    {ok, _} = emqtt:ConnFun(Client2),
%    timer:sleep(50),
%    erlang:exit(Client2, kill),
%    timer:sleep(2000),
%    ?assertEqual(0, length(receive_messages(1))),
%    timer:sleep(5000),
%    ?assertEqual(1, length(receive_messages(1))),
%
%    {ok, Client3} = emqtt:start_link([
%                                        {clientid, <<"t_connect_will_delay_interval">>},
%                                        {proto_ver, v5},
%                                        {clean_start, true},
%                                        {will_flag, true},
%                                        {will_qos, 2},
%                                        {will_topic, Topic},
%                                        {will_payload, Payload},
%                                        {will_props, #{'Will-Delay-Interval' => 7200}},
%                                        {properties, #{'Session-Expiry-Interval' => 3}},
%                                        {keepalive, 2} | Config
%                                        ]),
%    {ok, _} = emqtt:ConnFun(Client3),
%    timer:sleep(50),
%    erlang:exit(Client3, kill),
%
%    timer:sleep(2000),
%    ?assertEqual(0, length(receive_messages(1))),
%    timer:sleep(5000),
%    ?assertEqual(1, length(receive_messages(1))),
%
%    ok = emqtt:disconnect(Client1),
%
%    receive {'EXIT', _, _} -> ok
%    after 100 -> ok
%    end,
%    process_flag(trap_exit, false).

%% [MQTT-3.1.4-3]
t_connect_duplicate_clientid(Config) ->
    ConnFun = ?config(conn_fun, Config),
    process_flag(trap_exit, true),
    {ok, Client1} = emqtt:start_link([
        {clientid, <<"t_connect_duplicate_clientid">>},
        {proto_ver, v5}
        | Config
    ]),
    {ok, _} = emqtt:ConnFun(Client1),
    {ok, Client2} = emqtt:start_link([
        {clientid, <<"t_connect_duplicate_clientid">>},
        {proto_ver, v5}
        | Config
    ]),
    {ok, _} = emqtt:ConnFun(Client2),
    ?assertEqual(142, receive_disconnect_reasoncode()),
    waiting_client_process_exit(Client1),

    ok = emqtt:disconnect(Client2),
    waiting_client_process_exit(Client2),
    process_flag(trap_exit, false).

%%--------------------------------------------------------------------
%% Connack
%%--------------------------------------------------------------------

t_connack_session_present(Config) ->
    ConnFun = ?config(conn_fun, Config),
    {ok, Client1} = emqtt:start_link([
        {clientid, <<"t_connect_duplicate_clientid">>},
        {proto_ver, v5},
        {properties, #{'Session-Expiry-Interval' => 7200}},
        {clean_start, true}
        | Config
    ]),
    {ok, _} = emqtt:ConnFun(Client1),
    %% [MQTT-3.2.2-2]
    ?assertEqual(0, client_info(session_present, Client1)),
    ok = emqtt:disconnect(Client1),

    {ok, Client2} = emqtt:start_link([
        {clientid, <<"t_connect_duplicate_clientid">>},
        {proto_ver, v5},
        {properties, #{'Session-Expiry-Interval' => 7200}},
        {clean_start, false}
        | Config
    ]),
    {ok, _} = emqtt:ConnFun(Client2),
    %% [[MQTT-3.2.2-3]]
    ?assertEqual(1, client_info(session_present, Client2)),
    ok = emqtt:disconnect(Client2).

t_connack_max_qos_allowed(init, Config) ->
    Config;
t_connack_max_qos_allowed('end', _Config) ->
    emqx_config:put_zone_conf(default, [mqtt, max_qos_allowed], 2),
    emqx_config:put_zone_conf(default, [mqtt, max_qos_allowed], 2),
    ok.
t_connack_max_qos_allowed(Config) ->
    ConnFun = ?config(conn_fun, Config),
    process_flag(trap_exit, true),
    Topic = nth(1, ?TOPICS),

    %% max_qos_allowed = 0
    emqx_config:put_zone_conf(default, [mqtt, max_qos_allowed], 0),
    emqx_config:put_zone_conf(default, [mqtt, max_qos_allowed], 0),

    {ok, Client1} = emqtt:start_link([{proto_ver, v5} | Config]),
    {ok, Connack1} = emqtt:ConnFun(Client1),
    %% [MQTT-3.2.2-9]
    ?assertEqual(0, maps:get('Maximum-QoS', Connack1)),

    %% [MQTT-3.2.2-10]
    {ok, _, [0]} = emqtt:subscribe(Client1, Topic, 0),
    %% [MQTT-3.2.2-10]
    {ok, _, [1]} = emqtt:subscribe(Client1, Topic, 1),
    %% [MQTT-3.2.2-10]
    {ok, _, [2]} = emqtt:subscribe(Client1, Topic, 2),

    %% [MQTT-3.2.2-11]
    ?assertMatch(
        {error, {disconnected, 155, _}},
        emqtt:publish(Client1, Topic, <<"Unsupported Qos 1">>, qos1)
    ),
    ?assertEqual(155, receive_disconnect_reasoncode()),
    waiting_client_process_exit(Client1),

    {ok, Client2} = emqtt:start_link([
        {proto_ver, v5},
        {will_flag, true},
        {will_topic, Topic},
        {will_payload, <<"Unsupported Qos">>},
        {will_qos, 2}
        | Config
    ]),
    {error, Connack2} = emqtt:ConnFun(Client2),
    %% [MQTT-3.2.2-12]
    ?assertMatch({qos_not_supported, _}, Connack2),
    waiting_client_process_exit(Client2),

    %% max_qos_allowed = 1
    emqx_config:put_zone_conf(default, [mqtt, max_qos_allowed], 1),
    emqx_config:put_zone_conf(default, [mqtt, max_qos_allowed], 1),

    {ok, Client3} = emqtt:start_link([{proto_ver, v5} | Config]),
    {ok, Connack3} = emqtt:ConnFun(Client3),
    %% [MQTT-3.2.2-9]
    ?assertEqual(1, maps:get('Maximum-QoS', Connack3)),

    %% [MQTT-3.2.2-10]
    {ok, _, [0]} = emqtt:subscribe(Client3, Topic, 0),
    %% [MQTT-3.2.2-10]
    {ok, _, [1]} = emqtt:subscribe(Client3, Topic, 1),
    %% [MQTT-3.2.2-10]
    {ok, _, [2]} = emqtt:subscribe(Client3, Topic, 2),

    %% [MQTT-3.2.2-11]
    ?assertMatch(
        {error, {disconnected, 155, _}},
        emqtt:publish(Client3, Topic, <<"Unsupported Qos 2">>, qos2)
    ),
    ?assertEqual(155, receive_disconnect_reasoncode()),
    waiting_client_process_exit(Client3),

    {ok, Client4} = emqtt:start_link([
        {proto_ver, v5},
        {will_flag, true},
        {will_topic, Topic},
        {will_payload, <<"Unsupported Qos">>},
        {will_qos, 2}
        | Config
    ]),
    {error, Connack4} = emqtt:ConnFun(Client4),
    %% [MQTT-3.2.2-12]
    ?assertMatch({qos_not_supported, _}, Connack4),
    waiting_client_process_exit(Client4),

    %% max_qos_allowed = 2
    emqx_config:put_zone_conf(default, [mqtt, max_qos_allowed], 2),
    emqx_config:put_zone_conf(default, [mqtt, max_qos_allowed], 2),

    {ok, Client5} = emqtt:start_link([{proto_ver, v5} | Config]),
    {ok, Connack5} = emqtt:ConnFun(Client5),
    %% [MQTT-3.2.2-9]
    ?assertEqual(undefined, maps:get('Maximum-QoS', Connack5, undefined)),
    ok = emqtt:disconnect(Client5),
    waiting_client_process_exit(Client5),

    process_flag(trap_exit, false).

t_connack_assigned_clienid(Config) ->
    ConnFun = ?config(conn_fun, Config),
    {ok, Client1} = emqtt:start_link([{proto_ver, v5} | Config]),
    {ok, _} = emqtt:ConnFun(Client1),
    %%  [MQTT-3.2.2-16]
    ?assert(is_binary(client_info(clientid, Client1))),
    ok = emqtt:disconnect(Client1).

%%--------------------------------------------------------------------
%% Publish
%%--------------------------------------------------------------------

t_publish_rap(Config) ->
    ConnFun = ?config(conn_fun, Config),
    Topic = nth(1, ?TOPICS),

    {ok, Client1} = emqtt:start_link([{proto_ver, v5} | Config]),
    {ok, _} = emqtt:ConnFun(Client1),
    {ok, _, [2]} = emqtt:subscribe(Client1, #{}, [{Topic, [{rap, true}, {qos, 2}]}]),
    {ok, _} = emqtt:publish(
        Client1,
        Topic,
        #{},
        <<"retained message">>,
        [{qos, ?QOS_1}, {retain, true}]
    ),
    [Msg1 | _] = receive_messages(1),
    %% [MQTT-3.3.1-12]
    ?assertEqual(true, maps:get(retain, Msg1)),
    ok = emqtt:disconnect(Client1),

    {ok, Client2} = emqtt:start_link([{proto_ver, v5} | Config]),
    {ok, _} = emqtt:ConnFun(Client2),
    {ok, _, [2]} = emqtt:subscribe(Client2, #{}, [{Topic, [{rap, false}, {qos, 2}]}]),
    {ok, _} = emqtt:publish(
        Client2,
        Topic,
        #{},
        <<"retained message">>,
        [{qos, ?QOS_1}, {retain, true}]
    ),
    [Msg2 | _] = receive_messages(1),
    %% [MQTT-3.3.1-13]
    ?assertEqual(false, maps:get(retain, Msg2)),
    ok = emqtt:disconnect(Client2),

    clean_retained(Topic, Config).

t_publish_wildtopic(Config) ->
    ConnFun = ?config(conn_fun, Config),
    process_flag(trap_exit, true),
    Topic = nth(1, ?WILD_TOPICS),

    {ok, Client1} = emqtt:start_link([{proto_ver, v5} | Config]),
    {ok, _} = emqtt:ConnFun(Client1),
    ok = emqtt:publish(Client1, Topic, <<"error topic">>),
    ?assertEqual(144, receive_disconnect_reasoncode()),
    waiting_client_process_exit(Client1),

    process_flag(trap_exit, false).

t_publish_payload_format_indicator(Config) ->
    ConnFun = ?config(conn_fun, Config),
    Topic = nth(1, ?TOPICS),
    Properties = #{'Payload-Format-Indicator' => 233},

    {ok, Client1} = emqtt:start_link([{proto_ver, v5} | Config]),
    {ok, _} = emqtt:ConnFun(Client1),
    {ok, _, [2]} = emqtt:subscribe(Client1, Topic, qos2),
    ok = emqtt:publish(Client1, Topic, Properties, <<"Payload Format Indicator">>, [{qos, ?QOS_0}]),
    [Msg1 | _] = receive_messages(1),
    %% [MQTT-3.3.2-6]
    ?assertEqual(Properties, maps:get(properties, Msg1)),
    ok = emqtt:disconnect(Client1).

t_publish_topic_alias(Config) ->
    ConnFun = ?config(conn_fun, Config),
    process_flag(trap_exit, true),
    Topic = nth(1, ?TOPICS),

    {ok, Client1} = emqtt:start_link([{proto_ver, v5} | Config]),
    {ok, _} = emqtt:ConnFun(Client1),
    ok = emqtt:publish(Client1, Topic, #{'Topic-Alias' => 0}, <<"Topic-Alias">>, [{qos, ?QOS_0}]),
    %% [MQTT-3.3.2-8]
    ?assertEqual(148, receive_disconnect_reasoncode()),
    waiting_client_process_exit(Client1),

    {ok, Client2} = emqtt:start_link([{proto_ver, v5} | Config]),
    {ok, _} = emqtt:ConnFun(Client2),
    {ok, _, [2]} = emqtt:subscribe(Client2, Topic, qos2),
    ok = emqtt:publish(
        Client2,
        Topic,
        #{'Topic-Alias' => 233},
        <<"Topic-Alias">>,
        [{qos, ?QOS_0}]
    ),
    ok = emqtt:publish(
        Client2,
        <<"">>,
        #{'Topic-Alias' => 233},
        <<"Topic-Alias">>,
        [{qos, ?QOS_0}]
    ),
    %% [MQTT-3.3.2-12]
    ?assertEqual(2, length(receive_messages(2))),
    ok = emqtt:disconnect(Client2),
    waiting_client_process_exit(Client2),

    process_flag(trap_exit, false).

t_publish_response_topic(Config) ->
    ConnFun = ?config(conn_fun, Config),
    process_flag(trap_exit, true),
    Topic = nth(1, ?TOPICS),

    {ok, Client1} = emqtt:start_link([{proto_ver, v5} | Config]),
    {ok, _} = emqtt:ConnFun(Client1),
    ok = emqtt:publish(
        Client1,
        Topic,
        #{'Response-Topic' => nth(1, ?WILD_TOPICS)},
        <<"Response-Topic">>,
        [{qos, ?QOS_0}]
    ),
    %% [MQTT-3.3.2-14]
    ?assertEqual(130, receive_disconnect_reasoncode()),
    waiting_client_process_exit(Client1),

    process_flag(trap_exit, false).

t_publish_properties(Config) ->
    ConnFun = ?config(conn_fun, Config),
    Topic = nth(1, ?TOPICS),
    Properties = #{
        %% [MQTT-3.3.2-15]
        'Response-Topic' => Topic,
        %% [MQTT-3.3.2-16]
        'Correlation-Data' => <<"233">>,
        %% [MQTT-3.3.2-18]
        'User-Property' => [{<<"a">>, <<"2333">>}],
        %% [MQTT-3.3.2-20]
        'Content-Type' => <<"2333">>
    },

    {ok, Client1} = emqtt:start_link([{proto_ver, v5} | Config]),
    {ok, _} = emqtt:ConnFun(Client1),
    {ok, _, [2]} = emqtt:subscribe(Client1, Topic, qos2),
    ok = emqtt:publish(Client1, Topic, Properties, <<"Publish Properties">>, [{qos, ?QOS_0}]),
    [Msg1 | _] = receive_messages(1),
    %% [MQTT-3.3.2-16]
    ?assertEqual(Properties, maps:get(properties, Msg1)),
    ok = emqtt:disconnect(Client1).

t_publish_overlapping_subscriptions(Config) ->
    ConnFun = ?config(conn_fun, Config),
    Topic = nth(1, ?TOPICS),
    Properties = #{'Subscription-Identifier' => 2333},

    {ok, Client1} = emqtt:start_link([{proto_ver, v5} | Config]),
    {ok, _} = emqtt:ConnFun(Client1),
    {ok, _, [1]} = emqtt:subscribe(Client1, Properties, nth(1, ?WILD_TOPICS), qos1),
    {ok, _, [0]} = emqtt:subscribe(Client1, Properties, nth(3, ?WILD_TOPICS), qos0),
    {ok, _} = emqtt:publish(
        Client1,
        Topic,
        #{},
        <<"t_publish_overlapping_subscriptions">>,
        [{qos, ?QOS_2}]
    ),

    [Msg1 | _] = receive_messages(2),
    %% [MQTT-3.3.4-2]
    ?assert(maps:get(qos, Msg1) < 2),
    %% [MQTT-3.3.4-3]
    ?assertEqual(Properties, maps:get(properties, Msg1)),
    ok = emqtt:disconnect(Client1).

%%--------------------------------------------------------------------
%% Subsctibe
%%--------------------------------------------------------------------

t_subscribe_topic_alias(Config) ->
    ConnFun = ?config(conn_fun, Config),
    Topic1 = nth(1, ?TOPICS),
    Topic2 = nth(2, ?TOPICS),
    {ok, Client1} = emqtt:start_link([
        {proto_ver, v5},
        {properties, #{'Topic-Alias-Maximum' => 1}}
        | Config
    ]),
    {ok, _} = emqtt:ConnFun(Client1),
    {ok, _, [2]} = emqtt:subscribe(Client1, Topic1, qos2),
    {ok, _, [2]} = emqtt:subscribe(Client1, Topic2, qos2),

    ok = emqtt:publish(Client1, Topic1, #{}, <<"Topic-Alias">>, [{qos, ?QOS_0}]),
    [Msg1] = receive_messages(1),
    ?assertEqual({ok, #{'Topic-Alias' => 1}}, maps:find(properties, Msg1)),
    ?assertEqual({ok, Topic1}, maps:find(topic, Msg1)),

    ok = emqtt:publish(Client1, Topic1, #{}, <<"Topic-Alias">>, [{qos, ?QOS_0}]),
    [Msg2] = receive_messages(1),
    ?assertEqual({ok, #{'Topic-Alias' => 1}}, maps:find(properties, Msg2)),
    ?assertEqual({ok, <<>>}, maps:find(topic, Msg2)),

    ok = emqtt:publish(Client1, Topic2, #{}, <<"Topic-Alias">>, [{qos, ?QOS_0}]),
    [Msg3] = receive_messages(1),
    ?assertEqual({ok, #{}}, maps:find(properties, Msg3)),
    ?assertEqual({ok, Topic2}, maps:find(topic, Msg3)),

    ok = emqtt:disconnect(Client1).

t_subscribe_no_local(Config) ->
    ConnFun = ?config(conn_fun, Config),
    Topic = nth(1, ?TOPICS),

    {ok, Client1} = emqtt:start_link([{proto_ver, v5} | Config]),
    {ok, _} = emqtt:ConnFun(Client1),
    {ok, _, [2]} = emqtt:subscribe(Client1, #{}, [{Topic, [{nl, true}, {qos, 2}]}]),

    {ok, Client2} = emqtt:start_link([{proto_ver, v5} | Config]),
    {ok, _} = emqtt:ConnFun(Client2),
    {ok, _, [2]} = emqtt:subscribe(Client2, #{}, [{Topic, [{nl, true}, {qos, 2}]}]),

    ok = emqtt:publish(Client1, Topic, <<"t_subscribe_no_local">>, 0),
    %% [MQTT-3.8.3-3]
    ?assertEqual(1, length(receive_messages(2))),
    ok = emqtt:disconnect(Client1).

t_subscribe_no_local_mixed(Config) ->
    ConnFun = ?config(conn_fun, Config),
    Topic = nth(1, ?TOPICS),
    {ok, Client1} = emqtt:start_link([{proto_ver, v5} | Config]),
    {ok, _} = emqtt:ConnFun(Client1),

    {ok, Client2} = emqtt:start_link([{proto_ver, v5} | Config]),
    {ok, _} = emqtt:ConnFun(Client2),

    %% Given tow clients and  client1 subscribe to topic with 'no local' set to true
    {ok, _, [2]} = emqtt:subscribe(Client1, #{}, [{Topic, [{nl, true}, {qos, 2}]}]),

    %% When mixed publish traffic are sent from both clients (Client1 sent 6 and Client2 sent 2)
    CB = {fun emqtt:sync_publish_result/3, [self(), async_res]},
    ok = emqtt:publish_async(Client1, Topic, <<"t_subscribe_no_local_mixed1">>, 0, CB),
    ok = emqtt:publish_async(Client2, Topic, <<"t_subscribe_no_local_mixed2">>, 0, CB),
    ok = emqtt:publish_async(Client1, Topic, <<"t_subscribe_no_local_mixed3">>, 0, CB),
    ok = emqtt:publish_async(Client1, Topic, <<"t_subscribe_no_local_mixed4">>, 0, CB),
    ok = emqtt:publish_async(Client1, Topic, <<"t_subscribe_no_local_mixed5">>, 0, CB),
    ok = emqtt:publish_async(Client2, Topic, <<"t_subscribe_no_local_mixed6">>, 0, CB),
    ok = emqtt:publish_async(Client1, Topic, <<"t_subscribe_no_local_mixed7">>, 0, CB),
    ok = emqtt:publish_async(Client1, Topic, <<"t_subscribe_no_local_mixed8">>, 0, CB),
    [
        receive
            {async_res, Res} -> ?assertEqual(ok, Res)
        end
     || _ <- lists:seq(1, 8)
    ],

    %% Then only two messages from clients 2 are received
    PubRecvd = receive_messages(9),
    ct:pal("~p", [PubRecvd]),
    ?assertEqual(2, length(PubRecvd)),
    ok = emqtt:disconnect(Client1),
    ok = emqtt:disconnect(Client2).

t_subscribe_actions(Config) ->
    ConnFun = ?config(conn_fun, Config),
    Topic = nth(1, ?TOPICS),
    Properties = #{'Subscription-Identifier' => 2333},

    {ok, Client1} = emqtt:start_link([{proto_ver, v5} | Config]),
    {ok, _} = emqtt:ConnFun(Client1),
    {ok, _, [2]} = emqtt:subscribe(Client1, Properties, Topic, qos2),
    {ok, _, [1]} = emqtt:subscribe(Client1, Properties, Topic, qos1),
    {ok, _} = emqtt:publish(Client1, Topic, <<"t_subscribe_actions">>, 2),
    [Msg1 | _] = receive_messages(1),
    %% [MQTT-3.8.4-3] [MQTT-3.8.4-8]
    ?assertEqual(1, maps:get(qos, Msg1)),
    %% [MQTT-3.8.4-5] [MQTT-3.8.4-6] [MQTT-3.8.4-7]
    {ok, _, [2, 2]} = emqtt:subscribe(Client1, [
        {nth(1, ?TOPICS), qos2},
        {nth(2, ?TOPICS), qos2}
    ]),
    ok = emqtt:disconnect(Client1).
%%--------------------------------------------------------------------
%% Unsubsctibe Unsuback
%%--------------------------------------------------------------------

t_unscbsctibe(Config) ->
    ConnFun = ?config(conn_fun, Config),
    Topic1 = nth(1, ?TOPICS),
    Topic2 = nth(2, ?TOPICS),

    {ok, Client1} = emqtt:start_link([{proto_ver, v5} | Config]),
    {ok, _} = emqtt:ConnFun(Client1),
    {ok, _, [2]} = emqtt:subscribe(Client1, Topic1, qos2),
    %% [MQTT-3.10.4-4]
    {ok, _, [0]} = emqtt:unsubscribe(Client1, Topic1),
    %% [MQTT-3.10.4-5]
    {ok, _, [17]} = emqtt:unsubscribe(Client1, <<"noExistTopic">>),

    {ok, _, [2, 2]} = emqtt:subscribe(Client1, [{Topic1, qos2}, {Topic2, qos2}]),
    %% [[MQTT-3.10.4-6]]  [MQTT-3.11.3-1]  [MQTT-3.11.3-2]
    {ok, _, [0, 0, 17]} = emqtt:unsubscribe(Client1, [Topic1, Topic2, <<"noExistTopic">>]),
    ok = emqtt:disconnect(Client1).

%%--------------------------------------------------------------------
%% Pingreq
%%--------------------------------------------------------------------

t_pingreq(Config) ->
    ConnFun = ?config(conn_fun, Config),
    {ok, Client1} = emqtt:start_link([{proto_ver, v5} | Config]),
    {ok, _} = emqtt:ConnFun(Client1),
    %% [MQTT-3.12.4-1]
    pong = emqtt:ping(Client1),
    ok = emqtt:disconnect(Client1).

%%--------------------------------------------------------------------
%% Shared Subscriptions
%%--------------------------------------------------------------------

t_shared_subscriptions_client_terminates_when_qos_eq_2(init, Config) ->
    ok = meck:new(emqtt, [non_strict, passthrough, no_history, no_link]),
    Config;
t_shared_subscriptions_client_terminates_when_qos_eq_2('end', _Config) ->
    catch meck:unload(emqtt).

t_shared_subscriptions_client_terminates_when_qos_eq_2(Config) ->
    ConnFun = ?config(conn_fun, Config),
    process_flag(trap_exit, true),
    emqx_config:put([broker, shared_dispatch_ack_enabled], true),

    Topic = nth(1, ?TOPICS),
    SharedTopic = list_to_binary("$share/sharename/" ++ binary_to_list(<<"TopicA">>)),

    CRef = counters:new(1, [atomics]),
    meck:expect(
        emqtt,
        connected,
        fun
            (cast, {?PUBLISH_PACKET(?QOS_2, _PacketId), _Via}, _State) ->
                ok = counters:add(CRef, 1, 1),
                {stop, {shutdown, for_testing}};
            (Arg1, ARg2, Arg3) ->
                meck:passthrough([Arg1, ARg2, Arg3])
        end
    ),

    {ok, Sub1} = emqtt:start_link([
        {proto_ver, v5},
        {clientid, <<"sub_client_1">>},
        {keepalive, 5}
        | Config
    ]),
    {ok, _} = emqtt:ConnFun(Sub1),
    {ok, _, [2]} = emqtt:subscribe(Sub1, SharedTopic, qos2),

    {ok, Sub2} = emqtt:start_link([
        {proto_ver, v5},
        {clientid, <<"sub_client_2">>},
        {keepalive, 5}
        | Config
    ]),
    {ok, _} = emqtt:ConnFun(Sub2),
    {ok, _, [2]} = emqtt:subscribe(Sub2, SharedTopic, qos2),

    {ok, Pub} = emqtt:start_link([{proto_ver, v5}, {clientid, <<"pub_client">>} | Config]),
    {ok, _} = emqtt:ConnFun(Pub),
    {ok, _} = emqtt:publish(
        Pub,
        Topic,
        <<"t_shared_subscriptions_client_terminates_when_qos_eq_2">>,
        2
    ),

    receive
        {'EXIT', _, {shutdown, for_testing}} ->
            ok
    after 1000 ->
        ct:fail("disconnected timeout")
    end,

    ?assertEqual(1, counters:get(CRef, 1)),
    process_flag(trap_exit, false).

t_share_subscribe_no_local(Config) ->
    ConnFun = ?config(conn_fun, Config),
    process_flag(trap_exit, true),
    ShareTopic = <<"$share/sharename/TopicA">>,

    {ok, Client} = emqtt:start_link([{proto_ver, v5} | Config]),
    {ok, _} = emqtt:ConnFun(Client),
    %% MQTT-5.0 [MQTT-3.8.3-4] and [MQTT-4.13.1-1] (Disconnect)
    case catch emqtt:subscribe(Client, #{}, [{ShareTopic, [{nl, true}, {qos, 1}]}]) of
        {'EXIT', {Reason, _Stk}} ->
            ?assertEqual({disconnected, ?RC_PROTOCOL_ERROR, #{}}, Reason)
    end,

    process_flag(trap_exit, false).
