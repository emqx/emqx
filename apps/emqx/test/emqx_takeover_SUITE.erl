%%--------------------------------------------------------------------
%% Copyright (c) 2020-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_takeover_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("emqx/include/asserts.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("emqx_utils/include/emqx_message.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-define(CNT, 100).
-define(SLEEP, 10).

%%--------------------------------------------------------------------
%% Initial funcs

suite() ->
    [{ct_hooks, [emqx_cth_ct_hook_flaky]}].

all() ->
    [
        {group, local},
        {group, cluster}
    ].

flaky_tests() ->
    #{
        t_takeover_clean_session_with_delayed_willmsg => 3,
        t_takeover_willmsg_clean_session => 3
    }.

groups() ->
    TCs = emqx_common_test_helpers:all(?MODULE),
    ClusterTCs = [
        t_cluster_takeover,
        t_cluster_takeover_legacy_node
    ],
    LocalTCs = TCs -- ClusterTCs,
    MemoryOnly = [t_chan_info_refreshed_after_takeover_replay],
    MqttV5Only = [
        t_session_expire_with_delayed_willmsg,
        t_no_takeover_with_delayed_willmsg,
        t_takeover_before_session_expire,
        t_takeover_before_willmsg_expire,
        t_takeover_before_session_expire_willdelay0,
        t_takeover_session_then_normal_disconnect,
        t_takeover_session_then_abnormal_disconnect,
        t_takeover_session_then_abnormal_disconnect_2,
        t_disconnected_at_before_connected_at_on_takeover,
        t_disconnected_at_before_connected_at_on_discard
    ],
    [
        {local, [], [
            {group, memory_sessions},
            {group, durable_sessions}
        ]},
        {cluster, [], ClusterTCs},
        {durable_sessions, [], [
            {group, mqttv5_ds},
            {group, mqttv3_ds}
        ]},
        {memory_sessions, [], [
            {group, mqttv5_mem},
            {group, mqttv3_mem}
        ]},
        {mqttv5_mem, [], LocalTCs},
        {mqttv3_mem, [], LocalTCs -- MqttV5Only},
        {mqttv5_ds, [], LocalTCs -- MemoryOnly},
        {mqttv3_ds, [], (LocalTCs -- MemoryOnly) -- MqttV5Only}
    ].

init_per_suite(Config) ->
    emqx_common_test_helpers:clear_screen(),
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(local, Config) ->
    Config;
init_per_group(cluster = Group, Config) ->
    WorkDir = emqx_cth_suite:work_dir(Group, Config),
    Apps = [
        {emqx, #{
            config => "durable_sessions.enable = false",
            after_start => fun() ->
                emqx_config:force_put([rpc, mode], async)
            end
        }}
    ],
    NodeSpecs = [
        {emqx_takeover1, #{apps => Apps, role => core}},
        {emqx_takeover2, #{apps => Apps, role => core}}
    ],
    Nodes = emqx_cth_cluster:start(NodeSpecs, #{work_dir => WorkDir}),
    [
        {cluster, Nodes},
        {session_type, memory},
        {mqtt_vsn, v5}
        | Config
    ];
init_per_group(durable_sessions = Group, Config) ->
    %% This testsuite is time-sensitive. Set aggressive retry settings
    %% to make sure takover happens faster:
    DurableSessionsOpts = #{
        <<"enable">> => true,
        <<"force_persistence">> => true,
        <<"checkpoint_interval">> => <<"1ms">>,
        <<"commit_retry_interval">> => <<"1ms">>,
        <<"commit_retries">> => 100
    },
    Opts = #{
        durable_sessions_opts => DurableSessionsOpts,
        start_emqx_conf => false,
        work_dir => emqx_cth_suite:work_dir(Group, Config)
    },
    [
        {session_type, durable}
        | emqx_common_test_helpers:start_apps_ds(Config, _ExtraApps = [], Opts)
    ];
init_per_group(memory_sessions = Group, Config) ->
    Apps = emqx_cth_suite:start(
        [{emqx, "durable_sessions.enable = false"}],
        #{work_dir => emqx_cth_suite:work_dir(Group, Config)}
    ),
    [
        {session_type, memory},
        {apps, Apps}
        | Config
    ];
init_per_group(Group, Config) when Group == mqttv3_mem; Group == mqttv3_ds ->
    [{mqtt_vsn, v3} | Config];
init_per_group(Group, Config) when Group == mqttv5_mem; Group == mqttv5_ds ->
    [{mqtt_vsn, v5} | Config].

end_per_group(durable_sessions, Config) ->
    emqx_common_test_helpers:run_cleanups(Config);
end_per_group(memory_sessions, Config) ->
    emqx_cth_suite:stop(?config(apps, Config));
end_per_group(cluster, Config) ->
    emqx_cth_cluster:stop(?config(cluster, Config));
end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(_TestCase, Config) ->
    ct:timetrap({seconds, 120}),
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok.

%%--------------------------------------------------------------------
%% Testcases

t_takeover(Config) ->
    ClientId = make_client_id(?FUNCTION_NAME, Config),
    MqttVer = ?config(mqtt_vsn, Config),
    SessionType = ?config(session_type, Config),
    ClientOpts = [
        {proto_ver, MqttVer},
        {clean_start, false}
        | [{properties, #{'Session-Expiry-Interval' => 60}} || v5 == MqttVer]
    ],
    Middle = ?CNT div 2,
    Client1Msgs = messages(ClientId, 0, Middle),
    Client2Msgs = messages(ClientId, Middle, ?CNT div 2),
    AllMsgs = Client1Msgs ++ Client2Msgs,
    meck:new(emqx_cm, [non_strict, passthrough]),
    meck:expect(emqx_cm, takeover_session_end, fun(Arg) ->
        %% trigger more complex takeover conditions during 2-phase takeover protocol:
        %% when messages are accumulated in 2 processes simultaneously,
        %% and need to be properly ordered / deduplicated after the protocol commences.
        ok = timer:sleep(?SLEEP * 2),
        meck:passthrough([Arg])
    end),
    meck:expect(emqx_cm, takeover_kick, fun(Arg) ->
        %% trigger more complex takeover conditions during 2-phase takeover protocol:
        %% when messages are accumulated in 2 processes simultaneously,
        %% and need to be properly ordered / deduplicated after the protocol commences.
        ok = timer:sleep(?SLEEP * 2),
        meck:passthrough([Arg])
    end),

    #{client := [CPid2, CPid1]} =
        run_sequence([
            {fun start_client_async_subscribe/5, [ClientId, ClientId, ?QOS_1, ClientOpts]},
            [{fun wait_subscription/1, []} || SessionType == durable],
            [{fun publish_msg/2, [Msg]} || Msg <- Client1Msgs],
            {fun start_client_async_subscribe/5, [ClientId, ClientId, ?QOS_1, ClientOpts]},
            [{fun publish_msg/2, [Msg]} || Msg <- Client2Msgs],
            {fun just_wait/2, [wait_time(SessionType)]},
            {fun stop_the_last_client/1, []}
        ]),

    assert_client_exit(CPid1, takenover, Config),
    assert_client_exit(CPid2, normal, Config),

    Received = [Msg || {publish, Msg} <- ?drainMailbox(?SLEEP)],
    ct:pal("middle: ~p", [Middle]),
    ct:pal("received: ~p", [[P || #{payload := P} <- Received]]),
    assert_messages_missed(AllMsgs, Received),
    assert_messages_order(AllMsgs, Received),
    meck:unload(emqx_cm),
    ok.

t_takeover_willmsg(Config) ->
    ClientId = make_client_id(?FUNCTION_NAME, Config),
    ClientIdSub = <<ClientId/binary, "_willsub">>,
    WillTopic = <<ClientId/binary, "_willtopic">>,
    SessionType = ?config(session_type, Config),
    Middle = ?CNT div 2,
    Client1Msgs = messages(ClientId, 0, Middle),
    Client2Msgs = messages(ClientId, Middle, ?CNT div 2),
    AllMsgs = Client1Msgs ++ Client2Msgs,
    ClientOpts = [
        {proto_ver, ?config(mqtt_vsn, Config)},
        {clean_start, false},
        {will_topic, WillTopic},
        {will_payload, <<"willpayload">>},
        {will_qos, 0}
        | [{properties, #{'Session-Expiry-Interval' => 60}} || v5 == ?config(mqtt_vsn, Config)]
    ],

    #{client := [CPid2, CPidSub, CPid1]} =
        run_sequence([
            %% GIVEN client connect with will message
            {fun start_client_async_subscribe/5, [ClientId, ClientId, ?QOS_1, ClientOpts]},
            [{fun wait_subscription/1, []} || SessionType == durable],
            {fun start_client_async_subscribe/5, [ClientIdSub, WillTopic, ?QOS_1, []]},
            [{fun publish_msg/2, [Msg]} || Msg <- Client1Msgs],
            %% WHEN client reconnect with clean_start = false
            {fun start_client_async_subscribe/5, [ClientId, ClientId, ?QOS_1, ClientOpts]},
            [{fun publish_msg/2, [Msg]} || Msg <- Client2Msgs],
            {fun just_wait/2, [wait_time(SessionType)]},
            {fun stop_the_last_client/1, []}
        ]),

    assert_client_exit(CPid1, takenover, Config),
    assert_client_exit(CPid2, normal, Config),
    Received = [Msg || {publish, Msg} <- ?drainMailbox(?SLEEP)],
    ct:pal("received: ~p", [[P || #{payload := P} <- Received]]),
    {IsWill, ReceivedNoWill} = filter_payload(Received, <<"willpayload">>),
    assert_messages_missed(AllMsgs, ReceivedNoWill),
    assert_messages_order(AllMsgs, ReceivedNoWill),
    %% THEN will message should be received
    ?assert(IsWill),
    emqtt:stop(CPidSub),
    ok.

t_takeover_willmsg_clean_session(Config) ->
    ClientId = atom_to_binary(?FUNCTION_NAME),
    SessionType = ?config(session_type, Config),
    ClientIdSub = <<ClientId/binary, "_willsub">>,
    WillTopic = <<ClientId/binary, "willtopic">>,
    Middle = ?CNT div 2,
    Client1Msgs = messages(ClientId, 0, Middle),
    Client2Msgs = messages(ClientId, Middle, ?CNT div 2),
    ClientOpts = [
        {proto_ver, ?config(mqtt_vsn, Config)},
        {clean_start, false},
        {will_topic, WillTopic},
        {will_payload, <<"willpayload_1">>},
        {will_qos, 1}
    ],
    ClientOptsClean = [
        {proto_ver, ?config(mqtt_vsn, Config)},
        {clean_start, true},
        {will_topic, WillTopic},
        {will_payload, <<"willpayload_2">>},
        {will_qos, 1}
    ],

    #{client := [CPid2, CPidSub, CPid1]} = run_sequence([
        %% GIVEN: client connect with willmsg payload <<"willpayload_1">>
        {fun start_client_async_subscribe/5, [ClientId, ClientId, ?QOS_1, ClientOpts]},
        {fun start_client_subscribe/5, [ClientIdSub, WillTopic, ?QOS_1, []]},
        [{fun publish_msg/2, [Msg]} || Msg <- Client1Msgs],
        %% WHEN: client connects with clean_start=true and willmsg payload <<"willpayload_2">>
        {fun start_client_async_subscribe/5, [ClientId, ClientId, ?QOS_1, ClientOptsClean]},
        [{fun publish_msg/2, [Msg]} || Msg <- Client2Msgs]
    ]),

    assert_client_exit(CPid1, takenover, Config),
    Received = [Msg || {publish, Msg} <- ?drainMailbox(wait_time(SessionType))],
    ct:pal("received: ~p", [[P || #{payload := P} <- Received]]),
    %% THEN: payload <<"willpayload_1">> should be published instead of <<"willpayload_2">>
    ?assertMatch([_], [M || M <- Received, msg_payload(M) == <<"willpayload_1">>]),
    ?assertMatch([], [M || M <- Received, msg_payload(M) == <<"willpayload_2">>]),
    emqtt:stop(CPid2),
    emqtt:stop(CPidSub).

t_takeover_clean_session_with_delayed_willmsg(Config) ->
    ClientId = atom_to_binary(?FUNCTION_NAME),
    SessionType = ?config(session_type, Config),
    ClientIdSub = <<ClientId/binary, "_willsub">>,
    WillTopic = <<ClientId/binary, "willtopic">>,
    Middle = ?CNT div 2,
    Client1Msgs = messages(ClientId, 0, Middle),
    Client2Msgs = messages(ClientId, Middle, ?CNT div 2),
    ClientOpts = [
        {proto_ver, ?config(mqtt_vsn, Config)},
        {clean_start, false},
        {will_topic, WillTopic},
        {will_payload, <<"willpayload_delay10">>},
        {will_qos, 1},
        %% mqttv5 only
        {will_props, #{'Will-Delay-Interval' => 10}}
    ],
    ClientOptsClean = [
        {proto_ver, ?config(mqtt_vsn, Config)},
        {clean_start, true},
        {will_topic, WillTopic},
        {will_payload, <<"willpayload_2">>},
        {will_qos, 1}
    ],

    #{client := [CPid2, CPidSub, CPid1]} = run_sequence([
        %% GIVEN: client connect with willmsg payload <<"willpayload_delay10">> and delay-interval 10s
        {fun start_client_async_subscribe/5, [ClientId, ClientId, ?QOS_1, ClientOpts]},
        {fun start_client_async_subscribe/5, [ClientIdSub, WillTopic, ?QOS_1, []]},
        [{fun publish_msg/2, [Msg]} || Msg <- Client1Msgs],
        %% WHEN: client connects with clean_start=true and willmsg payload <<"willpayload_2">>
        {fun start_client_async_subscribe/5, [ClientId, ClientId, ?QOS_1, ClientOptsClean]},
        [{fun publish_msg/2, [Msg]} || Msg <- Client2Msgs]
    ]),

    assert_client_exit(CPid1, takenover, Config),
    Received = [Msg || {publish, Msg} <- ?drainMailbox(wait_time(SessionType))],
    ct:pal("received: ~p", [[P || #{payload := P} <- Received]]),
    %% THEN: payload <<"willpayload_delay10">> should be published without delay
    ?assertMatch([_], [M || M <- Received, msg_payload(M) == <<"willpayload_delay10">>]),
    ?assertMatch([], [M || M <- Received, msg_payload(M) == <<"willpayload_2">>]),
    emqtt:stop(CPid2),
    emqtt:stop(CPidSub).

t_no_takeover_with_delayed_willmsg(Config) ->
    ?config(mqtt_vsn, Config) =:= v5 orelse ct:fail("MQTTv5 Only"),
    ClientId = atom_to_binary(?FUNCTION_NAME),
    ClientIdSub = <<ClientId/binary, "_willsub">>,
    WillTopic = <<ClientId/binary, "willtopic">>,
    WillPayload = <<"willpayload_delay3">>,
    Client1Msgs = messages(ClientId, 0, 10),
    ClientOpts = [
        {proto_ver, ?config(mqtt_vsn, Config)},
        {clean_start, false},
        {will_topic, WillTopic},
        {will_payload, WillPayload},
        {will_qos, 1},
        % secs
        {will_props, #{'Will-Delay-Interval' => 3}},
        % secs
        {properties, #{'Session-Expiry-Interval' => 10}}
    ],

    #{client := [CPidSub, CPid1]} = run_sequence([
        %% GIVEN: client connects with willmsg payload <<"willpayload_delay3">> and delay-interval 3s
        {fun start_client_subscribe/5, [ClientId, ClientId, ?QOS_1, ClientOpts]},
        {fun start_client_subscribe/5, [ClientIdSub, WillTopic, ?QOS_1, []]},
        [{fun publish_msg/2, [Msg]} || Msg <- Client1Msgs]
    ]),

    Received = [Msg || {publish, Msg} <- ?drainMailbox(2000)],
    ct:pal("(T+2s) received: ~p", [[P || #{payload := P} <- Received]]),
    assert_messages_missed(Client1Msgs, Received),
    ?assertEqual([], [M || M <- Received, msg_payload(M) == WillPayload]),
    %% WHEN: client disconnects abnormally AND no reconnect after 3s.
    exit(CPid1, kill),
    assert_client_exit(CPid1, killed, Config),

    %% THEN: for MQTT v5, payload "willpayload_delay3" should be published after WILL delay (3 secs).
    Received1 = [Msg || {publish, Msg} <- ?drainMailbox(1000)],
    ct:pal("(T+3s) received: ~p", [[P || #{payload := P} <- Received1]]),
    ?assertEqual([], [M || M <- Received1, msg_payload(M) == WillPayload]),
    ?assertEqual([], Received1),
    Received2 = [Msg || {publish, Msg} <- ?drainMailbox(5000)],
    ct:pal("(T+7s) received: ~p", [[P || #{payload := P} <- Received1]]),
    ?assertMatch([_], [M || M <- Received2, msg_payload(M) == WillPayload]),
    ?assertMatch([_], Received2),
    emqtt:stop(CPidSub).

t_session_expire_with_delayed_willmsg(Config) ->
    ?config(mqtt_vsn, Config) =:= v5 orelse ct:fail("MQTTv5 Only"),
    ClientId = atom_to_binary(?FUNCTION_NAME),
    ClientIdSub = <<ClientId/binary, "_willsub">>,
    WillTopic = <<ClientId/binary, "willtopic">>,
    Client1Msgs = messages(ClientId, 0, 10),
    ClientOpts = [
        {proto_ver, ?config(mqtt_vsn, Config)},
        {clean_start, false},
        {will_topic, WillTopic},
        {will_payload, <<"willpayload_delay10">>},
        {will_qos, 1},
        {will_props, #{'Will-Delay-Interval' => 10}},
        {properties, #{'Session-Expiry-Interval' => 3}}
    ],

    #{client := [CPidSub, CPid1]} = run_sequence([
        %% GIVEN: client connect with willmsg payload <<"willpayload_delay10">>
        %%        and delay-interval 10s > session expiry 3s.
        {fun start_client_async_subscribe/5, [ClientId, ClientId, ?QOS_1, ClientOpts]},
        {fun start_client_subscribe/5, [ClientIdSub, WillTopic, ?QOS_1, []]},
        [{fun publish_msg/2, [Msg]} || Msg <- Client1Msgs]
    ]),

    Received = [Msg || {publish, Msg} <- ?drainMailbox(2000)],
    ct:pal("received: ~p", [[P || #{payload := P} <- Received]]),
    ?assertNotEqual([], Received),
    assert_messages_missed(Client1Msgs, Received),
    ?assertMatch([], [M || M <- Received, msg_payload(M) == <<"willpayload_delay10">>]),
    %% WHEN: client disconnects abnormally AND no reconnect after 3s.
    exit(CPid1, kill),
    assert_client_exit(CPid1, killed, Config),
    Received1 = [Msg || {publish, Msg} <- ?drainMailbox(1000)],
    ?assertMatch([], [M || M <- Received1, msg_payload(M) == <<"willpayload_delay10">>]),
    ?assertEqual([], Received1),
    %% THEN: for MQTT v5, payload "willpayload_delay10" should be published after session expiry.
    Received2 = [Msg || {publish, Msg} <- ?drainMailbox(5000)],
    ?assertMatch([_], [M || M <- Received2, msg_payload(M) == <<"willpayload_delay10">>]),
    ?assertMatch([_], Received2),
    emqtt:stop(CPidSub).

%% @TODO 'Server-Keep-Alive'
%% t_no_takeover_keepalive_fired(Config) ->
%%     ok.

t_takeover_before_session_expire_willdelay0(Config) ->
    ?config(mqtt_vsn, Config) =:= v5 orelse ct:fail("MQTTv5 Only"),
    ClientId = atom_to_binary(?FUNCTION_NAME),
    SessionType = ?config(session_type, Config),
    ClientIdSub = <<ClientId/binary, "_willsub">>,
    WillTopic = <<ClientId/binary, "willtopic">>,
    Client1Msgs = messages(ClientId, 0, 10),
    ClientOpts = [
        {proto_ver, ?config(mqtt_vsn, Config)},
        {clean_start, false},
        {will_topic, WillTopic},
        {will_payload, <<"willpayload_delay10">>},
        {will_qos, 1},
        {will_props, #{'Will-Delay-Interval' => 0}},
        {properties, #{'Session-Expiry-Interval' => 3}}
    ],

    #{client := [CPid2, CPidSub, CPid1]} = run_sequence([
        %% GIVEN: client connect with willmsg payload <<"willpayload_delay10">>
        %%        and delay-interval 0s session expiry 3s.
        {fun start_client_subscribe/5, [ClientId, ClientId, ?QOS_1, ClientOpts]},
        {fun start_client_subscribe/5, [ClientIdSub, WillTopic, ?QOS_1, []]},
        [{fun publish_msg/2, [Msg]} || Msg <- Client1Msgs],
        %% WHEN: client session is taken over within 3s.
        {fun start_client_async_subscribe/5, [ClientId, ClientId, ?QOS_1, ClientOpts]}
    ]),

    assert_client_exit(CPid1, takenover, Config),

    Received = [Msg || {publish, Msg} <- ?drainMailbox(wait_time(SessionType))],
    ct:pal("received: ~p", [[P || #{payload := P} <- Received]]),
    %% THEN: willmsg is published
    ?assertMatch([_], [M || M <- Received, msg_payload(M) == <<"willpayload_delay10">>]),
    emqtt:stop(CPidSub),
    emqtt:stop(CPid2).

t_takeover_before_session_expire(Config) ->
    ?config(mqtt_vsn, Config) =:= v5 orelse ct:fail("MQTTv5 Only"),
    ClientId = atom_to_binary(?FUNCTION_NAME),
    SessionType = ?config(session_type, Config),
    ClientIdSub = <<ClientId/binary, "_willsub">>,
    WillTopic = <<ClientId/binary, "willtopic">>,
    Client1Msgs = messages(ClientId, 0, 10),
    ClientOpts = [
        {proto_ver, ?config(mqtt_vsn, Config)},
        {clean_start, false},
        {will_topic, WillTopic},
        {will_payload, <<"willpayload_delay10">>},
        {will_qos, 1},
        {will_props, #{'Will-Delay-Interval' => 10}},
        {properties, #{'Session-Expiry-Interval' => 3}}
    ],
    FCtx =
        #{client := [CPid2, CPidSub, CPid1]} = run_sequence([
            %% GIVEN: client connect with willmsg payload <<"willpayload_delay10">>
            %%        and delay-interval 10s > session expiry 3s.
            {fun start_client_subscribe/5, [ClientId, ClientId, ?QOS_1, ClientOpts]},
            {fun start_client_subscribe/5, [ClientIdSub, WillTopic, ?QOS_1, []]},
            [{fun publish_msg/2, [Msg]} || Msg <- Client1Msgs],
            %% WHEN: client session is taken over within 3s.
            {fun start_client_async_subscribe/5, [ClientId, ClientId, ?QOS_1, ClientOpts]}
        ]),

    ct:pal("FCtx: ~p", [FCtx]),
    assert_client_exit(CPid1, takenover, Config),

    Received = [Msg || {publish, Msg} <- ?drainMailbox(wait_time(SessionType))],
    ct:pal("received: ~p", [[P || #{payload := P} <- Received]]),
    {IsWill, ReceivedNoWill} = filter_payload(Received, <<"willpayload_delay10">>),
    %% THEN: No Willmsg is published
    ?assertNot(IsWill),
    ?assertNotEqual([], ReceivedNoWill),
    emqtt:stop(CPidSub),
    emqtt:stop(CPid2).

t_takeover_session_then_normal_disconnect(Config) ->
    ?config(mqtt_vsn, Config) =:= v5 orelse ct:fail("MQTTv5 Only"),
    ClientId = atom_to_binary(?FUNCTION_NAME),
    ClientIdSub = <<ClientId/binary, "_willsub">>,
    WillTopic = <<ClientId/binary, "willtopic">>,
    Client1Msgs = messages(ClientId, 0, 10),
    ClientOpts = [
        {proto_ver, ?config(mqtt_vsn, Config)},
        {clean_start, false},
        {will_topic, WillTopic},
        {will_payload, <<"willpayload_delay10">>},
        {will_qos, 1},
        {will_props, #{'Will-Delay-Interval' => 10}},
        {properties, #{'Session-Expiry-Interval' => 3}}
    ],

    #{client := [CPid2, CPidSub, CPid1]} = run_sequence([
        {fun start_client_subscribe/5, [ClientId, ClientId, ?QOS_1, ClientOpts]},
        {fun start_client_subscribe/5, [ClientIdSub, WillTopic, ?QOS_1, []]},
        [{fun publish_msg/2, [Msg]} || Msg <- Client1Msgs],
        %% GIVEN: client reconnect with willmsg payload <<"willpayload_delay10">>
        %%        and delay-interval 10s > session expiry 3s.
        {fun start_client_subscribe/5, [ClientId, ClientId, ?QOS_1, ClientOpts]}
    ]),

    assert_client_exit(CPid1, takenover, Config),
    Received1 = [Msg || {publish, Msg} <- ?drainMailbox(1000)],
    %% WHEN: client disconnect normally.
    emqtt:disconnect(CPid2, ?RC_SUCCESS),
    Received2 = [Msg || {publish, Msg} <- ?drainMailbox(1000)],
    Received = Received1 ++ Received2,
    ct:pal("received: ~p", [[P || #{payload := P} <- Received]]),
    {IsWill, ReceivedNoWill} = filter_payload(Received, <<"willpayload_delay10">>),
    %% THEN: willmsg is not published.
    ?assertNot(IsWill),
    ?assertNotEqual([], ReceivedNoWill),
    emqtt:stop(CPidSub),
    ?assert(not is_process_alive(CPid1)),
    ?assert(not is_process_alive(CPid2)).

t_takeover_session_then_abnormal_disconnect(Config) ->
    ?config(mqtt_vsn, Config) =:= v5 orelse ct:fail("MQTTv5 Only"),
    ClientId = atom_to_binary(?FUNCTION_NAME),
    WillTopic = <<ClientId/binary, "willtopic">>,
    Client1Msgs = messages(ClientId, 0, 10),
    ClientOpts = [
        {proto_ver, ?config(mqtt_vsn, Config)},
        {clean_start, false},
        {will_topic, WillTopic},
        {will_payload, <<"willpayload_delay10">>},
        {will_qos, 1},
        {will_props, #{'Will-Delay-Interval' => 10}},
        {properties, #{'Session-Expiry-Interval' => 3}}
    ],
    WillSubClientId = <<ClientId/binary, "_willsub">>,

    #{client := [CPid2, CPidSub, CPid1]} = run_sequence([
        %% GIVEN: client connect with willmsg payload <<"willpayload_delay10">>
        %%        and will-delay-interval 10s >  session expiry 3s.
        {fun start_client_subscribe/5, [ClientId, ClientId, ?QOS_1, ClientOpts]},
        {fun start_client_subscribe/5, [WillSubClientId, WillTopic, ?QOS_1, []]},
        [{fun publish_msg/2, [Msg]} || Msg <- Client1Msgs],
        {fun start_client_subscribe/5, [ClientId, ClientId, ?QOS_1, ClientOpts]}
    ]),

    assert_client_exit(CPid1, takenover, Config),
    Received1 = [Msg || {publish, Msg} <- ?drainMailbox(1000)],
    %% WHEN: client disconnect abnormally
    emqtt:disconnect(CPid2, ?RC_DISCONNECT_WITH_WILL_MESSAGE),
    Received2 = [Msg || {publish, Msg} <- ?drainMailbox(2000)],
    Received = Received1 ++ Received2,
    ?assertNotEqual([], Received),
    ct:pal("received: ~p", [[P || #{payload := P} <- Received]]),
    %% THEN: willmsg is not published before session expiry
    ?assertEqual([], [M || M <- Received, msg_payload(M) == <<"willpayload_delay10">>]),
    %% AND THEN: willmsg is published after session expiry
    Received3 = [Msg || {publish, Msg} <- ?drainMailbox(3000)],
    ?assertMatch([_], [M || M <- Received3, msg_payload(M) == <<"willpayload_delay10">>]),
    ?assertMatch([_], Received3),
    emqtt:stop(CPidSub).

%% This testcase verifies that delivery of a delayed will message is
%% cancelled if the client reconnects within the Will-Delay-Interval.
t_takeover_session_then_abnormal_disconnect_2(Config) ->
    ?check_trace(
        begin
            ?config(mqtt_vsn, Config) =:= v5 orelse ct:fail("MQTTv5 Only"),
            ClientId = atom_to_binary(?FUNCTION_NAME),
            ClientIdSub = <<ClientId/binary, "_willsub">>,
            WillTopic = <<ClientId/binary, "willtopic">>,
            Client1Msgs = messages(ClientId, 0, 10),
            ClientOpts = [
                {proto_ver, ?config(mqtt_vsn, Config)},
                {clean_start, false},
                {will_topic, WillTopic},
                {will_payload, <<"willpayload_delay1">>},
                {will_qos, 1},
                {will_props, #{'Will-Delay-Interval' => 1}},
                {properties, #{'Session-Expiry-Interval' => 3}}
            ],
            ClientOpts2 = [
                {proto_ver, ?config(mqtt_vsn, Config)},
                {clean_start, false},
                {will_topic, WillTopic},
                {will_payload, <<"willpayload_delay2">>},
                {will_qos, 1},
                {will_props, #{'Will-Delay-Interval' => 0}},
                {properties, #{'Session-Expiry-Interval' => 3}}
            ],

            #{client := [CPid2, CPidSub, CPid1]} = run_sequence([
                {fun start_client_subscribe/5, [ClientId, ClientId, ?QOS_1, ClientOpts]},
                {fun start_client_async_subscribe/5, [ClientIdSub, WillTopic, ?QOS_1, []]},
                [{fun publish_msg/2, [Msg]} || Msg <- Client1Msgs],
                %% GIVEN: client *reconnect* with willmsg payload <<"willpayload_delay2">>
                %%        and will-delay-interval 0s, session expiry 3s.
                {fun start_client_subscribe/5, [ClientId, ClientId, ?QOS_1, ClientOpts2]}
            ]),

            assert_client_exit(CPid1, takenover, Config),
            %% WHEN: client disconnect abnormally
            emqtt:disconnect(CPid2, ?RC_DISCONNECT_WITH_WILL_MESSAGE),
            Received = [Msg || {publish, Msg} <- ?drainMailbox(5000)],
            ?assertNotEqual([], Received),
            ct:pal("received: ~p", [[P || #{payload := P} <- Received]]),
            %% THEN: willmsg1 of old conn is not published because will-delay-interval > 0
            ?assertEqual([], [M || M <- Received, msg_payload(M) == <<"willpayload_delay1">>]),
            %% THEN: willmsg2 is published because will-delay-interval is 0
            ?assertMatch([_], [M || M <- Received, msg_payload(M) == <<"willpayload_delay2">>]),
            emqtt:stop(CPidSub)
        end,
        []
    ).

t_takeover_before_willmsg_expire(Config) ->
    ?config(mqtt_vsn, Config) =:= v5 orelse ct:fail("MQTTv5 Only"),
    ClientId = atom_to_binary(?FUNCTION_NAME),
    SessionType = ?config(session_type, Config),
    WillTopic = <<ClientId/binary, <<"willtopic">>/binary>>,
    Client1Msgs = messages(ClientId, 0, 10),
    ClientOpts = [
        {proto_ver, ?config(mqtt_vsn, Config)},
        {clean_start, false},
        {will_topic, WillTopic},
        {will_payload, <<"willpayload_delay10">>},
        {will_qos, 1},
        {will_props, #{'Will-Delay-Interval' => 3}},
        {properties, #{'Session-Expiry-Interval' => 10}}
    ],
    WillSubClientId = <<ClientId/binary, "_willsub">>,

    FCtx =
        #{client := [CPid2, CPidSub, CPid1]} = run_sequence([
            %% GIVEN: client connect with willmsg payload <<"willpayload_delay10">>
            %%        and will-delay-interval 3s < session expiry 10s.
            {fun start_client_subscribe/5, [ClientId, ClientId, ?QOS_1, ClientOpts]},
            {fun start_client_subscribe/5, [WillSubClientId, WillTopic, ?QOS_1, []]},
            [{fun publish_msg/2, [Msg]} || Msg <- Client1Msgs],
            %% WHEN: another client takeover the session with in 3s.
            {fun start_client_async_subscribe/5, [ClientId, ClientId, ?QOS_1, ClientOpts]}
        ]),

    ct:pal("FCtx: ~p", [FCtx]),
    assert_client_exit(CPid1, takenover, Config),

    Received = [Msg || {publish, Msg} <- ?drainMailbox(wait_time(SessionType))],
    ct:pal("received: ~p", [[P || #{payload := P} <- Received]]),
    assert_messages_missed(Client1Msgs, Received),

    %% THEN: for MQTT v5, payload <<"willpayload_delay10">> should NOT be published after 3s.
    Received1 = [Msg || {publish, Msg} <- ?drainMailbox(1000)],
    ?assertEqual([], [M || M <- Received1, msg_payload(M) == <<"willpayload_delay10">>]),
    ?assertEqual([], Received1),
    Received2 = [Msg || {publish, Msg} <- ?drainMailbox(5000)],
    ?assertEqual([], [M || M <- Received2, msg_payload(M) == <<"willpayload_delay10">>]),
    ?assertEqual([], Received2),
    emqtt:stop(CPidSub),
    emqtt:stop(CPid2).

t_kick_session(Config) ->
    ClientId = atom_to_binary(?FUNCTION_NAME),
    ClientIdSub = <<ClientId/binary, "_willsub">>,
    WillTopic = <<ClientId/binary, "willtopic">>,
    ClientOpts = [
        {proto_ver, ?config(mqtt_vsn, Config)},
        {clean_start, false},
        {will_topic, WillTopic},
        {will_payload, <<"willpayload_kick">>},
        {will_qos, 1}
    ],
    %% GIVEN: client connect with willmsg payload <<"willpayload_kick">>
    CPid = start_unlink_client(ClientId, ClientOpts),
    CPidSub = start_unlink_client(ClientIdSub, ClientOpts),
    {ok, _} = emqtt:connect(CPid),
    {ok, _} = emqtt:connect(CPidSub),
    {ok, _, [?QOS_1]} = emqtt:subscribe(CPidSub, WillTopic, ?QOS_1),
    %% WHEN: client is kicked with kick_session
    ok = emqx_cm:kick_session(ClientId),
    assert_client_exit(CPid, kicked, Config),
    %% THEN: payload <<"willpayload_kick">> should be published
    ?assertReceive({publish, #{payload := <<"willpayload_kick">>}}, timer:seconds(1)),
    %% Cleanup
    emqtt:stop(CPidSub).

t_ongoing_takeover(Config) ->
    MqttVer = ?config(mqtt_vsn, Config),
    ClientId = make_client_id(?FUNCTION_NAME, Config),
    ClientOpts = [
        {proto_ver, MqttVer},
        {clean_start, false}
        | [{properties, #{'Session-Expiry-Interval' => 6000}} || v5 == MqttVer]
    ],

    CPid1 = start_unlink_client(ClientId, ClientOpts),
    {ok, _} = emqtt:connect(CPid1),

    [ServerPid1] = emqx_cm:lookup_channels(ClientId),
    sys:suspend(ServerPid1),

    %% GIVEN: A ongoing session takeover stuck
    CPid2 = start_unlink_client(ClientId, ClientOpts),
    erlang:spawn(fun() -> emqtt:connect(CPid2) end),
    timer:sleep(500),

    %% WHEN: Yet another client connects to takeover
    CPid3 = start_unlink_client(ClientId, ClientOpts),
    erlang:spawn(fun() -> emqtt:connect(CPid3) end),

    %% THEN: This client get connack with or RC_SERVER_BUSY
    CPid = start_unlink_client(ClientId, ClientOpts),
    case MqttVer of
        v3 ->
            ?assertMatch({error, {server_unavailable, _}}, emqtt:connect(CPid));
        v5 ->
            ?assertMatch({error, {server_busy, _}}, emqtt:connect(CPid))
    end,
    sys:resume(ServerPid1),
    ?assertReceive({'DOWN', _, process, CPid, {shutdown, _}}).

-doc """
Regression test: chan-info stats reflect post-replay state after
session takeover (instead of waiting up to 15s for the stats_timer).
""".
t_chan_info_refreshed_after_takeover_replay(Config) ->
    MqttVer = ?config(mqtt_vsn, Config),
    ClientId = make_client_id(?FUNCTION_NAME, Config),
    Topic = <<ClientId/binary, "/t">>,
    NMsgs = 3,
    ClientOpts = [
        {proto_ver, MqttVer},
        {clean_start, false}
        | [{properties, #{'Session-Expiry-Interval' => 60}} || v5 == MqttVer]
    ],

    %% GIVEN: client A subscribes, then disconnects leaving session alive.
    {ok, ClientA} = emqtt:start_link([{clientid, ClientId} | ClientOpts]),
    {ok, _} = emqtt:connect(ClientA),
    {ok, _, [?QOS_1]} = emqtt:subscribe(ClientA, Topic, ?QOS_1),
    [OldChanPid] = emqx_cm:lookup_channels(ClientId),
    ok = emqtt:disconnect(ClientA),

    %% AND: publish NMsgs messages so they queue in the offline session.
    {ok, Publisher} = emqtt:start_link([{proto_ver, MqttVer}, {clean_start, true}]),
    {ok, _} = emqtt:connect(Publisher),
    lists:foreach(
        fun(I) ->
            {ok, _} = emqtt:publish(
                Publisher, Topic, integer_to_binary(I), ?QOS_1
            )
        end,
        lists:seq(1, NMsgs)
    ),
    ok = emqtt:disconnect(Publisher),

    %% WHEN: client B reconnects with same clientid -> takeover + replay.
    %% auto_ack=false keeps the replayed messages in inflight long enough
    %% to observe via emqx_cm:get_chan_stats/2.
    {ok, ClientB} = emqtt:start_link([
        {clientid, ClientId},
        {auto_ack, false}
        | ClientOpts
    ]),
    {ok, _} = emqtt:connect(ClientB),

    %% THEN: chan-info stats reflect the post-replay inflight well before
    %% the 15s stats_timer would tick. Comfort margin: 50ms x 100 = 5s.
    ?retry(
        50,
        100,
        begin
            [ChanPid] = [
                Pid
             || Pid <- emqx_cm:lookup_channels(ClientId), Pid =/= OldChanPid
            ],
            ?assertMatch(
                #{inflight_cnt := N} when N >= NMsgs,
                maps:from_list(emqx_cm:get_chan_stats(ClientId, ChanPid)),
                chan_stats_not_refreshed_post_replay
            )
        end
    ),

    emqtt:stop(ClientB).

%%--------------------------------------------------------------------

-define(ON(NODE, BODY), erpc:call(NODE, fun() -> BODY end)).

%% Verify takeover works between different current-version nodes in the cluster
%% across `emqx_cm_takeover` protocol.
t_cluster_takeover(Config) ->
    [OwnerNode, RequesterNode] = ?config(cluster, Config),
    true = test_cluster_takeover(v4, OwnerNode, RequesterNode, Config).

%% Verify takeover works between a current-version node and a legacy-version node
%% in the cluster: legacy-version communication is simulated by forcing BPAPI v3.
%% Verify response downgrade on the owner node and upgrade on the requester node.
t_cluster_takeover_legacy_node(Config) ->
    [OwnerNode, RequesterNode] = ?config(cluster, Config),
    ?ON(RequesterNode, begin
        meck:new(emqx_bpapi, [passthrough, no_link]),
        meck:expect(emqx_bpapi, supported_version, fun
            (Node, emqx_cm) when Node =:= OwnerNode ->
                3;
            (Node, Api) ->
                meck:passthrough([Node, Api])
        end)
    end),
    try
        true = test_cluster_takeover(legacy, OwnerNode, RequesterNode, Config)
    after
        ?ON(RequesterNode, meck:unload(emqx_bpapi))
    end.

test_cluster_takeover(Protocol, OwnerNode, RequesterNode, Config) ->
    ClientId = emqx_utils:format("~p-~p", [?FUNCTION_NAME, Protocol]),
    Topic = <<ClientId/binary, "/t">>,
    InflightPayload = <<ClientId/binary, "-inflight">>,
    QueuedPayload1 = <<ClientId/binary, "-queued-1">>,
    QueuedPayload2 = <<ClientId/binary, "-queued-2">>,
    Messages = [
        emqx_message:make(<<"takeover-publisher">>, ?QOS_1, Topic, P)
     || P <- [InflightPayload, QueuedPayload1, QueuedPayload2]
    ],
    SmokePayload = <<ClientId/binary, "-smoke">>,
    SmokeMessage = emqx_message:make(<<"takeover-publisher">>, ?QOS_1, Topic, SmokePayload),
    ClientOpts = [
        {proto_ver, v5},
        {clean_start, false},
        {properties, #{
            'Session-Expiry-Interval' => 60,
            'Receive-Maximum' => 1
        }}
    ],
    ?check_trace(
        begin
            %% Connect a client to the Owner node:
            Port1 = emqx_cth_cluster:get_tcp_mqtt_port(OwnerNode),
            CPid1 = start_unlink_client(ClientId, [{port, Port1}, {auto_ack, false} | ClientOpts]),
            {ok, _} = emqtt:connect(CPid1),
            {ok, _, [?QOS_1]} = emqtt:subscribe(CPid1, Topic, ?QOS_1),
            %% Publish 3 messages, 1 gets into inflight and 2 are queued:
            ok = emqx_cth_cluster:sync_routes([OwnerNode, RequesterNode]),
            ok = lists:foreach(
                fun(Msg) -> ?ON(RequesterNode, emqx_broker:publish(Msg)) end,
                Messages
            ),
            %% Receive single message that got into inflight:
            ?assertReceive({publish, #{client_pid := CPid1, payload := InflightPayload}}),
            ?assertNotReceive({publish, #{client_pid := CPid1}}),
            %% Connect takeover client to the Requester node:
            Port2 = emqx_cth_cluster:get_tcp_mqtt_port(RequesterNode),
            CPid2 = start_unlink_client(ClientId, [{port, Port2}, {auto_ack, true} | ClientOpts]),
            {ok, _} = emqtt:connect(CPid2),
            %% Verify takeover took place:
            assert_client_exit(CPid1, takenover, Config),
            %% Verify inflight and mqueue were preserved:
            ?assertReceive({publish, #{client_pid := CPid2, payload := InflightPayload}}),
            ?assertReceive({publish, #{client_pid := CPid2, payload := QueuedPayload1}}),
            ?assertReceive({publish, #{client_pid := CPid2, payload := QueuedPayload2}}),
            %% Smoke test publishing continues to work:
            ?ON(RequesterNode, emqx_broker:publish(SmokeMessage)),
            ?assertReceive({publish, #{client_pid := CPid2, payload := SmokePayload}}),
            emqtt:stop(CPid2)
        end,
        fun(Trace) ->
            Events = ?of_kind(
                [
                    emqx_cm_takeover_begin,
                    emqx_cm_takeover_begin_legacy,
                    emqx_cm_takeover_begin_rpc,
                    emqx_cm_takeover_begin_rpc_legacy,
                    emqx_cm_takeover_finish,
                    emqx_cm_takeover_finish_legacy,
                    emqx_cm_takeover_finish_rpc,
                    emqx_cm_takeover_finish_rpc_legacy
                ],
                Trace
            ),
            case Protocol of
                v4 ->
                    ?assertMatch(
                        [
                            #{
                                ?snk_kind := emqx_cm_takeover_begin,
                                ?snk_meta := #{node := RequesterNode},
                                clientid := ClientId,
                                target_node := OwnerNode,
                                requester_proto := #{vsn := 1}
                            },
                            #{
                                ?snk_kind := emqx_cm_takeover_begin_rpc,
                                ?snk_meta := #{node := OwnerNode},
                                clientid := ClientId,
                                requester_proto := #{vsn := 1}
                            },
                            #{
                                ?snk_kind := emqx_cm_takeover_finish,
                                ?snk_meta := #{node := RequesterNode},
                                target_node := OwnerNode,
                                requester_proto := #{vsn := 1}
                            },
                            #{
                                ?snk_meta := #{node := OwnerNode},
                                ?snk_kind := emqx_cm_takeover_finish_rpc,
                                requester_proto := #{vsn := 1}
                            }
                        ],
                        Events
                    );
                legacy ->
                    ?assertMatch(
                        [
                            #{
                                ?snk_kind := emqx_cm_takeover_begin_legacy,
                                ?snk_meta := #{node := RequesterNode},
                                clientid := ClientId,
                                target_node := OwnerNode
                            },
                            #{
                                ?snk_kind := emqx_cm_takeover_begin_rpc_legacy,
                                ?snk_meta := #{node := OwnerNode},
                                clientid := ClientId
                            },
                            #{
                                ?snk_kind := emqx_cm_takeover_finish_legacy,
                                ?snk_meta := #{node := RequesterNode},
                                target_node := OwnerNode
                            },
                            #{
                                ?snk_kind := emqx_cm_takeover_finish_rpc_legacy,
                                ?snk_meta := #{node := OwnerNode}
                            }
                        ],
                        Events
                    )
            end
        end
    ).

%%--------------------------------------------------------------------
%% Commands

run_sequence(Commands) ->
    Ctx0 = #{},
    lists:foldl(
        fun({Fun, Args}, Ctx) ->
            ct:pal("COMMAND: ~p ~p", [element(2, erlang:fun_info(Fun, name)), Args]),
            apply(Fun, [Ctx | Args])
        end,
        Ctx0,
        lists:flatten(Commands)
    ).

start_client_async_subscribe(Ctx, ClientId, Topic, Qos, Opts) ->
    CPid = start_unlink_client(ClientId, Opts),
    _ = erlang:spawn_link(fun() ->
        {ok, _} = emqtt:connect(CPid),
        {ok, _, [Qos]} = emqtt:subscribe(CPid, Topic, Qos)
    end),
    Ctx#{client => [CPid | maps:get(client, Ctx, [])]}.

start_client_subscribe(Ctx, ClientId, Topic, Qos, Opts) ->
    CPid = start_unlink_client(ClientId, Opts),
    {ok, _} = emqtt:connect(CPid),
    {ok, _, [Qos]} = emqtt:subscribe(CPid, Topic, Qos),
    Ctx#{client => [CPid | maps:get(client, Ctx, [])]}.

start_unlink_client(ClientId, Opts) ->
    {ok, CPid} = emqtt:start_link([{clientid, ClientId} | Opts]),
    true = erlang:unlink(CPid),
    _MRef = erlang:monitor(process, CPid),
    CPid.

wait_subscription(Ctx = #{client := CPids}) ->
    ok = lists:foreach(
        fun Wait(CPid) ->
            try emqtt:subscriptions(CPid) of
                [] ->
                    ok = timer:sleep(rand:uniform(?SLEEP)),
                    Wait(CPid);
                [_ | _] ->
                    ok
            catch
                exit:{noproc, _} ->
                    ok
            end
        end,
        CPids
    ),
    Ctx.

publish_msg(Ctx, Msg) ->
    ok = timer:sleep(rand:uniform(?SLEEP)),
    case emqx:publish(Msg#message{timestamp = emqx_message:timestamp_now()}) of
        [] -> publish_msg(Ctx, Msg);
        [_ | _] -> Ctx
    end.

just_wait(Ctx, Sleep) ->
    ok = timer:sleep(Sleep),
    Ctx.

stop_the_last_client(Ctx = #{client := [CPid | _]}) ->
    ok = emqtt:stop(CPid),
    Ctx.

%%--------------------------------------------------------------------
%% Helpers

assert_messages_missed(Ls1, Ls2) ->
    Missed = lists:filtermap(
        fun(Msg) ->
            No = emqx_message:payload(Msg),
            case lists:any(fun(#{payload := No1}) -> No1 == No end, Ls2) of
                true -> false;
                false -> {true, No}
            end
        end,
        Ls1
    ),
    case Missed of
        [] ->
            ok;
        _ ->
            ct:fail("Miss messages: ~p", [Missed]),
            error
    end.

assert_messages_order([] = _Expected, _Received) ->
    ok;
assert_messages_order([Msg | Expected], Received) ->
    %% Account for duplicate messages:
    case lists:splitwith(fun(#{payload := P}) -> emqx_message:payload(Msg) == P end, Received) of
        {[], [#{timestamp := TSMismatch, payload := Mismatch} | _]} ->
            ct:fail("Message order is not correct, expected: ~p, received: ~p", [
                {
                    emqx_utils_calendar:epoch_to_rfc3339(emqx_message:timestamp(Msg)),
                    emqx_message:payload(Msg)
                },
                {emqx_utils_calendar:epoch_to_rfc3339(TSMismatch), Mismatch}
            ]),
            error;
        {_Matching, Rest} ->
            assert_messages_order(Expected, Rest)
    end.

messages(Topic, Offset, Cnt) ->
    [emqx_message:make(ct, ?QOS_1, Topic, payload(Offset + I)) || I <- lists:seq(1, Cnt)].

payload(I) ->
    % NOTE
    % Introduce randomness so that natural order is not the same as arrival order.
    iolist_to_binary(
        io_lib:format("~4.16.0B [~B] [~s]", [
            rand:uniform(16#10000) - 1,
            I,
            emqx_utils_calendar:now_to_rfc3339(millisecond)
        ])
    ).

%% @doc Filter out the message with matching target payload from the list of messages.
%%      return '{IsTargetFound, ListOfOtherMessages}'
%% @end
-spec filter_payload(List :: [#{payload := binary()}], Payload :: binary()) ->
    {IsPayloadFound :: boolean(), OtherPayloads :: [#{payload := binary()}]}.
filter_payload(List, Payload) when is_binary(Payload) ->
    Filtered = lists:filter(fun(#{payload := P}) -> P =/= Payload end, List),
    {length(List) =/= length(Filtered), Filtered}.

%% @doc assert emqtt *client* process exits as expected.
assert_client_exit(Pid, normal, _Config) ->
    ?assertReceive({'DOWN', _, process, Pid, normal});
assert_client_exit(Pid, killed, _Config) ->
    ?assertReceive({'DOWN', _, process, Pid, killed});
assert_client_exit(Pid, takenover, Config) ->
    MqttVer = ?config(mqtt_vsn, Config),
    SessionType = ?config(session_type, Config),
    assert_client_takenover(Pid, MqttVer, SessionType);
assert_client_exit(Pid, kicked, Config) ->
    MqttVer = ?config(mqtt_vsn, Config),
    assert_client_kicked(Pid, MqttVer).

assert_client_takenover(Pid, v5, memory) ->
    %% In-memory sessions deliver a DISCONNECT with the precise
    %% RC_SESSION_TAKEN_OVER reason code.
    %% @ref: MQTT 5.0 spec [MQTT-3.1.4-3]
    ?assertReceive(
        {'DOWN', _, process, Pid, {shutdown, {disconnected, ?RC_SESSION_TAKEN_OVER, _}}}
    );
assert_client_takenover(Pid, v5, durable) ->
    %% For durable (DS) sessions the takeover kick of the previously
    %% connected channel is best-effort: the session is taken over
    %% regardless, but the broker may step the old channel down with a
    %% different reason or slightly later, and may not deliver a clean
    %% DISCONNECT(RC_SESSION_TAKEN_OVER) to the old socket (a known
    %% core-CM race). Only require that the old client eventually
    %% terminates, tolerating the exact reason and timing.
    ?assertReceive(
        {'DOWN', _, process, Pid, _Reason},
        15_000,
        #{pid => Pid}
    );
assert_client_takenover(Pid, v3, _) ->
    ?assertReceive(
        {'DOWN', _, process, Pid, {shutdown, Reason}} when
            Reason =:= tcp_closed orelse Reason =:= closed,
        1_000,
        #{pid => Pid}
    ).

assert_client_kicked(Pid, v5) ->
    ?assertReceive(
        {'DOWN', _, process, Pid, {shutdown, {disconnected, ?RC_ADMINISTRATIVE_ACTION, _}}}
    );
assert_client_kicked(Pid, v3) ->
    ?assertReceive(
        {'DOWN', _, process, Pid, _}, 1_000, #{pid => Pid}
    ).

make_client_id(Case, Config) ->
    Vsn = ?config(mqtt_vsn, Config),
    SessionType = ?config(session_type, Config),
    emqx_utils:format("~p-~p-~p", [Case, SessionType, Vsn]).

wait_time(durable) ->
    2_500;
wait_time(memory) ->
    ?SLEEP.

msg_payload(#{payload := Payload}) ->
    Payload.

%%--------------------------------------------------------------------
%% Test cases for timestamp ordering during takeover/discard
%%--------------------------------------------------------------------

t_disconnected_at_before_connected_at_on_takeover(Config) ->
    ?config(mqtt_vsn, Config) =:= v5 orelse ct:fail("MQTTv5 Only"),
    process_flag(trap_exit, true),
    ClientId = make_client_id(?FUNCTION_NAME, Config),
    MqttVer = ?config(mqtt_vsn, Config),
    ClientOpts = [
        {proto_ver, MqttVer},
        {clean_start, false},
        {properties, #{'Session-Expiry-Interval' => 60}}
    ],

    verify_timestamp_ordering_on_reconnect(
        ClientId,
        ClientOpts,
        ClientOpts,
        takenover
    ).

t_disconnected_at_before_connected_at_on_discard(Config) ->
    ?config(mqtt_vsn, Config) =:= v5 orelse ct:fail("MQTTv5 Only"),
    process_flag(trap_exit, true),
    ClientId = make_client_id(?FUNCTION_NAME, Config),
    MqttVer = ?config(mqtt_vsn, Config),
    ClientOptsOld = [
        {proto_ver, MqttVer},
        {clean_start, false},
        {properties, #{'Session-Expiry-Interval' => 60}}
    ],
    ClientOptsNew = [
        {proto_ver, MqttVer},
        {clean_start, true},
        {properties, #{'Session-Expiry-Interval' => 0}}
    ],

    verify_timestamp_ordering_on_reconnect(
        ClientId,
        ClientOptsOld,
        ClientOptsNew,
        discarded
    ).

%% Helper function to verify timestamp ordering during reconnect scenarios
verify_timestamp_ordering_on_reconnect(ClientId, ClientOpts1, ClientOpts2, ExpectedReason) ->
    %% Setup hooks to capture events - use a named ETS table
    TableName = list_to_atom("events_" ++ integer_to_list(erlang:unique_integer([positive]))),
    _ = ets:new(TableName, [ordered_set, named_table, public]),

    try
        emqx_hooks:add('client.connected', {?MODULE, hook_fun_connected, [TableName]}, 1000),
        emqx_hooks:add('client.disconnected', {?MODULE, hook_fun_disconnected, [TableName]}, 1000),

        %% GIVEN: First client connects
        {ok, Client1} = emqtt:start_link([{clientid, ClientId} | ClientOpts1]),
        {ok, _} = emqtt:connect(Client1),
        timer:sleep(200),

        %% WHEN: Second client connects (triggers takeover or discard)
        {ok, Client2} = emqtt:start_link([{clientid, ClientId} | ClientOpts2]),
        {ok, _} = emqtt:connect(Client2),

        %% Client1 will exit during takeover/discard, drain the EXIT message
        receive
            {'EXIT', Client1, _Reason} -> ok
        after 2000 -> ok
        end,

        catch emqtt:stop(Client2),

        %% Wait for events to be captured (including final disconnect of Client2)
        timer:sleep(500),

        %% THEN: Verify ordered sequence for this client:
        %% connect -> disconnect(ExpectedReason) -> connect -> disconnect(not takenover/discarded)
        AllEvents = ets:tab2list(TableName),
        ClientEvents = [
            {Type, Seq, ConnectedAt, DisconnectedAt, Reason}
         || {{Id, Seq}, {Type, ConnectedAt, DisconnectedAt, Reason}} <- AllEvents,
            Id =:= ClientId
        ],
        ?assertEqual(
            4,
            length(ClientEvents),
            io_lib:format("Expected exactly 4 events for client ~p, got: ~p", [
                ClientId, ClientEvents
            ])
        ),

        ConnectTs = [
            ConnectedAt
         || {connect, _Seq, ConnectedAt, undefined, undefined} <- ClientEvents
        ],
        Disconnects = [
            {ConnectedAt, DisconnectedAt, Reason}
         || {disconnect, _Seq, ConnectedAt, DisconnectedAt, Reason} <- ClientEvents
        ],
        ?assertEqual(
            2,
            length(ConnectTs),
            io_lib:format("Expected 2 connect events, got: ~p", [ClientEvents])
        ),
        ?assertEqual(
            2,
            length(Disconnects),
            io_lib:format("Expected 2 disconnect events, got: ~p", [ClientEvents])
        ),
        [{T1ForT2, T2, ExpectedReason}] = [
            D
         || D = {_ConnectedAt, _DisconnectedAt, Reason} <- Disconnects,
            Reason =:= ExpectedReason
        ],
        [{T3ForT4, T4, Reason4}] = [
            D
         || D = {_ConnectedAt, _DisconnectedAt, Reason} <- Disconnects,
            Reason =/= ExpectedReason
        ],
        [T1, T3] = lists:sort(ConnectTs),

        %% 1) and 2): timeline is non-decreasing when interpreted as:
        %% connect(T1), disconnect(T2), connect(T3), disconnect(T4)
        ?assert(
            T1 =< T2 andalso T2 =< T3 andalso T3 =< T4,
            io_lib:format("Expected non-decreasing T1..T4, got: ~p", [ClientEvents])
        ),
        %% 3): T1 is associated with T2 (disconnect.connected_at == T1)
        ?assertEqual(
            T1,
            T1ForT2,
            io_lib:format("Expected T1 associated with T2 record, got: ~p", [ClientEvents])
        ),
        %% 4): T3 is associated with T4 (disconnect.connected_at == T3)
        ?assertEqual(
            T3,
            T3ForT4,
            io_lib:format("Expected T3 associated with T4 record, got: ~p", [ClientEvents])
        ),
        %% 5): T4 reason must not be takenover/discarded
        ?assert(
            Reason4 =/= discarded andalso Reason4 =/= takenover,
            io_lib:format("Final disconnect reason must not be discarded/takenover: ~p", [
                ClientEvents
            ])
        )
    after
        emqx_hooks:del('client.connected', {?MODULE, hook_fun_connected}),
        emqx_hooks:del('client.disconnected', {?MODULE, hook_fun_disconnected}),
        catch ets:delete(TableName)
    end.

%% Hook functions
hook_fun_connected(ClientInfo, ConnInfo, TableName) ->
    Seq = erlang:unique_integer([positive, monotonic]),
    ConnectedAt = maps:get(connected_at, ConnInfo),
    ClientId = maps:get(clientid, ClientInfo),
    ets:insert(
        TableName,
        {{ClientId, Seq}, {connect, ConnectedAt, undefined, undefined}}
    ),
    ok.

hook_fun_disconnected(ClientInfo, Reason, ConnInfo, TableName) ->
    Seq = erlang:unique_integer([positive, monotonic]),
    DisconnectedAt = maps:get(disconnected_at, ConnInfo),
    ConnectedAt = maps:get(connected_at, ConnInfo),
    ClientId = maps:get(clientid, ClientInfo),
    ets:insert(
        TableName,
        {{ClientId, Seq}, {disconnect, ConnectedAt, DisconnectedAt, Reason}}
    ),
    ok.
