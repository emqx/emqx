%%--------------------------------------------------------------------
%% Copyright (c) 2021-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_quic_multistreams_SUITE).

-ifndef(BUILD_WITHOUT_QUIC).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("emqx/include/asserts.hrl").
-include_lib("emqx/include/emqx_cm.hrl").
-include_lib("emqx_utils/include/emqx_message.hrl").

suite() ->
    [{timetrap, {seconds, 60}}].

all() ->
    [
        t_connect,
        {group, mstream},
        {group, shutdown},
        {group, misc},
        t_malformed_packet,
        t_wrong_stream_connect,
        t_zero_rtt_pubsub,
        t_zero_rtt_large_payload,
        t_zero_rtt_stream_continue,
        t_keepalive_data_only_timeout,
        t_keepalive_data_stream_active,
        t_stream_finish,
        t_stream_reset,
        t_stream_stop,
        t_manual_ack_qos1,
        t_manual_ack_qos2,
        t_session_resume_qos1,
        t_session_resume_qos2,
        t_session_store_qos1,
        t_session_store_qos2,
        t_mqtt_v5_basic,
        t_mqtt_v5_session,
        t_mqtt_v5_publish_properties,
        t_mqtt_v5_no_local,
        t_mqtt_v5_invalid_packets,
        t_mqtt_v5_batch_subscribe,
        t_mqtt_v5_subscribe_max_qos,
        t_mqtt_v5_max_qos_allowed,
        t_mqtt_v5_publish_packet_too_large,
        t_mqtt_v5_shared_qos2_abort,
        t_mqtt_v5_connack_client_id_unavailable,
        t_mqtt_v5_connect_will_message,
        t_mqtt_v5_connect_will_retain,
        t_mqtt_v5_connect_packet_too_large,
        t_mqtt_v5_max_qos_will_rejection,
        t_mqtt_v5_connack_unavailable_no_will,
        t_mqtt_v5_emit_stats_timeout,
        t_mqtt_v5_deliver_packet_too_large,
        t_mqtt_v5_subscribe_topic_alias,
        t_broker_connected_client_count_persistent,
        t_broker_connected_client_count_anonymous,
        t_broker_connected_client_count_transient_takeover,
        t_broker_connected_client_stats,
        t_source_bind,
        t_source_rebind,
        t_listener_with_lowlevel_settings,
        t_listener_inval_settings
    ].

groups() ->
    [
        {mstream, [], [{group, profiles}]},
        {profiles, [], [
            {group, profile_low_latency},
            {group, profile_max_throughput}
        ]},
        {profile_low_latency, [], [
            {group, pub_qos0},
            {group, pub_qos1},
            {group, pub_qos2}
        ]},
        {profile_max_throughput, [], [
            {group, pub_qos0},
            {group, pub_qos1},
            {group, pub_qos2}
        ]},
        {pub_qos0, [], [
            {group, sub_qos0},
            {group, sub_qos1},
            {group, sub_qos2}
        ]},
        {pub_qos1, [], [
            {group, sub_qos0},
            {group, sub_qos1},
            {group, sub_qos2}
        ]},
        {pub_qos2, [], [
            {group, sub_qos0},
            {group, sub_qos1},
            {group, sub_qos2}
        ]},
        {sub_qos0, [], [{group, qos}]},
        {sub_qos1, [], [{group, qos}]},
        {sub_qos2, [], [{group, qos}]},
        {qos, [], qos_cases()},
        {shutdown, [], [
            {group, graceful_shutdown},
            {group, abort_recv_shutdown},
            {group, abort_send_shutdown},
            {group, abort_send_recv_shutdown}
        ]},
        {graceful_shutdown, [], shutdown_groups()},
        {abort_recv_shutdown, [], shutdown_groups()},
        {abort_send_shutdown, [], shutdown_groups()},
        {abort_send_recv_shutdown, [], shutdown_groups()},
        {ctrl_stream_shutdown, [], [
            t_multi_streams_shutdown_ctrl_stream,
            t_multi_streams_shutdown_ctrl_stream_then_reconnect,
            t_multi_streams_remote_shutdown,
            t_multi_streams_emqx_ctrl_kill,
            t_multi_streams_emqx_ctrl_exit_normal,
            t_multi_streams_remote_shutdown_with_reconnect
        ]},
        {data_stream_shutdown, [], [
            t_multi_streams_shutdown_pub_data_stream,
            t_multi_streams_shutdown_sub_data_stream
        ]},
        {misc, [], [
            t_conn_silent_close,
            t_client_conn_bump_streams,
            t_olp_true,
            t_olp_reject,
            t_conn_resume,
            t_conn_without_ctrl_stream,
            t_data_stream_race_ctrl_stream
        ]}
    ].

qos_cases() ->
    [
        t_multi_streams_sub,
        t_multi_streams_pub_5x100,
        t_multi_streams_pub_parallel,
        t_multi_streams_pub_parallel_no_blocking,
        t_multi_streams_sub_pub_async,
        t_multi_streams_sub_pub_sync,
        t_multi_streams_unsub,
        t_multi_streams_corr_topic,
        t_multi_streams_unsub_via_other,
        t_multi_streams_dup_sub,
        t_multi_streams_packet_boundary,
        t_multi_streams_packet_malform,
        t_multi_streams_kill_sub_stream,
        t_multi_streams_packet_too_large,
        t_multi_streams_sub_0_rtt,
        t_multi_streams_sub_0_rtt_large_payload,
        t_multi_streams_sub_0_rtt_stream_data_cont,
        t_conn_change_client_addr
    ].

shutdown_groups() ->
    [
        {group, ctrl_stream_shutdown},
        {group, data_stream_shutdown}
    ].

init_per_suite(Config) ->
    Port = emqx_common_test_helpers:select_free_port(quic),
    Apps = start_emqx(Config, Port),
    [{port, Port}, {apps, Apps} | Config].

end_per_suite(Config) ->
    emqx_cth_suite:stop(?config(apps, Config)).

init_per_group(pub_qos0, Config) ->
    [{pub_qos, 0} | Config];
init_per_group(sub_qos0, Config) ->
    [{sub_qos, 0} | Config];
init_per_group(pub_qos1, Config) ->
    [{pub_qos, 1} | Config];
init_per_group(sub_qos1, Config) ->
    [{sub_qos, 1} | Config];
init_per_group(pub_qos2, Config) ->
    [{pub_qos, 2} | Config];
init_per_group(sub_qos2, Config) ->
    [{sub_qos, 2} | Config];
init_per_group(graceful_shutdown, Config) ->
    [{stream_shutdown_mode, "graceful"} | Config];
init_per_group(abort_recv_shutdown, Config) ->
    [{stream_shutdown_mode, "abort-receive"} | Config];
init_per_group(abort_send_shutdown, Config) ->
    [{stream_shutdown_mode, "abort-send"} | Config];
init_per_group(abort_send_recv_shutdown, Config) ->
    [{stream_shutdown_mode, "abort-both"} | Config];
init_per_group(_, Config) ->
    Config.

end_per_group(_, Config) ->
    Config.

t_connect(Config) ->
    run_scenario("connect", Config, []).

t_multistream_pubsub(Config) ->
    run_scenario("multistream", Config, qos_args(Config)).

t_unsubscribe(Config) ->
    run_scenario("unsubscribe", Config, qos_args(Config)).

t_malformed_packet(Config) ->
    run_scenario("malformed", Config, ["--malformed-hex", "00000000000000000000"]).

t_wrong_stream_connect(Config) ->
    run_scenario("wrong-stream-connect", Config, []).

t_zero_rtt_pubsub(Config) ->
    run_zero_rtt(Config, "zero-rtt-pubsub", []).

t_zero_rtt_large_payload(Config) ->
    run_zero_rtt(Config, "zero-rtt-large-payload", ["--timeout-ms", "30000"]).

t_zero_rtt_stream_continue(Config) ->
    run_zero_rtt(Config, "zero-rtt-stream-continue", []).

t_conn_resume(Config) ->
    run_scenario("conn-resume", Config, []).

t_data_stream_race_control_stream(Config) ->
    run_scenario("data-stream-race-control-stream", Config, []).

t_keepalive_data_only_timeout(Config) ->
    run_scenario("keepalive-data-only-timeout", Config, [
        "--keep-alive",
        "1",
        "--timeout-ms",
        "10000"
    ]).

t_keepalive_data_stream_active(Config) ->
    run_scenario("keepalive-data-stream-active", Config, [
        "--keep-alive",
        "1",
        "--timeout-ms",
        "10000"
    ]).

t_stream_finish(Config) ->
    run_scenario("stream-finish", Config, []).

t_stream_reset(Config) ->
    run_scenario("stream-reset", Config, []).

t_stream_stop(Config) ->
    run_scenario("stream-stop", Config, []).

t_manual_ack_qos1(Config) ->
    run_scenario("manual-ack-qos1", Config, []).

t_manual_ack_qos2(Config) ->
    run_scenario("manual-ack-qos2", Config, []).

t_session_resume_qos1(Config) ->
    run_scenario("session-resume-qos1", Config, []).

t_session_resume_qos2(Config) ->
    run_scenario("session-resume-qos2", Config, []).

t_session_store_qos1(Config) ->
    run_scenario("session-store-qos1", Config, []).

t_session_store_qos2(Config) ->
    run_scenario("session-store-qos2", Config, []).

t_mqtt_v5_basic(Config) ->
    %% Covers the former QUIC executions of t_basic_test, t_basic_large_packets,
    %% t_subscribe_actions, t_unsubscribe, and t_pingreq.
    run_scenario("mqtt-v5-basic", Config, ["--timeout-ms", "30000"]).

t_mqtt_v5_session(Config) ->
    %% Covers clean start, live/stale takeover, duplicate client ids, Session
    %% Present, assigned client ids, and reconnecting an unresponsive old client.
    run_scenario("mqtt-v5-session", Config, ["--timeout-ms", "30000"]).

t_mqtt_v5_publish_properties(Config) ->
    %% Covers RAP, payload format, PUBLISH properties, and overlapping subscriptions.
    run_scenario("mqtt-v5-publish-properties", Config, []).

t_mqtt_v5_no_local(Config) ->
    %% Covers both single and mixed-traffic No Local behavior.
    run_scenario("mqtt-v5-no-local", Config, []).

t_mqtt_v5_invalid_packets(Config) ->
    %% Covers wildcard topic names, invalid response topics, Topic Alias zero and
    %% reuse, and No Local on a shared subscription.
    run_scenario("mqtt-v5-invalid-packets", Config, []).

t_mqtt_v5_batch_subscribe(Config) ->
    emqx_config:put_zone_conf(default, [authorization, enable], true),
    ok = meck:new(emqx_access_control, [non_strict, passthrough, no_history, no_link]),
    meck:expect(emqx_access_control, authorize, fun(_, _, _) -> deny end),
    try
        run_scenario("mqtt-v5-batch-subscribe", Config, [])
    after
        emqx_config:put_zone_conf(default, [authorization, enable], false),
        meck:unload(emqx_access_control)
    end.

t_mqtt_v5_subscribe_max_qos(Config) ->
    OldMQTT = emqx_config:get_zone_conf(default, [mqtt]),
    #{mqtt := MQTTConf} = check_zone_config(
        "mqtt {"
        "\n max_qos_allowed = 2"
        "\n subscription_max_qos_rules = ["
        "\n   { topic { equals = \"t\" }, qos = 1 }"
        "\n   { topic { matches = \"glob/+/#\" }, qos = 0 }"
        "\n ] }"
    ),
    emqx_config:put_zone_conf(default, [mqtt], MQTTConf),
    try
        run_scenario("mqtt-v5-subscribe-max-qos", Config, [])
    after
        emqx_config:put_zone_conf(default, [mqtt], OldMQTT)
    end.

t_mqtt_v5_max_qos_allowed(Config) ->
    OldMax = emqx_config:get_zone_conf(default, [mqtt, max_qos_allowed]),
    try
        lists:foreach(
            fun(MaxQoS) ->
                emqx_config:put_zone_conf(default, [mqtt, max_qos_allowed], MaxQoS),
                run_scenario("mqtt-v5-max-qos", Config, [
                    "--sub-qos", integer_to_list(MaxQoS)
                ])
            end,
            [0, 1, 2]
        )
    after
        emqx_config:put_zone_conf(default, [mqtt, max_qos_allowed], OldMax)
    end.

t_mqtt_v5_publish_packet_too_large(Config) ->
    OldMax = emqx_config:get_zone_conf(default, [mqtt, max_packet_size]),
    emqx_config:put_zone_conf(default, [mqtt, max_packet_size], 1024),
    try
        run_scenario("mqtt-v5-publish-too-large", Config, [])
    after
        emqx_config:put_zone_conf(default, [mqtt, max_packet_size], OldMax)
    end.

t_mqtt_v5_shared_qos2_abort(Config) ->
    emqx_config:put([broker, shared_dispatch_ack_enabled], true),
    try
        run_scenario("mqtt-v5-shared-qos2-abort", Config, [])
    after
        emqx_config:put([broker, shared_dispatch_ack_enabled], false)
    end.

t_mqtt_v5_connack_client_id_unavailable(Config) ->
    ClientId = unique_name("connack-unavailable"),
    ClientIdBin = list_to_binary(ClientId),
    DeadPid = spawn(fun() -> exit(normal) end),
    true = ets:insert(?CHAN_CONN_TAB, #chan_conn{
        pid = DeadPid,
        mod = emqx_connection,
        clientid = ClientIdBin
    }),
    ok = emqx_cm_registry:register_channel({ClientIdBin, DeadPid}),
    try
        run_scenario_as("mqtt-v5-connack-unavailable", Config, ClientId, [])
    after
        ok = emqx_cm_registry:unregister_channel({ClientIdBin, DeadPid}),
        true = ets:delete(?CHAN_CONN_TAB, DeadPid)
    end.

t_mqtt_v5_connect_will_message(Config) ->
    run_scenario("mqtt-v5-will-message", Config, [
        "--will",
        "--will-payload",
        "will message"
    ]).

t_mqtt_v5_connect_will_retain(Config) ->
    lists:foreach(
        fun(Retain) ->
            run_scenario("mqtt-v5-will-retain", Config, [
                "--will",
                "--will-retain",
                atom_to_list(Retain)
            ])
        end,
        [false, true]
    ).

t_mqtt_v5_connect_packet_too_large(Config) ->
    OldMax = emqx_config:get_zone_conf(default, [mqtt, max_packet_size]),
    emqx_config:put_zone_conf(default, [mqtt, max_packet_size], 1024),
    try
        run_scenario("mqtt-v5-connect-packet-too-large", Config, [
            "--will",
            "--will-payload",
            lists:duplicate(1024, $a)
        ])
    after
        emqx_config:put_zone_conf(default, [mqtt, max_packet_size], OldMax)
    end.

t_mqtt_v5_max_qos_will_rejection(Config) ->
    OldMax = emqx_config:get_zone_conf(default, [mqtt, max_qos_allowed]),
    try
        lists:foreach(
            fun(MaxQoS) ->
                emqx_config:put_zone_conf(default, [mqtt, max_qos_allowed], MaxQoS),
                run_scenario("mqtt-v5-will-qos-rejected", Config, [
                    "--will", "--will-qos", "2"
                ])
            end,
            [0, 1]
        ),
        emqx_config:put_zone_conf(default, [mqtt, max_qos_allowed], 2),
        run_scenario("connect", Config, ["--will", "--will-qos", "2"])
    after
        emqx_config:put_zone_conf(default, [mqtt, max_qos_allowed], OldMax)
    end.

t_mqtt_v5_connack_unavailable_no_will(Config) ->
    ClientId = unique_name("connack-unavailable-will"),
    ClientIdBin = list_to_binary(ClientId),
    WillTopic = list_to_binary("ct/quic/" ++ ClientId ++ "/will"),
    DeadPid = spawn(fun() -> exit(normal) end),
    true = ets:insert(?CHAN_CONN_TAB, #chan_conn{
        pid = DeadPid,
        mod = emqx_connection,
        clientid = ClientIdBin
    }),
    ok = emqx_cm_registry:register_channel({ClientIdBin, DeadPid}),
    emqx_broker:subscribe(WillTopic),
    try
        try
            run_scenario_as("mqtt-v5-connack-unavailable", Config, ClientId, [
                "--will",
                "--will-topic",
                binary_to_list(WillTopic),
                "--will-payload",
                "WillMsg"
            ])
        after
            ok = emqx_cm_registry:unregister_channel({ClientIdBin, DeadPid}),
            true = ets:delete(?CHAN_CONN_TAB, DeadPid)
        end,
        emqx_broker:publish(#message{topic = WillTopic, payload = <<"NotWillMsg">>}),
        ?assertReceive({deliver, WillTopic, #message{payload = <<"NotWillMsg">>}}, 1000),
        ?assertNotReceive({deliver, WillTopic, #message{payload = <<"WillMsg">>}}, 100)
    after
        emqx_broker:subscriber_down(self())
    end.

t_mqtt_v5_emit_stats_timeout(Config) ->
    OldIdleTimeout = emqx_config:get_zone_conf(default, [mqtt, idle_timeout]),
    emqx_config:put_zone_conf(default, [mqtt, idle_timeout], 1000),
    ClientId = unique_name("stats-timer"),
    ClientIdBin = list_to_binary(ClientId),
    Client = start_async_scenario(Config, "mqtt-v5-stats-timer", ClientId, [
        "--keep-alive",
        "60",
        "--hold-ms",
        "4000"
    ]),
    try
        wait_async_client_ready(Client),
        [ClientPid] = emqx_cm:lookup_channels(ClientIdBin),
        ?assertMatch(
            TRef when is_reference(TRef),
            emqx_connection:info(stats_timer, sys:get_state(ClientPid))
        ),
        ?retry(
            100,
            30,
            ?assertEqual(
                undefined,
                emqx_connection:info(stats_timer, sys:get_state(ClientPid))
            )
        ),
        wait_async_client(Client)
    after
        emqx_config:put_zone_conf(default, [mqtt, idle_timeout], OldIdleTimeout)
    end.

t_mqtt_v5_deliver_packet_too_large(Config) ->
    ClientId = unique_name("deliver-too-large"),
    ClientIdBin = list_to_binary(ClientId),
    Client =
        #{
            topic := Topic
        } = start_async_scenario(Config, "mqtt-v5-receive-too-large", ClientId, [
            "--maximum-packet-size",
            "1024",
            "--hold-ms",
            "4000"
        ]),
    wait_async_client_ready(Client),
    Payload = binary:copy(<<"X">>, 1024),
    Message = emqx_message:make(<<?MODULE_STRING>>, 1, list_to_binary(Topic), Payload),
    ?assertMatch([{_, _, {ok, 1}}], emqx_broker:publish(Message)),
    [ChanPid] = emqx_cm:lookup_channels(ClientIdBin),
    ConnMod = emqx_cm:do_get_chann_conn_mod(ClientIdBin, ChanPid),
    ?retry(
        100,
        30,
        ?assertMatch(
            #{'send_msg.dropped.too_large' := 1},
            maps:from_list(ConnMod:stats(ChanPid))
        )
    ),
    wait_async_client(Client).

t_mqtt_v5_subscribe_topic_alias(Config) ->
    run_scenario("subscribe-topic-alias", Config, [
        "--topic-alias-maximum",
        "1"
    ]).

t_broker_connected_client_count_persistent(Config) ->
    reset_connected_clients(),
    ClientId = unique_name("broker-persistent"),
    ClientIdBin = list_to_binary(ClientId),
    Baseline = emqx_cm:get_connected_client_count(),
    SessionDir = filename:join(?config(priv_dir, Config), unique_name("session-store")),
    Client1 = start_async_scenario(Config, "session-checkpoint", ClientId, [
        "--session-store-dir",
        SessionDir,
        "--clean-start",
        "false",
        "--session-expiry-interval",
        "30",
        "--hold-ms",
        "1500"
    ]),
    wait_async_client_ready(Client1),
    ?retry(100, 20, ?assertEqual(Baseline + 1, emqx_cm:get_connected_client_count())),
    wait_async_client(Client1),
    ?retry(100, 30, ?assertEqual(Baseline, emqx_cm:get_connected_client_count())),

    Client2 = start_async_scenario(Config, "session-restore", ClientId, [
        "--session-store-dir",
        SessionDir,
        "--clean-start",
        "false",
        "--session-expiry-interval",
        "30",
        "--hold-ms",
        "5000"
    ]),
    wait_async_client_ready(Client2),
    Client3 = start_async_scenario(Config, "session-restore", ClientId, [
        "--session-store-dir",
        SessionDir,
        "--clean-start",
        "false",
        "--session-expiry-interval",
        "30",
        "--hold-ms",
        "5000"
    ]),
    wait_async_client_ready(Client3),
    ?retry(100, 30, ?assertEqual(Baseline + 1, emqx_cm:get_connected_client_count())),
    [ChanPid] = emqx_cm:lookup_channels(ClientIdBin),
    exit(ChanPid, kill),
    ?retry(100, 30, ?assertEqual(Baseline, emqx_cm:get_connected_client_count())),
    stop_async_client(Client2),
    stop_async_client(Client3).

t_broker_connected_client_count_anonymous(Config) ->
    reset_connected_clients(),
    Baseline = emqx_cm:get_connected_client_count(),
    BaselineChannels = emqx_cm:all_channels(),
    Client1 = start_async_client(Config, "", ["--hold-ms", "5000"]),
    wait_async_client_ready(Client1),
    Client2 = start_async_client(Config, "", ["--hold-ms", "5000"]),
    wait_async_client_ready(Client2),
    ?retry(100, 30, ?assertEqual(Baseline + 2, emqx_cm:get_connected_client_count())),
    [First | Rest] = emqx_cm:all_channels() -- BaselineChannels,
    exit(First, kill),
    ?retry(100, 30, ?assertEqual(Baseline + 1, emqx_cm:get_connected_client_count())),
    lists:foreach(fun(Pid) -> exit(Pid, kill) end, Rest),
    ?retry(100, 30, ?assertEqual(Baseline, emqx_cm:get_connected_client_count())),
    stop_async_client(Client1),
    stop_async_client(Client2).

t_broker_connected_client_count_transient_takeover(Config) ->
    reset_connected_clients(),
    ClientId = unique_name("broker-transient"),
    Baseline = emqx_cm:get_connected_client_count(),
    Clients = [
        start_async_client(Config, ClientId, ["--hold-ms", "1000"])
     || _ <- lists:seq(1, 20)
    ],
    ?retry(
        100,
        50,
        begin
            Count = emqx_cm:get_connected_client_count(),
            ?assert(Count >= Baseline),
            ?assert(Count =< Baseline + 1),
            ?assert(emqx_stats:getstat('live_connections.max') >= 1)
        end
    ),
    lists:foreach(fun wait_async_client_allow_failure/1, Clients),
    ?retry(100, 50, ?assertEqual(Baseline, emqx_cm:get_connected_client_count())).

t_broker_connected_client_stats(Config) ->
    reset_connected_clients(),
    Baseline = emqx_cm:get_connected_client_count(),
    ok = supervisor:terminate_child(emqx_kernel_sup, emqx_stats),
    {ok, _} = supervisor:restart_child(emqx_kernel_sup, emqx_stats),
    emqx_cm:stats_fun(),
    ?assertEqual(Baseline, emqx_stats:getstat('live_connections.count')),
    ClientId = unique_name("broker-stats"),
    Client = start_async_client(Config, ClientId, ["--hold-ms", "5000"]),
    wait_async_client_ready(Client),
    emqx_cm:stats_fun(),
    ?retry(100, 20, ?assertEqual(Baseline + 1, emqx_stats:getstat('live_connections.count'))),
    ?assert(emqx_stats:getstat('live_connections.max') >= Baseline + 1),
    [ChanPid] = emqx_cm:lookup_channels(list_to_binary(ClientId)),
    exit(ChanPid, kill),
    ?retry(100, 30, ?assertEqual(Baseline, emqx_cm:get_connected_client_count())),
    emqx_cm:stats_fun(),
    ?retry(100, 20, ?assertEqual(Baseline, emqx_stats:getstat('live_connections.count'))),
    stop_async_client(Client).

reset_connected_clients() ->
    lists:foreach(fun(Pid) -> exit(Pid, kill) end, emqx_cm:all_channels()),
    ?retry(100, 250, ?assertEqual(0, emqx_cm:get_connected_client_count())).

t_source_bind(Config) ->
    run_scenario("source-bind", Config, ["--local-bind-addr", "127.0.0.1:0"]).

t_source_rebind(Config) ->
    run_source_rebind(Config).

t_quic_sock(_Config) ->
    maybe_skip_missing_runner_scenario("raw emqtt_quic socket send/recv against test QUIC server").

t_quic_sock_fail(_Config) ->
    maybe_skip_missing_runner_scenario("raw emqtt_quic socket connection failure").

t_0_rtt(Config) ->
    run_zero_rtt(Config, "zero-rtt-pubsub", []).

t_0_rtt_fail(_Config) ->
    maybe_skip_missing_runner_scenario("invalid externally supplied QUIC session ticket").

t_keep_alive(Config) ->
    run_scenario("keepalive-data-only-timeout", Config, [
        "--keep-alive",
        "1",
        "--timeout-ms",
        "10000"
    ]).

t_keep_alive_idle_ctrl_stream(Config) ->
    run_scenario("keepalive-data-stream-active", Config, [
        "--keep-alive",
        "1",
        "--timeout-ms",
        "10000"
    ]).

t_multi_streams_sub(Config) ->
    run_scenario("pubsub", Config, qos_args(Config)).

t_multi_streams_pub_5x100(Config) ->
    run_scenario(
        "multistream-pub-5x100",
        Config,
        qos_args(Config) ++ ["--timeout-ms", "30000"]
    ).

t_multi_streams_pub_parallel(Config) ->
    run_scenario("parallel-publish", Config, qos_args(Config)).

t_multi_streams_pub_parallel_no_blocking(Config) ->
    run_scenario("parallel-no-blocking", Config, qos_args(Config)).

t_multi_streams_sub_pub_async(Config) ->
    run_scenario("multistream", Config, qos_args(Config)).

t_multi_streams_sub_pub_sync(Config) ->
    run_scenario("multistream", Config, qos_args(Config)).

t_multi_streams_unsub(Config) ->
    run_scenario("unsubscribe", Config, qos_args(Config)).

t_multi_streams_corr_topic(Config) ->
    run_scenario("correlation-topic", Config, qos_args(Config)).

t_multi_streams_unsub_via_other(Config) ->
    run_scenario("unsubscribe-via-other", Config, qos_args(Config)).

t_multi_streams_dup_sub(Config) ->
    run_scenario("duplicate-subscribe", Config, qos_args(Config)).

t_multi_streams_packet_boundary(Config) ->
    run_scenario("packet-boundary", Config, qos_args(Config) ++ ["--timeout-ms", "30000"]).

t_multi_streams_packet_malform(Config) ->
    run_scenario("malformed", Config, ["--malformed-hex", "00000000000000000000"]).

t_multi_streams_kill_sub_stream(Config) ->
    run_scenario("stream-reset", Config, qos_args(Config)).

t_multi_streams_packet_too_large(Config) ->
    OldMax = emqx_config:get_zone_conf(default, [mqtt, max_packet_size]),
    emqx_config:put_zone_conf(default, [mqtt, max_packet_size], 1000),
    try
        run_scenario("packet-too-large", Config, qos_args(Config))
    after
        emqx_config:put_zone_conf(default, [mqtt, max_packet_size], OldMax)
    end.

t_multi_streams_sub_0_rtt(Config) ->
    run_zero_rtt(Config, "zero-rtt-pubsub", qos_args(Config)).

t_multi_streams_sub_0_rtt_large_payload(Config) ->
    run_zero_rtt(
        Config,
        "zero-rtt-large-payload",
        qos_args(Config) ++ ["--timeout-ms", "30000"]
    ).

t_multi_streams_sub_0_rtt_stream_data_cont(Config) ->
    run_zero_rtt(Config, "zero-rtt-stream-continue", qos_args(Config)).

t_conn_change_client_addr(Config) ->
    run_source_rebind(Config).

t_multi_streams_shutdown_pub_data_stream(Config) ->
    run_scenario("stream-finish", Config, qos_args(Config)).

t_multi_streams_shutdown_sub_data_stream(Config) ->
    run_scenario("stream-stop", Config, qos_args(Config)).

t_multi_streams_shutdown_ctrl_stream(Config) ->
    run_scenario(
        "control-stream-shutdown",
        Config,
        qos_args(Config) ++ control_stream_shutdown_args(Config)
    ).

t_multi_streams_shutdown_ctrl_stream_then_reconnect(Config) ->
    run_scenario(
        "control-stream-shutdown-reconnect",
        Config,
        qos_args(Config) ++ control_stream_shutdown_args(Config)
    ).

t_multi_streams_remote_shutdown(Config) ->
    ClientId = unique_name("remote-shutdown"),
    Client = start_async_scenario(Config, "wait-remote-shutdown", ClientId, qos_args(Config)),
    wait_async_client_ready(Client),
    ok = stop_emqx(Config),
    try
        wait_async_client(Client)
    after
        start_emqx_dirty(Config)
    end.

t_multi_streams_emqx_ctrl_kill(Config) ->
    ClientId = unique_name("ctrl-kill"),
    Client = start_async_scenario(Config, "wait-remote-shutdown", ClientId, qos_args(Config)),
    wait_async_client_ready(Client),
    [{_, TransPid}] = ets:lookup(?CHAN_TAB, list_to_binary(ClientId)),
    exit(TransPid, kill),
    wait_async_client(Client).

t_multi_streams_emqx_ctrl_exit_normal(Config) ->
    ClientId = unique_name("ctrl-exit-normal"),
    Client = start_async_scenario(Config, "wait-remote-shutdown", ClientId, qos_args(Config)),
    wait_async_client_ready(Client),
    [{_, TransPid}] = ets:lookup(?CHAN_TAB, list_to_binary(ClientId)),
    emqx_connection:stop(TransPid),
    wait_async_client(Client).

t_multi_streams_remote_shutdown_with_reconnect(Config) ->
    ClientId = unique_name("remote-shutdown-reconnect"),
    Client = start_async_scenario(
        Config,
        "wait-remote-shutdown-reconnect",
        ClientId,
        qos_args(Config) ++
            [
                "--clean-start",
                "false",
                "--session-expiry-interval",
                "30",
                "--timeout-ms",
                "30000"
            ]
    ),
    wait_async_client_ready(Client),
    restart_emqx(Config),
    wait_async_client(Client).

t_conn_silent_close(Config) ->
    run_scenario("silent-close", Config, ["--keep-alive", "1", "--timeout-ms", "10000"]).

t_client_conn_bump_streams(_Config) ->
    maybe_skip_missing_runner_scenario("client-side connection stream-count setting change").

t_olp_true(_Config) ->
    maybe_skip_missing_runner_scenario(
        "overload protection pass-through on accepted QUIC connection"
    ).

t_olp_reject(_Config) ->
    maybe_skip_missing_runner_scenario("overload protection rejecting QUIC connection").

t_conn_without_ctrl_stream(Config) ->
    run_scenario("wrong-stream-connect", Config, []).

t_data_stream_race_ctrl_stream(Config) ->
    run_scenario("data-stream-race-control-stream", Config, []).

t_listener_inval_settings(_Config) ->
    LPort = emqx_common_test_helpers:select_free_port(quic),
    LowLevelTunings = #{stream_recv_buffer_default => 1024},
    ?assertThrow(
        {error, {failed_to_start, _}},
        emqx_common_test_helpers:ensure_quic_listener(?FUNCTION_NAME, LPort, LowLevelTunings)
    ).

t_listener_with_lowlevel_settings(Config) ->
    LPort = emqx_common_test_helpers:select_free_port(quic),
    LowLevelTunings = #{
        max_bytes_per_key => 274877906,
        handshake_idle_timeout_ms => 2000,
        idle_timeout_ms => 20000,
        tls_server_max_send_buffer => 10240,
        stream_recv_window_default => 16384 * 2,
        stream_recv_buffer_default => 16384,
        conn_flow_control_window => 1024,
        max_stateless_operations => 16,
        initial_window_packets => 1300,
        send_idle_timeout_ms => 12000,
        initial_rtt_ms => 300,
        max_ack_delay_ms => 6000,
        disconnect_timeout_ms => 60000,
        keep_alive_interval_ms => 12000,
        peer_bidi_stream_count => 100,
        peer_unidi_stream_count => 100,
        retry_memory_limit => 640,
        load_balancing_mode => 1,
        max_operations_per_drain => 32,
        send_buffering_enabled => 1,
        pacing_enabled => 0,
        migration_enabled => 0,
        datagram_receive_enabled => 1,
        server_resumption_level => 0,
        minimum_mtu => 1250,
        maximum_mtu => 1600,
        mtu_discovery_search_complete_timeout_us => 500000000,
        mtu_discovery_missing_probe_count => 6,
        max_binding_stateless_operations => 200,
        stateless_operation_expiration_ms => 200
    },
    ?assertEqual(
        ok,
        emqx_common_test_helpers:ensure_quic_listener(?FUNCTION_NAME, LPort, LowLevelTunings)
    ),
    try
        ListenerConfig = [{port, LPort} | Config],
        QoS2Args = ["--pub-qos", "2", "--sub-qos", "2"],
        ok = run_scenario("pubsub", ListenerConfig, QoS2Args),
        run_scenario("multistream", ListenerConfig, QoS2Args)
    after
        ok = emqx_listeners:stop_listener(
            emqx_listeners:listener_id(quic, ?FUNCTION_NAME)
        )
    end.

run_zero_rtt(Config, Scenario, ExtraArgs) ->
    run_scenario(Scenario, Config, ExtraArgs).

run_source_rebind(Config) ->
    run_scenario(
        "source-rebind",
        Config,
        qos_args(Config) ++
            [
                "--local-bind-addr",
                "127.0.0.1:0",
                "--rebind-addr",
                "127.0.0.1:0"
            ]
    ).

maybe_skip_missing_runner_scenario(Reason) ->
    {skip, "FlowSDK runner scenario missing: " ++ Reason}.

check_zone_config(ConfString) ->
    Fields = [{zone, hoconsc:mk(hoconsc:ref(emqx_schema, "zone"))}],
    Schema = #{roots => Fields},
    {ok, RawConf} = hocon:binary(unicode:characters_to_binary(ConfString)),
    {_, Conf} = emqx_config:check_config(Schema, #{<<"zone">> => RawConf}),
    maps:get(zone, Conf).

start_emqx(Config, Port) ->
    emqx_cth_suite:start(
        emqx_specs(Port),
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ).

stop_emqx(Config) ->
    emqx_cth_suite:stop(?config(apps, Config)).

restart_emqx(Config) ->
    ok = stop_emqx(Config),
    start_emqx_dirty(Config).

start_emqx_dirty(Config) ->
    emqx_cth_suite:start(
        emqx_specs(?config(port, Config)),
        #{work_dir => emqx_cth_suite:work_dir(Config), work_dir_dirty => true}
    ).

emqx_specs(Port) ->
    [
        {mria, #{
            override_env => [{db_backend, mnesia}],
            before_start => fun use_mria_mnesia_backend/0
        }},
        mk_emqx_spec(Port)
    ].

use_mria_mnesia_backend() ->
    persistent_term:put({mria, db_backend}, mnesia).

mk_emqx_spec(Port) ->
    {emqx,
        "force_shutdown.enable = false"
        "\n listeners.quic.default {"
        "\n   enable = true"
        "\n   bind = " ++ integer_to_list(Port) ++
            "\n   acceptors = 16"
            "\n   idle_timeout = 15s"
            "\n   datagram_receive_enabled = true"
            "\n   migration_enabled = true"
            "\n   server_resumption_level = 2"
            "\n   ssl_options.verify = verify_none"
            "\n }"}.

set_qos(PubQos, SubQos, Config) ->
    [{pub_qos, PubQos}, {sub_qos, SubQos} | Config].

qos_args(Config) ->
    [
        "--pub-qos",
        integer_to_list(proplists:get_value(pub_qos, Config, 1)),
        "--sub-qos",
        integer_to_list(proplists:get_value(sub_qos, Config, 1))
    ].

control_stream_shutdown_args(Config) ->
    [
        "--stream-shutdown-mode",
        proplists:get_value(stream_shutdown_mode, Config, "graceful"),
        "--stream-error-code",
        "500"
    ].

run_scenario(Scenario, Config, ExtraArgs) ->
    ClientId = unique_name(Scenario),
    run_scenario_as(Scenario, Config, ClientId, ExtraArgs).

run_scenario_as(Scenario, Config, ClientId, ExtraArgs) ->
    Exe = ensure_runner(),
    Topic = "ct/quic/" ++ ClientId,
    TimeoutArgs =
        case lists:member("--timeout-ms", ExtraArgs) of
            true -> [];
            false -> ["--timeout-ms", "15000"]
        end,
    Args =
        [
            "--scenario",
            Scenario,
            "--host",
            "127.0.0.1",
            "--server-name",
            "localhost",
            "--port",
            integer_to_list(?config(port, Config)),
            "--client-id",
            ClientId,
            "--topic",
            Topic,
            "--insecure"
        ] ++ TimeoutArgs ++ ExtraArgs,
    case run_executable(Exe, Args) of
        {ok, Output} ->
            ct:pal("mqtt_quic_test ~s output:~n~s", [Scenario, Output]),
            ok;
        {error, ExitStatus, Output} ->
            ct:fail("mqtt_quic_test ~s failed with status ~p:~n~s", [
                Scenario,
                ExitStatus,
                Output
            ])
    end.

start_async_client(Config, ClientId, ExtraArgs) ->
    start_async_scenario(Config, "connect", ClientId, ExtraArgs).

start_async_scenario(Config, Scenario, ClientId, ExtraArgs) ->
    Exe = ensure_runner(),
    ReadyFile = filename:join(
        ?config(priv_dir, Config),
        unique_name("mqtt-quic-ready")
    ),
    Topic = "ct/quic/" ++ unique_name("async"),
    TimeoutArgs =
        case lists:member("--timeout-ms", ExtraArgs) of
            true -> [];
            false -> ["--timeout-ms", "15000"]
        end,
    Args =
        [
            "--scenario",
            Scenario,
            "--host",
            "127.0.0.1",
            "--server-name",
            "localhost",
            "--port",
            integer_to_list(?config(port, Config)),
            "--client-id",
            ClientId,
            "--topic",
            Topic,
            "--ready-file",
            ReadyFile,
            "--insecure"
        ] ++ TimeoutArgs ++ ExtraArgs,
    Port = open_port({spawn_executable, Exe}, [
        binary,
        exit_status,
        stderr_to_stdout,
        use_stdio,
        {args, Args}
    ]),
    #{port => Port, ready_file => ReadyFile, topic => Topic, client_id => ClientId}.

wait_async_client_ready(#{port := Port, ready_file := ReadyFile}) ->
    wait_async_client_ready(Port, ReadyFile, 150, []).

wait_async_client_ready(_Port, _ReadyFile, 0, Acc) ->
    ct:fail("mqtt_quic_test did not become ready:~n~s", [port_output(Acc)]);
wait_async_client_ready(Port, ReadyFile, Attempts, Acc) ->
    case filelib:is_regular(ReadyFile) of
        true ->
            ok;
        false ->
            receive
                {Port, {data, Data}} ->
                    wait_async_client_ready(Port, ReadyFile, Attempts, [Data | Acc]);
                {Port, {exit_status, ExitStatus}} ->
                    ct:fail("mqtt_quic_test exited before ready (~p):~n~s", [
                        ExitStatus,
                        port_output(Acc)
                    ])
            after 100 ->
                wait_async_client_ready(Port, ReadyFile, Attempts - 1, Acc)
            end
    end.

wait_async_client(#{port := Port, ready_file := ReadyFile}) ->
    try
        case collect_port(Port, []) of
            {ok, _Output} ->
                ok;
            {error, ExitStatus, Output} ->
                ct:fail("async mqtt_quic_test failed (~p):~n~s", [ExitStatus, Output])
        end
    after
        file:delete(ReadyFile)
    end.

wait_async_client_allow_failure(#{port := Port, ready_file := ReadyFile}) ->
    _ = collect_port(Port, []),
    file:delete(ReadyFile),
    ok.

stop_async_client(#{port := Port, ready_file := ReadyFile}) ->
    catch port_close(Port),
    file:delete(ReadyFile),
    ok.

port_output(Acc) ->
    unicode:characters_to_list(iolist_to_binary(lists:reverse(Acc))).

unique_name(Scenario) ->
    "ct-" ++ Scenario ++ "-" ++ integer_to_list(erlang:unique_integer([positive])).

ensure_runner() ->
    emqx_mqtt_quic_test_runner:ensure().

run_executable(Exe, Args) ->
    Port = open_port({spawn_executable, Exe}, [
        binary,
        exit_status,
        stderr_to_stdout,
        use_stdio,
        {args, Args}
    ]),
    collect_port(Port, []).

collect_port(Port, Acc) ->
    receive
        {Port, {data, Data}} ->
            collect_port(Port, [Data | Acc]);
        {Port, {exit_status, 0}} ->
            {ok, unicode:characters_to_list(iolist_to_binary(lists:reverse(Acc)))};
        {Port, {exit_status, ExitStatus}} ->
            {error, ExitStatus, unicode:characters_to_list(iolist_to_binary(lists:reverse(Acc)))}
    after 60000 ->
        port_close(Port),
        {error, timeout, unicode:characters_to_list(iolist_to_binary(lists:reverse(Acc)))}
    end.

-endif.
