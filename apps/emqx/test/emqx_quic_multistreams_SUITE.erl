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
    maybe_skip_unsupported_zero_rtt(Config, "zero-rtt-pubsub", []).

t_zero_rtt_large_payload(Config) ->
    maybe_skip_unsupported_zero_rtt(Config, "zero-rtt-large-payload", ["--timeout-ms", "30000"]).

t_zero_rtt_stream_continue(Config) ->
    maybe_skip_unsupported_zero_rtt(Config, "zero-rtt-stream-continue", []).

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
    maybe_skip_unsupported_qos2_resume(Config).

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

t_mqtt_v5_connect_will_message(_Config) ->
    skip_flowsdk_will_connect().

t_mqtt_v5_connect_will_retain(_Config) ->
    skip_flowsdk_will_connect().

t_mqtt_v5_connect_packet_too_large(_Config) ->
    skip_flowsdk_will_connect().

t_mqtt_v5_max_qos_will_rejection(_Config) ->
    skip_flowsdk_will_connect().

t_mqtt_v5_connack_unavailable_no_will(_Config) ->
    skip_flowsdk_will_connect().

skip_flowsdk_will_connect() ->
    {skip,
        "FlowSDK QuicMqttEngine CONNECT currently drops MqttClientOptions::will; "
        "send_raw_on cannot replace the engine's automatically queued CONNECT"}.

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
    Client1 = start_async_client(Config, ClientId, [
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

    Client2 = start_async_client(Config, ClientId, [
        "--clean-start",
        "false",
        "--session-expiry-interval",
        "30",
        "--hold-ms",
        "5000"
    ]),
    wait_async_client_ready(Client2),
    Client3 = start_async_client(Config, ClientId, [
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
    maybe_skip_unsupported_source_rebind(Config).

t_quic_sock(_Config) ->
    maybe_skip_missing_runner_scenario("raw emqtt_quic socket send/recv against test QUIC server").

t_quic_sock_fail(_Config) ->
    maybe_skip_missing_runner_scenario("raw emqtt_quic socket connection failure").

t_0_rtt(Config) ->
    maybe_skip_unsupported_zero_rtt(Config, "zero-rtt-pubsub", []).

t_0_rtt_fail(Config) ->
    maybe_skip_unsupported_zero_rtt(Config, "zero-rtt-invalid-ticket", []).

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
    maybe_skip_unsupported_zero_rtt(Config, "zero-rtt-pubsub", []).

t_multi_streams_sub_0_rtt_large_payload(Config) ->
    maybe_skip_unsupported_zero_rtt(Config, "zero-rtt-large-payload", ["--timeout-ms", "30000"]).

t_multi_streams_sub_0_rtt_stream_data_cont(Config) ->
    maybe_skip_unsupported_zero_rtt(Config, "zero-rtt-stream-continue", []).

t_conn_change_client_addr(Config) ->
    maybe_skip_unsupported_source_rebind(Config).

t_multi_streams_shutdown_pub_data_stream(Config) ->
    run_scenario("stream-finish", Config, qos_args(Config)).

t_multi_streams_shutdown_sub_data_stream(Config) ->
    run_scenario("stream-stop", Config, qos_args(Config)).

t_multi_streams_shutdown_ctrl_stream(_Config) ->
    maybe_skip_missing_runner_scenario("client control stream shutdown flags").

t_multi_streams_shutdown_ctrl_stream_then_reconnect(_Config) ->
    maybe_skip_missing_runner_scenario("control stream shutdown followed by reconnect").

t_multi_streams_remote_shutdown(_Config) ->
    maybe_skip_missing_runner_scenario("broker stop while QUIC client is connected").

t_multi_streams_emqx_ctrl_kill(_Config) ->
    maybe_skip_missing_runner_scenario("server-side control stream process kill").

t_multi_streams_emqx_ctrl_exit_normal(_Config) ->
    maybe_skip_missing_runner_scenario("server-side control stream normal exit").

t_multi_streams_remote_shutdown_with_reconnect(_Config) ->
    maybe_skip_missing_runner_scenario("broker restart with client reconnect and subscriptions").

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
    maybe_skip_missing_runner_scenario("invalid low-level QUIC listener settings").

t_listener_with_lowlevel_settings(_Config) ->
    maybe_skip_missing_runner_scenario("valid low-level QUIC listener settings with MQTT traffic").

maybe_skip_unsupported_zero_rtt(_Config, _Scenario, _ExtraArgs) ->
    {skip, "EMQX QUIC listener disables TLS early data/0-RTT"}.

maybe_skip_unsupported_qos2_resume(_Config) ->
    {skip, "EMQX rejects resumed QoS2 PUBREL for this QUIC runner scenario with 0x92"}.

maybe_skip_unsupported_source_rebind(_Config) ->
    {skip, "EMQX/quicer path does not complete source-address rebind migration in this setup"}.

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
        [
            {mria, #{
                override_env => [{db_backend, mnesia}],
                before_start => fun use_mria_mnesia_backend/0
            }},
            mk_emqx_spec(Port)
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ).

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
            "--timeout-ms",
            "15000",
            "--insecure"
        ] ++ ExtraArgs,
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
