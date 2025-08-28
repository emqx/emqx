%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_takeover_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("emqx/include/asserts.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("emqx_utils/include/emqx_message.hrl").

-define(CNT, 100).
-define(SLEEP, 10).

%%--------------------------------------------------------------------
%% Initial funcs

all() ->
    [
        {group, persistence_disabled},
        {group, persistence_enabled}
    ].

groups() ->
    MQTTGroups = [{group, G} || G <- [mqttv3, mqttv5]],
    [
        {persistence_enabled, MQTTGroups},
        {persistence_disabled, MQTTGroups},
        {mqttv3, [], emqx_common_test_helpers:all(?MODULE) -- tc_v5_only()},
        {mqttv5, [], emqx_common_test_helpers:all(?MODULE)}
    ].

tc_v5_only() ->
    [
        t_session_expire_with_delayed_willmsg,
        t_no_takeover_with_delayed_willmsg,
        t_takeover_before_session_expire,
        t_takeover_before_willmsg_expire,
        t_takeover_before_session_expire_willdelay0,
        t_takeover_session_then_normal_disconnect,
        t_takeover_session_then_abnormal_disconnect,
        t_takeover_session_then_abnormal_disconnect_2
    ].

init_per_suite(Config) ->
    emqx_common_test_helpers:clear_screen(),
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(persistence_enabled = Group, Config) ->
    DurableSessionsOpts = #{
        <<"enable">> => true,
        <<"force_persistence">> => true,
        <<"checkpoint_interval">> => <<"100ms">>
    },
    Opts = #{
        durable_sessions_opts => DurableSessionsOpts,
        start_emqx_conf => false,
        work_dir => emqx_cth_suite:work_dir(Group, Config)
    },
    [
        {persistence_enabled, true}
        | emqx_common_test_helpers:start_apps_ds(Config, _ExtraApps = [], Opts)
    ];
init_per_group(persistence_disabled = Group, Config) ->
    Apps = emqx_cth_suite:start(
        [{emqx, "durable_sessions.enable = false"}],
        #{work_dir => emqx_cth_suite:work_dir(Group, Config)}
    ),
    [
        {apps, Apps},
        {persistence_enabled, false}
        | Config
    ];
init_per_group(mqttv3, Config) ->
    lists:keystore(mqtt_vsn, 1, Config, {mqtt_vsn, v3});
init_per_group(mqttv5, Config) ->
    lists:keystore(mqtt_vsn, 1, Config, {mqtt_vsn, v5}).

end_per_group(Group, Config) when
    Group =:= persistence_enabled
->
    emqx_common_test_helpers:stop_apps_ds(Config),
    ok;
end_per_group(Group, Config) when
    Group =:= persistence_disabled
->
    Apps = ?config(apps, Config),
    ok = emqx_cth_suite:stop(Apps),
    ok;
end_per_group(_Group, _Config) ->
    ok.

%%--------------------------------------------------------------------
%% Testcases

t_takeover(Config) ->
    process_flag(trap_exit, true),
    ClientId = make_client_id(?FUNCTION_NAME, Config),
    MqttVer = ?config(mqtt_vsn, Config),
    PersistenceEnabled = ?config(persistence_enabled, Config),
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

    Sleep = maps:get(PersistenceEnabled, #{true => 2000, false => ?SLEEP}),
    Commands =
        lists:flatten([
            {fun start_client_async_subscribe/5, [ClientId, ClientId, ?QOS_1, ClientOpts]},
            [{fun wait_subscription/1, []} || PersistenceEnabled],
            [{fun publish_msg/2, [Msg]} || Msg <- Client1Msgs],
            {fun start_client_async_subscribe/5, [ClientId, ClientId, ?QOS_1, ClientOpts]},
            [{fun publish_msg/2, [Msg]} || Msg <- Client2Msgs],
            {fun just_wait/2, [Sleep]},
            {fun stop_the_last_client/1, []}
        ]),

    FCtx = lists:foldl(
        fun({Fun, Args}, Ctx) ->
            ct:pal("COMMAND: ~p ~p", [element(2, erlang:fun_info(Fun, name)), Args]),
            apply(Fun, [Ctx | Args])
        end,
        #{},
        Commands
    ),

    #{client := [CPid2, CPid1]} = FCtx,

    assert_client_exit(CPid1, ?config(mqtt_vsn, Config), takenover),
    ?assertReceive({'EXIT', CPid2, normal}),
    Received = [Msg || {publish, Msg} <- ?drainMailbox(Sleep)],
    ct:pal("middle: ~p", [Middle]),
    ct:pal("received: ~p", [[P || #{payload := P} <- Received]]),
    assert_messages_missed(AllMsgs, Received),
    assert_messages_order(AllMsgs, Received),
    meck:unload(emqx_cm),
    ok.

t_takeover_willmsg(Config) ->
    process_flag(trap_exit, true),
    ClientId = make_client_id(?FUNCTION_NAME, Config),
    ClientIdSub = <<ClientId/binary, "_willsub">>,
    WillTopic = <<ClientId/binary, "_willtopic">>,
    PersistenceEnabled = ?config(persistence_enabled, Config),
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
    Sleep = maps:get(PersistenceEnabled, #{true => 2000, false => ?SLEEP}),
    Commands =
        lists:flatten([
            %% GIVEN client connect with will message
            {fun start_client_async_subscribe/5, [ClientId, ClientId, ?QOS_1, ClientOpts]},
            [{fun wait_subscription/1, []} || PersistenceEnabled],
            {fun start_client_async_subscribe/5, [ClientIdSub, WillTopic, ?QOS_1, []]},
            [{fun publish_msg/2, [Msg]} || Msg <- Client1Msgs],
            %% WHEN client reconnect with clean_start = false
            {fun start_client_async_subscribe/5, [ClientId, ClientId, ?QOS_1, ClientOpts]},
            [{fun publish_msg/2, [Msg]} || Msg <- Client2Msgs],
            {fun just_wait/2, [Sleep]},
            {fun stop_the_last_client/1, []}
        ]),

    FCtx = lists:foldl(
        fun({Fun, Args}, Ctx) ->
            ct:pal("COMMAND: ~p ~p", [element(2, erlang:fun_info(Fun, name)), Args]),
            apply(Fun, [Ctx | Args])
        end,
        #{},
        Commands
    ),

    #{client := [CPid2, CPidSub, CPid1]} = FCtx,
    assert_client_exit(CPid1, ?config(mqtt_vsn, Config), takenover),
    ?assertReceive({'EXIT', CPid2, normal}),
    Received = [Msg || {publish, Msg} <- ?drainMailbox(Sleep)],
    ct:pal("received: ~p", [[P || #{payload := P} <- Received]]),
    {IsWill, ReceivedNoWill} = filter_payload(Received, <<"willpayload">>),
    assert_messages_missed(AllMsgs, ReceivedNoWill),
    assert_messages_order(AllMsgs, ReceivedNoWill),
    %% THEN will message should be received
    ?assert(IsWill),
    emqtt:stop(CPidSub),
    ok.

t_takeover_willmsg_clean_session(Config) ->
    process_flag(trap_exit, true),
    ClientId = atom_to_binary(?FUNCTION_NAME),
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

    Commands =
        %% GIVEN: client connect with willmsg payload <<"willpayload_1">>
        [
            {fun start_client_async_subscribe/5, [ClientId, ClientId, ?QOS_1, ClientOpts]},
            {fun start_client_subscribe/5, [ClientIdSub, WillTopic, ?QOS_1, []]}
        ] ++
            [{fun publish_msg/2, [Msg]} || Msg <- Client1Msgs] ++
            %% WHEN: client connects with clean_start=true and willmsg payload <<"willpayload_2">>
            [{fun start_client_async_subscribe/5, [ClientId, ClientId, ?QOS_1, ClientOptsClean]}] ++
            [{fun publish_msg/2, [Msg]} || Msg <- Client2Msgs],

    FCtx = lists:foldl(
        fun({Fun, Args}, Ctx) ->
            ct:pal("COMMAND: ~p ~p", [element(2, erlang:fun_info(Fun, name)), Args]),
            apply(Fun, [Ctx | Args])
        end,
        #{},
        Commands
    ),
    #{client := [CPid2, CPidSub, CPid1]} = FCtx,
    assert_client_exit(CPid1, ?config(mqtt_vsn, Config), takenover),
    Sleep =
        case ?config(persistence_enabled, Config) of
            true -> 3_000;
            false -> ?SLEEP
        end,
    Received = [Msg || {publish, Msg} <- ?drainMailbox(Sleep)],
    ct:pal("received: ~p", [[P || #{payload := P} <- Received]]),
    {IsWill1, ReceivedNoWill0} = filter_payload(Received, <<"willpayload_1">>),
    {IsWill2, _ReceivedNoWill} = filter_payload(ReceivedNoWill0, <<"willpayload_2">>),
    %% THEN: payload <<"willpayload_1">> should be published instead of <<"willpayload_2">>
    ?assert(IsWill1),
    ?assertNot(IsWill2),
    emqtt:stop(CPid2),
    emqtt:stop(CPidSub),
    ?assert(not is_process_alive(CPid1)),
    ok.

t_takeover_clean_session_with_delayed_willmsg(Config) ->
    process_flag(trap_exit, true),
    ClientId = atom_to_binary(?FUNCTION_NAME),
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

    Commands =
        %% GIVEN: client connect with willmsg payload <<"willpayload_delay10">> and delay-interval 10s
        [
            {fun start_client_async_subscribe/5, [ClientId, ClientId, ?QOS_1, ClientOpts]},
            {fun start_client_async_subscribe/5, [ClientIdSub, WillTopic, ?QOS_1, []]}
        ] ++
            [{fun publish_msg/2, [Msg]} || Msg <- Client1Msgs] ++
            %% WHEN: client connects with clean_start=true and willmsg payload <<"willpayload_2">>
            [{fun start_client_async_subscribe/5, [ClientId, ClientId, ?QOS_1, ClientOptsClean]}] ++
            [{fun publish_msg/2, [Msg]} || Msg <- Client2Msgs],

    FCtx = lists:foldl(
        fun({Fun, Args}, Ctx) ->
            ct:pal("COMMAND: ~p ~p", [element(2, erlang:fun_info(Fun, name)), Args]),
            apply(Fun, [Ctx | Args])
        end,
        #{},
        Commands
    ),
    #{client := [CPid2, CPidSub, CPid1]} = FCtx,
    assert_client_exit(CPid1, ?config(mqtt_vsn, Config), takenover),
    Sleep =
        case ?config(persistence_enabled, Config) of
            true -> 2_000;
            false -> ?SLEEP
        end,
    Received = [Msg || {publish, Msg} <- ?drainMailbox(Sleep)],
    ct:pal("received: ~p", [[P || #{payload := P} <- Received]]),
    {IsWill1, ReceivedNoWill0} = filter_payload(Received, <<"willpayload_delay10">>),
    {IsWill2, _ReceivedNoWill} = filter_payload(ReceivedNoWill0, <<"willpayload_2">>),
    %% THEN: payload <<"willpayload_delay10">> should be published without delay
    ?assert(IsWill1),
    ?assertNot(IsWill2),
    emqtt:stop(CPid2),
    emqtt:stop(CPidSub),
    ?assert(not is_process_alive(CPid1)),
    ok.

t_no_takeover_with_delayed_willmsg(Config) ->
    ?config(mqtt_vsn, Config) =:= v5 orelse ct:fail("MQTTv5 Only"),
    process_flag(trap_exit, true),
    ClientId = atom_to_binary(?FUNCTION_NAME),
    ClientIdSub = <<ClientId/binary, "_willsub">>,
    WillTopic = <<ClientId/binary, "willtopic">>,
    Client1Msgs = messages(ClientId, 0, 10),
    ClientOpts = [
        {proto_ver, ?config(mqtt_vsn, Config)},
        {clean_start, false},
        {will_topic, WillTopic},
        {will_payload, <<"willpayload_delay3">>},
        {will_qos, 1},
        % secs
        {will_props, #{'Will-Delay-Interval' => 3}},
        % secs
        {properties, #{'Session-Expiry-Interval' => 10}}
    ],
    Commands =
        %% GIVEN: client connects with willmsg payload <<"willpayload_delay3">> and delay-interval 3s
        lists:flatten(
            [
                {fun start_client_subscribe/5, [ClientId, ClientId, ?QOS_1, ClientOpts]},
                {fun start_client_subscribe/5, [ClientIdSub, WillTopic, ?QOS_1, []]},
                [{fun publish_msg/2, [Msg]} || Msg <- Client1Msgs]
            ]
        ),

    FCtx = lists:foldl(
        fun({Fun, Args}, Ctx) ->
            ct:pal("COMMAND: ~p ~p", [element(2, erlang:fun_info(Fun, name)), Args]),
            apply(Fun, [Ctx | Args])
        end,
        #{},
        Commands
    ),

    Received = [Msg || {publish, Msg} <- ?drainMailbox(2000)],
    ct:pal("received: ~p", [[P || #{payload := P} <- Received]]),
    assert_messages_missed(Client1Msgs, Received),
    {IsWill0, ReceivedNoWill0} = filter_payload(Received, <<"willpayload_delay3">>),
    ?assertNot(IsWill0),
    ?assertNotEqual([], ReceivedNoWill0),
    #{client := [CPidSub, CPid1]} = FCtx,
    %% WHEN: client disconnects abnormally AND no reconnect after 3s.
    exit(CPid1, kill),
    assert_client_exit(CPid1, ?config(mqtt_vsn, Config), killed),

    Received1 = [Msg || {publish, Msg} <- ?drainMailbox(1000)],
    ct:pal("received: ~p", [[P || #{payload := P} <- Received1]]),
    {IsWill1, ReceivedNoWill1} = filter_payload(Received1, <<"willpayload_delay3">>),
    ?assertNot(IsWill1),
    ?assertEqual([], ReceivedNoWill1),
    %% THEN: for MQTT v5, payload <<"willpayload_delay3">> should be published after WILL delay (3 secs).
    Received2 = [Msg || {publish, Msg} <- ?drainMailbox(5000)],
    {IsWill11, ReceivedNoWill11} = filter_payload(Received2, <<"willpayload_delay3">>),
    ?assertEqual([], ReceivedNoWill11),
    ?assert(IsWill11),
    emqtt:stop(CPidSub),
    ?assert(not is_process_alive(CPid1)),
    ok.

t_session_expire_with_delayed_willmsg(Config) ->
    ?config(mqtt_vsn, Config) =:= v5 orelse ct:fail("MQTTv5 Only"),
    process_flag(trap_exit, true),
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
    Commands =
        lists:flatten([
            %% GIVEN: client connect with willmsg payload <<"willpayload_delay10">>
            %%        and delay-interval 10s > session expiry 3s.
            {fun start_client_async_subscribe/5, [ClientId, ClientId, ?QOS_1, ClientOpts]},
            {fun start_client_subscribe/5, [ClientIdSub, WillTopic, ?QOS_1, []]},
            [{fun publish_msg/2, [Msg]} || Msg <- Client1Msgs]
        ]),

    FCtx = lists:foldl(
        fun({Fun, Args}, Ctx) ->
            ct:pal("COMMAND: ~p ~p", [element(2, erlang:fun_info(Fun, name)), Args]),
            apply(Fun, [Ctx | Args])
        end,
        #{},
        Commands
    ),

    Received = [Msg || {publish, Msg} <- ?drainMailbox(2000)],
    ct:pal("received: ~p", [[P || #{payload := P} <- Received]]),
    {IsWill, ReceivedNoWill} = filter_payload(Received, <<"willpayload_delay10">>),
    ?assertNot(IsWill),
    ?assertNotEqual([], ReceivedNoWill),
    assert_messages_missed(Client1Msgs, Received),
    #{client := [CPidSub, CPid1]} = FCtx,
    %% WHEN: client disconnects abnormally AND no reconnect after 3s.
    exit(CPid1, kill),
    assert_client_exit(CPid1, ?config(mqtt_vsn, Config), killed),

    Received1 = [Msg || {publish, Msg} <- ?drainMailbox(1000)],
    {IsWill1, ReceivedNoWill1} = filter_payload(Received1, <<"willpayload_delay10">>),
    ?assertNot(IsWill1),
    ?assertEqual([], ReceivedNoWill1),
    %% THEN: for MQTT v5, payload <<"willpayload_delay3">> should be published after session expiry.
    SessionSleep = 5_000,
    Received2 = [Msg || {publish, Msg} <- ?drainMailbox(SessionSleep)],
    {IsWill12, ReceivedNoWill2} = filter_payload(Received2, <<"willpayload_delay10">>),
    ?assertEqual([], ReceivedNoWill2),
    ?assert(IsWill12),
    emqtt:stop(CPidSub),
    ?assert(not is_process_alive(CPid1)),
    ok.

%% @TODO 'Server-Keep-Alive'
%% t_no_takeover_keepalive_fired(Config) ->
%%     ok.

t_takeover_before_session_expire_willdelay0(Config) ->
    ?config(mqtt_vsn, Config) =:= v5 orelse ct:fail("MQTTv5 Only"),
    process_flag(trap_exit, true),
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
        {will_props, #{'Will-Delay-Interval' => 0}},
        {properties, #{'Session-Expiry-Interval' => 3}}
    ],
    Commands =
        %% GIVEN: client connect with willmsg payload <<"willpayload_delay10">>
        %%        and delay-interval 0s session expiry 3s.
        [
            {fun start_client_subscribe/5, [ClientId, ClientId, ?QOS_1, ClientOpts]},
            {fun start_client_subscribe/5, [ClientIdSub, WillTopic, ?QOS_1, []]}
        ] ++
            [{fun publish_msg/2, [Msg]} || Msg <- Client1Msgs] ++
            %% WHEN: client session is taken over within 3s.
            [{fun start_client_async_subscribe/5, [ClientId, ClientId, ?QOS_1, ClientOpts]}],

    FCtx = lists:foldl(
        fun({Fun, Args}, Ctx) ->
            ct:pal("COMMAND: ~p ~p", [element(2, erlang:fun_info(Fun, name)), Args]),
            apply(Fun, [Ctx | Args])
        end,
        #{},
        Commands
    ),
    #{client := [CPid2, CPidSub, CPid1]} = FCtx,
    assert_client_exit(CPid1, ?config(mqtt_vsn, Config), takenover),

    Sleep =
        case ?config(persistence_enabled, Config) of
            true -> 3_000;
            false -> ?SLEEP
        end,
    Received = [Msg || {publish, Msg} <- ?drainMailbox(Sleep)],
    ct:pal("received: ~p", [[P || #{payload := P} <- Received]]),
    {IsWill, _ReceivedNoWill} = filter_payload(Received, <<"willpayload_delay10">>),
    %% THEN: willmsg is published
    ?assert(IsWill),
    emqtt:stop(CPidSub),
    emqtt:stop(CPid2),
    ?assert(not is_process_alive(CPid1)),
    ok.

t_takeover_before_session_expire(Config) ->
    ?config(mqtt_vsn, Config) =:= v5 orelse ct:fail("MQTTv5 Only"),
    process_flag(trap_exit, true),
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
    Commands =
        %% GIVEN: client connect with willmsg payload <<"willpayload_delay10">>
        %%        and delay-interval 10s > session expiry 3s.
        lists:flatten([
            {fun start_client_subscribe/5, [ClientId, ClientId, ?QOS_1, ClientOpts]},
            {fun start_client_subscribe/5, [ClientIdSub, WillTopic, ?QOS_1, []]},
            [{fun publish_msg/2, [Msg]} || Msg <- Client1Msgs],
            %% WHEN: client session is taken over within 3s.
            {fun start_client_async_subscribe/5, [ClientId, ClientId, ?QOS_1, ClientOpts]}
        ]),

    FCtx = lists:foldl(
        fun({Fun, Args}, Ctx) ->
            ct:pal("COMMAND: ~p ~p", [element(2, erlang:fun_info(Fun, name)), Args]),
            apply(Fun, [Ctx | Args])
        end,
        #{},
        Commands
    ),
    #{client := [CPid2, CPidSub, CPid1]} = FCtx,
    ct:pal("FCtx: ~p", [FCtx]),
    assert_client_exit(CPid1, ?config(mqtt_vsn, Config), takenover),

    Sleep =
        case ?config(persistence_enabled, Config) of
            true -> 3_000;
            false -> ?SLEEP
        end,
    Received = [Msg || {publish, Msg} <- ?drainMailbox(Sleep)],
    ct:pal("received: ~p", [[P || #{payload := P} <- Received]]),
    {IsWill, ReceivedNoWill} = filter_payload(Received, <<"willpayload_delay10">>),
    %% THEN: No Willmsg is published
    ?assertNot(IsWill),
    ?assertNotEqual([], ReceivedNoWill),
    emqtt:stop(CPidSub),
    emqtt:stop(CPid2),
    ?assert(not is_process_alive(CPid1)),
    ok.

t_takeover_session_then_normal_disconnect(Config) ->
    ?config(mqtt_vsn, Config) =:= v5 orelse ct:fail("MQTTv5 Only"),
    process_flag(trap_exit, true),
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
    Commands =
        lists:flatten([
            {fun start_client_subscribe/5, [ClientId, ClientId, ?QOS_1, ClientOpts]},
            {fun start_client_subscribe/5, [ClientIdSub, WillTopic, ?QOS_1, []]},
            [{fun publish_msg/2, [Msg]} || Msg <- Client1Msgs],
            %% GIVEN: client reconnect with willmsg payload <<"willpayload_delay10">>
            %%        and delay-interval 10s > session expiry 3s.
            {fun start_client_subscribe/5, [ClientId, ClientId, ?QOS_1, ClientOpts]}
        ]),

    FCtx = lists:foldl(
        fun({Fun, Args}, Ctx) ->
            ct:pal("COMMAND: ~p ~p", [element(2, erlang:fun_info(Fun, name)), Args]),
            apply(Fun, [Ctx | Args])
        end,
        #{},
        Commands
    ),
    #{client := [CPid2, CPidSub, CPid1]} = FCtx,
    assert_client_exit(CPid1, ?config(mqtt_vsn, Config), takenover),
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
    ?assert(not is_process_alive(CPid2)),
    ok.

t_takeover_session_then_abnormal_disconnect(Config) ->
    ?config(mqtt_vsn, Config) =:= v5 orelse ct:fail("MQTTv5 Only"),
    process_flag(trap_exit, true),
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
    Commands =
        lists:flatten([
            %% GIVEN: client connect with willmsg payload <<"willpayload_delay10">>
            %%        and will-delay-interval 10s >  session expiry 3s.
            {fun start_client_subscribe/5, [ClientId, ClientId, ?QOS_1, ClientOpts]},
            {fun start_client_subscribe/5, [WillSubClientId, WillTopic, ?QOS_1, []]},
            [{fun publish_msg/2, [Msg]} || Msg <- Client1Msgs],
            {fun start_client_async_subscribe/5, [ClientId, ClientId, ?QOS_1, ClientOpts]}
        ]),

    FCtx = lists:foldl(
        fun({Fun, Args}, Ctx) ->
            ct:pal("COMMAND: ~p ~p", [element(2, erlang:fun_info(Fun, name)), Args]),
            apply(Fun, [Ctx | Args])
        end,
        #{},
        Commands
    ),
    #{client := [CPid2, CPidSub, CPid1]} = FCtx,
    assert_client_exit(CPid1, ?config(mqtt_vsn, Config), takenover),
    Received1 = [Msg || {publish, Msg} <- ?drainMailbox(1000)],
    %% WHEN: client disconnect abnormally
    emqtt:disconnect(CPid2, ?RC_DISCONNECT_WITH_WILL_MESSAGE),
    Received2 = [Msg || {publish, Msg} <- ?drainMailbox(2000)],
    Received = Received1 ++ Received2,
    ct:pal("received: ~p", [[P || #{payload := P} <- Received]]),
    {IsWill, ReceivedNoWill} = filter_payload(Received, <<"willpayload_delay10">>),
    %% THEN: willmsg is not published before session expiry
    ?assertNot(IsWill),
    ?assertNotEqual([], ReceivedNoWill),
    SessionSleep = 3_000,
    Received3 = [Msg || {publish, Msg} <- ?drainMailbox(SessionSleep)],
    {IsWill1, ReceivedNoWill1} = filter_payload(Received3, <<"willpayload_delay10">>),
    %% AND THEN: willmsg is published after session expiry
    ?assert(IsWill1),
    ?assertEqual([], ReceivedNoWill1),
    emqtt:stop(CPidSub),
    ?assert(not is_process_alive(CPid1)),
    ?assert(not is_process_alive(CPid2)),
    ok.

t_takeover_session_then_abnormal_disconnect_2(Config) ->
    ?config(mqtt_vsn, Config) =:= v5 orelse ct:fail("MQTTv5 Only"),
    process_flag(trap_exit, true),
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
    Commands =
        [
            {fun start_client_subscribe/5, [ClientId, ClientId, ?QOS_1, ClientOpts]},
            {fun start_client_async_subscribe/5, [ClientIdSub, WillTopic, ?QOS_1, []]}
        ] ++
            [{fun publish_msg/2, [Msg]} || Msg <- Client1Msgs] ++
            %% GIVEN: client *reconnect* with willmsg payload <<"willpayload_delay2">>
            %%        and will-delay-interval 0s, session expiry 3s.
            [{fun start_client_async_subscribe/5, [ClientId, ClientId, ?QOS_1, ClientOpts2]}],

    FCtx = lists:foldl(
        fun({Fun, Args}, Ctx) ->
            ct:pal("COMMAND: ~p ~p", [element(2, erlang:fun_info(Fun, name)), Args]),
            apply(Fun, [Ctx | Args])
        end,
        #{},
        Commands
    ),
    #{client := [CPid2, CPidSub, CPid1]} = FCtx,
    assert_client_exit(CPid1, ?config(mqtt_vsn, Config), takenover),
    %% WHEN: client disconnect abnormally
    emqtt:disconnect(CPid2, ?RC_DISCONNECT_WITH_WILL_MESSAGE),
    Received = [Msg || {publish, Msg} <- ?drainMailbox(2000)],
    ct:pal("received: ~p", [[P || #{payload := P} <- Received]]),
    {IsWill, ReceivedNoWill} = filter_payload(Received, <<"willpayload_delay1">>),
    %% THEN: willmsg1 of old conn is not published because will-delay-interval > 0
    ?assertNot(IsWill),
    ?assertNotEqual([], ReceivedNoWill),
    %% THEN: willmsg1 is published because will-delay-interval is 0
    {IsWill2, _} = filter_payload(Received, <<"willpayload_delay2">>),
    ?assert(IsWill2),
    emqtt:stop(CPidSub),
    ?assert(not is_process_alive(CPid1)),
    ?assert(not is_process_alive(CPid2)),
    ok.

t_takeover_before_willmsg_expire(Config) ->
    ?config(mqtt_vsn, Config) =:= v5 orelse ct:fail("MQTTv5 Only"),
    process_flag(trap_exit, true),
    ClientId = atom_to_binary(?FUNCTION_NAME),
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
    Commands =
        lists:flatten([
            %% GIVEN: client connect with willmsg payload <<"willpayload_delay10">>
            %%        and will-delay-interval 3s < session expiry 10s.
            {fun start_client_subscribe/5, [ClientId, ClientId, ?QOS_1, ClientOpts]},
            {fun start_client_subscribe/5, [WillSubClientId, WillTopic, ?QOS_1, []]},
            [{fun publish_msg/2, [Msg]} || Msg <- Client1Msgs],
            %% WHEN: another client takeover the session with in 3s.
            {fun start_client_async_subscribe/5, [ClientId, ClientId, ?QOS_1, ClientOpts]}
        ]),

    FCtx = lists:foldl(
        fun({Fun, Args}, Ctx) ->
            ct:pal("COMMAND: ~p ~p", [element(2, erlang:fun_info(Fun, name)), Args]),
            apply(Fun, [Ctx | Args])
        end,
        #{},
        Commands
    ),
    #{client := [CPid2, CPidSub, CPid1]} = FCtx,
    ct:pal("FCtx: ~p", [FCtx]),
    assert_client_exit(CPid1, ?config(mqtt_vsn, Config), takenover),

    Sleep =
        case ?config(persistence_enabled, Config) of
            true -> 3_000;
            false -> ?SLEEP
        end,
    Received = [Msg || {publish, Msg} <- ?drainMailbox(Sleep)],
    ct:pal("received: ~p", [[P || #{payload := P} <- Received]]),
    assert_messages_missed(Client1Msgs, Received),

    Received1 = [Msg || {publish, Msg} <- ?drainMailbox(1000)],
    {IsWill, ReceivedNoWill} = filter_payload(Received1, <<"willpayload_delay10">>),
    ?assertNot(IsWill),
    ?assertEqual([], ReceivedNoWill),
    %% THEN: for MQTT v5, payload <<"willpayload_delay10">> should NOT be published after 3s.
    Received2 = [Msg || {publish, Msg} <- ?drainMailbox(5000)],
    {IsWill11, ReceivedNoWill11} = filter_payload(Received2, <<"willpayload_delay10">>),
    ?assertEqual([], ReceivedNoWill11),
    ?assertNot(IsWill11),
    emqtt:stop(CPidSub),
    emqtt:stop(CPid2),
    ?assert(not is_process_alive(CPid1)),
    ok.

t_kick_session(Config) ->
    process_flag(trap_exit, true),
    ProtoVer = ?config(mqtt_vsn, Config),
    ClientId = atom_to_binary(?FUNCTION_NAME),
    ClientIdSub = <<ClientId/binary, "_willsub">>,
    WillTopic = <<ClientId/binary, "willtopic">>,
    ClientOpts = [
        {proto_ver, ProtoVer},
        {clean_start, false},
        {will_topic, WillTopic},
        {will_payload, <<"willpayload_kick">>},
        {will_qos, 1}
    ],
    %% GIVEN: client connect with willmsg payload <<"willpayload_kick">>
    CPid = start_connect_client(ClientId, ClientOpts),
    CPidSub = start_connect_client(ClientIdSub, ClientOpts),
    {ok, _, [?QOS_1]} = emqtt:subscribe(CPidSub, WillTopic, ?QOS_1),
    %% WHEN: client is kicked with kick_session
    ok = emqx_cm:kick_session(ClientId),
    assert_client_exit(CPid, ProtoVer, kicked),
    %% THEN: payload <<"willpayload_kick">> should be published
    ?assertReceive({publish, #{payload := <<"willpayload_kick">>}}, timer:seconds(1)),
    %% Cleanup
    emqtt:stop(CPidSub).

t_ongoing_takeover(Config) ->
    process_flag(trap_exit, true),
    MqttVer = ?config(mqtt_vsn, Config),
    ClientId = make_client_id(?FUNCTION_NAME, Config),
    Topic = atom_to_binary(?FUNCTION_NAME),
    InitCtx = #{},
    QoS = 0,
    ClientOpts = [
        {proto_ver, MqttVer},
        {clean_start, false}
        | [{properties, #{'Session-Expiry-Interval' => 6000}} || v5 == MqttVer]
    ],

    true = emqx:get_config([broker, enable_session_registry]),

    _ = start_client_async_subscribe(InitCtx, ClientId, Topic, QoS, ClientOpts),
    ?retry(
        3_000,
        100,
        true = is_map(emqx_cm:get_chan_info(ClientId))
    ),

    [ServerPid1] = emqx_cm:lookup_channels(ClientId),
    sys:suspend(ServerPid1),

    %% GIVEN: A ongoing session takeover stuck
    _ = start_client_async_subscribe(InitCtx, ClientId, Topic, QoS, ClientOpts),

    timer:sleep(500),

    %% WHEN: Yet another client connects to takeover
    _ = start_client_async_subscribe(InitCtx, ClientId, Topic, QoS, ClientOpts),

    {ok, CPid} = emqtt:start_link([{clientid, ClientId} | ClientOpts]),

    %% THEN: This client get connack with or RC_SERVER_BUSY
    case MqttVer of
        v3 ->
            ?assertMatch({error, {server_unavailable, _}}, emqtt:connect(CPid));
        v5 ->
            ?assertMatch({error, {server_busy, _}}, emqtt:connect(CPid))
    end,
    sys:resume(ServerPid1),
    receive
        {'EXIT', CPid, {shutdown, _}} ->
            ok
    after 1000 -> ct:fail("No EXIT")
    end,
    ok.

%% t_takover_in_cluster(_) ->
%%     todo.

%%--------------------------------------------------------------------
%% Commands

start_client_async_subscribe(Ctx, ClientId, Topic, Qos, Opts) ->
    {ok, CPid} = emqtt:start_link([{clientid, ClientId} | Opts]),
    _ = erlang:spawn_link(fun() ->
        {ok, _} = emqtt:connect(CPid),
        {ok, _, [Qos]} = emqtt:subscribe(CPid, Topic, Qos)
    end),
    Ctx#{client => [CPid | maps:get(client, Ctx, [])]}.

start_client_subscribe(Ctx, ClientId, Topic, Qos, Opts) ->
    {ok, CPid} = emqtt:start_link([{clientid, ClientId} | Opts]),
    {ok, _} = emqtt:connect(CPid),
    {ok, _, [Qos]} = emqtt:subscribe(CPid, Topic, Qos),
    Ctx#{client => [CPid | maps:get(client, Ctx, [])]}.

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

start_connect_client(ClientId, Opts) ->
    {ok, CPid} = emqtt:start_link([{clientid, ClientId} | Opts]),
    unlink(CPid),
    case emqtt:connect(CPid) of
        {ok, _} ->
            link(CPid),
            CPid;
        {error, {server_busy, _}} ->
            timer:sleep(10),
            ct:pal("server busy, clientid=~s retry connect after delay", [ClientId]),
            start_connect_client(ClientId, Opts);
        {error, Reason} ->
            error(Reason)
    end.

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
assert_client_exit(Pid, v5, takenover) ->
    %% @ref: MQTT 5.0 spec [MQTT-3.1.4-3]
    ?assertReceive({'EXIT', Pid, {shutdown, {disconnected, ?RC_SESSION_TAKEN_OVER, _}}});
assert_client_exit(Pid, v3, takenover) ->
    ?assertReceive(
        {'EXIT', Pid, {shutdown, Reason}} when
            Reason =:= tcp_closed orelse
                Reason =:= closed,
        1_000,
        #{pid => Pid}
    );
assert_client_exit(Pid, v3, kicked) ->
    ?assertReceive({'EXIT', Pid, _}, 1_000, #{pid => Pid});
assert_client_exit(Pid, v5, kicked) ->
    ?assertReceive({'EXIT', Pid, {shutdown, {disconnected, ?RC_ADMINISTRATIVE_ACTION, _}}});
assert_client_exit(Pid, _, killed) ->
    ?assertReceive({'EXIT', Pid, killed}).

make_client_id(Case, Config) ->
    Vsn = atom_to_list(?config(mqtt_vsn, Config)),
    Persist =
        case ?config(persistence_enabled, Config) of
            true ->
                "-persistent-";
            false ->
                "-not-persistent-"
        end,
    iolist_to_binary([atom_to_binary(Case), Persist ++ Vsn]).

sleep(_PersistenceEnabled = true) -> 1_000;
sleep(_) -> ?SLEEP.
