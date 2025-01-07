%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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
    Apps = emqx_cth_suite:start(
        [
            {emqx,
                "durable_sessions = {\n"
                "  enable = true\n"
                "  force_persistence = true\n"
                "  heartbeat_interval = 100ms\n"
                "  renew_streams_interval = 100ms\n"
                "  idle_poll_interval = 1s\n"
                "  session_gc_interval = 2s\n"
                "}\n"}
        ],
        #{work_dir => emqx_cth_suite:work_dir(Group, Config)}
    ),
    [
        {apps, Apps},
        {persistence_enabled, true}
        | Config
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
    Group =:= persistence_disabled;
    Group =:= persistence_enabled
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
    Commands =
        lists:flatten([
            {fun start_client/5, [ClientId, ClientId, ?QOS_1, ClientOpts]},
            {fun maybe_wait_subscriptions/1, []},
            [{fun publish_msg/2, [Msg]} || Msg <- Client1Msgs],
            {fun start_client/5, [ClientId, ClientId, ?QOS_1, ClientOpts]},
            [{fun publish_msg/2, [Msg]} || Msg <- Client2Msgs],
            {fun stop_the_last_client/1, []}
        ]),

    Sleep =
        case ?config(persistence_enabled, Config) of
            true -> 3000;
            false -> ?SLEEP
        end,
    FCtx = lists:foldl(
        fun({Fun, Args}, Ctx) ->
            ct:pal("COMMAND: ~p ~p", [element(2, erlang:fun_info(Fun, name)), Args]),
            apply(Fun, [Ctx | Args])
        end,
        #{persistence_enabled => ?config(persistence_enabled, Config), sleep => Sleep},
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
    WillTopic = <<ClientId/binary, <<"_willtopic">>/binary>>,
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
    Commands =
        lists:flatten([
            %% GIVEN client connect with will message
            {fun start_client/5, [ClientId, ClientId, ?QOS_1, ClientOpts]},
            {fun maybe_wait_subscriptions/1, []},
            {fun start_client/5, [
                <<ClientId/binary, <<"_willsub">>/binary>>, WillTopic, ?QOS_1, []
            ]},
            [{fun publish_msg/2, [Msg]} || Msg <- Client1Msgs],
            %% WHEN client reconnect with clean_start = false
            {fun start_client/5, [ClientId, ClientId, ?QOS_1, ClientOpts]},
            [{fun publish_msg/2, [Msg]} || Msg <- Client2Msgs],
            {fun stop_the_last_client/1, []}
        ]),
    Sleep =
        case ?config(persistence_enabled, Config) of
            true -> 2_000;
            false -> ?SLEEP
        end,

    FCtx = lists:foldl(
        fun({Fun, Args}, Ctx) ->
            ct:pal("COMMAND: ~p ~p", [element(2, erlang:fun_info(Fun, name)), Args]),
            apply(Fun, [Ctx | Args])
        end,
        #{persistence_enabled => ?config(persistence_enabled, Config), sleep => Sleep},
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
    WillTopic = <<ClientId/binary, <<"willtopic">>/binary>>,
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
        [{fun start_client/5, [ClientId, ClientId, ?QOS_1, ClientOpts]}] ++
            [
                {fun start_client/5, [
                    <<ClientId/binary, <<"_willsub">>/binary>>, WillTopic, ?QOS_1, []
                ]}
            ] ++
            [{fun publish_msg/2, [Msg]} || Msg <- Client1Msgs] ++
            %% WHEN: client connects with clean_start=true and willmsg payload <<"willpayload_2">>
            [{fun start_client/5, [ClientId, ClientId, ?QOS_1, ClientOptsClean]}] ++
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
    WillTopic = <<ClientId/binary, <<"willtopic">>/binary>>,
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
        [{fun start_client/5, [ClientId, ClientId, ?QOS_1, ClientOpts]}] ++
            [
                {fun start_client/5, [
                    <<ClientId/binary, <<"_willsub">>/binary>>, WillTopic, ?QOS_1, []
                ]}
            ] ++
            [{fun publish_msg/2, [Msg]} || Msg <- Client1Msgs] ++
            %% WHEN: client connects with clean_start=true and willmsg payload <<"willpayload_2">>
            [{fun start_client/5, [ClientId, ClientId, ?QOS_1, ClientOptsClean]}] ++
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
    WillTopic = <<ClientId/binary, "willtopic">>,
    Client1Msgs = messages(ClientId, 0, 10),
    ClientOpts = [
        {proto_ver, ?config(mqtt_vsn, Config)},
        {clean_start, false},
        {will_topic, WillTopic},
        {will_payload, <<"willpayload_delay3">>},
        {will_qos, 1},
        %secs
        {will_props, #{'Will-Delay-Interval' => 3}},
        % secs
        {properties, #{'Session-Expiry-Interval' => 10}}
    ],
    Commands =
        %% GIVEN: client connect with willmsg payload <<"willpayload_delay3">> and delay-interval 3s
        lists:flatten(
            [
                {fun start_client/5, [ClientId, ClientId, ?QOS_1, ClientOpts]},
                {fun maybe_wait_subscriptions/1, []},
                {fun start_client/5, [<<ClientId/binary, "_willsub">>, WillTopic, ?QOS_1, []]},
                [{fun publish_msg/2, [Msg]} || Msg <- Client1Msgs]
            ]
        ),

    FCtx = lists:foldl(
        fun({Fun, Args}, Ctx) ->
            ct:pal("COMMAND: ~p ~p", [element(2, erlang:fun_info(Fun, name)), Args]),
            apply(Fun, [Ctx | Args])
        end,
        #{persistence_enabled => ?config(persistence_enabled, Config)},
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
    WillTopic = <<ClientId/binary, <<"willtopic">>/binary>>,
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
            {fun start_client/5, [ClientId, ClientId, ?QOS_1, ClientOpts]},
            {fun start_client/5, [
                <<ClientId/binary, <<"_willsub">>/binary>>, WillTopic, ?QOS_1, []
            ]},
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
    SessionSleep =
        case ?config(persistence_enabled, Config) of
            true ->
                %% Session GC uses a larger, safer cutoff time.
                10_000;
            false ->
                5_000
        end,
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
    WillTopic = <<ClientId/binary, <<"willtopic">>/binary>>,
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
        [{fun start_client/5, [ClientId, ClientId, ?QOS_1, ClientOpts]}] ++
            [
                {fun start_client/5, [
                    <<ClientId/binary, <<"_willsub">>/binary>>, WillTopic, ?QOS_1, []
                ]}
            ] ++
            [{fun publish_msg/2, [Msg]} || Msg <- Client1Msgs] ++
            [
                %% avoid two clients race for session takeover
                {
                    fun(CTX) ->
                        timer:sleep(1000),
                        CTX
                    end,
                    []
                }
            ] ++
            %% WHEN: client session is taken over within 3s.
            [{fun start_client/5, [ClientId, ClientId, ?QOS_1, ClientOpts]}],

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

    Received = [Msg || {publish, Msg} <- ?drainMailbox(1000)],
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
    WillTopic = <<ClientId/binary, <<"willtopic">>/binary>>,
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
            {fun start_client/5, [ClientId, ClientId, ?QOS_1, ClientOpts]},
            {fun start_client/5, [
                <<ClientId/binary, <<"_willsub">>/binary>>, WillTopic, ?QOS_1, []
            ]},
            [{fun publish_msg/2, [Msg]} || Msg <- Client1Msgs],
            %% avoid two clients race for session takeover
            {
                fun(CTX) ->
                    timer:sleep(100),
                    CTX
                end,
                []
            },
            %% WHEN: client session is taken over within 3s.
            {fun start_client/5, [ClientId, ClientId, ?QOS_1, ClientOpts]}
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

    Received = [Msg || {publish, Msg} <- ?drainMailbox(1000)],
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
            {fun start_client/5, [ClientId, ClientId, ?QOS_1, ClientOpts]},
            {fun maybe_wait_subscriptions/1, []},
            {fun start_client/5, [
                <<ClientId/binary, "_willsub">>, WillTopic, ?QOS_1, []
            ]},
            [{fun publish_msg/2, [Msg]} || Msg <- Client1Msgs],
            %% avoid two clients race for session takeover
            {
                fun(CTX) ->
                    timer:sleep(100),
                    CTX
                end,
                []
            },
            %% GIVEN: client reconnect with willmsg payload <<"willpayload_delay10">>
            %%        and delay-interval 10s > session expiry 3s.
            {fun start_client/5, [ClientId, ClientId, ?QOS_1, ClientOpts]},
            {fun maybe_wait_subscriptions/1, []}
        ]),

    FCtx = lists:foldl(
        fun({Fun, Args}, Ctx) ->
            ct:pal("COMMAND: ~p ~p", [element(2, erlang:fun_info(Fun, name)), Args]),
            apply(Fun, [Ctx | Args])
        end,
        #{persistence_enabled => ?config(persistence_enabled, Config)},
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
    Commands =
        lists:flatten([
            %% GIVEN: client connect with willmsg payload <<"willpayload_delay10">>
            %%        and will-delay-interval 10s >  session expiry 3s.
            {fun start_client/5, [ClientId, ClientId, ?QOS_1, ClientOpts]},
            {fun start_client/5, [
                <<ClientId/binary, "_willsub">>, WillTopic, ?QOS_1, []
            ]},
            %% avoid two clients race for session takeover
            {
                fun(CTX) ->
                    timer:sleep(100),
                    CTX
                end,
                []
            },
            [{fun publish_msg/2, [Msg]} || Msg <- Client1Msgs],
            {fun start_client/5, [ClientId, ClientId, ?QOS_1, ClientOpts]}
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
    Received2 = [Msg || {publish, Msg} <- ?drainMailbox(1000)],
    Received = Received1 ++ Received2,
    ct:pal("received: ~p", [[P || #{payload := P} <- Received]]),
    {IsWill, ReceivedNoWill} = filter_payload(Received, <<"willpayload_delay10">>),
    %% THEN: willmsg is not published before session expiry
    ?assertNot(IsWill),
    ?assertNotEqual([], ReceivedNoWill),
    SessionSleep =
        case ?config(persistence_enabled, Config) of
            true ->
                %% Session GC uses a larger, safer cutoff time (GC interval x 3)
                10_000;
            false ->
                3_000
        end,
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
    WillTopic = <<ClientId/binary, <<"willtopic">>/binary>>,
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
        [{fun start_client/5, [ClientId, ClientId, ?QOS_1, ClientOpts]}] ++
            [
                {fun start_client/5, [
                    <<ClientId/binary, <<"_willsub">>/binary>>, WillTopic, ?QOS_1, []
                ]}
            ] ++
            [{fun publish_msg/2, [Msg]} || Msg <- Client1Msgs] ++
            [
                %% avoid two clients race for session takeover
                {
                    fun(CTX) ->
                        timer:sleep(100),
                        CTX
                    end,
                    []
                }
            ] ++
            %% GIVEN: client *reconnect* with willmsg payload <<"willpayload_delay2">>
            %%        and will-delay-interval 0s, session expiry 3s.
            [{fun start_client/5, [ClientId, ClientId, ?QOS_1, ClientOpts2]}],

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
    Commands =
        %% GIVEN: client connect with willmsg payload <<"willpayload_delay10">>
        %%        and will-delay-interval 3s < session expiry 10s.
        [{fun start_client/5, [ClientId, ClientId, ?QOS_1, ClientOpts]}] ++
            [
                {fun start_client/5, [
                    <<ClientId/binary, <<"_willsub">>/binary>>, WillTopic, ?QOS_1, []
                ]}
            ] ++
            [{fun publish_msg/2, [Msg]} || Msg <- Client1Msgs] ++
            [
                %% avoid two clients race for session takeover
                {
                    fun(CTX) ->
                        timer:sleep(100),
                        CTX
                    end,
                    []
                }
            ] ++
            %% WHEN: another client takeover the session with in 3s.
            [{fun start_client/5, [ClientId, ClientId, ?QOS_1, ClientOpts]}],

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

    Received = [Msg || {publish, Msg} <- ?drainMailbox(1000)],
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
    ClientId = atom_to_binary(?FUNCTION_NAME),
    WillTopic = <<ClientId/binary, <<"willtopic">>/binary>>,
    ClientOpts = [
        {proto_ver, ?config(mqtt_vsn, Config)},
        {clean_start, false},
        {will_topic, WillTopic},
        {will_payload, <<"willpayload_kick">>},
        {will_qos, 1}
    ],
    Commands =
        lists:flatten([
            %% GIVEN: client connect with willmsg payload <<"willpayload_kick">>
            {fun start_client/5, [ClientId, ClientId, ?QOS_1, ClientOpts]},
            {fun start_client/5, [
                <<ClientId/binary, <<"_willsub">>/binary>>, WillTopic, ?QOS_1, []
            ]},
            {fun wait_for_chan_reg/2, [ClientId]},
            %% WHEN: client is kicked with kick_session
            {fun kick_client/2, [ClientId]},
            {fun wait_for_chan_dereg/2, [ClientId]},
            {fun wait_for_pub_client_down/1, []}
        ]),
    FCtx = lists:foldl(
        fun({Fun, Args}, Ctx) ->
            ct:pal("COMMAND: ~p ~p", [element(2, erlang:fun_info(Fun, name)), Args]),
            apply(Fun, [Ctx | Args])
        end,
        #{},
        Commands
    ),
    #{client := [CPidSub, CPid1]} = FCtx,
    assert_client_exit(CPid1, ?config(mqtt_vsn, Config), kicked),
    Received = [Msg || {publish, Msg} <- ?drainMailbox(timer:seconds(1))],
    ct:pal("received: ~p", [[P || #{payload := P} <- Received]]),
    %% THEN: payload <<"willpayload_kick">> should be published
    {IsWill, _ReceivedNoWill} = filter_payload(Received, <<"willpayload_kick">>),
    ?assert(IsWill),
    emqtt:stop(CPidSub),
    ?assert(not is_process_alive(CPid1)),
    ok.

wait_for_chan_reg(CTX, ClientId) ->
    ?retry(
        3_000,
        100,
        true = is_map(emqx_cm:get_chan_info(ClientId))
    ),
    CTX.

wait_for_chan_dereg(CTX, ClientId) ->
    ?retry(
        3_000,
        100,
        undefined = emqx_cm:get_chan_info(ClientId)
    ),
    CTX.

wait_for_pub_client_down(#{client := [_SubClient, PubClient]} = CTX) ->
    ?retry(
        3_000,
        100,
        false = is_process_alive(PubClient)
    ),
    CTX.

%% t_takover_in_cluster(_) ->
%%     todo.

%%--------------------------------------------------------------------
%% Commands
start_client(Ctx, ClientId, Topic, Qos, Opts) ->
    {ok, CPid} = emqtt:start_link([{clientid, ClientId} | Opts]),
    _ = erlang:spawn_link(fun() ->
        {ok, _} = emqtt:connect(CPid),
        {ok, _, [Qos]} = emqtt:subscribe(CPid, Topic, Qos)
    end),
    Ctx#{client => [CPid | maps:get(client, Ctx, [])]}.

maybe_wait_subscriptions(Ctx = #{persistence_enabled := true, client := CPids}) ->
    ok = do_wait_subscription(CPids),
    Ctx;
maybe_wait_subscriptions(Ctx) ->
    Ctx.

do_wait_subscription([]) ->
    ok;
do_wait_subscription([CPid | Rest]) ->
    try emqtt:subscriptions(CPid) of
        [] ->
            ok = timer:sleep(rand:uniform(?SLEEP)),
            do_wait_subscription([CPid | Rest]);
        [_ | _] ->
            do_wait_subscription(Rest)
    catch
        exit:{noproc, _} ->
            ok
    end.

kick_client(Ctx, ClientId) ->
    ok = emqx_cm:kick_session(ClientId),
    Ctx.

publish_msg(Ctx, Msg) ->
    ok = timer:sleep(rand:uniform(?SLEEP)),
    case emqx:publish(Msg#message{timestamp = emqx_message:timestamp_now()}) of
        [] -> publish_msg(Ctx, Msg);
        [_ | _] -> Ctx
    end.

stop_the_last_client(Ctx = #{client := [CPid | _], sleep := Sleep}) ->
    ok = timer:sleep(Sleep),
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
    Filtered = [
        Msg
     || #{payload := P} = Msg <- List,
        P =/= Payload
    ],
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
