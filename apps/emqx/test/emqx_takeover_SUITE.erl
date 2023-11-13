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

-module(emqx_takeover_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("emqx/include/asserts.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-define(CNT, 100).
-define(SLEEP, 10).

%%--------------------------------------------------------------------
%% Initial funcs

all() ->
    [
        {group, mqttv3},
        {group, mqttv5}
    ].

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [emqx],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    Apps = ?config(apps, Config),
    ok = emqx_cth_suite:stop(Apps),
    ok.

groups() ->
    [
        {mqttv3, [],
            emqx_common_test_helpers:all(?MODULE) --
                [
                    t_session_expire_with_delayed_willmsg,
                    t_no_takeover_with_delayed_willmsg,
                    t_takeover_before_session_expire,
                    t_takeover_before_willmsg_expire
                ]},
        {mqttv5, [], emqx_common_test_helpers:all(?MODULE)}
    ].

init_per_group(mqttv3, Config) ->
    lists:keystore(mqtt_vsn, 1, Config, {mqtt_vsn, v3});
init_per_group(mqttv5, Config) ->
    lists:keystore(mqtt_vsn, 1, Config, {mqtt_vsn, v5}).

end_per_group(_Group, _Config) ->
    ok.

%%--------------------------------------------------------------------
%% Testcases

t_takeover(Config) ->
    process_flag(trap_exit, true),
    ClientId = atom_to_binary(?FUNCTION_NAME),
    ClientOpts = [
        {proto_ver, ?config(mqtt_vsn, Config)},
        {clean_start, false}
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
    Commands =
        [{fun start_client/5, [ClientId, ClientId, ?QOS_1, ClientOpts]}] ++
            [{fun publish_msg/2, [Msg]} || Msg <- Client1Msgs] ++
            [{fun start_client/5, [ClientId, ClientId, ?QOS_1, ClientOpts]}] ++
            [{fun publish_msg/2, [Msg]} || Msg <- Client2Msgs] ++
            [{fun stop_client/1, []}],

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
    Received = [Msg || {publish, Msg} <- ?drainMailbox(?SLEEP)],
    ct:pal("middle: ~p", [Middle]),
    ct:pal("received: ~p", [[P || #{payload := P} <- Received]]),
    assert_messages_missed(AllMsgs, Received),
    assert_messages_order(AllMsgs, Received),
    meck:unload(emqx_cm),
    ok.

t_takeover_willmsg(Config) ->
    process_flag(trap_exit, true),
    ClientId = atom_to_binary(?FUNCTION_NAME),
    WillTopic = <<ClientId/binary, <<"willtopic">>/binary>>,
    Middle = ?CNT div 2,
    Client1Msgs = messages(ClientId, 0, Middle),
    Client2Msgs = messages(ClientId, Middle, ?CNT div 2),
    AllMsgs = Client1Msgs ++ Client2Msgs,
    WillOpts = [
        {proto_ver, ?config(mqtt_vsn, Config)},
        {clean_start, false},
        {will_topic, WillTopic},
        {will_payload, <<"willpayload">>},
        {will_qos, 0}
    ],
    Commands =
        %% GIVEN client connect with will message
        [{fun start_client/5, [ClientId, ClientId, ?QOS_1, WillOpts]}] ++
            [
                {fun start_client/5, [
                    <<ClientId/binary, <<"_willsub">>/binary>>, WillTopic, ?QOS_1, []
                ]}
            ] ++
            [{fun publish_msg/2, [Msg]} || Msg <- Client1Msgs] ++
            %% WHEN client reconnect with clean_start = false
            [{fun start_client/5, [ClientId, ClientId, ?QOS_1, WillOpts]}] ++
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
    Received = [Msg || {publish, Msg} <- ?drainMailbox(?SLEEP)],
    ct:pal("received: ~p", [[P || #{payload := P} <- Received]]),
    {IsWill, ReceivedNoWill} = filter_payload(Received, <<"willpayload">>),
    assert_messages_missed(AllMsgs, ReceivedNoWill),
    assert_messages_order(AllMsgs, ReceivedNoWill),
    %% THEN will message should be received
    ?assert(IsWill),
    emqtt:stop(CPidSub),
    emqtt:stop(CPid2),
    ?assertReceive({'EXIT', CPid2, normal}),
    ?assert(not is_process_alive(CPid1)),
    ok.

t_takeover_willmsg_clean_session(Config) ->
    process_flag(trap_exit, true),
    ClientId = atom_to_binary(?FUNCTION_NAME),
    WillTopic = <<ClientId/binary, <<"willtopic">>/binary>>,
    Middle = ?CNT div 2,
    Client1Msgs = messages(ClientId, 0, Middle),
    Client2Msgs = messages(ClientId, Middle, ?CNT div 2),
    AllMsgs = Client1Msgs ++ Client2Msgs,
    WillOpts = [
        {proto_ver, ?config(mqtt_vsn, Config)},
        {clean_start, false},
        {will_topic, WillTopic},
        {will_payload, <<"willpayload_1">>},
        {will_qos, 1}
    ],
    WillOptsClean = [
        {proto_ver, ?config(mqtt_vsn, Config)},
        {clean_start, true},
        {will_topic, WillTopic},
        {will_payload, <<"willpayload_2">>},
        {will_qos, 1}
    ],

    Commands =
        %% GIVEN: client connect with willmsg payload <<"willpayload_1">>
        [{fun start_client/5, [ClientId, ClientId, ?QOS_1, WillOpts]}] ++
            [
                {fun start_client/5, [
                    <<ClientId/binary, <<"_willsub">>/binary>>, WillTopic, ?QOS_1, []
                ]}
            ] ++
            [{fun publish_msg/2, [Msg]} || Msg <- Client1Msgs] ++
            %% WHEN: client connects with clean_start=true and willmsg payload <<"willpayload_2">>
            [{fun start_client/5, [ClientId, ClientId, ?QOS_1, WillOptsClean]}] ++
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
    Received = [Msg || {publish, Msg} <- ?drainMailbox(?SLEEP)],
    ct:pal("received: ~p", [[P || #{payload := P} <- Received]]),
    {IsWill1, ReceivedNoWill0} = filter_payload(Received, <<"willpayload_1">>),
    {IsWill2, ReceivedNoWill} = filter_payload(ReceivedNoWill0, <<"willpayload_2">>),
    assert_messages_missed(AllMsgs, ReceivedNoWill),
    assert_messages_order(AllMsgs, ReceivedNoWill),
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
    AllMsgs = Client1Msgs ++ Client2Msgs,
    WillOpts = [
        {proto_ver, ?config(mqtt_vsn, Config)},
        {clean_start, false},
        {will_topic, WillTopic},
        {will_payload, <<"willpayload_delay10">>},
        {will_qos, 1},
        %% mqttv5 only
        {will_props, #{'Will-Delay-Interval' => 10}}
    ],
    WillOptsClean = [
        {proto_ver, ?config(mqtt_vsn, Config)},
        {clean_start, true},
        {will_topic, WillTopic},
        {will_payload, <<"willpayload_2">>},
        {will_qos, 1}
    ],

    Commands =
        %% GIVEN: client connect with willmsg payload <<"willpayload_delay10">> and delay-interval 10s
        [{fun start_client/5, [ClientId, ClientId, ?QOS_1, WillOpts]}] ++
            [
                {fun start_client/5, [
                    <<ClientId/binary, <<"_willsub">>/binary>>, WillTopic, ?QOS_1, []
                ]}
            ] ++
            [{fun publish_msg/2, [Msg]} || Msg <- Client1Msgs] ++
            %% WHEN: client connects with clean_start=true and willmsg payload <<"willpayload_2">>
            [{fun start_client/5, [ClientId, ClientId, ?QOS_1, WillOptsClean]}] ++
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
    Received = [Msg || {publish, Msg} <- ?drainMailbox(?SLEEP)],
    ct:pal("received: ~p", [[P || #{payload := P} <- Received]]),
    {IsWill1, ReceivedNoWill0} = filter_payload(Received, <<"willpayload_delay10">>),
    {IsWill2, ReceivedNoWill} = filter_payload(ReceivedNoWill0, <<"willpayload_2">>),
    assert_messages_missed(AllMsgs, ReceivedNoWill),
    assert_messages_order(AllMsgs, ReceivedNoWill),
    %% THEN: payload <<"willpayload_delay10">> should be published without delay.
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
    WillTopic = <<ClientId/binary, <<"willtopic">>/binary>>,
    Client1Msgs = messages(ClientId, 0, 10),
    WillOpts = [
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
        [{fun start_client/5, [ClientId, ClientId, ?QOS_1, WillOpts]}] ++
            [
                {fun start_client/5, [
                    <<ClientId/binary, <<"_willsub">>/binary>>, WillTopic, ?QOS_1, []
                ]}
            ] ++
            [{fun publish_msg/2, [Msg]} || Msg <- Client1Msgs],

    FCtx = lists:foldl(
        fun({Fun, Args}, Ctx) ->
            ct:pal("COMMAND: ~p ~p", [element(2, erlang:fun_info(Fun, name)), Args]),
            apply(Fun, [Ctx | Args])
        end,
        #{},
        Commands
    ),

    Received = [Msg || {publish, Msg} <- ?drainMailbox(1000)],
    ct:pal("received: ~p", [[P || #{payload := P} <- Received]]),
    assert_messages_missed(Client1Msgs, Received),
    #{client := [CPidSub, CPid1]} = FCtx,
    %% WHEN: client disconnects abnormally AND no reconnect after 3s.
    exit(CPid1, kill),
    assert_client_exit(CPid1, ?config(mqtt_vsn, Config), killed),

    Received1 = [Msg || {publish, Msg} <- ?drainMailbox(1000)],

    {IsWill, ReceivedNoWill} = filter_payload(Received1, <<"willpayload_delay3">>),
    ?assertNot(IsWill),
    ?assertEqual([], ReceivedNoWill),
    %% THEN: for MQTT v5, payload <<"willpayload_delay3">> should be published after WILL delay.
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
    WillOpts = [
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
        [{fun start_client/5, [ClientId, ClientId, ?QOS_1, WillOpts]}] ++
            [
                {fun start_client/5, [
                    <<ClientId/binary, <<"_willsub">>/binary>>, WillTopic, ?QOS_1, []
                ]}
            ] ++
            [{fun publish_msg/2, [Msg]} || Msg <- Client1Msgs],

    FCtx = lists:foldl(
        fun({Fun, Args}, Ctx) ->
            ct:pal("COMMAND: ~p ~p", [element(2, erlang:fun_info(Fun, name)), Args]),
            apply(Fun, [Ctx | Args])
        end,
        #{},
        Commands
    ),

    Received = [Msg || {publish, Msg} <- ?drainMailbox(1000)],
    ct:pal("received: ~p", [[P || #{payload := P} <- Received]]),
    assert_messages_missed(Client1Msgs, Received),
    #{client := [CPidSub, CPid1]} = FCtx,
    %% WHEN: client disconnects abnormally AND no reconnect after 3s.
    exit(CPid1, kill),
    assert_client_exit(CPid1, ?config(mqtt_vsn, Config), killed),

    Received1 = [Msg || {publish, Msg} <- ?drainMailbox(1000)],
    {IsWill, ReceivedNoWill} = filter_payload(Received1, <<"willpayload_delay10">>),
    ?assertNot(IsWill),
    ?assertEqual([], ReceivedNoWill),
    %% THEN: for MQTT v5, payload <<"willpayload_delay3">> should be published after session expiry.
    Received2 = [Msg || {publish, Msg} <- ?drainMailbox(5000)],
    {IsWill11, ReceivedNoWill11} = filter_payload(Received2, <<"willpayload_delay10">>),
    ?assertEqual([], ReceivedNoWill11),
    ?assert(IsWill11),
    emqtt:stop(CPidSub),
    ?assert(not is_process_alive(CPid1)),
    ok.

t_takeover_before_session_expire(Config) ->
    ?config(mqtt_vsn, Config) =:= v5 orelse ct:fail("MQTTv5 Only"),
    process_flag(trap_exit, true),
    ClientId = atom_to_binary(?FUNCTION_NAME),
    WillTopic = <<ClientId/binary, <<"willtopic">>/binary>>,
    Client1Msgs = messages(ClientId, 0, 10),
    emqx_logger:set_log_level(debug),
    WillOpts = [
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
        [{fun start_client/5, [ClientId, ClientId, ?QOS_1, WillOpts]}] ++
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
            %% WHEN: client session is taken over with in 3s.
            [{fun start_client/5, [ClientId, ClientId, ?QOS_1, WillOpts]}],

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
    %% THEN: for MQTT v5, payload <<"willpayload_delay10">> should NOT be published.
    Received2 = [Msg || {publish, Msg} <- ?drainMailbox(5000)],
    {IsWill11, ReceivedNoWill11} = filter_payload(Received2, <<"willpayload_delay10">>),
    ?assertEqual([], ReceivedNoWill11),
    ?assertNot(IsWill11),
    emqtt:stop(CPidSub),
    emqtt:stop(CPid2),
    ?assert(not is_process_alive(CPid1)),
    ok.

t_takeover_before_willmsg_expire(Config) ->
    ?config(mqtt_vsn, Config) =:= v5 orelse ct:fail("MQTTv5 Only"),
    process_flag(trap_exit, true),
    ClientId = atom_to_binary(?FUNCTION_NAME),
    WillTopic = <<ClientId/binary, <<"willtopic">>/binary>>,
    Client1Msgs = messages(ClientId, 0, 10),
    emqx_logger:set_log_level(debug),
    WillOpts = [
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
        [{fun start_client/5, [ClientId, ClientId, ?QOS_1, WillOpts]}] ++
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
            [{fun start_client/5, [ClientId, ClientId, ?QOS_1, WillOpts]}],

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
    WillOpts = [
        {proto_ver, ?config(mqtt_vsn, Config)},
        {clean_start, false},
        {will_topic, WillTopic},
        {will_payload, <<"willpayload_kick">>},
        {will_qos, 1}
    ],
    Commands =
        %% GIVEN: client connect with willmsg payload <<"willpayload_kick">>
        [{fun start_client/5, [ClientId, ClientId, ?QOS_1, WillOpts]}] ++
            [
                {fun start_client/5, [
                    <<ClientId/binary, <<"_willsub">>/binary>>, WillTopic, ?QOS_1, []
                ]}
            ] ++
            [
                %% kick may fail (not found) without this delay
                {
                    fun(CTX) ->
                        timer:sleep(100),
                        CTX
                    end,
                    []
                }
            ] ++
            %% WHEN: client is kicked with kick_session
            [{fun kick_client/2, [ClientId]}],

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
    Received = [Msg || {publish, Msg} <- ?drainMailbox(?SLEEP)],
    ct:pal("received: ~p", [[P || #{payload := P} <- Received]]),
    %% THEN: payload <<"willpayload_kick">> should be published
    {IsWill1, _ReceivedNoWill} = filter_payload(Received, <<"willpayload_kick">>),
    ?assert(IsWill1),
    emqtt:stop(CPidSub),
    ?assert(not is_process_alive(CPid1)),
    ok.

%% t_takover_in_cluster(_) ->
%%     todo.

%%--------------------------------------------------------------------
%% Commands
start_client(Ctx, ClientId, Topic, Qos, Opts) ->
    {ok, CPid} = emqtt:start_link([{clientid, ClientId} | Opts]),
    _ = erlang:spawn_link(fun() ->
        {ok, _} = emqtt:connect(CPid),
        ct:pal("CLIENT: connected ~p", [CPid]),
        {ok, _, [Qos]} = emqtt:subscribe(CPid, Topic, Qos)
    end),
    Ctx#{client => [CPid | maps:get(client, Ctx, [])]}.

kick_client(Ctx, ClientId) ->
    ok = emqx_cm:kick_session(ClientId),
    Ctx.

publish_msg(Ctx, Msg) ->
    ok = timer:sleep(rand:uniform(?SLEEP)),
    case emqx:publish(Msg) of
        [] -> publish_msg(Ctx, Msg);
        [_ | _] -> Ctx
    end.

stop_client(Ctx = #{client := [CPid | _]}) ->
    ok = timer:sleep(?SLEEP),
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

assert_messages_order([], []) ->
    ok;
assert_messages_order([Msg | Expected], Received) ->
    %% Account for duplicate messages:
    case lists:splitwith(fun(#{payload := P}) -> emqx_message:payload(Msg) == P end, Received) of
        {[], [#{payload := Mismatch} | _]} ->
            ct:fail("Message order is not correct, expected: ~p, received: ~p", [
                emqx_message:payload(Msg), Mismatch
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
    ?assertReceive({'EXIT', Pid, {disconnected, ?RC_SESSION_TAKEN_OVER, _}});
assert_client_exit(Pid, v3, takenover) ->
    ?assertReceive({'EXIT', Pid, {shutdown, tcp_closed}});
assert_client_exit(Pid, v3, kicked) ->
    ?assertReceive({'EXIT', Pid, _});
assert_client_exit(Pid, v5, kicked) ->
    ?assertReceive({'EXIT', Pid, {disconnected, ?RC_ADMINISTRATIVE_ACTION, _}});
assert_client_exit(Pid, _, killed) ->
    ?assertReceive({'EXIT', Pid, killed}).
