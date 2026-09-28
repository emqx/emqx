%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_extsub_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("emqx/include/asserts.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").

-include("../src/emqx_extsub_internal.hrl").

-define(BUFFER_SIZE, 100).

all() ->
    [
        {group, durable_sessions},
        {group, memory_sessions}
    ].

groups() ->
    All = emqx_common_test_helpers:all(?MODULE),
    [
        {durable_sessions, [], All},
        {memory_sessions, [], All}
    ].

init_per_group(durable_sessions, Config) ->
    Apps =
        emqx_cth_suite:start(
            [
                emqx_durable_timer,
                {emqx, #{config => #{<<"durable_sessions">> => #{<<"enable">> => true}}}},
                emqx_extsub
            ],
            #{work_dir => emqx_cth_suite:work_dir(Config)}
        ),
    emqx_persistent_message:wait_readiness(15_000),
    [{suite_apps, Apps} | Config];
init_per_group(memory_sessions, Config) ->
    Apps =
        emqx_cth_suite:start(
            [emqx, emqx_extsub],
            #{work_dir => emqx_cth_suite:work_dir(Config)}
        ),
    [{suite_apps, Apps} | Config].

end_per_group(_, Config) ->
    emqx_cth_suite:stop(?config(suite_apps, Config)).

init_per_testcase(_CaseName, Config) ->
    ok = snabbkaffe:start_trace(),
    ok = emqx_extsub_handler_registry:register(emqx_extsub_test_st_handler, #{
        buffer_size => ?BUFFER_SIZE, multi_topic => false
    }),
    ok = emqx_extsub_handler_registry:register(emqx_extsub_test_mt_handler, #{
        buffer_size => ?BUFFER_SIZE, multi_topic => true
    }),
    Config.

end_per_testcase(_CaseName, _Config) ->
    ok = emqx_extsub_handler_registry:unregister(emqx_extsub_test_st_handler),
    ok = emqx_extsub_handler_registry:unregister(emqx_extsub_test_mt_handler),
    ok = snabbkaffe:stop().

%%--------------------------------------------------------------------
%% Test cases
%%--------------------------------------------------------------------

-doc "New and resumed sessions cache only the protocol version and ACK capability.".
t_channel_info_created_and_resumed(Config) ->
    Group = proplists:get_value(name, ?config(tc_group_properties, Config)),
    CanReceiveAcks = Group =:= memory_sessions,
    lists:foreach(
        fun({ProtoVer, Version}) ->
            ClientId = atom_to_binary(ProtoVer),
            Opts = [
                {clientid, ClientId},
                {proto_ver, ProtoVer},
                {clean_start, false},
                {properties, #{'Session-Expiry-Interval' => 1000}}
            ],
            {ok, C0} = emqtt:start_link(Opts),
            {ok, _} = emqtt:connect(C0),
            ?assertEqual(0, proplists:get_value(session_present, emqtt:info(C0))),
            assert_channel_info(ClientId, Version, CanReceiveAcks),
            ok = emqtt:disconnect(C0),
            {ok, C1} = emqtt:start_link(Opts),
            {ok, _} = emqtt:connect(C1),
            ?assertEqual(1, proplists:get_value(session_present, emqtt:info(C1))),
            assert_channel_info(ClientId, Version, CanReceiveAcks),
            ok = emqtt:disconnect(C1)
        end,
        [{v4, ?MQTT_PROTO_V4}, {v5, ?MQTT_PROTO_V5}]
    ).

-doc "Delivery requires channel metadata initialized by a session lifecycle hook.".
t_channel_info_required(_Config) ->
    ?assertEqual(undefined, erlang:get(extsub_channel_info)),
    Msg = emqx_message:make(<<"test">>, ?QOS_1, <<"t">>, <<"payload">>),
    ?assertError({badmatch, undefined}, emqx_extsub:on_message_delivered(#{}, Msg)),
    ?assertEqual(undefined, erlang:get(extsub_channel_info)).

-doc "Handler messages keep their destination when sent from another process.".
t_message_target(_Config) ->
    Ref = make_ref(),
    Target = emqx_extsub_handler:message_target(Ref),
    {Sender, Monitor} = spawn_monitor(fun() ->
        ok = emqx_extsub_handler:send(Target, immediate),
        ?assert(is_reference(emqx_extsub_handler:send_after(0, Target, delayed)))
    end),
    receive
        #info_to_extsub{handler_ref = Ref, info = immediate} -> ok
    after 1000 ->
        ct:fail(immediate_message_not_received)
    end,
    receive
        #info_to_extsub{handler_ref = Ref, info = delayed} -> ok
    after 1000 ->
        ct:fail(delayed_message_not_received)
    end,
    receive
        {'DOWN', Monitor, process, Sender, normal} -> ok
    after 1000 ->
        ct:fail(sender_did_not_finish)
    end.

-doc "Active external subscriptions retain data instead of callback closures.".
t_handler_state_has_no_closures(_Config) ->
    ClientId = <<"handler-state">>,
    C = emqx_extsub_test_utils:emqtt_connect([
        {clientid, ClientId},
        {clean_start, false},
        {properties, #{'Session-Expiry-Interval' => 1000}}
    ]),
    try
        lists:foreach(
            fun(TopicFilter) ->
                ok = emqx_extsub_test_utils:emqtt_subscribe(C, TopicFilter)
            end,
            [<<"extsub_st_test/state/1/1/0">>, <<"extsub_mt_test/state/1/1/0">>]
        ),
        {ok, Msgs} = emqx_extsub_test_utils:emqtt_drain(2, 1000),
        ?assertEqual(2, length(Msgs)),
        [ChanPid] = emqx_cm:lookup_channels(ClientId),
        emqx_extsub_test_utils:assert_no_retained_functions(ChanPid)
    after
        emqtt:disconnect(C)
    end.

assert_channel_info(ClientId, Version, CanReceiveAcks) ->
    ?retry(20, 100, begin
        [ChanPid] = emqx_cm:lookup_channels(ClientId),
        {dictionary, Dict} = process_info(ChanPid, dictionary),
        ?assertEqual(
            #{proto_ver => Version, can_receive_acks => CanReceiveAcks},
            proplists:get_value(extsub_channel_info, Dict)
        )
    end).

%% We emulate different kinds of message flow into the external subscription handler.
%% We deliver message by 1, in batches smaller and larger than the extsub buffer size.
%% We expect the handler to deliver the messages into the channel correctly.
t_smoke(Config) ->
    Samples = [
        {10, 1, 10},
        {10, 1, 0},
        {10, ?BUFFER_SIZE div 2, 10},
        {10, ?BUFFER_SIZE div 2, 0},
        {10, ?BUFFER_SIZE * 2, 10},
        {10, ?BUFFER_SIZE * 2, 0}
    ],
    lists:foreach(
        fun({NBatches, BatchSize, IntervalMs}) ->
            test_smoke(Config, NBatches, BatchSize, IntervalMs)
        end,
        Samples
    ).

test_smoke(_Config, NBatches, BatchSize, IntervalMs) ->
    NBatchesBin = integer_to_binary(NBatches),
    BatchSizeBin = integer_to_binary(BatchSize),
    IntervalMsBin = integer_to_binary(IntervalMs),
    TopicTail = <<NBatchesBin/binary, "/", BatchSizeBin/binary, "/", IntervalMsBin/binary>>,
    TopicFilters = [
        <<"extsub_st_test/a/", TopicTail/binary>>,
        <<"extsub_st_test/a/", TopicTail/binary>>,
        <<"extsub_st_test/b/", TopicTail/binary>>,
        <<"extsub_mt_test/a/", TopicTail/binary>>,
        <<"extsub_mt_test/a/", TopicTail/binary>>,
        <<"extsub_mt_test/b/", TopicTail/binary>>
    ],
    ?tp(warning, test_smoke, #{
        n_batches => NBatches,
        batch_size => BatchSize,
        interval_ms => IntervalMs,
        topic_filter => TopicFilters
    }),
    CSub = emqx_extsub_test_utils:emqtt_connect([
        {clientid, <<"csub-", TopicTail/binary>>},
        {properties, #{'Session-Expiry-Interval' => 10000}},
        {clean_start, false}
    ]),
    ok = lists:foreach(
        fun(TopicFilter) ->
            ok = emqx_extsub_test_utils:emqtt_subscribe(CSub, TopicFilter)
        end,
        TopicFilters
    ),
    ExpectedMessages = NBatches * BatchSize * length(lists:usort(TopicFilters)),
    {ok, Msgs} = emqx_extsub_test_utils:emqtt_drain(0, 5000),
    CountsByTopic = lists:foldl(
        fun(#{topic := Topic}, Acc) ->
            case Acc of
                #{Topic := Cnt} ->
                    Acc#{Topic => Cnt + 1};
                _ ->
                    Acc#{Topic => 1}
            end
        end,
        #{},
        Msgs
    ),
    Payloads = [Payload || #{payload := Payload} <- Msgs],
    ?tp(warning, test_smoke_drain, #{
        counts_by_topic => CountsByTopic,
        expected_by_topic => NBatches * BatchSize,
        payloads => lists:sort(Payloads)
    }),
    ?assertEqual(ExpectedMessages, length(Msgs)),
    ok = emqtt:disconnect(CSub).

%% Test that session's QoS0 fallback for `delivery.completed` does
%% not interfere with extsubs's fallback.
%%
%% Previously, there was an issue where extsubs's fallback counted a message
%% as delivered in `message.delivered` and then once more in `delivery.completed`.
t_qos0_not_double_completed(_Config) ->
    ClientId = <<"qos0-not-double-completed">>,
    TopicFilter = <<"extsub_st_test/qos0/1/5/0">>,
    CSub = emqx_extsub_test_utils:emqtt_connect([
        {clientid, ClientId},
        {clean_start, true}
    ]),
    [ChanPid] = emqx_cm:lookup_channels(ClientId),
    ok = emqx_extsub_test_utils:emqtt_subscribe(CSub, TopicFilter),
    {ok, Msgs} = emqx_extsub_test_utils:emqtt_drain(5, 5000),
    ?assertEqual(5, length(Msgs)),
    ?retry(
        100,
        20,
        begin
            {ok, #{
                registry := #{
                    buffer := #{
                        message_buffer_size := 0,
                        delivering := Delivering
                    }
                },
                unacked_count := 0
            }} = emqx_extsub:inspect(ChanPid),
            ?assert(lists:all(fun(N) -> N =:= 0 end, maps:values(Delivering)))
        end
    ),
    ok = emqtt:disconnect(CSub).
