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

-doc "Delivery to a channel without channel metadata does not raise.".
t_missing_channel_info(_Config) ->
    ?assertEqual(undefined, erlang:get(extsub_channel_info)),
    Msg = emqx_message:make(<<"test">>, ?QOS_1, <<"t">>, <<"payload">>),
    ?assertEqual({ok, Msg}, emqx_extsub:on_message_delivered(#{}, Msg)),
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

-doc "A handler monitor sends a tagged down message, and demonitor/2 removes it from the mailbox.".
t_monitor_message(_Config) ->
    Ref = make_ref(),
    Target = emqx_extsub_handler:message_target(Ref),
    %% A down message carries the handler reference in its tag
    Pid1 = spawn(fun() ->
        receive
            stop -> ok
        end
    end),
    MRef1 = emqx_extsub_handler:monitor(Target, Pid1),
    exit(Pid1, kill),
    receive
        {#extsub_down{handler_ref = Ref}, MRef1, process, Pid1, killed} -> ok
    after 1000 ->
        ct:fail(down_message_not_received)
    end,
    %% demonitor/2 removes a down message that is already in the mailbox
    Pid2 = spawn(fun() ->
        receive
            stop -> ok
        end
    end),
    MRef2 = emqx_extsub_handler:monitor(Target, Pid2),
    exit(Pid2, kill),
    ?retry(20, 50, ?assert(has_message_with_ref(MRef2))),
    ok = emqx_extsub_handler:demonitor(Target, MRef2),
    ?assertNot(has_message_with_ref(MRef2)).

-doc "Only the process that owns the message target can create or remove a handler monitor.".
t_monitor_owner(_Config) ->
    Target = emqx_extsub_handler:message_target(make_ref()),
    Pid = spawn(fun() ->
        receive
            stop -> ok
        end
    end),
    MRef = emqx_extsub_handler:monitor(Target, Pid),
    {Caller, CallerMRef} = spawn_monitor(fun() ->
        exit(
            {result, [
                catch emqx_extsub_handler:monitor(Target, Pid),
                catch emqx_extsub_handler:demonitor(Target, MRef)
            ]}
        )
    end),
    receive
        {'DOWN', CallerMRef, process, Caller, {result, Results}} ->
            ?assertMatch([{'EXIT', {function_clause, _}}, {'EXIT', {function_clause, _}}], Results)
    after 1000 ->
        ct:fail(caller_did_not_finish)
    end,
    ok = emqx_extsub_handler:demonitor(Target, MRef),
    Pid ! stop.

-doc "A channel routes the down message of a handler monitor to that handler as a 'DOWN' message.".
t_monitor_down(_Config) ->
    Pid = start_monitored_process(?FUNCTION_NAME),
    C = emqx_extsub_test_utils:emqtt_connect([{clientid, <<"monitor-down">>}]),
    {ok, {ok, #{mref := MRef}}} = ?wait_async_action(
        emqx_extsub_test_utils:emqtt_subscribe(C, monitor_topic(<<"demonitor">>, ?FUNCTION_NAME)),
        #{?snk_kind := extsub_test_handler_monitor, pid := Pid},
        5000
    ),
    {true, {ok, _}} = ?wait_async_action(
        exit(Pid, kill),
        #{?snk_kind := extsub_test_handler_down, mref := MRef, pid := Pid, reason := killed},
        5000
    ),
    ok = emqtt:disconnect(C).

-doc "A handler that calls demonitor/2 in handle_terminate/1 leaves no monitor and gets no down message.".
t_monitor_demonitor_on_terminate(_Config) ->
    Pid = start_monitored_process(?FUNCTION_NAME),
    ClientId = <<"monitor-demonitor">>,
    Topic = monitor_topic(<<"demonitor">>, ?FUNCTION_NAME),
    C = emqx_extsub_test_utils:emqtt_connect([{clientid, ClientId}]),
    [ChanPid] = emqx_cm:lookup_channels(ClientId),
    {ok, {ok, _}} = ?wait_async_action(
        emqx_extsub_test_utils:emqtt_subscribe(C, Topic),
        #{?snk_kind := extsub_test_handler_monitor, pid := Pid},
        5000
    ),
    ?assert(lists:member(ChanPid, monitored_by(Pid))),
    %% Unsubscribe terminates the handler, which removes the monitor
    {ok, _, _} = emqtt:unsubscribe(C, Topic),
    ?retry(20, 50, ?assertNot(lists:member(ChanPid, monitored_by(Pid)))),
    exit(Pid, kill),
    ?assertEqual(timeout, ?block_until(#{?snk_kind := extsub_test_handler_down}, 500)),
    ok = emqtt:disconnect(C).

-doc "A down message for a destroyed handler does not reach any handler and does not crash the channel.".
t_monitor_down_after_destroy(_Config) ->
    Pid = start_monitored_process(?FUNCTION_NAME),
    ClientId = <<"monitor-after-destroy">>,
    Topic = monitor_topic(<<"keep">>, ?FUNCTION_NAME),
    C = emqx_extsub_test_utils:emqtt_connect([{clientid, ClientId}]),
    [ChanPid] = emqx_cm:lookup_channels(ClientId),
    {ok, {ok, _}} = ?wait_async_action(
        emqx_extsub_test_utils:emqtt_subscribe(C, Topic),
        #{?snk_kind := extsub_test_handler_monitor, pid := Pid},
        5000
    ),
    %% The handler keeps its monitor after it is destroyed
    {ok, _, _} = emqtt:unsubscribe(C, Topic),
    ?assert(lists:member(ChanPid, monitored_by(Pid))),
    exit(Pid, kill),
    ?assertEqual(timeout, ?block_until(#{?snk_kind := extsub_test_handler_down}, 500)),
    ?retry(20, 50, ?assertEqual({message_queue_len, 0}, process_info(ChanPid, message_queue_len))),
    ?assertEqual([ChanPid], emqx_cm:lookup_channels(ClientId)),
    ok = emqtt:disconnect(C).

start_monitored_process(Name) ->
    Pid = spawn(fun() ->
        receive
            stop -> ok
        end
    end),
    true = register(Name, Pid),
    Pid.

monitor_topic(Mode, Name) ->
    <<"extsub_st_monitor/", Mode/binary, "/", (atom_to_binary(Name))/binary>>.

monitored_by(Pid) ->
    {monitored_by, MonitoredBy} = process_info(Pid, monitored_by),
    MonitoredBy.

has_message_with_ref(MRef) ->
    {messages, Msgs} = process_info(self(), messages),
    lists:any(
        fun
            ({_Tag, Ref, process, _Pid, _Reason}) -> Ref =:= MRef;
            (_) -> false
        end,
        Msgs
    ).

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

-doc """
Checks `handle_subscribe/4` results with `ignore_resubscribe => false` for single-topic and
multi-topic handlers: `ignore` keeps an existing handler and creates none, `destroy` removes
the topic from an existing handler and creates none.
""".
t_subscribe_results(_Config) ->
    lists:foreach(fun test_subscribe_results/1, [false, true]).

test_subscribe_results(MultiTopic) ->
    Module = emqx_extsub_test_resub_handler,
    ok = emqx_extsub_handler_registry:register(Module, #{
        multi_topic => MultiTopic, ignore_resubscribe => false
    }),
    Topic = <<"resub/a">>,
    OtherTopic = <<"resub/b">>,
    Subscribe = fun(Registry, TopicFilter, Result) ->
        emqx_extsub_handler_registry:subscribe(
            Registry,
            subscribe,
            #{clientinfo => #{}, can_receive_acks => true},
            #{TopicFilter => #{result => Result}}
        )
    end,
    Handled = fun(Registry, TopicFilter) ->
        #{by_topic_cbm := ByTopicCBM} = emqx_extsub_handler_registry:inspect(Registry),
        maps:is_key({Module, TopicFilter}, ByTopicCBM)
    end,

    %% Without a handler, `ignore' and `destroy' create nothing.
    R1 = Subscribe(emqx_extsub_handler_registry:new(), Topic, ignore),
    R2 = Subscribe(R1, Topic, destroy),
    ?assertEqual(false, Handled(R2, Topic), #{multi_topic => MultiTopic}),

    %% `{ok, State}' creates the handler for each topic.
    R3 = Subscribe(R2, OtherTopic, ok),
    R4 = Subscribe(R3, Topic, ok),
    ?assertReceive({subscribed, Topic, #{Topic := 1}}),

    %% `ignore' keeps the existing state: the next `{ok, State}' sees the earlier count.
    R5 = Subscribe(R4, Topic, ignore),
    ?assert(Handled(R5, Topic)),
    R6 = Subscribe(R5, Topic, ok),
    ?assertReceive({subscribed, Topic, #{Topic := 2}}),

    %% `destroy' removes only this topic. The other topic stays handled.
    R7 = Subscribe(R6, Topic, destroy),
    ?assertNot(Handled(R7, Topic)),
    ?assert(Handled(R7, OtherTopic)),
    case MultiTopic of
        false ->
            %% A single-topic handler has no topics left, so it is terminated.
            ?assertReceive({terminated, #{}});
        true ->
            %% The multi-topic handler keeps the other topic and is not terminated.
            ?assertNotReceive({terminated, _}, 100)
    end,
    ok = emqx_extsub_handler_registry:unregister(Module).

-doc """
Checks that destroy results of `handle_info/3` and `handle_delivered/4` keep the state the
callback returned: the remaining topics of a multi-topic handler and `handle_terminate/1` see it.
""".
t_destroy_results(_Config) ->
    lists:foreach(fun test_destroy_results/1, [info, delivered]).

test_destroy_results(Via) ->
    Module = emqx_extsub_test_resub_handler,
    TopicA = <<"destroy/a">>,
    TopicB = <<"destroy/b">>,
    Subscribe = fun(Registry, TopicFilter) ->
        emqx_extsub_handler_registry:subscribe(
            Registry,
            subscribe,
            #{clientinfo => #{}, can_receive_acks => true},
            #{TopicFilter => #{result => ok}}
        )
    end,
    HandlerRef = fun(Registry, TopicFilter) ->
        case emqx_extsub_handler_registry:inspect(Registry) of
            #{by_topic_cbm := #{{Module, TopicFilter} := FoundRef}} -> FoundRef;
            #{} -> undefined
        end
    end,
    Command = fun(Registry, CmdRef, Cmd) -> run_command(Via, Registry, CmdRef, Cmd) end,

    %% A multi-topic handler keeps the returned state for its remaining topic.
    ok = emqx_extsub_handler_registry:register(Module, #{multi_topic => true}),
    R0 = Subscribe(Subscribe(emqx_extsub_handler_registry:new(), TopicA), TopicB),
    Ref = HandlerRef(R0, TopicA),
    R1 = Command(R0, Ref, {destroy, [TopicA]}),
    ?assertEqual(undefined, HandlerRef(R1, TopicA), #{via => Via}),
    ?assertEqual(Ref, HandlerRef(R1, TopicB)),
    R2 = Command(R1, Ref, report),
    ?assertReceive({state, #{marked := true, TopicB := 1}}),

    %% `handle_terminate/1' gets the state that the destroy result returned.
    _ = Command(R2, Ref, destroy_all),
    ?assertReceive({terminated, #{marked := true}}),
    ok = emqx_extsub_handler_registry:unregister(Module),

    %% A single-topic handler without topics left is terminated with the returned state.
    ok = emqx_extsub_handler_registry:register(Module, #{multi_topic => false}),
    R3 = Subscribe(emqx_extsub_handler_registry:new(), TopicA),
    R4 = Command(R3, HandlerRef(R3, TopicA), {destroy, [TopicA]}),
    ?assertEqual(undefined, HandlerRef(R4, TopicA)),
    ?assertReceive({terminated, #{marked := true}}),
    ok = emqx_extsub_handler_registry:unregister(Module).

run_command(info, Registry0, Ref, Cmd) ->
    {ok, Registry} = emqx_extsub_handler_registry:info(Registry0, Ref, #{}, Cmd),
    Registry;
run_command(delivered, Registry0, Ref, Cmd) ->
    Msg0 = emqx_message:make(test, <<"destroy/msg">>, term_to_binary(Cmd)),
    Registry1 = emqx_extsub_handler_registry:buffer_add(Registry0, Ref, [Msg0]),
    {[{Ref, SeqId, Msg}], Registry2} = emqx_extsub_handler_registry:buffer_take(Registry1, 1),
    emqx_extsub_handler_registry:delivered(Registry2, Ref, #{}, Msg, SeqId, ack).

-doc "Checks that `handle_info/3' latencies are observed only after `observe_latencies(true)'.".
t_observe_latencies(_Config) ->
    {ok, Handler} = emqx_extsub_handler:subscribe_new(
        subscribe,
        emqx_extsub_test_resub_handler,
        #{},
        #{subopts => #{result => ok}},
        <<"latency/t">>
    ),
    Count = fun() ->
        #{handle_info_latency_ms := #{count := N}} =
            emqx_metrics_worker:get_hists(ext_sub_metrics, ext_sub_metrics_id),
        N
    end,

    %% Disabled by default: the histogram does not change.
    Count0 = Count(),
    {ok, _} = emqx_extsub_handler:info(Handler, #{}, test_info),
    ?assertEqual(Count0, Count()),

    %% Enabled: each call adds one observation.
    ok = emqx_extsub:observe_latencies(true),
    {ok, _} = emqx_extsub_handler:info(Handler, #{}, test_info),
    ?assertEqual(Count0 + 1, Count()),

    %% Disabled again: the histogram does not change.
    ok = emqx_extsub:observe_latencies(false),
    {ok, _} = emqx_extsub_handler:info(Handler, #{}, test_info),
    ?assertEqual(Count0 + 1, Count()).

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
