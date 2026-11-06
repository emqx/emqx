%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_retainer_dispatcher_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include("emqx_retainer.hrl").
-include_lib("emqx/include/asserts.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include("../../emqx_extsub/src/emqx_extsub_internal.hrl").

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [
            emqx,
            emqx_conf,
            {emqx_retainer, #{config => #{retainer => #{delivery_rate => infinity}}}}
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    emqx_cth_suite:stop(?config(apps, Config)).

init_per_testcase(_Case, Config) ->
    emqx_config:put([retainer, backend, reindex_delivery_action], redo),
    emqx_config:put([retainer, flow_control, batch_deliver_limiter], infinity),
    emqx_config:put([retainer, flow_control, batch_read_number], 0),
    emqx_config:put([retainer, max_dispatcher_queue_length], 50_000),
    ok = update_limiter(),
    ok = restart_workers(),
    ok = emqx_retainer:clean(),
    ok = emqx_retainer_mnesia:reindex([[1, 3]], true, fun(_) -> ok end),
    Config.

end_per_testcase(_Case, _Config) ->
    emqx_common_test_helpers:call_janitor(),
    ok.

-doc "Reindexing restarts an old cursor when the action is redo.".
t_reindex_redo(_Config) ->
    store_messages(),
    {ok, [First], Cursor0, 1} = batch(undefined),
    ok = emqx_retainer_mnesia:reindex([[1, 3]], false, fun(_) -> ok end),
    ?assertMatch({ok, [First], _, 1}, batch(Cursor0)).

-doc "Reindexing stops an old cursor when the action is stop.".
t_reindex_stop(_Config) ->
    store_messages(),
    {ok, [_], Cursor0, 1} = batch(undefined),
    emqx_config:put([retainer, backend, reindex_delivery_action], stop),
    ok = emqx_retainer_mnesia:reindex([[1, 3]], false, fun(_) -> ok end),
    ?assertEqual({ok, [], undefined, 0}, batch(Cursor0)).

-doc "A cursor created during refill continues in the message table after finalize.".
t_refill_cursor(_Config) ->
    store_messages(),
    Self = self(),
    ok = emqx_retainer_mnesia:reindex([[1, 3]], false, fun(_) ->
        Self ! {cursor, batch(undefined)}
    end),
    receive
        {cursor, {ok, [First], Cursor0, 1}} ->
            {ok, [Second], Cursor1, 1} = batch(Cursor0),
            {ok, [Third], Cursor2, 1} = batch(Cursor1),
            ?assertEqual(3, length(lists:usort([First, Second, Third]))),
            ?assertEqual({ok, [], undefined, 0}, batch(Cursor2))
    after 1000 ->
        ct:fail(no_refill_cursor)
    end.

-doc "A wildcard batch waits for the dispatcher and does not read in the channel.".
t_async_fetch_and_demand(_Config) ->
    store_messages(),
    ok = set_batch_read_number(1),
    Worker = emqx_retainer_dispatcher:worker(),
    ok = sys:suspend(Worker),
    emqx_common_test_helpers:on_exit(fun() -> catch sys:resume(Worker) end),
    {Ref, Handler0} = subscribe(),
    ?assertNotReceive(#info_to_extsub{handler_ref = Ref}, 100),
    ok = sys:resume(Worker),
    Batch0 = receive_batch(Ref),
    {ok, Handler1, [Msg0]} = info(Handler0, Batch0, 0),
    ?assertNotReceive(#info_to_extsub{handler_ref = Ref}, 100),
    AckCtx = #{desired_message_count => 1, delivering_count => 0, unacked_count => 0},
    {ok, Handler2} = emqx_retainer_extsub_handler:handle_delivered(Handler1, AckCtx, Msg0, ack),
    {ok, Handler3, [_]} = info(Handler2, receive_batch(Ref), 100),
    {ok, Handler4, [_]} = info(Handler3, receive_batch(Ref), 100),
    ?assertMatch({destroy, _, [_]}, info(Handler4, receive_batch(Ref), 1)),
    ok = emqx_retainer_extsub_handler:handle_terminate(Handler4),
    assert_no_monitor(Worker).

-doc "A handler retries an outstanding request after its dispatcher dies.".
t_dispatcher_death(_Config) ->
    store_messages(),
    ok = set_batch_read_number(1),
    Worker0 = emqx_retainer_dispatcher:worker(),
    ok = sys:suspend(Worker0),
    {Ref, Handler0} = subscribe(),
    exit(Worker0, kill),
    Down0 = receive_down(Ref),
    {ok, Handler1} = info(Handler0, Down0, 1),
    Retry = receive_info(Ref),
    {ok, Handler2} = info(Handler1, Retry, 1),
    {ok, Handler3, [_]} = info(Handler2, receive_batch(Ref), 0),
    %% No request is outstanding after the reply, so death must not replay that batch.
    Worker1 = emqx_retainer_dispatcher:worker(),
    exit(Worker1, kill),
    {ok, Handler4} = info(Handler3, receive_down(Ref), 0),
    ?assertNotReceive(#info_to_extsub{handler_ref = Ref}, 400),
    ok = emqx_retainer_extsub_handler:handle_terminate(Handler4).

-doc "Unsubscribe removes the dispatcher monitor and ignores an outstanding reply.".
t_unsubscribe_pending(_Config) ->
    store_messages(),
    Worker = emqx_retainer_dispatcher:worker(),
    ok = sys:suspend(Worker),
    emqx_common_test_helpers:on_exit(fun() -> catch sys:resume(Worker) end),
    {Ref, Handler0} = subscribe(),
    Handler1 = emqx_retainer_extsub_handler:handle_unsubscribe(
        unsubscribe, #{}, Handler0, <<"t/+/a">>
    ),
    assert_no_monitor(Worker),
    ok = sys:resume(Worker),
    ?assertEqual({ok, Handler1}, info(Handler1, receive_batch(Ref), 1)),
    ok = emqx_retainer_extsub_handler:handle_terminate(Handler1).

-doc "Empty reads consume no tokens; a read that finds messages waits for tokens.".
t_empty_read_limiter(_Config) ->
    store_messages(),
    emqx_config:put([retainer, flow_control, batch_deliver_limiter], {1, 10000}),
    ok = update_limiter(),
    ok = restart_workers(),

    %% Empty wildcard and exact-topic reads complete without tokens
    lists:foreach(
        fun(Topic) ->
            {Ref, Handler} = subscribe(#{rh => 0}, Topic),
            ?assertMatch({destroy, _, [Topic]}, info(Handler, receive_batch(Ref), 1))
        end,
        [<<"x/+/a">>, <<"t/+/b">>, <<"x/1/a">>, <<"t/1/b">>]
    ),

    %% The first read of a worker is free, so one token allows two reads that find a message
    {Ref0, _} = subscribe(#{rh => 0}, <<"t/1/a">>),
    {Ref1, _} = subscribe(#{rh => 0}, <<"t/2/a">>),
    _ = receive_batch(Ref0),
    _ = receive_batch(Ref1),
    {Ref2, Handler2} = subscribe(#{rh => 0}, <<"t/3/a">>),
    ?assertNotReceive(#info_to_extsub{handler_ref = Ref2}, 200),

    %% A barrier completes while a read waits for tokens
    ok = emqx_retainer_dispatcher:wait_dispatch_complete(1000),
    emqx_config:put([retainer, flow_control, batch_deliver_limiter], infinity),
    ok = update_limiter(),
    ?assertMatch(
        {ok, _, [#message{topic = <<"t/3/a">>}]}, info(Handler2, receive_batch(Ref2), 1)
    ).

-doc "Session takeover drops an outstanding retained batch and does not restart iteration.".
t_takeover_pending(_Config) ->
    store_messages(),
    ClientId = <<"retainer_dispatcher_takeover">>,
    Opts = #{
        clientid => ClientId,
        proto_ver => v5,
        clean_start => false,
        properties => #{'Session-Expiry-Interval' => 30}
    },
    {ok, Client0} = emqtt:start_link(Opts#{clean_start := true}),
    {ok, _} = emqtt:connect(Client0),
    [Channel0] = emqx_cm:lookup_channels(ClientId),
    Worker = gproc_pool:pick_worker(?DISPATCHER_POOL, Channel0),
    ok = sys:suspend(Worker),
    emqx_common_test_helpers:on_exit(fun() -> catch sys:resume(Worker) end),
    {ok, _, _} = emqtt:subscribe(Client0, <<"t/+/a">>, 1),
    ?assertNotReceive({publish, _}, 100),
    unlink(Client0),
    {ok, Client1} = emqtt:start_link(Opts),
    {ok, _} = emqtt:connect(Client1),
    ok = sys:resume(Worker),
    ?assertNotReceive({publish, _}, 200),
    [Channel1] = emqx_cm:lookup_channels(ClientId),
    ?assertMatch({ok, #{registry := #{by_ref := []}}}, emqx_extsub:inspect(Channel1)),
    ok = emqtt:stop(Client1).

-doc "Filtered index candidates do not end iteration before a later matching message.".
t_filtered_batch(_Config) ->
    ok = emqx_retainer_mnesia:reindex([[2, 3]], false, fun(_) -> ok end),
    store(<<"$SYS/a/1">>),
    store(<<"t/a/2">>),
    Opts = #{batch_read_number => 1},
    {ok, [], Cursor0, 1} = emqx_retainer_mnesia:match_messages(#{}, <<"+/a/#">>, undefined, Opts),
    ?assertNotEqual(undefined, Cursor0),
    ?assertMatch(
        {ok, [#message{topic = <<"t/a/2">>}], _, 1},
        emqx_retainer_mnesia:match_messages(#{}, <<"+/a/#">>, Cursor0, Opts)
    ).

-doc "A resubscription ignores saved delivery counts and starts from the first retained message.".
t_saved_deliveries(_Config) ->
    store_messages(),
    ok = set_batch_read_number(1),
    {Ref, Handler0} = subscribe(#{rh => 0, emqx_retainer_extsub_handler => #{delivered => 1}}),
    {ok, Handler1, [#message{topic = <<"t/1/a">>}]} = info(Handler0, receive_batch(Ref), 100),
    {ok, Handler2, [#message{topic = <<"t/2/a">>}]} = info(Handler1, receive_batch(Ref), 0),
    {ok, Handler3} = emqx_retainer_extsub_handler:handle_save_subopts(Handler2, #{}, #{}),
    assert_no_monitor(emqx_retainer_dispatcher:worker()),
    ok = emqx_retainer_extsub_handler:handle_terminate(Handler3).

-doc "Each batch bounds selected records and resumes after deleting the last selected record.".
t_bounded_scan(_Config) ->
    lists:foreach(
        fun(Indices) ->
            ok = emqx_retainer:clean(),
            ok = emqx_retainer_mnesia:reindex(Indices, false, fun(_) -> ok end),
            lists:foreach(fun store/1, [<<"s/0/no">>, <<"$SYS/a/1">>, <<"t/a/2">>, <<"u/b/yes">>]),
            Opts = #{batch_read_number => 1},
            {ok, [], Cursor0, 1} = emqx_retainer_mnesia:match_messages(
                #{}, <<"+/a/#">>, undefined, Opts
            ),
            ?assertNotEqual(undefined, Cursor0),
            ok = emqx_retainer_mnesia:delete_message(#{}, <<"$SYS/a/1">>),
            {ok, [#message{topic = <<"t/a/2">>}], Cursor1, 1} =
                emqx_retainer_mnesia:match_messages(#{}, <<"+/a/#">>, Cursor0, Opts),
            ?assertEqual(
                {ok, [], undefined, 0},
                emqx_retainer_mnesia:match_messages(#{}, <<"+/a/#">>, Cursor1, Opts)
            )
        end,
        [[], [[2]], [[2, 3]]]
    ).

-doc "Search streams preserve parent topics, empty levels and MQTT wildcard matching.".
t_scan_ranges(_Config) ->
    Topics = [<<"/a">>, <<"$SYS/a">>, <<"a">>, <<"a/">>, <<"a//b">>, <<"a/b">>, <<"b/a">>],
    Filters = [<<"#">>, <<"+/a">>, <<"a/#">>, <<"a/+">>, <<"a/+/b">>, <<"/a">>, <<"$SYS/#">>],
    lists:foreach(
        fun(Indices) ->
            ok = emqx_retainer:clean(),
            ok = emqx_retainer_mnesia:reindex(Indices, false, fun(_) -> ok end),
            lists:foreach(fun store/1, Topics),
            lists:foreach(
                fun(Filter) ->
                    {ok, Messages, undefined, Cost} = emqx_retainer_mnesia:match_messages(
                        #{}, Filter, undefined, #{batch_read_number => all_remaining}
                    ),
                    ?assert(Cost >= length(Messages)),
                    ?assertEqual(
                        lists:sort([T || T <- Topics, emqx_topic:match(T, Filter)]),
                        lists:sort([T || #message{topic = T} <- Messages])
                    )
                end,
                Filters
            )
        end,
        [[], [[1]], [[2, 3]], [[1, 3], [2]]]
    ).

-doc "An exact-topic read runs in the channel, not in the dispatcher, also with a read limit.".
t_exact_in_channel(_Config) ->
    store_messages(),
    emqx_common_test_helpers:on_exit(fun() ->
        emqx_retainer_channel:on_session_disconnected(#{}, #{})
    end),
    lists:foreach(
        fun(#{limiter := Limiter} = Case) ->
            emqx_config:put([retainer, flow_control, batch_deliver_limiter], Limiter),
            ok = update_limiter(),
            ok = restart_workers(),
            Worker = emqx_retainer_dispatcher:worker(),
            ok = sys:suspend(Worker),
            ok = emqx_retainer_channel:on_session_subscribed(
                #{protocol => mqtt}, <<"t/1/a">>, #{rh => 0}
            ),
            ?assertReceive(
                #retained_batch{
                    topic_filter = <<"t/1/a">>,
                    messages = [#message{topic = <<"t/1/a">>}],
                    cursor = done
                },
                1000,
                Case
            ),
            assert_no_monitor(Worker),
            ok = sys:resume(Worker)
        end,
        [#{limiter => infinity}, #{limiter => {1, 10000}}]
    ).

-doc "Exact-topic requests of ExtSub handlers use the dispatcher queue alongside wildcard reads.".
t_exact_limited(_Config) ->
    store_messages(),
    emqx_config:put([retainer, flow_control, batch_read_number], 1),
    %% The first read of a worker is free, so one token allows two reads.
    emqx_config:put([retainer, flow_control, batch_deliver_limiter], {1, 10000}),
    ok = update_limiter(),
    ok = emqx_retainer_sup:stop_workers(),
    ok = emqx_retainer_sup:start_workers(),
    Worker = emqx_retainer_dispatcher:worker(),
    ok = sys:suspend(Worker),
    emqx_common_test_helpers:on_exit(fun() -> catch sys:resume(Worker) end),
    {Ref0, Handler0} = subscribe(#{rh => 0}, <<"t/1/a">>),
    ?assertNotReceive(#info_to_extsub{handler_ref = Ref0}, 100),
    {Ref1, Handler1} = subscribe(),
    {Ref2, Handler2} = subscribe(#{rh => 0}, <<"t/2/a">>),
    ok = sys:resume(Worker),
    {ok, Handler3, [#message{topic = <<"t/1/a">>}]} = info(Handler0, receive_batch(Ref0), 0),
    {ok, Handler4, [_]} = info(Handler1, receive_batch(Ref1), 0),
    ?assertNotReceive(#info_to_extsub{handler_ref = Ref2}, 200),
    emqx_config:put([retainer, flow_control, batch_deliver_limiter], infinity),
    ok = update_limiter(),
    {ok, Handler5, [#message{topic = <<"t/2/a">>}]} = info(Handler2, receive_batch(Ref2), 0),
    lists:foreach(fun emqx_retainer_extsub_handler:handle_terminate/1, [
        Handler3, Handler4, Handler5
    ]).

-doc "Requests that arrive while the limiter refuses reads are served in arrival order.".
t_paused_order(_Config) ->
    store_messages(),
    %% The first read of a worker is free, so one token allows two reads.
    emqx_config:put([retainer, flow_control, batch_deliver_limiter], {1, 10000}),
    ok = update_limiter(),
    ok = restart_workers(),
    Topics = [<<"t/1/a">>, <<"t/2/a">>, <<"t/3/a">>, <<"t/+/a">>, <<"t/1/a">>],
    Refs = [element(1, subscribe(#{rh => 0}, Topic)) || Topic <- Topics],
    [Ref0, Ref1 | WaitingRefs] = Refs,
    _ = receive_batch(Ref0),
    _ = receive_batch(Ref1),
    ?assertNotReceive(#info_to_extsub{}, 200),

    %% After the limit is removed, the waiting requests are served in order
    emqx_config:put([retainer, flow_control, batch_deliver_limiter], infinity),
    ok = update_limiter(),
    ?assertEqual(WaitingRefs, [receive_any_batch() || _ <- WaitingRefs]),

    %% A new request is served at once
    {Ref2, _} = subscribe(),
    _ = receive_batch(Ref2).

-doc "A waiting request of a dead channel is not read when the limiter allows reads again.".
t_paused_dead_target(_Config) ->
    store_messages(),
    emqx_config:put([retainer, flow_control, batch_deliver_limiter], {1, 10000}),
    ok = update_limiter(),
    ok = restart_workers(),
    Worker = emqx_retainer_dispatcher:worker(),
    %% The first read of a worker is free, so one token allows two exact-topic reads.
    {Ref0, _} = subscribe(#{rh => 0}, <<"t/1/a">>),
    {Ref1, _} = subscribe(#{rh => 0}, <<"t/2/a">>),
    _ = receive_batch(Ref0),
    _ = receive_batch(Ref1),

    %% The request of a dead channel waits, then a live channel's request waits
    {Dead, MRef} = spawn_monitor(fun() -> ok end),
    receive
        {'DOWN', MRef, process, Dead, _} -> ok
    end,
    DeadTopic = <<"t/+/dead">>,
    ok = emqx_retainer_dispatcher:fetch(
        Worker, {Dead, make_ref()}, make_ref(), DeadTopic, emqx_topic:words(DeadTopic), undefined
    ),
    {Ref2, _} = subscribe(),
    ?assertNotReceive(#info_to_extsub{}, 200),

    %% Only the live request is read after the limit is removed
    ok = snabbkaffe:start_trace(),
    emqx_common_test_helpers:on_exit(fun snabbkaffe:stop/0),
    emqx_config:put([retainer, flow_control, batch_deliver_limiter], infinity),
    ok = update_limiter(),
    _ = receive_batch(Ref2),
    {ok, _} = ?block_until(#{?snk_kind := retainer_dispatcher_batch, topic := <<"t/+/a">>}, 5000),
    ?assertEqual(
        [<<"t/+/a">>],
        ?projection(topic, ?of_kind(retainer_dispatcher_batch, snabbkaffe:collect_trace()))
    ).

-doc """
A worker with a full queue drops new requests; the requests in the queue are served in order.
The requests of dead targets count until the worker reaches them.
""".
t_queue_full(_Config) ->
    emqx_config:put([retainer, max_dispatcher_queue_length], 3),
    Worker = emqx_retainer_dispatcher:worker(),
    lists:foreach(
        fun(#{requests := Kinds, served := Served, dropped := Dropped} = Case) ->
            ok = sys:suspend(Worker),
            Refs = [Ref || {live, Ref} <- [fetch_request(Worker, Kind) || Kind <- Kinds]],
            ok = sys:resume(Worker),

            %% The queued live requests are served, the requests above the limit are dropped
            Replies = [element(1, receive_info(Ref)) || Ref <- Refs],
            ?assertEqual(
                lists:duplicate(Served, retained_batch) ++
                    lists:duplicate(Dropped, retained_dropped),
                Replies,
                Case
            )
        end,
        [
            #{requests => [live, live, live, live, live], served => 3, dropped => 2},
            #{requests => [dead, dead, dead, live, live], served => 0, dropped => 2}
        ]
    ),

    %% The worker accepts requests again after it empties the queue
    {live, Ref} = fetch_request(Worker, live),
    ?assertMatch(#retained_batch{}, receive_info(Ref)).

-doc "A configuration update sets `retainer.max_dispatcher_queue_length` and rejects 0.".
t_max_dispatcher_queue_length_config(_Config) ->
    emqx_common_test_helpers:on_exit(fun() ->
        {ok, _} = emqx_retainer:update_config(#{<<"max_dispatcher_queue_length">> => 50_000})
    end),
    {ok, _} = emqx_retainer:update_config(#{<<"max_dispatcher_queue_length">> => 7}),
    ?assertEqual(7, emqx_config:get([retainer, max_dispatcher_queue_length])),
    ?assertMatch(
        {error, _}, emqx_retainer:update_config(#{<<"max_dispatcher_queue_length">> => 0})
    ).

-doc "A channel ends a read that the dispatcher worker drops and ignores the late reply.".
t_lazy_dropped(_Config) ->
    store_messages(),
    {Client, Channel} = connect(#{}),
    Worker = gproc_pool:pick_worker(?DISPATCHER_POOL, Channel),
    ok = sys:suspend(Worker),
    emqx_common_test_helpers:on_exit(fun() -> catch sys:resume(Worker) end),
    {ok, _, _} = emqtt:subscribe(Client, <<"t/+/a">>, 1),
    {dictionary, Dict} = process_info(Channel, dictionary),
    #{<<"t/+/a">> := {read, Ref, Worker}} = proplists:get_value(emqx_retainer_reads, Dict),

    %% The drop reply ends the read and releases the worker monitor
    Channel ! #retained_dropped{topic_filter = <<"t/+/a">>, request_ref = Ref},
    ?retry(50, 20, assert_no_read_state(Channel)),
    assert_no_monitor(Channel, Worker),

    %% The late reply of the worker to the same request delivers nothing
    ok = sys:resume(Worker),
    ?assertEqual([], receive_publishes(1)),
    ok = emqtt:stop(Client).

-doc "A handler whose request is dropped stops after the messages that it already delivers.".
t_dropped_handler(_Config) ->
    emqx_config:put([retainer, max_dispatcher_queue_length], 1),
    Worker = emqx_retainer_dispatcher:worker(),
    ok = sys:suspend(Worker),
    emqx_common_test_helpers:on_exit(fun() -> catch sys:resume(Worker) end),
    {Ref0, Handler0} = subscribe(),
    {Ref1, Handler1} = subscribe(),
    ok = sys:resume(Worker),

    %% The second request is dropped: the handler waits for messages in delivery, then stops
    Dropped = receive_info(Ref1),
    ?assertMatch(#retained_dropped{topic_filter = <<"t/+/a">>}, Dropped),
    DeliveringCtx = #{desired_message_count => 1, delivering_count => 1},
    ?assertMatch(
        {ok, _, []}, emqx_retainer_extsub_handler:handle_info(Handler1, DeliveringCtx, Dropped)
    ),
    ?assertMatch({destroy, _, [<<"t/+/a">>]}, info(Handler1, Dropped, 1)),

    %% The first request is served
    ?assertMatch({destroy, _, [<<"t/+/a">>]}, info(Handler0, receive_batch(Ref0), 1)),
    lists:foreach(fun emqx_retainer_extsub_handler:handle_terminate/1, [Handler0, Handler1]).

-doc "A handler with a pending read stops when the retainer is disabled.".
t_retainer_disabled(_Config) ->
    store_messages(),
    Worker = emqx_retainer_dispatcher:worker(),
    ok = sys:suspend(Worker),
    {Ref, Handler0} = subscribe(),
    emqx_common_test_helpers:on_exit(fun() ->
        {ok, _} = emqx_retainer:update_config(#{<<"enable">> => true})
    end),
    {ok, _} = emqx_retainer:update_config(#{<<"enable">> => false}),
    %% The handler schedules one retry after the dispatcher stops.
    {ok, Handler1} = info(Handler0, receive_down(Ref), 1),
    ?assertMatch({destroy, _, [<<"t/+/a">>]}, info(Handler1, receive_info(Ref), 1)).

-doc "A failed read replies to its handler and keeps the dispatcher worker alive.".
t_read_failure(_Config) ->
    Worker = emqx_retainer_dispatcher:worker(),
    Ref = make_ref(),
    Target = emqx_extsub_handler:message_target(Ref),
    Incarnation = emqx_retainer_mnesia:current_index_incarnation(undefined),
    Cursor = #{incarnation => Incarnation, index => undefined, continuation => invalid},
    Request = make_ref(),
    Topic = <<"t/+/a">>,
    ok = emqx_retainer_dispatcher:fetch(
        Worker, Target, Request, Topic, emqx_topic:words(Topic), Cursor
    ),
    ?assertMatch(
        #retained_batch{topic_filter = Topic, request_ref = Request, messages = [], cursor = done},
        receive_info(Ref)
    ),
    ?assert(is_process_alive(Worker)).

-doc "A repeated SUBSCRIBE that needs no retained messages keeps the running delivery.".
t_resubscribe_keeps_delivery(_Config) ->
    store_messages(),
    lists:foreach(
        fun(RH) ->
            %% A new client ID for each case: an immediate reconnect with the same client ID
            %% can be throttled while the previous channel unregisters.
            ClientId = <<"retainer_dispatcher_resubscribe_", (integer_to_binary(RH))/binary>>,
            {Client, Worker} = connect_with_suspended_dispatcher(ClientId),
            {ok, _, _} = emqtt:subscribe(Client, <<"t/+/a">>, [{qos, 0}, {rh, 1}]),
            {ok, _, _} = emqtt:subscribe(Client, <<"t/+/a">>, [{qos, 0}, {rh, RH}]),
            ok = sys:resume(Worker),
            ?assertEqual(
                [<<"t/1/a">>, <<"t/2/a">>, <<"t/3/a">>],
                lists:sort(receive_publishes(3)),
                #{rh => RH}
            ),
            ?assertEqual([], receive_publishes(1)),
            ok = emqtt:stop(Client)
        end,
        [1, 2]
    ).

-doc "A repeated SUBSCRIBE with Retain Handling 0 restarts delivery and drops the old reply.".
t_resubscribe_restarts_delivery(_Config) ->
    store_messages(),
    {Client, Worker} = connect_with_suspended_dispatcher(<<"retainer_dispatcher_resubscribe">>),
    {ok, _, _} = emqtt:subscribe(Client, <<"t/+/a">>, [{qos, 0}, {rh, 0}]),
    {ok, _, _} = emqtt:subscribe(Client, <<"t/+/a">>, [{qos, 0}, {rh, 0}]),
    ok = sys:resume(Worker),
    ?assertEqual([<<"t/1/a">>, <<"t/2/a">>, <<"t/3/a">>], lists:sort(receive_publishes(3))),
    ?assertEqual([], receive_publishes(1)),
    ok = emqtt:stop(Client).

-doc "A limited rate reduces the batch size to the capacity share of each dispatcher worker.".
t_batch_size_limited(_Config) ->
    lists:foreach(
        fun(N) -> store(<<"t/", (integer_to_binary(N))/binary, "/a">>) end,
        lists:seq(1, 100)
    ),
    Share = 20,
    emqx_config:put([retainer, flow_control, batch_read_number], 1000),
    emqx_config:put(
        [retainer, flow_control, batch_deliver_limiter],
        {Share * emqx_vm:schedulers(), 1000}
    ),
    ok = update_limiter(),
    {Ref, Handler0} = subscribe(),
    {ok, Handler1, Messages} = info(Handler0, receive_batch(Ref), 0),
    ?assertEqual(Share, length(Messages)),
    ok = emqx_retainer_extsub_handler:handle_terminate(Handler1).

-doc "A finished handler stops after its last delivery even when other handlers have unacked messages.".
t_done_with_other_unacked(_Config) ->
    store_messages(),
    {Ref, Handler0} = subscribe(),
    {ok, Handler1, [Msg | _]} = info(Handler0, receive_batch(Ref), 100),
    AckCtx = #{desired_message_count => 100, delivering_count => 0, unacked_count => 5},
    ?assertMatch(
        {destroy, _, [<<"t/+/a">>]},
        emqx_retainer_extsub_handler:handle_delivered(Handler1, AckCtx, Msg, ack)
    ).

-doc "A handler does not request another batch when the received batch fills its buffer.".
t_no_request_when_buffer_full(_Config) ->
    store_messages(),
    ok = set_batch_read_number(1),
    {Ref, Handler0} = subscribe(),
    Ctx = #{desired_message_count => 100, delivering_count => 99},
    {ok, Handler1, [_]} = emqx_retainer_extsub_handler:handle_info(
        Handler0, Ctx, receive_batch(Ref)
    ),
    ?assertNotReceive(#info_to_extsub{handler_ref = Ref}, 200),
    ok = emqx_retainer_extsub_handler:handle_terminate(Handler1).

-doc "A read that ends in one batch and fits the session is delivered without an ExtSub handler.".
t_lazy_no_handover(_Config) ->
    store_messages(),
    ok = snabbkaffe:start_trace(),
    emqx_common_test_helpers:on_exit(fun snabbkaffe:stop/0),
    {Client, Channel} = connect(#{}),
    {ok, _, _} = emqtt:subscribe(Client, <<"t/+/a">>, 1),
    ?assertEqual([<<"t/1/a">>, <<"t/2/a">>, <<"t/3/a">>], lists:sort(receive_publishes(3))),
    ?assertEqual([], ?of_kind(retainer_handover, snabbkaffe:collect_trace())),
    ?assertMatch({ok, #{registry := #{by_ref := []}}}, emqx_extsub:inspect(Channel)),
    assert_no_monitor(Channel, gproc_pool:pick_worker(?DISPATCHER_POOL, Channel)),
    assert_no_read_state(Channel),
    ok = emqtt:stop(Client).

-doc "A read with more batches is handed over to ExtSub, which delivers the rest and stops.".
t_handover_more_batches(_Config) ->
    store_qos1_messages(),
    ok = set_batch_read_number(1),
    ok = snabbkaffe:start_trace(),
    emqx_common_test_helpers:on_exit(fun snabbkaffe:stop/0),
    {Client, Channel} = connect(#{}),
    {ok, _, _} = emqtt:subscribe(Client, <<"t/+/a">>, 1),
    ?assertEqual(
        [<<"t/1/a">>, <<"t/2/a">>, <<"t/3/a">>],
        lists:sort(ack_publishes(Client, 3))
    ),
    ?assertMatch(
        [#{topic_filter := <<"t/+/a">>, cursor := {cursor, _}, count := 0}],
        ?of_kind(retainer_handover, snabbkaffe:collect_trace())
    ),
    ?retry(
        100,
        20,
        ?assertMatch({ok, #{registry := #{by_ref := []}}}, emqx_extsub:inspect(Channel))
    ),
    ok = emqtt:stop(Client).

-doc "Messages beyond the available inflight capacity are handed over together.".
t_handover_nack(_Config) ->
    store_qos1_messages(),
    ok = snabbkaffe:start_trace(),
    emqx_common_test_helpers:on_exit(fun snabbkaffe:stop/0),
    {Client, Channel} = connect(#{receive_maximum => 1}),
    {ok, _, _} = emqtt:subscribe(Client, <<"t/+/a">>, 1),

    %% Send one message and hand over the other two together.
    {publish, #{topic := Topic1, packet_id := PacketId1}} = ?assertReceive({publish, _}, 2000),
    ?assertNotReceive({publish, _}, 500),
    ?assertMatch(
        [#{cursor := done, count := 2}],
        ?of_kind(retainer_handover, snabbkaffe:collect_trace())
    ),

    %% ExtSub delivers the remaining messages after the acknowledgements.
    ok = emqtt:puback(Client, PacketId1),
    ?assertEqual(
        [<<"t/1/a">>, <<"t/2/a">>, <<"t/3/a">>],
        lists:sort([Topic1 | ack_publishes(Client, 2)])
    ),
    ?retry(
        100,
        20,
        ?assertMatch({ok, #{registry := #{by_ref := []}}}, emqx_extsub:inspect(Channel))
    ),
    assert_no_read_state(Channel),
    ok = emqtt:stop(Client).

-doc "A batch larger than the session's room is split: the rest is handed over at once.".
t_handover_room(_Config) ->
    store_qos1_messages(),
    ok = emqx_extsub:set_max_unacked(1),
    emqx_common_test_helpers:on_exit(fun() -> emqx_extsub:set_max_unacked(?EXTSUB_MAX_UNACKED) end),
    ok = snabbkaffe:start_trace(),
    emqx_common_test_helpers:on_exit(fun snabbkaffe:stop/0),
    {Client, _Channel} = connect(#{receive_maximum => 1}),
    {ok, _, _} = emqtt:subscribe(Client, <<"t/+/a">>, 1),
    ?assertEqual(
        [<<"t/1/a">>, <<"t/2/a">>, <<"t/3/a">>],
        lists:sort(ack_publishes(Client, 3))
    ),
    ?assertMatch(
        [#{cursor := done, count := 2}],
        ?of_kind(retainer_handover, snabbkaffe:collect_trace())
    ),
    ok = emqtt:stop(Client).

-doc "Unsubscribe stops a pending read: the channel drops the monitor and the late reply.".
t_lazy_unsubscribe_pending(_Config) ->
    store_messages(),
    {Client, Channel} = connect(#{}),
    Worker = gproc_pool:pick_worker(?DISPATCHER_POOL, Channel),
    ok = sys:suspend(Worker),
    emqx_common_test_helpers:on_exit(fun() -> catch sys:resume(Worker) end),
    {ok, _, _} = emqtt:subscribe(Client, <<"t/+/a">>, 1),
    {monitors, Monitors} = process_info(Channel, monitors),
    ?assert(lists:member({process, Worker}, Monitors)),
    {ok, _, _} = emqtt:unsubscribe(Client, <<"t/+/a">>),
    assert_no_monitor(Channel, Worker),
    ok = sys:resume(Worker),
    ?assertEqual([], receive_publishes(1)),
    ?assertMatch({ok, #{registry := #{by_ref := []}}}, emqx_extsub:inspect(Channel)),
    ok = emqtt:stop(Client).

-doc "Unsubscribe while the retainer is disabled removes the state of a pending channel read.".
t_lazy_unsubscribe_disabled(_Config) ->
    store_messages(),
    {Client, Channel} = connect(#{}),
    Worker = gproc_pool:pick_worker(?DISPATCHER_POOL, Channel),
    ok = sys:suspend(Worker),
    emqx_common_test_helpers:on_exit(fun() -> catch sys:resume(Worker) end),
    {ok, _, _} = emqtt:subscribe(Client, <<"t/+/a">>, 1),
    {dictionary, Dict} = process_info(Channel, dictionary),
    ?assertMatch(#{<<"t/+/a">> := _}, proplists:get_value(emqx_retainer_reads, Dict)),

    %% Disable stops the worker while the channel read waits for it
    emqx_common_test_helpers:on_exit(fun() ->
        {ok, _} = emqx_retainer:update_config(#{<<"enable">> => true})
    end),
    {ok, _} = emqx_retainer:update_config(#{<<"enable">> => false}),
    ?assertNot(is_process_alive(Worker)),

    %% Unsubscribe while disabled removes the read and the worker bookkeeping
    {ok, _, _} = emqtt:unsubscribe(Client, <<"t/+/a">>),
    assert_no_read_state(Channel),
    ok = emqtt:stop(Client).

-doc "When the dispatcher worker dies before it replies, ExtSub repeats the read.".
t_lazy_worker_down(_Config) ->
    store_messages(),
    {Client, Channel} = connect(#{}),
    Worker = gproc_pool:pick_worker(?DISPATCHER_POOL, Channel),
    ok = sys:suspend(Worker),
    {ok, _, _} = emqtt:subscribe(Client, <<"t/+/a">>, 1),
    exit(Worker, kill),
    ?assertEqual([<<"t/1/a">>, <<"t/2/a">>, <<"t/3/a">>], lists:sort(receive_publishes(3))),
    ok = emqtt:stop(Client).

store_messages() ->
    lists:foreach(fun store/1, [<<"t/1/a">>, <<"t/2/a">>, <<"t/3/a">>]).

store_qos1_messages() ->
    lists:foreach(
        fun(Topic) ->
            emqx_retainer_mnesia:store_retained(
                #{max_retained_messages => 0}, emqx_message:make(test, 1, Topic, <<"v">>)
            )
        end,
        [<<"t/1/a">>, <<"t/2/a">>, <<"t/3/a">>]
    ).

connect(Props) ->
    ClientId = <<"retainer_dispatcher_lazy">>,
    Properties =
        case Props of
            #{receive_maximum := N} -> #{'Receive-Maximum' => N};
            #{} -> #{}
        end,
    {ok, Client} = emqtt:start_link([
        {clientid, ClientId}, {proto_ver, v5}, {auto_ack, false}, {properties, Properties}
    ]),
    {ok, _} = emqtt:connect(Client),
    [Channel] = emqx_cm:lookup_channels(ClientId),
    {Client, Channel}.

%% Acknowledge each received QoS 1 publish until N publishes are received.
ack_publishes(Client, N) ->
    ack_publishes(Client, N, []).

ack_publishes(_Client, 0, Acc) ->
    lists:reverse(Acc);
ack_publishes(Client, N, Acc) ->
    receive
        {publish, #{topic := Topic, packet_id := PacketId}} ->
            ok = emqtt:puback(Client, PacketId),
            ack_publishes(Client, N - 1, [Topic | Acc])
    after 2000 ->
        lists:reverse(Acc)
    end.

set_batch_read_number(N) ->
    emqx_config:put([retainer, flow_control, batch_read_number], N).

%% Start each case with workers that have no token debt from earlier reads.
restart_workers() ->
    ok = emqx_retainer_sup:stop_workers(),
    ok = emqx_retainer_sup:start_workers().

%% `emqx_retainer_limiter:update/1' takes the retainer config rather than
%% reading it back itself, the same way `emqx_retainer' passes it on a config
%% update, so these cases have to hand it the config they just put.
update_limiter() ->
    emqx_retainer_limiter:update(emqx:get_config([retainer])).

store(Topic) ->
    emqx_retainer_mnesia:store_retained(
        #{max_retained_messages => 0}, emqx_message:make(Topic, <<"v">>)
    ).

%% Send one request for `t/+/a' to the worker, from a live or a dead target.
fetch_request(Worker, live) ->
    Ref = make_ref(),
    Target = emqx_extsub_handler:message_target(Ref),
    ok = fetch_topic(Worker, Target),
    {live, Ref};
fetch_request(Worker, dead) ->
    {Dead, MRef} = spawn_monitor(fun() -> ok end),
    receive
        {'DOWN', MRef, process, Dead, _} -> ok
    end,
    ok = fetch_topic(Worker, {Dead, make_ref()}),
    dead.

fetch_topic(Worker, Target) ->
    Topic = <<"t/+/a">>,
    emqx_retainer_dispatcher:fetch(
        Worker, Target, make_ref(), Topic, emqx_topic:words(Topic), undefined
    ).

batch(Cursor) ->
    emqx_retainer_mnesia:match_messages(#{}, <<"t/+/a">>, Cursor, #{batch_read_number => 1}).

subscribe() ->
    subscribe(#{rh => 0}).

subscribe(SubOpts) ->
    subscribe(SubOpts, <<"t/+/a">>).

%% Create a handler as `emqx_retainer_channel' does when it hands over a read before the
%% first batch.
subscribe(SubOpts, Topic) ->
    Ref = make_ref(),
    Ctx = #{
        clientinfo => #{protocol => mqtt},
        subopts => SubOpts,
        message_target => emqx_extsub_handler:message_target(Ref),
        handover => {cursor, undefined}
    },
    {ok, Handler} = emqx_retainer_extsub_handler:handle_subscribe(
        handover, Ctx, undefined, Topic
    ),
    {Ref, Handler}.

info(Handler, Info, Demand) ->
    Ctx = #{desired_message_count => Demand, delivering_count => 0},
    emqx_retainer_extsub_handler:handle_info(Handler, Ctx, Info).

receive_info(Ref) ->
    receive
        #info_to_extsub{handler_ref = Ref, info = Info} -> Info
    after 5000 -> ct:fail(no_handler_message)
    end.

receive_batch(Ref) ->
    Info = receive_info(Ref),
    ?assertMatch(#retained_batch{}, Info),
    Info.

receive_any_batch() ->
    receive
        #info_to_extsub{handler_ref = Ref, info = #retained_batch{}} -> Ref
    after 5000 -> ct:fail(no_batch)
    end.

connect_with_suspended_dispatcher(ClientId) ->
    {ok, Client} = emqtt:start_link(#{clientid => ClientId, proto_ver => v5}),
    {ok, _} = emqtt:connect(Client),
    [Channel] = emqx_cm:lookup_channels(ClientId),
    Worker = gproc_pool:pick_worker(?DISPATCHER_POOL, Channel),
    ok = sys:suspend(Worker),
    emqx_common_test_helpers:on_exit(fun() -> catch sys:resume(Worker) end),
    {Client, Worker}.

receive_publishes(N) ->
    receive_publishes(N, []).

receive_publishes(0, Acc) ->
    lists:reverse(Acc);
receive_publishes(N, Acc) ->
    receive
        {publish, #{topic := Topic}} -> receive_publishes(N - 1, [Topic | Acc])
    after 1000 ->
        lists:reverse(Acc)
    end.

receive_down(Ref) ->
    receive
        {#extsub_down{handler_ref = Ref}, MRef, process, Pid, Reason} ->
            {'DOWN', MRef, process, Pid, Reason}
    after 5000 -> ct:fail(no_dispatcher_down)
    end.

assert_no_monitor(Worker) ->
    assert_no_monitor(self(), Worker).

%% A channel keeps no retainer state after its reads end.
assert_no_read_state(Channel) ->
    {dictionary, Dict} = process_info(Channel, dictionary),
    ?assertEqual(undefined, proplists:get_value(emqx_retainer_reads, Dict)),
    ?assertEqual(undefined, proplists:get_value(emqx_retainer_monitors, Dict)).

assert_no_monitor(Pid, Worker) ->
    {monitors, Monitors} = process_info(Pid, monitors),
    ?assertNot(lists:member({process, Worker}, Monitors)).
