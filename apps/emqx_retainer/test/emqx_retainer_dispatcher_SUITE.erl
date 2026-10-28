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
    ok = emqx_retainer_limiter:update(),
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
    ?assertEqual({ok, [], undefined, 1}, batch(Cursor0)).

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
            ?assertEqual({ok, [], undefined, 1}, batch(Cursor2))
    after 1000 ->
        ct:fail(no_refill_cursor)
    end.

-doc "A wildcard batch waits for the dispatcher and does not read in the channel.".
t_async_fetch_and_demand(_Config) ->
    store_messages(),
    emqx_config:put([retainer, flow_control, batch_read_number], 1),
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
    {ok, Handler3, [_]} = info(Handler2, receive_batch(Ref), 1),
    {ok, Handler4, [_]} = info(Handler3, receive_batch(Ref), 1),
    ?assertMatch({destroy, [_]}, info(Handler4, receive_batch(Ref), 1)),
    ok = emqx_retainer_extsub_handler:handle_terminate(Handler4),
    assert_no_monitor(Worker).

-doc "A handler retries an outstanding request after its dispatcher dies.".
t_dispatcher_death(_Config) ->
    store_messages(),
    emqx_config:put([retainer, flow_control, batch_read_number], 1),
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

-doc "Empty wildcard reads consume tokens and resume when the limiter allows reads.".
t_empty_read_limiter(_Config) ->
    emqx_config:put([retainer, flow_control, batch_deliver_limiter], {2, 10000}),
    ok = emqx_retainer_limiter:update(),
    ok = emqx_retainer_sup:stop_workers(),
    ok = emqx_retainer_sup:start_workers(),
    {Ref0, Handler0} = subscribe(),
    ?assertMatch({destroy, [_]}, info(Handler0, receive_batch(Ref0), 1)),
    {Ref1, Handler1} = subscribe(),
    ?assertMatch({destroy, [_]}, info(Handler1, receive_batch(Ref1), 1)),
    {Ref2, Handler2} = subscribe(),
    ?assertNotReceive(#info_to_extsub{handler_ref = Ref2}, 200),
    %% A barrier must complete even when requests are paused by the limiter.
    ok = emqx_retainer_dispatcher:wait_dispatch_complete(1000),
    emqx_config:put([retainer, flow_control, batch_deliver_limiter], infinity),
    ok = emqx_retainer_limiter:update(),
    ?assertMatch({destroy, [_]}, info(Handler2, receive_batch(Ref2), 1)),
    lists:foreach(fun emqx_retainer_extsub_handler:handle_terminate/1, [
        Handler0, Handler1, Handler2
    ]).

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
    emqx_config:put([retainer, flow_control, batch_read_number], 1),
    {Ref, Handler0} = subscribe(#{rh => 0, emqx_retainer_extsub_handler => #{delivered => 1}}),
    {ok, Handler1, [#message{topic = <<"t/1/a">>}]} = info(Handler0, receive_batch(Ref), 1),
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
                {ok, [], undefined, 1},
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
                    ?assert(Cost >= max(1, length(Messages))),
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

-doc "Unlimited exact-topic reads bypass the dispatcher and finish after acknowledgement.".
t_exact_unlimited(_Config) ->
    store_messages(),
    Worker = emqx_retainer_dispatcher:worker(),
    ok = sys:suspend(Worker),
    emqx_common_test_helpers:on_exit(fun() -> catch sys:resume(Worker) end),
    {Ref, Handler0} = subscribe(#{rh => 0}, <<"t/1/a">>),
    {ok, Handler1, [Msg]} = info(Handler0, receive_info(Ref), 1),
    ?assertEqual(<<"t/1/a">>, Msg#message.topic),
    assert_no_monitor(Worker),
    AckCtx = #{desired_message_count => 1, delivering_count => 0, unacked_count => 0},
    ?assertEqual(
        {destroy, [<<"t/1/a">>]},
        emqx_retainer_extsub_handler:handle_delivered(Handler1, AckCtx, Msg, ack)
    ),
    ok = emqx_retainer_extsub_handler:handle_terminate(Handler1).

-doc "Limited exact-topic reads use the dispatcher queue alongside wildcard reads.".
t_exact_limited(_Config) ->
    store_messages(),
    emqx_config:put([retainer, flow_control, batch_read_number], 1),
    emqx_config:put([retainer, flow_control, batch_deliver_limiter], {2, 10000}),
    ok = emqx_retainer_limiter:update(),
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
    ok = emqx_retainer_limiter:update(),
    {ok, Handler5, [#message{topic = <<"t/2/a">>}]} = info(Handler2, receive_batch(Ref2), 0),
    lists:foreach(fun emqx_retainer_extsub_handler:handle_terminate/1, [
        Handler3, Handler4, Handler5
    ]).

store_messages() ->
    lists:foreach(fun store/1, [<<"t/1/a">>, <<"t/2/a">>, <<"t/3/a">>]).

store(Topic) ->
    emqx_retainer_mnesia:store_retained(
        #{max_retained_messages => 0}, emqx_message:make(Topic, <<"v">>)
    ).

batch(Cursor) ->
    emqx_retainer_mnesia:match_messages(#{}, <<"t/+/a">>, Cursor, #{batch_read_number => 1}).

subscribe() ->
    subscribe(#{rh => 0}).

subscribe(SubOpts) ->
    subscribe(SubOpts, <<"t/+/a">>).

subscribe(SubOpts, Topic) ->
    Ref = make_ref(),
    Ctx = #{
        clientinfo => #{protocol => mqtt},
        subopts => SubOpts,
        message_target => emqx_extsub_handler:message_target(Ref)
    },
    {ok, Handler} = emqx_retainer_extsub_handler:handle_subscribe(
        subscribe, Ctx, undefined, Topic
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
    ?assertMatch({retained_batch, _, _}, Info),
    Info.

receive_down(Ref) ->
    receive
        {#extsub_down{handler_ref = Ref}, MRef, process, Pid, Reason} ->
            {'DOWN', MRef, process, Pid, Reason}
    after 5000 -> ct:fail(no_dispatcher_down)
    end.

assert_no_monitor(Worker) ->
    {monitors, Monitors} = process_info(self(), monitors),
    ?assertNot(lists:member({process, Worker}, Monitors)).
