%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_mq_batch_delete_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("emqx_utils/include/emqx_ds_dbs.hrl").

all() ->
    [{group, mq}].

groups() ->
    Cases = [
        t_gc_batches,
        t_gc_quota_cutoffs,
        t_gc_slab_isolation,
        t_gc_queue_retry,
        t_gc_queue_give_up,
        t_gc_slab_abort,
        t_drop_batches,
        t_drop_discovery_error,
        t_drop_batch_error,
        t_gc_zero_cutoff
    ],
    [{mq, [], Cases}, {streams, [], Cases}].

init_per_group(mq, Config) ->
    [
        {module, emqx_mq_message_db},
        {db, ?MQ_MESSAGE_LASTVALUE_DB},
        {regular_db, ?MQ_MESSAGE_REGULAR_DB},
        {worker, emqx_mq_gc_slab_worker},
        {taken_event, mq_gc_queue_taken},
        {sent_event, mq_gc_batch_sent},
        {event, mq_gc_batch_reply}
        | Config
    ];
init_per_group(streams, Config) ->
    [
        {module, emqx_streams_message_db},
        {db, ?STREAMS_MESSAGE_LASTVALUE_DB},
        {regular_db, ?STREAMS_MESSAGE_REGULAR_DB},
        {worker, emqx_streams_gc_slab_worker},
        {taken_event, streams_gc_queue_taken},
        {sent_event, streams_gc_batch_sent},
        {event, streams_gc_batch_reply}
        | Config
    ].

end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(TestCase, Config) ->
    Apps = emqx_cth_suite:start(
        [
            emqx_durable_storage,
            {emqx,
                emqx_mq_test_utils:cth_config(emqx, #{
                    <<"durable_storage">> => #{
                        <<"mq_messages">> => #{<<"n_shards">> => 2},
                        <<"streams_messages">> => #{<<"n_shards">> => 2}
                    }
                })},
            {emqx_mq, emqx_mq_test_utils:cth_config(emqx_mq)}
        ],
        #{work_dir => emqx_cth_suite:work_dir(TestCase, Config)}
    ),
    ok = snabbkaffe:start_trace(),
    [{apps, Apps} | Config].

end_per_testcase(_TestCase, Config) ->
    emqx_common_test_helpers:call_janitor(),
    snabbkaffe:stop(),
    emqx_cth_suite:stop(?config(apps, Config)).

-doc "GC drains multiple batches per shard and preserves newer data and neighboring queues".
t_gc_batches(Config) ->
    Event = ?config(event, Config),
    DB = ?config(db, Config),
    Queue0 = queue(true),
    Topic0 = message_topic(Queue0),
    Topic1 = message_topic(Queue0#{id => <<"neighbor">>}),
    Future0 = erlang:system_time(microsecond) + 3600_000_000,
    Shards0 = emqx_ds:list_shards(DB),
    %% Put more than one deletion batch into each shard's stream.
    lists:foreach(
        fun(Shard) ->
            seed(DB, Shard, 1, Topic0, lists:seq(1, 1001)),
            seed(DB, Shard, 1, Topic0, [Future0]),
            seed(DB, Shard, 1, Topic1, [1])
        end,
        Shards0
    ),
    ?check_trace(
        begin
            ok = run_gc(Config, [Queue0]),
            ?assertEqual(length(Shards0), length(emqx_ds:dirty_read(DB, Topic0))),
            ?assert(
                lists:all(fun({_, TS, _}) -> TS =:= Future0 end, emqx_ds:dirty_read(DB, Topic0))
            ),
            ?assertEqual(length(Shards0), length(emqx_ds:dirty_read(DB, Topic1)))
        end,
        fun(Trace) ->
            lists:foreach(
                fun(Shard) ->
                    Results0 = [
                        Result
                     || #{slab := {S, _}, result := Result} <- ?of_kind(Event, Trace),
                        S =:= Shard
                    ],
                    ?assert(length(Results0) > 1),
                    ?assertEqual({ok, false}, lists:last(Results0)),
                    ?assert(
                        lists:all(
                            fun(Result) -> Result =:= {ok, true} end,
                            lists:droplast(Results0)
                        )
                    )
                end,
                Shards0
            )
        end
    ).

-doc "GC uses each shard's quota cutoff and preserves records exactly at that cutoff".
t_gc_quota_cutoffs(Config) ->
    DB = ?config(db, Config),
    Queue0 = maps:merge(queue(true), #{
        data_retention_period => erlang:system_time(millisecond) + 3600_000,
        limits => #{max_shard_message_count => 1, max_shard_message_bytes => infinity}
    }),
    Topic0 = message_topic(Queue0),
    IndexTopic0 = index_topic(Queue0),
    IndexOpts0 = #{count => 1, bytes => infinity, threshold_percentage => 10},
    Shards0 = emqx_ds:list_shards(DB),
    %% Store a different quota deadline in each shard.
    Cutoffs0 = lists:map(
        fun({Shard, Scale}) ->
            Index0 = emqx_mq_quota_index:new(IndexOpts0, 0),
            Index1 = emqx_mq_quota_index:apply_updates(
                Index0,
                [{I * Scale * 1000_000, 0, 1} || I <- lists:seq(1, 20)]
            ),
            Cutoff0 = emqx_mq_quota_index:deadline(Index1),
            ?assert(Cutoff0 > 0),
            seed(DB, Shard, 1, Topic0, [Cutoff0 - 1, Cutoff0, Cutoff0 + 1]),
            ?assertMatch(
                {atomic, _, _},
                emqx_ds:trans(
                    #{db => DB, shard => Shard, generation => 1},
                    fun() ->
                        emqx_ds:tx_write({IndexTopic0, 1, emqx_mq_quota_index:encode(Index1)})
                    end
                )
            ),
            {Shard, Cutoff0}
        end,
        lists:zip(Shards0, lists:seq(1, length(Shards0)))
    ),
    ok = run_gc(Config, [Queue0]),
    lists:foreach(
        fun({Shard, Cutoff}) ->
            ?assertEqual(
                [{Topic0, Cutoff, <<>>}, {Topic0, Cutoff + 1, <<>>}],
                lists:sort(emqx_ds:dirty_read(#{db => DB, shard => Shard}, Topic0))
            )
        end,
        Cutoffs0
    ),
    ?assertEqual(length(Shards0), length(emqx_ds:dirty_read(DB, IndexTopic0))).

-doc "Drop drains every generation and shard, removes indexes, and preserves other queues and slabs".
t_drop_batches(Config) ->
    Mod = ?config(module, Config),
    lists:foreach(
        fun(#{db := DB, lastvalue := IsLastvalue}) ->
            Queue0 = queue(IsLastvalue),
            Topic0 = message_topic(Queue0),
            Topic1 = message_topic(Queue0#{id => <<"neighbor">>}),
            IndexTopic0 = index_topic(Queue0),
            IndexTopic1 = index_topic(Queue0#{id => <<"neighbor">>}),
            Shards0 = emqx_ds:list_shards(DB),
            seed_drop_generation(DB, Shards0, 1, [Topic0, Topic1, IndexTopic0, IndexTopic1]),
            ok = emqx_ds:add_generation(DB),
            seed_drop_generation(DB, Shards0, 2, [Topic0, Topic1, IndexTopic0, IndexTopic1]),
            Slabs0 = emqx_ds:list_slabs(DB),
            ?check_trace(
                begin
                    ok = Mod:drop(Queue0),
                    ?assertEqual([], emqx_ds:dirty_read(DB, Topic0)),
                    ?assertEqual(4, length(emqx_ds:dirty_read(DB, Topic1))),
                    case IsLastvalue of
                        true -> ?assertEqual([], emqx_ds:dirty_read(DB, IndexTopic0));
                        false -> ok
                    end,
                    ?assertEqual(4, length(emqx_ds:dirty_read(DB, IndexTopic1))),
                    ?assertEqual(Slabs0, emqx_ds:list_slabs(DB))
                end,
                fun(Trace) ->
                    Results0 = [
                        Result
                     || #{topic := Topic, result := Result} <- ?of_kind(
                            mq_queue_delete_batch, Trace
                        ),
                        Topic =:= message_filter(Queue0)
                    ],
                    ?assert(length([more || {ok, true} <- Results0]) >= 4),
                    ?assertEqual(4, length([done || {ok, false} <- Results0]))
                end
            ),
            ok = Mod:drop(Queue0)
        end,
        [
            #{db => ?config(db, Config), lastvalue => true},
            #{db => ?config(regular_db, Config), lastvalue => false}
        ]
    ).

-doc "A partial discovery failure returns before deleting any discovered streams".
t_drop_discovery_error(Config) ->
    Mod = ?config(module, Config),
    DB = ?config(db, Config),
    Queue0 = queue(true),
    Topic0 = message_topic(Queue0),
    [Shard0, Shard1] = emqx_ds:list_shards(DB),
    seed(DB, Shard0, 1, Topic0, [1]),
    seed(DB, Shard1, 1, Topic0, [1]),
    mock_ds(),
    ok = meck:expect(emqx_ds, get_streams, fun(DB0, Topic, Start, Opts) ->
        {Streams, []} = meck:passthrough([DB0, Topic, Start, Opts]),
        {[Entry || {{Shard, _}, _} = Entry <- Streams, Shard =:= Shard0], [
            {Shard1, {error, recoverable, test_unavailable}}
        ]}
    end),
    ?assertMatch({error, {stream_lookup, [_ | _]}}, Mod:drop(Queue0)),
    ok = meck:delete(emqx_ds, get_streams, 4),
    ?assertEqual(2, length(emqx_ds:dirty_read(DB, Topic0))),
    ok = Mod:drop(Queue0),
    ?assertEqual([], emqx_ds:dirty_read(DB, Topic0)).

-doc "A failure after a committed batch stops the loop and a later drop completes deletion".
t_drop_batch_error(Config) ->
    Mod = ?config(module, Config),
    DB = ?config(db, Config),
    Queue0 = queue(true),
    Topic0 = message_topic(Queue0),
    [Shard0 | _] = emqx_ds:list_shards(DB),
    seed(DB, Shard0, 1, Topic0, lists:seq(1, 1001)),
    mock_ds(),
    ok = meck:expect(emqx_ds, del_batch, fun(DB0, Stream, Topic, From, Until, Opts) ->
        case length(emqx_ds:dirty_read(DB0, Topic0)) of
            Count when Count < 1001 -> {error, unrecoverable, test_failure};
            _ -> meck:passthrough([DB0, Stream, Topic, From, Until, Opts])
        end
    end),
    ?assertMatch(
        {error, [_ | _]},
        Mod:drop(Queue0)
    ),
    Count0 = length(emqx_ds:dirty_read(DB, Topic0)),
    ?assert(Count0 > 0 andalso Count0 < 1001),
    ok = meck:delete(emqx_ds, del_batch, 6),
    ok = Mod:drop(Queue0),
    ?assertEqual([], emqx_ds:dirty_read(DB, Topic0)).

-doc "A zero retention cutoff leaves data intact and does not submit an empty deletion range".
t_gc_zero_cutoff(Config) ->
    DB = ?config(db, Config),
    Queue0 = maps:merge(queue(true), #{
        data_retention_period => erlang:system_time(millisecond) + 1000
    }),
    Topic0 = message_topic(Queue0),
    [Shard0 | _] = emqx_ds:list_shards(DB),
    seed(DB, Shard0, 1, Topic0, [0]),
    mock_ds(),
    ok = meck:expect(emqx_ds, del_batch, fun(_, _, _, _, _, _) -> error(empty_range) end),
    ok = run_gc(Config, [Queue0]),
    ?assertEqual([{Topic0, 0, <<>>}], emqx_ds:dirty_read(DB, Topic0)).

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

-doc "A failed slab does not prevent another slab worker from deleting expired data".
t_gc_slab_isolation(Config) ->
    Worker = ?config(worker, Config),
    DB = ?config(db, Config),
    Queue0 = queue(true),
    Topic0 = message_topic(Queue0),
    [Shard0, Shard1] = emqx_ds:list_shards(DB),
    seed(DB, Shard0, 1, Topic0, [1]),
    seed(DB, Shard1, 1, Topic0, lists:seq(1, 1001)),
    mock_ds(),
    meck:expect(emqx_ds, get_streams, fun(DB0, Topic, From, #{shard := Shard} = Opts) ->
        case Shard of
            Shard0 -> {[], [{Shard0, {error, recoverable, unavailable}}]};
            _ -> meck:passthrough([DB0, Topic, From, Opts])
        end
    end),
    Workers0 = [
        begin
            {ok, Pid} = Worker:start_link({Shard, 1}, [Queue0]),
            Pid
        end
     || Shard <- [Shard0, Shard1]
    ],
    Refs0 = [monitor(process, Pid) || Pid <- Workers0],
    lists:foreach(
        fun(Ref) ->
            receive
                {'DOWN', Ref, process, _, normal} -> ok
            after 30000 -> error(worker_stalled)
            end
        end,
        Refs0
    ),
    ?assertEqual(1, length(emqx_ds:dirty_read(DB, Topic0))).

queue(IsLastvalue) ->
    #{
        id => <<"target">>,
        topic_filter => <<"batch/#">>,
        is_lastvalue => IsLastvalue,
        data_retention_period => 0,
        limits => #{max_shard_message_count => infinity, max_shard_message_bytes => infinity}
    }.

-doc "A failed request is retried for that queue only, and the other queues keep going".
t_gc_queue_retry(Config) ->
    DB = ?config(db, Config),
    SentEvent = ?config(sent_event, Config),
    ReplyEvent = ?config(event, Config),
    [Shard0 | _] = emqx_ds:list_shards(DB),
    Base0 = queue(true),
    Queues0 = [Base0#{id := integer_to_binary(N)} || N <- lists:seq(1, 12)],
    [seed(DB, Shard0, 1, message_topic(Q), [1]) || Q <- Queues0],
    Counter0 = atomics:new(1, []),
    mock_ds(),
    %% Fail only the first request.
    meck:expect(emqx_ds, del_batch, fun(DB0, Stream, Topic, From, To, Opts) ->
        case atomics:add_get(Counter0, 1, 1) of
            1 -> {error, recoverable, unavailable};
            _ -> meck:passthrough([DB0, Stream, Topic, From, To, Opts])
        end
    end),
    ?check_trace(
        ok = run_gc(Config, [Shard0], Queues0),
        fun(Trace) ->
            %% The failed queue is retried once, after the retry interval.
            ?assertMatch([#{failures := 1}], ?of_kind(retry_event(Config), Trace)),
            %% At most ten queues are in deletion at a time. The failed queue holds one of the
            %% ten places while it waits for its retry, without a request in flight.
            {0, Max0} = lists:foldl(
                fun
                    (#{?snk_kind := Kind}, {N, Max}) when Kind =:= SentEvent ->
                        {N + 1, max(Max, N + 1)};
                    (#{?snk_kind := Kind}, {N, Max}) when Kind =:= ReplyEvent -> {N - 1, Max};
                    (_, Acc) ->
                        Acc
                end,
                {0, 0},
                Trace
            ),
            ?assert(Max0 >= 9 andalso Max0 =< 10, Max0)
        end
    ),
    ?assertEqual([], emqx_ds:dirty_read(DB, [<<"topic">>, <<"batch/#">>, '#'])).

-doc "A queue whose requests keep failing is given up after three failures".
t_gc_queue_give_up(Config) ->
    DB = ?config(db, Config),
    [Shard0 | _] = emqx_ds:list_shards(DB),
    Base0 = queue(true),
    Bad0 = Base0#{id := <<"bad">>},
    Good0 = Base0#{id := <<"good">>},
    seed(DB, Shard0, 1, message_topic(Bad0), [1]),
    seed(DB, Shard0, 1, message_topic(Good0), [1]),
    mock_ds(),
    BadTopic0 = message_filter(Bad0),
    %% Fail every request for one queue with a queue-specific error.
    meck:expect(emqx_ds, del_batch, fun(DB0, Stream, Topic, From, To, Opts) ->
        case Topic of
            BadTopic0 -> {error, unrecoverable, test_failure};
            _ -> meck:passthrough([DB0, Stream, Topic, From, To, Opts])
        end
    end),
    ?check_trace(
        ok = run_gc(Config, [Shard0], [Bad0, Good0]),
        fun(Trace) ->
            ?assertMatch([_], ?of_kind(failed_event(Config), Trace)),
            ?assertEqual([], ?of_kind(slab_failed_event(Config), Trace))
        end
    ),
    ?assertEqual(1, length(emqx_ds:dirty_read(DB, message_topic(Bad0)))),
    ?assertEqual([], emqx_ds:dirty_read(DB, message_topic(Good0))).

-doc "The worker stops after five shard failures, even when other requests succeed".
t_gc_slab_abort(Config) ->
    DB = ?config(db, Config),
    [Shard0 | _] = emqx_ds:list_shards(DB),
    Base0 = queue(true),
    Good0 = Base0#{id := <<"good">>},
    %% Three failing queues reach five shard failures on their second attempt, which is
    %% after the good queue's request has succeeded and before any queue is given up.
    Bad0 = [Base0#{id := integer_to_binary(N)} || N <- lists:seq(1, 3)],
    [seed(DB, Shard0, 1, message_topic(Q), [1]) || Q <- [Good0 | Bad0]],
    GoodTopic0 = message_filter(Good0),
    mock_ds(),
    %% Requests for every queue except one report the shard as unavailable.
    meck:expect(emqx_ds, del_batch, fun(DB0, Stream, Topic, From, To, Opts) ->
        case Topic of
            GoodTopic0 -> meck:passthrough([DB0, Stream, Topic, From, To, Opts]);
            _ -> {error, recoverable, unavailable}
        end
    end),
    ?check_trace(
        ok = run_gc(Config, [Shard0], [Good0 | Bad0]),
        fun(Trace) ->
            ?assertMatch([_], ?of_kind(slab_failed_event(Config), Trace)),
            ?assertEqual([], ?of_kind(failed_event(Config), Trace))
        end
    ),
    ?assertEqual([], emqx_ds:dirty_read(DB, message_topic(Good0))),
    ?assertEqual(3, length(lists:append([emqx_ds:dirty_read(DB, message_topic(Q)) || Q <- Bad0]))).

message_topic(#{topic_filter := Filter, id := Id}) ->
    [<<"topic">>, Filter, Id, <<"key">>, <<"key">>].

message_filter(#{topic_filter := Filter, id := Id}) ->
    [<<"topic">>, Filter, Id, <<"key">>, '#'].

index_topic(#{topic_filter := Filter, id := Id}) ->
    [<<"topic">>, Filter, Id, <<"index">>].

seed_drop_generation(DB, Shards, Generation, [Topic, NeighborTopic, IndexTopic, NeighborIndexTopic]) ->
    lists:foreach(
        fun(Shard) ->
            seed(DB, Shard, Generation, Topic, lists:seq(1, 1001)),
            lists:foreach(
                fun(T) -> seed(DB, Shard, Generation, T, [1]) end,
                [NeighborTopic, IndexTopic, NeighborIndexTopic]
            )
        end,
        Shards
    ).

seed(_DB, _Shard, _Generation, _Topic, []) ->
    ok;
seed(DB, Shard, Generation, Topic, Timestamps0) ->
    {Batch, Timestamps} = lists:split(min(100, length(Timestamps0)), Timestamps0),
    ?assertMatch(
        {atomic, _, ok},
        emqx_ds:trans(
            #{db => DB, shard => Shard, generation => Generation},
            fun() -> lists:foreach(fun(TS) -> emqx_ds:tx_write({Topic, TS, <<>>}) end, Batch) end
        )
    ),
    seed(DB, Shard, Generation, Topic, Timestamps).

retry_event(Config) -> event_name(Config, gc_queue_retry).

failed_event(Config) -> event_name(Config, gc_queue_failed).

slab_failed_event(Config) -> event_name(Config, gc_slab_failed).

event_name(Config, Suffix) ->
    Prefix =
        case ?config(worker, Config) of
            emqx_mq_gc_slab_worker -> "mq_";
            emqx_streams_gc_slab_worker -> "streams_"
        end,
    list_to_atom(Prefix ++ atom_to_list(Suffix)).

%% Run the GC slab workers for an explicit list of queues and wait for them to stop.
run_gc(Config, Queues) ->
    run_gc(Config, emqx_ds:list_shards(?config(db, Config)), Queues).

run_gc(Config, Shards, Queues) ->
    Worker = ?config(worker, Config),
    Refs = [
        begin
            {ok, Pid} = Worker:start_link({Shard, 1}, Queues),
            unlink(Pid),
            monitor(process, Pid)
        end
     || Shard <- Shards
    ],
    lists:foreach(
        fun(Ref) ->
            %% A worker with no work can stop before the monitor is set: `noproc`.
            receive
                {'DOWN', Ref, process, _, Reason} when Reason =:= normal; Reason =:= noproc -> ok
            after 30000 -> error(gc_worker_stalled)
            end
        end,
        Refs
    ).

mock_ds() ->
    ok = meck:new(emqx_ds, [passthrough, no_link]),
    emqx_common_test_helpers:on_exit(fun() -> meck:unload(emqx_ds) end).
