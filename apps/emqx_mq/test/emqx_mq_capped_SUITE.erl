%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mq_capped_SUITE).
-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("emqx_durable_storage/include/emqx_ds.hrl").
-include_lib("emqx/include/asserts.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include("../src/emqx_mq_internal.hrl").
-include("../gen_src/MessageQueue.hrl").

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [
            emqx_durable_storage,
            {emqx,
                emqx_mq_test_utils:cth_config(emqx, #{
                    <<"durable_storage">> => #{<<"mq_messages">> => #{<<"n_shards">> => 1}}
                })},
            {emqx_mq, emqx_mq_test_utils:cth_config(emqx_mq)}
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{suite_apps, Apps} | Config].

end_per_suite(Config) ->
    emqx_cth_suite:stop(?config(suite_apps, Config)).

init_per_testcase(_, Config) ->
    ok = emqx_mq_test_utils:cleanup_mqs(),
    ok = snabbkaffe:start_trace(),
    Config.

end_per_testcase(_, _) ->
    emqx_common_test_helpers:call_janitor(),
    meck:unload(),
    snabbkaffe:stop(),
    emqx_mq_test_utils:cleanup_mqs().

%% Check that count-one admission resumes after a completely acknowledged buffer retires.
t_consume_releases_count_one(_Config) ->
    MQ = queue(#{max_shard_message_count => 1}),
    ok = fill(MQ, [<<"first">>, <<"second">>]),
    ?assertEqual({error, queue_full}, emqx_mq_message_db:insert(MQ, message(<<"third">>))),
    C = subscriber(),
    {ok, Received} = emqx_mq_test_utils:emqtt_drain(2, 1000),
    ?assertEqual([<<"first">>, <<"second">>], [P || #{payload := P} <- Received]),
    ?assertMatch({ok, #{count := 2}}, occupancy(MQ)),
    ok = emqx_mq_test_utils:emqtt_ack(Received),
    ?retry(50, 100, ?assertMatch({ok, #{buckets := 1}}, occupancy(MQ))),
    ok = emqx_mq_message_db:insert(MQ, message(<<"third">>)),
    {ok, [Received1]} = emqx_mq_test_utils:emqtt_drain(1, 1000),
    ?assertMatch(#{payload := <<"third">>}, Received1),
    ok = emqtt:disconnect(C).

%% Check count, byte, and combined admission without modifying existing records on rejection.
t_admission_limits(_Config) ->
    Message = message(<<"value">>),
    Size = byte_size(emqx_mq_message_db:encode_message(Message)),
    lists:foreach(
        fun(Limits) ->
            MQ = queue(Limits),
            ?assertMatch({ok, #{count := 0, bytes := 0}}, occupancy(MQ)),
            ok = fill(MQ, [<<"value">>, <<"value">>]),
            ?assertMatch({ok, #{buckets := 2}}, occupancy(MQ)),
            Records = emqx_mq_message_db:dirty_read_all(MQ),
            ?assertEqual({error, queue_full}, emqx_mq_message_db:insert(MQ, Message)),
            ?assertEqual(Records, emqx_mq_message_db:dirty_read_all(MQ)),
            ok = emqx_mq_registry:delete(<<"capped">>)
        end,
        [
            #{max_shard_message_count => 1},
            #{max_shard_message_bytes => Size},
            #{max_shard_message_count => 2, max_shard_message_bytes => Size}
        ]
    ).

%% Check that last-value admission does not credit the previous value and cleanup deletes consumed values.
t_lastvalue_replacement(_Config) ->
    MQ = queue(#{max_shard_message_count => 1}, #{
        is_lastvalue => true,
        key_expression => <<"message.from">>
    }),
    %% The second value is admitted into the single bucket and replaces the first one
    ok = fill(MQ, [<<"old">>, <<"mid">>]),
    ?assertMatch({ok, #{buckets := 2}}, occupancy(MQ)),
    ?assertEqual({error, queue_full}, emqx_mq_message_db:insert(MQ, message(<<"new">>))),
    ?assertEqual([<<"mid">>], stored_payloads(MQ)),
    C = subscriber(),
    {ok, Received} = emqx_mq_test_utils:emqtt_drain(1, 1000),
    ok = emqx_mq_test_utils:emqtt_ack(Received),
    ?retry(50, 100, ?assertMatch({ok, #{buckets := 1}}, occupancy(MQ))),
    Message1 = message(<<"new">>),
    ok = emqx_mq_message_db:insert(MQ, Message1#message{from = <<"other">>}),
    ok = flush(),
    {ok, Received1} = emqx_mq_test_utils:emqtt_drain(1, 1000),
    ok = emqx_mq_test_utils:emqtt_ack(Received1),
    ?retry(50, 100, ?assertNot(lists:member(<<"mid">>, stored_payloads(MQ)))),
    ok = emqtt:disconnect(C).

%% Check that a failed occupancy read returns a storage error rather than admitting a write.
t_failed_index_read(_Config) ->
    MQ = queue(#{max_shard_message_count => 1}),
    meck:new(emqx_ds, [passthrough, no_link]),
    meck:expect(emqx_ds, dirty_read, fun(_, _) -> error(index_unavailable) end),
    ?assertMatch(
        {error, {recoverable, {quota_index_read_failed, error, index_unavailable}}},
        emqx_mq_message_db:insert(MQ, message(<<"value">>))
    ),
    meck:unload(emqx_ds),
    ?assertEqual([], emqx_mq_message_db:dirty_read_all(MQ)).

%% Check that stale low estimates allow overshoot without capacity GC and expiration releases quota.
t_overshoot_and_retention(_Config) ->
    MQ = queue(#{max_shard_message_count => 1}),
    meck:new(emqx_mq_message_db, [passthrough, no_link]),
    meck:expect(emqx_mq_message_db, occupancy, fun(_, _) -> {ok, #{count => 0, bytes => 0}} end),
    lists:foreach(
        fun(N) ->
            ok = emqx_mq_message_db:insert(MQ, message(integer_to_binary(N)))
        end,
        lists:seq(1, 4)
    ),
    ok = flush(),
    meck:unload(emqx_mq_message_db),
    ?assertMatch({ok, #{count := 4, buckets := 1}}, occupancy(MQ)),
    %% The single bucket admits another write, and its flush closes the bucket
    ok = fill(MQ, [<<"5">>]),
    ?assertMatch({ok, #{count := 5, buckets := 2}}, occupancy(MQ)),
    Records = emqx_mq_message_db:dirty_read_all(MQ),
    ok = gc([MQ]),
    ?assertEqual(Records, emqx_mq_message_db:dirty_read_all(MQ)),
    ok = gc([MQ#{data_retention_period := 0}]),
    ?assertEqual([], emqx_mq_message_db:dirty_read_all(MQ)),
    %% The closed bucket is released. The index keeps its newest bucket
    ?retry(50, 100, ?assertMatch({ok, #{count := 1, buckets := 1}}, occupancy(MQ))).

-doc "Check that slab GC processes multiple reject queues without dropping unexpired data".
t_gc_batches_reject_queues(_Config) ->
    MQ0 = queue(#{max_shard_message_count => 1}),
    MQ1 = queue(#{max_shard_message_count => 1}, #{name => <<"other">>}),
    MQ2 = queue(#{max_shard_message_count => 1}, #{name => <<"empty">>}),
    ok = emqx_mq_message_db:insert(MQ0, message(<<"first">>)),
    ok = emqx_mq_message_db:insert(MQ1, message(<<"second">>)),
    ok = flush(),
    ?check_trace(
        ok = gc([MQ0, MQ1, MQ2]),
        fun(Trace) ->
            ?assertEqual(3, length(?of_kind(mq_gc_queue_taken, Trace))),
            ?assertEqual([], ?of_kind(mq_gc_queue_failed, Trace))
        end
    ),
    ?assertEqual([<<"first">>], stored_payloads(MQ0)),
    ?assertEqual([<<"second">>], stored_payloads(MQ1)).

%% Check that a failed retention release on one queue does not block another queue, and is retried.
t_retention_conflict_isolation(_Config) ->
    #{id := Id0} = MQ0 = queue(#{max_shard_message_count => 1}),
    MQ1 = queue(#{max_shard_message_count => 1}, #{name => <<"other">>}),
    ok = fill(MQ0, [<<"first">>, <<"second">>]),
    ok = fill(MQ1, [<<"third">>, <<"fourth">>]),
    meck:new(emqx_mq_cleanup_worker, [passthrough, no_link]),
    meck:expect(emqx_mq_cleanup_worker, retry_interval, fun() -> 100 end),
    meck:new(emqx_mq_message_db, [passthrough, no_link]),
    meck:expect(emqx_mq_message_db, release_before, fun
        (#{id := Id}, _, _) when Id =:= Id0 -> {error, {recoverable, injected_conflict}};
        (MQ, Shard, Boundary) -> meck:passthrough([MQ, Shard, Boundary])
    end),
    ok = gc([
        MQ0#{data_retention_period := 0}, MQ1#{data_retention_period := 0}
    ]),
    ?assertEqual([], stored_payloads(MQ0)),
    ?assertEqual([], stored_payloads(MQ1)),
    %% The closed bucket is released. The index keeps its newest bucket
    ?retry(50, 100, ?assertMatch({ok, #{count := 1, buckets := 1}}, occupancy(MQ1))),
    {ok, _} = ?block_until(#{?snk_kind := mq_cleanup_retry}, 5000, infinity),
    ?assertMatch({ok, #{count := 2, buckets := 2}}, occupancy(MQ0)),
    meck:unload(emqx_mq_message_db),
    ?retry(50, 100, ?assertMatch({ok, #{count := 1, buckets := 1}}, occupancy(MQ0))).

%% Check that GC reports expiry only when its cutoff reaches the end of the oldest bucket.
t_gc_reports_whole_buckets(_Config) ->
    MQ = queue(#{max_shard_message_count => 10}),
    ok = fill(MQ, [<<"first">>]),
    %% One bucket has no end to reach
    ok = gc([MQ#{data_retention_period := 0}]),
    ?assertEqual([], ?of_kind(mq_cleanup_report, snabbkaffe:collect_trace(0))),
    ok = fill(MQ, [<<"second">>, <<"third">>]),
    ?assertMatch({ok, #{buckets := 3}}, occupancy(MQ)),
    %% The default retention cutoff is inside the oldest bucket
    ok = gc([MQ]),
    ?assertEqual([], ?of_kind(mq_cleanup_report, snabbkaffe:collect_trace(0))),
    ?assertWaitEvent(
        ok = gc([MQ#{data_retention_period := 0}]),
        #{?snk_kind := mq_cleanup_released},
        5000
    ).

%% Check that a stale high estimate can reject a write until the next occupancy read refreshes it.
t_stale_high_admission(_Config) ->
    MQ = queue(#{max_shard_message_count => 1}),
    meck:new(emqx_mq_message_db, [passthrough, no_link]),
    meck:expect(emqx_mq_message_db, occupancy, fun(_, _) -> {ok, #{count => 1, bytes => 0}} end),
    ?assertEqual({error, queue_full}, emqx_mq_message_db:insert(MQ, message(<<"value">>))),
    ?assertEqual([], stored_payloads(MQ)),
    meck:unload(emqx_mq_message_db),
    ok = emqx_mq_message_db:insert(MQ, message(<<"value">>)).

%% Check that consumption before the quota flush does not block the queue.
t_delayed_quota_flush(_Config) ->
    MQ = queue(#{max_shard_message_count => 1}),
    meck:new(emqx_mq_message_db, [passthrough, no_link]),
    meck:expect(emqx_mq_message_db, quota_buffer_flush_interval, fun() -> 60000 end),
    C = subscriber(),
    ok = emqx_mq_message_db:insert(MQ, message(<<"first">>)),
    {ok, Received} = emqx_mq_test_utils:emqtt_drain(1, 1000),
    %% The consumer reports before the quota of the record is flushed, so nothing is released
    ?assertWaitEvent(
        emqx_mq_test_utils:emqtt_ack(Received),
        #{?snk_kind := mq_cleanup_refreshed},
        5000
    ),
    ok = flush(),
    ?assertMatch({ok, #{count := 1, buckets := 1}}, occupancy(MQ)),
    %% The single bucket admits the next message. Its consumption releases the first bucket
    ok = emqx_mq_message_db:insert(MQ, message(<<"second">>)),
    ok = flush(),
    {ok, Received1} = emqx_mq_test_utils:emqtt_drain(1, 1000),
    ?assertWaitEvent(
        emqx_mq_test_utils:emqtt_ack(Received1),
        #{?snk_kind := mq_cleanup_released},
        5000
    ),
    ?assertMatch({ok, #{count := 1, buckets := 1}}, occupancy(MQ)),
    ?assertEqual([], stored_payloads(MQ)),
    ok = emqtt:disconnect(C).

%% Check that failed deletion of consumed records keeps their quota until a retry deletes them.
t_failed_cleanup_deletion(_Config) ->
    MQ = queue(#{max_shard_message_count => 2}),
    ok = fill(MQ, [<<"first">>, <<"second">>]),
    meck:new(emqx_mq_ds_queue_deleter, [passthrough, no_link]),
    meck:expect(emqx_mq_ds_queue_deleter, delete, fun(_, _, _) -> {error, injected} end),
    meck:new(emqx_mq_cleanup_worker, [passthrough, no_link]),
    meck:expect(emqx_mq_cleanup_worker, retry_interval, fun() -> 100 end),
    C = subscriber(),
    {ok, Received} = emqx_mq_test_utils:emqtt_drain(2, 1000),
    ?assertWaitEvent(
        emqx_mq_test_utils:emqtt_ack(Received), #{?snk_kind := mq_cleanup_retry}, 5000
    ),
    %% The consumed records stay on disk, so their quota stays charged
    ?assertMatch([_, _], emqx_mq_message_db:dirty_read_all(MQ)),
    ?assertMatch({ok, #{count := 2, buckets := 2}}, occupancy(MQ)),
    ?assertEqual({error, queue_full}, emqx_mq_message_db:insert(MQ, message(<<"third">>))),
    %% A retry after the deletion recovers releases the closed bucket
    meck:unload(emqx_mq_ds_queue_deleter),
    ?retry(50, 100, ?assertMatch({ok, #{count := 1, buckets := 1}}, occupancy(MQ))),
    ?assertEqual([], emqx_mq_message_db:dirty_read_all(MQ)),
    ok = emqx_mq_message_db:insert(MQ, message(<<"third">>)),
    ok = emqtt:disconnect(C).

%% Check MQTT 5 direct rejection for QoS 1/2 and metrics for QoS 0 without ordinary-topic feedback.
t_publisher_feedback(_Config) ->
    MQ = queue(#{max_shard_message_count => 1}),
    Other = emqx_mq_test_utils:ensure_mq_created(#{
        name => <<"other">>, topic_filter => <<"capped/#">>
    }),
    ok = fill(MQ, [<<"first">>, <<"second">>]),
    C = emqx_mq_test_utils:emqtt_connect([]),
    #{queue_full := Before} = emqx_mq_metrics:get_counters(regular_limited),
    lists:foreach(
        fun(QoS) ->
            ?assertMatch(
                {ok, #{reason_code := ?RC_QUOTA_EXCEEDED}},
                emqtt:publish(C, <<"$queue/capped">>, <<"rejected">>, QoS)
            )
        end,
        [1, 2]
    ),
    ok = emqtt:publish(C, <<"$queue/capped">>, <<"rejected">>, 0),
    {ok, _, _} = emqtt:subscribe(C, {<<"capped/message">>, 1}),
    ?assertMatch(
        {ok, #{reason_code := ?RC_SUCCESS}},
        emqtt:publish(C, <<"capped/message">>, <<"routed">>, 1)
    ),
    {ok, [#{payload := <<"routed">>}]} = emqx_mq_test_utils:emqtt_drain(1, 1000),
    ?retry(50, 100, ?assertMatch([_], emqx_mq_message_db:dirty_read_all(Other))),
    ?assertMatch([_, _], emqx_mq_message_db:dirty_read_all(MQ)),
    ?assertMatch(
        #{queue_full := Count} when Count =:= Before + 4,
        emqx_mq_metrics:get_counters(regular_limited)
    ),
    ok = emqtt:disconnect(C).

%% Check policy defaults, immutable updates, finite limits, persistence, and the cluster capability gate.
t_policy_configuration(_Config) ->
    MQ0 = emqx_mq_config:mq_from_raw_post(#{
        <<"name">> => <<"default">>, <<"topic_filter">> => <<"t">>
    }),
    ?assertMatch(#{limits := #{on_full := evict}}, MQ0),
    MQ1 = queue(#{max_shard_message_count => 1}),
    ?assertMatch({ok, #{limits := #{on_full := reject}}}, emqx_mq_registry:find(<<"capped">>)),
    #{limits := Limits0} = MQ1,
    ?assertEqual(
        {error, on_full_not_allowed_to_be_updated},
        emqx_mq_registry:update(<<"capped">>, MQ1#{limits := maps:remove(on_full, Limits0)})
    ),
    ?assertMatch(
        {ok, #{limits := #{on_full := reject, max_shard_message_count := 2}}},
        emqx_mq_registry:update(<<"capped">>, MQ1#{limits := Limits0#{max_shard_message_count := 2}})
    ),
    ?assertEqual(
        {error, on_full_reject_requires_finite_limit},
        emqx_mq_registry:update(<<"capped">>, MQ1#{
            limits := Limits0#{max_shard_message_count := infinity}
        })
    ),
    ?assertException(
        throw,
        _,
        emqx_mq_config:mq_from_raw_post(#{
            <<"name">> => <<"invalid">>,
            <<"topic_filter">> => <<"t">>,
            <<"limits">> => #{<<"on_full">> => <<"reject">>}
        })
    ),
    ok = emqx_mq_test_utils:stub_bpapi_supported_version(emqx_mq_consumer, 1),
    ?assertEqual(
        {error, cluster_upgrade_in_progress},
        emqx_mq_registry:create(MQ1#{name := <<"old_cluster">>})
    ).

%% Check that unknown messages leave the cleanup worker running with the same PID.
t_cleanup_unknown_message(_Config) ->
    #{id := Id} = queue(#{max_shard_message_count => 1}),
    Worker = gproc_pool:pick_worker(emqx_mq_cleanup_worker, Id),
    Worker ! unknown_message,
    gen_server:cast(Worker, unknown_request),
    ?assertEqual({error, unknown_request}, gen_server:call(Worker, synchronize)),
    ?assertEqual(Worker, gproc_pool:pick_worker(emqx_mq_cleanup_worker, Id)).

%% Check that a consumed head stays charged, its records are deleted, and repeated reports are ignored.
t_unclosable_head(_Config) ->
    MQ = queue(#{max_shard_message_count => 100}),
    ok = emqx_mq_message_db:insert(MQ, message(<<"value">>)),
    ok = flush(),
    C = subscriber(),
    {ok, Received} = emqx_mq_test_utils:emqtt_drain(1, 1000),
    ?assertWaitEvent(
        emqx_mq_test_utils:emqtt_ack(Received),
        #{?snk_kind := mq_cleanup_released},
        5000
    ),
    ?assertMatch({ok, #{count := 1, buckets := 1}}, occupancy(MQ)),
    ?assertEqual([], stored_payloads(MQ)),
    %% The consumer repeats its boundary at each persistence flush
    {ok, _} = ?block_until(#{?snk_kind := mq_cleanup_report_ignored}, 5000, infinity),
    ?assertEqual(timeout, ?block_until(#{?snk_kind := mq_cleanup_refreshed}, 3000, 0)),
    ok = emqtt:disconnect(C).

%% Check that consecutive consumption of a count-one queue keeps the queue open.
%% Each consumed record is deleted. Its bucket is released when the next record is consumed.
t_consecutive_consumption(_Config) ->
    MQ = queue(#{max_shard_message_count => 1}),
    C = subscriber(),
    lists:foreach(
        fun(Payload) ->
            ok = emqx_mq_message_db:insert(MQ, message(Payload)),
            ok = flush(),
            {ok, Received} = emqx_mq_test_utils:emqtt_drain(1, 1000),
            ?assertWaitEvent(
                emqx_mq_test_utils:emqtt_ack(Received),
                #{?snk_kind := mq_cleanup_released},
                5000
            ),
            ?assertMatch({ok, #{count := 1, buckets := 1}}, occupancy(MQ)),
            ?assertEqual([], stored_payloads(MQ))
        end,
        [<<"first">>, <<"second">>, <<"third">>]
    ),
    ok = emqtt:disconnect(C).

%% Check that a worker ignores reports at or below the processed boundary of a consumer.
t_cleanup_ignores_processed_reports(_Config) ->
    MQ = queue(#{max_shard_message_count => 10}),
    ?assertWaitEvent(
        emqx_mq_cleanup_worker:report_consumed(MQ, shard(), 100),
        #{?snk_kind := mq_cleanup_refreshed, boundary := 100},
        5000
    ),
    lists:foreach(
        fun(Boundary) ->
            ?assertWaitEvent(
                emqx_mq_cleanup_worker:report_consumed(MQ, shard(), Boundary),
                #{?snk_kind := mq_cleanup_report_ignored, boundary := Boundary},
                5000
            )
        end,
        [100, 50]
    ),
    ?assertWaitEvent(
        emqx_mq_cleanup_worker:report_consumed(MQ, shard(), 200),
        #{?snk_kind := mq_cleanup_refreshed, boundary := 200},
        5000
    ),
    ?assertEqual(
        [100, 200],
        [B || #{boundary := B} <- ?of_kind(mq_cleanup_refreshed, snabbkaffe:collect_trace(0))]
    ).

%% Check that a worker drops the boundaries of a consumer that stops.
t_cleanup_consumer_down(_Config) ->
    #{id := Id} = MQ = queue(#{max_shard_message_count => 10}),
    Worker = gproc_pool:pick_worker(emqx_mq_cleanup_worker, Id),
    Shard = shard(),
    Consumer = spawn(fun() ->
        ok = emqx_mq_cleanup_worker:report_consumed(MQ, Shard, 100),
        receive
            stop -> ok
        end
    end),
    {ok, _} = ?block_until(#{?snk_kind := mq_cleanup_refreshed, boundary := 100}, 5000, infinity),
    ?assertMatch(
        #{consumers := #{Consumer := _}, work := #{{Id, Shard, Consumer} := _}},
        sys:get_state(Worker)
    ),
    Consumer ! stop,
    ?retry(
        50,
        100,
        begin
            #{consumers := Consumers, work := Work} = sys:get_state(Worker),
            ?assertNot(maps:is_key(Consumer, Consumers)),
            ?assertNot(maps:is_key({Id, Shard, Consumer}, Work))
        end
    ).

%% Check that a quota flush during a release keeps the charge of records after the boundary,
%% and that the bucket it closes is released once the boundary passes its end.
t_cleanup_flush_conflict(_Config) ->
    MQ = queue(#{max_shard_message_count => 10}),
    ok = emqx_mq_message_db:insert(MQ, message(<<"consumed">>)),
    ok = flush(),
    [{_, DSPosition, _}] = emqx_mq_message_db:dirty_read_all(MQ),
    %% Store another record and close the bucket between the deletion and the index transaction
    meck:new(emqx_mq_ds_queue_deleter, [passthrough, no_link]),
    meck:expect(emqx_mq_ds_queue_deleter, delete, fun(DB, Topics, Bounds) ->
        ok = emqx_mq_message_db:insert(MQ, message(<<"unconsumed">>)),
        ok = flush(),
        meck:passthrough([DB, Topics, Bounds])
    end),
    ok = emqx_mq_message_db:release_before(MQ, shard(), DSPosition + 1),
    meck:unload(emqx_mq_ds_queue_deleter),
    ?assertEqual([<<"unconsumed">>], stored_payloads(MQ)),
    %% The closed bucket ends at a fence after the boundary, so both buckets stay charged
    ?assertMatch({ok, #{count := 2, buckets := 2}}, occupancy(MQ)),
    [{_, UnconsumedPosition, _}] = emqx_mq_message_db:dirty_read_all(MQ),
    ok = emqx_mq_message_db:release_before(MQ, shard(), UnconsumedPosition + 1),
    ?assertEqual([], stored_payloads(MQ)),
    ?assertMatch({ok, #{count := 1, buckets := 1}}, occupancy(MQ)).

%% Check that a single retained head bucket admits messages above the limit,
%% and that the bucket is released after it closes and a later record is consumed.
t_single_head_admission(_Config) ->
    Large = message(binary:copy(<<1>>, 2000)),
    MaxBytes = byte_size(emqx_mq_message_db:encode_message(Large)),
    MQ = queue(#{max_shard_message_bytes => MaxBytes}),
    Small = message(<<"small">>),
    SmallBytes = byte_size(emqx_mq_message_db:encode_message(Small)),
    ok = emqx_mq_message_db:insert(MQ, Small),
    ok = flush(),
    C = subscriber(),
    {ok, Received} = emqx_mq_test_utils:emqtt_drain(1, 1000),
    %% The consumed head is the only bucket, so it stays charged
    ?assertWaitEvent(
        emqx_mq_test_utils:emqtt_ack(Received),
        #{?snk_kind := mq_cleanup_released},
        5000
    ),
    ?assertMatch({ok, #{bytes := SmallBytes, buckets := 1}}, occupancy(MQ)),
    %% The single bucket admits a message above the limit, and fills
    ok = emqx_mq_message_db:insert(MQ, Large),
    ok = flush(),
    {ok, Received1} = emqx_mq_test_utils:emqtt_drain(1, 1000),
    ?assertWaitEvent(
        emqx_mq_test_utils:emqtt_ack(Received1),
        #{?snk_kind := mq_cleanup_released},
        5000
    ),
    ?assertEqual([], stored_payloads(MQ)),
    %% The next message closes the full bucket. Its consumption releases the bucket
    ok = emqx_mq_message_db:insert(MQ, Small),
    ok = flush(),
    {ok, Received2} = emqx_mq_test_utils:emqtt_drain(1, 1000),
    ?assertWaitEvent(
        emqx_mq_test_utils:emqtt_ack(Received2),
        #{?snk_kind := mq_cleanup_released},
        5000
    ),
    ?assertMatch({ok, #{bytes := SmallBytes, buckets := 1}}, occupancy(MQ)),
    ok = emqtt:disconnect(C).

%% Check that the cleanup worker retries reported progress after a release fails.
t_cleanup_retry(_Config) ->
    MQ = queue(#{max_shard_message_count => 1}),
    ok = emqx_mq_message_db:insert(MQ, message(<<"value">>)),
    ok = flush(),
    meck:new(emqx_mq_cleanup_worker, [passthrough, no_link]),
    meck:expect(emqx_mq_cleanup_worker, retry_interval, fun() -> 100 end),
    meck:new(emqx_mq_message_db, [passthrough, no_link]),
    meck:expect(
        emqx_mq_message_db,
        release_before,
        fun(_, _, _) -> {error, {recoverable, injected_conflict}} end
    ),
    C = subscriber(),
    {ok, Received} = emqx_mq_test_utils:emqtt_drain(1, 1000),
    ?assertWaitEvent(
        emqx_mq_test_utils:emqtt_ack(Received),
        #{?snk_kind := mq_cleanup_retry},
        5000
    ),
    ?assertEqual([<<"value">>], stored_payloads(MQ)),
    meck:unload(emqx_mq_message_db),
    ?retry(50, 100, ?assertEqual([], stored_payloads(MQ))),
    ok = emqtt:disconnect(C).

%% Check that cleanup releases quota after a full purge that keeps the consumer running.
t_purge_with_running_cleanup(_Config) ->
    FutureMs = erlang:system_time(millisecond) + 60000,
    #{id := Id} = MQ0 = queue(#{max_shard_message_count => 1}),
    ok = emqx_mq_message_db:insert(MQ0, message(<<"first">>)),
    ok = flush(),
    C = subscriber(),
    {ok, Received} = emqx_mq_test_utils:emqtt_drain(1, 1000),
    ok = emqx_mq_test_utils:emqtt_ack(Received),
    ?retry(50, 100, ?assertEqual([], stored_payloads(MQ0))),
    {ok, Consumer} = emqx_mq_consumer:find(Id),
    %% A later stored cutoff keeps the consumer running through the purge
    {ok, MQ1, true} = emqx_mq_state_storage:update_mq_state(Id, #{deliver_cutoff_date => FutureMs}),
    ?assertEqual({ok, MQ1}, emqx_mq_registry:purge(<<"capped">>)),
    ?assertEqual({ok, Consumer}, emqx_mq_consumer:find(Id)),
    %% The running consumer keeps its settings and delivers the new record.
    %% Its acknowledgment deletes it. The reset index keeps one bucket, so admission stays open.
    ok = emqx_mq_message_db:insert(MQ1, message(<<"second">>)),
    ok = flush(),
    {ok, [#{payload := <<"second">>}] = Received1} = emqx_mq_test_utils:emqtt_drain(1, 1000),
    ok = emqx_mq_test_utils:emqtt_ack(Received1),
    ?retry(50, 100, ?assertEqual([], stored_payloads(MQ1))),
    ?assertMatch({ok, #{buckets := 1}}, occupancy(MQ1)),
    ok = emqx_mq_message_db:insert(MQ1, message(<<"third">>)),
    ok = emqtt:disconnect(C).

%% Check that GC removes records left by failed cleanup and purge deletions below the reset index.
t_purge_after_failed_cleanup_deletion(_Config) ->
    MQ = queue(#{max_shard_message_count => 2}),
    ok = emqx_mq_message_db:insert(MQ, message(<<"first">>)),
    ok = emqx_mq_message_db:insert(MQ, message(<<"second">>)),
    ok = flush(),
    %% GC deletes with its own requests, so this fails only cleanup deletion
    meck:new(emqx_mq_ds_queue_deleter, [passthrough, no_link]),
    meck:expect(emqx_mq_ds_queue_deleter, delete, fun(_, _, _) -> {error, injected} end),
    C = subscriber(),
    {ok, Received} = emqx_mq_test_utils:emqtt_drain(2, 1000),
    ?assertWaitEvent(
        emqx_mq_test_utils:emqtt_ack(Received), #{?snk_kind := mq_cleanup_retry}, 5000
    ),
    %% Full purge commits the index rotation without scheduling data deletion.
    meck:new(emqx_mq_delete_worker, [passthrough, no_link]),
    meck:expect(emqx_mq_delete_worker, enqueue_purge, fun(_, _) -> {error, no_delete_worker} end),
    {ok, _} = emqx_mq_registry:purge(emqx_mq_prop:name(MQ)),
    meck:unload(emqx_mq_delete_worker),
    ok = emqx_mq_message_db:insert(MQ, message(<<"fresh">>)),
    ok = flush(),
    ?assertEqual(3, length(stored_payloads(MQ))),
    %% GC deletes the old records below the reset boundary and keeps the new record
    ok = gc([MQ]),
    meck:unload(emqx_mq_ds_queue_deleter),
    ?assertEqual([<<"fresh">>], stored_payloads(MQ)),
    ?assertMatch({ok, #{count := 1}}, occupancy(MQ)),
    ok = emqtt:disconnect(C).

%% Check that recreation after deletion uses a distinct immutable ID and its own quota.
t_queue_recreation(_Config) ->
    #{id := Id0} = MQ0 = queue(#{max_shard_message_count => 1}),
    C = subscriber(),
    ok = emqx_mq_message_db:insert(MQ0, message(<<"old">>)),
    {ok, Received} = emqx_mq_test_utils:emqtt_drain(1, 1000),
    ok = emqx_mq_test_utils:emqtt_ack(Received),
    ok = emqx_mq_registry:delete(<<"capped">>),
    #{id := Id1} = MQ1 = queue(#{max_shard_message_count => 1}),
    ?assertNotEqual(Id0, Id1),
    ok = emqx_mq_message_db:insert(MQ1, message(<<"new">>)),
    ok = flush(),
    ?assertEqual([<<"new">>], stored_payloads(MQ1)),
    ok = emqtt:disconnect(C).

%% Measure admission overhead and transaction conflicts while concurrent publishers race consumption.
t_concurrent_publication_measurement(_Config) ->
    MQ = queue(#{max_shard_message_count => 1000}),
    C = emqx_mq_test_utils:emqtt_connect([]),
    ok = emqx_mq_test_utils:emqtt_sub_mq(C, <<"capped">>),
    ?check_trace(
        begin
            Times = lists:append(
                emqx_utils:pmap(
                    fun(Publisher) ->
                        [
                            begin
                                Message = message(term_to_binary({Publisher, N})),
                                {Time, ok} = timer:tc(fun() ->
                                    emqx_mq_message_db:insert(MQ, Message)
                                end),
                                Time
                            end
                         || N <- lists:seq(1, 50)
                        ]
                    end,
                    lists:seq(1, 4)
                )
            ),
            ok = flush(),
            {ok, Received} = emqx_mq_test_utils:emqtt_drain(200, 2000),
            ?assertEqual(200, length(lists:usort([Payload || #{payload := Payload} <- Received]))),
            ct:pal("Concurrent reject insert us: ~p", [stats(Times)])
        end,
        fun(Trace) ->
            Attempts = ?of_kind(emqx_ds_optimistic_tx_commit_begin, Trace),
            Conflicts = [
                Event
             || #{reason := {read_conflict, _}} = Event <-
                    ?of_kind(emqx_ds_optimistic_tx_commit_abort, Trace)
            ],
            ct:pal("Concurrent storage transactions: ~p; read conflicts: ~p", [
                length(Attempts), length(Conflicts)
            ])
        end
    ),
    ok = emqtt:disconnect(C).

stats(Times) ->
    Sorted = lists:sort(Times),
    #{
        mean => lists:sum(Times) div length(Times),
        p50 => lists:nth(length(Times) div 2, Sorted),
        p99 => lists:nth(length(Times) * 99 div 100, Sorted)
    }.

queue(Limits) ->
    queue(Limits, #{}).

queue(Limits, Overrides) ->
    emqx_mq_test_utils:ensure_mq_created(
        maps:merge(
            #{
                name => <<"capped">>,
                topic_filter => <<"capped/#">>,
                consumer_max_inactive => 60000,
                limits => maps:merge(
                    #{
                        on_full => reject,
                        max_shard_message_count => infinity,
                        max_shard_message_bytes => infinity
                    },
                    Limits
                )
            },
            Overrides
        )
    ).

subscriber() ->
    C = emqx_mq_test_utils:emqtt_connect([{auto_ack, false}]),
    ok = emqx_mq_test_utils:emqtt_sub_mq(C, <<"capped">>),
    C.

occupancy(MQ) -> emqx_mq_message_db:occupancy(MQ, shard()).

stored_payloads(MQ) ->
    [
        emqx_message:payload(emqx_mq_message_db:decode_message(Bin))
     || {_, _, Bin} <- emqx_mq_message_db:dirty_read_all(MQ)
    ].

message(Payload) ->
    emqx_message:make(<<"publisher">>, 1, <<"capped/message">>, Payload).

shard() ->
    [Shard] = emqx_ds:list_shards(?MQ_MESSAGE_LASTVALUE_DB),
    Shard.

flush() ->
    emqx_mq_quota_buffer:flush(?MQ_QUOTA_BUFFER).

%% Insert and flush each message. A count-one or one-message byte limit then has two buckets.
fill(MQ, Payloads) ->
    lists:foreach(
        fun(Payload) ->
            ok = emqx_mq_message_db:insert(MQ, message(Payload)),
            ok = flush()
        end,
        Payloads
    ).

gc(MQs) ->
    lists:foreach(
        fun(Shard) ->
            {ok, Pid} = emqx_mq_gc_slab_worker:start_link({Shard, 1}, MQs),
            Ref = monitor(process, Pid),
            receive
                {'DOWN', Ref, process, Pid, normal} -> ok
            after 10000 -> error(gc_timeout)
            end
        end,
        emqx_ds:list_shards(?MQ_MESSAGE_LASTVALUE_DB)
    ).
