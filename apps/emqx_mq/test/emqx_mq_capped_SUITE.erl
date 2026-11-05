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

%% Measure dirty-index reads against synchronous publication before enabling admission checks.
t_dirty_read_latency(_Config) ->
    MQ = queue(#{max_shard_message_count => 100000, on_full => evict}),
    Message = message(<<"sample">>),
    ok = emqx_mq_message_db:insert(MQ, Message),
    ok = flush(),
    Shard = shard(),
    Reads = [
        begin
            {Time, {ok, #{count := 1}}} = timer:tc(fun() ->
                emqx_mq_message_db:occupancy(MQ, Shard)
            end),
            Time
        end
     || _ <- lists:seq(1, 1000)
    ],
    Writes = [
        begin
            {Time, ok} = timer:tc(fun() -> emqx_mq_message_db:insert(MQ, Message) end),
            Time
        end
     || _ <- lists:seq(1, 100)
    ],
    ct:print("Dirty index read us: ~p; synchronous insert us: ~p", [stats(Reads), stats(Writes)]).

stats(Times) ->
    Sorted = lists:sort(Times),
    #{
        mean => lists:sum(Times) div length(Times),
        p50 => lists:nth(length(Times) div 2, Sorted),
        p99 => lists:nth(length(Times) * 99 div 100, Sorted)
    }.

%% Check that count-one admission resumes after a completely acknowledged buffer retires.
t_consume_releases_count_one(_Config) ->
    MQ = queue(#{max_shard_message_count => 1}),
    ok = emqx_mq_message_db:insert(MQ, message(<<"first">>)),
    ok = flush(),
    ?assertEqual({error, queue_full}, emqx_mq_message_db:insert(MQ, message(<<"second">>))),
    C = subscriber(),
    {ok, [Received]} = emqx_mq_test_utils:emqtt_drain(1, 1000),
    ?assertMatch(#{payload := <<"first">>}, Received),
    ?assertMatch({ok, #{count := 1}}, occupancy(MQ)),
    ok = emqx_mq_test_utils:emqtt_ack([Received]),
    ?retry(50, 100, ?assertMatch({ok, #{count := 0}}, occupancy(MQ))),
    ok = emqx_mq_message_db:insert(MQ, message(<<"second">>)),
    {ok, [Received1]} = emqx_mq_test_utils:emqtt_drain(1, 1000),
    ?assertMatch(#{payload := <<"second">>}, Received1),
    ok = emqtt:disconnect(C).

%% Check count, byte, and combined admission without modifying existing records on rejection.
t_admission_limits(_Config) ->
    Message = message(<<"value">>),
    Size = byte_size(emqx_mq_message_db:encode_message(Message)),
    lists:foreach(
        fun(Limits) ->
            MQ = queue(Limits),
            ?assertMatch({ok, #{count := 0, bytes := 0}}, occupancy(MQ)),
            ok = emqx_mq_message_db:insert(MQ, Message),
            ok = flush(),
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
    ok = emqx_mq_message_db:insert(MQ, message(<<"old">>)),
    ok = flush(),
    ?assertEqual({error, queue_full}, emqx_mq_message_db:insert(MQ, message(<<"new">>))),
    [Record] = emqx_mq_message_db:dirty_read_all(MQ),
    {_, _, Bin} = Record,
    ?assertMatch(#message{payload = <<"old">>}, emqx_mq_message_db:decode_message(Bin)),
    C = subscriber(),
    {ok, Received} = emqx_mq_test_utils:emqtt_drain(1, 1000),
    ok = emqx_mq_test_utils:emqtt_ack(Received),
    ?retry(50, 100, ?assertMatch({ok, #{count := 0}}, occupancy(MQ))),
    Message1 = message(<<"new">>),
    ok = emqx_mq_message_db:insert(MQ, Message1#message{from = <<"other">>}),
    ok = flush(),
    {ok, Received1} = emqx_mq_test_utils:emqtt_drain(1, 1000),
    ok = emqx_mq_test_utils:emqtt_ack(Received1),
    ?retry(50, 100, ?assertEqual([<<"new">>], stored_payloads(MQ))),
    ok = emqtt:disconnect(C).

%% Check that last-value replacement removes the old charge by publication time rather than storage time.
t_lastvalue_replacement_timestamp(_Config) ->
    MQ = queue(#{max_shard_message_count => 10}, #{
        is_lastvalue => true, key_expression => <<"message.from">>
    }),
    OldMs = erlang:system_time(millisecond) - 60000,
    Message0 = message(<<"old">>),
    ok = emqx_mq_message_db:insert(MQ, Message0#message{timestamp = OldMs}),
    ok = flush(),
    Message1 = message(<<"new">>),
    ok = emqx_mq_message_db:insert(MQ, Message1),
    ok = flush(),
    Expected = #{count => 1, bytes => byte_size(emqx_mq_message_db:encode_message(Message1))},
    ?assertEqual({ok, Expected}, occupancy(MQ)),
    {ok, _, _} = emqx_mq_message_db:truncate_index(MQ, shard(), OldMs * 1000 + 1),
    ?assertEqual({ok, Expected}, occupancy(MQ)),
    ?assertEqual([<<"new">>], stored_payloads(MQ)).

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
    ?assertMatch({ok, #{count := 4}}, occupancy(MQ)),
    Records = emqx_mq_message_db:dirty_read_all(MQ),
    ok = emqx_mq_message_db:delete_expired_data([MQ]),
    ?assertEqual(Records, emqx_mq_message_db:dirty_read_all(MQ)),
    ok = emqx_mq_message_db:delete_expired_data([MQ#{data_retention_period := 0}]),
    ?assertEqual([], emqx_mq_message_db:dirty_read_all(MQ)),
    ?assertMatch({ok, #{count := 0}}, occupancy(MQ)).

%% Check that GC retains unconsumed records when the publisher clock is ahead of storage time.
t_gc_publisher_clock_ahead(_Config) ->
    MQ = queue(#{max_shard_message_count => 1}),
    Message0 = message(<<"future">>),
    Message1 = Message0#message{timestamp = erlang:system_time(millisecond) + 1000},
    ok = emqx_mq_message_db:insert(MQ, Message1),
    ok = flush(),
    Records = emqx_mq_message_db:dirty_read_all(MQ),
    ok = emqx_mq_message_db:delete_expired_data([MQ]),
    ?assertEqual(Records, emqx_mq_message_db:dirty_read_all(MQ)),
    ?assertMatch({ok, #{count := 1}}, occupancy(MQ)).

%% Check that unexpired reject queues share one data transaction per shard without index transactions.
t_gc_batches_reject_queues(_Config) ->
    MQ0 = queue(#{max_shard_message_count => 1}),
    MQ1 = queue(#{max_shard_message_count => 1}, #{name => <<"other">>}),
    MQ2 = queue(#{max_shard_message_count => 1}, #{name => <<"empty">>}),
    ok = emqx_mq_message_db:insert(MQ0, message(<<"first">>)),
    ok = emqx_mq_message_db:insert(MQ1, message(<<"second">>)),
    ok = flush(),
    ?check_trace(
        ok = emqx_mq_message_db:delete_expired_data([MQ0, MQ1, MQ2]),
        fun(Trace) ->
            Commits = [
                E
             || #{db := ?MQ_MESSAGE_LASTVALUE_DB} = E <- ?of_kind(
                    emqx_ds_optimistic_tx_commit_begin, Trace
                )
            ],
            ?assertEqual(length(emqx_ds:list_shards(?MQ_MESSAGE_LASTVALUE_DB)), length(Commits)),
            ?assertEqual([], ?of_kind(mq_cleanup_index_read, Trace))
        end
    ),
    ?assertEqual([<<"first">>], stored_payloads(MQ0)),
    ?assertEqual([<<"second">>], stored_payloads(MQ1)).

%% Check that a retention conflict on one queue does not prevent expiration of another queue.
t_retention_conflict_isolation(_Config) ->
    #{id := Id0} = MQ0 = queue(#{max_shard_message_count => 1}),
    MQ1 = queue(#{max_shard_message_count => 1}, #{name => <<"other">>}),
    ok = emqx_mq_message_db:insert(MQ0, message(<<"first">>)),
    ok = emqx_mq_message_db:insert(MQ1, message(<<"second">>)),
    ok = flush(),
    meck:new(emqx_mq_message_db, [passthrough, no_link]),
    meck:expect(emqx_mq_message_db, truncate_index, fun
        (#{id := Id}, _, _) when Id =:= Id0 -> {error, {recoverable, injected_conflict}};
        (MQ, Shard, Boundary) -> meck:passthrough([MQ, Shard, Boundary])
    end),
    ok = emqx_mq_message_db:delete_expired_data([
        MQ0#{data_retention_period := 0}, MQ1#{data_retention_period := 0}
    ]),
    ?assertEqual([], stored_payloads(MQ0)),
    ?assertMatch({ok, #{count := 1}}, occupancy(MQ0)),
    ?assertEqual([], stored_payloads(MQ1)),
    ?assertMatch({ok, #{count := 0}}, occupancy(MQ1)),
    meck:unload(emqx_mq_message_db),
    ok = emqx_mq_message_db:delete_expired_data([MQ0#{data_retention_period := 0}]),
    ?assertMatch({ok, #{count := 0}}, occupancy(MQ0)).

%% Check that a stale high estimate can reject a write until the next occupancy read refreshes it.
t_stale_high_admission(_Config) ->
    MQ = queue(#{max_shard_message_count => 1}),
    meck:new(emqx_mq_message_db, [passthrough, no_link]),
    meck:expect(emqx_mq_message_db, occupancy, fun(_, _) -> {ok, #{count => 1, bytes => 0}} end),
    ?assertEqual({error, queue_full}, emqx_mq_message_db:insert(MQ, message(<<"value">>))),
    ?assertEqual([], stored_payloads(MQ)),
    meck:unload(emqx_mq_message_db),
    ok = emqx_mq_message_db:insert(MQ, message(<<"value">>)).

%% Check that cleaner refresh releases quota when consumption precedes the quota flush.
t_delayed_quota_flush(_Config) ->
    MQ = queue(#{max_shard_message_count => 1}),
    meck:new(emqx_mq_message_db, [passthrough, no_link]),
    meck:expect(emqx_mq_message_db, quota_buffer_flush_interval, fun() -> 60000 end),
    meck:new(emqx_mq_cleanup_worker, [passthrough, no_link]),
    meck:expect(emqx_mq_cleanup_worker, retry_interval, fun() -> 100 end),
    C = subscriber(),
    ok = emqx_mq_message_db:insert(MQ, message(<<"value">>)),
    {ok, Received} = emqx_mq_test_utils:emqtt_drain(1, 1000),
    %% The first refresh finds no index and keeps refreshing
    ?assertWaitEvent(
        emqx_mq_test_utils:emqtt_ack(Received),
        #{?snk_kind := mq_cleanup_refreshed, next := undefined, pending := true},
        5000
    ),
    %% A later refresh sees the flushed charge and releases it without another acknowledgment
    ?assertWaitEvent(
        ok = flush(),
        #{?snk_kind := mq_cleanup_delete},
        5000
    ),
    ?assertMatch({ok, #{count := 0}}, occupancy(MQ)),
    ok = emqtt:disconnect(C).

%% Check that committed quota is available after failed deletion and GC removes only consumed records.
t_failed_cleanup_deletion(_Config) ->
    MQ = queue(#{max_shard_message_count => 2}),
    ok = emqx_mq_message_db:insert(MQ, message(<<"first">>)),
    ok = emqx_mq_message_db:insert(MQ, message(<<"second">>)),
    ok = flush(),
    meck:new(emqx_mq_message_db, [passthrough, no_link]),
    meck:expect(emqx_mq_message_db, delete_data_before, fun(_, _, _) -> {error, injected} end),
    %% A short retry interval would expose a deletion retry within the wait below
    meck:new(emqx_mq_cleanup_worker, [passthrough, no_link]),
    meck:expect(emqx_mq_cleanup_worker, retry_interval, fun() -> 10 end),
    C = subscriber(),
    {ok, Received} = emqx_mq_test_utils:emqtt_drain(2, 1000),
    ok = emqx_mq_test_utils:emqtt_ack(Received),
    ?retry(50, 100, ?assertMatch({ok, #{count := 0}}, occupancy(MQ))),
    {ok, _} = ?block_until(#{?snk_kind := mq_cleanup_delete_failed}, 5000, infinity),
    CutoffUs = lists:max([
        C0
     || #{cutoff_us := C0} <- ?of_kind(mq_cleanup_delete_failed, snabbkaffe:collect_trace(0))
    ]),
    ?assertMatch([_, _], emqx_mq_message_db:dirty_read_all(MQ)),
    ok = emqx_mq_message_db:insert(MQ, message(<<"third">>)),
    Records = emqx_mq_message_db:dirty_read_all(MQ),
    Expected = [Record || {_, Ts, _} = Record <- Records, Ts >= CutoffUs],
    meck:unload(emqx_mq_message_db),
    ?assertEqual(timeout, ?block_until(#{?snk_kind := mq_cleanup_delete}, 200, 0)),
    ok = emqx_mq_message_db:delete_expired_data([MQ]),
    ?assertEqual(Expected, emqx_mq_message_db:dirty_read_all(MQ)),
    ok = emqtt:disconnect(C).

%% Check MQTT 5 direct rejection for QoS 1/2 and metrics for QoS 0 without ordinary-topic feedback.
t_publisher_feedback(_Config) ->
    MQ = queue(#{max_shard_message_count => 1}),
    Other = emqx_mq_test_utils:ensure_mq_created(#{
        name => <<"other">>, topic_filter => <<"capped/#">>
    }),
    ok = emqx_mq_message_db:insert(MQ, message(<<"first">>)),
    ok = flush(),
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
    ?assertMatch([_], emqx_mq_message_db:dirty_read_all(MQ)),
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
    meck:new(emqx_bpapi, [passthrough, no_link]),
    meck:expect(emqx_bpapi, supported_version, fun(_, emqx_mq_quota_index) -> undefined end),
    ?assertEqual(
        {error, on_full_reject_not_supported_by_cluster},
        emqx_mq_registry:create(MQ1#{name := <<"old_cluster">>})
    ),
    meck:unload(emqx_bpapi).

%% Check that an unknown message leaves the cleaner running with the same PID.
t_cleanup_unknown_message(_Config) ->
    #{id := Id} = MQ = queue(#{max_shard_message_count => 1}),
    {ok, Consumer} = emqx_mq_sup:start_consumer(Id, [MQ]),
    Sup = queue_sup(Id),
    Cleaner = cleaner(Sup),
    Cleaner ! unknown_message,
    ?assertEqual({error, unknown_request}, gen_server:call(Cleaner, synchronize)),
    ?assertEqual(Cleaner, cleaner(Sup)),
    ok = emqx_mq_consumer:stop(MQ, Consumer).

%% Check that a cleaner ignores startup when its consumer has already exited.
t_cleanup_missing_consumer(_Config) ->
    MQ = queue(#{max_shard_message_count => 1}),
    meck:new(emqx_mq_consumer, [passthrough, no_link]),
    meck:expect(emqx_mq_consumer, find, fun(_) -> not_found end),
    ?assertEqual(ignore, emqx_mq_cleanup_worker:start_link(MQ)).

%% Check that cleaner crashes preserve the consumer and consumer exits stop its paired supervisor.
t_cleanup_lifecycle(_Config) ->
    #{id := Id} = MQ = queue(#{max_shard_message_count => 1}),
    %% A cleaner crash restarts only the cleaner
    {ok, Consumer0} = emqx_mq_sup:start_consumer(Id, [MQ]),
    Sup0 = queue_sup(Id),
    Cleaner0 = cleaner(Sup0),
    exit(Cleaner0, kill),
    ?retry(50, 100, ?assert(is_pid(cleaner(Sup0)) andalso cleaner(Sup0) =/= Cleaner0)),
    ?assertEqual({ok, Consumer0}, emqx_mq_consumer:find(Id)),
    Cleaner1 = cleaner(Sup0),
    %% A consumer crash stops the paired supervisor and the cleaner without a restart
    MRef = monitor(process, Sup0),
    exit(Consumer0, kill),
    ?assertReceive({'DOWN', MRef, process, Sup0, _}, 1000),
    ?assertNot(is_process_alive(Cleaner1)),
    ?assertEqual(not_found, emqx_mq_consumer:find(Id)),
    ?assertEqual([], supervisor:which_children(emqx_mq_consumer_sup)),
    %% A later lookup starts a new consumer with a new cleaner
    {ok, Consumer1} = emqx_mq_sup:start_consumer(Id, [MQ]),
    ?assertNotEqual(Consumer0, Consumer1),
    Sup1 = queue_sup(Id),
    ?assert(is_pid(cleaner(Sup1))),
    %% A normal consumer stop also stops the paired supervisor
    ok = emqx_mq_consumer:stop(MQ, Consumer1),
    ?assertNot(is_process_alive(Sup1)),
    ?assertEqual(not_found, emqx_mq_consumer:find(Id)),
    ok = emqx_mq_registry:delete(emqx_mq_prop:name(MQ)).

%% Check that cleanup stops refreshing an index without an older bucket after a bounded backoff.
t_bounded_refresh(_Config) ->
    MQ = queue(#{max_shard_message_count => 100}),
    meck:new(emqx_mq_cleanup_worker, [passthrough, no_link]),
    meck:expect(emqx_mq_cleanup_worker, retry_interval, fun() -> 10 end),
    ok = emqx_mq_message_db:insert(MQ, message(<<"value">>)),
    ok = flush(),
    C = subscriber(),
    {ok, Received} = emqx_mq_test_utils:emqtt_drain(1, 1000),
    %% Refreshes retain the single charged bucket and end with a non-pending report
    ok = emqx_mq_test_utils:emqtt_ack(Received),
    {ok, _} = ?block_until(
        #{?snk_kind := mq_cleanup_refreshed, cutoff_us := 0, next := undefined, pending := false},
        5000,
        infinity
    ),
    Refreshes0 = ?of_kind(mq_cleanup_refreshed, snabbkaffe:collect_trace(0)),
    ?assertEqual(
        [true, true, true, true, true, false],
        [Pending || #{pending := Pending} <- Refreshes0]
    ),
    ?assertMatch({ok, #{count := 1}}, occupancy(MQ)),
    %% No further refreshes happen without new consumption progress
    ?assertEqual(timeout, ?block_until(#{?snk_kind := mq_cleanup_refreshed}, 500, 0)),
    ok = emqtt:disconnect(C).

%% Check that new consumption progress runs cleanup at once instead of waiting for a refresh backoff.
t_progress_preempts_refresh_backoff(_Config) ->
    MQ = queue(#{max_shard_message_count => 1}),
    meck:new(emqx_mq_cleanup_worker, [passthrough, no_link]),
    meck:expect(emqx_mq_cleanup_worker, retry_interval, fun() -> 60000 end),
    C = subscriber(),
    %% Consuming the first record releases it and leaves a refresh scheduled for later
    ok = emqx_mq_message_db:insert(MQ, message(<<"first">>)),
    ok = flush(),
    {ok, Received0} = emqx_mq_test_utils:emqtt_drain(1, 1000),
    ?assertWaitEvent(
        emqx_mq_test_utils:emqtt_ack(Received0),
        #{?snk_kind := mq_cleanup_refreshed, cutoff_us := 0, next := undefined, pending := true},
        5000
    ),
    ?assertMatch({ok, #{count := 0}}, occupancy(MQ)),
    %% Consuming the second record releases it without waiting for the scheduled refresh
    ok = emqx_mq_message_db:insert(MQ, message(<<"second">>)),
    ok = flush(),
    {ok, Received1} = emqx_mq_test_utils:emqtt_drain(1, 1000),
    ok = emqx_mq_test_utils:emqtt_ack(Received1),
    ?retry(50, 100, ?assertMatch({ok, #{count := 0}}, occupancy(MQ))),
    ok = emqtt:disconnect(C).

%% Check that stale index boundaries cannot delete delayed records beyond the consumed DS prefix.
t_timestamp_skew(_Config) ->
    #{id := Id, topic_filter := Topic} = MQ = queue(#{max_shard_message_count => 100}),
    Now = erlang:system_time(microsecond),
    IndexTopic = [<<"topic">>, Topic, Id, <<"index">>],
    DataTopic = [<<"topic">>, Topic, Id, <<"key">>, <<"publisher">>],
    Index0 = emqx_mq_quota_index:new(emqx_mq_prop:quota_index_opts(MQ), Now - 100),
    Index1 = emqx_mq_quota_index:apply_updates(Index0, [{Now - 100, 100, 10}, {Now - 50, 100, 10}]),
    ?assertMatch(
        {atomic, _, ok},
        emqx_ds:trans(
            #{db => ?MQ_MESSAGE_LASTVALUE_DB, shard => shard(), generation => 1}, fun() ->
                ok = emqx_ds:tx_write(
                    {IndexTopic, ?QUOTA_INDEX_TS, emqx_mq_quota_index:encode(Index1)}
                ),
                ok = emqx_ds:tx_write(
                    {DataTopic, Now - 80,
                        emqx_mq_message_db:encode_message(message(<<"consumed">>))}
                ),
                emqx_ds:tx_write(
                    {DataTopic, Now, emqx_mq_message_db:encode_message(message(<<"delayed">>))}
                )
            end
        )
    ),
    {ok, CutoffUs, _} = emqx_mq_message_db:truncate_index(MQ, shard(), Now - 40),
    ?assert(CutoffUs =< Now - 40),
    ok = emqx_mq_message_db:delete_data_before(MQ, shard(), CutoffUs),
    [{_, Now, Bin}] = emqx_mq_message_db:dirty_read_all(MQ),
    ?assertMatch(#message{payload = <<"delayed">>}, emqx_mq_message_db:decode_message(Bin)).

%% Check that cleanup conflicts with a concurrent quota flush and retries against the committed index.
t_cleanup_flush_conflict(_Config) ->
    #{id := Id} = MQ = queue(#{max_shard_message_count => 10}),
    ok = emqx_mq_message_db:insert(MQ, message(<<"consumed">>)),
    ok = flush(),
    [{_, DSPosition, _}] = emqx_mq_message_db:dirty_read_all(MQ),
    ?force_ordering(
        #{?snk_kind := mq_cleanup_index_read, id := Id},
        #{?snk_kind := mq_cleanup_concurrent_flush, ?snk_span := start}
    ),
    ?force_ordering(
        #{?snk_kind := mq_cleanup_concurrent_flush, ?snk_span := {complete, _}},
        #{?snk_kind := mq_cleanup_index_write, id := Id}
    ),
    spawn_link(fun() ->
        ?tp_span(mq_cleanup_concurrent_flush, #{}, begin
            ok = emqx_mq_message_db:insert(MQ, message(<<"unconsumed">>)),
            ok = flush()
        end)
    end),
    ?assertMatch(
        {error, {recoverable, _}},
        emqx_mq_message_db:truncate_index(MQ, shard(), DSPosition + 1)
    ),
    ?assertMatch({ok, #{count := 2}}, occupancy(MQ)),
    {ok, CutoffUs, _} = emqx_mq_message_db:truncate_index(MQ, shard(), DSPosition + 1),
    ?assert(CutoffUs =< DSPosition + 1),
    ?assertMatch({ok, #{count := 1}}, occupancy(MQ)),
    ok = emqx_mq_message_db:delete_data_before(MQ, shard(), CutoffUs),
    ?assert(lists:member(<<"unconsumed">>, stored_payloads(MQ))).

%% Check that a consumed newest bucket can reject a large message that would fit in an empty queue.
t_retained_head_rejection(_Config) ->
    Large = message(binary:copy(<<1>>, 2000)),
    MaxBytes = byte_size(emqx_mq_message_db:encode_message(Large)),
    MQ = queue(#{max_shard_message_bytes => MaxBytes}),
    Small = message(<<"small">>),
    SmallBytes = byte_size(emqx_mq_message_db:encode_message(Small)),
    ok = emqx_mq_message_db:insert(MQ, Small),
    ok = flush(),
    C = subscriber(),
    {ok, Received} = emqx_mq_test_utils:emqtt_drain(1, 1000),
    ?assertWaitEvent(
        emqx_mq_test_utils:emqtt_ack(Received),
        #{?snk_kind := mq_cleanup_refreshed, cutoff_us := 0},
        5000
    ),
    ?assertMatch({ok, #{bytes := SmallBytes}}, occupancy(MQ)),
    ?assertEqual({error, queue_full}, emqx_mq_message_db:insert(MQ, Large)),
    ok = emqtt:disconnect(C).

%% Check that cleaner replacement recovers reported progress after an index transaction fails.
t_cleanup_retry_and_reattach(_Config) ->
    #{id := Id} = MQ = queue(#{max_shard_message_count => 1}),
    ok = emqx_mq_message_db:insert(MQ, message(<<"value">>)),
    ok = flush(),
    meck:new(emqx_mq_message_db, [passthrough, no_link]),
    meck:expect(
        emqx_mq_message_db,
        truncate_index,
        fun(_, _, _) -> {error, {recoverable, injected_conflict}} end
    ),
    C = subscriber(),
    {ok, Received} = emqx_mq_test_utils:emqtt_drain(1, 1000),
    ?assertWaitEvent(
        emqx_mq_test_utils:emqtt_ack(Received),
        #{?snk_kind := mq_cleanup_retry},
        5000
    ),
    ?assertMatch({ok, #{count := 1}}, occupancy(MQ)),
    {ok, Consumer} = emqx_mq_consumer:find(Id),
    Cleaner = cleaner(queue_sup(Id)),
    meck:unload(emqx_mq_message_db),
    exit(Cleaner, kill),
    ?retry(50, 100, ?assertMatch({ok, #{count := 0}}, occupancy(MQ))),
    ?assertEqual({ok, Consumer}, emqx_mq_consumer:find(Id)),
    ok = emqtt:disconnect(C).

%% Check that cleanup releases quota after a full purge that keeps the consumer and cleaner running.
t_purge_with_running_cleanup(_Config) ->
    FutureMs = erlang:system_time(millisecond) + 60000,
    #{id := Id} = MQ0 = queue(#{max_shard_message_count => 1}),
    ok = emqx_mq_message_db:insert(MQ0, message(<<"first">>)),
    ok = flush(),
    C = subscriber(),
    {ok, Received} = emqx_mq_test_utils:emqtt_drain(1, 1000),
    ok = emqx_mq_test_utils:emqtt_ack(Received),
    ?retry(50, 100, ?assertMatch({ok, #{count := 0}}, occupancy(MQ0))),
    {ok, Consumer} = emqx_mq_consumer:find(Id),
    Cleaner = cleaner(queue_sup(Id)),
    %% A later stored cutoff keeps the consumer running through the purge
    {ok, MQ1, true} = emqx_mq_state_storage:update_mq_state(Id, #{deliver_cutoff_date => FutureMs}),
    ?assertEqual({ok, MQ1}, emqx_mq_registry:purge(<<"capped">>)),
    ?assertEqual({ok, Consumer}, emqx_mq_consumer:find(Id)),
    ?assertEqual(Cleaner, cleaner(queue_sup(Id))),
    %% The running consumer keeps its settings and delivers the new record.
    %% Its acknowledgment releases the charge in the reset index.
    ok = emqx_mq_message_db:insert(MQ1, message(<<"second">>)),
    ok = flush(),
    {ok, [#{payload := <<"second">>}] = Received1} = emqx_mq_test_utils:emqtt_drain(1, 1000),
    ok = emqx_mq_test_utils:emqtt_ack(Received1),
    ?retry(50, 100, ?assertMatch({ok, #{count := 0}}, occupancy(MQ1))),
    ok = emqx_mq_message_db:insert(MQ1, message(<<"third">>)),
    ok = emqtt:disconnect(C).

%% Check that GC removes records left by failed cleanup and purge deletions below the reset index.
t_purge_after_failed_cleanup_deletion(_Config) ->
    MQ = queue(#{max_shard_message_count => 2}),
    ok = emqx_mq_message_db:insert(MQ, message(<<"first">>)),
    ok = emqx_mq_message_db:insert(MQ, message(<<"second">>)),
    ok = flush(),
    meck:new(emqx_mq_message_db, [passthrough, no_link]),
    meck:expect(emqx_mq_message_db, delete_data_before, fun(_, _, _) -> {error, injected} end),
    C = subscriber(),
    {ok, Received} = emqx_mq_test_utils:emqtt_drain(2, 1000),
    ?assertWaitEvent(
        emqx_mq_test_utils:emqtt_ack(Received), #{?snk_kind := mq_cleanup_delete_failed}, 5000
    ),
    meck:unload(emqx_mq_message_db),
    %% Full purge commits the index reset and fails before deleting the old data
    CrashRef = ?inject_crash(
        #{?snk_kind := mq_message_db_purge_delete}, snabbkaffe_nemesis:always_crash()
    ),
    BeforeMs = erlang:system_time(millisecond),
    ?assertExit(_, emqx_mq_message_db:purge(MQ)),
    snabbkaffe_nemesis:fix_crash(CrashRef),
    ok = emqx_mq_message_db:insert(MQ, message(<<"fresh">>)),
    ok = flush(),
    ?assertEqual(3, length(stored_payloads(MQ))),
    %% GC deletes the old records below the reset boundary and keeps the new record
    ok = emqx_mq_message_db:delete_expired_data([MQ]),
    ok = emqx_mq_message_db:purge(MQ, BeforeMs),
    ?assertEqual([<<"fresh">>], stored_payloads(MQ)),
    ?assertMatch({ok, #{count := 1}}, occupancy(MQ)),
    ok = emqtt:disconnect(C).

%% Check that queue deletion terminates cleanup and recreation uses a distinct immutable ID.
t_queue_recreation(_Config) ->
    #{id := Id0} = MQ0 = queue(#{max_shard_message_count => 1}),
    C = subscriber(),
    ok = emqx_mq_message_db:insert(MQ0, message(<<"old">>)),
    {ok, Received} = emqx_mq_test_utils:emqtt_drain(1, 1000),
    ok = emqx_mq_test_utils:emqtt_ack(Received),
    Sup0 = queue_sup(Id0),
    Cleaner0 = cleaner(Sup0),
    ok = emqx_mq_registry:delete(<<"capped">>),
    ?assertNot(is_process_alive(Sup0)),
    ?assertNot(is_process_alive(Cleaner0)),
    #{id := Id1} = MQ1 = queue(#{max_shard_message_count => 1}),
    ?assertNotEqual(Id0, Id1),
    ok = emqx_mq_message_db:insert(MQ1, message(<<"new">>)),
    ok = flush(),
    ?assertEqual([<<"new">>], stored_payloads(MQ1)),
    ok = emqtt:disconnect(C).

%% Check that v2 stops only the expected consumer and that v1 stops the consumer and its supervisor.
t_stop_versions(_Config) ->
    #{id := Id} = MQ = queue(#{max_shard_message_count => 1}),
    {ok, Consumer0} = emqx_mq_sup:start_consumer(Id, [MQ]),
    Sup0 = queue_sup(Id),
    %% A v2 stop for a different consumer PID leaves the running tree intact
    ok = emqx_mq_consumer:stop_v2(MQ, self()),
    ?assertEqual({ok, Consumer0}, emqx_mq_consumer:find(Id)),
    ?assert(is_process_alive(Sup0)),
    %% A v1 stop from an older node stops the consumer, and the supervisor follows it
    ok = emqx_mq_consumer:stop_v1(Consumer0),
    ?assertNot(is_process_alive(Consumer0)),
    ?retry(50, 100, ?assertEqual([], supervisor:which_children(emqx_mq_consumer_sup))),
    ?assertNot(is_process_alive(Sup0)),
    %% A v2 stop returns after the consumer and the supervisor are gone
    {ok, Consumer1} = emqx_mq_sup:start_consumer(Id, [MQ]),
    Sup1 = queue_sup(Id),
    ok = emqx_mq_consumer:stop_v2(MQ, Consumer1),
    ?assertNot(is_process_alive(Consumer1)),
    ?assertNot(is_process_alive(Sup1)),
    ?assertEqual([], supervisor:which_children(emqx_mq_consumer_sup)).

%% Check concurrent consumer startup returns consumer PIDs and leaves one paired supervisor.
t_concurrent_startup(_Config) ->
    #{id := Id} = MQ = queue(#{max_shard_message_count => 1}),
    Results = emqx_utils:pmap(fun(_) -> emqx_mq_sup:start_consumer(Id, [MQ]) end, lists:seq(1, 10)),
    {ok, Consumer} = emqx_mq_consumer:find(Id),
    ?assertEqual(lists:duplicate(10, {ok, Consumer}), Results),
    ?assertMatch([{Id, _, supervisor, _}], supervisor:which_children(emqx_mq_consumer_sup)),
    Sup = queue_sup(Id),
    ?assertEqual(2, length(supervisor:which_children(Sup))),
    ok = emqx_mq_consumer:stop(MQ, Consumer),
    ?retry(50, 100, ?assertNot(is_process_alive(Sup))).

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

queue_sup(Id) ->
    {Id, Sup, _, _} = lists:keyfind(Id, 1, supervisor:which_children(emqx_mq_consumer_sup)),
    Sup.

cleaner(Sup) ->
    {?MQ_CLEANER_CHILD_ID, Cleaner, _, _} = lists:keyfind(
        ?MQ_CLEANER_CHILD_ID, 1, supervisor:which_children(Sup)
    ),
    Cleaner.

message(Payload) ->
    emqx_message:make(<<"publisher">>, 1, <<"capped/message">>, Payload).

shard() ->
    [Shard] = emqx_ds:list_shards(?MQ_MESSAGE_LASTVALUE_DB),
    Shard.

flush() ->
    emqx_mq_quota_buffer:flush(?MQ_QUOTA_BUFFER).
