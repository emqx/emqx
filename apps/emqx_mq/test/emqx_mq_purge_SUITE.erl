%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mq_purge_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx_durable_storage/include/emqx_ds.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include("../src/emqx_mq_internal.hrl").
-include("../gen_src/MessageQueue.hrl").

all() ->
    [
        {group, regular},
        {group, limited_regular},
        {group, lastvalue},
        {group, limited_lastvalue},
        {group, failed_messages},
        {group, failed_index},
        t_legacy_index,
        t_concurrent_index_update,
        t_failed_transaction,
        t_failed_deletion
    ].

groups() ->
    Common = [t_full_purge, t_bounded_purge_connected_client, t_full_purge_connected_client],
    [
        {regular, [], [t_regular_unlimited | Common]},
        {limited_regular, [], [t_bounded_purge, t_missing_index, t_full_purge_updates | Common]},
        {lastvalue, [], [t_bounded_purge, t_missing_index, t_full_purge_updates | Common]},
        {limited_lastvalue, [], [t_bounded_purge, t_missing_index, t_full_purge_updates | Common]},
        {failed_messages, [], [t_full_purge_failure]},
        {failed_index, [], [t_full_purge_failure]}
    ].

init_per_group(failed_messages, Config) ->
    [{fail_index, false}, {queue_name, <<"failed_full_messages">>} | Config];
init_per_group(failed_index, Config) ->
    [{fail_index, true}, {queue_name, <<"failed_full_index">>} | Config];
init_per_group(Group, Config) ->
    {IsLastvalue, IsLimited} =
        case Group of
            regular -> {false, false};
            limited_regular -> {false, true};
            lastvalue -> {true, false};
            limited_lastvalue -> {true, true}
        end,
    [
        {queue_name, atom_to_binary(Group)},
        {is_lastvalue, IsLastvalue},
        {is_limited, IsLimited}
        | Config
    ].

end_per_group(_Group, _Config) ->
    ok.

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [
            emqx_durable_storage,
            {emqx,
                emqx_mq_test_utils:cth_config(emqx, #{
                    <<"durable_storage">> => #{<<"mq_messages">> => #{<<"n_shards">> => 2}}
                })},
            {emqx_mq, emqx_mq_test_utils:cth_config(emqx_mq)}
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{suite_apps, Apps} | Config].

end_per_suite(Config) ->
    emqx_cth_suite:stop(?config(suite_apps, Config)).

init_per_testcase(t_full_purge_updates, Config) ->
    ok = snabbkaffe:start_trace(),
    init_per_testcase(default, Config);
init_per_testcase(_Case, Config) ->
    ok = emqx_mq_test_utils:cleanup_mqs(),
    Config.

end_per_testcase(t_full_purge_updates, Config) ->
    ok = snabbkaffe:stop(),
    end_per_testcase(default, Config);
end_per_testcase(_Case, _Config) ->
    ok = emqx_common_test_helpers:call_janitor(),
    ok = emqx_mq_test_utils:cleanup_mqs().

%% Check that purge trims each slab at a common index boundary
%% while delivery excludes all messages before the request.
t_bounded_purge(Config) ->
    %% Create the queue without a delivery cutoff.
    Limits = #{max_shard_message_count => 1000, max_shard_message_bytes => 1000000},
    QueueOpts = queue_opts(Config, Limits),
    {ok, #{name := Name} = MQ} = emqx_mq_test_utils:create_mq(QueueOpts#{
        topic_filter => <<"purge/#">>
    }),
    ?assertNot(maps:is_key(deliver_cutoff_date, MQ)),
    DB = message_db(MQ),
    Slabs = maps:keys(emqx_ds:list_slabs(DB)),
    ?assertEqual(2, length(Slabs)),
    %% Place messages around bucket boundaries with DS timestamps at or after their message timestamps.
    NowMs = erlang:system_time(millisecond),
    OldMs = NowMs - 30000,
    MiddleMs = NowMs - 20000,
    NewMs = NowMs - 10000,
    BeforeMs = MiddleMs + 5000,
    OldUs = OldMs * 1000,
    MiddleUs = MiddleMs * 1000,
    NewUs = NewMs * 1000,
    Old = message_record(MQ, <<"old">>, OldUs + 2000, OldMs + 1),
    Boundary = message_record(MQ, <<"data_boundary">>, MiddleUs, MiddleMs - 1),
    Delayed = message_record(MQ, <<"delayed_old">>, NewUs + 1000, OldMs + 2),
    Between = message_record(MQ, <<"between_cutoffs">>, MiddleUs + 2000, MiddleMs + 1),
    Before = message_record(MQ, <<"before_boundary">>, BeforeMs * 1000 + 1000, BeforeMs),
    Fresh = message_record(MQ, <<"fresh">>, NewUs + 2000, NewMs + 1),
    Records = [Old, Boundary, Delayed, Between, Before, Fresh],
    OldBytes = record_bytes([Old, Boundary, Delayed]),
    MiddleBytes = record_bytes([Between, Before]),
    NewBytes = record_bytes([Fresh]),
    %% Count messages in common and legacy buckets by their message timestamps.
    Index = #'StorageIndex'{
        maxTsUs = NewUs + 1000,
        common = [
            #'StorageIndexBucket'{tsUs = NewUs, bytes = NewBytes, count = 1},
            #'StorageIndexBucket'{tsUs = MiddleUs, bytes = MiddleBytes, count = 2},
            #'StorageIndexBucket'{tsUs = OldUs, bytes = OldBytes, count = 3}
        ],
        bytes = [
            #'StorageIndexRecord'{tsUs = NewUs, value = NewBytes},
            #'StorageIndexRecord'{tsUs = MiddleUs, value = MiddleBytes},
            #'StorageIndexRecord'{tsUs = OldUs, value = OldBytes}
        ],
        count = [
            #'StorageIndexRecord'{tsUs = NewUs, value = 1},
            #'StorageIndexRecord'{tsUs = MiddleUs, value = 2},
            #'StorageIndexRecord'{tsUs = OldUs, value = 3}
        ]
    },
    OtherMQ = MQ#{id => emqx_guid:gen()},
    Other = message_record(OtherMQ, <<"other_queue">>, OldUs + 2000, OldMs + 1),
    %% Write one copy of each record and index to every slab.
    lists:foreach(
        fun(Slab) ->
            write_records(DB, Slab, [index_record(MQ, Index), Other | Records])
        end,
        Slabs
    ),

    %% A request exactly at a bucket start selects the preceding boundary.
    ok = emqx_mq_message_db:purge(MQ, MiddleMs),
    %% Verify that every slab retains all records and the original index.
    ?assertEqual(
        repeated_records(Records, Slabs), lists:sort(emqx_mq_message_db:dirty_read_all(MQ))
    ),
    lists:foreach(fun(Slab) -> ?assertEqual(Index, read_index(DB, Slab, MQ)) end, Slabs),

    %% Purge removes old DS records and advances delivery to the requested timestamp.
    ?assertEqual(
        {ok, MQ#{deliver_cutoff_date => BeforeMs}}, emqx_mq_registry:purge(Name, BeforeMs)
    ),
    ExpectedRecords = repeated_records([Boundary, Delayed, Between, Before, Fresh], Slabs),
    %% Verify that only the record stored before the selected boundary was deleted from each slab.
    ?assertEqual(ExpectedRecords, lists:sort(emqx_mq_message_db:dirty_read_all(MQ))),
    %% Verify that each index retains only the middle and newest buckets.
    ExpectedIndex = Index#'StorageIndex'{
        common = [
            #'StorageIndexBucket'{tsUs = NewUs, bytes = NewBytes, count = 1},
            #'StorageIndexBucket'{tsUs = MiddleUs, bytes = MiddleBytes, count = 2}
        ],
        bytes = [
            #'StorageIndexRecord'{tsUs = NewUs, value = NewBytes},
            #'StorageIndexRecord'{tsUs = MiddleUs, value = MiddleBytes}
        ],
        count = [
            #'StorageIndexRecord'{tsUs = NewUs, value = 1},
            #'StorageIndexRecord'{tsUs = MiddleUs, value = 2}
        ]
    },
    lists:foreach(
        fun(Slab) -> ?assertEqual(ExpectedIndex, read_index(DB, Slab, MQ)) end, Slabs
    ),
    %% Verify that the other queue's record remains in every slab.
    ?assertEqual(
        repeated_records([Other], Slabs),
        lists:sort(emqx_mq_message_db:dirty_read_all(OtherMQ))
    ),
    %% Verify that repeating the same purge keeps the retained records.
    ?assertMatch(
        {ok, #{deliver_cutoff_date := BeforeMs}}, emqx_mq_registry:purge(Name, BeforeMs)
    ),
    ?assertEqual(ExpectedRecords, lists:sort(emqx_mq_message_db:dirty_read_all(MQ))),

    %% Messages retained only because of bucket rounding or later DS timestamps must not be delivered.
    Client = emqx_mq_test_utils:emqtt_connect([]),
    ok = emqx_mq_test_utils:emqtt_sub_mq(Client, Name),
    {ok, Delivered} = emqx_mq_test_utils:emqtt_drain(2 * length(Slabs), 1000),
    ?assertEqual(
        lists:sort(lists:append([[<<"before_boundary">>, <<"fresh">>] || _ <- Slabs])),
        lists:sort([Payload || #{payload := Payload} <- Delivered])
    ),
    ok = emqtt:disconnect(Client),
    ok = emqx_mq_registry:delete(Name).

%% Check that purge creates missing indices without deleting records when no bucket boundary exists.
t_missing_index(Config) ->
    %% Create a queue and store one old record per slab without an index.
    Limits = #{max_shard_message_count => 1000, max_shard_message_bytes => infinity},
    QueueOpts = queue_opts(Config, Limits),
    {ok, #{name := Name} = MQ} = emqx_mq_test_utils:create_mq(QueueOpts#{
        topic_filter => <<"purge/#">>
    }),
    DB = message_db(MQ),
    Slabs = maps:keys(emqx_ds:list_slabs(DB)),
    BeforeMs = erlang:system_time(millisecond),
    Record = message_record(MQ, <<"old">>, BeforeMs * 1000 - 1000, BeforeMs - 2),
    lists:foreach(fun(Slab) -> write_records(DB, Slab, [Record]) end, Slabs),
    {ok, _} = emqx_mq_registry:purge(Name, BeforeMs),
    %% Verify that the record remains in every slab because no bucket boundary exists.
    ?assertEqual(
        repeated_records([Record], Slabs), lists:sort(emqx_mq_message_db:dirty_read_all(MQ))
    ),
    %% Verify that purge creates an empty common and legacy index in every slab.
    lists:foreach(
        fun(Slab) ->
            ?assertEqual(
                #'StorageIndex'{maxTsUs = 0, common = [], bytes = [], count = []},
                read_index(DB, Slab, MQ)
            )
        end,
        Slabs
    ),
    %% Verify that a later purge still retains every copy of the record.
    {ok, _} = emqx_mq_registry:purge(Name, BeforeMs + 1),
    ?assertEqual(
        repeated_records([Record], Slabs), lists:sort(emqx_mq_message_db:dirty_read_all(MQ))
    ),
    ok = emqx_mq_registry:delete(Name).

%% Check that regular unlimited purge deletes at the requested DS timestamp without creating a quota index.
t_regular_unlimited(_Config) ->
    %% Create an unlimited regular queue with records before, at, and after the DS cutoff.
    Name = <<"regular_unlimited">>,
    {ok, MQ} = emqx_mq_test_utils:create_mq(#{
        name => Name, topic_filter => <<"purge/#">>, is_lastvalue => false
    }),
    DB = ?MQ_MESSAGE_REGULAR_DB,
    Slabs = maps:keys(emqx_ds:list_slabs(DB)),
    BeforeMs = erlang:system_time(millisecond) - 10000,
    BeforeUs = BeforeMs * 1000,
    Old = message_record(MQ, <<"old">>, BeforeUs - 1, BeforeMs - 1),
    Boundary = message_record(MQ, <<"boundary">>, BeforeUs, BeforeMs - 1),
    Fresh = message_record(MQ, <<"fresh">>, BeforeUs + 1, BeforeMs),
    Delayed = message_record(MQ, <<"delayed_old">>, BeforeUs + 2, BeforeMs - 1),
    Records = [Old, Boundary, Fresh, Delayed],
    %% Store the same records in every slab to check deletion across shards.
    lists:foreach(fun(Slab) -> write_records(DB, Slab, Records) end, Slabs),

    %% A zero cutoff preserves every record without creating an index.
    ok = emqx_mq_message_db:purge(MQ, 0),
    ?assertEqual(
        repeated_records(Records, Slabs), lists:sort(emqx_mq_message_db:dirty_read_all(MQ))
    ),
    ?assertEqual([], emqx_ds:dirty_read(DB, index_topic(MQ))),
    ?assertEqual({ok, MQ}, emqx_mq_registry:find(Name)),

    %% Delete only records with DS timestamps strictly before the requested cutoff.
    ?assertEqual(
        {ok, MQ#{deliver_cutoff_date => BeforeMs}}, emqx_mq_registry:purge(Name, BeforeMs)
    ),
    ExpectedRecords = repeated_records([Boundary, Fresh, Delayed], Slabs),
    %% Verify that each slab retains the boundary record and both later records.
    ?assertEqual(ExpectedRecords, lists:sort(emqx_mq_message_db:dirty_read_all(MQ))),
    ?assertEqual([], emqx_ds:dirty_read(DB, index_topic(MQ))),
    ?assertEqual({ok, MQ#{deliver_cutoff_date => BeforeMs}}, emqx_mq_registry:find(Name)),
    %% Verify that repeating purge preserves the retained records without creating an index.
    ?assertMatch({ok, _}, emqx_mq_registry:purge(Name, BeforeMs)),
    ?assertEqual(ExpectedRecords, lists:sort(emqx_mq_message_db:dirty_read_all(MQ))),
    ?assertEqual([], emqx_ds:dirty_read(DB, index_topic(MQ))),

    %% The delivery cutoff skips old messages whose DS timestamps kept them in storage.
    Client = emqx_mq_test_utils:emqtt_connect([]),
    ok = emqx_mq_test_utils:emqtt_sub_mq(Client, Name),
    {ok, Delivered} = emqx_mq_test_utils:emqtt_drain(length(Slabs), 1000),
    ?assertEqual(
        lists:duplicate(length(Slabs), <<"fresh">>),
        [Payload || #{payload := Payload} <- Delivered]
    ),
    ok = emqtt:disconnect(Client).

%% Check that purge persists common buckets reconstructed from legacy records before deleting messages.
t_legacy_index(_Config) ->
    %% Store a message and legacy-only counters in one slab.
    {ok, MQ} = emqx_mq_test_utils:create_mq(#{
        topic_filter => <<"purge/#">>,
        is_lastvalue => true,
        limits => #{max_shard_message_count => 1000, max_shard_message_bytes => infinity}
    }),
    DB = message_db(MQ),
    [Slab | _] = maps:keys(emqx_ds:list_slabs(DB)),
    BeforeMs = erlang:system_time(millisecond),
    StartUs = (BeforeMs - 1000) * 1000,
    Record = message_record(MQ, <<"old">>, StartUs + 1000, BeforeMs - 1000),
    Bytes = record_bytes([Record]),
    Legacy = #'StorageIndex'{
        maxTsUs = StartUs,
        bytes = [#'StorageIndexRecord'{tsUs = StartUs, value = Bytes}],
        count = [#'StorageIndexRecord'{tsUs = StartUs, value = 1}]
    },
    ok = write_records(DB, Slab, [index_record(MQ, Legacy), Record]),
    ok = emqx_mq_message_db:purge(MQ, BeforeMs),
    %% Verify that purge adds a common bucket and preserves the legacy counters.
    ?assertEqual(
        Legacy#'StorageIndex'{
            common = [#'StorageIndexBucket'{tsUs = StartUs, bytes = Bytes, count = 1}]
        },
        read_index(DB, Slab, MQ)
    ),
    %% Verify that the message remains because its DS timestamp follows the selected boundary.
    ?assertEqual([Record], emqx_mq_message_db:dirty_read_all(MQ)).

%% Check that retrying purge after a transaction error uses the latest bucket boundary.
t_concurrent_index_update(_Config) ->
    %% Store two messages with separate common buckets in one slab.
    {ok, MQ} = emqx_mq_test_utils:create_mq(#{topic_filter => <<"purge/#">>, is_lastvalue => true}),
    DB = message_db(MQ),
    [Slab | _] = maps:keys(emqx_ds:list_slabs(DB)),
    BeforeMs = erlang:system_time(millisecond),
    OldUs = (BeforeMs - 3000) * 1000,
    MiddleUs = (BeforeMs - 2000) * 1000,
    NewUs = (BeforeMs - 1000) * 1000,
    Old = message_record(MQ, <<"old">>, OldUs + 1, BeforeMs - 3000),
    Middle = message_record(MQ, <<"middle">>, MiddleUs + 1, BeforeMs - 2000),
    Fresh = message_record(MQ, <<"fresh">>, NewUs + 1, BeforeMs - 1000),
    Index0 = #'StorageIndex'{
        maxTsUs = MiddleUs,
        common = [
            #'StorageIndexBucket'{tsUs = MiddleUs, bytes = record_bytes([Middle]), count = 1},
            #'StorageIndexBucket'{tsUs = OldUs, bytes = record_bytes([Old]), count = 1}
        ]
    },
    ok = write_records(DB, Slab, [index_record(MQ, Index0), Old, Middle]),
    %% Return a transaction error once, then pass subsequent transactions through to DS.
    ok = meck:new(emqx_ds, [passthrough, no_link]),
    ok = emqx_common_test_helpers:on_exit(fun() -> meck:unload(emqx_ds) end),
    ok = meck:expect(
        emqx_ds,
        trans,
        2,
        meck:seq([
            {error, recoverable, conflict},
            meck:exec(fun(Opts, Fun) -> meck:passthrough([Opts, Fun]) end)
        ])
    ),
    ?assertMatch(
        {error, [{_, {recoverable, conflict}}]}, emqx_mq_message_db:purge(MQ, BeforeMs)
    ),
    %% Commit a newer message and bucket before retrying purge.
    NewBucket = #'StorageIndexBucket'{tsUs = NewUs, bytes = record_bytes([Fresh]), count = 1},
    Index1 = Index0#'StorageIndex'{
        maxTsUs = NewUs, common = [NewBucket | Index0#'StorageIndex'.common]
    },
    ok = write_records(DB, Slab, [index_record(MQ, Index1), Fresh]),
    ok = emqx_mq_message_db:purge(MQ, BeforeMs),
    %% Verify that the retry selects the newest boundary and retains only the newest message and bucket.
    ?assertEqual([Fresh], emqx_mq_message_db:dirty_read_all(MQ)),
    ?assertEqual(Index1#'StorageIndex'{common = [NewBucket]}, read_index(DB, Slab, MQ)).

%% Check that a failed index transaction prevents message deletion and delivery cutoff updates.
t_failed_transaction(_Config) ->
    %% Store two messages and their common buckets before injecting a transaction failure.
    {ok, #{name := Name} = MQ} = emqx_mq_test_utils:create_mq(#{
        topic_filter => <<"purge/#">>, is_lastvalue => true
    }),
    DB = message_db(MQ),
    [Slab | _] = maps:keys(emqx_ds:list_slabs(DB)),
    BeforeMs = erlang:system_time(millisecond),
    OldUs = (BeforeMs - 2000) * 1000,
    NewUs = (BeforeMs - 1000) * 1000,
    Old = message_record(MQ, <<"old">>, OldUs + 1, BeforeMs - 2000),
    Fresh = message_record(MQ, <<"fresh">>, NewUs + 1, BeforeMs - 1000),
    Index = #'StorageIndex'{
        maxTsUs = NewUs,
        common = [
            #'StorageIndexBucket'{tsUs = NewUs, bytes = record_bytes([Fresh]), count = 1},
            #'StorageIndexBucket'{tsUs = OldUs, bytes = record_bytes([Old]), count = 1}
        ]
    },
    ok = write_records(DB, Slab, [index_record(MQ, Index), Old, Fresh]),
    ok = meck:new(emqx_ds, [passthrough, no_link]),
    ok = emqx_common_test_helpers:on_exit(fun() -> meck:unload(emqx_ds) end),
    %% Fail the first index transaction, then pass subsequent transactions through to DS.
    ok = meck:expect(
        emqx_ds,
        trans,
        2,
        meck:seq([
            {error, unrecoverable, test_error},
            meck:exec(fun(Opts, Fun) -> meck:passthrough([Opts, Fun]) end)
        ])
    ),
    ?assertMatch(
        {error, [{Slab, {unrecoverable, test_error}}]},
        emqx_mq_registry:purge(Name, BeforeMs)
    ),
    %% Verify that the failed transaction preserves both messages and the original index.
    ?assertEqual(lists:sort([Old, Fresh]), lists:sort(emqx_mq_message_db:dirty_read_all(MQ))),
    ?assertEqual(Index, read_index(DB, Slab, MQ)),
    %% Verify that purge neither attempts deletion nor updates the queue settings.
    ?assertNot(meck:called(emqx_ds, tx_del_topic, ['_', 0, '_'])),
    ?assertEqual({ok, MQ}, emqx_mq_registry:find(Name)).

%% Check that a failed deletion leaves the committed index cutoff intact and a retry removes the old messages.
t_failed_deletion(_Config) ->
    %% Store an old message and a fresh message with separate common buckets.
    {ok, #{name := Name} = MQ} = emqx_mq_test_utils:create_mq(#{
        topic_filter => <<"purge/#">>, is_lastvalue => true
    }),
    DB = message_db(MQ),
    [Slab | _] = maps:keys(emqx_ds:list_slabs(DB)),
    BeforeMs = erlang:system_time(millisecond),
    OldUs = (BeforeMs - 2000) * 1000,
    NewUs = (BeforeMs - 1000) * 1000,
    Old = message_record(MQ, <<"old">>, OldUs + 1, BeforeMs - 2000),
    Fresh = message_record(MQ, <<"fresh">>, NewUs + 1, BeforeMs - 1000),
    NewBucket = #'StorageIndexBucket'{tsUs = NewUs, bytes = record_bytes([Fresh]), count = 1},
    Index = #'StorageIndex'{
        maxTsUs = NewUs,
        common = [
            NewBucket,
            #'StorageIndexBucket'{tsUs = OldUs, bytes = record_bytes([Old]), count = 1}
        ]
    },
    ExpectedIndex = Index#'StorageIndex'{common = [NewBucket]},
    IndexTopic = index_topic(MQ),
    ok = write_records(DB, Slab, [index_record(MQ, Index), Old, Fresh]),
    ok = meck:new(emqx_ds, [passthrough, no_link]),
    ok = emqx_common_test_helpers:on_exit(fun() -> meck:unload(emqx_ds) end),

    %% Fail deletion after checking that the index cutoff is already committed.
    ok = meck:expect(emqx_ds, tx_del_topic, fun(Topic, From, To) ->
        ?assertEqual(NewUs, To),
        ?assertEqual(ExpectedIndex, read_index(DB, Slab, MQ)),
        ok = emqx_ds:tx_ttv_assert_absent(IndexTopic, ?QUOTA_INDEX_TS),
        meck:passthrough([Topic, From, To])
    end),
    ?assertMatch(
        {error, [{Slab, {unrecoverable, {precondition_failed, _}}}]},
        emqx_mq_registry:purge(Name, BeforeMs)
    ),
    ?assertEqual(ExpectedIndex, read_index(DB, Slab, MQ)),
    %% Verify that failed deletion preserves both messages and the original queue settings.
    ?assertEqual(lists:sort([Old, Fresh]), lists:sort(emqx_mq_message_db:dirty_read_all(MQ))),
    ?assertEqual({ok, MQ}, emqx_mq_registry:find(Name)),

    %% Retry deletion using the boundary retained in the trimmed index.
    ok = meck:delete(emqx_ds, tx_del_topic, 3),
    ?assertEqual(
        {ok, MQ#{deliver_cutoff_date => BeforeMs}}, emqx_mq_registry:purge(Name, BeforeMs)
    ),
    %% Verify that the retry deletes the old message and preserves the committed index.
    ?assertEqual([Fresh], emqx_mq_message_db:dirty_read_all(MQ)),
    ?assertEqual(ExpectedIndex, read_index(DB, Slab, MQ)).

%% Check that full purge resets indices before deleting old messages and preserves records at or after its cutoff.
t_full_purge(Config) ->
    %% Prepare quota counters for the queue type under test.
    Limits = #{max_shard_message_count => 1000, max_shard_message_bytes => 1000000},
    QueueOpts = queue_opts(Config, Limits),
    Index = #'StorageIndex'{
        maxTsUs = 1000,
        common = [#'StorageIndexBucket'{tsUs = 1000, bytes = 100, count = 1}],
        bytes = [#'StorageIndexRecord'{tsUs = 1000, value = 100}],
        count = [#'StorageIndexRecord'{tsUs = 1000, value = 1}]
    },
    ok = meck:new(emqx_ds, [passthrough, no_link]),
    ok = emqx_common_test_helpers:on_exit(fun() -> meck:unload(emqx_ds) end),
    {ok, MQ} = emqx_mq_test_utils:create_mq(QueueOpts#{topic_filter => <<"purge/#">>}),
    DB = message_db(MQ),
    Slabs = maps:keys(emqx_ds:list_slabs(DB)),
    OtherMQ = MQ#{id => emqx_guid:gen()},
    %% Include past and future timestamps to check both sides of the purge cutoff.
    Old = message_record(MQ, <<"old">>, 1000, 1),
    FutureMs = erlang:system_time(millisecond) + 60000,
    Future = message_record(MQ, <<"future">>, FutureMs * 1000, FutureMs),
    Other = message_record(OtherMQ, <<"other">>, 1000, 1),
    IndexTopic = index_topic(MQ),
    IndexRecords =
        case emqx_mq_prop:is_append_only(MQ) of
            true -> [];
            false -> [index_record(MQ, Index)]
        end,
    %% Store both queues' records and indices in every slab.
    lists:foreach(
        fun(Slab) ->
            write_records(DB, Slab, [
                Old, Future, Other, index_record(OtherMQ, Index) | IndexRecords
            ])
        end,
        Slabs
    ),

    %% Verify that each slab commits its reset before the message-deletion transaction starts.
    ok = meck:expect(emqx_ds, tx_del_topic, fun(Topic, From, To) ->
        case emqx_mq_prop:is_append_only(MQ) of
            true ->
                ?assertEqual([], emqx_ds:dirty_read(DB, IndexTopic));
            false ->
                [{_, _, Bin}] = emqx_ds:tx_read(IndexTopic),
                ?assertEqual(
                    #'StorageIndex'{
                        maxTsUs = To,
                        common = [#'StorageIndexBucket'{tsUs = To, bytes = 0, count = 0}],
                        bytes = [#'StorageIndexRecord'{tsUs = To, value = 0}],
                        count = [#'StorageIndexRecord'{tsUs = To, value = 0}]
                    },
                    'MessageQueue':decode('StorageIndex', Bin)
                )
        end,
        meck:passthrough([Topic, From, To])
    end),
    StartMs = erlang:system_time(millisecond),
    {ok, CutoffMs} = emqx_mq_message_db:purge(MQ),
    ?assert(CutoffMs >= StartMs),
    ?assert(CutoffMs =< erlang:system_time(millisecond)),
    CutoffUs = CutoffMs * 1000,
    %% Verify that old messages are gone and future records remain in every slab.
    ?assertEqual(
        repeated_records([Future], Slabs), lists:sort(emqx_mq_message_db:dirty_read_all(MQ))
    ),
    ExpectedIndex = #'StorageIndex'{
        maxTsUs = CutoffUs,
        common = [#'StorageIndexBucket'{tsUs = CutoffUs, bytes = 0, count = 0}],
        bytes = [#'StorageIndexRecord'{tsUs = CutoffUs, value = 0}],
        count = [#'StorageIndexRecord'{tsUs = CutoffUs, value = 0}]
    },
    case emqx_mq_prop:is_append_only(MQ) of
        true ->
            ?assertEqual([], emqx_ds:dirty_read(DB, IndexTopic));
        false ->
            lists:foreach(
                fun(Slab) -> ?assertEqual(ExpectedIndex, read_index(DB, Slab, MQ)) end, Slabs
            )
    end,
    %% Verify that the other queue's records and indices remain in every slab.
    ?assertEqual(
        repeated_records([Other], Slabs),
        lists:sort(emqx_mq_message_db:dirty_read_all(OtherMQ))
    ),
    lists:foreach(
        fun(Slab) -> ?assertEqual(Index, read_index(DB, Slab, OtherMQ)) end, Slabs
    ),

    %% Repeating full purge still preserves records newer than its cutoff.
    {ok, _} = emqx_mq_message_db:purge(MQ),
    ?assertEqual(
        repeated_records([Future], Slabs), lists:sort(emqx_mq_message_db:dirty_read_all(MQ))
    ).

%% Check that full-purge failures preserve messages and queue settings while keeping any committed index reset.
t_full_purge_failure(Config) ->
    %% Select the failing transaction and prepare a common-only index.
    Name = ?config(queue_name, Config),
    FailIndex = ?config(fail_index, Config),
    Index = #'StorageIndex'{
        maxTsUs = 1000,
        common = [#'StorageIndexBucket'{tsUs = 1000, bytes = 100, count = 1}]
    },
    ok = meck:new(emqx_ds, [passthrough, no_link]),
    ok = emqx_common_test_helpers:on_exit(fun() -> meck:unload(emqx_ds) end),
    {ok, MQ} = emqx_mq_test_utils:create_mq(#{
        name => Name, topic_filter => <<"purge/#">>, is_lastvalue => true
    }),
    DB = message_db(MQ),
    Slabs = maps:keys(emqx_ds:list_slabs(DB)),
    Record = message_record(MQ, <<"old">>, 1000, 1),
    %% Store a message and index in every slab before injecting the selected failure.
    lists:foreach(
        fun(Slab) -> write_records(DB, Slab, [Record, index_record(MQ, Index)]) end, Slabs
    ),
    Failure = {error, unrecoverable, test_error},
    Transactions =
        case FailIndex of
            true ->
                meck:val(Failure);
            false ->
                meck:loop([
                    meck:exec(fun(Opts, Fun) -> meck:passthrough([Opts, Fun]) end),
                    Failure
                ])
        end,
    ok = meck:expect(emqx_ds, trans, 2, Transactions),

    %% Failure must not advance the delivery cutoff or delete messages.
    ?assertMatch({error, [_ | _]}, emqx_mq_registry:purge(Name)),
    ?assertEqual({ok, MQ}, emqx_mq_registry:find(Name)),
    ?assertEqual(
        repeated_records([Record], Slabs), lists:sort(emqx_mq_message_db:dirty_read_all(MQ))
    ),
    %% Index failure preserves the old index; deletion failure preserves the committed reset.
    lists:foreach(
        fun(Slab) ->
            Stored = read_index(DB, Slab, MQ),
            case FailIndex of
                true ->
                    ?assertEqual(Index, Stored);
                false ->
                    TsUs = Stored#'StorageIndex'.maxTsUs,
                    ?assert(TsUs > 1000),
                    ?assertEqual(
                        Index#'StorageIndex'{
                            maxTsUs = TsUs,
                            common = [#'StorageIndexBucket'{tsUs = TsUs, bytes = 0, count = 0}]
                        },
                        Stored
                    )
            end
        end,
        Slabs
    ),
    case FailIndex of
        true -> ?assertNot(meck:called(emqx_ds, tx_del_topic, ['_', 0, '_']));
        false -> ok
    end,

    %% A retry resets the index, deletes old messages, and stores the same delivery cutoff.
    ok = meck:delete(emqx_ds, trans, 2),
    {ok, #{deliver_cutoff_date := CutoffMs}} = emqx_mq_registry:purge(Name),
    ?assertEqual([], emqx_mq_message_db:dirty_read_all(MQ)),
    lists:foreach(
        fun(Slab) ->
            ?assertEqual(
                Index#'StorageIndex'{
                    maxTsUs = CutoffMs * 1000,
                    common = [#'StorageIndexBucket'{tsUs = CutoffMs * 1000, bytes = 0, count = 0}]
                },
                read_index(DB, Slab, MQ)
            )
        end,
        Slabs
    ).

%% Check that full purge keeps messages and quota updates committed after the index reset.
t_full_purge_updates(Config) ->
    %% Store old data with a common-only index in one slab.
    Limits = #{max_shard_message_count => 1000, max_shard_message_bytes => 1000000},
    QueueOpts = queue_opts(Config, Limits),
    Stored = #'StorageIndex'{
        maxTsUs = 1000,
        common = [#'StorageIndexBucket'{tsUs = 1000, bytes = 100, count = 1}]
    },
    {ok, MQ} = emqx_mq_test_utils:create_mq(QueueOpts#{topic_filter => <<"purge/#">>}),
    DB = message_db(MQ),
    [Slab | _] = maps:keys(emqx_ds:list_slabs(DB)),
    Old = message_record(MQ, <<"old">>, 1000, 1),
    ok = write_records(DB, Slab, [Old, index_record(MQ, Stored)]),

    %% Commit new messages and buffered quota updates between reset and deletion.
    TxKey = {DB, Slab},
    ?force_ordering(
        #{?snk_kind := mq_message_db_purge_index_committed, tx_key := TxKey},
        #{?snk_kind := mq_purge_concurrent_write, ?snk_span := start, tx_key := TxKey}
    ),
    ?force_ordering(
        #{?snk_kind := mq_purge_concurrent_write, ?snk_span := {complete, _}, tx_key := TxKey},
        #{?snk_kind := mq_message_db_purge_delete, tx_key := TxKey}
    ),
    spawn_link(fun() ->
        ?tp_span(mq_purge_concurrent_write, #{tx_key => TxKey}, write_during_purge(MQ, TxKey))
    end),
    {ok, CutoffMs} = emqx_mq_message_db:purge(MQ),
    CutoffUs = CutoffMs * 1000,
    Records = emqx_mq_message_db:dirty_read_all(MQ),
    %% Verify strict deletion and retained counts without restoring removed legacy indices.
    ?assertEqual(
        [{CutoffUs, CutoffMs, <<"boundary">>}, {CutoffUs + 1000, CutoffMs + 1, <<"fresh">>}],
        lists:sort([
            {TsUs, emqx_message:timestamp(Message), emqx_message:payload(Message)}
         || {_, TsUs, Bin} <- Records,
            Message <- [emqx_mq_message_db:decode_message(Bin)]
        ])
    ),
    ?assertEqual(
        Stored#'StorageIndex'{
            maxTsUs = CutoffUs + 1000,
            common = [
                #'StorageIndexBucket'{tsUs = CutoffUs, bytes = record_bytes(Records), count = 2}
            ]
        },
        read_index(DB, Slab, MQ)
    ).

%% Check that bounded purge preserves the MQTT connection and subscription for messages published afterward.
t_bounded_purge_connected_client(Config) ->
    test_purge_connected_client(bounded, Config).

%% Check that full purge preserves the MQTT connection and subscription for messages published afterward.
t_full_purge_connected_client(Config) ->
    test_purge_connected_client(full, Config).

test_purge_connected_client(Purge, Config) ->
    %% Use distinct payloads to distinguish deliveries before and after purge.
    Limits = #{max_shard_message_count => 20, max_shard_message_bytes => infinity},
    QueueOpts = queue_opts(Config, Limits),
    BeforePayloads = [<<"before-", (integer_to_binary(I))/binary>> || I <- lists:seq(1, 5)],
    AfterPayloads = [<<"after-", (integer_to_binary(I))/binary>> || I <- lists:seq(1, 3)],
    #{name := Name, id := Id} = emqx_mq_test_utils:ensure_mq_created(QueueOpts#{
        topic_filter => <<"purge/#">>, ping_interval => 200
    }),
    %% Disable MQTT reconnect so a disconnected subscriber cannot silently recover.
    Publisher = emqx_mq_test_utils:emqtt_connect([]),
    Subscriber = emqx_mq_test_utils:emqtt_connect([{reconnect, 0}]),
    ok = emqx_mq_test_utils:emqtt_sub_mq(Subscriber, Name),

    %% Establish delivery through the subscription before purging the active queue.
    lists:foreach(
        fun(Payload) ->
            {ok, _} = emqx_mq_test_utils:emqtt_pub_mq(
                Publisher, <<"purge/1">>, Payload, #{key => Payload}
            )
        end,
        BeforePayloads
    ),
    {ok, BeforeMessages} = emqx_mq_test_utils:emqtt_drain(length(BeforePayloads), 1000),
    ?assertEqual(
        BeforePayloads,
        lists:sort([
            Payload
         || #{client_pid := Pid, payload := Payload} <- BeforeMessages, Pid =:= Subscriber
        ])
    ),
    %% Monitor the active consumer before purge updates the queue settings.
    {ok, Consumer} = emqx_mq_consumer:find(Id),
    ConsumerMonitor = monitor(process, Consumer),
    {ok, _} =
        case Purge of
            bounded -> emqx_mq_registry:purge(Name, erlang:system_time(millisecond));
            full -> emqx_mq_registry:purge(Name)
        end,

    %% Publish immediately after purge without reconnecting or subscribing again.
    lists:foreach(
        fun(Payload) ->
            {ok, _} = emqx_mq_test_utils:emqtt_pub_mq(
                Publisher, <<"purge/1">>, Payload, #{key => Payload}
            )
        end,
        AfterPayloads
    ),
    %% Verify that purge stops the consumer serving the original subscription.
    receive
        {'DOWN', ConsumerMonitor, process, Consumer, _} -> ok
    after 5000 -> ct:fail(consumer_not_stopped)
    end,
    %% Verify that the same MQTT subscriber receives every post-purge message.
    {ok, AfterMessages} = emqx_mq_test_utils:emqtt_drain(length(AfterPayloads), 5000),
    ?assertEqual(
        AfterPayloads,
        lists:sort([
            Payload
         || #{client_pid := Pid, payload := Payload} <- AfterMessages, Pid =:= Subscriber
        ])
    ),
    %% Verify that the original MQTT connection remains active and answers a ping.
    ?assertEqual(connected, emqtt:status(Subscriber)),
    ?assertEqual(pong, emqtt:ping(Subscriber)),
    ok = emqtt:disconnect(Subscriber),
    ok = emqtt:disconnect(Publisher),
    ok = emqx_mq_registry:delete(Name).

write_during_purge(MQ, {DB, {Shard, Generation} = Slab} = TxKey) ->
    %% Read the committed reset boundary before writing new messages.
    Stored = read_index(DB, Slab, MQ),
    CutoffUs = Stored#'StorageIndex'.maxTsUs,
    Boundary = message_record(MQ, <<"boundary">>, CutoffUs, CutoffUs div 1000),
    Fresh = message_record(MQ, <<"fresh">>, CutoffUs + 1000, CutoffUs div 1000 + 1),
    IndexTopic = index_topic(MQ),
    IndexOpts = emqx_mq_prop:quota_index_opts(MQ),
    %% Commit new data and out-of-order quota updates while purge waits before deletion.
    ?assertMatch(
        {atomic, _, ok},
        emqx_ds:trans(#{db => DB, shard => Shard, generation => Generation}, fun() ->
            Index0 = emqx_mq_message_db:quota_buffer_flush_tx_read(TxKey, IndexTopic, IndexOpts),
            ?assertEqual(Index0, emqx_mq_quota_index:apply_update(Index0, {1000, 100, 1})),
            Index1 = emqx_mq_quota_index:apply_updates(Index0, [
                {CutoffUs + 1000, record_bytes([Fresh]), 1},
                {CutoffUs, record_bytes([Boundary]), 1},
                {1000, -100, -1}
            ]),
            ok = emqx_ds:tx_write(Boundary),
            ok = emqx_ds:tx_write(Fresh),
            emqx_mq_message_db:quota_buffer_flush_tx_write(TxKey, IndexTopic, Index1)
        end)
    ).

queue_opts(Config, Limits) ->
    Opts = #{name => ?config(queue_name, Config), is_lastvalue => ?config(is_lastvalue, Config)},
    case ?config(is_limited, Config) of
        true -> Opts#{limits => Limits};
        false -> Opts
    end.

message_db(MQ) ->
    case emqx_mq_prop:is_append_only(MQ) of
        true -> ?MQ_MESSAGE_REGULAR_DB;
        false -> ?MQ_MESSAGE_LASTVALUE_DB
    end.

message_record(#{topic_filter := TopicFilter, id := Id}, Payload, StoredUs, PublishedMs) ->
    Message = emqx_message:make(<<"publisher">>, 1, <<"purge/1">>, Payload),
    {
        [<<"topic">>, TopicFilter, Id, <<"key">>, Payload],
        StoredUs,
        emqx_mq_message_db:encode_message(Message#message{timestamp = PublishedMs})
    }.

index_topic(#{topic_filter := TopicFilter, id := Id}) ->
    [<<"topic">>, TopicFilter, Id, <<"index">>].

index_record(MQ, Index) ->
    {index_topic(MQ), ?QUOTA_INDEX_TS, 'MessageQueue':encode('StorageIndex', Index)}.

write_records(DB, {Shard, Generation}, Records) ->
    ?assertMatch(
        {atomic, _, ok},
        emqx_ds:trans(#{db => DB, shard => Shard, generation => Generation}, fun() ->
            lists:foreach(fun emqx_ds:tx_write/1, Records)
        end)
    ).

read_index(DB, {Shard, Generation}, MQ) ->
    [{_, ?QUOTA_INDEX_TS, Bin}] = emqx_ds:dirty_read(
        #{db => DB, shard => Shard, generation => Generation}, index_topic(MQ)
    ),
    'MessageQueue':decode('StorageIndex', Bin).

record_bytes(Records) ->
    lists:sum([byte_size(Bin) || {_, _, Bin} <- Records]).

repeated_records(Records, Slabs) ->
    lists:sort(lists:append([Records || _ <- Slabs])).
