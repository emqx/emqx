%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mq_quota_index_tx_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("emqx_durable_storage/include/emqx_ds.hrl").
-include("../src/emqx_mq_internal.hrl").
-include("../src/emqx_mq_quota/emqx_mq_quota.hrl").

-define(INDEX_TOPIC, [<<"topic">>, <<"q/#">>, <<"id">>, <<"index">>]).
-define(FENCE_TOPIC, [<<"topic">>, <<"q/#">>, <<"id">>, <<"fence">>]).
-define(MESSAGE_TOPIC, [<<"topic">>, <<"q/#">>, <<"id">>, <<"key">>, <<"k">>]).

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [emqx_durable_storage, emqx_ds_builtin_raft],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    emqx_cth_suite:stop(?config(apps, Config)).

init_per_testcase(TestCase, Config) ->
    ok = emqx_ds:open_db(TestCase, #{
        backend => builtin_raft,
        storage => {emqx_ds_storage_skipstream_lts_v2, #{}},
        payload_type => ?ds_pt_ttv,
        n_shards => 1,
        n_sites => 1,
        replication_factor => 1
    }),
    ok = emqx_ds:wait_db(TestCase, all, 30000),
    [{db, TestCase} | Config].

end_per_testcase(_TestCase, Config) ->
    ok = emqx_ds:drop_db(?config(db, Config)).

-doc """
A fence gets the serial and timestamp of its flush transaction.
The next flush uses it once and keeps it. The fence is replaced only when the index asks again.
""".
t_fence_lifecycle(Config) ->
    DB = ?config(db, Config),
    Opts = #{bytes => infinity, count => 10, threshold_percentage => 20},
    %% The first flush fills the head and writes a fence.
    Serial0 = write_message(DB),
    flush(DB, Opts, [add(Serial0, 2)], true),
    [{_, FenceTsUs, FenceSerialBin}] = read(DB, ?FENCE_TOPIC),
    FenceSerial = binary:decode_unsigned(FenceSerialBin),
    ?assert(FenceSerial > Serial0),
    %% The second flush closes the head at the fence and marks it seen. The fence stays.
    Serial1 = write_message(DB),
    flush(DB, Opts, [add(Serial1, 1)], false),
    ?assertEqual([{?FENCE_TOPIC, FenceTsUs, FenceSerialBin}], read(DB, ?FENCE_TOPIC)),
    ?assertMatch(
        #{
            serial := #{
                records := [#{ts_us := FenceTsUs, count := 1}, #{ts_us := 0, count := 2}],
                seen_fence_serial := FenceSerial
            }
        },
        emqx_mq_quota_index:inspect(read_index(DB, Opts))
    ),
    %% The third flush fills the head again. The seen fence cannot close it, so it is replaced.
    Serial2 = write_message(DB),
    flush(DB, Opts, [add(Serial2, 1)], false),
    [{_, _, NewFenceSerialBin}] = read(DB, ?FENCE_TOPIC),
    ?assert(binary:decode_unsigned(NewFenceSerialBin) > Serial2).

-doc "Deleting messages before the deadline keeps the newest messages that the limit requires".
t_cleanup_boundary(Config) ->
    DB = ?config(db, Config),
    Opts = #{bytes => infinity, count => 2, threshold_percentage => 50},
    Serial0 = write_message(DB),
    flush(DB, Opts, [add(Serial0, 1)], false),
    Serial1 = write_message(DB),
    Serial2 = write_message(DB),
    flush(DB, Opts, [add(Serial1, 1), add(Serial2, 1)], false),
    Deadline = emqx_mq_quota_index:deadline(read_index(DB, Opts)),
    ?assert(Deadline > 0),
    {atomic, _, ok} = trans(DB, fun() -> emqx_ds:tx_del_topic(?MESSAGE_TOPIC, 0, Deadline) end),
    Remaining = read(DB, ?MESSAGE_TOPIC),
    ?assertEqual(2, length(Remaining)),
    ?assert(lists:all(fun({_, TsUs, _}) -> TsUs >= Deadline end, Remaining)).

-doc "Concurrent flushes of one index conflict and retry, so every update is counted and at most one fence remains".
t_concurrent_flushes(Config) ->
    DB = ?config(db, Config),
    Opts = #{bytes => infinity, count => 100, threshold_percentage => 1},
    Serials = [write_message(DB) || _ <- lists:seq(1, 8)],
    ok = emqx_utils:pforeach(
        fun(Serial) -> flush(DB, Opts, [add(Serial, 1)], false) end,
        Serials,
        infinity
    ),
    %% When no node needs the legacy index, a new index is created without it.
    ?assertMatch(
        #{legacy := undefined, serial := #{total := #{count := 8}}},
        emqx_mq_quota_index:inspect(read_index(DB, Opts))
    ),
    ?assert(length(read(DB, ?FENCE_TOPIC)) =< 1).

-doc "Rotation at a fresh fence starts an empty bucket at the fence and drops older buckets".
t_rotate(Config) ->
    DB = ?config(db, Config),
    Opts = #{bytes => infinity, count => 10, threshold_percentage => 20},
    Serial0 = write_message(DB),
    flush(DB, Opts, [add(Serial0, 1)], false),
    %% Write a fresh fence and rotate at it in the next transaction.
    {FenceSerial, ok} = trans_result(DB, fun() ->
        emqx_mq_quota_index_tx:write_fence_tx(?FENCE_TOPIC)
    end),
    {_, {ok, #{serial := FenceSerial, ts_us := FenceTsUs}}} = trans_result(DB, fun() ->
        emqx_mq_quota_index_tx:rotate_tx(?INDEX_TOPIC, ?FENCE_TOPIC, Opts)
    end),
    ?assertMatch(
        #{
            legacy := undefined,
            serial := #{
                records := [#{serial := FenceSerial, ts_us := FenceTsUs, count := 0}],
                seen_fence_serial := FenceSerial
            }
        },
        emqx_mq_quota_index:inspect(read_index(DB, Opts))
    ),
    %% The message stored before the fence is older than the new bucket.
    [{_, MessageTsUs, _}] = read(DB, ?MESSAGE_TOPIC),
    ?assert(MessageTsUs < FenceTsUs).

-doc "Rotation at a fence that a flush has seen without closing the head starts an empty head there".
t_rotate_seen_fence(Config) ->
    DB = ?config(db, Config),
    Opts = #{bytes => infinity, count => 10, threshold_percentage => 20},
    Serial0 = write_message(DB),
    flush(DB, Opts, [add(Serial0, 1)], false),
    {FenceSerial, ok} = trans_result(DB, fun() ->
        emqx_mq_quota_index_tx:write_fence_tx(?FENCE_TOPIC)
    end),
    %% A flush sees the fence. The head is not full, so the flush only marks it seen.
    Serial1 = write_message(DB),
    flush(DB, Opts, [add(Serial1, 0)], false),
    {_, {ok, #{serial := FenceSerial}}} = trans_result(DB, fun() ->
        emqx_mq_quota_index_tx:rotate_tx(?INDEX_TOPIC, ?FENCE_TOPIC, Opts)
    end),
    %% The new head misses the message that the flush counted after the fence.
    ?assertMatch(
        #{serial := #{records := [#{serial := FenceSerial, count := 0}]}},
        emqx_mq_quota_index:inspect(read_index(DB, Opts))
    ).

-doc "Truncation drops the buckets that end at or before the given time".
t_truncate(Config) ->
    DB = ?config(db, Config),
    Opts = #{bytes => infinity, count => 10, threshold_percentage => 20},
    %% Fill the head twice, so the second flush closes it at the first fence.
    Serial0 = write_message(DB),
    flush(DB, Opts, [add(Serial0, 2)], false),
    [{_, FenceTsUs, _}] = read(DB, ?FENCE_TOPIC),
    Serial1 = write_message(DB),
    flush(DB, Opts, [add(Serial1, 1)], false),
    %% Truncation before the boundary keeps everything.
    ?assertMatch(
        {_, ok},
        trans_result(DB, fun() ->
            emqx_mq_quota_index_tx:truncate_tx(?INDEX_TOPIC, Opts, FenceTsUs - 1)
        end)
    ),
    ?assertMatch(
        #{serial := #{size := 2}}, emqx_mq_quota_index:inspect(read_index(DB, Opts))
    ),
    %% A later truncation drops the bucket before the boundary.
    ?assertMatch(
        {_, ok},
        trans_result(DB, fun() ->
            emqx_mq_quota_index_tx:truncate_tx(?INDEX_TOPIC, Opts, FenceTsUs + 1)
        end)
    ),
    ?assertMatch(
        #{serial := #{records := [#{ts_us := FenceTsUs, count := 1}]}},
        emqx_mq_quota_index:inspect(read_index(DB, Opts))
    ).

%%------------------------------------------------------------------------------
%% Helpers
%%------------------------------------------------------------------------------

trans(DB, Fun) ->
    emqx_ds:trans(
        #{db => DB, shard => <<"0">>, generation => 1, sync => true, retries => 100}, Fun
    ).

write_message(DB) ->
    {atomic, Serial, ok} = trans(DB, fun() ->
        emqx_ds:tx_write({?MESSAGE_TOPIC, ?ds_tx_ts_monotonic, <<"message">>})
    end),
    binary:decode_unsigned(Serial).

read(DB, Topic) ->
    emqx_ds:dirty_read(#{db => DB, shard => <<"0">>, generation => 1}, Topic).

read_index(DB, Opts) ->
    [{_, ?QUOTA_INDEX_TS, Bin}] = read(DB, ?INDEX_TOPIC),
    emqx_mq_quota_index:decode(Opts, Bin).

%% Publication time 1 is far before DS time, as if the publisher clock were wrong.
add(Serial, Count) ->
    #quota_add{pub_ts_us = 1, serial = Serial, bytes = Count * 7, count = Count}.

trans_result(DB, Fun) ->
    {atomic, SerialBin, Result} = trans(DB, Fun),
    {binary:decode_unsigned(SerialBin), Result}.

flush(DB, Opts, Updates, NeedLegacy) ->
    {atomic, _, ok} = trans(DB, fun() ->
        emqx_mq_quota_index_tx:update_tx(?INDEX_TOPIC, ?FENCE_TOPIC, Opts, Updates, NeedLegacy)
    end),
    ok.
