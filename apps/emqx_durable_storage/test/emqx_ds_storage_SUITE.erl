%%--------------------------------------------------------------------
%% Copyright (c) 2024-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_ds_storage_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include("../../emqx/include/emqx.hrl").
-include("emqx_ds.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

suite() -> [{timetrap, {minutes, 1}}].

opts(Config) ->
    Group = proplists:get_value(db_group, Config),
    #{
        storage => {emqx_ds_storage_skipstream_lts_v2, #{}},
        payload_type => ?ds_pt_ttv,
        rocksdb => #{},
        db_group => Group
    }.

%%

t_snapshot_take_restore(Config) ->
    Shard = {?FUNCTION_NAME, _ShardId = <<"42">>},
    {ok, Pid} = emqx_ds_storage_layer:start_link(Shard, opts(Config)),

    %% Push some data to the shard.
    Batch1 = [gen_payload(N) || N <- lists:seq(1000, 2000)],
    ?assertEqual(ok, store_batch(Shard, 1, 1, Batch1)),

    %% Add new generation and push some more.
    ?assertEqual(ok, emqx_ds_storage_layer:add_generation(Shard, 3000)),
    Batch2 = [gen_payload(N) || N <- lists:seq(4000, 5000)],
    ?assertEqual(ok, store_batch(Shard, 2, 2, Batch2)),
    ?assertEqual(ok, emqx_ds_storage_layer:add_generation(Shard, 6000)),

    %% Take a snapshot of the shard.
    {ok, SnapReader} = emqx_ds_storage_layer:take_snapshot(Shard),

    %% Push even more messages to the shard AFTER taking the snapshot.
    Batch3 = [gen_payload(N) || N <- lists:seq(7000, 8000)],
    ?assertEqual(ok, store_batch(Shard, 2, 3, Batch3)),

    %% Destroy the shard.
    ok = stop_shard(Pid),
    ok = emqx_ds_storage_layer:drop_shard(Shard),

    %% Restore the shard from the snapshot.
    {ok, SnapWriter} = emqx_ds_storage_layer:accept_snapshot(Shard),
    ?assertEqual(ok, transfer_snapshot(SnapReader, SnapWriter)),

    %% Verify that the restored shard contains the messages up until the snapshot.
    {ok, _Pid} = emqx_ds_storage_layer:start_link(Shard, opts(Config)),
    snabbkaffe_diff:assert_lists_eq(
        Batch1 ++ Batch2,
        %% Sort by timestamp (2nd element):
        lists:keysort(2, emqx_ds_test_helpers:storage_consume(Shard, ['#']))
    ).

transfer_snapshot(Reader, Writer) ->
    ChunkSize = rand:uniform(1024),
    ReadResult = emqx_ds_storage_snapshot:read_chunk(Reader, ChunkSize),
    ?assertMatch({RStatus, _, _} when RStatus == next; RStatus == last, ReadResult),
    {RStatus, Chunk, NReader} = ReadResult,
    Data = iolist_to_binary(Chunk),
    {WStatus, NWriter} = emqx_ds_storage_snapshot:write_chunk(Writer, Data),
    %% Verify idempotency.
    ?assertMatch(
        {WStatus, NWriter},
        emqx_ds_storage_snapshot:write_chunk(NWriter, Data)
    ),
    %% Verify convergence.
    ?assertEqual(
        RStatus,
        WStatus,
        #{reader => NReader, writer => NWriter}
    ),
    case WStatus of
        last ->
            ?assertEqual(ok, emqx_ds_storage_snapshot:release_reader(NReader)),
            ?assertEqual(ok, emqx_ds_storage_snapshot:release_writer(NWriter)),
            ok;
        next ->
            transfer_snapshot(NReader, NWriter)
    end.

%%

gen_payload(N) ->
    Topic = [<<"foo">>, <<"bar">>, integer_to_binary(N)],
    {Topic, N, crypto:strong_rand_bytes(16)}.

stop_shard(Pid) ->
    _ = unlink(Pid),
    proc_lib:stop(Pid, shutdown, infinity).

store_batch(DBShard, Generation, Serial, Batch) ->
    {ok, CookedBatch} = emqx_ds_storage_layer_ttv:prepare_tx(
        DBShard,
        Generation,
        <<Serial:64>>,
        #{?ds_tx_write => Batch},
        #{}
    ),
    emqx_ds_storage_layer_ttv:commit_batch(
        DBShard,
        [{Generation, [CookedBatch]}],
        #{durable => true}
    ).

%%

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_testcase(TCName, Config) ->
    WorkDir = emqx_cth_suite:work_dir(TCName, Config),
    Apps = emqx_cth_suite:start(
        [{emqx_durable_storage, #{override_env => [{db_data_dir, WorkDir}]}}],
        #{work_dir => WorkDir}
    ),
    {ok, Group} = emqx_ds_storage_layer:create_db_group(TCName, #{}),
    [{apps, Apps}, {db_group, Group} | Config].

end_per_testcase(TCName, Config) ->
    Group = proplists:get_value(db_group, Config),
    emqx_ds_storage_layer:destroy_db_group(TCName, Group),
    ok = emqx_cth_suite:stop(?config(apps, Config)),
    ok.
