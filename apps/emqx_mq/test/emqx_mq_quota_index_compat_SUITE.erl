%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mq_quota_index_compat_SUITE).

-moduledoc """
Rolling-upgrade tests for the quota index record.
`MessageQueueOld.asn` is the schema of nodes that have only the legacy index.
The suite compiles it as module `MessageQueueOld`, to encode and decode records as an older node does.
""".

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include("../src/emqx_mq_quota/emqx_mq_quota.hrl").

-define(PUB_TS_US, 9_000_000).

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    PrivDir = ?config(priv_dir, Config),
    Schema = filename:join(?config(data_dir, Config), "MessageQueueOld.asn"),
    ok = asn1ct:compile(Schema, [ber, no_ok_wrapper, {outdir, PrivDir}]),
    true = code:add_patha(PrivDir),
    {module, 'MessageQueueOld'} = code:load_file('MessageQueueOld'),
    Config.

end_per_suite(Config) ->
    _ = code:purge('MessageQueueOld'),
    _ = code:delete('MessageQueueOld'),
    _ = code:del_path(?config(priv_dir, Config)),
    ok.

end_per_testcase(_TestCase, _Config) ->
    emqx_common_test_helpers:call_janitor().

-doc "An older node reads the legacy lists from a record that also holds serial buckets".
t_old_node_reads_new_record(_Config) ->
    Bin = encode(new_index_with_data()),
    {'StorageIndex', MaxTsUs, Count, Bytes, [_ | _], _SeenFenceSerial, _ReleasedBeforeUs} =
        'MessageQueue':decode('StorageIndex', Bin),
    ?assertEqual(
        {'StorageIndex', MaxTsUs, Count, Bytes},
        'MessageQueueOld':decode('StorageIndex', Bin)
    ).

-doc "After an older node rewrites the record, a new node rebuilds the serial index and ignores the old fence".
t_new_node_reads_old_record(_Config) ->
    %% An older node decodes the record and writes it back without the serial buckets.
    OldBin = 'MessageQueueOld':encode(
        'StorageIndex', 'MessageQueueOld':decode('StorageIndex', encode(new_index_with_data()))
    ),
    Index0 = emqx_mq_quota_index:decode(opts(), OldBin),
    #{legacy := #{count := #{total := 2}}, serial := #{records := Records0}} =
        emqx_mq_quota_index:inspect(Index0),
    ?assertEqual([bucket(0, 20, 2)], Records0),
    %% The fence in DS was written before the rewrite, so it is ignored.
    {Index1, true} = emqx_mq_quota_index:apply_updates(Index0, [add(11, 10, 1)], fence(10)),
    #{serial := #{records := Records1}} = emqx_mq_quota_index:inspect(Index1),
    ?assertEqual([bucket(0, 30, 3)], Records1),
    %% The next fence belongs to the rebuilt index and is used.
    {Index2, false} = emqx_mq_quota_index:apply_updates(Index1, [add(21, 10, 1)], fence(20)),
    #{serial := #{records := Records2}} = emqx_mq_quota_index:inspect(Index2),
    ?assertEqual([bucket(20, 10, 1), bucket(0, 30, 3)], Records2).

-doc "An older node cannot decode a record without legacy lists, so the legacy index is dropped only after the upgrade".
t_old_node_cannot_read_serial_only_record(_Config) ->
    Bin = encode(emqx_mq_quota_index:drop_legacy(new_index_with_data())),
    ?assertExit({error, {asn1, _}}, 'MessageQueueOld':decode('StorageIndex', Bin)).

-doc "Each index is updated by its own timestamp; the serial deadline is used after the legacy index is dropped".
t_both_indices(_Config) ->
    Index0 = new_index_with_data(),
    {Index1, true} = emqx_mq_quota_index:apply_updates(
        decode(encode(Index0)), [add(11, 100, 10)], fence(10)
    ),
    %% Replace the first message. The legacy index finds it by publication time,
    %% the serial index by DS timestamp.
    Del = #quota_del{pub_ts_us = pub_ts(1), ds_ts_us = ts(1), bytes = 20, count = 2},
    {Index2, true} = emqx_mq_quota_index:apply_updates(Index1, [Del], undefined),
    ?assertMatch(
        #{
            legacy := #{bytes := #{total := 100}, count := #{total := 10}},
            serial := #{total := #{bytes := 100, count := 10}}
        },
        emqx_mq_quota_index:inspect(Index2)
    ),
    %% The legacy deadline is a publication time.
    ?assert(emqx_mq_quota_index:deadline(Index2) >= ?PUB_TS_US),
    Index3 = decode(encode(emqx_mq_quota_index:drop_legacy(Index2))),
    ?assertMatch(#{legacy := undefined}, emqx_mq_quota_index:inspect(Index3)),
    %% The serial deadline is a DS timestamp.
    ?assertEqual(ts(10), emqx_mq_quota_index:deadline(Index3)).

-doc "The legacy index is dropped only when every node supports version 2 of the consumer BPAPI".
t_bpapi_guard(_Config) ->
    lists:foreach(
        fun(#{version := Version, expected := Expected}) ->
            ok = emqx_mq_test_utils:stub_bpapi_supported_version(emqx_mq_consumer, Version),
            ?assertEqual(Expected, emqx_mq_quota_index:is_serial_index_supported())
        end,
        [
            #{version => 1, expected => false},
            #{version => 2, expected => true},
            %% No node has announced the API yet.
            #{version => fun() -> error(badarg) end, expected => false}
        ]
    ).

%%------------------------------------------------------------------------------
%% Helpers
%%------------------------------------------------------------------------------

%% Limits: 100 bytes and 10 messages. Threshold: 20 bytes or 2 messages.
opts() ->
    #{bytes => 100, count => 10, threshold_percentage => 20}.

%% A transaction with serial S has DS timestamp S * 100.
ts(Serial) ->
    Serial * 100.

%% Publication times are far from DS timestamps, so the two indices place messages differently.
pub_ts(Serial) ->
    ?PUB_TS_US + Serial.

add(Serial, Bytes, Count) ->
    #quota_add{pub_ts_us = pub_ts(Serial), serial = Serial, bytes = Bytes, count = Count}.

fence(Serial) ->
    #{serial => Serial, ts_us => ts(Serial)}.

bucket(Serial, Bytes, Count) ->
    #{serial => Serial, ts_us => ts(Serial), bytes => Bytes, count => Count}.

%% An index with both formats and one message of 20 bytes counted as 2.
new_index_with_data() ->
    Index0 = emqx_mq_quota_index:new(opts(), ?PUB_TS_US, true),
    {Index, true} = emqx_mq_quota_index:apply_updates(Index0, [add(1, 20, 2)], undefined),
    Index.

encode(Index) ->
    emqx_mq_quota_index:encode(Index).

decode(Bin) ->
    emqx_mq_quota_index:decode(opts(), Bin).
