%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mq_quota_index_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include("../src/emqx_mq_internal.hrl").
-include_lib("../src/emqx_mq_quota/emqx_mq_quota.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

all() ->
    [t_byte_limit_regular, t_delete_all, {group, lastvalue10}, {group, lastvalue1000}].

groups() ->
    [
        {lastvalue10, [], [t_byte_limit_lastvalue]},
        {lastvalue1000, [], [t_byte_limit_lastvalue]}
    ].

init_per_group(lastvalue10, Config) ->
    [{key_count, 10} | Config];
init_per_group(lastvalue1000, Config) ->
    [{key_count, 1000} | Config].

end_per_group(_Group, _Config) ->
    ok.

%% We have:
%% Each payload is 1 - 2000 bytes, i.e 1000 bytes average
%% The size limit is 1000 * 1000 / 4 = 250000 bytes
%%
%% The Threshold is 10% of the max, i.e 25000 bytes
%% In the end of test, we should have < 250000 * 1.1 = 275000 bytes.
t_byte_limit_regular(_Config) ->
    Index0 = emqx_mq_quota_index:new(
        #{bytes => 250000, threshold_percentage => 10}, now_us()
    ),
    NowTime = now_us(),
    EndTime = NowTime + 2_000_000,
    {Index, _} = loop_insert(
        Index0,
        fun(IndexAcc0, NowUs, St) ->
            IndexAcc =
                emqx_mq_quota_index:apply_update(
                    IndexAcc0,
                    ?QUOTA_INDEX_UPDATE(NowUs, payload_size(2000), 0)
                ),
            {IndexAcc, St}
        end,
        undefined,
        EndTime
    ),
    Info = emqx_mq_quota_index:inspect(Index),
    ct:pal("Index: ~p", [Info]),
    #{bytes := #{total := BytesTotal, size := BytesIndexSize}} = Info,
    ?assert(BytesTotal < 250000 * 1.2),
    ?assert(BytesIndexSize =< 11),
    ok.

%% We have:
%% Each payload is 1 - 2000 bytes, i.e 1000 bytes average
%% The size limit is 1000 * 1000 / 4 = 250000 bytes, thus allowing ~250 distinct keys.
%% We test with 1000 and 10 keys.
%% With 1000 keys, the index should be full in the end of the test.
%% With 10 keys, it should be almost empty.
%%
%% The Threshold is 10% of the max, i.e 25000 bytes
%% In the end of test, we should have < 250000 * 1.1 = 275000 bytes.
t_byte_limit_lastvalue(Config) ->
    KeyCount = ?config(key_count, Config),
    Index0 = emqx_mq_quota_index:new(
        #{bytes => 250000, threshold_percentage => 10}, now_us()
    ),
    NowTime = now_us(),
    EndTime = NowTime + 2_000_000,
    DB0 = #{},
    {Index, _DB} = loop_insert(
        Index0,
        fun(IndexAcc0, NowUs, DBAcc0) ->
            Key = key(KeyCount),
            DelUpdate =
                case DBAcc0 of
                    #{Key := ?QUOTA_INDEX_UPDATE(PrevTsUs, PrevBytes, PrevCount)} ->
                        [?QUOTA_INDEX_UPDATE(PrevTsUs, -PrevBytes, -PrevCount)];
                    _ ->
                        []
                end,
            InsertUpdate =
                ?QUOTA_INDEX_UPDATE(NowUs, payload_size(2000), 0),
            DBAcc = DBAcc0#{Key => InsertUpdate},
            IndexAcc =
                emqx_mq_quota_index:apply_updates(IndexAcc0, DelUpdate ++ [InsertUpdate]),
            {IndexAcc, DBAcc}
        end,
        DB0,
        EndTime
    ),
    Info = emqx_mq_quota_index:inspect(Index),
    ct:pal("Index: ~p", [Info]),
    #{bytes := #{total := BytesTotal, size := BytesIndexSize}} = Info,
    ?assert(BytesTotal < 250000 * 1.2),
    ?assert(BytesIndexSize =< 21),
    ok.

%% Verify that after deleting all messages, the index shrinks.
t_delete_all(_Config) ->
    KeyCount = 10000,
    PayloadSize = 200,
    Index0 = emqx_mq_quota_index:new(
        #{bytes => 250000, threshold_percentage => 10}, now_us()
    ),
    %% Fill the index with messages
    {Index1, DB} = lists:foldl(
        fun(Key, {IndexAcc0, DBAcc0}) ->
            TsUs = now_us(),
            Update = ?QUOTA_INDEX_UPDATE(TsUs, payload_size(PayloadSize), 0),
            IndexAcc = emqx_mq_quota_index:apply_update(
                IndexAcc0,
                Update
            ),
            DBAcc = DBAcc0#{Key => Update},
            {IndexAcc, DBAcc}
        end,
        {Index0, #{}},
        lists:seq(1, KeyCount)
    ),
    Info1 = #{bytes := #{size := BytesIndexSize1}} = emqx_mq_quota_index:inspect(Index1),
    ct:pal("Index before delete: ~p", [Info1]),
    ?assert(BytesIndexSize1 >= 10),
    Index2 = maps:fold(
        fun(_Key, ?QUOTA_INDEX_UPDATE(TsUs, Bytes, Count), IndexAcc) ->
            DeleteUpdate = ?QUOTA_INDEX_UPDATE(TsUs, -Bytes, -Count),
            emqx_mq_quota_index:apply_update(IndexAcc, DeleteUpdate)
        end,
        Index1,
        DB
    ),
    Info2 =
        #{
            bytes := #{
                size := BytesIndexSize2,
                total := BytesTotal2
            }
        } = emqx_mq_quota_index:inspect(Index2),
    ct:pal("Index after delete: ~p", [Info2]),
    ?assertEqual(0, BytesTotal2),
    ?assertEqual(2, BytesIndexSize2),
    ok.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

loop_insert(Index, GenF, GenSt, EndTime) ->
    NowUs = now_us(),
    case NowUs >= EndTime of
        true ->
            {Index, GenSt};
        false ->
            {Index1, NewGenSt} = GenF(Index, NowUs, GenSt),
            loop_insert(Index1, GenF, NewGenSt, EndTime)
    end.

payload_size(MaxBytes) ->
    rand:uniform(MaxBytes).

key(KeyCount) ->
    rand:uniform(KeyCount).

now_us() -> erlang:system_time(microsecond).
