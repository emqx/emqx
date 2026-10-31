%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mq_quota_index_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include("../src/emqx_mq_internal.hrl").
-include_lib("../src/emqx_mq_quota/emqx_mq_quota.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

all() ->
    [
        {group, with_legacy},
        {group, without_legacy}
    ].

groups() ->
    Cases = [
        t_byte_limit_regular,
        t_delete_all,
        t_consistency_property,
        {group, lastvalue10},
        {group, lastvalue1000}
    ],
    [
        {with_legacy, [], Cases},
        {without_legacy, [], Cases},
        {lastvalue10, [], [t_byte_limit_lastvalue]},
        {lastvalue1000, [], [t_byte_limit_lastvalue]}
    ].

init_per_group(with_legacy, Config) ->
    [{legacy_updates, true} | Config];
init_per_group(without_legacy, Config) ->
    [{legacy_updates, false} | Config];
init_per_group(lastvalue10, Config) ->
    [{key_count, 10} | Config];
init_per_group(lastvalue1000, Config) ->
    [{key_count, 1000} | Config].

end_per_group(_Group, _Config) ->
    ok.

%% Check that repeated inserts leave fewer than 300000 indexed bytes in at most 11 buckets.
t_byte_limit_regular(Config) ->
    Index0 = new_index(
        #{bytes => 250000, threshold_percentage => 10}, now_us(), Config
    ),
    NowTime = now_us(),
    EndTime = NowTime + 2_000_000,
    {Index1, _} = loop_insert(
        Index0,
        fun(IndexAcc0, NowUs, St) ->
            IndexAcc1 =
                emqx_mq_quota_index:apply_update(
                    IndexAcc0,
                    ?QUOTA_INDEX_UPDATE(NowUs, payload_size(2000), 0)
                ),
            {IndexAcc1, St}
        end,
        undefined,
        EndTime
    ),
    Info = emqx_mq_quota_index:inspect(Index1),
    ct:pal("Index: ~p", [Info]),
    #{common := #{total := #{bytes := BytesTotal}, size := BytesIndexSize}} = Info,
    ?assert(BytesTotal < 250000 * 1.2),
    ?assert(BytesIndexSize =< 11),
    case ?config(legacy_updates, Config) of
        true ->
            #{bytes := #{total := LegacyTotal, size := LegacySize}} = Info,
            ?assert(LegacyTotal < 250000 * 1.2),
            ?assert(LegacySize =< 11);
        false ->
            ?assertMatch(#{bytes := undefined, count := undefined}, Info)
    end,
    ok.

%% Check that removing every inserted message leaves zero indexed bytes and two buckets.
t_delete_all(Config) ->
    KeyCount = 10000,
    PayloadSize = 200,
    Index0 = new_index(
        #{bytes => 250000, threshold_percentage => 10}, now_us(), Config
    ),
    %% Insert enough messages to create at least ten buckets.
    {Index1, DB} = lists:foldl(
        fun(Key, {IndexAcc0, DBAcc0}) ->
            TsUs = now_us(),
            Update = ?QUOTA_INDEX_UPDATE(TsUs, payload_size(PayloadSize), 0),
            IndexAcc1 = emqx_mq_quota_index:apply_update(
                IndexAcc0,
                Update
            ),
            DBAcc1 = DBAcc0#{Key => Update},
            {IndexAcc1, DBAcc1}
        end,
        {Index0, #{}},
        lists:seq(1, KeyCount)
    ),
    Info1 = #{common := #{size := BytesIndexSize1}} = emqx_mq_quota_index:inspect(Index1),
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
            common := #{
                size := BytesIndexSize2,
                total := #{bytes := BytesTotal2}
            }
        } = emqx_mq_quota_index:inspect(Index2),
    ct:pal("Index after delete: ~p", [Info2]),
    ?assertEqual(0, BytesTotal2),
    ?assertEqual(2, BytesIndexSize2),
    case ?config(legacy_updates, Config) of
        true ->
            #{bytes := #{size := LegacySize1}} = Info1,
            ?assert(LegacySize1 >= 10),
            ?assertMatch(#{bytes := #{total := 0, size := 2}}, Info2);
        false ->
            ?assertMatch(#{bytes := undefined, count := undefined}, Info1),
            ?assertMatch(#{bytes := undefined, count := undefined}, Info2)
    end,
    ok.

%% Check that repeated message replacements leave fewer than 300000 indexed bytes in at most 21 buckets.
t_byte_limit_lastvalue(Config) ->
    KeyCount = ?config(key_count, Config),
    Index0 = new_index(
        #{bytes => 250000, threshold_percentage => 10}, now_us(), Config
    ),
    NowTime = now_us(),
    EndTime = NowTime + 2_000_000,
    DB0 = #{},
    {Index1, _DB1} = loop_insert(
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
            DBAcc1 = DBAcc0#{Key => InsertUpdate},
            IndexAcc1 =
                emqx_mq_quota_index:apply_updates(IndexAcc0, DelUpdate ++ [InsertUpdate]),
            {IndexAcc1, DBAcc1}
        end,
        DB0,
        EndTime
    ),
    Info = emqx_mq_quota_index:inspect(Index1),
    ct:pal("Index: ~p", [Info]),
    #{common := #{total := #{bytes := BytesTotal}, size := BytesIndexSize}} = Info,
    ?assert(BytesTotal < 250000 * 1.2),
    ?assert(BytesIndexSize =< 21),
    case ?config(legacy_updates, Config) of
        true ->
            #{bytes := #{total := LegacyTotal, size := LegacySize}} = Info,
            ?assert(LegacyTotal < 250000 * 1.2),
            ?assert(LegacySize =< 21);
        false ->
            ?assertMatch(#{bytes := undefined, count := undefined}, Info)
    end,
    ok.

%% Check that randomized replacements followed by zero-byte replacements leave zero bytes and one message per key.
t_consistency_property(Config) ->
    Property = emqx_mq_quota_index:t_consistency(?config(legacy_updates, Config)),
    ?assert(proper:quickcheck(Property, [{numtests, 1000}, {to_file, user}])).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

new_index(Opts, TsUs, Config) ->
    Index = emqx_mq_quota_index:new(Opts, TsUs),
    case ?config(legacy_updates, Config) of
        true -> Index;
        false -> emqx_mq_quota_index:drop_legacy(Index)
    end.

loop_insert(Index0, GenF, GenSt0, EndTime) ->
    NowUs = now_us(),
    case NowUs >= EndTime of
        true ->
            {Index0, GenSt0};
        false ->
            {Index1, GenSt1} = GenF(Index0, NowUs, GenSt0),
            loop_insert(Index1, GenF, GenSt1, EndTime)
    end.

payload_size(MaxBytes) ->
    rand:uniform(MaxBytes).

key(KeyCount) ->
    rand:uniform(KeyCount).

now_us() -> erlang:system_time(microsecond).
