%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mq_quota_index_basic_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include("../src/emqx_mq_internal.hrl").
-include_lib("../src/emqx_mq_quota/emqx_mq_quota.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include("../gen_src/MessageQueue.hrl").

all() ->
    emqx_common_test_helpers:all(?MODULE).

%% Check that the first insertion closes a full bucket and exact consumption retains its new head.
t_first_bucket_consumption(_Config) ->
    Opts = #{count => 1, on_full => reject, threshold_percentage => 10},
    Index0 = emqx_mq_quota_index:new(Opts, 10),
    Index1 = emqx_mq_quota_index:apply_update(Index0, {10, 100, 1}),
    ?assertEqual(0, emqx_mq_quota_index:oldest_boundary(Index0)),
    ?assertEqual(0, emqx_mq_quota_index:oldest_boundary(Index1)),
    ?assertMatch(
        #'StorageIndex'{
            common = [#'StorageIndexBucket'{tsUs = 11}, #'StorageIndexBucket'{tsUs = 0}]
        },
        index_encode_asn_decode(Index1)
    ),
    ?assertEqual(11, emqx_mq_quota_index:next_boundary(Index1)),
    ?assertEqual(#{bytes => 100, count => 1}, emqx_mq_quota_index:occupancy(Index1)),
    {0, Index2} = emqx_mq_quota_index:truncate(Index1, 9),
    ?assertEqual(Index1, Index2),
    {11, Index3} = emqx_mq_quota_index:truncate(Index2, 11),
    ?assertEqual(11, emqx_mq_quota_index:oldest_boundary(Index3)),
    ?assertEqual(#{bytes => 0, count => 0}, emqx_mq_quota_index:occupancy(Index3)),
    ?assertEqual(undefined, emqx_mq_quota_index:next_boundary(Index3)),
    ?assertMatch(#'StorageIndex'{common = [_]}, index_encode_asn_decode(Index3)).

%% Check that a delayed quota update before the first message timestamp remains charged.
t_reject_out_of_order_first_bucket(_Config) ->
    Opts = #{count => 100, on_full => reject, threshold_percentage => 10},
    Index0 = emqx_mq_quota_index:new(Opts, 101),
    Index1 = emqx_mq_quota_index:apply_updates(Index0, [{101, 20, 1}, {100, 10, 1}]),
    ?assertEqual(0, emqx_mq_quota_index:oldest_boundary(Index1)),
    ?assertEqual(#{bytes => 30, count => 2}, emqx_mq_quota_index:occupancy(Index1)),
    ?assertMatch(#'StorageIndex'{maxTsUs = 101}, index_encode_asn_decode(Index1)).

%% Check that reject updates preserve overshoot in common and legacy indices without a capacity deadline.
t_reject_retains_overshoot(_Config) ->
    lists:foreach(
        fun(Limits) ->
            Opts = maps:merge(Limits, #{on_full => reject, threshold_percentage => 10}),
            Index0 = emqx_mq_quota_index:new(Opts, 10),
            Index1 = emqx_mq_quota_index:apply_updates(
                Index0, [{Ts, 100, 1} || Ts <- lists:seq(10, 100, 10)]
            ),
            ?assertEqual(#{bytes => 1000, count => 10}, emqx_mq_quota_index:occupancy(Index1)),
            ?assertEqual(0, emqx_mq_quota_index:deadline(Index1)),
            Stored0 = index_encode_asn_decode(Index1),
            Index2 = asn_encode_index_decode(Opts, Stored0#'StorageIndex'{common = asn1_NOVALUE}),
            Totals = emqx_mq_quota_index:occupancy(Index2),
            maps:foreach(
                fun(Kind, _) ->
                    Expected =
                        case Kind of
                            bytes -> 1000;
                            count -> 10
                        end,
                    ?assertEqual(Expected, maps:get(Kind, Totals))
                end,
                Limits
            )
        end,
        [#{count => 2}, #{bytes => 200}, #{count => 2, bytes => 200}]
    ).

%% Check that consumption retains a charged head until normal insertion creates a newer boundary.
t_reject_retains_charged_head(_Config) ->
    Opts = #{bytes => 1000, on_full => reject, threshold_percentage => 10},
    Index0 = emqx_mq_quota_index:new(Opts, 10),
    Index1 = emqx_mq_quota_index:apply_update(Index0, {10, 90, 1}),
    {0, Index2} = emqx_mq_quota_index:truncate(Index1, 100),
    ?assertEqual(#{bytes => 90, count => 1}, emqx_mq_quota_index:occupancy(Index2)),
    Index3 = emqx_mq_quota_index:apply_update(Index2, {110, 20, 1}),
    {111, Index4} = emqx_mq_quota_index:truncate(Index3, 111),
    ?assertEqual(#{bytes => 0, count => 0}, emqx_mq_quota_index:occupancy(Index4)).

%% Check that decoding old-format data combines separate byte and message counts into one bucket.
t_legacy_migration(_Config) ->
    Opts = #{bytes => 1000, count => 100, threshold_percentage => 10},
    %% The old ASN.1 codec produced these bytes before the common field existed.
    LegacyBin = <<
        <<48, 62, 160, 3, 2, 1, 100, 161, 26, 48, 24>>/binary,
        <<48, 10, 160, 3, 2, 1, 80, 161, 3, 2, 1, 2>>/binary,
        <<48, 10, 160, 3, 2, 1, 20, 161, 3, 2, 1, 3>>/binary,
        <<162, 27, 48, 25, 48, 10, 160, 3, 2, 1, 70, 161, 3, 2, 1, 100>>/binary,
        <<48, 11, 160, 3, 2, 1, 10, 161, 4, 2, 2, 0, 200>>/binary
    >>,
    Index = emqx_mq_quota_index:decode(Opts, LegacyBin),
    ?assertMatch(
        #'StorageIndex'{
            maxTsUs = 100,
            common = [#'StorageIndexBucket'{tsUs = 10, bytes = 300, count = 5}]
        },
        index_encode_asn_decode(Index)
    ),
    ?assertEqual(0, emqx_mq_quota_index:deadline(Index)).

%% Check that decoding creates a common bucket from empty, bytes-only, or count-only legacy lists.
t_empty_and_single_dimension_migration(_Config) ->
    Opts = #{bytes => 1000, count => 100, threshold_percentage => 10},
    lists:foreach(
        fun(#{bytes := Bytes, count := Count, expected := Expected}) ->
            Index = asn_encode_index_decode(Opts, #'StorageIndex'{
                maxTsUs = 100, bytes = Bytes, count = Count
            }),
            Stored = index_encode_asn_decode(Index),
            ?assertEqual([Expected], Stored#'StorageIndex'.common)
        end,
        [
            #{
                bytes => [],
                count => [],
                expected => #'StorageIndexBucket'{tsUs = 100, bytes = 0, count = 0}
            },
            #{
                bytes => [#'StorageIndexRecord'{tsUs = 10, value = 300}],
                count => [],
                expected => #'StorageIndexBucket'{tsUs = 10, bytes = 300, count = 0}
            },
            #{
                bytes => [],
                count => [#'StorageIndexRecord'{tsUs = 20, value = 5}],
                expected => #'StorageIndexBucket'{tsUs = 20, bytes = 0, count = 5}
            }
        ]
    ).

%% Check that decoding rebuilds common buckets only when absent, preserving existing lists including [].
t_common_index_authoritative(_Config) ->
    Opts = #{bytes => 1000, count => 100, threshold_percentage => 10},
    lists:foreach(
        fun(Common) ->
            Index = asn_encode_index_decode(Opts, #'StorageIndex'{
                maxTsUs = 100,
                bytes = [#'StorageIndexRecord'{tsUs = 10, value = 999}],
                count = [#'StorageIndexRecord'{tsUs = 10, value = 99}],
                common = Common
            }),
            Stored = index_encode_asn_decode(Index),
            ?assertEqual(Common, Stored#'StorageIndex'.common)
        end,
        [[], [#'StorageIndexBucket'{tsUs = 20, bytes = 30, count = 2}]]
    ).

%% Check that message totals survive writes in the old format and subsequent removal of legacy lists.
t_rolling_upgrade(_Config) ->
    Opts = #{bytes => 1000, count => 100, threshold_percentage => 10},
    Index0 = emqx_mq_quota_index:new(Opts, 10),
    Index1 = emqx_mq_quota_index:apply_updates(Index0, [{10, 30, 1}, {20, 40, 1}]),
    Stored0 = index_encode_asn_decode(Index1),
    ?assertMatch(
        #'StorageIndex'{
            bytes = [#'StorageIndexRecord'{tsUs = 10, value = 70}],
            count = [#'StorageIndexRecord'{tsUs = 10, value = 2}],
            common = [#'StorageIndexBucket'{tsUs = 10, bytes = 70, count = 2}]
        },
        Stored0
    ),

    %% Simulate old code adding a message and saving without the common field.
    Stored1 = Stored0#'StorageIndex'{
        maxTsUs = 30,
        bytes = [#'StorageIndexRecord'{tsUs = 10, value = 120}],
        count = [#'StorageIndexRecord'{tsUs = 10, value = 3}],
        common = asn1_NOVALUE
    },
    Index2 = asn_encode_index_decode(Opts, Stored1),
    Stored2 = index_encode_asn_decode(Index2),
    ?assertMatch(
        [#'StorageIndexBucket'{tsUs = 10, bytes = 120, count = 3}],
        Stored2#'StorageIndex'.common
    ),

    %% Remove the legacy lists, add another message, and check the saved common totals.
    Index3 = emqx_mq_quota_index:apply_updates(
        emqx_mq_quota_index:drop_legacy(Index2), [{40, 10, 1}]
    ),
    ?assertMatch(
        #'StorageIndex'{bytes = asn1_NOVALUE, count = asn1_NOVALUE, common = [_ | _]},
        index_encode_asn_decode(Index3)
    ),
    Index4 = emqx_mq_quota_index:decode(Opts, emqx_mq_quota_index:encode(Index3)),
    ?assertEqual(emqx_mq_quota_index:inspect(Index3), emqx_mq_quota_index:inspect(Index4)),
    ?assertMatch(
        #{common := #{total := #{bytes := 130, count := 4}}}, emqx_mq_quota_index:inspect(Index4)
    ).

%% Check that legacy byte and message-count lists retain their own timestamp ranges during updates.
t_independent_legacy_updates(_Config) ->
    Opts = #{bytes => 10000, count => 1000, threshold_percentage => 10},
    Stored0 = #'StorageIndex'{
        maxTsUs = 100,
        bytes = [
            #'StorageIndexRecord'{tsUs = 80, value = 100},
            #'StorageIndexRecord'{tsUs = 10, value = 200}
        ],
        count = [
            #'StorageIndexRecord'{tsUs = 70, value = 2},
            #'StorageIndexRecord'{tsUs = 20, value = 3}
        ]
    },
    Index0 = asn_encode_index_decode(Opts, Stored0),
    Stored1 = index_encode_asn_decode(Index0),
    ?assertEqual(Stored0#'StorageIndex'.bytes, Stored1#'StorageIndex'.bytes),
    ?assertEqual(Stored0#'StorageIndex'.count, Stored1#'StorageIndex'.count),

    Index1 = emqx_mq_quota_index:apply_updates(Index0, [{90, 40, 1}, {30, -20, -1}]),
    Stored2 = index_encode_asn_decode(Index1),
    ?assertEqual(
        [
            #'StorageIndexRecord'{tsUs = 80, value = 140},
            #'StorageIndexRecord'{tsUs = 10, value = 180}
        ],
        Stored2#'StorageIndex'.bytes
    ),
    ?assertEqual(
        [#'StorageIndexRecord'{tsUs = 70, value = 3}, #'StorageIndexRecord'{tsUs = 20, value = 2}],
        Stored2#'StorageIndex'.count
    ),
    ?assertEqual(
        [#'StorageIndexBucket'{tsUs = 10, bytes = 320, count = 5}],
        Stored2#'StorageIndex'.common
    ),
    ?assertMatch(
        #{
            bytes := #{records := [{80, 140, _}, {10, 180, _}], total := 320, size := 2},
            count := #{records := [{70, 3, _}, {20, 2, _}], total := 5, size := 2},
            common := #{
                records := [{10, #{bytes := 320, count := 5}, _}],
                total := #{bytes := 320, count := 5},
                size := 1
            },
            max_ts := 100,
            opts := Opts
        },
        emqx_mq_quota_index:inspect(Index1)
    ),

    %% Check that decoding restores the same buckets after old code omits the common field.
    Stored3 = Stored2#'StorageIndex'{common = asn1_NOVALUE},
    Index2 = asn_encode_index_decode(Opts, Stored3),
    ?assertEqual(Stored2, index_encode_asn_decode(Index2)).

%% Check that reaching the message-count threshold splits count and common buckets but not byte buckets.
t_independent_legacy_thresholds(_Config) ->
    Opts = #{bytes => 10000, count => 20, threshold_percentage => 10},
    Index0 = emqx_mq_quota_index:new(Opts, 10),
    Updates = [{Ts, 10, 1} || Ts <- lists:seq(10, 100, 10)],
    Index1 = emqx_mq_quota_index:apply_updates(Index0, Updates),
    #'StorageIndex'{bytes = Bytes, count = Count, common = Common} = index_encode_asn_decode(
        Index1
    ),
    ?assertEqual([#'StorageIndexRecord'{tsUs = 10, value = 100}], Bytes),
    ?assert(length(Count) > 2),
    ?assert(length(Common) > 2),
    ?assertEqual(10, lists:sum([Value || #'StorageIndexRecord'{value = Value} <- Count])),
    ?assertEqual(100, lists:sum([Value || #'StorageIndexBucket'{bytes = Value} <- Common])).

%% Check that removing legacy lists preserves common buckets and later updates do not recreate those lists.
t_common_only_updates(_Config) ->
    Opts = #{bytes => 1000, count => 100, threshold_percentage => 10},
    Index0 = emqx_mq_quota_index:new(Opts, 10),
    ?assertMatch(
        #{
            bytes := #{records := [], total := 0, size := 0},
            count := #{records := [], total := 0, size := 0},
            common := #{records := [], total := #{bytes := 0, count := 0}, size := 0}
        },
        emqx_mq_quota_index:inspect(Index0)
    ),
    Index1 = emqx_mq_quota_index:apply_updates(Index0, [{10, 30, 1}, {20, 40, 1}]),
    #'StorageIndex'{common = Common} = index_encode_asn_decode(Index1),
    Index2 = emqx_mq_quota_index:drop_legacy(Index1),
    ?assertEqual(Index2, emqx_mq_quota_index:drop_legacy(Index2)),
    ?assertEqual(
        #'StorageIndex'{maxTsUs = 20, common = Common},
        index_encode_asn_decode(Index2)
    ),
    ?assertMatch(
        #{
            bytes := undefined,
            count := undefined,
            common := #{records := [{10, #{bytes := 70, count := 2}, _}], size := 1},
            max_ts := 20,
            opts := Opts
        },
        emqx_mq_quota_index:inspect(Index2)
    ),
    Index3 = asn_encode_index_decode(Opts, index_encode_asn_decode(Index2)),
    Index4 = emqx_mq_quota_index:apply_updates(Index3, [{10, -30, -1}, {30, 50, 1}]),
    ?assertMatch(
        #'StorageIndex'{
            bytes = asn1_NOVALUE,
            count = asn1_NOVALUE,
            common = [#'StorageIndexBucket'{tsUs = 10, bytes = 90, count = 2}]
        },
        index_encode_asn_decode(Index4)
    ).

%% Check common bucket byte and message counts after out-of-order inserts, replacements, and deletions.
t_shared_buckets(_Config) ->
    lists:foreach(
        fun(Opts) ->
            Updates = [{Ts, 10, 1} || Ts <- lists:seq(10, 100, 10)],
            Index0 = emqx_mq_quota_index:new(Opts, 10),
            Index1 = emqx_mq_quota_index:apply_updates(Index0, Updates),
            #'StorageIndex'{common = Buckets} = index_encode_asn_decode(Index1),
            ?assert(length(Buckets) > 2),
            ?assert(
                lists:all(
                    fun(#'StorageIndexBucket'{bytes = B, count = C}) -> B =:= C * 10 end, Buckets
                )
            ),

            Index2 = emqx_mq_quota_index:apply_updates(Index1, [
                {15, 20, 1}, {20, -10, -1}, {110, 30, 1}
            ]),
            ?assertMatch(
                #{common := #{total := #{bytes := 140, count := 11}}},
                emqx_mq_quota_index:inspect(Index2)
            ),
            Deletes = [{Ts, -B, -C} || {Ts, B, C} <- Updates, Ts =/= 20],
            Index3 = emqx_mq_quota_index:apply_updates(
                Index2, Deletes ++ [{15, -20, -1}, {110, -30, -1}]
            ),
            ?assertMatch(
                #{common := #{total := #{bytes := 0, count := 0}}},
                emqx_mq_quota_index:inspect(Index3)
            )
        end,
        [
            #{bytes => 1000, count => 20, threshold_percentage => 10},
            #{bytes => 200, count => 1000, threshold_percentage => 10}
        ]
    ).

%% Check that summing buckets from newest to oldest sets the deletion deadline at the start
%% of the first bucket where total bytes or message count reaches its limit.
t_shared_deadline(_Config) ->
    Common = [
        #'StorageIndexBucket'{tsUs = 50, bytes = 0, count = 0},
        #'StorageIndexBucket'{tsUs = 40, bytes = 100, count = 1},
        #'StorageIndexBucket'{tsUs = 30, bytes = 10, count = 5},
        #'StorageIndexBucket'{tsUs = 20, bytes = 50, count = 2},
        #'StorageIndexBucket'{tsUs = 10, bytes = 20, count = 2}
    ],
    lists:foreach(
        fun(#{bytes := Bytes, count := Count, expected_deadline := Expected}) ->
            Opts = #{bytes => Bytes, count => Count, threshold_percentage => 10},
            Index0 = asn_encode_index_decode(Opts, #'StorageIndex'{maxTsUs = 50, common = Common}),
            ?assertEqual(Expected, emqx_mq_quota_index:deadline(Index0)),
            Index1 = emqx_mq_quota_index:apply_update(Index0, {50, 0, 0}),
            ?assertEqual(Expected, emqx_mq_quota_index:deadline(Index1)),
            Stored = index_encode_asn_decode(Index1),
            ?assert(
                lists:all(
                    fun(#'StorageIndexBucket'{tsUs = Ts}) -> Ts >= Expected end,
                    Stored#'StorageIndex'.common
                )
            )
        end,
        [
            #{bytes => 100, count => infinity, expected_deadline => 40},
            #{bytes => infinity, count => 6, expected_deadline => 30},
            #{bytes => 160, count => 6, expected_deadline => 30},
            #{bytes => 160, count => infinity, expected_deadline => 20}
        ]
    ).

%% Check that legacy lists determine the deletion deadline until removed, when common buckets take over.
t_legacy_deadline(_Config) ->
    EmptyOpts = #{bytes => 1000, count => 100, threshold_percentage => 10},
    Legacy = #'StorageIndex'{
        maxTsUs = 600,
        bytes = [
            #'StorageIndexRecord'{tsUs = 500, value = 0},
            #'StorageIndexRecord'{tsUs = 400, value = 100},
            #'StorageIndexRecord'{tsUs = 300, value = 50},
            #'StorageIndexRecord'{tsUs = 200, value = 1000},
            #'StorageIndexRecord'{tsUs = 100, value = 999}
        ],
        count = [
            #'StorageIndexRecord'{tsUs = 500, value = 0},
            #'StorageIndexRecord'{tsUs = 450, value = 1},
            #'StorageIndexRecord'{tsUs = 300, value = 2},
            #'StorageIndexRecord'{tsUs = 200, value = 20},
            #'StorageIndexRecord'{tsUs = 100, value = 99}
        ]
    },
    lists:foreach(
        fun(#{bytes := Bytes, count := Count, expected_deadline := Expected}) ->
            Opts = #{bytes => Bytes, count => Count, threshold_percentage => 10},
            lists:foreach(
                fun(#{common := Common, expected_deadline := CommonDeadline}) ->
                    Index = asn_encode_index_decode(Opts, Legacy#'StorageIndex'{common = Common}),
                    ?assertEqual(Expected, emqx_mq_quota_index:deadline(Index)),
                    ?assertEqual(
                        CommonDeadline,
                        emqx_mq_quota_index:deadline(emqx_mq_quota_index:drop_legacy(Index))
                    )
                end,
                [
                    #{common => [], expected_deadline => 0},
                    #{
                        common => [#'StorageIndexBucket'{tsUs = 100, bytes = 1000, count = 100}],
                        expected_deadline => 100
                    },
                    #{
                        common => [#'StorageIndexBucket'{tsUs = 500, bytes = 1000, count = 100}],
                        expected_deadline => 500
                    }
                ]
            )
        end,
        [
            #{bytes => 100, count => infinity, expected_deadline => 400},
            #{bytes => infinity, count => 1, expected_deadline => 450},
            #{bytes => 100, count => 1, expected_deadline => 450},
            #{bytes => 100, count => 3, expected_deadline => 400}
        ]
    ),
    %% Empty legacy lists give a zero deadline even when common counts exceed a limit.
    Empty = Legacy#'StorageIndex'{
        bytes = [],
        count = [],
        common = [#'StorageIndexBucket'{tsUs = 100, bytes = 1000, count = 100}]
    },
    ?assertEqual(0, emqx_mq_quota_index:deadline(asn_encode_index_decode(EmptyOpts, Empty))).

%% Check that decoding builds missing common buckets only from legacy buckets at or after the deletion deadline.
t_migration_deadline(_Config) ->
    Legacy = #'StorageIndex'{
        maxTsUs = 600,
        bytes = [
            #'StorageIndexRecord'{tsUs = 500, value = 0},
            #'StorageIndexRecord'{tsUs = 400, value = 100},
            #'StorageIndexRecord'{tsUs = 300, value = 50},
            #'StorageIndexRecord'{tsUs = 200, value = 1000},
            #'StorageIndexRecord'{tsUs = 100, value = 999}
        ],
        count = [
            #'StorageIndexRecord'{tsUs = 500, value = 0},
            #'StorageIndexRecord'{tsUs = 450, value = 1},
            #'StorageIndexRecord'{tsUs = 300, value = 2},
            #'StorageIndexRecord'{tsUs = 200, value = 20},
            #'StorageIndexRecord'{tsUs = 100, value = 99}
        ]
    },
    lists:foreach(
        fun(
            #{
                bytes := MaxBytes,
                count := MaxCount,
                expected_deadline := Deadline,
                expected_bucket := Expected
            }
        ) ->
            Opts = #{bytes => MaxBytes, count => MaxCount, threshold_percentage => 10},
            Index = asn_encode_index_decode(Opts, Legacy),
            Stored = index_encode_asn_decode(Index),
            ?assertEqual(Deadline, emqx_mq_quota_index:deadline(Index)),
            ?assertEqual([Expected], Stored#'StorageIndex'.common),
            ?assertEqual(Legacy#'StorageIndex'.bytes, Stored#'StorageIndex'.bytes),
            ?assertEqual(Legacy#'StorageIndex'.count, Stored#'StorageIndex'.count)
        end,
        [
            #{
                bytes => 100,
                count => 3,
                expected_deadline => 400,
                expected_bucket => #'StorageIndexBucket'{tsUs = 400, bytes = 100, count = 1}
            },
            #{
                bytes => 150,
                count => 1,
                expected_deadline => 450,
                expected_bucket => #'StorageIndexBucket'{tsUs = 450, bytes = 0, count = 1}
            },
            #{
                bytes => 150,
                count => 3,
                expected_deadline => 300,
                expected_bucket => #'StorageIndexBucket'{tsUs = 300, bytes = 150, count = 3}
            },
            #{
                bytes => infinity,
                count => infinity,
                expected_deadline => 0,
                expected_bucket => #'StorageIndexBucket'{tsUs = 100, bytes = 2149, count = 122}
            }
        ]
    ).

%% Check that a migrated common bucket ignores updates before its start but accepts updates at its start.
t_migration_ignores_old_updates(_Config) ->
    Opts = #{bytes => 100, count => 3, threshold_percentage => 10},
    Legacy = #'StorageIndex'{
        maxTsUs = 600,
        bytes = [
            #'StorageIndexRecord'{tsUs = 500, value = 0},
            #'StorageIndexRecord'{tsUs = 400, value = 100},
            #'StorageIndexRecord'{tsUs = 300, value = 50},
            #'StorageIndexRecord'{tsUs = 200, value = 1000},
            #'StorageIndexRecord'{tsUs = 100, value = 999}
        ],
        count = [
            #'StorageIndexRecord'{tsUs = 500, value = 0},
            #'StorageIndexRecord'{tsUs = 450, value = 1},
            #'StorageIndexRecord'{tsUs = 300, value = 2},
            #'StorageIndexRecord'{tsUs = 200, value = 20},
            #'StorageIndexRecord'{tsUs = 100, value = 99}
        ]
    },
    Index0 = asn_encode_index_decode(Opts, Legacy),
    %% Simulate buffered updates for messages that garbage collection may already have deleted.
    Index1 = emqx_mq_quota_index:apply_updates(Index0, [{350, 20, 1}, {200, -50, -1}]),
    Stored1 = index_encode_asn_decode(Index1),
    ?assertEqual(
        [#'StorageIndexBucket'{tsUs = 400, bytes = 100, count = 1}],
        Stored1#'StorageIndex'.common
    ),
    %% Remove a message exactly at the deadline and check that both counts decrease.
    Index2 = emqx_mq_quota_index:apply_update(Index1, {400, -10, -1}),
    Stored2 = index_encode_asn_decode(Index2),
    ?assertEqual(
        [#'StorageIndexBucket'{tsUs = 400, bytes = 90, count = 0}],
        Stored2#'StorageIndex'.common
    ).

%% Check that repeated writes without the common field still advance deletion deadlines and bound retained messages.
t_repeated_legacy_rewrites(_Config) ->
    Opts = #{bytes => 100, count => 10, threshold_percentage => 10},
    Index0 = emqx_mq_quota_index:new(Opts, 10),
    {Index1, _Messages} = lists:foldl(
        fun(N, {IndexAcc0, Messages0}) ->
            Deadline0 = emqx_mq_quota_index:deadline(IndexAcc0),
            %% Remove the common field as old code would, then decode to rebuild it.
            Stored = index_encode_asn_decode(IndexAcc0),
            IndexAcc1 = asn_encode_index_decode(Opts, Stored#'StorageIndex'{common = asn1_NOVALUE}),
            ?assertEqual(Deadline0, emqx_mq_quota_index:deadline(IndexAcc1)),
            #'StorageIndex'{common = [#'StorageIndexBucket'{tsUs = Start}]} = index_encode_asn_decode(
                IndexAcc1
            ),
            ?assert(Start >= Deadline0),
            IndexAcc2 = emqx_mq_quota_index:apply_update(IndexAcc1, {N * 10, 10, 1}),
            Deadline1 = emqx_mq_quota_index:deadline(IndexAcc2),
            ?assert(Deadline1 >= Deadline0),
            Messages1 = [Ts || Ts <- [N * 10 | Messages0], Ts >= Deadline1],
            ?assert(length(Messages1) =< 12),
            {IndexAcc2, Messages1}
        end,
        {Index0, []},
        lists:seq(1, 1000)
    ),
    ?assert(emqx_mq_quota_index:deadline(Index1) > 9800).

%% Check that saved common counts support newly configured byte or message-count limits after legacy removal.
t_limit_changes(_Config) ->
    lists:foreach(
        fun(Opts) ->
            Index0 = emqx_mq_quota_index:new(Opts, 10),
            Index1 = emqx_mq_quota_index:apply_updates(Index0, [{10, 50, 1}, {20, 60, 1}]),
            ?assertMatch(
                #{common := #{total := #{bytes := 110, count := 2}}},
                emqx_mq_quota_index:inspect(Index1)
            ),
            Bin = emqx_mq_quota_index:encode(emqx_mq_quota_index:drop_legacy(Index1)),
            ?assertEqual(
                10,
                emqx_mq_quota_index:deadline(
                    emqx_mq_quota_index:decode(
                        #{bytes => 100, threshold_percentage => 10}, Bin
                    )
                )
            ),
            ?assertEqual(
                10,
                emqx_mq_quota_index:deadline(
                    emqx_mq_quota_index:decode(
                        #{count => 2, threshold_percentage => 10}, Bin
                    )
                )
            )
        end,
        [
            #{bytes => 10000, threshold_percentage => 10},
            #{count => 1000, threshold_percentage => 10},
            #{bytes => infinity, count => infinity, threshold_percentage => 10}
        ]
    ).

%% Check that truncate_before keeps the nearest common boundary strictly before the request and trims older bucket starts.
t_truncate_before(_Config) ->
    Opts = #{bytes => 1000, count => 100, threshold_percentage => 10},
    Common60 = #'StorageIndexBucket'{tsUs = 60, bytes = 60, count = 6},
    Common40 = #'StorageIndexBucket'{tsUs = 40, bytes = 40, count = 4},
    Common20 = #'StorageIndexBucket'{tsUs = 20, bytes = 20, count = 2},
    Bytes65 = #'StorageIndexRecord'{tsUs = 65, value = 60},
    Bytes35 = #'StorageIndexRecord'{tsUs = 35, value = 40},
    Bytes10 = #'StorageIndexRecord'{tsUs = 10, value = 20},
    Count70 = #'StorageIndexRecord'{tsUs = 70, value = 6},
    Count40 = #'StorageIndexRecord'{tsUs = 40, value = 4},
    Count10 = #'StorageIndexRecord'{tsUs = 10, value = 2},
    Stored = #'StorageIndex'{
        maxTsUs = 80,
        common = [Common60, Common40, Common20],
        bytes = [Bytes65, Bytes35, Bytes10],
        count = [Count70, Count40, Count10]
    },
    lists:foreach(
        fun(
            #{
                before := Before,
                cutoff_us := CutoffUs,
                common := Common,
                bytes := Bytes,
                count := Count
            }
        ) ->
            lists:foreach(
                fun(LegacyUpdates) ->
                    Index0 = asn_encode_index_decode(Opts, Stored),
                    Index1 =
                        case LegacyUpdates of
                            true -> Index0;
                            false -> emqx_mq_quota_index:drop_legacy(Index0)
                        end,
                    {ActualCutoffUs, Index2} = emqx_mq_quota_index:truncate_before(Index1, Before),
                    ?assertEqual(CutoffUs, ActualCutoffUs),
                    Expected0 = Stored#'StorageIndex'{
                        common = Common, bytes = Bytes, count = Count
                    },
                    Expected1 =
                        case LegacyUpdates of
                            true ->
                                Expected0;
                            false ->
                                Expected0#'StorageIndex'{bytes = asn1_NOVALUE, count = asn1_NOVALUE}
                        end,
                    ?assertEqual(Expected1, index_encode_asn_decode(Index2)),
                    ?assertEqual(
                        {CutoffUs, Index2}, emqx_mq_quota_index:truncate_before(Index2, Before)
                    )
                end,
                [true, false]
            )
        end,
        [
            #{
                before => 0,
                cutoff_us => 0,
                common => [Common60, Common40, Common20],
                bytes => [Bytes65, Bytes35, Bytes10],
                count => [Count70, Count40, Count10]
            },
            #{
                before => 20,
                cutoff_us => 0,
                common => [Common60, Common40, Common20],
                bytes => [Bytes65, Bytes35, Bytes10],
                count => [Count70, Count40, Count10]
            },
            #{
                before => 40,
                cutoff_us => 20,
                common => [Common60, Common40, Common20],
                bytes => [Bytes65, Bytes35],
                count => [Count70, Count40]
            },
            #{
                before => 41,
                cutoff_us => 40,
                common => [Common60, Common40],
                bytes => [Bytes65],
                count => [Count70, Count40]
            },
            #{
                before => 60,
                cutoff_us => 40,
                common => [Common60, Common40],
                bytes => [Bytes65],
                count => [Count70, Count40]
            },
            #{
                before => 100,
                cutoff_us => 60,
                common => [Common60],
                bytes => [Bytes65],
                count => [Count70]
            }
        ]
    ).

%% Check that an empty common index prevents truncation even when legacy buckets are present.
t_truncate_before_empty(_Config) ->
    Opts = #{bytes => 1000, count => 100, threshold_percentage => 10},
    Stored = #'StorageIndex'{
        maxTsUs = 100,
        common = [],
        bytes = [#'StorageIndexRecord'{tsUs = 10, value = 100}],
        count = [#'StorageIndexRecord'{tsUs = 10, value = 1}]
    },
    Index = asn_encode_index_decode(Opts, Stored),
    ?assertEqual({0, Index}, emqx_mq_quota_index:truncate_before(Index, 100)),
    ?assertEqual(Stored, index_encode_asn_decode(Index)).

%% Check that updates before a purge boundary cannot recreate removed counts.
t_truncate_before_late_updates(_Config) ->
    Opts = #{bytes => 1000, count => 100, threshold_percentage => 10},
    Stored = #'StorageIndex'{
        maxTsUs = 60,
        common = [
            #'StorageIndexBucket'{tsUs = 40, bytes = 40, count = 4},
            #'StorageIndexBucket'{tsUs = 20, bytes = 20, count = 2}
        ],
        bytes = [
            #'StorageIndexRecord'{tsUs = 40, value = 40},
            #'StorageIndexRecord'{tsUs = 20, value = 20}
        ],
        count = [
            #'StorageIndexRecord'{tsUs = 40, value = 4}, #'StorageIndexRecord'{tsUs = 20, value = 2}
        ]
    },
    Index0 = asn_encode_index_decode(Opts, Stored),
    {40, Index1} = emqx_mq_quota_index:truncate_before(Index0, 50),
    ?assertEqual(Index1, emqx_mq_quota_index:apply_updates(Index1, [{30, 10, 1}, {20, -20, -2}])),
    Index2 = emqx_mq_quota_index:apply_update(Index1, {40, -10, -1}),
    ?assertMatch(
        #{
            common := #{total := #{bytes := 30, count := 3}},
            bytes := #{total := 30},
            count := #{total := 3}
        },
        emqx_mq_quota_index:inspect(Index2)
    ).

%% Check that reset clears counters, rejects older updates, and accepts updates at the new boundary.
t_reset(_Config) ->
    Opts = #{bytes => 1000, count => 100, threshold_percentage => 10},
    Stored = #'StorageIndex'{
        maxTsUs = 60,
        common = [#'StorageIndexBucket'{tsUs = 40, bytes = 40, count = 4}],
        bytes = [#'StorageIndexRecord'{tsUs = 40, value = 40}],
        count = [#'StorageIndexRecord'{tsUs = 40, value = 4}]
    },
    %% Persist the reset boundary with zero counters in every existing index.
    Index0 = asn_encode_index_decode(Opts, Stored),
    Index1 = emqx_mq_quota_index:reset(Index0, 100),
    Expected = #'StorageIndex'{
        maxTsUs = 100,
        common = [#'StorageIndexBucket'{tsUs = 100, bytes = 0, count = 0}],
        bytes = [#'StorageIndexRecord'{tsUs = 100, value = 0}],
        count = [#'StorageIndexRecord'{tsUs = 100, value = 0}]
    },
    ?assertEqual(Expected, index_encode_asn_decode(Index1)),
    Index2 = asn_encode_index_decode(Opts, Expected),
    %% Ignore both delayed insertions and deletions from before the reset.
    ?assertEqual(Index2, emqx_mq_quota_index:apply_update(Index2, {99, 10, 1})),
    ?assertEqual(Index2, emqx_mq_quota_index:apply_update(Index2, {40, -40, -4})),
    %% Count an out-of-order insertion at the boundary after a newer insertion.
    Index3 = emqx_mq_quota_index:apply_updates(Index2, [{110, 20, 2}, {100, 10, 1}]),
    ?assertMatch(
        #{
            common := #{total := #{bytes := 30, count := 3}},
            bytes := #{total := 30},
            count := #{total := 3}
        },
        emqx_mq_quota_index:inspect(Index3)
    ).

%% Check that resetting a common-only index keeps legacy indices absent.
t_reset_without_legacy(_Config) ->
    Opts = #{bytes => 1000, count => 100, threshold_percentage => 10},
    Index0 = emqx_mq_quota_index:drop_legacy(emqx_mq_quota_index:new(Opts, 0)),
    Index1 = emqx_mq_quota_index:reset(Index0, 100),
    ?assertEqual(
        #'StorageIndex'{
            maxTsUs = 100,
            common = [#'StorageIndexBucket'{tsUs = 100, bytes = 0, count = 0}]
        },
        index_encode_asn_decode(Index1)
    ).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

asn_encode_index_decode(Opts, StorageIndex) ->
    emqx_mq_quota_index:decode(Opts, 'MessageQueue':encode('StorageIndex', StorageIndex)).

index_encode_asn_decode(Index) ->
    'MessageQueue':decode('StorageIndex', emqx_mq_quota_index:encode(Index)).
