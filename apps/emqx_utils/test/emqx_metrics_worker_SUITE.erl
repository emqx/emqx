%%--------------------------------------------------------------------
%% Copyright (c) 2020-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(emqx_metrics_worker_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

all() ->
    emqx_common_test_helpers:all(?MODULE).

suite() ->
    [{ct_hooks, [cth_surefire]}, {timetrap, {seconds, 30}}].

-define(NAME, ?MODULE).

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(_, Config) ->
    {ok, _} = emqx_metrics_worker:start_link(?NAME),
    Config.

end_per_testcase(_, _Config) ->
    ok = emqx_metrics_worker:stop(?NAME),
    ok.

t_get_metrics(_) ->
    Metrics = [a, b, c, {slide, d}],
    Id = <<"testid">>,
    ok = emqx_metrics_worker:create_metrics(?NAME, Id, Metrics),
    %% all the metrics are set to zero at start
    ?assertMatch(
        #{
            rate := #{
                a := #{current := +0.0, max := +0.0, last5m := +0.0},
                b := #{current := +0.0, max := +0.0, last5m := +0.0},
                c := #{current := +0.0, max := +0.0, last5m := +0.0}
            },
            gauges := #{},
            counters := #{
                a := 0,
                b := 0,
                c := 0
            }
        },
        emqx_metrics_worker:get_metrics(?NAME, Id)
    ),
    ok = emqx_metrics_worker:inc(?NAME, Id, a),
    ok = emqx_metrics_worker:inc(?NAME, Id, b),
    ok = emqx_metrics_worker:inc(?NAME, Id, c),
    ok = emqx_metrics_worker:inc(?NAME, Id, c),
    ok = emqx_metrics_worker:set_gauge(?NAME, Id, worker_id0, inflight, 5),
    ok = emqx_metrics_worker:set_gauge(?NAME, Id, worker_id1, inflight, 7),
    ok = emqx_metrics_worker:set_gauge(?NAME, Id, worker_id2, queuing, 9),
    ok = emqx_metrics_worker:observe(?NAME, Id, d, 10),
    ok = emqx_metrics_worker:observe(?NAME, Id, d, 30),
    ct:sleep(1500),
    ?LET(
        #{
            rate := #{
                a := #{current := CurrA, max := MaxA, last5m := _},
                b := #{current := CurrB, max := MaxB, last5m := _},
                c := #{current := CurrC, max := MaxC, last5m := _}
            },
            gauges := #{
                inflight := Inflight,
                queuing := Queuing
            },
            counters := #{
                a := 1,
                b := 1,
                c := 2
            } = Counters,
            slides := #{
                d := #{n_samples := 2, last5m := 20, current := _}
            }
        },
        emqx_metrics_worker:get_metrics(?NAME, Id),
        {
            ?assert(CurrA > 0),
            ?assert(CurrB > 0),
            ?assert(CurrC > 0),
            ?assert(MaxA > 0),
            ?assert(MaxB > 0),
            ?assert(MaxC > 0),
            ?assert(Inflight == 12),
            ?assert(Queuing == 9),
            ?assertNot(maps:is_key(d, Counters))
        }
    ),
    ok = emqx_metrics_worker:clear_metrics(?NAME, Id).

t_clear_metrics(_Config) ->
    Metrics = [a, b, c],
    Id = <<"testid">>,
    ok = emqx_metrics_worker:create_metrics(?NAME, Id, Metrics),
    ?assertMatch(
        #{
            rate := #{
                a := #{current := +0.0, max := +0.0, last5m := +0.0},
                b := #{current := +0.0, max := +0.0, last5m := +0.0},
                c := #{current := +0.0, max := +0.0, last5m := +0.0}
            },
            gauges := #{},
            slides := #{},
            counters := #{
                a := 0,
                b := 0,
                c := 0
            }
        },
        emqx_metrics_worker:get_metrics(?NAME, Id)
    ),
    ok = emqx_metrics_worker:inc(?NAME, Id, a),
    ok = emqx_metrics_worker:inc(?NAME, Id, b),
    ok = emqx_metrics_worker:inc(?NAME, Id, c),
    ok = emqx_metrics_worker:inc(?NAME, Id, c),
    ok = emqx_metrics_worker:set_gauge(?NAME, Id, worker_id0, inflight, 5),
    ok = emqx_metrics_worker:set_gauge(?NAME, Id, worker_id1, inflight, 7),
    ok = emqx_metrics_worker:set_gauge(?NAME, Id, worker_id2, queuing, 9),
    ct:sleep(1500),
    ok = emqx_metrics_worker:clear_metrics(?NAME, Id),
    ?assertEqual(
        #{
            counters => #{},
            gauges => #{},
            rate => #{current => +0.0, last5m => +0.0, max => +0.0},
            slides => #{}
        },
        emqx_metrics_worker:get_metrics(?NAME, Id)
    ),
    ok.

t_reset_metrics(_) ->
    Metrics = [a, b, c, {slide, d}],
    Id = <<"testid">>,
    ok = emqx_metrics_worker:create_metrics(?NAME, Id, Metrics),
    %% all the metrics are set to zero at start
    ?assertMatch(
        #{
            rate := #{
                a := #{current := +0.0, max := +0.0, last5m := +0.0},
                b := #{current := +0.0, max := +0.0, last5m := +0.0},
                c := #{current := +0.0, max := +0.0, last5m := +0.0}
            },
            gauges := #{},
            counters := #{
                a := 0,
                b := 0,
                c := 0
            },
            slides := #{
                d := #{n_samples := 0, last5m := 0, current := 0}
            }
        },
        emqx_metrics_worker:get_metrics(?NAME, Id)
    ),
    ok = emqx_metrics_worker:inc(?NAME, Id, a),
    ok = emqx_metrics_worker:inc(?NAME, Id, b),
    ok = emqx_metrics_worker:inc(?NAME, Id, c),
    ok = emqx_metrics_worker:inc(?NAME, Id, c),
    ok = emqx_metrics_worker:set_gauge(?NAME, Id, worker_id0, inflight, 5),
    ok = emqx_metrics_worker:set_gauge(?NAME, Id, worker_id1, inflight, 7),
    ok = emqx_metrics_worker:set_gauge(?NAME, Id, worker_id2, queuing, 9),
    ok = emqx_metrics_worker:observe(?NAME, Id, d, 100),
    ok = emqx_metrics_worker:observe(?NAME, Id, d, 200),
    ct:sleep(1500),
    ?assertMatch(
        #{d := #{n_samples := 2}}, emqx_metrics_worker:get_slide(?NAME, <<"testid">>)
    ),
    ok = emqx_metrics_worker:reset_metrics(?NAME, Id),
    ?LET(
        #{
            rate := #{
                a := #{current := CurrA, max := MaxA, last5m := _},
                b := #{current := CurrB, max := MaxB, last5m := _},
                c := #{current := CurrC, max := MaxC, last5m := _}
            },
            gauges := Gauges,
            counters := #{
                a := 0,
                b := 0,
                c := 0
            },
            slides := #{
                d := #{n_samples := 0, last5m := 0, current := 0}
            }
        },
        emqx_metrics_worker:get_metrics(?NAME, Id),
        {
            ?assert(CurrA == 0),
            ?assert(CurrB == 0),
            ?assert(CurrC == 0),
            ?assert(MaxA == 0),
            ?assert(MaxB == 0),
            ?assert(MaxC == 0),
            ?assertEqual(#{}, Gauges)
        }
    ),
    ok = emqx_metrics_worker:clear_metrics(?NAME, Id).

t_get_metrics_2(_) ->
    Metrics = [a, b, c, {slide, d}],
    Id = <<"testid">>,
    ok = emqx_metrics_worker:create_metrics(
        ?NAME,
        Id,
        Metrics,
        [a]
    ),
    ok = emqx_metrics_worker:inc(?NAME, Id, a),
    ok = emqx_metrics_worker:inc(?NAME, Id, b),
    ok = emqx_metrics_worker:inc(?NAME, Id, c),
    ok = emqx_metrics_worker:set_gauge(?NAME, Id, worker_id0, inflight, 5),
    ok = emqx_metrics_worker:set_gauge(?NAME, Id, worker_id1, inflight, 7),
    ok = emqx_metrics_worker:set_gauge(?NAME, Id, worker_id2, queuing, 9),
    ?assertMatch(
        #{
            rate := Rate = #{
                a := #{current := _, max := _, last5m := _}
            },
            gauges := #{},
            counters := #{
                a := 1,
                b := 1,
                c := 1
            }
        } when map_size(Rate) =:= 1,
        emqx_metrics_worker:get_metrics(?NAME, Id)
    ),
    ok = emqx_metrics_worker:clear_metrics(?NAME, Id).

t_recreate_metrics(_) ->
    Id = <<"testid">>,
    ok = emqx_metrics_worker:create_metrics(?NAME, Id, [a]),
    ok = emqx_metrics_worker:inc(?NAME, Id, a),
    ok = emqx_metrics_worker:set_gauge(?NAME, Id, worker_id0, inflight, 5),
    ok = emqx_metrics_worker:set_gauge(?NAME, Id, worker_id1, inflight, 7),
    ok = emqx_metrics_worker:set_gauge(?NAME, Id, worker_id2, queuing, 9),
    ?assertMatch(
        #{
            rate := R = #{
                a := #{current := _, max := _, last5m := _}
            },
            gauges := #{
                inflight := 12,
                queuing := 9
            },
            counters := C = #{
                a := 1
            }
        } when map_size(R) == 1 andalso map_size(C) == 1,
        emqx_metrics_worker:get_metrics(?NAME, Id)
    ),
    %% we create the metrics again, to add some counters
    ok = emqx_metrics_worker:create_metrics(?NAME, Id, [a, b, c]),
    ok = emqx_metrics_worker:inc(?NAME, Id, b),
    ok = emqx_metrics_worker:inc(?NAME, Id, c),
    ?assertMatch(
        #{
            rate := R = #{
                a := #{current := _, max := _, last5m := _},
                b := #{current := _, max := _, last5m := _},
                c := #{current := _, max := _, last5m := _}
            },
            gauges := #{
                inflight := 12,
                queuing := 9
            },
            counters := C = #{
                a := 1, b := 1, c := 1
            }
        } when map_size(R) == 3 andalso map_size(C) == 3,
        emqx_metrics_worker:get_metrics(?NAME, Id)
    ),
    ok = emqx_metrics_worker:clear_metrics(?NAME, Id).

t_inc_matched(_) ->
    Metrics = ['rules.matched'],
    ok = emqx_metrics_worker:create_metrics(?NAME, <<"rule1">>, Metrics),
    ok = emqx_metrics_worker:create_metrics(?NAME, <<"rule2">>, Metrics),
    ok = emqx_metrics_worker:inc(?NAME, <<"rule1">>, 'rules.matched'),
    ok = emqx_metrics_worker:inc(?NAME, <<"rule2">>, 'rules.matched'),
    ok = emqx_metrics_worker:inc(?NAME, <<"rule2">>, 'rules.matched'),
    ?assertEqual(1, emqx_metrics_worker:get(?NAME, <<"rule1">>, 'rules.matched')),
    ?assertEqual(2, emqx_metrics_worker:get(?NAME, <<"rule2">>, 'rules.matched')),
    ?assertEqual(0, emqx_metrics_worker:get(?NAME, <<"rule3">>, 'rules.matched')),
    ok = emqx_metrics_worker:clear_metrics(?NAME, <<"rule1">>),
    ok = emqx_metrics_worker:clear_metrics(?NAME, <<"rule2">>).

t_rate(_) ->
    ok = emqx_metrics_worker:create_metrics(?NAME, <<"rule1">>, ['rules.matched']),
    ok = emqx_metrics_worker:create_metrics(?NAME, <<"rule:2">>, ['rules.matched']),
    ok = emqx_metrics_worker:inc(?NAME, <<"rule1">>, 'rules.matched'),
    ok = emqx_metrics_worker:inc(?NAME, <<"rule1">>, 'rules.matched'),
    ok = emqx_metrics_worker:inc(?NAME, <<"rule:2">>, 'rules.matched'),
    ?assertEqual(2, emqx_metrics_worker:get(?NAME, <<"rule1">>, 'rules.matched')),
    ct:sleep(1000),
    ?LET(
        #{'rules.matched' := #{max := Max, current := Current}},
        emqx_metrics_worker:get_rate(?NAME, <<"rule1">>),
        {?assert(Max =< 2), ?assert(Current =< 2)}
    ),
    ct:sleep(2100),
    ?LET(
        #{'rules.matched' := #{max := Max, current := Current, last5m := Last5Min}},
        emqx_metrics_worker:get_rate(?NAME, <<"rule1">>),
        {?assert(Max =< 2), ?assert(Current == 0), ?assert(Last5Min =< 0.67)}
    ),
    ct:sleep(3000),
    ok = emqx_metrics_worker:clear_metrics(?NAME, <<"rule1">>),
    ok = emqx_metrics_worker:clear_metrics(?NAME, <<"rule:2">>).

t_get_gauge(_Config) ->
    Metric = 'queueing',
    %% unknown handler name (inexistent table)
    ?assertEqual(0, emqx_metrics_worker:get_gauge(unknown_name, unknown_id, Metric)),
    %% unknown resource id
    ?assertEqual(0, emqx_metrics_worker:get_gauge(?NAME, unknown_id, Metric)),

    Id = <<"some id">>,
    ok = emqx_metrics_worker:set_gauge(?NAME, Id, worker_id0, Metric, 2),

    ?assertEqual(2, emqx_metrics_worker:get_gauge(?NAME, Id, Metric)),
    ?assertEqual(0, emqx_metrics_worker:get_gauge(?NAME, unknown, Metric)),

    ok = emqx_metrics_worker:set_gauge(?NAME, Id, worker_id1, Metric, 3),
    ?assertEqual(5, emqx_metrics_worker:get_gauge(?NAME, Id, Metric)),

    ok = emqx_metrics_worker:set_gauge(?NAME, Id, worker_id0, Metric, 1),
    ?assertEqual(4, emqx_metrics_worker:get_gauge(?NAME, Id, Metric)),

    ?assertEqual(0, emqx_metrics_worker:get_gauge(?NAME, Id, another_metric)),

    ok.

t_get_gauges(_Config) ->
    %% unknown handler name (inexistent table)
    ?assertEqual(#{}, emqx_metrics_worker:get_gauges(unknown_name, unknown_id)),
    %% unknown resource id
    ?assertEqual(#{}, emqx_metrics_worker:get_gauges(?NAME, unknown_id)),

    Metric = 'queuing',
    Id = <<"some id">>,
    ok = emqx_metrics_worker:set_gauge(?NAME, Id, worker_id0, Metric, 2),

    ?assertEqual(#{queuing => 2}, emqx_metrics_worker:get_gauges(?NAME, Id)),
    ?assertEqual(#{}, emqx_metrics_worker:get_gauges(?NAME, unknown)),

    ok = emqx_metrics_worker:set_gauge(?NAME, Id, worker_id1, Metric, 3),
    ?assertEqual(#{queuing => 5}, emqx_metrics_worker:get_gauges(?NAME, Id)),

    AnotherMetric = 'inflight',
    ok = emqx_metrics_worker:set_gauge(?NAME, Id, worker_id0, Metric, 1),
    ok = emqx_metrics_worker:set_gauge(?NAME, Id, worker_id0, AnotherMetric, 10),
    ?assertEqual(#{queuing => 4, inflight => 10}, emqx_metrics_worker:get_gauges(?NAME, Id)),

    ok.

t_delete_gauge(_Config) ->
    %% unknown handler name (inexistent table)
    ?assertEqual(ok, emqx_metrics_worker:delete_gauges(unknown_name, unknown_id)),
    %% unknown resource id
    ?assertEqual(ok, emqx_metrics_worker:delete_gauges(?NAME, unknown_id)),

    Metric = 'queuing',
    AnotherMetric = 'inflight',
    Id = <<"some id">>,
    AnotherId = <<"another id">>,
    ok = emqx_metrics_worker:set_gauge(?NAME, Id, worker_id0, Metric, 2),
    ok = emqx_metrics_worker:set_gauge(?NAME, Id, worker_id1, Metric, 3),
    ok = emqx_metrics_worker:set_gauge(?NAME, Id, worker_id0, AnotherMetric, 10),
    ok = emqx_metrics_worker:set_gauge(?NAME, AnotherId, worker_id1, AnotherMetric, 10),
    ?assertEqual(#{queuing => 5, inflight => 10}, emqx_metrics_worker:get_gauges(?NAME, Id)),

    ?assertEqual(ok, emqx_metrics_worker:delete_gauges(?NAME, Id)),

    ?assertEqual(#{}, emqx_metrics_worker:get_gauges(?NAME, Id)),
    ?assertEqual(#{inflight => 10}, emqx_metrics_worker:get_gauges(?NAME, AnotherId)),

    ok.

t_shift_gauge(_Config) ->
    Metric = 'queueing',
    Id = <<"some id">>,
    AnotherId = <<"another id">>,

    %% unknown handler name (inexistent table)
    ?assertEqual(
        ok, emqx_metrics_worker:shift_gauge(unknown_name, unknown_id, worker_id0, Metric, 2)
    ),
    ?assertEqual(0, emqx_metrics_worker:get_gauge(unknown_name, unknown_id, Metric)),

    %% empty resource id
    ?assertEqual(ok, emqx_metrics_worker:shift_gauge(?NAME, Id, worker_id0, Metric, 2)),
    ?assertEqual(ok, emqx_metrics_worker:shift_gauge(?NAME, AnotherId, worker_id0, Metric, 2)),
    ?assertEqual(2, emqx_metrics_worker:get_gauge(?NAME, Id, Metric)),
    ?assertEqual(2, emqx_metrics_worker:get_gauge(?NAME, AnotherId, Metric)),

    ?assertEqual(ok, emqx_metrics_worker:shift_gauge(?NAME, Id, worker_id0, Metric, 3)),
    ?assertEqual(5, emqx_metrics_worker:get_gauge(?NAME, Id, Metric)),

    ?assertEqual(ok, emqx_metrics_worker:shift_gauge(?NAME, Id, worker_id1, Metric, 10)),
    ?assertEqual(15, emqx_metrics_worker:get_gauge(?NAME, Id, Metric)),

    ?assertEqual(ok, emqx_metrics_worker:shift_gauge(?NAME, Id, worker_id1, Metric, -4)),
    ?assertEqual(11, emqx_metrics_worker:get_gauge(?NAME, Id, Metric)),

    ?assertEqual(2, emqx_metrics_worker:get_gauge(?NAME, AnotherId, Metric)),

    ok.

%% Tests that check the behavior of `ensure_metrics'.
t_ensure_metrics(_Config) ->
    Id1 = <<"id1">>,
    Metrics1 = [c1, {counter, c2}, c3, {slide, s1}, {slide, s2}],
    RateMetrics1 = [c2, c3],
    %% Behaves as `create_metrics' if absent
    ?assertEqual(
        {ok, created},
        emqx_metrics_worker:ensure_metrics(?NAME, Id1, Metrics1, RateMetrics1)
    ),
    ?assertMatch(
        #{
            counters := #{c1 := _, c2 := _, c3 := _},
            rate := #{c2 := _, c3 := _},
            gauges := #{},
            slides := #{s1 := _, s2 := _}
        },
        emqx_metrics_worker:get_metrics(?NAME, Id1)
    ),
    %% Does nothing if everything is in place
    ?assertEqual(
        {ok, already_created},
        emqx_metrics_worker:ensure_metrics(?NAME, Id1, Metrics1, RateMetrics1)
    ),
    ?assertMatch(
        #{
            counters := #{c1 := _, c2 := _, c3 := _},
            rate := #{c2 := _, c3 := _},
            gauges := #{},
            slides := #{s1 := _, s2 := _}
        },
        emqx_metrics_worker:get_metrics(?NAME, Id1)
    ),

    %% Does nothing if asked to ensure a subset of existing metrics
    Metrics2 = [c1],
    RateMetrics2 = [c3],
    ?assertEqual(
        {ok, already_created},
        emqx_metrics_worker:ensure_metrics(?NAME, Id1, Metrics2, RateMetrics2)
    ),
    ?assertEqual(
        {ok, already_created},
        emqx_metrics_worker:ensure_metrics(?NAME, Id1, [], [])
    ),
    ?assertMatch(
        #{
            counters := #{c1 := _, c2 := _, c3 := _},
            rate := #{c2 := _, c3 := _},
            gauges := #{},
            slides := #{s1 := _, s2 := _}
        },
        emqx_metrics_worker:get_metrics(?NAME, Id1)
    ),

    %% If we have an initially smaller set of metrics, `ensure_metrics' will behave as
    %% `create_metrics' if one is missing.
    Id2 = <<"id2">>,
    lists:foreach(
        fun(
            #{remove_from_metrics := RemoveFromMetrics, remove_from_rates := RemoveFromRates} = Ctx
        ) ->
            ok = emqx_metrics_worker:clear_metrics(?NAME, Id2),
            Metrics3 = Metrics1 -- RemoveFromMetrics,
            RateMetrics3 = RateMetrics1 -- RemoveFromRates,
            ok = emqx_metrics_worker:create_metrics(?NAME, Id2, Metrics3, RateMetrics3),
            ?assertEqual(
                {ok, created},
                emqx_metrics_worker:ensure_metrics(?NAME, Id2, Metrics1, RateMetrics1),
                Ctx
            ),
            ?assertMatch(
                #{
                    counters := #{c1 := _, c2 := _, c3 := _},
                    rate := #{c2 := _, c3 := _},
                    gauges := #{},
                    slides := #{s1 := _, s2 := _}
                },
                emqx_metrics_worker:get_metrics(?NAME, Id2)
            ),
            ok
        end,
        [
            #{
                remove_from_metrics => [c1],
                remove_from_rates => []
            },
            #{
                remove_from_metrics => [{counter, c2}],
                remove_from_rates => [c2]
            },
            #{
                remove_from_metrics => [{slide, s2}],
                remove_from_rates => []
            },
            #{
                remove_from_metrics => [],
                remove_from_rates => RateMetrics1
            }
        ]
    ),
    ok.
