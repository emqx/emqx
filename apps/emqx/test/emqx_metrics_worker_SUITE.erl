%%--------------------------------------------------------------------
%% Copyright (c) 2020-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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
    {ok, _} = emqx_metrics_worker:start_link(?NAME),
    Config.

end_per_suite(_Config) ->
    ok = emqx_metrics_worker:stop(?NAME).

init_per_testcase(_, Config) ->
    ok = emqx_metrics_worker:stop(?NAME),
    {ok, _} = emqx_metrics_worker:start_link(?NAME),
    Config.

end_per_testcase(_, _Config) ->
    ok.

t_get_metrics(_) ->
    Metrics = [a, b, c],
    ok = emqx_metrics_worker:create_metrics(?NAME, <<"testid">>, Metrics),
    %% all the metrics are set to zero at start
    ?assertMatch(
        #{
            rate := #{
                a := #{current := 0.0, max := 0.0, last5m := 0.0},
                b := #{current := 0.0, max := 0.0, last5m := 0.0},
                c := #{current := 0.0, max := 0.0, last5m := 0.0}
            },
            counters := #{
                a := 0,
                b := 0,
                c := 0
            }
        },
        emqx_metrics_worker:get_metrics(?NAME, <<"testid">>)
    ),
    ok = emqx_metrics_worker:inc(?NAME, <<"testid">>, a),
    ok = emqx_metrics_worker:inc(?NAME, <<"testid">>, b),
    ok = emqx_metrics_worker:inc(?NAME, <<"testid">>, c),
    ok = emqx_metrics_worker:inc(?NAME, <<"testid">>, c),
    ct:sleep(1500),
    ?LET(
        #{
            rate := #{
                a := #{current := CurrA, max := MaxA, last5m := _},
                b := #{current := CurrB, max := MaxB, last5m := _},
                c := #{current := CurrC, max := MaxC, last5m := _}
            },
            counters := #{
                a := 1,
                b := 1,
                c := 2
            }
        },
        emqx_metrics_worker:get_metrics(?NAME, <<"testid">>),
        {
            ?assert(CurrA > 0),
            ?assert(CurrB > 0),
            ?assert(CurrC > 0),
            ?assert(MaxA > 0),
            ?assert(MaxB > 0),
            ?assert(MaxC > 0)
        }
    ),
    ok = emqx_metrics_worker:clear_metrics(?NAME, <<"testid">>).

t_reset_metrics(_) ->
    Metrics = [a, b, c],
    ok = emqx_metrics_worker:create_metrics(?NAME, <<"testid">>, Metrics),
    %% all the metrics are set to zero at start
    ?assertMatch(
        #{
            rate := #{
                a := #{current := 0.0, max := 0.0, last5m := 0.0},
                b := #{current := 0.0, max := 0.0, last5m := 0.0},
                c := #{current := 0.0, max := 0.0, last5m := 0.0}
            },
            counters := #{
                a := 0,
                b := 0,
                c := 0
            }
        },
        emqx_metrics_worker:get_metrics(?NAME, <<"testid">>)
    ),
    ok = emqx_metrics_worker:inc(?NAME, <<"testid">>, a),
    ok = emqx_metrics_worker:inc(?NAME, <<"testid">>, b),
    ok = emqx_metrics_worker:inc(?NAME, <<"testid">>, c),
    ok = emqx_metrics_worker:inc(?NAME, <<"testid">>, c),
    ct:sleep(1500),
    ok = emqx_metrics_worker:reset_metrics(?NAME, <<"testid">>),
    ?LET(
        #{
            rate := #{
                a := #{current := CurrA, max := MaxA, last5m := _},
                b := #{current := CurrB, max := MaxB, last5m := _},
                c := #{current := CurrC, max := MaxC, last5m := _}
            },
            counters := #{
                a := 0,
                b := 0,
                c := 0
            }
        },
        emqx_metrics_worker:get_metrics(?NAME, <<"testid">>),
        {
            ?assert(CurrA == 0),
            ?assert(CurrB == 0),
            ?assert(CurrC == 0),
            ?assert(MaxA == 0),
            ?assert(MaxB == 0),
            ?assert(MaxC == 0)
        }
    ),
    ok = emqx_metrics_worker:clear_metrics(?NAME, <<"testid">>).

t_get_metrics_2(_) ->
    Metrics = [a, b, c],
    ok = emqx_metrics_worker:create_metrics(
        ?NAME,
        <<"testid">>,
        Metrics,
        [a]
    ),
    ok = emqx_metrics_worker:inc(?NAME, <<"testid">>, a),
    ok = emqx_metrics_worker:inc(?NAME, <<"testid">>, b),
    ok = emqx_metrics_worker:inc(?NAME, <<"testid">>, c),
    ?assertMatch(
        #{
            rate := Rate = #{
                a := #{current := _, max := _, last5m := _}
            },
            counters := #{
                a := 1,
                b := 1,
                c := 1
            }
        } when map_size(Rate) =:= 1,
        emqx_metrics_worker:get_metrics(?NAME, <<"testid">>)
    ),
    ok = emqx_metrics_worker:clear_metrics(?NAME, <<"testid">>).

t_recreate_metrics(_) ->
    ok = emqx_metrics_worker:create_metrics(?NAME, <<"testid">>, [a]),
    ok = emqx_metrics_worker:inc(?NAME, <<"testid">>, a),
    ?assertMatch(
        #{
            rate := R = #{
                a := #{current := _, max := _, last5m := _}
            },
            counters := C = #{
                a := 1
            }
        } when map_size(R) == 1 andalso map_size(C) == 1,
        emqx_metrics_worker:get_metrics(?NAME, <<"testid">>)
    ),
    %% we create the metrics again, to add some counters
    ok = emqx_metrics_worker:create_metrics(?NAME, <<"testid">>, [a, b, c]),
    ok = emqx_metrics_worker:inc(?NAME, <<"testid">>, b),
    ok = emqx_metrics_worker:inc(?NAME, <<"testid">>, c),
    ?assertMatch(
        #{
            rate := R = #{
                a := #{current := _, max := _, last5m := _},
                b := #{current := _, max := _, last5m := _},
                c := #{current := _, max := _, last5m := _}
            },
            counters := C = #{
                a := 1, b := 1, c := 1
            }
        } when map_size(R) == 3 andalso map_size(C) == 3,
        emqx_metrics_worker:get_metrics(?NAME, <<"testid">>)
    ),
    ok = emqx_metrics_worker:clear_metrics(?NAME, <<"testid">>).

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
