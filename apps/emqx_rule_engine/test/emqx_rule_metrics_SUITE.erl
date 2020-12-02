%%--------------------------------------------------------------------
%% Copyright (c) 2020 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_rule_metrics_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

all() ->
    [ {group, metrics}
    , {group, speed} ].

suite() ->
    [{ct_hooks, [cth_surefire]}, {timetrap, {seconds, 30}}].

groups() ->
    [{metrics, [sequence],
        [ t_action
        , t_rule
        , t_clear
        ]},
    {speed, [sequence],
        [ rule_speed
        ]}
    ].

init_per_suite(Config) ->
    emqx_ct_helpers:start_apps([emqx]),
    {ok, _} = emqx_rule_metrics:start_link(),
    Config.

end_per_suite(_Config) ->
    catch emqx_rule_metrics:stop(),
    emqx_ct_helpers:stop_apps([emqx]),
    ok.

init_per_testcase(_, Config) ->
    catch emqx_rule_metrics:stop(),
    {ok, _} = emqx_rule_metrics:start_link(),
    [emqx_metrics:set(M, 0) || M <- emqx_rule_metrics:overall_metrics()],
    Config.

end_per_testcase(_, _Config) ->
    ok.

t_action(_) ->
    ?assertEqual(0, emqx_rule_metrics:get(<<"action:1">>, 'actions.success')),
    ?assertEqual(0, emqx_rule_metrics:get(<<"action:1">>, 'actions.failure')),
    ?assertEqual(0, emqx_rule_metrics:get(<<"action:2">>, 'actions.success')),
    ok = emqx_rule_metrics:inc(<<"action:1">>, 'actions.success'),
    ok = emqx_rule_metrics:inc(<<"action:1">>, 'actions.failure'),
    ok = emqx_rule_metrics:inc(<<"action:2">>, 'actions.success'),
    ok = emqx_rule_metrics:inc(<<"action:2">>, 'actions.success'),
    ?assertEqual(1, emqx_rule_metrics:get(<<"action:1">>, 'actions.success')),
    ?assertEqual(1, emqx_rule_metrics:get(<<"action:1">>, 'actions.failure')),
    ?assertEqual(2, emqx_rule_metrics:get(<<"action:2">>, 'actions.success')),
    ?assertEqual(0, emqx_rule_metrics:get(<<"action:3">>, 'actions.success')),
    ?assertEqual(3, emqx_rule_metrics:get_overall('actions.success')),
    ?assertEqual(1, emqx_rule_metrics:get_overall('actions.failure')).

t_rule(_) ->
    ok = emqx_rule_metrics:inc(<<"rule:1">>, 'rules.matched'),
    ok = emqx_rule_metrics:inc(<<"rule:2">>, 'rules.matched'),
    ok = emqx_rule_metrics:inc(<<"rule:2">>, 'rules.matched'),
    ?assertEqual(1, emqx_rule_metrics:get(<<"rule:1">>, 'rules.matched')),
    ?assertEqual(2, emqx_rule_metrics:get(<<"rule:2">>, 'rules.matched')),
    ?assertEqual(0, emqx_rule_metrics:get(<<"rule:3">>, 'rules.matched')),
    ?assertEqual(3, emqx_rule_metrics:get_overall('rules.matched')).

t_clear(_) ->
    ok = emqx_rule_metrics:inc(<<"action:1">>, 'actions.success'),
    ?assertEqual(1, emqx_rule_metrics:get(<<"action:1">>, 'actions.success')),
    ok = emqx_rule_metrics:clear(<<"action:1">>),
    ?assertEqual(0, emqx_rule_metrics:get(<<"action:1">>, 'actions.success')).

rule_speed(_) ->
    ok = emqx_rule_metrics:inc(<<"rule:1">>, 'rules.matched'),
    ok = emqx_rule_metrics:inc(<<"rule:1">>, 'rules.matched'),
    ok = emqx_rule_metrics:inc(<<"rule:2">>, 'rules.matched'),
    ?assertEqual(2, emqx_rule_metrics:get(<<"rule:1">>, 'rules.matched')),
    ct:sleep(1000),
    ?LET(#{max := Max, current := Current}, emqx_rule_metrics:get_rule_speed(<<"rule:1">>),
         {?assert(Max =< 2),
          ?assert(Current =< 2)}),
    ct:pal("===== Speed: ~p~n", [emqx_rule_metrics:get_overall_rule_speed()]),
    ?LET(#{max := Max, current := Current}, emqx_rule_metrics:get_overall_rule_speed(),
         {?assert(Max =< 3),
          ?assert(Current =< 3)}),
    ct:sleep(2100),
    ?LET(#{max := Max, current := Current, last5m := Last5Min}, emqx_rule_metrics:get_rule_speed(<<"rule:1">>),
         {?assert(Max =< 2),
          ?assert(Current == 0),
          ?assert(Last5Min =< 0.67)}),
    ?LET(#{max := Max, current := Current, last5m := Last5Min}, emqx_rule_metrics:get_overall_rule_speed(),
         {?assert(Max =< 3),
          ?assert(Current == 0),
          ?assert(Last5Min =< 1)}),
    ct:sleep(3000),
    ?LET(#{max := Max, current := Current, last5m := Last5Min}, emqx_rule_metrics:get_overall_rule_speed(),
         {?assert(Max =< 3),
          ?assert(Current == 0),
          ?assert(Last5Min == 0)}).

% t_create(_) ->
%     error('TODO').

% t_get(_) ->
%     error('TODO').

% t_get_overall(_) ->
%     error('TODO').

% t_get_rule_speed(_) ->
%     error('TODO').

% t_get_overall_rule_speed(_) ->
%     error('TODO').

% t_get_rule_metrics(_) ->
%     error('TODO').

% t_get_action_metrics(_) ->
%     error('TODO').

% t_inc(_) ->
%     error('TODO').

% t_overall_metrics(_) ->
%     error('TODO').

