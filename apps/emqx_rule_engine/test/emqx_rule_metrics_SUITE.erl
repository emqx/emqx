%%--------------------------------------------------------------------
%% Copyright (c) 2020-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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
        , t_no_creation_1
        , t_no_creation_2
        ]},
    {speed, [sequence],
        [ rule_speed
        ]}
    ].

init_per_suite(Config) ->
    emqx_ct_helpers:start_apps([emqx]),
    catch emqx_rule_metrics:stop(),
    {ok, _} = emqx_rule_metrics:start_link(),
    Config.

end_per_suite(_Config) ->
    catch emqx_rule_metrics:stop(),
    emqx_ct_helpers:stop_apps([emqx]),
    ok.

init_per_testcase(_, Config) ->
    catch emqx_rule_metrics:stop(),
    {ok, _} = emqx_rule_metrics:start_link(),
    Config.

end_per_testcase(_, _Config) ->
    ok.

t_no_creation_1(_) ->
    ?assertEqual(ok, emqx_rule_metrics:inc_rules_matched(<<"rule1">>)).

t_no_creation_2(_) ->
    ?assertEqual(ok, emqx_rule_metrics:inc_actions_taken(<<"action:0">>)).

t_action(_) ->
    ?assertEqual(0, emqx_rule_metrics:get_actions_taken(<<"action:1">>)),
    ?assertEqual(0, emqx_rule_metrics:get_actions_exception(<<"action:1">>)),
    ?assertEqual(0, emqx_rule_metrics:get_actions_taken(<<"action:2">>)),
    ok = emqx_rule_metrics:create_metrics(<<"action:1">>),
    ok = emqx_rule_metrics:create_metrics(<<"action:2">>),
    ok = emqx_rule_metrics:inc_actions_taken(<<"action:1">>),
    ok = emqx_rule_metrics:inc_actions_exception(<<"action:1">>),
    ok = emqx_rule_metrics:inc_actions_taken(<<"action:2">>),
    ok = emqx_rule_metrics:inc_actions_taken(<<"action:2">>),
    ?assertEqual(1, emqx_rule_metrics:get_actions_taken(<<"action:1">>)),
    ?assertEqual(1, emqx_rule_metrics:get_actions_exception(<<"action:1">>)),
    ?assertEqual(2, emqx_rule_metrics:get_actions_taken(<<"action:2">>)),
    ?assertEqual(0, emqx_rule_metrics:get_actions_taken(<<"action:3">>)),
    ok = emqx_rule_metrics:clear_metrics(<<"action:1">>),
    ok = emqx_rule_metrics:clear_metrics(<<"action:2">>),
    ?assertEqual(0, emqx_rule_metrics:get_actions_taken(<<"action:1">>)),
    ?assertEqual(0, emqx_rule_metrics:get_actions_taken(<<"action:2">>)).

t_rule(_) ->
    ok = emqx_rule_metrics:create_rule_metrics(<<"rule:1">>),
    ok = emqx_rule_metrics:create_rule_metrics(<<"rule2">>),
    ok = emqx_rule_metrics:inc(<<"rule:1">>, 'rules.matched'),
    ok = emqx_rule_metrics:inc(<<"rule:1">>, 'rules.passed'),
    ok = emqx_rule_metrics:inc(<<"rule:1">>, 'rules.exception'),
    ok = emqx_rule_metrics:inc(<<"rule:1">>, 'rules.no_result'),
    ok = emqx_rule_metrics:inc(<<"rule:1">>, 'rules.failed'),
    ok = emqx_rule_metrics:inc(<<"rule2">>, 'rules.matched'),
    ok = emqx_rule_metrics:inc(<<"rule2">>, 'rules.matched'),
    ?assertEqual(1, emqx_rule_metrics:get(<<"rule:1">>, 'rules.matched')),
    ?assertEqual(1, emqx_rule_metrics:get(<<"rule:1">>, 'rules.passed')),
    ?assertEqual(1, emqx_rule_metrics:get(<<"rule:1">>, 'rules.exception')),
    ?assertEqual(1, emqx_rule_metrics:get(<<"rule:1">>, 'rules.no_result')),
    ?assertEqual(1, emqx_rule_metrics:get(<<"rule:1">>, 'rules.failed')),
    ?assertEqual(2, emqx_rule_metrics:get(<<"rule2">>, 'rules.matched')),
    ?assertEqual(0, emqx_rule_metrics:get(<<"rule3">>, 'rules.matched')),
    ok = emqx_rule_metrics:clear_rule_metrics(<<"rule:1">>),
    ok = emqx_rule_metrics:clear_rule_metrics(<<"rule2">>).

t_clear(_) ->
    ok = emqx_rule_metrics:create_metrics(<<"action:1">>),
    ok = emqx_rule_metrics:inc_actions_taken(<<"action:1">>),
    ?assertEqual(1, emqx_rule_metrics:get_actions_taken(<<"action:1">>)),
    ok = emqx_rule_metrics:clear_metrics(<<"action:1">>),
    ?assertEqual(0, emqx_rule_metrics:get_actions_taken(<<"action:1">>)).

rule_speed(_) ->
    ok = emqx_rule_metrics:create_rule_metrics(<<"rule1">>),
    ok = emqx_rule_metrics:create_rule_metrics(<<"rule:2">>),
    ok = emqx_rule_metrics:inc(<<"rule1">>, 'rules.matched'),
    ok = emqx_rule_metrics:inc(<<"rule1">>, 'rules.matched'),
    ok = emqx_rule_metrics:inc(<<"rule:2">>, 'rules.matched'),
    ?assertEqual(2, emqx_rule_metrics:get(<<"rule1">>, 'rules.matched')),
    ct:sleep(1000),
    ?LET(#{max := Max, current := Current}, emqx_rule_metrics:get_rule_speed(<<"rule1">>),
         {?assert(Max =< 2),
          ?assert(Current =< 2)}),
    ct:sleep(2100),
    ?LET(#{max := Max, current := Current, last5m := Last5Min}, emqx_rule_metrics:get_rule_speed(<<"rule1">>),
         {?assert(Max =< 2),
          ?assert(Current == 0),
          ?assert(Last5Min =< 0.67)}),
    ok = emqx_rule_metrics:clear_rule_metrics(<<"rule1">>),
    ok = emqx_rule_metrics:clear_rule_metrics(<<"rule:2">>).

% t_create(_) ->
%     error('TODO').

% t_get(_) ->
%     error('TODO').


% t_get_rule_speed(_) ->
%     error('TODO').


% t_get_rule_metrics(_) ->
%     error('TODO').

% t_get_action_metrics(_) ->
%     error('TODO').

% t_inc(_) ->
%     error('TODO').
