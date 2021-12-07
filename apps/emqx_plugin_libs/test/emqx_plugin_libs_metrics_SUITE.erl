%%--------------------------------------------------------------------
%% Copyright (c) 2020-2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_plugin_libs_metrics_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

all() ->
    [ {group, metrics}
    , {group, rate} ].

suite() ->
    [{ct_hooks, [cth_surefire]}, {timetrap, {seconds, 30}}].

groups() ->
    [{metrics, [sequence],
        [ t_rule
        , t_no_creation_1
        ]},
    {rate, [sequence],
        [ rule_rate
        ]}
    ].

-define(NAME, ?MODULE).

init_per_suite(Config) ->
    emqx_common_test_helpers:start_apps([emqx_conf]),
    {ok, _} = emqx_plugin_libs_metrics:start_link(?NAME),
    Config.

end_per_suite(_Config) ->
    catch emqx_plugin_libs_metrics:stop(?NAME),
    emqx_common_test_helpers:stop_apps([emqx_conf]),
    ok.

init_per_testcase(_, Config) ->
    catch emqx_plugin_libs_metrics:stop(?NAME),
    {ok, _} = emqx_plugin_libs_metrics:start_link(?NAME),
    Config.

end_per_testcase(_, _Config) ->
    ok.

t_no_creation_1(_) ->
    ?assertEqual(ok, emqx_plugin_libs_metrics:inc(?NAME, <<"rule1">>, 'rules.matched')).

t_rule(_) ->
    ok = emqx_plugin_libs_metrics:create_metrics(?NAME, <<"rule1">>),
    ok = emqx_plugin_libs_metrics:create_metrics(?NAME, <<"rule2">>),
    ok = emqx_plugin_libs_metrics:inc(?NAME, <<"rule1">>, 'rules.matched'),
    ok = emqx_plugin_libs_metrics:inc(?NAME, <<"rule2">>, 'rules.matched'),
    ok = emqx_plugin_libs_metrics:inc(?NAME, <<"rule2">>, 'rules.matched'),
    ?assertEqual(1, emqx_plugin_libs_metrics:get(?NAME, <<"rule1">>, 'rules.matched')),
    ?assertEqual(2, emqx_plugin_libs_metrics:get(?NAME, <<"rule2">>, 'rules.matched')),
    ?assertEqual(0, emqx_plugin_libs_metrics:get(?NAME, <<"rule3">>, 'rules.matched')),
    ok = emqx_plugin_libs_metrics:clear_metrics(?NAME, <<"rule1">>),
    ok = emqx_plugin_libs_metrics:clear_metrics(?NAME, <<"rule2">>).

rule_rate(_) ->
    ok = emqx_plugin_libs_metrics:create_metrics(?NAME, <<"rule1">>),
    ok = emqx_plugin_libs_metrics:create_metrics(?NAME, <<"rule:2">>),
    ok = emqx_plugin_libs_metrics:inc(?NAME, <<"rule1">>, 'rules.matched'),
    ok = emqx_plugin_libs_metrics:inc(?NAME, <<"rule1">>, 'rules.matched'),
    ok = emqx_plugin_libs_metrics:inc(?NAME, <<"rule:2">>, 'rules.matched'),
    ?assertEqual(2, emqx_plugin_libs_metrics:get(?NAME, <<"rule1">>, 'rules.matched')),
    ct:sleep(1000),
    ?LET(#{max := Max, current := Current},
         emqx_plugin_libs_metrics:get_rate(?NAME, <<"rule1">>),
         {?assert(Max =< 2),
          ?assert(Current =< 2)}),
    ct:sleep(2100),
    ?LET(#{max := Max, current := Current, last5m := Last5Min}, emqx_plugin_libs_metrics:get_rate(?NAME, <<"rule1">>),
         {?assert(Max =< 2),
          ?assert(Current == 0),
          ?assert(Last5Min =< 0.67)}),
    ct:sleep(3000),
    ok = emqx_plugin_libs_metrics:clear_metrics(?NAME, <<"rule1">>),
    ok = emqx_plugin_libs_metrics:clear_metrics(?NAME, <<"rule:2">>).
