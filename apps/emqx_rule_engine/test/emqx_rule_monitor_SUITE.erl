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

-module(emqx_rule_monitor_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("emqx_rule_engine/include/rule_engine.hrl").
-include_lib("emqx/include/emqx.hrl").

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

all() ->
    [ {group, resource}
    ].

suite() ->
    [{ct_hooks, [cth_surefire]}, {timetrap, {seconds, 30}}].

groups() ->
    [{resource, [sequence],
      [ t_restart_resource
      , t_refresh_resources_rules
      ]}
    ].

init_per_suite(Config) ->
    %% ensure alarm_handler started
    {ok, _} = application:ensure_all_started(sasl),
    ok = ekka_mnesia:start(),
    ok = emqx_rule_registry:mnesia(boot),
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(t_restart_resource, Config) ->
    emqx_rule_monitor:put_resource_retry_interval(100),
    Opts = [public, named_table, set, {read_concurrency, true}],
    _ = ets:new(?RES_PARAMS_TAB, [{keypos, #resource_params.id}|Opts]),
    ets:new(t_restart_resource, [named_table, public]),
    ets:insert(t_restart_resource, {failed_count, 0}),
    ets:insert(t_restart_resource, {succ_count, 0}),
    common_init_per_testcase(),
    Config;
init_per_testcase(t_refresh_resources_rules, Config) ->
    meck:unload(),
    ets:new(t_refresh_resources_rules, [named_table, public]),
    ok = meck:new(emqx_rule_engine, [no_link, passthrough]),
    meck:expect(emqx_rule_engine, refresh_resources, fun() ->
        timer:sleep(500),
        ets:update_counter(t_refresh_resources_rules, refresh_resources, 1, {refresh_resources, 0}),
        ok
    end),
    meck:expect(emqx_rule_engine, refresh_rules_when_boot, fun() ->
        timer:sleep(500),
        ets:update_counter(t_refresh_resources_rules, refresh_rules, 1, {refresh_rules, 0}),
        ok
    end),
    common_init_per_testcase(),
    Config;
init_per_testcase(_, Config) ->
    common_init_per_testcase(),
    Config.

end_per_testcase(t_restart_resource, Config) ->
    ets:delete(t_restart_resource),
    common_end_per_testcases(),
    Config;
end_per_testcase(t_refresh_resources_rules, Config) ->
    meck:unload(),
    common_end_per_testcases(),
    Config;
end_per_testcase(_, Config) ->
    common_end_per_testcases(),
    Config.

common_init_per_testcase() ->
    AlarmOpts = [{actions, [log, publish]}, {size_limit, 1000}, {validity_period, 86400}],
    _ = emqx_alarm:mnesia(boot),
    {ok, _} = emqx_alarm:start_link(AlarmOpts),
    {ok, _} = emqx_rule_monitor:start_link().

common_end_per_testcases() ->
    ok = emqx_alarm:stop(),
    emqx_rule_monitor:erase_resource_retry_interval(),
    emqx_rule_monitor:stop().

t_restart_resource(_) ->
    ct:pal("emqx_alarm: ~p", [sys:get_state(whereis(emqx_alarm))]),
    ok = emqx_rule_registry:register_resource_types(
            [#resource_type{
                name = test_res_1,
                provider = ?APP,
                params_spec = #{},
                on_create = {?MODULE, on_resource_create},
                on_destroy = {?MODULE, on_resource_destroy},
                on_status = {?MODULE, on_get_resource_status},
                title = #{en => <<"Test Resource">>},
                description = #{en => <<"Test Resource">>}}]),
    ok = emqx_rule_engine:load_providers(),
    {ok, #resource{id = ResId}} = emqx_rule_engine:create_resource(
            #{type => test_res_1,
              config => #{},
              restart_interval => 100,
              description => <<"debug resource">>}),
    ?assertMatch([{_, 0}], ets:lookup(t_restart_resource, succ_count)),
    ?assertMatch([{_, N}] when N == 1 orelse N == 2 orelse N == 3,
        ets:lookup(t_restart_resource, failed_count)),
    ct:pal("monitor: ~p", [whereis(emqx_rule_monitor)]),
    timer:sleep(1000),
    [{_, 5}] = ets:lookup(t_restart_resource, failed_count),
    [{_, 1}] = ets:lookup(t_restart_resource, succ_count),
    #{retryers := Pids} = sys:get_state(whereis(emqx_rule_monitor)),
    ?assertEqual(0, map_size(Pids)),
    ok = emqx_rule_engine:unload_providers(),
    emqx_rule_registry:remove_resource(ResId),
    ok.

t_refresh_resources_rules(_) ->
    ok = emqx_rule_monitor:async_refresh_resources_rules(),
    ok = emqx_rule_monitor:async_refresh_resources_rules(),
    %% there should be only one refresh handler at the same time
    ?assertMatch(#{boot_refresh_pid := Pid} when is_pid(Pid), sys:get_state(whereis(emqx_rule_monitor))),
    timer:sleep(1200),
    ?assertEqual([{refresh_resources, 1}], ets:lookup(t_refresh_resources_rules, refresh_resources)),
    ?assertEqual([{refresh_rules, 1}], ets:lookup(t_refresh_resources_rules, refresh_rules)),
    ok = emqx_rule_monitor:async_refresh_resources_rules(),
    timer:sleep(1200),
    ?assertEqual([{refresh_resources, 2}], ets:lookup(t_refresh_resources_rules, refresh_resources)),
    ?assertEqual([{refresh_rules, 2}], ets:lookup(t_refresh_resources_rules, refresh_rules)).

on_resource_create(Id, _) ->
    case ets:lookup(t_restart_resource, failed_count) of
        [{_, 5}] ->
            ets:insert(t_restart_resource, {succ_count, 1}),
            #{};
        [{_, N}] ->
            ets:insert(t_restart_resource, {failed_count, N+1}),
            error({incorrect_params, Id})
    end.
on_resource_destroy(_Id, _) -> ok.
on_get_resource_status(_Id, _) -> #{}.
