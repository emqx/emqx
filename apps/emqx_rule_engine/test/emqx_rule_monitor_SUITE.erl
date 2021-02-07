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
      ]}
    ].

init_per_suite(Config) ->
    ok = ekka_mnesia:start(),
    ok = emqx_rule_registry:mnesia(boot),
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(t_restart_resource, Config) ->
    Opts = [public, named_table, set, {read_concurrency, true}],
    _ = ets:new(?RES_PARAMS_TAB, [{keypos, #resource_params.id}|Opts]),
    ets:new(t_restart_resource, [named_table, public]),
    ets:insert(t_restart_resource, {failed_count, 0}),
    ets:insert(t_restart_resource, {succ_count, 0}),
    Config;

init_per_testcase(_, Config) ->
    Config.

end_per_testcase(t_restart_resource, Config) ->
    ets:delete(t_restart_resource),
    Config;
end_per_testcase(_, Config) ->
    Config.

t_restart_resource(_) ->
    {ok, _} = emqx_rule_monitor:start_link(),
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
              description => <<"debug resource">>}),
    [{_, 1}] = ets:lookup(t_restart_resource, failed_count),
    [{_, 0}] = ets:lookup(t_restart_resource, succ_count),
    ct:pal("monitor: ~p", [whereis(emqx_rule_monitor)]),
    emqx_rule_monitor:ensure_resource_retrier(ResId, 100),
    timer:sleep(1000),
    [{_, 5}] = ets:lookup(t_restart_resource, failed_count),
    [{_, 1}] = ets:lookup(t_restart_resource, succ_count),
    #{pids := Pids} = sys:get_state(whereis(emqx_rule_monitor)),
    ?assertEqual(0, map_size(Pids)),
    ok = emqx_rule_engine:unload_providers(),
    emqx_rule_registry:remove_resource(ResId),
    emqx_rule_monitor:stop(),
    ok.

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
