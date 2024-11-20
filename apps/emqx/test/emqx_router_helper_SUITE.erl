%%--------------------------------------------------------------------
%% Copyright (c) 2019-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_router_helper_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-define(ROUTER_HELPER, emqx_router_helper).

all() ->
    [
        {group, smoke},
        {group, cleanup},
        {group, cluster}
    ].

groups() ->
    SmokeTCs = [t_monitor, t_message],
    CleanupTCs = [t_membership_node_leaving],
    ClusterTCs = [
        t_cluster_node_leaving,
        t_cluster_node_down,
        t_cluster_node_restart
    ],
    SchemaTCs = [
        {mria_match_delete, [], CleanupTCs},
        {fallback, [], CleanupTCs}
    ],
    [
        {smoke, [], SmokeTCs},
        {cleanup, [], [
            {group, routing_schema_v1},
            {group, routing_schema_v2}
        ]},
        {cluster, [], ClusterTCs},
        {routing_schema_v1, [], SchemaTCs},
        {routing_schema_v2, [], SchemaTCs}
    ].

init_per_group(GroupName, Config) when
    GroupName == smoke;
    GroupName == cluster;
    GroupName == routing_schema_v1;
    GroupName == routing_schema_v2
->
    WorkDir = emqx_cth_suite:work_dir(Config),
    AppSpecs = [{emqx, mk_config(GroupName)}],
    Apps = emqx_cth_suite:start(AppSpecs, #{work_dir => WorkDir}),
    [{group_name, GroupName}, {group_apps, Apps} | Config];
init_per_group(fallback, Config) ->
    ok = mock_mria_match_delete(),
    Config;
init_per_group(_GroupName, Config) ->
    Config.

end_per_group(GroupName, Config) when
    GroupName == smoke;
    GroupName == cluster;
    GroupName == routing_schema_v1;
    GroupName == routing_schema_v2
->
    ok = emqx_cth_suite:stop(?config(group_apps, Config));
end_per_group(fallback, _Config) ->
    unmock_mria_match_delete(),
    ok;
end_per_group(_GroupName, _Config) ->
    ok.

mk_config(routing_schema_v1) ->
    #{
        config => "broker.routing.storage_schema = v1",
        override_env => [{boot_modules, [broker]}]
    };
mk_config(routing_schema_v2) ->
    #{
        config => "broker.routing.storage_schema = v2",
        override_env => [{boot_modules, [broker]}]
    };
mk_config(_) ->
    #{override_env => [{boot_modules, [broker]}]}.

mock_mria_match_delete() ->
    ok = meck:new(mria, [no_link, passthrough]),
    ok = meck:expect(mria, match_delete, fun(_, _) -> {error, unsupported_otp_version} end).

unmock_mria_match_delete() ->
    ok = meck:unload(mria).

init_per_testcase(TC, Config) ->
    ok = snabbkaffe:start_trace(),
    emqx_common_test_helpers:init_per_testcase(?MODULE, TC, Config).

end_per_testcase(TC, Config) ->
    ok = snabbkaffe:stop(),
    emqx_common_test_helpers:end_per_testcase(?MODULE, TC, Config).

t_monitor(_) ->
    ok = emqx_router_helper:monitor({undefined, node()}),
    ok = emqx_router_helper:monitor(undefined).

t_membership_node_leaving(_Config) ->
    AnotherNode = emqx_cth_cluster:node_name(leaving),
    ok = emqx_router:add_route(<<"leaving1/b/c">>, AnotherNode),
    ok = emqx_router:add_route(<<"test/e/f">>, node()),
    ?assertMatch([_, _], emqx_router:topics()),
    {_, {ok, _}} = ?wait_async_action(
        ?ROUTER_HELPER ! {membership, {node, leaving, AnotherNode}},
        #{?snk_kind := emqx_router_node_purged, node := AnotherNode},
        1_000
    ),
    ?assertEqual([<<"test/e/f">>], emqx_router:topics()).

t_cluster_node_leaving('init', Config) ->
    start_join_node(cluster_node_leaving, Config);
t_cluster_node_leaving('end', Config) ->
    stop_leave_node(Config).

t_cluster_node_leaving(Config) ->
    ClusterNode = ?config(cluster_node, Config),
    ok = emqx_router:add_route(<<"leaving/b/c">>, ClusterNode),
    ok = emqx_router:add_route(<<"test/e/f">>, node()),
    ?assertMatch([_, _], emqx_router:topics()),
    {ok, {ok, _}} = ?wait_async_action(
        erpc:call(ClusterNode, ekka, leave, []),
        #{?snk_kind := emqx_router_node_purged, node := ClusterNode},
        3_000
    ),
    ?assertEqual([<<"test/e/f">>], emqx_router:topics()).

t_cluster_node_down('init', Config) ->
    start_join_node(cluster_node_down, Config);
t_cluster_node_down('end', Config) ->
    stop_leave_node(Config).

t_cluster_node_down(Config) ->
    ClusterNode = ?config(cluster_node, Config),
    emqx_router:add_route(<<"down/b/#">>, ClusterNode),
    emqx_router:add_route(<<"test/e/f">>, node()),
    ?assertMatch([_, _], emqx_router:topics()),
    ok = emqx_cth_cluster:stop([ClusterNode]),
    {scheduled, {ok, _}} = ?wait_async_action(
        %% The same as what would have happened after dead node timeout had passed.
        emqx_router_helper:purge_force(),
        #{?snk_kind := emqx_router_node_purged, node := ClusterNode},
        3_000
    ),
    ?assertEqual([<<"test/e/f">>], emqx_router:topics()).

t_cluster_node_restart('init', Config) ->
    start_join_node(cluster_node_restart, Config);
t_cluster_node_restart('end', Config) ->
    stop_leave_node(Config).

t_cluster_node_restart(Config) ->
    ClusterNode = ?config(cluster_node, Config),
    ClusterSpec = ?config(cluster_node_spec, Config),
    emqx_router:add_route(<<"restart/b/+">>, ClusterNode),
    emqx_router:add_route(<<"test/e/f">>, node()),
    ?assertMatch([_, _], emqx_router:topics()),
    ok = emqx_cth_cluster:stop([ClusterNode]),
    %% The route should still be there, still expecting the node to come back up.
    ?assertMatch([_, _], emqx_router:topics()),
    %% Verify broker is aware there's no reason to route to a node that is down.
    ok = timer:sleep(500),
    ?assertEqual(
        [],
        emqx_broker:publish(emqx_message:make(<<?MODULE_STRING>>, <<"restart/b/c">>, <<>>))
    ),
    _ = emqx_cth_cluster:restart(ClusterSpec),
    %% Node should have cleaned up upon restart.
    ?assertEqual([<<"test/e/f">>], emqx_router:topics()).

t_message(_) ->
    Pid = erlang:whereis(?ROUTER_HELPER),
    ?ROUTER_HELPER ! testing,
    gen_server:cast(?ROUTER_HELPER, testing),
    gen_server:call(?ROUTER_HELPER, testing),
    ?assert(erlang:is_process_alive(Pid)),
    ?assertEqual(Pid, erlang:whereis(?ROUTER_HELPER)).

%%

start_join_node(Name, Config) ->
    [ClusterSpec] = emqx_cth_cluster:mk_nodespecs(
        [{Name, #{apps => [emqx], join_to => node()}}],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [ClusterNode] = emqx_cth_cluster:start([ClusterSpec]),
    [{cluster_node, ClusterNode}, {cluster_node_spec, ClusterSpec} | Config].

stop_leave_node(Config) ->
    ClusterNode = ?config(cluster_node, Config),
    ekka:force_leave(ClusterNode),
    emqx_cth_cluster:stop([ClusterNode]).
