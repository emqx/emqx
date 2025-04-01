%%--------------------------------------------------------------------
%% Copyright (c) 2019-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-include_lib("emqx/include/asserts.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-define(ROUTER_HELPER, emqx_router_helper).

all() ->
    [
        {group, smoke},
        {group, cleanup},
        {group, cluster},
        {group, cluster_replicant},
        t_cluster_migration
    ].

groups() ->
    SmokeTCs = [t_monitor, t_message],
    CleanupTCs = [t_membership_node_leaving],
    ClusterTCs = [
        t_cluster_node_leaving,
        t_cluster_node_force_leave,
        t_cluster_node_restart
    ],
    ClusterReplicantTCs = [
        t_cluster_node_leaving,
        t_cluster_node_force_leave,
        t_cluster_node_down,
        t_cluster_node_orphan,
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
        {cluster_replicant, [], ClusterReplicantTCs},
        {routing_schema_v1, [], SchemaTCs},
        {routing_schema_v2, [], SchemaTCs}
    ].

init_per_group(GroupName, Config) when
    GroupName == smoke;
    GroupName == cluster;
    GroupName == cluster_replicant;
    GroupName == routing_schema_v1;
    GroupName == routing_schema_v2
->
    WorkDir = emqx_cth_suite:work_dir(Config),
    AppSpecs = [
        {mria, mk_config(mria, GroupName)},
        {emqx, mk_config(emqx, GroupName)}
    ],
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
    GroupName == cluster_replicant;
    GroupName == routing_schema_v1;
    GroupName == routing_schema_v2
->
    ok = emqx_cth_suite:stop(?config(group_apps, Config));
end_per_group(fallback, _Config) ->
    unmock_mria_match_delete(),
    ok;
end_per_group(_GroupName, _Config) ->
    ok.

mk_config(emqx, routing_schema_v1) ->
    #{
        config => "broker.routing.storage_schema = v1",
        override_env => [{boot_modules, [broker]}]
    };
mk_config(emqx, routing_schema_v2) ->
    #{
        config => "broker.routing.storage_schema = v2",
        override_env => [{boot_modules, [broker]}]
    };
mk_config(mria, cluster_replicant) ->
    #{
        override_env => [{node_role, core}, {db_backend, rlog}]
    };
mk_config(emqx, _) ->
    #{override_env => [{boot_modules, [broker]}]};
mk_config(_App, _) ->
    #{}.

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
        #{?snk_kind := router_node_routing_table_purged, node := AnotherNode},
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
        #{?snk_kind := router_node_routing_table_purged, node := ClusterNode},
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
    {ok, SRef} = snabbkaffe:subscribe(
        %% Should be purged after ~2 reconciliations.
        ?match_event(#{?snk_kind := router_node_routing_table_purged, node := ClusterNode}),
        1,
        10_000
    ),
    ok = emqx_cth_cluster:stop([ClusterNode]),
    {ok, _Event} = snabbkaffe:receive_events(SRef),
    ?assertEqual([<<"test/e/f">>], emqx_router:topics()).

t_cluster_node_orphan('init', Config) ->
    Config;
t_cluster_node_orphan('end', _Config) ->
    ok.

t_cluster_node_orphan(_Config) ->
    OrphanNode = emqx_cth_cluster:node_name(cluster_node_orphan),
    emqx_router:add_route(<<"orphan/b/#">>, OrphanNode),
    emqx_router:add_route(<<"test/e/f">>, node()),
    ?assertMatch([_, _], emqx_router:topics()),
    {ok, SRef} = snabbkaffe:subscribe(
        %% Should be purged after ~2 reconciliations.
        ?match_event(#{?snk_kind := router_node_routing_table_purged, node := OrphanNode}),
        1,
        10_000
    ),
    {ok, _Event} = snabbkaffe:receive_events(SRef),
    ?assertEqual([<<"test/e/f">>], emqx_router:topics()).

t_cluster_node_force_leave('init', Config) ->
    start_join_node(cluster_node_force_leave, Config);
t_cluster_node_force_leave('end', Config) ->
    stop_leave_node(Config).

t_cluster_node_force_leave(Config) ->
    ClusterNode = ?config(cluster_node, Config),
    emqx_router:add_route(<<"forceleave/b/#">>, ClusterNode),
    emqx_router:add_route(<<"test/e/f">>, node()),
    ?assertMatch([_, _], emqx_router:topics()),
    {ok, SRef} = snabbkaffe:subscribe(
        ?match_event(#{?snk_kind := router_node_routing_table_purged, node := ClusterNode}),
        1,
        10_000
    ),
    %% Simulate node crash.
    ok = emqx_cth_peer:kill(ClusterNode),
    %% Give Mria some time to recognize the node is down.
    ok = timer:sleep(500),
    case ?config(group_name, Config) of
        cluster ->
            %% Force-leave it.
            ok = emqx_cluster:force_leave(ClusterNode);
        cluster_replicant ->
            %% Replicant is considered as "left" already
            ok
    end,
    {ok, _Event} = snabbkaffe:receive_events(SRef),
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

t_cluster_migration('init', Config) ->
    WorkDir = emqx_cth_suite:work_dir(Config),
    AppSpecs = [emqx],
    NodeSpecs = [
        {t_cluster_migration1, #{apps => AppSpecs}},
        {t_cluster_migration2, #{apps => AppSpecs}},
        {t_cluster_migration3, #{apps => AppSpecs}}
    ],
    Nodes = emqx_cth_cluster:start(NodeSpecs, #{work_dir => WorkDir}),
    ok = snabbkaffe:start_trace(),
    [{cluster, Nodes} | Config];
t_cluster_migration('end', Config) ->
    ok = snabbkaffe:stop(),
    ok = emqx_cth_cluster:stop(?config(cluster, Config)).

t_cluster_migration(Config) ->
    %% Ensuse that the node to leave the cluster follow `emqx_machine` behavior.
    [N1, N2, N3] = ?config(cluster, Config),
    ok = erpc:call(N1, ekka, callback, [stop, fun() -> application:stop(emqx) end]),
    ok = erpc:call(N1, ekka, callback, [start, fun() -> ?tp(leave_restart_complete, #{}) end]),

    %% Start client that constantly subscribes to new, unique topics.
    %% Let it run for a while.
    {Client, CMRef} = connect_client(N1),
    {Loadgen, LMRef} = erlang:spawn_monitor(?MODULE, loop_subscriber, [Client]),
    ok = timer:sleep(1000),

    %% Tell the cluster to "leave" the first node, the one that holds the client.
    %% There's a tiny time window between "leave" announcement and the node actually
    %% disconnecting the clients.
    ok = erpc:call(N2, emqx_cluster, force_leave, [N1]),
    ?block_until(#{?snk_kind := leave_restart_complete, ?snk_meta := #{node := N1}}),
    ?assertEqual([N2, N3], erpc:call(N2, emqx, running_nodes, [])),

    %% No routes are expected to be present in the global routing table.
    ?block_until(#{?snk_kind := router_node_routing_table_purged, node := N1}),
    ?assertEqual([], erpc:call(N2, emqx_router, topics, [])),
    ?assertEqual([], erpc:call(N3, emqx_router, topics, [])),

    %% The client should have been disconnected, and subscriber died as well.
    ?assertReceive({'DOWN', CMRef, process, Client, _Disconnected}),
    ?assertReceive({'DOWN', LMRef, process, Loadgen, _Disconnected}).

connect_client(Node) ->
    {_, Port} = erpc:call(Node, emqx_config, get, [[listeners, tcp, default, bind]]),
    {ok, Client} = emqtt:start_link([{port, Port}]),
    {ok, _} = emqtt:connect(Client),
    MRef = erlang:monitor(process, Client),
    true = erlang:unlink(Client),
    {Client, MRef}.

loop_subscriber(Client) ->
    Interval = 1,
    loop_subscriber(1, Interval, Client).

loop_subscriber(N, Interval, Client) ->
    Topic = emqx_topic:join(["mig", integer_to_list(N), '#']),
    {ok, _, [0]} = emqtt:subscribe(Client, Topic, 0),
    ok = timer:sleep(Interval),
    loop_subscriber(N + 1, Interval, Client).

%%

start_join_node(Name, Config) ->
    case ?config(group_name, Config) of
        cluster_replicant ->
            Role = replicant;
        _Cluster ->
            Role = core
    end,
    [ClusterSpec] = emqx_cth_cluster:mk_nodespecs(
        [{Name, #{apps => [emqx], role => Role, join_to => node()}}],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [ClusterNode] = emqx_cth_cluster:start([ClusterSpec]),
    [{cluster_node, ClusterNode}, {cluster_node_spec, ClusterSpec} | Config].

stop_leave_node(Config) ->
    ClusterNode = ?config(cluster_node, Config),
    emqx_cluster:force_leave(ClusterNode),
    emqx_cth_cluster:stop([ClusterNode]).
