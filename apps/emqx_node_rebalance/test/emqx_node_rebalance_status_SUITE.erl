%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_node_rebalance_status_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

all() -> emqx_common_test_helpers:all(?MODULE).

suite() ->
    [{timetrap, {seconds, 90}}].

init_per_suite(Config) ->
    WorkDir = ?config(priv_dir, Config),
    Apps = [
        emqx_conf,
        emqx,
        emqx_node_rebalance
    ],
    Cluster = [
        {emqx_node_rebalance_status_SUITE1, #{
            role => core,
            apps => Apps
        }},
        {emqx_node_rebalance_status_SUITE2, #{
            role => replicant,
            apps => Apps
        }}
    ],
    Nodes = emqx_cth_cluster:start(Cluster, #{work_dir => WorkDir}),
    [{cluster_nodes, Nodes} | Config].

end_per_suite(Config) ->
    ok = emqx_cth_cluster:stop(?config(cluster_nodes, Config)),
    ok.

%%--------------------------------------------------------------------
%% Tests
%%--------------------------------------------------------------------

t_cluster_status(Config) ->
    [CoreNode, ReplicantNode] = ?config(cluster_nodes, Config),
    ok = emqx_node_rebalance_api_proto_v2:node_rebalance_evacuation_start(CoreNode, #{}),

    ?assertMatch(
        #{evacuations := [_], rebalances := []},
        rpc:call(ReplicantNode, emqx_node_rebalance_status, global_status, [])
    ).
