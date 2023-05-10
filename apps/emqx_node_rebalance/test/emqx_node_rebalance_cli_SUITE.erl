%%--------------------------------------------------------------------
%% Copyright (c) 2022-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%%--------------------------------------------------------------------

-module(emqx_node_rebalance_cli_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-import(
    emqx_eviction_agent_test_helpers,
    [emqtt_connect_many/2, stop_many/1, case_specific_node_name/3]
).

-define(START_APPS, [emqx_eviction_agent, emqx_node_rebalance]).

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    emqx_common_test_helpers:start_apps(?START_APPS),
    Config.

end_per_suite(Config) ->
    emqx_common_test_helpers:stop_apps(lists:reverse(?START_APPS)),
    Config.

init_per_testcase(Case = t_rebalance, Config) ->
    _ = emqx_node_rebalance_evacuation:stop(),
    ClusterNodes = emqx_eviction_agent_test_helpers:start_cluster(
        [
            {case_specific_node_name(?MODULE, Case, '_donor'), 2883},
            {case_specific_node_name(?MODULE, Case, '_recipient'), 3883}
        ],
        ?START_APPS
    ),
    [{cluster_nodes, ClusterNodes} | Config];
init_per_testcase(_Case, Config) ->
    _ = emqx_node_rebalance_evacuation:stop(),
    _ = emqx_node_rebalance:stop(),
    Config.

end_per_testcase(t_rebalance, Config) ->
    _ = emqx_node_rebalance_evacuation:stop(),
    _ = emqx_node_rebalance:stop(),
    _ = emqx_eviction_agent_test_helpers:stop_cluster(
        ?config(cluster_nodes, Config),
        ?START_APPS
    );
end_per_testcase(_Case, _Config) ->
    _ = emqx_node_rebalance_evacuation:stop(),
    _ = emqx_node_rebalance:stop().

%%--------------------------------------------------------------------
%% Tests
%%--------------------------------------------------------------------

t_evacuation(_Config) ->
    %% usage
    ok = emqx_node_rebalance_cli:cli(["foobar"]),

    %% status
    ok = emqx_node_rebalance_cli:cli(["status"]),
    ok = emqx_node_rebalance_cli:cli(["node-status"]),
    ok = emqx_node_rebalance_cli:cli(["node-status", atom_to_list(node())]),

    %% start with invalid args
    ?assertNot(
        emqx_node_rebalance_cli:cli(["start", "--evacuation", "--foo-bar"])
    ),

    ?assertNot(
        emqx_node_rebalance_cli:cli(["start", "--evacuation", "--conn-evict-rate", "foobar"])
    ),

    ?assertNot(
        emqx_node_rebalance_cli:cli(["start", "--evacuation", "--sess-evict-rate", "foobar"])
    ),

    ?assertNot(
        emqx_node_rebalance_cli:cli(["start", "--evacuation", "--wait-takeover", "foobar"])
    ),

    ?assertNot(
        emqx_node_rebalance_cli:cli([
            "start",
            "--evacuation",
            "--migrate-to",
            "nonexistent@node"
        ])
    ),
    ?assertNot(
        emqx_node_rebalance_cli:cli([
            "start",
            "--evacuation",
            "--migrate-to",
            ""
        ])
    ),
    ?assertNot(
        emqx_node_rebalance_cli:cli([
            "start",
            "--evacuation",
            "--unknown-arg"
        ])
    ),
    ?assert(
        emqx_node_rebalance_cli:cli([
            "start",
            "--evacuation",
            "--conn-evict-rate",
            "10",
            "--sess-evict-rate",
            "10",
            "--wait-takeover",
            "10",
            "--migrate-to",
            atom_to_list(node()),
            "--redirect-to",
            "srv"
        ])
    ),

    %% status
    ok = emqx_node_rebalance_cli:cli(["status"]),
    ok = emqx_node_rebalance_cli:cli(["node-status"]),
    ok = emqx_node_rebalance_cli:cli(["node-status", atom_to_list(node())]),

    ?assertMatch(
        {enabled, #{}},
        emqx_node_rebalance_evacuation:status()
    ),

    %% already enabled
    ?assertNot(
        emqx_node_rebalance_cli:cli([
            "start",
            "--evacuation",
            "--conn-evict-rate",
            "10",
            "--redirect-to",
            "srv"
        ])
    ),

    %% stop
    true = emqx_node_rebalance_cli:cli(["stop"]),

    false = emqx_node_rebalance_cli:cli(["stop"]),

    ?assertEqual(
        disabled,
        emqx_node_rebalance_evacuation:status()
    ).

t_rebalance(Config) ->
    process_flag(trap_exit, true),

    [{DonorNode, DonorPort}, {RecipientNode, _}] = ?config(cluster_nodes, Config),

    %% start with invalid args
    ?assertNot(
        emqx_node_rebalance_cli(DonorNode, ["start", "--foo-bar"])
    ),

    ?assertNot(
        emqx_node_rebalance_cli(DonorNode, ["start", "--conn-evict-rate", "foobar"])
    ),

    ?assertNot(
        emqx_node_rebalance_cli(DonorNode, ["start", "--abs-conn-threshold", "foobar"])
    ),

    ?assertNot(
        emqx_node_rebalance_cli(DonorNode, ["start", "--rel-conn-threshold", "foobar"])
    ),

    ?assertNot(
        emqx_node_rebalance_cli(DonorNode, ["start", "--sess-evict-rate", "foobar"])
    ),

    ?assertNot(
        emqx_node_rebalance_cli(DonorNode, ["start", "--abs-sess-threshold", "foobar"])
    ),

    ?assertNot(
        emqx_node_rebalance_cli(DonorNode, ["start", "--rel-sess-threshold", "foobar"])
    ),

    ?assertNot(
        emqx_node_rebalance_cli(DonorNode, ["start", "--wait-takeover", "foobar"])
    ),

    ?assertNot(
        emqx_node_rebalance_cli(DonorNode, ["start", "--wait-health-check", "foobar"])
    ),

    ?assertNot(
        emqx_node_rebalance_cli(DonorNode, [
            "start",
            "--nodes",
            "nonexistent@node"
        ])
    ),
    ?assertNot(
        emqx_node_rebalance_cli(DonorNode, [
            "start",
            "--nodes",
            ""
        ])
    ),
    ?assertNot(
        emqx_node_rebalance_cli(DonorNode, [
            "start",
            "--nodes",
            atom_to_list(RecipientNode)
        ])
    ),
    ?assertNot(
        emqx_node_rebalance_cli(DonorNode, [
            "start",
            "--unknown-arg"
        ])
    ),

    Conns = emqtt_connect_many(DonorPort, 20),

    ?assert(
        emqx_node_rebalance_cli(DonorNode, [
            "start",
            "--conn-evict-rate",
            "10",
            "--abs-conn-threshold",
            "10",
            "--rel-conn-threshold",
            "1.1",
            "--sess-evict-rate",
            "10",
            "--abs-sess-threshold",
            "10",
            "--rel-sess-threshold",
            "1.1",
            "--wait-takeover",
            "10",
            "--nodes",
            atom_to_list(DonorNode) ++ "," ++
                atom_to_list(RecipientNode)
        ])
    ),

    %% status
    ok = emqx_node_rebalance_cli(DonorNode, ["status"]),
    ok = emqx_node_rebalance_cli(DonorNode, ["node-status"]),
    ok = emqx_node_rebalance_cli(DonorNode, ["node-status", atom_to_list(DonorNode)]),

    ?assertMatch(
        {enabled, #{}},
        rpc:call(DonorNode, emqx_node_rebalance, status, [])
    ),

    %% already enabled
    ?assertNot(
        emqx_node_rebalance_cli(DonorNode, ["start"])
    ),

    %% stop
    true = emqx_node_rebalance_cli(DonorNode, ["stop"]),

    false = emqx_node_rebalance_cli(DonorNode, ["stop"]),

    ?assertEqual(
        disabled,
        rpc:call(DonorNode, emqx_node_rebalance, status, [])
    ),

    ok = stop_many(Conns).

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

emqx_node_rebalance_cli(Node, Args) ->
    case rpc:call(Node, emqx_node_rebalance_cli, cli, [Args]) of
        {badrpc, Reason} ->
            error(Reason);
        Result ->
            Result
    end.
