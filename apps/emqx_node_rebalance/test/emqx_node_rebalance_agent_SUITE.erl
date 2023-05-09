%%--------------------------------------------------------------------
%% Copyright (c) 2022-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_node_rebalance_agent_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-import(
    emqx_eviction_agent_test_helpers,
    [case_specific_node_name/2]
).

all() ->
    [
        {group, local},
        {group, cluster}
    ].

groups() ->
    [
        {local, [], [
            t_enable_disable,
            t_enable_egent_busy,
            t_unknown_messages
        ]},
        {cluster, [], [
            t_rebalance_agent_coordinator_fail,
            t_rebalance_agent_fail
        ]}
    ].

init_per_suite(Config) ->
    ok = emqx_common_test_helpers:start_apps([emqx_eviction_agent, emqx_node_rebalance]),
    Config.

end_per_suite(_Config) ->
    ok = emqx_common_test_helpers:stop_apps([emqx_eviction_agent, emqx_node_rebalance]),
    ok.

init_per_group(local, Config) ->
    [{cluster, false} | Config];
init_per_group(cluster, Config) ->
    [{cluster, true} | Config].

end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(Case, Config) ->
    case ?config(cluster, Config) of
        true ->
            ClusterNodes = emqx_eviction_agent_test_helpers:start_cluster(
                [{case_specific_node_name(?MODULE, Case), 2883}],
                [emqx_eviction_agent, emqx_node_rebalance]
            ),
            [{cluster_nodes, ClusterNodes} | Config];
        false ->
            Config
    end.

end_per_testcase(_Case, Config) ->
    case ?config(cluster, Config) of
        true ->
            emqx_eviction_agent_test_helpers:stop_cluster(
                ?config(cluster_nodes, Config),
                [emqx_eviction_agent, emqx_node_rebalance]
            );
        false ->
            ok
    end.

%%--------------------------------------------------------------------
%% Tests
%%--------------------------------------------------------------------

%% Local tests

t_enable_disable(_Config) ->
    ?assertEqual(
        disabled,
        emqx_node_rebalance_agent:status()
    ),

    ?assertEqual(
        ok,
        emqx_node_rebalance_agent:enable(self())
    ),

    ?assertEqual(
        {error, already_enabled},
        emqx_node_rebalance_agent:enable(self())
    ),

    ?assertEqual(
        {enabled, self()},
        emqx_node_rebalance_agent:status()
    ),

    ?assertEqual(
        {error, invalid_coordinator},
        emqx_node_rebalance_agent:disable(spawn_link(fun() -> ok end))
    ),

    ?assertEqual(
        ok,
        emqx_node_rebalance_agent:disable(self())
    ),

    ?assertEqual(
        {error, already_disabled},
        emqx_node_rebalance_agent:disable(self())
    ),

    ?assertEqual(
        disabled,
        emqx_node_rebalance_agent:status()
    ).

t_enable_egent_busy(_Config) ->
    ok = emqx_eviction_agent:enable(rebalance_test, undefined),

    ?assertEqual(
        {error, eviction_agent_busy},
        emqx_node_rebalance_agent:enable(self())
    ),

    ok = emqx_eviction_agent:disable(rebalance_test).

t_unknown_messages(_Config) ->
    Pid = whereis(emqx_node_rebalance_agent),

    ok = gen_server:cast(Pid, unknown),

    Pid ! unknown,

    ignored = gen_server:call(Pid, unknown).

%% Cluster tests

% The following tests verify that emqx_node_rebalance_agent correctly links
% coordinator process with emqx_eviction_agent-s.

t_rebalance_agent_coordinator_fail(Config) ->
    process_flag(trap_exit, true),

    [{Node, _}] = ?config(cluster_nodes, Config),

    CoordinatorPid = spawn_link(
        fun() ->
            receive
                done -> ok
            end
        end
    ),

    ?assertEqual(
        disabled,
        rpc:call(Node, emqx_eviction_agent, status, [])
    ),

    ?assertEqual(
        ok,
        rpc:call(Node, emqx_node_rebalance_agent, enable, [CoordinatorPid])
    ),

    ?assertMatch(
        {enabled, _},
        rpc:call(Node, emqx_eviction_agent, status, [])
    ),

    EvictionAgentPid = rpc:call(Node, erlang, whereis, [emqx_eviction_agent]),
    true = link(EvictionAgentPid),

    true = exit(CoordinatorPid, kill),

    receive
        {'EXIT', EvictionAgentPid, _} -> true
    after 1000 ->
        ct:fail("emqx_eviction_agent did not exit")
    end.

t_rebalance_agent_fail(Config) ->
    process_flag(trap_exit, true),

    [{Node, _}] = ?config(cluster_nodes, Config),

    CoordinatorPid = spawn_link(
        fun() ->
            receive
                done -> ok
            end
        end
    ),

    ?assertEqual(
        ok,
        rpc:call(Node, emqx_node_rebalance_agent, enable, [CoordinatorPid])
    ),

    EvictionAgentPid = rpc:call(Node, erlang, whereis, [emqx_eviction_agent]),
    true = exit(EvictionAgentPid, kill),

    receive
        {'EXIT', CoordinatorPid, _} -> true
    after 1000 ->
        ct:fail("emqx_node_rebalance_agent did not exit")
    end.
