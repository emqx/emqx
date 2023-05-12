%%--------------------------------------------------------------------
%% Copyright (c) 2022-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_node_rebalance_evacuation_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("emqx/include/asserts.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-import(
    emqx_eviction_agent_test_helpers,
    [emqtt_connect/1, emqtt_try_connect/1, case_specific_node_name/3]
).

all() -> [{group, one_node}, {group, two_node}].

groups() ->
    [
        {one_node, [], one_node_cases()},
        {two_node, [], two_node_cases()}
    ].

two_node_cases() ->
    [
        t_conn_evicted,
        t_migrate_to,
        t_session_evicted
    ].

one_node_cases() ->
    emqx_common_test_helpers:all(?MODULE) -- two_node_cases().

init_per_suite(Config) ->
    ok = emqx_common_test_helpers:start_apps([]),
    Config.

end_per_suite(_Config) ->
    ok = emqx_common_test_helpers:stop_apps([]),
    ok.

init_per_group(one_node, Config) ->
    [{cluster_type, one_node} | Config];
init_per_group(two_node, Config) ->
    [{cluster_type, two_node} | Config].

end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(Case, Config) ->
    NodesWithPorts =
        case ?config(cluster_type, Config) of
            one_node ->
                [{case_specific_node_name(?MODULE, Case, '_evacuated'), 2883}];
            two_node ->
                [
                    {case_specific_node_name(?MODULE, Case, '_evacuated'), 2883},
                    {case_specific_node_name(?MODULE, Case, '_recipient'), 3883}
                ]
        end,
    ClusterNodes = emqx_eviction_agent_test_helpers:start_cluster(
        NodesWithPorts,
        [emqx_eviction_agent, emqx_node_rebalance],
        [{emqx, data_dir, case_specific_data_dir(Case, Config)}]
    ),
    ok = snabbkaffe:start_trace(),
    [{cluster_nodes, ClusterNodes} | Config].

end_per_testcase(_Case, Config) ->
    ok = snabbkaffe:stop(),
    ok = emqx_eviction_agent_test_helpers:stop_cluster(
        ?config(cluster_nodes, Config),
        [emqx_eviction_agent, emqx_node_rebalance]
    ).

%%--------------------------------------------------------------------
%% Tests
%%--------------------------------------------------------------------

%% One node tests

t_agent_busy(Config) ->
    [{DonorNode, _DonorPort}] = ?config(cluster_nodes, Config),
    ok = rpc:call(DonorNode, emqx_eviction_agent, enable, [other_rebalance, undefined]),

    ?assertEqual(
        {error, eviction_agent_busy},
        rpc:call(DonorNode, emqx_node_rebalance_evacuation, start, [opts(Config)])
    ).

t_already_started(Config) ->
    [{DonorNode, _DonorPort}] = ?config(cluster_nodes, Config),
    ok = rpc:call(DonorNode, emqx_node_rebalance_evacuation, start, [opts(Config)]),

    ?assertEqual(
        {error, already_started},
        rpc:call(DonorNode, emqx_node_rebalance_evacuation, start, [opts(Config)])
    ).

t_not_started(Config) ->
    [{DonorNode, _DonorPort}] = ?config(cluster_nodes, Config),

    ?assertEqual(
        {error, not_started},
        rpc:call(DonorNode, emqx_node_rebalance_evacuation, stop, [])
    ).

t_start(Config) ->
    process_flag(trap_exit, true),

    [{DonorNode, DonorPort}] = ?config(cluster_nodes, Config),

    ok = rpc:call(DonorNode, emqx_node_rebalance_evacuation, start, [opts(Config)]),
    ?assertMatch(
        {error, {use_another_server, #{}}},
        emqtt_try_connect([{port, DonorPort}])
    ).

t_persistence(Config) ->
    process_flag(trap_exit, true),

    [{DonorNode, DonorPort}] = ?config(cluster_nodes, Config),

    ok = rpc:call(DonorNode, emqx_node_rebalance_evacuation, start, [opts(Config)]),

    ?assertMatch(
        {error, {use_another_server, #{}}},
        emqtt_try_connect([{port, DonorPort}])
    ),

    ok = rpc:call(DonorNode, supervisor, terminate_child, [
        emqx_node_rebalance_sup, emqx_node_rebalance_evacuation
    ]),
    {ok, _} = rpc:call(DonorNode, supervisor, restart_child, [
        emqx_node_rebalance_sup, emqx_node_rebalance_evacuation
    ]),

    ?assertMatch(
        {error, {use_another_server, #{}}},
        emqtt_try_connect([{port, DonorPort}])
    ),
    ?assertMatch(
        {enabled, #{conn_evict_rate := 10}},
        rpc:call(DonorNode, emqx_node_rebalance_evacuation, status, [])
    ).

t_unknown_messages(Config) ->
    process_flag(trap_exit, true),

    [{DonorNode, _DonorPort}] = ?config(cluster_nodes, Config),

    ok = rpc:call(DonorNode, emqx_node_rebalance_evacuation, start, [opts(Config)]),

    Pid = rpc:call(DonorNode, erlang, whereis, [emqx_node_rebalance_evacuation]),

    Pid ! unknown,

    ok = gen_server:cast(Pid, unknown),

    ?assertEqual(
        ignored,
        gen_server:call(Pid, unknown)
    ).

%% Two node tests

t_conn_evicted(Config) ->
    process_flag(trap_exit, true),

    [{DonorNode, DonorPort}, _] = ?config(cluster_nodes, Config),

    {ok, C} = emqtt_connect([{clientid, <<"evacuated">>}, {port, DonorPort}]),

    ?assertWaitEvent(
        ok = rpc:call(DonorNode, emqx_node_rebalance_evacuation, start, [opts(Config)]),
        #{?snk_kind := node_evacuation_evict_conn},
        1000
    ),

    ?assertMatch(
        {error, {use_another_server, #{}}},
        emqtt_try_connect([{clientid, <<"connecting">>}, {port, DonorPort}])
    ),

    receive
        {'EXIT', C, {disconnected, 156, _}} -> ok
    after 1000 ->
        ct:fail("Connection not evicted")
    end.

t_migrate_to(Config) ->
    [{DonorNode, _DonorPort}, {RecipientNode, _RecipientPort}] = ?config(cluster_nodes, Config),

    ?assertEqual(
        [RecipientNode],
        rpc:call(DonorNode, emqx_node_rebalance_evacuation, migrate_to, [undefined])
    ),

    ?assertEqual(
        [],
        rpc:call(DonorNode, emqx_node_rebalance_evacuation, migrate_to, [['unknown@node']])
    ),

    ok = rpc:call(RecipientNode, emqx_eviction_agent, enable, [test_rebalance, undefined]),

    ?assertEqual(
        [],
        rpc:call(DonorNode, emqx_node_rebalance_evacuation, migrate_to, [undefined])
    ).

t_session_evicted(Config) ->
    process_flag(trap_exit, true),

    [{DonorNode, DonorPort}, {RecipientNode, _RecipientPort}] = ?config(cluster_nodes, Config),

    {ok, C} = emqtt_connect([
        {port, DonorPort}, {clientid, <<"client_with_sess">>}, {clean_start, false}
    ]),

    ?assertWaitEvent(
        ok = rpc:call(DonorNode, emqx_node_rebalance_evacuation, start, [opts(Config)]),
        #{?snk_kind := node_evacuation_evict_sess_over},
        5000
    ),

    receive
        {'EXIT', C, {disconnected, ?RC_USE_ANOTHER_SERVER, _}} -> ok
    after 1000 ->
        ct:fail("Connection not evicted")
    end,

    [ChannelPid] = rpc:call(DonorNode, emqx_cm_registry, lookup_channels, [<<"client_with_sess">>]),

    ?assertEqual(
        RecipientNode,
        node(ChannelPid)
    ).

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

opts(Config) ->
    #{
        server_reference => <<"srv">>,
        conn_evict_rate => 10,
        sess_evict_rate => 10,
        wait_takeover => 1,
        migrate_to => migrate_to(Config)
    }.

migrate_to(Config) ->
    case ?config(cluster_type, Config) of
        one_node ->
            [];
        two_node ->
            [_, {RecipientNode, _RecipientPort}] = ?config(cluster_nodes, Config),
            [RecipientNode]
    end.

case_specific_data_dir(Case, Config) ->
    case ?config(priv_dir, Config) of
        undefined -> undefined;
        PrivDir -> filename:join(PrivDir, atom_to_list(Case))
    end.
