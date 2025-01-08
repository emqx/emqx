%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_node_rebalance_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("emqx/include/asserts.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-import(
    emqx_eviction_agent_test_helpers,
    [
        emqtt_connect_many/1,
        emqtt_connect_many/2,
        emqtt_try_connect/1,
        stop_many/1,
        case_specific_node_name/3,
        start_cluster/3,
        stop_cluster/1
    ]
).

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start([emqx], #{
        work_dir => ?config(priv_dir, Config)
    }),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    emqx_cth_suite:stop(?config(apps, Config)).

init_per_testcase(Case, Config) ->
    NodeNames =
        [
            case_specific_node_name(?MODULE, Case, '_donor'),
            case_specific_node_name(?MODULE, Case, '_recipient')
        ],
    ClusterNodes = start_cluster(
        Config,
        NodeNames,
        [emqx, emqx_node_rebalance]
    ),
    ok = snabbkaffe:start_trace(),
    [{cluster_nodes, ClusterNodes} | Config].

end_per_testcase(_Case, Config) ->
    ok = snabbkaffe:stop(),
    stop_cluster(?config(cluster_nodes, Config)).

%%--------------------------------------------------------------------
%% Tests
%%--------------------------------------------------------------------

t_rebalance(Config) ->
    process_flag(trap_exit, true),

    [{DonorNode, DonorPort}, {RecipientNode, _RecipientPort}] = ?config(cluster_nodes, Config),

    Nodes = [DonorNode, RecipientNode],

    Conns = emqtt_connect_many(DonorPort, 500),

    Opts = #{
        conn_evict_rate => 10,
        sess_evict_rate => 10,
        conn_evict_rpc_timeout => 1000,
        sess_evict_rpc_timeout => 1000,
        evict_interval => 10,
        abs_conn_threshold => 50,
        abs_sess_threshold => 50,
        rel_conn_threshold => 1.0,
        rel_sess_threshold => 1.0,
        wait_health_check => 0.01,
        wait_takeover => 0.01,
        nodes => Nodes
    },

    ?assertWaitEvent(
        ok = rpc:call(DonorNode, emqx_node_rebalance, start, [Opts]),
        #{?snk_kind := emqx_node_rebalance_evict_sess_over},
        10000
    ),

    DonorConnCount = rpc:call(DonorNode, emqx_eviction_agent, connection_count, []),
    DonorSessCount = rpc:call(DonorNode, emqx_eviction_agent, session_count, []),
    DonorDSessCount = rpc:call(DonorNode, emqx_eviction_agent, session_count, [disconnected]),

    RecipientConnCount = rpc:call(RecipientNode, emqx_eviction_agent, connection_count, []),
    RecipientSessCount = rpc:call(RecipientNode, emqx_eviction_agent, session_count, []),
    RecipientDSessCount = rpc:call(RecipientNode, emqx_eviction_agent, session_count, [disconnected]),

    ct:pal(
        "Donor: conn=~p, sess=~p, dsess=~p",
        [DonorConnCount, DonorSessCount, DonorDSessCount]
    ),
    ct:pal(
        "Recipient: conn=~p, sess=~p, dsess=~p",
        [RecipientConnCount, RecipientSessCount, RecipientDSessCount]
    ),

    ?assert(DonorConnCount - 50 =< RecipientConnCount),
    ?assert(DonorDSessCount - 50 =< RecipientDSessCount),

    ok = stop_many(Conns).

t_rebalance_node_crash(Config) ->
    process_flag(trap_exit, true),

    [{DonorNode, DonorPort}, {RecipientNode, _RecipientPort}] = ?config(cluster_nodes, Config),

    Nodes = [DonorNode, RecipientNode],

    Conns = emqtt_connect_many(DonorPort, 500),

    Opts = #{
        conn_evict_rate => 10,
        sess_evict_rate => 10,
        conn_evict_rpc_timeout => 1000,
        sess_evict_rpc_timeout => 1000,
        evict_interval => 10,
        abs_conn_threshold => 50,
        abs_sess_threshold => 50,
        rel_conn_threshold => 1.0,
        rel_sess_threshold => 1.0,
        wait_health_check => 0.01,
        wait_takeover => 0.01,
        nodes => Nodes
    },

    ?assertWaitEvent(
        begin
            ok = rpc:call(DonorNode, emqx_node_rebalance, start, [Opts]),
            emqx_common_test_helpers:stop_peer(RecipientNode)
        end,
        #{?snk_kind := emqx_node_rebalance_started},
        1000
    ),

    ?assertEqual(
        disabled,
        rpc:call(DonorNode, emqx_node_rebalance, status, [])
    ),

    ok = stop_many(Conns).

t_no_need_to_rebalance(Config) ->
    process_flag(trap_exit, true),

    [{DonorNode, DonorPort}, {RecipientNode, _RecipientPort}] = ?config(cluster_nodes, Config),

    Nodes = [DonorNode, RecipientNode],

    Opts = #{
        conn_evict_rate => 10,
        sess_evict_rate => 10,
        conn_evict_rpc_timeout => 1000,
        sess_evict_rpc_timeout => 1000,
        evict_interval => 10,
        abs_conn_threshold => 50,
        abs_sess_threshold => 50,
        rel_conn_threshold => 1.0,
        rel_sess_threshold => 1.0,
        wait_health_check => 0.01,
        wait_takeover => 0.01,
        nodes => Nodes
    },

    ?assertEqual(
        {error, nothing_to_balance},
        rpc:call(DonorNode, emqx_node_rebalance, start, [Opts])
    ),

    Conns = emqtt_connect_many(DonorPort, 50),

    ?assertEqual(
        {error, nothing_to_balance},
        rpc:call(DonorNode, emqx_node_rebalance, start, [Opts])
    ),

    ok = stop_many(Conns).

t_unknown_mesages(Config) ->
    process_flag(trap_exit, true),
    [{DonorNode, DonorPort}, {RecipientNode, _RecipientPort}] = ?config(cluster_nodes, Config),

    Nodes = [DonorNode, RecipientNode],

    Conns = emqtt_connect_many(DonorPort, 500),

    Opts = #{
        wait_health_check => 100,
        abs_conn_threshold => 50,
        nodes => Nodes
    },

    Pid = rpc:call(DonorNode, erlang, whereis, [emqx_node_rebalance]),

    Pid ! unknown,
    ok = gen_server:cast(Pid, unknown),
    ?assertEqual(
        ignored,
        gen_server:call(Pid, unknown)
    ),

    ok = rpc:call(DonorNode, emqx_node_rebalance, start, [Opts]),

    Pid ! unknown,
    ok = gen_server:cast(Pid, unknown),
    ?assertEqual(
        ignored,
        gen_server:call(Pid, unknown)
    ),

    ok = stop_many(Conns).

t_available_nodes(Config) ->
    [{DonorNode, _DonorPort}, {RecipientNode, _RecipientPort}] = ?config(cluster_nodes, Config),

    %% Start eviction agent on RecipientNode so that it will be "occupied"
    %% and not available for rebalance
    ok = rpc:call(RecipientNode, emqx_eviction_agent, enable, [test_rebalance, undefined]),

    %% Only DonorNode should be is available for rebalance, since RecipientNode is "occupied"
    ?assertEqual(
        [DonorNode],
        rpc:call(
            DonorNode,
            emqx_node_rebalance,
            available_nodes,
            [[DonorNode, RecipientNode]]
        )
    ).

t_before_health_check_over(Config) ->
    process_flag(trap_exit, true),

    [{DonorNode, DonorPort}, {RecipientNode, _RecipientPort}] = ?config(cluster_nodes, Config),

    Nodes = [DonorNode, RecipientNode],

    Conns = emqtt_connect_many(DonorPort, 50),

    Opts = #{
        conn_evict_rate => 1,
        sess_evict_rate => 1,
        conn_evict_rpc_timeout => 1000,
        sess_evict_rpc_timeout => 1000,
        evict_interval => 1000,
        abs_conn_threshold => 1,
        abs_sess_threshold => 1,
        rel_conn_threshold => 1.0,
        rel_sess_threshold => 1.0,
        wait_health_check => 2,
        wait_takeover => 100,
        nodes => Nodes
    },

    ?assertWaitEvent(
        begin
            ok = rpc:call(DonorNode, emqx_node_rebalance, start, [Opts]),
            ?assertMatch(
                ok,
                emqtt_try_connect([{port, DonorPort}])
            )
        end,
        #{?snk_kind := node_rebalance_enable_started_prohibiting},
        5000
    ),
    ?assertMatch(
        {error, {use_another_server, #{}}},
        emqtt_try_connect([{port, DonorPort}])
    ),

    stop_many(Conns).
