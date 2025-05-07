%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_lcr_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("stdlib/include/assert.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("emqx/include/emqx.hrl").

suite() ->
    [{timetrap, {seconds, 60}}].

all() ->
    [
        {group, v5},
        {group, v3}
    ].

groups() ->
    TCRace = t_massive_connect_race,
    AllTCs = [{group, race} | emqx_common_test_helpers:all(?MODULE) -- [TCRace]],
    [
        {clean_start_true, [{group, v3}, {group, v5}]},
        {clean_start_false, [{group, v3}, {group, v5}]},
        {v3, [{group, lcr_only}, {group, lcr_hybrid}]},
        {v5, [{group, lcr_only}, {group, lcr_hybrid}]},
        {lcr_only, [], AllTCs},
        {lcr_hybrid, [], AllTCs},
        {race, [
            %% this is the group lcr could not support
            %   {group, race_sleep_0},
            {group, race_sleep_10},
            {group, race_sleep_100}
        ]},
        {race_sleep_0, [TCRace]},
        {race_sleep_1, [TCRace]},
        {race_sleep_10, [TCRace]},
        {race_sleep_100, [TCRace]},
        %% @TODO durable sessions
        {ds, []}
    ].

init_per_suite(Config) ->
    ensure_dist(),
    ClientDefaults = #{
        port => 1883,
        proto_ver => v5,
        clean_start => false,
        properties => #{'Session-Expiry-Interval' => 300}
    },
    case emqx_common_test_helpers:ensure_loaded(emqx_conf) of
        true ->
            [
                {client_opts, ClientDefaults},
                {race_sleep, 0}
                | Config
            ];
        false ->
            {skip, standalone_not_supported}
    end.

end_per_suite(_Config) ->
    ok.

init_per_group(clean_start_true, Config) ->
    ClientOpts = ?config(client_opts, Config),
    lists:keystore(client_opts, 1, Config, {client_opts, ClientOpts#{clean_start => true}});
init_per_group(clean_start_false, Config) ->
    ClientOpts = ?config(client_opts, Config),
    lists:keystore(client_opts, 1, Config, {client_opts, ClientOpts#{clean_start => false}});
init_per_group(v3, Config) ->
    ClientOpts = ?config(client_opts, Config),
    lists:keystore(client_opts, 1, Config, {client_opts, ClientOpts#{proto_ver => v3}});
init_per_group(v5, Config) ->
    ClientOpts = ?config(client_opts, Config),
    lists:keystore(client_opts, 1, Config, {client_opts, ClientOpts#{proto_ver => v5}});
init_per_group(lcr_only, Config) ->
    Conf = "broker.enable_linear_channel_registry = true\nbroker.enable_session_registry=false",
    lists:keystore(emqx_conf, 1, Config, {emqx_conf, Conf});
init_per_group(lcr_hybrid, Config) ->
    Conf = "broker.enable_linear_channel_registry = true\nbroker.enable_session_registry=true",
    lists:keystore(emqx_conf, 1, Config, {emqx_conf, Conf});
init_per_group(race_sleep_0, Config) ->
    lists:keystore(race_sleep, 1, Config, {race_sleep, 0});
init_per_group(race_sleep_1, Config) ->
    lists:keystore(race_sleep, 1, Config, {race_sleep, 1});
init_per_group(race_sleep_10, Config) ->
    lists:keystore(race_sleep, 1, Config, {race_sleep, 10});
init_per_group(race_sleep_100, Config) ->
    lists:keystore(race_sleep, 1, Config, {race_sleep, 100});
init_per_group(_, Config) ->
    Config.

end_per_group(_, Config) ->
    Config.

init_per_testcase(TestCase, Config) ->
    process_flag(trap_exit, true),
    ?MODULE:TestCase(init, Config).

end_per_testcase(TestCase, Config) ->
    ct:pal("end _per testcase: ~p~n", [Config]),
    ok = emqx_cth_cluster:stop(
        ?config(cluster_nodes, Config)
    ),
    snabbkaffe:stop(),
    _ = [Fun() || {cleanup, Fun} <- Config],
    emqx_common_test_helpers:call_janitor(60_000),
    emqx_cth_suite:clean_work_dir(emqx_cth_suite:work_dir(TestCase, Config)),
    ok.

t_cores(init, Config) ->
    %% GIVEN: 3 nodes cluster
    Nodes = start_cluster(?FUNCTION_NAME, Config, 3),
    [{cluster_nodes, Nodes} | Config].
t_cores(Config) ->
    ClientId = <<"client1">>,
    [Node1 | _] = Nodes = ?config(cluster_nodes, Config),
    Port1 = get_mqtt_port(Node1, tcp),
    %% WHEN: client connected to the cluster
    start_connect_client(Config, #{clientid => ClientId, port => Port1}),
    %% THEN: client should be registered in all nodes
    ?assertMatch(
        {[Pid, Pid, Pid], []},
        rpc:multicall(Nodes, emqx_cm, lookup_channels, [ClientId])
    ).

t_replicant(init, Config) ->
    %% GIVEN: 5 nodes cluster with 2 replicants
    Nodes = start_cluster(?FUNCTION_NAME, Config, 5),
    [{cluster_nodes, Nodes} | Config].
t_replicant(Config) ->
    ClientId = <<"client1">>,
    [Node1 | _] = Nodes = ?config(cluster_nodes, Config),
    Port1 = get_mqtt_port(Node1, tcp),
    %% WHEN: client connected to the cluster via core node.
    start_connect_client(Config, #{clientid => ClientId, port => Port1}),
    %% THEN: client should be registered in all nodes eventually.
    ?assertMatch(
        {[[Pid], [Pid], [Pid], [Pid], [Pid]], []},
        rpc:multicall(Nodes, emqx_cm, lookup_channels, [ClientId])
    ).

t_replicant_2(init, Config) ->
    %% GIVEN: 5 nodes cluster with 2 replicants
    Nodes = start_cluster(?FUNCTION_NAME, Config, 5),
    [{cluster_nodes, Nodes} | Config].
t_replicant_2(Config) ->
    ClientId = <<"client1">>,
    Nodes = ?config(cluster_nodes, Config),
    Port1 = get_mqtt_port(lists:last(Nodes), tcp),
    %% WHEN: client connected to the cluster via replicant node
    start_connect_client(Config, #{clientid => ClientId, port => Port1}),
    %% THEN: client should be registered in all nodes eventually.
    ?assertMatch(
        {[[Pid], [Pid], [Pid], [Pid], [Pid]], []},
        ?retry(
            _Interval = 100,
            _NTimes = 10,
            {[[Pid], [Pid], [Pid], [Pid], [Pid]], []} =
                rpc:multicall(Nodes, emqx_cm, lookup_channels, [ClientId])
        )
    ).

t_discard_when_replicant_lagging(init, Config) ->
    %% GIVEN: 5 nodes cluster with 2 replicants
    Nodes = start_cluster(?FUNCTION_NAME, Config, 5),
    [{cluster_nodes, Nodes} | Config].
t_discard_when_replicant_lagging(Config) ->
    ClientId = <<"client1">>,
    Nodes = ?config(cluster_nodes, Config),
    Replicant = lists:last(Nodes),
    SuspendedPids = suspend_replicant_rlog(Replicant),
    Port1 = get_mqtt_port(hd(Nodes), tcp),
    start_connect_client(Config, #{
        clientid => ClientId, port => Port1
    }),
    %% Given when replicant (last node) is lagging
    ?assertMatch(
        {[Pid, Pid, Pid, Pid, []], []},
        ?retry(
            _Interval = 100,
            _NTimes = 10,
            {[Pid, Pid, Pid, Pid, []], []} = rpc:multicall(Nodes, emqx_cm, lookup_channels, [
                ClientId
            ])
        )
    ),

    %% WHEN: client reconnect to the lagging replicant
    start_connect_client(Config, #{
        clientid => ClientId, port => get_mqtt_port(Replicant, tcp)
    }),

    %% WHEN: replicant is catching up
    unsuspend_remote_pids(SuspendedPids),
    %% THEN: two clients should be registered in all nodes eventually.
    %% THEN: Only on client should be registered in all nodes.
    ?assertMatch(
        {[[_], [_], [_], [_], [_]], []},
        ?retry(
            _Interval = 100,
            _NTimes = 10,
            {[[_], [_], [_], [_], [_]], []} =
                rpc:multicall(Nodes, emqx_cm, lookup_channels, [ClientId])
        )
    ).

t_takeover_when_replicant_lagging(init, Config) ->
    %% GIVEN: 5 nodes cluster with 2 replicants
    Nodes = start_cluster(?FUNCTION_NAME, Config, 5),
    [{cluster_nodes, Nodes} | Config].
t_takeover_when_replicant_lagging(Config) ->
    ClientId = <<"client1">>,
    Nodes = ?config(cluster_nodes, Config),
    Replicant = lists:last(Nodes),
    SuspendedPids = suspend_replicant_rlog(Replicant),
    Port1 = get_mqtt_port(hd(Nodes), tcp),
    start_connect_client(Config, #{
        clientid => ClientId, port => Port1
    }),
    %% Given when replicant (last node) is lagging
    ?assertMatch(
        {[Pid, Pid, Pid, Pid, []], []},
        ?retry(
            _Interval = 100,
            _NTimes = 10,
            {[Pid, Pid, Pid, Pid, []], []} = rpc:multicall(Nodes, emqx_cm, lookup_channels, [
                ClientId
            ])
        )
    ),

    %% WHEN: client reconnect to the lagging replicant
    ReplicantPort = get_mqtt_port(Replicant, tcp),
    start_connect_client(Config, #{
        clientid => ClientId, port => ReplicantPort
    }),

    %% WHEN: replicant is catching up
    unsuspend_remote_pids(SuspendedPids),
    %% THEN: Only on client should be registered in all nodes.
    ?assertMatch(
        {[[_], [_], [_], [_], [_]], []},
        ?retry(
            _Interval = 100,
            _NTimes = 10,
            {[[_], [_], [_], [_], [_]], []} =
                rpc:multicall(Nodes, emqx_cm, lookup_channels, [ClientId])
        )
    ).

t_takeover_race(init, Config) ->
    %% GIVEN: 5 nodes cluster with 2 replicants
    Nodes = start_cluster(?FUNCTION_NAME, Config, 6),
    [{cluster_nodes, Nodes} | Config].
t_takeover_race(Config) ->
    ClientId = <<"client1">>,
    Nodes = ?config(cluster_nodes, Config),
    Core1 = hd(Nodes),
    Replicant = lists:last(Nodes),
    Port1 = get_mqtt_port(Replicant, tcp),

    Replicant2 = lists:last(Nodes -- [Replicant]),
    ReplicantPort2 = get_mqtt_port(Replicant2, tcp),

    Replicant3 = lists:last(Nodes -- [Replicant, Replicant2]),
    ReplicantPort3 = get_mqtt_port(Replicant3, tcp),
    %% GIVEN: One existing session by Client1
    _Client1 = start_connect_client(Config, #{
        clientid => ClientId, port => Port1
    }),
    SuspendedPids = suspend_channel(Core1, ClientId),

    Parent = self(),
    %% WHEN: Client2 and Client3 race to takeover the session
    Client2 = spawn_link(fun() ->
        Client = start_client(Config, #{
            clientid => ClientId, port => ReplicantPort2
        }),
        Result =
            case emqtt:connect(Client) of
                {ok, _} -> ok;
                {error, {session_taken_over, _} = E} -> E
            end,
        Parent ! {done, self(), Result}
    end),

    timer:sleep(10),
    Client3 = spawn_link(fun() ->
        Client = start_client(Config, #{
            clientid => ClientId, port => ReplicantPort3
        }),
        Result =
            case emqtt:connect(Client) of
                {ok, _} -> ok;
                {error, {session_taken_over, _}} = E -> E
            end,
        Parent ! {done, self(), Result}
    end),
    timer:sleep(10),
    unsuspend_remote_pids(SuspendedPids),

    Results = [
        receive
            {done, C, Result} -> Result
        after 10000 ->
            {timeout, C}
        end
     || C <- [Client2, Client3]
    ],

    %% THEN: the last one (Client3) should never fail
    ?assertMatch([_, ok], Results),

    %% THEN: Last connect wins.
    ok = ?retry(
        100,
        100,
        begin
            [LastWin] = rpc:call(Core1, emqx_cm, lookup_channels, [ClientId]),
            ?assertEqual(Replicant3, node(LastWin))
        end
    ).

t_takeover_timeout(init, Config) ->
    %% GIVEN: 5 nodes cluster with 2 replicants
    Nodes = start_cluster(?FUNCTION_NAME, Config, 6),
    [{cluster_nodes, Nodes} | Config].
t_takeover_timeout(Config) ->
    ClientId = <<"client1">>,
    Nodes = ?config(cluster_nodes, Config),
    Core1 = hd(Nodes),
    Replicant = lists:last(Nodes),
    Port1 = get_mqtt_port(Replicant, tcp),

    Replicant2 = lists:last(Nodes -- [Replicant]),
    ReplicantPort2 = get_mqtt_port(Replicant2, tcp),

    Replicant3 = lists:last(Nodes -- [Replicant, Replicant2]),
    ReplicantPort3 = get_mqtt_port(Replicant3, tcp),
    %% GIVEN: One existing session by Client1
    _Client1 = start_connect_client(Config, #{
        clientid => ClientId, port => Port1
    }),
    SuspendedPids = suspend_channel(Core1, ClientId),

    Parent = self(),
    %% WHEN: Client2 and Client3 race to takeover the session
    Client2 = spawn_link(fun() ->
        Client = start_client(Config, #{
            clientid => ClientId, port => ReplicantPort2
        }),
        Result =
            case emqtt:connect(Client) of
                {ok, _} -> ok;
                {error, {session_taken_over, _} = E} -> E
            end,
        Parent ! {done, self(), Result}
    end),

    timer:sleep(10),
    Client3 = spawn_link(fun() ->
        Client = start_client(Config, #{
            clientid => ClientId, port => ReplicantPort3
        }),
        Result =
            case emqtt:connect(Client) of
                {ok, _} -> ok;
                {error, {session_taken_over, _}} = E -> E
            end,
        Parent ! {done, self(), Result}
    end),
    timer:sleep(10),

    Results = [
        receive
            {done, C, Result} -> Result
        after 10000 ->
            {timeout, C}
        end
     || C <- [Client2, Client3]
    ],
    unsuspend_remote_pids(SuspendedPids),

    %% THEN: the last one (Client3) should never fail
    ?assertMatch([_, ok], Results),

    %% THEN: Last connect wins.
    ok = ?retry(
        100,
        100,
        begin
            [LastWin] = rpc:call(Core1, emqx_cm, lookup_channels, [ClientId]),
            ?assertEqual(Replicant3, node(LastWin))
        end
    ).

t_lcr_cleanup_replicant(init, Config) ->
    %% GIVEN: 5 nodes cluster with 2 replicants
    Nodes = start_cluster(?FUNCTION_NAME, Config, 6),
    [{cluster_nodes, Nodes} | Config].
t_lcr_cleanup_replicant(Config) ->
    ClientId = <<"client1">>,
    ClientsOnR1 = [<<"client11">>, <<"client12">>, <<"client13">>, <<"client14">>],

    ClientId2 = <<"client2">>,
    ClientId3 = <<"client3">>,

    Nodes = ?config(cluster_nodes, Config),

    Replicant = lists:last(Nodes),
    Port1 = get_mqtt_port(Replicant, tcp),

    Replicant2 = lists:last(Nodes -- [Replicant]),
    ReplicantPort2 = get_mqtt_port(Replicant2, tcp),

    Replicant3 = lists:last(Nodes -- [Replicant, Replicant2]),
    ReplicantPort3 = get_mqtt_port(Replicant3, tcp),
    %% GIVEN: Client1[1..4] on replicant node, client2 on replicant2 node
    _Client1 = start_connect_client(Config, #{
        clientid => ClientId, port => Port1
    }),

    _Client2 = start_connect_client(Config, #{
        clientid => ClientId2, port => ReplicantPort2
    }),

    _Client3 = start_connect_client(Config, #{
        clientid => ClientId3, port => ReplicantPort3
    }),

    lists:foreach(
        fun(C) ->
            start_connect_client(Config, #{
                clientid => C,
                port => Port1
            })
        end,
        ClientsOnR1
    ),

    true = erlang:monitor_node(Replicant, true),

    %% WHEN: replicant node is down
    halt_node(Replicant),

    receive
        {nodedown, Replicant} ->
            ok
    end,

    timer:sleep(1000),
    %% THEN: lcr of client1[1..4] should be cleaned up
    %%%      while client2 and client3 remains
    [
        ?assertEqual(
            {[[], [], [], [], []], [Replicant]},
            rpc:multicall(Nodes, emqx_cm, lookup_channels, [C])
        )
     || C <- [ClientId | ClientsOnR1]
    ],
    ?assertMatch(
        {[[_], [_], [_], [_], [_]], [Replicant]},
        rpc:multicall(Nodes, emqx_cm, lookup_channels, [ClientId2])
    ),
    ?assertMatch(
        {[[_], [_], [_], [_], [_]], [Replicant]},
        rpc:multicall(Nodes, emqx_cm, lookup_channels, [ClientId3])
    ).

t_lcr_cleanup_core(init, Config) ->
    %% GIVEN: 5 nodes cluster with 2 replicants
    ClusterSize = 6,
    ClusterSpec = cluster_spec(?FUNCTION_NAME, ClusterSize, Config),
    Nodes = start_cluster(ClusterSpec),
    [{cluster_nodes, Nodes}, {cluster_spec, ClusterSpec} | Config].
t_lcr_cleanup_core(Config) ->
    ClientId = <<"client1">>,
    ClientsOnC1 = [<<"client11">>, <<"client12">>, <<"client13">>, <<"client14">>],

    ClientId2 = <<"client2">>,
    ClientId3 = <<"client3">>,
    Nodes = ?config(cluster_nodes, Config),

    Core1 = hd(Nodes),
    Core1Spec = hd(?config(cluster_spec, Config)),
    Replicant = lists:last(Nodes),
    Port1 = get_mqtt_port(Core1, tcp),

    Replicant2 = lists:last(Nodes -- [Replicant]),
    ReplicantPort2 = get_mqtt_port(Replicant2, tcp),

    Replicant3 = lists:last(Nodes -- [Replicant, Replicant2]),
    ReplicantPort3 = get_mqtt_port(Replicant3, tcp),
    %% GIVEN: Client1[1..4] on core node, client2 on replicant2 node
    _Client1 = start_connect_client(Config, #{
        clientid => ClientId, port => Port1
    }),

    _Client2 = start_connect_client(Config, #{
        clientid => ClientId2, port => ReplicantPort2
    }),

    _Client3 = start_connect_client(Config, #{
        clientid => ClientId3, port => ReplicantPort3
    }),

    lists:foreach(
        fun(C) ->
            start_connect_client(Config, #{
                clientid => C,
                port => Port1
            })
        end,
        ClientsOnC1
    ),

    [
        ?assertMatch(
            {[[_], [_], [_], [_], [_], [_]], []},
            rpc:multicall(Nodes, emqx_cm, lookup_channels, [C])
        )
     || C <- [ClientId | ClientsOnC1]
    ],

    true = erlang:monitor_node(Core1, true),

    %% WHEN: core node is down
    halt_node(Core1),

    receive
        {nodedown, Core1} ->
            ok
    after 5000 ->
        ct:pal("Node down timeout, core node: ~p", [Core1])
    end,

    timer:sleep(1000),

    %% THEN: all clients should be cleaned.
    [
        ?assertMatch(
            {[[_], [_], [_], [_], [_]], [Core1]},
            %rpc:multicall(Nodes, emqx_cm, lookup_channels, [C]))
            rpc:multicall(Nodes, ets, lookup, [emqx_lcr, C])
        )
     || C <- [ClientId | ClientsOnC1]
    ],
    ?assertMatch(
        {[[_], [_], [_], [_], [_]], [Core1]},
        rpc:multicall(Nodes, emqx_cm, lookup_channels, [ClientId2])
    ),
    ?assertMatch(
        {[[_], [_], [_], [_], [_]], [Core1]},
        rpc:multicall(Nodes, emqx_cm, lookup_channels, [ClientId3])
    ),

    %% WHEN: the code node is up
    emqx_cth_cluster:restart(Core1Spec),

    timer:sleep(1000),

    %% THEN: clients which is previously down must be cleaned up
    ?retry(
        100,
        20,
        [
            ?assertEqual(
                {[[], [], [], [], [], []], []},
                rpc:multicall(Nodes, emqx_cm, lookup_channels, [C])
            )
         || C <- [ClientId | ClientsOnC1]
        ]
    ),
    ?assertMatch(
        {[[_], [_], [_], [_], [_], [_]], []},
        rpc:multicall(Nodes, emqx_cm, lookup_channels, [ClientId2])
    ),
    ?assertMatch(
        {[[_], [_], [_], [_], [_], [_]], []},
        rpc:multicall(Nodes, emqx_cm, lookup_channels, [ClientId3])
    ).

t_lcr_batch_cleanup(init, Config) ->
    %% GIVEN: 5 nodes cluster with 2 replicants
    Nodes = start_cluster(?FUNCTION_NAME, Config, 6),
    [{cluster_nodes, Nodes} | Config].
t_lcr_batch_cleanup(Config) ->
    NoCLients = 400,
    ClientsOnR1 = lists:map(
        fun(N) ->
            <<"client", (integer_to_binary(N))/binary>>
        end,
        lists:seq(1, NoCLients)
    ),

    Nodes = ?config(cluster_nodes, Config),

    Replicant = lists:last(Nodes),
    Port1 = get_mqtt_port(Replicant, tcp),

    %% GIVEN: 120 Clients on replicant node
    lists:foreach(
        fun(C) ->
            start_connect_client(Config, #{
                clientid => C,
                port => Port1
            })
        end,
        ClientsOnR1
    ),

    ok = ?retry(
        _Interval = 100,
        _NTimes = 10,
        ?assertEqual(
            {[400, 400, 400, 400, 400, 400], []},
            rpc:multicall(Nodes, emqx_cm, global_chan_cnt, [])
        )
    ),

    true = erlang:monitor_node(Replicant, true),
    %% WHEN: replicant node is down
    halt_node(Replicant),

    receive
        {nodedown, Replicant} ->
            ok
    end,

    timer:sleep(1000),

    %% THEN: eventually there should be no channels on all nodes
    ?assertEqual(
        {[0, 0, 0, 0, 0], [Replicant]},
        rpc:multicall(Nodes, emqx_cm, global_chan_cnt, [])
    ).

t_massive_connect_race(init, Config) ->
    %% GIVEN: 5 nodes cluster with 2 replicants
    Nodes = start_cluster(?FUNCTION_NAME, Config, 5),
    logger:set_module_level(emqtt, debug),
    [{cluster_nodes, Nodes} | Config].
t_massive_connect_race(Config) ->
    ClientId = <<"client_massive_race">>,
    ct:timetrap(10000),
    Nodes = ?config(cluster_nodes, Config),
    Ports = lists:flatten(
        lists:duplicate(
            10,
            lists:map(fun(Node) -> get_mqtt_port(Node, tcp) end, Nodes)
        )
    ),

    Sleep = ?config(race_sleep, Config),

    %%% WHEN: number of clients connect with the same clientid all nodes in the cluster
    Clients = [
        begin
            C = spawn_test_client(ClientId, Port, Config),
            timer:sleep(Sleep),
            C
        end
     || Port <- Ports
    ],

    Results = wait_for_clients_conn_result(Clients, 0, []),
    ct:pal("Results: ~p", [Results]),
    Connected = lists:filtermap(
        fun
            ({Pid, connected}) ->
                {true, Pid};
            ({Pid, {error, _}}) ->
                receive
                    {'EXIT', Pid, _} ->
                        ok
                end,
                false;
            (_) ->
                false
        end,
        Results
    ),
    ct:pal("Connected: ~p", [Connected]),
    ?assert(length(Connected) > 0),

    case Connected of
        [_] ->
            ok;
        _ ->
            wait_for_clients_conn_result(Connected, 1, [])
    end,

    ok = ?retry(
        _Interval = 100,
        _NTimes = 10,
        begin
            {Res, []} = rpc:multicall(Nodes, emqx_cm, lookup_channels, [ClientId]),
            [_] = lists:usort(lists:flatten(Res)),
            ok
        end
    ).

%% t_robustness(init, _Config) ->
%%     ok.
%% t_robustness(_) ->
%%     todo.

%% Helpers
suspend_channel(Node, ClientId) ->
    ChanPids = rpc:call(Node, emqx_cm, lookup_channels, [ClientId]),
    lists:foreach(fun(Pid) -> rpc:call(node(Pid), sys, suspend, [Pid]) end, ChanPids),
    ChanPids.

suspend_replicant_rlog(TargetNode) ->
    ?assertEqual(replicant, mria_rlog:role(TargetNode)),
    lists:map(
        fun(Shard) ->
            {ok, UpstreamPid} = rpc:call(TargetNode, mria_status, upstream, [Shard]),
            ok = rpc:call(node(UpstreamPid), sys, suspend, [UpstreamPid]),
            UpstreamPid
        end,
        [?CM_SHARD, ?LCR_SHARD]
    ).

unsuspend_remote_pids(Pids) ->
    lists:map(
        fun(Pid) ->
            rpc:call(node(Pid), sys, resume, [Pid])
        end,
        Pids
    ).

get_mqtt_port(Node, Type) ->
    {_IP, Port} = erpc:call(Node, emqx_config, get, [[listeners, Type, default, bind]]),
    Port.

wait_nodeup(Node) ->
    ?retry(
        _Sleep0 = 500,
        _Attempts0 = 50,
        pong = net_adm:ping(Node)
    ).

start_client(Config, Opts0 = #{}) ->
    Opts = emqx_utils_maps:deep_merge(?config(client_opts, Config), Opts0),
    ?tp(notice, "starting client", Opts),
    {ok, Client} = emqtt:start_link(maps:to_list(Opts)),
    emqx_common_test_helpers:on_exit(fun() -> catch emqtt:stop(Client) end),
    Client.

start_connect_client(Config, Opts = #{}) ->
    Client = start_client(Config, Opts),
    ?assertMatch({ok, _}, emqtt:connect(Client)),
    Client.

start_cluster(TestCase, Config, ClusterSize) ->
    start_cluster(cluster_spec(TestCase, ClusterSize, Config)).

start_cluster(ClusterSpec) ->
    emqx_cth_cluster:start(ClusterSpec).

ensure_dist() ->
    case net_kernel:get_state() of
        #{started := no} ->
            net_kernel:start(['ct@127.0.0.1', longnames]);
        _ ->
            ok
    end.

-spec cluster_spec(TCName :: atom(), Size :: non_neg_integer(), Config :: proplists:proplist()) ->
    [emqx_cth_cluster:nodespec()].
cluster_spec(TCName, Size, Config) ->
    EmqxConf = ?config(emqx_conf, Config),
    NoCores = min(3, Size),
    NoReplicants = Size - NoCores,
    Cores = [
        {list_to_atom("core" ++ integer_to_list(N)), #{
            role => core, db_backend => rlog, apps => [{emqx, EmqxConf}]
        }}
     || N <- lists:seq(1, NoCores)
    ],
    Replicants = [
        {list_to_atom("replicant" ++ integer_to_list(N)), #{
            role => replicant, db_backend => rlog, apps => [{emqx, EmqxConf}]
        }}
     || N <- lists:seq(1, NoReplicants)
    ],
    Cluster = Cores ++ Replicants,
    emqx_cth_cluster:mk_nodespecs(Cluster, #{work_dir => emqx_cth_suite:work_dir(TCName, Config)}).

halt_node(Node) ->
    rpc:call(Node, timer, apply_after, [10, erlang, halt, [0, [{flush, false}]]]).

%% @doc spawn_test_client
%%      Spawn a test client process which links to the testcase process.
%%      Process terminates with final state or keep running with preliminary state.
%%          (see below)
%%       1. (preliminary/final) connect success with positive MQTT.CONNACK.
%%       3. (final) connect fail with negative MQTT.CONNACK.
%%       2. (final) connect success but then recv MQTT.disconnected by broker.
%%       4. (final) connected but exit due to a crash in broker.
%% @see also wait_for_clients_conn_result/3
%% @end
spawn_test_client(ClientId, Port, TCConfig) ->
    Parent = self(),
    Fun = fun() ->
        Client = start_client(TCConfig, #{
            clientid => ClientId,
            port => Port
        }),
        case emqtt:connect(Client) of
            {ok, _} ->
                Parent ! {done, self(), connected},
                receive
                    {disconnected, ReasonCode, _Properties} ->
                        Parent ! {disconnected, self(), ReasonCode}
                end;
            {error, _} = E ->
                Parent ! {done, self(), E}
        end
    end,
    spawn_link(Fun).

%% @doc wait_for_clients_conn_result
%%     Wait state reports from the test clients excluding the N clients
%% @see also spawn_test_client/3
%% @end
wait_for_clients_conn_result(ClientPids, N, Acc) when length(ClientPids) =< N ->
    Acc;
wait_for_clients_conn_result(ClientPids, N, Acc) ->
    {LeftPids, NewAcc} =
        receive
            {done, Pid, R} ->
                %% emqtt:connect returns
                true = lists:member(Pid, ClientPids),
                {ClientPids -- [Pid], [{Pid, R} | Acc]};
            {disconnected, Pid, R} ->
                %% connected then MQTT.disconnected
                receive
                    {'EXIT', Pid, normal} ->
                        ok
                end,
                {ClientPids -- [Pid], lists:keyreplace(Pid, 1, Acc, {Pid, {disconnected, R}})};
            {'EXIT', Pid, R} when R =/= normal ->
                %% abnormal EXITs during connect or after connected
                case lists:member(Pid, ClientPids) of
                    true ->
                        {ClientPids -- [Pid], [{Pid, {exit, R}} | Acc]};
                    false ->
                        New = lists:keyreplace(Pid, 1, Acc, {Pid, {exit, R}}),
                        {ClientPids, New}
                end
        end,
    wait_for_clients_conn_result(LeftPids, N, NewAcc).
