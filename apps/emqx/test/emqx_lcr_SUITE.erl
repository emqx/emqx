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

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    ensure_dist(),
    case emqx_common_test_helpers:ensure_loaded(emqx_conf) of
        true ->
            Config;
        false ->
            {skip, standalone_not_supported}
    end.

end_per_suite(_Config) ->
    ok.

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
    start_connect_client(#{clientid => ClientId, port => Port1}),
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
    start_connect_client(#{clientid => ClientId, port => Port1}),
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
    start_connect_client(#{clientid => ClientId, port => Port1}),
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
    CleanSession = true,
    Nodes = ?config(cluster_nodes, Config),
    Replicant = lists:last(Nodes),
    SuspendedPids = suspend_replicant_rlog(Replicant),
    Port1 = get_mqtt_port(hd(Nodes), tcp),
    start_connect_client(#{clientid => ClientId, port => Port1, clean_session => CleanSession}),
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
    start_connect_client(#{
        clientid => ClientId, port => get_mqtt_port(Replicant, tcp), clean_session => CleanSession
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
    CleanSession = false,
    Nodes = ?config(cluster_nodes, Config),
    Replicant = lists:last(Nodes),
    SuspendedPids = suspend_replicant_rlog(Replicant),
    Port1 = get_mqtt_port(hd(Nodes), tcp),
    start_connect_client(#{clientid => ClientId, port => Port1, clean_session => CleanSession}),
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
    start_connect_client(#{
        clientid => ClientId, port => ReplicantPort, clean_session => CleanSession
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
    process_flag(trap_exit, true),
    ClientId = <<"client1">>,
    CleanSession = false,
    Nodes = ?config(cluster_nodes, Config),
    Core1 = hd(Nodes),
    Replicant = lists:last(Nodes),
    Port1 = get_mqtt_port(Replicant, tcp),

    Replicant2 = lists:last(Nodes -- [Replicant]),
    ReplicantPort2 = get_mqtt_port(Replicant2, tcp),

    Replicant3 = lists:last(Nodes -- [Replicant, Replicant2]),
    ReplicantPort3 = get_mqtt_port(Replicant3, tcp),
    %% GIVEN: One existing session by Client1
    _Client1 = start_connect_client(#{
        clientid => ClientId, port => Port1, clean_session => CleanSession
    }),
    SuspendedPids = suspend_channel(Core1, ClientId),

    Parent = self(),
    %% WHEN: Client2 and Client3 race to takeover the session
    Client2 = spawn_link(fun() ->
        Client = start_client(#{
            clientid => ClientId, port => ReplicantPort2, clean_session => CleanSession
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
        Client = start_client(#{
            clientid => ClientId, port => ReplicantPort3, clean_session => CleanSession
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
    ?assert(
        Results == [{error, {session_taken_over, #{}}}, ok] orelse
            Results == [ok, ok]
    ),

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
    process_flag(trap_exit, true),
    ClientId = <<"client1">>,
    CleanSession = false,
    Nodes = ?config(cluster_nodes, Config),
    Core1 = hd(Nodes),
    Replicant = lists:last(Nodes),
    Port1 = get_mqtt_port(Replicant, tcp),

    Replicant2 = lists:last(Nodes -- [Replicant]),
    ReplicantPort2 = get_mqtt_port(Replicant2, tcp),

    Replicant3 = lists:last(Nodes -- [Replicant, Replicant2]),
    ReplicantPort3 = get_mqtt_port(Replicant3, tcp),
    %% GIVEN: One existing session by Client1
    _Client1 = start_connect_client(#{
        clientid => ClientId, port => Port1, clean_session => CleanSession
    }),
    SuspendedPids = suspend_channel(Core1, ClientId),

    Parent = self(),
    %% WHEN: Client2 and Client3 race to takeover the session
    Client2 = spawn_link(fun() ->
        Client = start_client(#{
            clientid => ClientId, port => ReplicantPort2, clean_session => CleanSession
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
        Client = start_client(#{
            clientid => ClientId, port => ReplicantPort3, clean_session => CleanSession
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
    process_flag(trap_exit, true),
    ClientId = <<"client1">>,
    ClientsOnR1 = [<<"client11">>, <<"client12">>, <<"client13">>, <<"client14">>],

    ClientId2 = <<"client2">>,
    ClientId3 = <<"client3">>,
    CleanSession = false,
    Nodes = ?config(cluster_nodes, Config),

    Replicant = lists:last(Nodes),
    Port1 = get_mqtt_port(Replicant, tcp),

    Replicant2 = lists:last(Nodes -- [Replicant]),
    ReplicantPort2 = get_mqtt_port(Replicant2, tcp),

    Replicant3 = lists:last(Nodes -- [Replicant, Replicant2]),
    ReplicantPort3 = get_mqtt_port(Replicant3, tcp),
    %% GIVEN: Client1[1..4] on replicant node, client2 on replicant2 node
    _Client1 = start_connect_client(#{
        clientid => ClientId, port => Port1, clean_session => CleanSession
    }),

    _Client2 = start_connect_client(#{
        clientid => ClientId2, port => ReplicantPort2, clean_session => CleanSession
    }),

    _Client3 = start_connect_client(#{
        clientid => ClientId3, port => ReplicantPort3, clean_session => CleanSession
    }),

    lists:foreach(
        fun(C) ->
            start_connect_client(#{
                clientid => C,
                port => Port1,
                clean_session => CleanSession
            })
        end,
        ClientsOnR1
    ),

    true = erlang:monitor_node(Replicant, true),

    %% WHEN: replicant node is down
    emqx_cth_cluster:stop([Replicant]),

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
    process_flag(trap_exit, true),
    ClientId = <<"client1">>,
    ClientsOnC1 = [<<"client11">>, <<"client12">>, <<"client13">>, <<"client14">>],

    ClientId2 = <<"client2">>,
    ClientId3 = <<"client3">>,
    CleanSession = false,
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
    _Client1 = start_connect_client(#{
        clientid => ClientId, port => Port1, clean_session => CleanSession
    }),

    _Client2 = start_connect_client(#{
        clientid => ClientId2, port => ReplicantPort2, clean_session => CleanSession
    }),

    _Client3 = start_connect_client(#{
        clientid => ClientId3, port => ReplicantPort3, clean_session => CleanSession
    }),

    lists:foreach(
        fun(C) ->
            start_connect_client(#{
                clientid => C,
                port => Port1,
                clean_session => CleanSession
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
    rpc:call(Core1, timer, apply_after, [1000, erlang, halt, [0]]),

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

    timer:sleep(5000),

    %% THEN: clients which is previously down must be cleaned up
    [
        ?assertEqual(
            {[[], [], [], [], [], []], []},
            rpc:multicall(Nodes, emqx_cm, lookup_channels, [C])
        )
     || C <- [ClientId | ClientsOnC1]
    ],
    ?assertMatch(
        {[[_], [_], [_], [_], [_], [_]], []},
        rpc:multicall(Nodes, emqx_cm, lookup_channels, [ClientId2])
    ),
    ?assertMatch(
        {[[_], [_], [_], [_], [_], [_]], []},
        rpc:multicall(Nodes, emqx_cm, lookup_channels, [ClientId3])
    ).

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

start_client(Opts0 = #{}) ->
    Defaults = #{
        port => 1883,
        proto_ver => v5,
        clean_start => false,
        properties => #{'Session-Expiry-Interval' => 300}
    },
    Opts = emqx_utils_maps:deep_merge(Defaults, Opts0),
    ?tp(notice, "starting client", Opts),
    {ok, Client} = emqtt:start_link(maps:to_list(Opts)),
    emqx_common_test_helpers:on_exit(fun() -> catch emqtt:stop(Client) end),
    Client.

start_connect_client(Opts = #{}) ->
    Client = start_client(Opts),
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
    EmqxConf = "broker.enable_linear_channel_registry = true\nbroker.enable_session_registry=false",
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
