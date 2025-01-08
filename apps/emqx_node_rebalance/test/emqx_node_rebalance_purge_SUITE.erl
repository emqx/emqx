%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_node_rebalance_purge_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("emqx/include/asserts.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-import(
    emqx_eviction_agent_test_helpers,
    [
        emqtt_connect/1,
        emqtt_try_connect/1,
        case_specific_node_name/3,
        stop_many/1,
        get_mqtt_port/2
    ]
).

-define(ON(NODE, BODY), erpc:call(NODE, fun() -> BODY end)).

all() ->
    [
        {group, durability_enabled},
        {group, one_node},
        {group, two_nodes}
    ].

groups() ->
    Groups = [
        {one_node, [], one_node_cases()},
        {two_nodes, [], two_nodes_cases()}
    ],
    [
        {durability_enabled, [], Groups}
        | Groups
    ].

two_nodes_cases() ->
    [
        t_already_started_two,
        t_session_purged
    ].

one_node_cases() ->
    emqx_common_test_helpers:all(?MODULE) -- two_nodes_cases().

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(one_node, Config) ->
    [{cluster_type, one_node} | Config];
init_per_group(two_nodes, Config) ->
    [{cluster_type, two_nodes} | Config];
init_per_group(durability_enabled, Config) ->
    [{durability, enabled} | Config].

end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(TestCase, Config) ->
    ct:timetrap({seconds, 30}),
    Nodes =
        [Node1 | _] =
        case ?config(cluster_type, Config) of
            one_node ->
                [case_specific_node_name(?MODULE, TestCase, '_1')];
            two_nodes ->
                [
                    case_specific_node_name(?MODULE, TestCase, '_1'),
                    case_specific_node_name(?MODULE, TestCase, '_2')
                ]
        end,
    Spec = #{
        role => core,
        join_to => emqx_cth_cluster:node_name(Node1),
        listeners => true,
        apps => app_specs(Config)
    },
    Cluster = [{Node, Spec} || Node <- Nodes],
    ClusterNodes = emqx_cth_cluster:start(
        Cluster,
        #{work_dir => emqx_cth_suite:work_dir(TestCase, Config)}
    ),
    ok = snabbkaffe:start_trace(),
    [{cluster_nodes, ClusterNodes} | Config].

end_per_testcase(_TestCase, Config) ->
    Nodes = ?config(cluster_nodes, Config),
    ok = snabbkaffe:stop(),
    erpc:multicall(Nodes, meck, unload, []),
    ok = emqx_cth_cluster:stop(Nodes),
    ok.

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

app_specs(CTConfig) ->
    DSConfig =
        case proplists:get_value(durability, CTConfig, disabled) of
            enabled ->
                #{
                    durable_sessions =>
                        #{
                            enable => true
                        }
                };
            disabled ->
                #{}
        end,
    [
        {emqx, #{
            before_start => fun() ->
                emqx_app:set_config_loader(?MODULE)
            end,
            config => DSConfig,
            override_env => [{boot_modules, [broker, listeners]}]
        }},
        emqx_conf,
        {emqx_retainer, #{
            config =>
                #{
                    retainer =>
                        #{enable => true}
                }
        }},
        {emqx_modules, #{
            config =>
                #{delayed => #{enable => true}}
        }},
        emqx_node_rebalance
    ].

opts(_Config) ->
    #{
        purge_rate => 10
    }.

case_specific_data_dir(Case, Config) ->
    case ?config(priv_dir, Config) of
        undefined -> undefined;
        PrivDir -> filename:join(PrivDir, atom_to_list(Case))
    end.

%% to avoid it finishing too fast
with_some_sessions(Node, Fn) ->
    Port = get_mqtt_port(Node, tcp),
    Conns = emqtt_connect_many(Port, 100),
    _ = erpc:call(Node, Fn),
    ok = stop_many(Conns).

drain_exits([ClientPid | Rest]) ->
    receive
        {'EXIT', ClientPid, _Reason} ->
            drain_exits(Rest)
    after 1_000 ->
        ct:pal("mailbox:\n  ~p", [process_info(self(), messages)]),
        ct:fail("pid ~p didn't die", [ClientPid])
    end;
drain_exits([]) ->
    ok.

emqtt_connect_many(Port, Count) ->
    emqtt_connect_many(Port, Count, _StartN = 1).

%% start many clients with mixed clean_start flags
emqtt_connect_many(Port, Count, StartN) ->
    lists:map(
        fun(N) ->
            NBin = integer_to_binary(N),
            ClientId = <<"client-", NBin/binary>>,
            CleanStart = N rem 2 == 0,
            {ok, C} = emqtt_connect([{clientid, ClientId}, {clean_start, CleanStart}, {port, Port}]),
            C
        end,
        lists:seq(StartN, StartN + Count - 1)
    ).

%%--------------------------------------------------------------------
%% Test Cases : one node
%%--------------------------------------------------------------------

t_agent_busy(Config) ->
    [Node] = ?config(cluster_nodes, Config),

    ok = rpc:call(Node, emqx_eviction_agent, enable, [other_rebalance, undefined]),

    erpc:call(Node, fun() ->
        ?assertExit(
            {{{bad_nodes, [{Node, {error, eviction_agent_busy}}]}, _}, _},
            emqx_node_rebalance_purge:start(opts(Config))
        )
    end),

    ok.

t_already_started(Config) ->
    process_flag(trap_exit, true),
    [Node] = ?config(cluster_nodes, Config),
    with_some_sessions(Node, fun() ->
        ok = emqx_node_rebalance_purge:start(opts(Config)),

        ?assertEqual(
            {error, already_started},
            emqx_node_rebalance_purge:start(opts(Config))
        ),

        ?assertEqual(
            ok,
            emqx_node_rebalance_purge:stop()
        ),

        ok
    end),
    ok.

t_not_started(Config) ->
    [Node] = ?config(cluster_nodes, Config),

    ?assertEqual(
        {error, not_started},
        rpc:call(Node, emqx_node_rebalance_purge, stop, [])
    ).

t_start(Config) ->
    process_flag(trap_exit, true),
    [Node] = ?config(cluster_nodes, Config),
    Port = get_mqtt_port(Node, tcp),

    with_some_sessions(Node, fun() ->
        process_flag(trap_exit, true),
        ok = snabbkaffe:start_trace(),

        ?assertEqual(
            ok,
            emqx_node_rebalance_purge:start(opts(Config))
        ),
        ?assertEqual({error, {use_another_server, #{}}}, emqtt_try_connect([{port, Port}])),
        ok
    end),
    ok.

t_non_persistence(Config) ->
    process_flag(trap_exit, true),
    [Node] = ?config(cluster_nodes, Config),
    Port = get_mqtt_port(Node, tcp),

    %% to avoid it finishing too fast
    with_some_sessions(Node, fun() ->
        process_flag(trap_exit, true),
        ok = snabbkaffe:start_trace(),

        ?assertEqual(
            ok,
            emqx_node_rebalance_purge:start(opts(Config))
        ),

        ?assertMatch(
            {error, {use_another_server, #{}}},
            emqtt_try_connect([{port, Port}])
        ),

        ok = supervisor:terminate_child(emqx_node_rebalance_sup, emqx_node_rebalance_purge),
        {ok, _} = supervisor:restart_child(emqx_node_rebalance_sup, emqx_node_rebalance_purge),

        ?assertMatch(
            ok,
            emqtt_try_connect([{port, Port}])
        ),
        ?assertMatch(disabled, emqx_node_rebalance_purge:status()),
        ok
    end),
    ok.

t_unknown_messages(Config) ->
    process_flag(trap_exit, true),

    [Node] = ?config(cluster_nodes, Config),

    ok = rpc:call(Node, emqx_node_rebalance_purge, start, [opts(Config)]),
    Pid = rpc:call(Node, erlang, whereis, [emqx_node_rebalance_purge]),
    Pid ! unknown,
    ok = gen_server:cast(Pid, unknown),
    ?assertEqual(
        ignored,
        gen_server:call(Pid, unknown)
    ),

    ok.

%%--------------------------------------------------------------------
%% Test Cases : two nodes
%%--------------------------------------------------------------------

t_already_started_two(Config) ->
    process_flag(trap_exit, true),
    [Node1, _Node2] = ?config(cluster_nodes, Config),
    with_some_sessions(Node1, fun() ->
        ok = emqx_node_rebalance_purge:start(opts(Config)),

        ?assertEqual(
            {error, already_started},
            emqx_node_rebalance_purge:start(opts(Config))
        ),

        ?assertEqual(
            ok,
            emqx_node_rebalance_purge:stop()
        ),

        ok
    end),
    ?assertEqual(
        {error, not_started},
        rpc:call(Node1, emqx_node_rebalance_purge, stop, [])
    ),

    ok.

t_session_purged(Config) ->
    process_flag(trap_exit, true),

    [Node1, Node2] = ?config(cluster_nodes, Config),
    Port1 = get_mqtt_port(Node1, tcp),
    Port2 = get_mqtt_port(Node2, tcp),

    %% N.B.: it's important to have an asymmetric number of clients for this test, as
    %% otherwise the scenario might happen to finish successfully due to the wrong
    %% reasons!
    NumClientsNode1 = 5,
    NumClientsNode2 = 35,
    Node1Clients = emqtt_connect_many(Port1, NumClientsNode1, _StartN1 = 1),
    Node2Clients = emqtt_connect_many(Port2, NumClientsNode2, _StartN2 = 21),
    AllClientPids = Node1Clients ++ Node2Clients,
    AllClientIds =
        lists:map(
            fun(ClientPid) -> proplists:get_value(clientid, emqtt:info(ClientPid)) end,
            AllClientPids
        ),
    lists:foreach(
        fun(C) ->
            ClientId = proplists:get_value(clientid, emqtt:info(C)),
            Topic = emqx_topic:join([<<"t">>, ClientId]),
            Props = #{},
            Payload = ClientId,
            Opts = [{retain, true}],
            ok = emqtt:publish(C, Topic, Props, Payload, Opts),
            DelayedTopic = emqx_topic:join([<<"$delayed/120">>, Topic]),
            ok = emqtt:publish(C, DelayedTopic, Payload),
            {ok, _, [?RC_GRANTED_QOS_0]} = emqtt:subscribe(C, Topic),
            ok
        end,
        Node1Clients ++ Node2Clients
    ),
    %% Retained messages are persisted asynchronously
    ct:sleep(500),

    ?assertEqual(40, erpc:call(Node2, emqx_retainer, retained_count, [])),
    ?assertEqual(NumClientsNode1, erpc:call(Node1, emqx_delayed, delayed_count, [])),
    ?assertEqual(NumClientsNode2, erpc:call(Node2, emqx_delayed, delayed_count, [])),

    {ok, SRef0} = snabbkaffe:subscribe(
        ?match_event(#{?snk_kind := "cluster_purge_done"}),
        15_000
    ),
    ok = rpc:call(Node1, emqx_node_rebalance_purge, start, [opts(Config)]),
    {ok, _} = snabbkaffe:receive_events(SRef0),

    ?assertEqual([], erpc:call(Node1, emqx_cm, all_channels, [])),
    ?assertEqual([], erpc:call(Node2, emqx_cm, all_channels, [])),
    ?assertEqual(0, erpc:call(Node1, emqx_retainer, retained_count, [])),
    ?assertEqual(0, erpc:call(Node2, emqx_retainer, retained_count, [])),
    ?assertEqual(0, erpc:call(Node1, emqx_delayed, delayed_count, [])),
    ?assertEqual(0, erpc:call(Node2, emqx_delayed, delayed_count, [])),

    ok = drain_exits(AllClientPids),

    FormatFun = undefined,
    ?assertEqual(
        [],
        ?ON(
            Node1,
            lists:flatmap(
                fun(ClientId) ->
                    emqx_mgmt:lookup_client({clientid, ClientId}, FormatFun)
                end,
                AllClientIds
            )
        )
    ),
    ?assertEqual(
        [],
        ?ON(
            Node2,
            lists:flatmap(
                fun(ClientId) ->
                    emqx_mgmt:lookup_client({clientid, ClientId}, FormatFun)
                end,
                AllClientIds
            )
        )
    ),

    ok.
