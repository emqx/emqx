%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_cluster_link_config_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("emqx/include/asserts.hrl").
-include_lib("emqx_utils/include/emqx_message.hrl").

-compile(export_all).
-compile(nowarn_export_all).

-define(BASE_CLINK_MQTT_PORT, 1883).
-define(BASE_CLUSTER_NODE_PORT, 10000).

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    [{work_dir, emqx_cth_suite:work_dir(Config)} | Config].

end_per_suite(_Config) ->
    ok.

init_per_testcase(TCName, Config) ->
    emqx_common_test_helpers:init_per_testcase(?MODULE, TCName, [{tc_name, TCName} | Config]).

end_per_testcase(TCName, Config) ->
    emqx_common_test_helpers:end_per_testcase(?MODULE, TCName, Config).

mk_cluster(N, ClusterName, BaseSpecs, ExtraConf, CTConfig) when is_list(BaseSpecs) ->
    Specs = [
        mk_cluster_nodespec(N, ClusterName, S, I, ExtraConf)
     || {I, S} <- lists:enumerate(BaseSpecs)
    ],
    emqx_cth_cluster:mk_nodespecs(
        Specs,
        #{work_dir => emqx_cth_suite:work_dir(CTConfig)}
    );
mk_cluster(N, ClusterName, Size, ExtraConf, CTConfig) when is_integer(Size) ->
    mk_cluster(N, ClusterName, lists:duplicate(Size, #{}), ExtraConf, CTConfig).

mk_cluster_nodespec(N, ClusterName, BaseSpec, NodeI, ExtraConf) ->
    Conf = mk_emqx_conf(N, ClusterName, NodeI, ExtraConf),
    Spec = BaseSpec#{
        apps => [{emqx_conf, Conf}, emqx_cluster_link],
        base_port => N * ?BASE_CLUSTER_NODE_PORT + NodeI * 100
    },
    {mk_nodename(ClusterName, NodeI), Spec}.

mk_emqx_conf(N, ClusterName, _NodeI = 1, ExtraConf) ->
    MQTTPort = ?BASE_CLINK_MQTT_PORT + N * 10000,
    ListenerConf = conf_mqtt_listener(MQTTPort),
    combine([conf_cluster(ClusterName), ListenerConf, ExtraConf]);
mk_emqx_conf(_, ClusterName, _NodeI, ExtraConf) ->
    combine([conf_cluster(ClusterName), ExtraConf]).

t_config_update_cli('init', Config0) ->
    Config = t_config_update('init', Config0),
    lists:keystore(update_from, 1, Config, {update_from, cli});
t_config_update_cli('end', Config) ->
    t_config_update('end', Config).

t_config_update_cli(Config) ->
    t_config_update(Config).

t_config_update('init', Config) ->
    NamePrefix = ?config(tc_name, Config),
    NameA = fmt("~s_~s", [NamePrefix, "a"]),
    NameB = fmt("~s_~s", [NamePrefix, "b"]),
    NodesA = mk_cluster(1, NameA, 2, conf_log(), Config),
    NodesB = mk_cluster(2, NameB, 2, conf_log(), Config),
    ClusterA = emqx_cth_cluster:start(NodesA),
    ClusterB = emqx_cth_cluster:start(NodesB),
    ok = snabbkaffe:start_trace(),
    [
        {cluster_a, ClusterA},
        {cluster_b, ClusterB},
        {name_a, NameA},
        {name_b, NameB},
        {update_from, api}
        | Config
    ];
t_config_update('end', Config) ->
    ok = snabbkaffe:stop(),
    ok = emqx_cth_cluster:stop(?config(cluster_a, Config)),
    ok = emqx_cth_cluster:stop(?config(cluster_b, Config)).

t_config_update(Config) ->
    ClusterA = [NodeA1 | _] = ?config(cluster_a, Config),
    ClusterB = [NodeB1 | _] = ?config(cluster_b, Config),
    LPortA = tcp_port(NodeA1, clink),
    LPortB = tcp_port(NodeB1, clink),
    NameA = ?config(name_a, Config),
    NameB = ?config(name_b, Config),

    ClientA = start_client("t_config_a", NodeA1),
    ClientB = start_client("t_config_b", NodeB1),

    {ok, _, _} = emqtt:subscribe(ClientA, <<"t/test/1/+">>, qos1),
    {ok, _, _} = emqtt:subscribe(ClientB, <<"t/test-topic">>, qos1),

    %% add link
    LinkConfA = #{
        <<"enable">> => true,
        <<"pool_size">> => 1,
        <<"server">> => fmt("localhost:~p", [LPortB]),
        <<"topics">> => [<<"t/test-topic">>, <<"t/test/#">>],
        <<"name">> => NameB
    },
    LinkConfB = #{
        <<"enable">> => true,
        <<"pool_size">> => 1,
        <<"server">> => fmt("localhost:~p", [LPortA]),
        <<"topics">> => [<<"t/test-topic">>, <<"t/test/#">>],
        <<"name">> => NameA
    },

    {ok, SubRef} = snabbkaffe:subscribe(
        ?match_event(#{?snk_kind := clink_route_bootstrap_complete}),
        %% Num nodes = num actors (durable storage is disabled)
        length(ClusterA) + length(ClusterB),
        30_000
    ),
    ?assertMatch({ok, _}, update(NodeA1, [LinkConfA], Config)),
    ?assertMatch({ok, _}, update(NodeB1, [LinkConfB], Config)),

    ?assertMatch(
        {ok, [#{?snk_kind := clink_route_bootstrap_complete} | _]},
        snabbkaffe:receive_events(SubRef)
    ),

    {ok, _} = emqtt:publish(ClientA, <<"t/test-topic">>, <<"hello-from-a">>, qos1),
    {ok, _} = emqtt:publish(ClientB, <<"t/test/1/1">>, <<"hello-from-b">>, qos1),

    ?assertReceive(
        {publish, #{
            topic := <<"t/test-topic">>, payload := <<"hello-from-a">>, client_pid := ClientB
        }},
        7000
    ),
    ?assertReceive(
        {publish, #{
            topic := <<"t/test/1/1">>, payload := <<"hello-from-b">>, client_pid := ClientA
        }},
        7000
    ),
    %% no more messages expected
    ?assertNotReceive({publish, _Message = #{}}),

    {ok, SubRef1} = snabbkaffe:subscribe(
        ?match_event(#{?snk_kind := clink_route_bootstrap_complete}),
        length(ClusterA),
        30_000
    ),

    %% update link
    LinkConfA1 = LinkConfA#{<<"pool_size">> => 2, <<"topics">> => [<<"t/new/+">>]},
    ?assertMatch({ok, _}, update(NodeA1, [LinkConfA1], Config)),

    ?assertMatch(
        {ok, [#{?snk_kind := clink_route_bootstrap_complete} | _]},
        snabbkaffe:receive_events(SubRef1)
    ),

    %% wait for route sync on ClientA node
    {{ok, _, _}, {ok, _}} = ?wait_async_action(
        emqtt:subscribe(ClientA, <<"t/new/1">>, qos1),
        #{?snk_kind := clink_route_sync_complete, ?snk_meta := #{node := NodeA1}},
        10_000
    ),

    %% not expected to be received anymore
    {ok, _} = emqtt:publish(ClientB, <<"t/test/1/1">>, <<"not-expected-hello-from-b">>, qos1),
    {ok, _} = emqtt:publish(ClientB, <<"t/new/1">>, <<"hello-from-b-1">>, qos1),
    ?assertReceive(
        {publish, #{topic := <<"t/new/1">>, payload := <<"hello-from-b-1">>, client_pid := ClientA}},
        7000
    ),
    ?assertNotReceive({publish, _Message = #{}}),

    %% disable link
    LinkConfA2 = LinkConfA1#{<<"enable">> => false},
    ?assertMatch({ok, _}, update(NodeA1, [LinkConfA2], Config)),
    %% must be already blocked by the receiving cluster even if external routing state is not
    %% updated yet
    {ok, _} = emqtt:publish(ClientB, <<"t/new/1">>, <<"not-expected-hello-from-b-1">>, qos1),

    LinkConfB1 = LinkConfB#{<<"enable">> => false},
    ?assertMatch({ok, _}, update(NodeB1, [LinkConfB1], Config)),
    {ok, _} = emqtt:publish(ClientA, <<"t/test-topic">>, <<"not-expected-hello-from-a">>, qos1),

    ?assertNotReceive({publish, _Message = #{}}, 3000),

    %% delete links
    ?assertMatch({ok, _}, update(NodeA1, [], Config)),
    ?assertMatch({ok, _}, update(NodeB1, [], Config)),
    validate_update_cli_failed(NodeA1, Config),

    ok = emqtt:stop(ClientA),
    ok = emqtt:stop(ClientB).

t_config_validations('init', Config) ->
    NameA = fmt("~s_~s", [?FUNCTION_NAME, "a"]),
    NameB = fmt("~s_~s", [?FUNCTION_NAME, "b"]),
    NodesA = mk_cluster(1, NameA, 1, conf_log(), Config),
    NodesB = mk_cluster(2, NameB, 1, conf_log(), Config),
    ClusterA = emqx_cth_cluster:start(NodesA),
    ClusterB = emqx_cth_cluster:start(NodesB),
    ok = snabbkaffe:start_trace(),
    [
        {cluster_a, ClusterA},
        {cluster_b, ClusterB},
        {name_a, NameA},
        {name_b, NameB}
        | Config
    ];
t_config_validations('end', Config) ->
    ok = snabbkaffe:stop(),
    ok = emqx_cth_cluster:stop(?config(cluster_a, Config)),
    ok = emqx_cth_cluster:stop(?config(cluster_b, Config)).

t_config_validations(Config) ->
    [NodeA] = ?config(cluster_a, Config),
    [NodeB] = ?config(cluster_b, Config),
    NameB = ?config(name_b, Config),

    LPortB = tcp_port(NodeB, clink),

    LinkConfA = #{
        <<"enable">> => true,
        <<"pool_size">> => 1,
        <<"server">> => fmt("localhost:~p", [LPortB]),
        <<"topics">> => [<<"t/test-topic">>, <<"t/test/#">>],
        <<"name">> => NameB
    },
    DuplicatedLinks = [LinkConfA, LinkConfA#{<<"enable">> => false, <<"pool_size">> => 2}],
    ?assertMatch(
        {error, #{reason := #{reason := duplicated_cluster_links, duplicates := _}}},
        erpc:call(NodeA, emqx_cluster_link_config, update, [DuplicatedLinks])
    ),

    InvalidTopics = [<<"t/test/#">>, <<"$LINK/cluster/test/#">>],
    InvalidTopics1 = [<<"t/+/#/+">>, <<>>],
    ?assertMatch(
        {error, #{reason := #{reason := invalid_topics, topics := _}}},
        erpc:call(NodeA, emqx_cluster_link_config, update, [
            [LinkConfA#{<<"topics">> => InvalidTopics}]
        ])
    ),
    ?assertMatch(
        {error, #{reason := #{reason := invalid_topics, topics := _}}},
        erpc:call(NodeA, emqx_cluster_link_config, update, [
            [LinkConfA#{<<"topics">> => InvalidTopics1}]
        ])
    ),
    ?assertMatch(
        {error, #{reason := required_field}},
        erpc:call(NodeA, emqx_cluster_link_config, update, [
            [maps:remove(<<"name">>, LinkConfA)]
        ])
    ),
    ?assertMatch(
        {error, #{reason := required_field}},
        erpc:call(NodeA, emqx_cluster_link_config, update, [[maps:remove(<<"server">>, LinkConfA)]])
    ),
    ?assertMatch(
        {error, #{reason := required_field}},
        erpc:call(NodeA, emqx_cluster_link_config, update, [[maps:remove(<<"topics">>, LinkConfA)]])
    ),

    %% Some valid changes to cover different update scenarios (msg resource changed, actor changed, both changed)
    ?assertMatch(
        {ok, _},
        erpc:call(NodeA, emqx_cluster_link_config, update, [[LinkConfA]])
    ),
    LinkConfUnknown = LinkConfA#{
        <<"name">> => <<"no-cluster">>, <<"server">> => <<"no-cluster.emqx:31883">>
    },
    ?assertMatch(
        {ok, _},
        erpc:call(NodeA, emqx_cluster_link_config, update, [
            [LinkConfA#{<<"pool_size">> => 5}, LinkConfUnknown]
        ])
    ),

    ?assertMatch(
        {ok, _},
        erpc:call(NodeA, emqx_cluster_link_config, update, [
            [LinkConfA, LinkConfUnknown#{<<"topics">> => []}]
        ])
    ),

    ?assertMatch(
        {ok, _},
        erpc:call(
            NodeA,
            emqx_cluster_link_config,
            update,
            [
                [
                    LinkConfA#{
                        <<"clientid">> => <<"new-client">>,
                        <<"username">> => <<"user">>
                    },
                    LinkConfUnknown#{
                        <<"clientid">> => <<"new-client">>,
                        <<"username">> => <<"user">>
                    }
                ]
            ]
        )
    ).

t_config_update_ds('init', Config) ->
    Conf = combine([conf_log(), conf_ds()]),
    NameA = fmt("~s_~s", [?FUNCTION_NAME, "a"]),
    NameB = fmt("~s_~s", [?FUNCTION_NAME, "b"]),
    NodesA = mk_cluster(1, NameA, [#{role => replicant}, #{role => core}], Conf, Config),
    NodesB = mk_cluster(2, NameB, [#{role => replicant}, #{role => core}], Conf, Config),
    ClusterA = emqx_cth_cluster:start(NodesA),
    ClusterB = emqx_cth_cluster:start(NodesB),
    ok = snabbkaffe:start_trace(),
    [
        {cluster_a, ClusterA},
        {cluster_b, ClusterB},
        {name_a, NameA},
        {name_b, NameB}
        | Config
    ];
t_config_update_ds('end', Config) ->
    ok = snabbkaffe:stop(),
    ok = emqx_cth_cluster:stop(?config(cluster_a, Config)),
    ok = emqx_cth_cluster:stop(?config(cluster_b, Config)),
    %% @NOTE: Clean work_dir for this TC to avoid running out of disk space
    %% causing other test run flaky. Uncomment it if you need to preserve the
    %% work_dir for troubleshooting
    emqx_cth_suite:clean_work_dir(?config(work_dir, Config)).

t_config_update_ds(Config) ->
    %% @NOTE: for troubleshooting this TC,
    %% take a look in end_per_testcase/2 to preserve the work dir
    [NodeA1 | _] = ?config(cluster_a, Config),
    [NodeB1 | _] = ?config(cluster_b, Config),
    LPortA = tcp_port(NodeA1, clink),
    LPortB = tcp_port(NodeB1, clink),
    NameA = ?config(name_a, Config),
    NameB = ?config(name_b, Config),

    ClientA = start_client("t_config_a", NodeA1, false),
    ClientB = start_client("t_config_b", NodeB1, false),
    {ok, _, _} = emqtt:subscribe(ClientA, <<"t/test/1/+">>, qos1),
    {ok, _, _} = emqtt:subscribe(ClientB, <<"t/test-topic">>, qos1),

    LinkConfA = #{
        <<"enable">> => true,
        <<"pool_size">> => 1,
        <<"server">> => fmt("localhost:~p", [LPortB]),
        <<"topics">> => [<<"t/test-topic">>, <<"t/test/#">>],
        <<"name">> => NameB
    },
    LinkConfB = #{
        <<"enable">> => true,
        <<"pool_size">> => 1,
        <<"server">> => fmt("localhost:~p", [LPortA]),
        <<"topics">> => [<<"t/test-topic">>, <<"t/test/#">>],
        <<"name">> => NameA
    },

    {ok, SubRef} = snabbkaffe:subscribe(
        ?match_event(#{?snk_kind := clink_route_bootstrap_complete}),
        %% 2 cores = 4 actors (durable storage enabled) + 2 replicants = 2 more actors
        6,
        30_000
    ),
    ?assertMatch({ok, _}, erpc:call(NodeA1, emqx_cluster_link_config, update, [[LinkConfA]])),
    ?assertMatch({ok, _}, erpc:call(NodeB1, emqx_cluster_link_config, update, [[LinkConfB]])),

    ?assertMatch(
        [#{ps_actor_incarnation := 0}], erpc:call(NodeA1, emqx, get_config, [[cluster, links]])
    ),
    ?assertMatch(
        [#{ps_actor_incarnation := 0}], erpc:call(NodeB1, emqx, get_config, [[cluster, links]])
    ),

    ?assertMatch(
        {ok, [#{?snk_kind := clink_route_bootstrap_complete} | _]},
        snabbkaffe:receive_events(SubRef)
    ),

    {ok, _} = emqtt:publish(ClientA, <<"t/test-topic">>, <<"hello-from-a">>, qos1),
    {ok, _} = emqtt:publish(ClientB, <<"t/test/1/1">>, <<"hello-from-b">>, qos1),

    ?assertReceive(
        {publish, #{
            topic := <<"t/test-topic">>, payload := <<"hello-from-a">>, client_pid := ClientB
        }},
        30_000
    ),
    ?assertReceive(
        {publish, #{
            topic := <<"t/test/1/1">>, payload := <<"hello-from-b">>, client_pid := ClientA
        }},
        30_000
    ),
    %% no more messages expected
    ?assertNotReceive({publish, _Message = #{}}),
    {ok, SubRef1} = snabbkaffe:subscribe(
        ?match_event(#{?snk_kind := clink_route_bootstrap_complete}),
        %% 2 nodes (1 replicant) in cluster a (3 actors including ds)
        3,
        30_000
    ),

    %% update link

    LinkConfA1 = LinkConfA#{<<"pool_size">> => 2, <<"topics">> => [<<"t/new/+">>]},
    ?assertMatch({ok, _}, erpc:call(NodeA1, emqx_cluster_link_config, update, [[LinkConfA1]])),

    ?assertMatch(
        {ok, [#{?snk_kind := clink_route_bootstrap_complete} | _]},
        snabbkaffe:receive_events(SubRef1)
    ),

    %% wait for route sync on ClientA node
    {{ok, _, _}, {ok, _}} = ?wait_async_action(
        emqtt:subscribe(ClientA, <<"t/new/1">>, qos1),
        #{
            ?snk_kind := clink_route_sync_complete,
            ?snk_meta := #{node := NodeA1},
            actor := {<<"ps-routes-v1">>, 1}
        },
        10_000
    ),
    %% not expected to be received anymore
    {ok, _} = emqtt:publish(ClientB, <<"t/test/1/1">>, <<"not-expected-hello-from-b">>, qos1),
    {ok, _} = emqtt:publish(ClientB, <<"t/new/1">>, <<"hello-from-b-1">>, qos1),
    ?assertReceive(
        {publish, #{topic := <<"t/new/1">>, payload := <<"hello-from-b-1">>, client_pid := ClientA}},
        30_000
    ),
    ?assertNotReceive({publish, _Message = #{}}),

    ?assertMatch(
        [#{ps_actor_incarnation := 1}], erpc:call(NodeA1, emqx, get_config, [[cluster, links]])
    ),
    ?assertMatch(
        [#{ps_actor_incarnation := 1}], erpc:call(NodeA1, emqx, get_config, [[cluster, links]])
    ),

    ok = emqtt:stop(ClientA),
    ok = emqtt:stop(ClientB).

t_misconfigured_links('init', Config) ->
    NameA = fmt("~s_~s", [?FUNCTION_NAME, "a"]),
    NameB = fmt("~s_~s", [?FUNCTION_NAME, "b"]),
    NodesA = mk_cluster(1, NameA, [#{role => replicant}, #{role => core}], conf_log(), Config),
    NodesB = mk_cluster(2, NameB, [#{role => core}], conf_log(), Config),
    ClusterA = emqx_cth_cluster:start(NodesA),
    ClusterB = emqx_cth_cluster:start(NodesB),
    ok = snabbkaffe:start_trace(),
    [
        {cluster_a, ClusterA},
        {cluster_b, ClusterB},
        {name_a, NameA},
        {name_b, NameB}
        | Config
    ];
t_misconfigured_links('end', Config) ->
    ok = snabbkaffe:stop(),
    ok = emqx_cth_cluster:stop(?config(cluster_a, Config)),
    ok = emqx_cth_cluster:stop(?config(cluster_b, Config)).

t_misconfigured_links(Config) ->
    [NodeA1 | _] = ?config(cluster_a, Config),
    [NodeB1 | _] = ?config(cluster_b, Config),
    LPortA = tcp_port(NodeA1, clink),
    LPortB = tcp_port(NodeB1, clink),
    NameA = ?config(name_a, Config),
    NameB = ?config(name_b, Config),

    ClientA = start_client("t_config_a", NodeA1),
    ClientB = start_client("t_config_b", NodeB1),

    {ok, _, _} = emqtt:subscribe(ClientA, <<"t/test/1/+">>, qos1),
    {ok, _, _} = emqtt:subscribe(ClientB, <<"t/test-topic">>, qos1),

    LinkConfA = #{
        <<"enable">> => true,
        <<"pool_size">> => 1,
        <<"server">> => fmt("localhost:~p", [LPortB]),
        <<"topics">> => [<<"t/test-topic">>, <<"t/test/#">>],
        <<"name">> => <<"bad-b-name">>
    },
    LinkConfB = #{
        <<"enable">> => true,
        <<"pool_size">> => 1,
        <<"server">> => fmt("localhost:~p", [LPortA]),
        <<"topics">> => [<<"t/test-topic">>, <<"t/test/#">>],
        <<"name">> => NameA
    },

    ?assertMatch({ok, _}, erpc:call(NodeB1, emqx_cluster_link_config, update, [[LinkConfB]])),

    {{ok, _}, {ok, _}} = ?wait_async_action(
        erpc:call(NodeA1, emqx_cluster_link_config, update, [[LinkConfA]]),
        #{
            ?snk_kind := clink_handshake_error,
            reason := <<"bad_remote_cluster_link_name">>,
            ?snk_meta := #{node := NodeA1}
        },
        10_000
    ),
    timer:sleep(10),
    ?assertMatch(
        #{error := <<"bad_remote_cluster_link_name">>},
        erpc:call(NodeA1, emqx_cluster_link_router_syncer, status, [<<"bad-b-name">>])
    ),

    {{ok, _}, {ok, _}} = ?wait_async_action(
        erpc:call(NodeA1, emqx_cluster_link_config, update, [[LinkConfA#{<<"name">> => NameB}]]),
        #{
            ?snk_kind := clink_route_bootstrap_complete,
            ?snk_meta := #{node := NodeA1}
        },
        10_000
    ),
    ?assertMatch(
        #{status := connected, error := undefined},
        erpc:call(NodeA1, emqx_cluster_link_router_syncer, status, [NameB])
    ),
    ?assertEqual(
        undefined, erpc:call(NodeA1, emqx_cluster_link_router_syncer, status, [<<"bad-b-name">>])
    ),

    ?assertMatch(
        {ok, _},
        erpc:call(
            NodeB1,
            emqx_cluster_link_config,
            update,
            [
                [
                    LinkConfB#{<<"enable">> => false},
                    %% An extra dummy link to keep B hook/external_broker registered and be able to
                    %% respond with "link disabled error" for the first disabled link
                    LinkConfB#{<<"name">> => <<"bad-a-name">>}
                ]
            ]
        )
    ),

    ?assertMatch({ok, _}, erpc:call(NodeA1, emqx_cluster_link_config, update, [[]])),
    {{ok, _}, {ok, _}} = ?wait_async_action(
        erpc:call(NodeA1, emqx_cluster_link_config, update, [[LinkConfA#{<<"name">> => NameB}]]),
        #{
            ?snk_kind := clink_handshake_error,
            reason := <<"cluster_link_disabled">>,
            ?snk_meta := #{node := NodeA1}
        },
        10_000
    ),
    timer:sleep(10),
    ?assertMatch(
        #{error := <<"cluster_link_disabled">>},
        erpc:call(NodeA1, emqx_cluster_link_router_syncer, status, [NameB])
    ),

    ?assertMatch(
        {ok, _},
        erpc:call(NodeB1, emqx_cluster_link_config, update, [
            [LinkConfB#{<<"name">> => <<"bad-a-name">>}]
        ])
    ),
    ?assertMatch({ok, _}, erpc:call(NodeA1, emqx_cluster_link_config, update, [[]])),

    {{ok, _}, {ok, _}} = ?wait_async_action(
        erpc:call(NodeA1, emqx_cluster_link_config, update, [[LinkConfA#{<<"name">> => NameB}]]),
        #{
            ?snk_kind := clink_handshake_error,
            reason := <<"unknown_cluster">>,
            ?snk_meta := #{node := NodeA1}
        },
        10_000
    ),
    timer:sleep(10),
    ?assertMatch(
        #{error := <<"unknown_cluster">>},
        erpc:call(NodeA1, emqx_cluster_link_router_syncer, status, [NameB])
    ),

    ok = emqtt:stop(ClientA),
    ok = emqtt:stop(ClientB).

start_client(ClientId, Node) ->
    start_client(ClientId, Node, true).

start_client(ClientId, Node, CleanStart) ->
    Port = tcp_port(Node),
    {ok, Client} = emqtt:start_link(
        [
            {proto_ver, v5},
            {clientid, ClientId},
            {port, Port},
            {clean_start, CleanStart}
            | [{properties, #{'Session-Expiry-Interval' => 300}} || CleanStart =:= false]
        ]
    ),
    {ok, _} = emqtt:connect(Client),
    Client.

tcp_port(Node) ->
    tcp_port(Node, default).

tcp_port(Node, Listener) ->
    get_bind_port(erpc:call(Node, emqx_config, get, [[listeners, tcp, Listener, bind]])).

get_bind_port({_Host, Port}) ->
    Port;
get_bind_port(Port) when is_integer(Port) ->
    Port.

combine([Entry | Rest]) ->
    lists:foldl(fun emqx_cth_suite:merge_config/2, Entry, Rest).

conf_mqtt_listener(LPort) when is_integer(LPort) ->
    fmt("listeners.tcp.clink { bind = ~p }", [LPort]);
conf_mqtt_listener(_) ->
    "".

conf_cluster(ClusterName) ->
    fmt("cluster.name = ~s", [ClusterName]).

conf_log() ->
    "log.file { enable = true, level = debug, path = node.log, supervisor_reports = progress }".

conf_ds() ->
    "durable_sessions { enable = true, renew_streams_interval = 100ms, idle_poll_interval = 1s} \n"
    "durable_storage.messages.n_shards = 2".

fmt(Fmt, Args) ->
    emqx_utils:format(Fmt, Args).

mk_nodename(BaseName, Idx) ->
    binary_to_atom(fmt("emqx_clink_~s_~b", [BaseName, Idx])).

validate_update_cli_failed(Node, Config) ->
    case ?config(update_from, Config) of
        api ->
            ok;
        cli ->
            ConfBin = hocon_pp:do(
                #{
                    <<"cluster">> => #{
                        <<"links">> => [],
                        <<"autoclean">> => <<"12h">>
                    }
                },
                #{}
            ),
            ConfFile = prepare_conf_file(?FUNCTION_NAME, ConfBin, Config),
            ?assertMatch(
                {error, <<"Cannot update read-only key 'cluster.autoclean'.">>},
                erpc:call(Node, emqx_conf_cli, conf, [["load", ConfFile]])
            )
    end.

update(Node, Links, Config) ->
    case ?config(update_from, Config) of
        api -> update_links_from_api(Node, Links, Config);
        cli -> update_links_from_cli(Node, Links, Config)
    end.

update_links_from_api(Node, Links, _Config) ->
    erpc:call(Node, emqx_cluster_link_config, update, [Links]).

update_links_from_cli(Node, Links, Config) ->
    ConfBin = hocon_pp:do(#{<<"cluster">> => #{<<"links">> => Links}}, #{}),
    ConfFile = prepare_conf_file(?FUNCTION_NAME, ConfBin, Config),
    case erpc:call(Node, emqx_conf_cli, conf, [["load", ConfFile]]) of
        ok -> {ok, Links};
        Error -> Error
    end.

prepare_conf_file(Name, Content, CTConfig) ->
    Filename = tc_conf_file(Name, CTConfig),
    filelib:ensure_dir(Filename),
    ok = file:write_file(Filename, Content),
    Filename.

tc_conf_file(TC, Config) ->
    DataDir = ?config(data_dir, Config),
    filename:join([DataDir, TC, 'emqx.conf']).
