%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_cluster_link_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("emqx/include/asserts.hrl").
-include_lib("emqx_utils/include/emqx_message.hrl").

-compile(export_all).
-compile(nowarn_export_all).

%%

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(TCName, Config) ->
    emqx_common_test_helpers:init_per_testcase(?MODULE, TCName, Config).

end_per_testcase(TCName, Config) ->
    emqx_common_test_helpers:end_per_testcase(?MODULE, TCName, Config).

%%

mk_source_cluster(BaseName, Config) ->
    SourceConf =
        "cluster {"
        "\n name = cl.source"
        "\n links = ["
        "\n   { enable = true"
        "\n     upstream = cl.target"
        "\n     server = \"localhost:31883\""
        "\n     clientid = client.source"
        "\n     topics = []"
        "\n   }"
        "\n ]}",
    SourceApps1 = [{emqx_conf, combine([conf_log(), SourceConf])}],
    SourceApps2 = [{emqx_conf, combine([conf_log(), conf_mqtt_listener(41883), SourceConf])}],
    emqx_cth_cluster:mk_nodespecs(
        [
            {mk_nodename(BaseName, s1), #{apps => SourceApps1}},
            {mk_nodename(BaseName, s2), #{apps => SourceApps2}}
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ).

mk_target_cluster(BaseName, Config) ->
    TargetConf =
        "cluster {"
        "\n name = cl.target"
        "\n links = ["
        "\n   { enable = true"
        "\n     upstream = cl.source"
        "\n     server = \"localhost:41883\""
        "\n     clientid = client.target"
        "\n     topics = [\"#\"]"
        "\n   }"
        "\n ]}",
    TargetApps1 = [{emqx_conf, combine([conf_log(), TargetConf])}],
    TargetApps2 = [{emqx_conf, combine([conf_log(), conf_mqtt_listener(31883), TargetConf])}],
    emqx_cth_cluster:mk_nodespecs(
        [
            {mk_nodename(BaseName, t1), #{apps => TargetApps1, base_port => 20100}},
            {mk_nodename(BaseName, t2), #{apps => TargetApps2, base_port => 20200}}
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ).

mk_nodename(BaseName, Suffix) ->
    binary_to_atom(fmt("emqx_clink_~s_~s", [BaseName, Suffix])).

conf_mqtt_listener(LPort) when is_integer(LPort) ->
    fmt("listeners.tcp.clink { bind = ~p }", [LPort]);
conf_mqtt_listener(_) ->
    "".

conf_log() ->
    "log.file { enable = true, level = debug, path = node.log, supervisor_reports = progress }".

combine([Entry | Rest]) ->
    lists:foldl(fun emqx_cth_suite:merge_config/2, Entry, Rest).

start_cluster_link(Nodes, Config) ->
    [{ok, Apps}] = lists:usort(
        erpc:multicall(Nodes, emqx_cth_suite, start_apps, [
            [emqx_cluster_link],
            #{work_dir => emqx_cth_suite:work_dir(Config)}
        ])
    ),
    Apps.

stop_cluster_link(Config) ->
    Apps = ?config(tc_apps, Config),
    Nodes = nodes_all(Config),
    [{ok, ok}] = lists:usort(
        erpc:multicall(Nodes, emqx_cth_suite, stop_apps, [Apps])
    ).

%%

nodes_all(Config) ->
    nodes_source(Config) ++ nodes_target(Config).

nodes_source(Config) ->
    ?config(source_nodes, Config).

nodes_target(Config) ->
    ?config(target_nodes, Config).

%%

t_message_forwarding('init', Config) ->
    SourceNodes = emqx_cth_cluster:start(mk_source_cluster(?FUNCTION_NAME, Config)),
    TargetNodes = emqx_cth_cluster:start(mk_target_cluster(?FUNCTION_NAME, Config)),
    _Apps = start_cluster_link(SourceNodes ++ TargetNodes, Config),
    ok = snabbkaffe:start_trace(),
    [
        {source_nodes, SourceNodes},
        {target_nodes, TargetNodes}
        | Config
    ];
t_message_forwarding('end', Config) ->
    ok = snabbkaffe:stop(),
    ok = emqx_cth_cluster:stop(?config(source_nodes, Config)),
    ok = emqx_cth_cluster:stop(?config(target_nodes, Config)).

t_message_forwarding(Config) ->
    [SourceNode1 | _] = nodes_source(Config),
    [TargetNode1, TargetNode2 | _] = nodes_target(Config),
    SourceC1 = start_client("t_message_forwarding", SourceNode1),
    TargetC1 = start_client("t_message_forwarding1", TargetNode1),
    TargetC2 = start_client("t_message_forwarding2", TargetNode2),
    {ok, _, _} = emqtt:subscribe(TargetC1, <<"t/+">>, qos1),
    {ok, _, _} = emqtt:subscribe(TargetC2, <<"t/#">>, qos1),
    {ok, _} = ?block_until(#{?snk_kind := clink_route_sync_complete}),
    {ok, _} = emqtt:publish(SourceC1, <<"t/42">>, <<"hello">>, qos1),
    ?assertReceive(
        {publish, #{topic := <<"t/42">>, payload := <<"hello">>, client_pid := TargetC1}}
    ),
    ?assertReceive(
        {publish, #{topic := <<"t/42">>, payload := <<"hello">>, client_pid := TargetC2}}
    ),
    ?assertNotReceive({publish, _Message = #{}}),
    ok = emqtt:stop(SourceC1),
    ok = emqtt:stop(TargetC1),
    ok = emqtt:stop(TargetC2).

t_target_extrouting_gc('init', Config) ->
    SourceCluster = mk_source_cluster(?FUNCTION_NAME, Config),
    SourceNodes = emqx_cth_cluster:start(SourceCluster),
    TargetCluster = mk_target_cluster(?FUNCTION_NAME, Config),
    TargetNodes = emqx_cth_cluster:start(TargetCluster),
    _Apps = start_cluster_link(SourceNodes ++ TargetNodes, Config),
    ok = snabbkaffe:start_trace(),
    [
        {source_cluster, SourceCluster},
        {source_nodes, SourceNodes},
        {target_cluster, TargetCluster},
        {target_nodes, TargetNodes}
        | Config
    ];
t_target_extrouting_gc('end', Config) ->
    ok = snabbkaffe:stop(),
    ok = emqx_cth_cluster:stop(?config(source_nodes, Config)).

t_target_extrouting_gc(Config) ->
    [SourceNode1 | _] = nodes_source(Config),
    [TargetNode1, TargetNode2 | _] = nodes_target(Config),
    SourceC1 = start_client("t_target_extrouting_gc", SourceNode1),
    TargetC1 = start_client_unlink("t_target_extrouting_gc1", TargetNode1),
    TargetC2 = start_client_unlink("t_target_extrouting_gc2", TargetNode2),
    {ok, _, _} = emqtt:subscribe(TargetC1, <<"t/#">>, qos1),
    {ok, _, _} = emqtt:subscribe(TargetC2, <<"t/+">>, qos1),
    {ok, _} = ?block_until(#{?snk_kind := clink_route_sync_complete}),
    {ok, _} = emqtt:publish(SourceC1, <<"t/1">>, <<"HELLO1">>, qos1),
    {ok, _} = emqtt:publish(SourceC1, <<"t/2/ext">>, <<"HELLO2">>, qos1),
    {ok, _} = emqtt:publish(SourceC1, <<"t/3/ext">>, <<"HELLO3">>, qos1),
    Pubs1 = [M || {publish, M} <- ?drainMailbox(1_000)],
    {ok, _} = ?wait_async_action(
        emqx_cth_cluster:stop_node(TargetNode1),
        #{?snk_kind := clink_extrouter_actor_cleaned, cluster := <<"cl.target">>}
    ),
    {ok, _} = emqtt:publish(SourceC1, <<"t/4/ext">>, <<"HELLO4">>, qos1),
    {ok, _} = emqtt:publish(SourceC1, <<"t/5">>, <<"HELLO5">>, qos1),
    Pubs2 = [M || {publish, M} <- ?drainMailbox(1_000)],
    {ok, _} = ?wait_async_action(
        emqx_cth_cluster:stop_node(TargetNode2),
        #{?snk_kind := clink_extrouter_actor_cleaned, cluster := <<"cl.target">>}
    ),
    ok = emqtt:stop(SourceC1),
    %% Verify that extrouter table eventually becomes empty.
    ?assertEqual(
        [],
        erpc:call(SourceNode1, emqx_cluster_link_extrouter, topics, []),
        {
            erpc:call(SourceNode1, ets, tab2list, [emqx_external_router_actor]),
            erpc:call(SourceNode1, ets, tab2list, [emqx_external_router_route])
        }
    ),
    %% Verify all relevant messages were forwarded.
    ?assertMatch(
        [
            #{topic := <<"t/1">>, payload := <<"HELLO1">>, client_pid := _C1},
            #{topic := <<"t/1">>, payload := <<"HELLO1">>, client_pid := _C2},
            #{topic := <<"t/2/ext">>, payload := <<"HELLO2">>},
            #{topic := <<"t/3/ext">>, payload := <<"HELLO3">>},
            #{topic := <<"t/5">>, payload := <<"HELLO5">>}
        ],
        lists:sort(emqx_utils_maps:key_comparer(topic), Pubs1 ++ Pubs2)
    ),
    %% Verify there was no unnecessary forwarding.
    Trace = snabbkaffe:collect_trace(),
    ?assertMatch(
        [
            #{message := #message{topic = <<"t/1">>, payload = <<"HELLO1">>}},
            #{message := #message{topic = <<"t/2/ext">>, payload = <<"HELLO2">>}},
            #{message := #message{topic = <<"t/3/ext">>, payload = <<"HELLO3">>}},
            #{message := #message{topic = <<"t/5">>, payload = <<"HELLO5">>}}
        ],
        ?of_kind(clink_message_forwarded, Trace),
        Trace
    ).

%%

start_client_unlink(ClientId, Node) ->
    Client = start_client(ClientId, Node),
    _ = erlang:unlink(Client),
    Client.

start_client(ClientId, Node) ->
    Port = tcp_port(Node),
    {ok, Client} = emqtt:start_link([{proto_ver, v5}, {clientid, ClientId}, {port, Port}]),
    {ok, _} = emqtt:connect(Client),
    Client.

tcp_port(Node) ->
    {_Host, Port} = erpc:call(Node, emqx_config, get, [[listeners, tcp, default, bind]]),
    Port.

fmt(Fmt, Args) ->
    emqx_utils:format(Fmt, Args).
