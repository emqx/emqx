%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_cluster_link_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("emqx/include/asserts.hrl").

-compile(export_all).
-compile(nowarn_export_all).

%%

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    SourceCluster = start_source_cluster(Config),
    TargetCluster = start_target_cluster(Config),
    [
        {source_cluster, SourceCluster},
        {target_cluster, TargetCluster}
        | Config
    ].

end_per_suite(Config) ->
    ok = emqx_cth_cluster:stop(?config(source_cluster, Config)),
    ok = emqx_cth_cluster:stop(?config(target_cluster, Config)).

init_per_testcase(TCName, Config) ->
    emqx_common_test_helpers:init_per_testcase(?MODULE, TCName, Config).

end_per_testcase(TCName, Config) ->
    emqx_common_test_helpers:end_per_testcase(?MODULE, TCName, Config).

%%

start_source_cluster(Config) ->
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
    emqx_cth_cluster:start(
        [
            {emqx_clink_msgfwd_source1, #{apps => SourceApps1 ++ [emqx]}},
            {emqx_clink_msgfwd_source2, #{apps => SourceApps2 ++ [emqx]}}
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ).

start_target_cluster(Config) ->
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
    emqx_cth_cluster:start(
        [
            {emqx_clink_msgfwd_target1, #{apps => TargetApps1 ++ [emqx], base_port => 20100}},
            {emqx_clink_msgfwd_target2, #{apps => TargetApps2 ++ [emqx], base_port => 20200}}
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ).

conf_mqtt_listener(LPort) when is_integer(LPort) ->
    fmt("listeners.tcp.clink { bind = ~p }", [LPort]);
conf_mqtt_listener(_) ->
    "".

conf_log() ->
    "log.file { enable = true, level = debug, path = node.log, supervisor_reports = progress }".

combine([Entry | Rest]) ->
    lists:foldl(fun emqx_cth_suite:merge_config/2, Entry, Rest).

start_cluster_link(Config) ->
    Nodes = nodes_all(Config),
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
    ?config(source_cluster, Config).

nodes_target(Config) ->
    ?config(target_cluster, Config).

%%

t_message_forwarding('init', Config) ->
    Apps = start_cluster_link(Config),
    ok = snabbkaffe:start_trace(),
    [{tc_apps, Apps} | Config];
t_message_forwarding('end', Config) ->
    ok = snabbkaffe:stop(),
    stop_cluster_link(Config).

t_message_forwarding(Config) ->
    [SourceNode1 | _] = nodes_source(Config),
    [TargetNode1 | _] = nodes_target(Config),
    SourceC1 = start_client("t_message_forwarding", SourceNode1),
    TargetC1 = start_client("t_message_forwarding", TargetNode1),
    {ok, _, _} = emqtt:subscribe(TargetC1, <<"t/+">>, qos1),
    {ok, _} = ?block_until(#{?snk_kind := clink_route_sync_complete}),
    {ok, _} = emqtt:publish(SourceC1, <<"t/42">>, <<"hello">>, qos1),
    ?assertReceive({publish, #{topic := <<"t/42">>, payload := <<"hello">>}}),
    ok = emqtt:stop(SourceC1),
    ok = emqtt:stop(TargetC1).

%%

start_client(ClientId, Node) ->
    Port = tcp_port(Node),
    {ok, Client} = emqtt:start_link([{proto_ver, v5}, {clientid, ClientId}, {port, Port}]),
    {ok, _} = emqtt:connect(Client),
    Client.

tcp_port(Node) ->
    {_Host, Port} = erpc:call(Node, emqx_config, get, [[listeners, tcp, default, bind]]),
    Port.

fmt(Fmt, Args) ->
    io_lib:format(Fmt, Args).
