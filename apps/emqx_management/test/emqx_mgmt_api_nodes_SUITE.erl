%%--------------------------------------------------------------------
%% Copyright (c) 2020-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------
-module(emqx_mgmt_api_nodes_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    emqx_mgmt_api_test_util:init_suite([emqx_conf, emqx_management]),
    Config.

end_per_suite(_) ->
    emqx_mgmt_api_test_util:end_suite([emqx_management, emqx_conf]).

init_per_testcase(t_log_path, Config) ->
    emqx_config_logger:add_handler(),
    Log = emqx_conf:get_raw([log], #{}),
    File = "log/emqx-test.log",
    Log1 = emqx_utils_maps:deep_put([<<"file">>, <<"default">>, <<"enable">>], Log, true),
    Log2 = emqx_utils_maps:deep_put([<<"file">>, <<"default">>, <<"path">>], Log1, File),
    {ok, #{}} = emqx_conf:update([log], Log2, #{rawconf_with_defaults => true}),
    Config;
init_per_testcase(_, Config) ->
    Config.

end_per_testcase(t_log_path, Config) ->
    Log = emqx_conf:get_raw([log], #{}),
    Log1 = emqx_utils_maps:deep_put([<<"file">>, <<"default">>, <<"enable">>], Log, false),
    {ok, #{}} = emqx_conf:update([log], Log1, #{rawconf_with_defaults => true}),
    emqx_config_logger:remove_handler(),
    Config;
end_per_testcase(_, Config) ->
    Config.

t_nodes_api(_) ->
    NodesPath = emqx_mgmt_api_test_util:api_path(["nodes"]),
    {ok, Nodes} = emqx_mgmt_api_test_util:request_api(get, NodesPath),
    NodesResponse = emqx_utils_json:decode(Nodes, [return_maps]),
    LocalNodeInfo = hd(NodesResponse),
    Node = binary_to_atom(maps:get(<<"node">>, LocalNodeInfo), utf8),
    ?assertEqual(Node, node()),
    Edition = maps:get(<<"edition">>, LocalNodeInfo),
    ?assertEqual(emqx_release:edition_longstr(), Edition),

    Conns = maps:get(<<"connections">>, LocalNodeInfo),
    ?assertEqual(0, Conns),
    LiveConns = maps:get(<<"live_connections">>, LocalNodeInfo),
    ?assertEqual(0, LiveConns),

    NodePath = emqx_mgmt_api_test_util:api_path(["nodes", atom_to_list(node())]),
    {ok, NodeInfo} = emqx_mgmt_api_test_util:request_api(get, NodePath),
    NodeNameResponse =
        binary_to_atom(maps:get(<<"node">>, emqx_utils_json:decode(NodeInfo, [return_maps])), utf8),
    ?assertEqual(node(), NodeNameResponse),

    BadNodePath = emqx_mgmt_api_test_util:api_path(["nodes", "badnode"]),
    ?assertMatch(
        {error, {_, 404, _}},
        emqx_mgmt_api_test_util:request_api(get, BadNodePath)
    ).

t_log_path(_) ->
    NodePath = emqx_mgmt_api_test_util:api_path(["nodes", atom_to_list(node())]),
    {ok, NodeInfo} = emqx_mgmt_api_test_util:request_api(get, NodePath),
    #{<<"log_path">> := Path} = emqx_utils_json:decode(NodeInfo, [return_maps]),
    ?assertEqual(
        <<"log">>,
        filename:basename(Path)
    ).

t_node_stats_api(_) ->
    StatsPath = emqx_mgmt_api_test_util:api_path(["nodes", atom_to_binary(node(), utf8), "stats"]),
    SystemStats = emqx_mgmt:get_stats(),
    {ok, StatsResponse} = emqx_mgmt_api_test_util:request_api(get, StatsPath),
    Stats = emqx_utils_json:decode(StatsResponse, [return_maps]),
    Fun =
        fun(Key) ->
            ?assertEqual(maps:get(Key, SystemStats), maps:get(atom_to_binary(Key, utf8), Stats))
        end,
    lists:foreach(Fun, maps:keys(SystemStats)),

    BadNodePath = emqx_mgmt_api_test_util:api_path(["nodes", "badnode", "stats"]),
    ?assertMatch(
        {error, {_, 404, _}},
        emqx_mgmt_api_test_util:request_api(get, BadNodePath)
    ).

t_node_metrics_api(_) ->
    MetricsPath =
        emqx_mgmt_api_test_util:api_path(["nodes", atom_to_binary(node(), utf8), "metrics"]),
    SystemMetrics = emqx_mgmt:get_metrics(),
    {ok, MetricsResponse} = emqx_mgmt_api_test_util:request_api(get, MetricsPath),
    Metrics = emqx_utils_json:decode(MetricsResponse, [return_maps]),
    Fun =
        fun(Key) ->
            ?assertEqual(maps:get(Key, SystemMetrics), maps:get(atom_to_binary(Key, utf8), Metrics))
        end,
    lists:foreach(Fun, maps:keys(SystemMetrics)),

    BadNodePath = emqx_mgmt_api_test_util:api_path(["nodes", "badnode", "metrics"]),
    ?assertMatch(
        {error, {_, 404, _}},
        emqx_mgmt_api_test_util:request_api(get, BadNodePath)
    ).

t_multiple_nodes_api(_) ->
    net_kernel:start(['node_api@127.0.0.1', longnames]),
    ct:timetrap({seconds, 120}),
    snabbkaffe:fix_ct_logging(),
    Seq1 = list_to_atom(atom_to_list(?MODULE) ++ "1"),
    Seq2 = list_to_atom(atom_to_list(?MODULE) ++ "2"),
    Cluster = [{Name, Opts}, {Name1, Opts1}] = cluster([{core, Seq1}, {core, Seq2}]),
    ct:pal("Starting ~p", [Cluster]),
    Node1 = emqx_common_test_helpers:start_slave(Name, Opts),
    Node2 = emqx_common_test_helpers:start_slave(Name1, Opts1),
    try
        {200, NodesList} = rpc:call(Node1, emqx_mgmt_api_nodes, nodes, [get, #{}]),
        All = [Node1, Node2],
        lists:map(
            fun(N) ->
                N1 = maps:get(node, N),
                ?assertEqual(true, lists:member(N1, All))
            end,
            NodesList
        ),
        ?assertEqual(2, length(NodesList)),

        {200, Node11} = rpc:call(Node1, emqx_mgmt_api_nodes, node, [
            get, #{bindings => #{node => Node1}}
        ]),
        ?assertMatch(#{node := Node1}, Node11)
    after
        emqx_common_test_helpers:stop_slave(Node1),
        emqx_common_test_helpers:stop_slave(Node2)
    end,
    ok.

cluster(Specs) ->
    Env = [{emqx, boot_modules, []}],
    emqx_common_test_helpers:emqx_cluster(Specs, [
        {env, Env},
        {apps, [emqx_conf, emqx_management]},
        {load_schema, false},
        {env_handler, fun
            (emqx) ->
                application:set_env(emqx, boot_modules, []),
                ok;
            (_) ->
                ok
        end}
    ]).
