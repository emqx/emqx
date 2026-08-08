%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_maptabs_cluster_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-define(TABLE, <<"can_signals">>).

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    try
        Package = emqx_maptabs_SUITE:plugin_package(),
        {ok, PackageBin} = file:read_file(Package),
        NameVsn = filename:basename(Package, ".tar.gz"),
        [
            {plugin_name_vsn, NameVsn},
            {plugin_package_bin, PackageBin}
            | Config
        ]
    catch
        error:{plugin_package_build_failed, _Package, Output} ->
            ct:log("plugin_package build failed: ~s", [Output]),
            {skip, "Run 'make compile-emqx-enterprise' first to build plugin dependencies."}
    end.

end_per_suite(_Config) ->
    ok.

init_per_testcase(TestCase, Config) ->
    WorkDir = emqx_cth_suite:work_dir(TestCase, Config),
    NodeSpecs = emqx_cth_cluster:mk_nodespecs(cluster_specs(), #{work_dir => WorkDir}),
    Nodes = emqx_cth_cluster:start(NodeSpecs),
    ok = install_and_start_plugin_on_nodes(Nodes, Config),
    [{nodes, Nodes}, {node_specs, NodeSpecs} | Config].

end_per_testcase(_TestCase, Config) ->
    Nodes = ?config(nodes, Config),
    ok = cleanup_plugin_on_nodes(Nodes, Config),
    ok = emqx_cth_cluster:stop(Nodes).

%%--------------------------------------------------------------------
%% Test cases
%%--------------------------------------------------------------------

t_load_replicates_to_all_nodes(Config) ->
    [Node1, Node2] = ?config(nodes, Config),
    ok = load_table_on_node(Node1, ?TABLE, [#{key => 1, v => <<"v1">>}]),
    lists:foreach(
        fun(Node) ->
            ?assertEqual(
                #{<<"v">> => <<"v1">>},
                erpc:call(Node, emqx_maptabs, lookup, [?TABLE, 1])
            ),
            Path = table_file_path_on_node(Node),
            ?assert(erpc:call(Node, filelib, is_regular, [Path]))
        end,
        [Node1, Node2]
    ),
    %% both nodes report the same version
    [#{version := Vsn1}] = erpc:call(Node1, emqx_maptabs, list_local, []),
    [#{version := Vsn2}] = erpc:call(Node2, emqx_maptabs, list_local, []),
    ?assertEqual(Vsn1, Vsn2),
    ok.

t_delete_replicates_to_all_nodes(Config) ->
    [Node1, Node2] = ?config(nodes, Config),
    ok = load_table_on_node(Node1, ?TABLE, [#{key => 1, v => <<"v1">>}]),
    %% delete initiated from the other node
    ok = erpc:call(Node2, emqx_maptabs, delete, [?TABLE]),
    lists:foreach(
        fun(Node) ->
            ?assertEqual(undefined, erpc:call(Node, emqx_maptabs, lookup, [?TABLE, 1])),
            Path = table_file_path_on_node(Node),
            ?assertNot(erpc:call(Node, filelib, is_regular, [Path]))
        end,
        [Node1, Node2]
    ),
    ok.

t_preflight_blocks_when_plugin_stopped(Config) ->
    [Node1, Node2] = ?config(nodes, Config),
    NameVsn = ?config(plugin_name_vsn, Config),
    ok = erpc:call(Node2, emqx_plugins, ensure_stopped, [NameVsn]),
    ?assertMatch(
        {error, #{reason := preflight_failed, errors := [{Node2, _}]}},
        load_table_on_node(Node1, ?TABLE, [#{key => 1, v => <<"v1">>}])
    ),
    ?assertEqual(undefined, erpc:call(Node1, emqx_maptabs, lookup, [?TABLE, 1])),
    ok = erpc:call(Node2, emqx_plugins, ensure_started, [NameVsn]),
    ok = load_table_on_node(Node1, ?TABLE, [#{key => 1, v => <<"v1">>}]),
    ?assertEqual(
        #{<<"v">> => <<"v1">>},
        erpc:call(Node2, emqx_maptabs, lookup, [?TABLE, 1])
    ),
    ok.

t_down_node_catches_up_on_restart(Config) ->
    [Node1, Node2] = ?config(nodes, Config),
    [_, Node2Spec] = ?config(node_specs, Config),
    ok = load_table_on_node(Node1, ?TABLE, [#{key => 1, v => <<"v1">>}]),
    ok = emqx_cth_cluster:stop([Node2]),
    %% update while the peer is down: cluster_rpc records the transaction
    ok = load_table_on_node(Node1, ?TABLE, [#{key => 1, v => <<"v2">>}]),
    ?assertEqual(#{<<"v">> => <<"v2">>}, erpc:call(Node1, emqx_maptabs, lookup, [?TABLE, 1])),
    %% the restarted node replays the missed transaction; the file is
    %% rewritten during replay and the plugin picks it up on start
    [Node2] = emqx_cth_cluster:restart(Node2Spec),
    ok = erpc:call(Node2, emqx_plugins, ensure_started, [?config(plugin_name_vsn, Config)]),
    ?retry(
        1000,
        10,
        ?assertEqual(
            #{<<"v">> => <<"v2">>},
            erpc:call(Node2, emqx_maptabs, lookup, [?TABLE, 1])
        )
    ),
    ok.

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

cluster_specs() ->
    Apps = [
        emqx_conf,
        emqx,
        emqx_ctl,
        emqx_rule_engine,
        {emqx_plugins, #{config => #{plugins => #{install_dir => "plugins"}}}}
    ],
    [
        {emqx_maptabs_cluster1, #{role => core, apps => Apps}},
        {emqx_maptabs_cluster2, #{role => core, apps => Apps}}
    ].

load_table_on_node(Node, Name, Rows) ->
    Json = emqx_utils_json:encode(Rows),
    Dir = erpc:call(Node, emqx, data_dir, []),
    Path = filename:join([Dir, "uploads", binary_to_list(Name) ++ ".json"]),
    ok = erpc:call(Node, filelib, ensure_dir, [Path]),
    ok = erpc:call(Node, file, write_file, [Path, Json]),
    erpc:call(Node, emqx_maptabs, load_file, [Path]).

table_file_path_on_node(Node) ->
    Dir = erpc:call(Node, emqx_maptabs, tables_dir, []),
    filename:join(Dir, binary_to_list(?TABLE) ++ ".json").

install_and_start_plugin_on_nodes(Nodes, Config) ->
    lists:foreach(
        fun(Node) -> ok = install_and_start_plugin_on_node(Node, Config) end,
        Nodes
    ).

cleanup_plugin_on_nodes(Nodes, Config) ->
    lists:foreach(
        fun(Node) -> _ = cleanup_plugin_on_node(Node, Config) end,
        Nodes
    ).

install_and_start_plugin_on_node(Node, Config) ->
    NameVsn = ?config(plugin_name_vsn, Config),
    PackageBin = ?config(plugin_package_bin, Config),
    Sha256 = binary:encode_hex(crypto:hash(sha256, PackageBin), lowercase),
    ok = erpc:call(Node, emqx_plugins, write_package, [NameVsn, PackageBin]),
    ok = erpc:call(Node, emqx_plugins, allow_installation, [NameVsn, Sha256]),
    ok = erpc:call(Node, emqx_plugins, ensure_installed, [NameVsn, fresh_install]),
    ok = erpc:call(Node, emqx_plugins, ensure_started, [NameVsn]).

cleanup_plugin_on_node(Node, Config) ->
    NameVsn = ?config(plugin_name_vsn, Config),
    try
        _ = erpc:call(Node, emqx_plugins, ensure_stopped, [NameVsn]),
        _ = erpc:call(Node, emqx_plugins, ensure_disabled, [NameVsn]),
        _ = erpc:call(Node, emqx_plugins, ensure_uninstalled, [NameVsn]),
        _ = erpc:call(Node, emqx_plugins, delete_package, [NameVsn]),
        _ = erpc:call(Node, emqx_plugins, forget_allowed_installation, [NameVsn]),
        ok
    catch
        _:_ -> ok
    end.
