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
%% default of plugins.api_endpoint.timeout
-define(GATEWAY_TIMEOUT, 5000).

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
            {skip, "Run 'make emqx-enterprise' first to build plugin dependencies."}
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

-doc "A load on one node replicates to the peer's cache.".
t_load_replicates_to_all_nodes(Config) ->
    [Node1, Node2] = ?config(nodes, Config),
    ok = load_table_on_node(Node1, ?TABLE, [#{key => 1, v => <<"v1">>}]),
    %% the initiating node's cache is up to date synchronously; the
    %% peer's cache follows the replicated record through table events
    ?assertEqual(#{<<"v">> => <<"v1">>}, erpc:call(Node1, emqx_maptabs, lookup, [?TABLE, 1])),
    ?retry(
        200,
        50,
        ?assertEqual(
            #{<<"v">> => <<"v1">>},
            erpc:call(Node2, emqx_maptabs, lookup, [?TABLE, 1])
        )
    ),
    %% both nodes report the same version
    [#{version := Vsn1}] = erpc:call(Node1, emqx_maptabs, list_local, []),
    [#{version := Vsn2}] = erpc:call(Node2, emqx_maptabs, list_local, []),
    ?assertEqual(Vsn1, Vsn2),
    ok.

-doc "A delete initiated on either node removes the table cluster-wide.".
t_delete_replicates_to_all_nodes(Config) ->
    [Node1, Node2] = ?config(nodes, Config),
    ok = load_table_on_node(Node1, ?TABLE, [#{key => 1, v => <<"v1">>}]),
    %% delete initiated from the other node
    ?retry(
        200,
        50,
        ?assertEqual(
            #{<<"v">> => <<"v1">>},
            erpc:call(Node2, emqx_maptabs, lookup, [?TABLE, 1])
        )
    ),
    ok = erpc:call(Node2, emqx_maptabs, delete, [?TABLE]),
    ?assertEqual(undefined, erpc:call(Node2, emqx_maptabs, lookup, [?TABLE, 1])),
    ?retry(
        200,
        50,
        ?assertEqual(undefined, erpc:call(Node1, emqx_maptabs, lookup, [?TABLE, 1]))
    ),
    ok.

-doc "A peer with a stopped plugin catches up when its plugin starts again.".
t_load_while_peer_plugin_stopped(Config) ->
    [Node1, Node2] = ?config(nodes, Config),
    NameVsn = ?config(plugin_name_vsn, Config),
    ok = erpc:call(Node2, emqx_plugins, ensure_stopped, [NameVsn]),
    %% a stopped peer does not block the update: storage replicates
    %% regardless, and the peer's cache is rebuilt when its plugin
    %% starts again
    ok = load_table_on_node(Node1, ?TABLE, [#{key => 1, v => <<"v1">>}]),
    ?assertEqual(#{<<"v">> => <<"v1">>}, erpc:call(Node1, emqx_maptabs, lookup, [?TABLE, 1])),
    ?assertEqual(undefined, erpc:call(Node2, emqx_maptabs, lookup, [?TABLE, 1])),
    ok = erpc:call(Node2, emqx_plugins, ensure_started, [NameVsn]),
    ?retry(
        200,
        50,
        ?assertEqual(
            #{<<"v">> => <<"v1">>},
            erpc:call(Node2, emqx_maptabs, lookup, [?TABLE, 1])
        )
    ),
    ok.

-doc "An update made while a node is down reaches it on restart via mria recovery.".
t_down_node_catches_up_on_restart(Config) ->
    [Node1, Node2] = ?config(nodes, Config),
    [_, Node2Spec] = ?config(node_specs, Config),
    ok = load_table_on_node(Node1, ?TABLE, [#{key => 1, v => <<"v1">>}]),
    ok = emqx_cth_cluster:stop([Node2]),
    %% update while the peer is down: mria recovers the replica on
    %% restart and the plugin rebuilds its cache from it
    ok = load_table_on_node(Node1, ?TABLE, [#{key => 1, v => <<"v2">>}]),
    ?assertEqual(#{<<"v">> => <<"v2">>}, erpc:call(Node1, emqx_maptabs, lookup, [?TABLE, 1])),
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

-doc "A delete made while a node is down is gone from its recovered replica and cache.".
t_down_node_catches_up_missed_delete(Config) ->
    [Node1, Node2] = ?config(nodes, Config),
    [_, Node2Spec] = ?config(node_specs, Config),
    ok = load_table_on_node(Node1, ?TABLE, [#{key => 1, v => <<"v1">>}]),
    ?retry(
        200,
        50,
        ?assertEqual(
            #{<<"v">> => <<"v1">>},
            erpc:call(Node2, emqx_maptabs, lookup, [?TABLE, 1])
        )
    ),
    ok = emqx_cth_cluster:stop([Node2]),
    %% delete while the peer is down: the record is gone from the
    %% recovered replica, so the rebuilt cache follows
    ok = erpc:call(Node1, emqx_maptabs, delete, [?TABLE]),
    [Node2] = emqx_cth_cluster:restart(Node2Spec),
    ok = erpc:call(Node2, emqx_plugins, ensure_started, [?config(plugin_name_vsn, Config)]),
    ?retry(
        1000,
        10,
        ?assertEqual(undefined, erpc:call(Node2, emqx_maptabs, lookup, [?TABLE, 1]))
    ),
    ?retry(
        1000,
        10,
        ?assertEqual([], erpc:call(Node2, emqx_maptabs, list_local, []))
    ),
    ok.

-doc """
The same content uploaded twice through the API: the second upload
succeeds with the same version, and neither node rebuilds its cache.
""".
t_api_put_same_content_twice(Config) ->
    [Node1, Node2] = Nodes = ?config(nodes, Config),
    Rows = [#{key => 1, v => <<"v1">>}],
    {200, #{version := Version, nodes := Confirmed}} = api_put(Node1, ?TABLE, Rows),
    ?assertEqual(lists:sort(Nodes), lists:sort(Confirmed)),
    %% a rebuild swaps a fresh ETS table into the registry
    Tids1 = cached_tids(Nodes),
    ?assertMatch({200, #{version := Version}}, api_put(Node1, ?TABLE, Rows)),
    %% flush any reconcile the second write scheduled
    [ok = erpc:call(N, emqx_maptabs_server, reconcile, [N]) || N <- Nodes],
    ?assertEqual(Tids1, cached_tids(Nodes)),
    ?assertEqual(#{<<"v">> => <<"v1">>}, erpc:call(Node2, emqx_maptabs, lookup, [?TABLE, 1])),
    ok.

-doc """
A peer whose plugin is stopped cannot confirm: the upload is a 503
naming it, the write stays, and repeating the upload after the peer is
back answers 200.
""".
t_api_put_partial_when_peer_plugin_stopped(Config) ->
    [Node1, Node2] = ?config(nodes, Config),
    NameVsn = ?config(plugin_name_vsn, Config),
    ok = erpc:call(Node2, emqx_plugins, ensure_stopped, [NameVsn]),
    Rows = [#{key => 1, v => <<"v1">>}],
    {503, Body} = api_put(Node1, ?TABLE, Rows),
    ?assertMatch(
        #{
            code := <<"PARTIALLY_APPLIED">>,
            nodes := [Node1],
            failed_nodes := [#{node := Node2, reason := plugin_not_running}]
        },
        Body
    ),
    #{version := Version} = Body,
    %% not rolled back: stored on both nodes, served by the survivor
    ?assertMatch(
        [{maptab_index, ?TABLE, Version, _, _}],
        erpc:call(Node2, mnesia, dirty_read, [emqx_maptabs_index, ?TABLE])
    ),
    ?assertEqual(#{<<"v">> => <<"v1">>}, erpc:call(Node1, emqx_maptabs, lookup, [?TABLE, 1])),
    %% heal: the peer is back, the same upload completes
    ok = erpc:call(Node2, emqx_plugins, ensure_started, [NameVsn]),
    ?assertMatch({200, #{version := Version}}, api_put(Node1, ?TABLE, Rows)),
    ?assertEqual(#{<<"v">> => <<"v1">>}, erpc:call(Node2, emqx_maptabs, lookup, [?TABLE, 1])),
    ok.

-doc """
A peer that does not answer is reported as a 503 naming it within the
gateway's call budget, not as the gateway's own timeout.
""".
t_api_put_partial_within_gateway_budget(Config) ->
    [Node1, Node2] = ?config(nodes, Config),
    Server2 = erpc:call(Node2, erlang, whereis, [emqx_maptabs_server]),
    ok = erpc:call(Node2, sys, suspend, [Server2]),
    Rows = [#{key => 1, v => <<"v1">>}],
    try
        {Micros, Result} = timer:tc(fun() -> api_put(Node1, ?TABLE, Rows) end),
        ?assertMatch(
            {503, #{
                code := <<"PARTIALLY_APPLIED">>,
                failed_nodes := [#{node := Node2, reason := timeout}]
            }},
            Result
        ),
        ?assert(Micros div 1000 < ?GATEWAY_TIMEOUT)
    after
        ok = erpc:call(Node2, sys, resume, [Server2])
    end,
    ?assertMatch({200, #{nodes := [_, _]}}, api_put(Node1, ?TABLE, Rows)),
    ok.

-doc """
A delete a peer could not confirm is completed by repeating it; the
repeat answers 204 once every node confirms the table is gone.
""".
t_api_delete_partial_then_repeat(Config) ->
    [Node1, Node2] = ?config(nodes, Config),
    NameVsn = ?config(plugin_name_vsn, Config),
    {200, _} = api_put(Node1, ?TABLE, [#{key => 1, v => <<"v1">>}]),
    ok = erpc:call(Node2, emqx_plugins, ensure_stopped, [NameVsn]),
    ?assertMatch(
        {503, #{failed_nodes := [#{node := Node2, reason := plugin_not_running}]}},
        api_call(Node1, delete, [<<"tables">>, ?TABLE], #{})
    ),
    ?assertEqual([], erpc:call(Node2, mnesia, dirty_read, [emqx_maptabs_index, ?TABLE])),
    ok = erpc:call(Node2, emqx_plugins, ensure_started, [NameVsn]),
    ?assertMatch({204, _}, api_call(Node1, delete, [<<"tables">>, ?TABLE], #{})),
    ?assertEqual(undefined, erpc:call(Node2, emqx_maptabs, lookup, [?TABLE, 1])),
    ok.

-doc "Reload asks every node and names the one that did not answer.".
t_api_reload(Config) ->
    [Node1, Node2] = Nodes = ?config(nodes, Config),
    {200, #{nodes := Confirmed}} = api_call(Node1, post, [<<"reload">>], #{}),
    ?assertEqual(lists:sort(Nodes), lists:sort(Confirmed)),
    NameVsn = ?config(plugin_name_vsn, Config),
    ok = erpc:call(Node2, emqx_plugins, ensure_stopped, [NameVsn]),
    ?assertMatch(
        {503, #{nodes := [Node1], failed_nodes := [#{node := Node2}]}},
        api_call(Node1, post, [<<"reload">>], #{})
    ),
    ok.

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

%% Calls the plugin through the gateway entry point the HTTP handler
%% uses, with its default budget. HTTP body decoding is covered by
%% emqx_maptabs_api_SUITE.
api_put(Node, Name, Rows) ->
    Body = emqx_utils_json:decode(emqx_utils_json:encode(Rows)),
    api_call(Node, put, [<<"tables">>, Name], #{
        headers => #{<<"content-type">> => <<"application/json">>},
        body => Body
    }).

api_call(Node, Method, Path, ReqInfo) ->
    Request = #{
        method => Method,
        path => Path,
        request => maps:merge(#{method => Method, query_string => #{}, headers => #{}}, ReqInfo),
        context => #{}
    },
    %% the response body is the handler's term, before JSON encoding
    Args = [<<"emqx_maptabs">>, Request, ?GATEWAY_TIMEOUT],
    case erpc:call(Node, emqx_plugins, handle_api_call, Args) of
        {Status, _Headers, Resp} -> {Status, Resp};
        {Status, Resp} -> {Status, Resp}
    end.

cached_tids(Nodes) ->
    [
        {N, erpc:call(N, ets, lookup_element, [emqx_maptabs_registry, ?TABLE, 2])}
     || N <- Nodes
    ].

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
