%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_conf_app_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

all() ->
    emqx_common_test_helpers:all(?MODULE).

t_copy_conf_override_on_restarts(Config) ->
    ct:timetrap({seconds, 120}),
    Cluster = cluster(
        ?FUNCTION_NAME,
        [cluster_spec(1), cluster_spec(2), cluster_spec(3)],
        Config
    ),

    %% 1. Start all nodes
    Nodes = start_cluster(Cluster),
    try
        assert_config_load_done(Nodes),

        %% 2. Stop each in order.
        stop_cluster(Nodes),

        %% 3. Restart nodes in the same order.  This should not
        %% crash and eventually all nodes should be ready.
        restart_cluster_async(Cluster),

        timer:sleep(15000),

        assert_config_load_done(Nodes),

        ok
    after
        stop_cluster(Nodes)
    end.

t_copy_new_data_dir(Config) ->
    ct:timetrap({seconds, 120}),
    Cluster = cluster(
        ?FUNCTION_NAME,
        [cluster_spec(4), cluster_spec(5), cluster_spec(6)],
        Config
    ),

    %% 1. Start all nodes
    Nodes = start_cluster(Cluster),
    [First | Rest] = sort_highest_uptime(Nodes),
    try
        NodeDataDir = erpc:call(First, emqx, data_dir, []),
        File = NodeDataDir ++ "/configs/cluster.hocon",
        assert_config_load_done(Nodes),
        rpc:call(First, ?MODULE, create_data_dir, [File]),
        {[ok, ok, ok], []} = rpc:multicall(Nodes, application, stop, [emqx_conf]),
        {[ok, ok, ok], []} = rpc:multicall(Nodes, ?MODULE, set_data_dir_env, []),
        ok = rpc:call(First, application, start, [emqx_conf]),
        {[ok, ok], []} = rpc:multicall(Rest, application, start, [emqx_conf]),
        ?retry(200, 10, ok = assert_data_copy_done(Nodes, File))
    after
        stop_cluster(Nodes)
    end.

t_copy_deprecated_data_dir(Config) ->
    ct:timetrap({seconds, 120}),
    Cluster = cluster(
        ?FUNCTION_NAME,
        [cluster_spec(7), cluster_spec(8), cluster_spec(9)],
        Config
    ),

    %% 1. Start all nodes
    Nodes = start_cluster(Cluster),
    [First | Rest] = sort_highest_uptime(Nodes),
    try
        NodeDataDir = erpc:call(First, emqx, data_dir, []),
        File = NodeDataDir ++ "/configs/cluster-override.conf",
        assert_config_load_done(Nodes),
        rpc:call(First, ?MODULE, create_data_dir, [File]),
        {[ok, ok, ok], []} = rpc:multicall(Nodes, application, stop, [emqx_conf]),
        {[ok, ok, ok], []} = rpc:multicall(Nodes, ?MODULE, set_data_dir_env, []),
        ok = rpc:call(First, application, start, [emqx_conf]),
        {[ok, ok], []} = rpc:multicall(Rest, application, start, [emqx_conf]),
        ?retry(200, 10, ok = assert_data_copy_done(Nodes, File))
    after
        stop_cluster(Nodes)
    end.

t_no_copy_from_newer_version_node(Config) ->
    ct:timetrap({seconds, 120}),
    Cluster = cluster(
        ?FUNCTION_NAME,
        [cluster_spec(10), cluster_spec(11), cluster_spec(12)],
        Config
    ),
    OKs = [ok, ok, ok],
    [First | Rest] = Nodes = start_cluster(Cluster),
    try
        File = "/configs/cluster.hocon",
        assert_config_load_done(Nodes),
        rpc:call(First, ?MODULE, create_data_dir, [File]),
        {OKs, []} = rpc:multicall(Nodes, application, stop, [emqx_conf]),
        {OKs, []} = rpc:multicall(Nodes, ?MODULE, set_data_dir_env, []),
        {OKs, []} = rpc:multicall(Nodes, meck, new, [
            emqx_release, [passthrough, no_history, no_link, non_strict]
        ]),
        %% 99.9.9 is always newer than the current version
        {OKs, []} = rpc:multicall(Nodes, meck, expect, [
            emqx_release, version_with_prefix, 0, "e99.9.9"
        ]),
        ok = rpc:call(First, application, start, [emqx_conf]),
        {[ok, ok], []} = rpc:multicall(Rest, application, start, [emqx_conf]),
        ok = assert_no_cluster_conf_copied(Rest, File)
    after
        stop_cluster(Nodes)
    end.
%%------------------------------------------------------------------------------
%% Helper functions
%%------------------------------------------------------------------------------

create_data_dir(File) ->
    NodeDataDir = emqx:data_dir(),
    ok = filelib:ensure_dir(NodeDataDir ++ "/certs/"),
    ok = filelib:ensure_dir(NodeDataDir ++ "/authz/"),
    ok = filelib:ensure_dir(NodeDataDir ++ "/configs/"),
    ok = file:write_file(NodeDataDir ++ "/certs/fake-cert", list_to_binary(NodeDataDir)),
    ok = file:write_file(NodeDataDir ++ "/authz/fake-authz", list_to_binary(NodeDataDir)),
    Telemetry = <<"telemetry.enable = false">>,
    ok = file:write_file(File, Telemetry).

set_data_dir_env() ->
    NodeDataDir = emqx:data_dir(),
    NodeConfigDir = filename:join(NodeDataDir, "configs"),
    %% will create certs and authz dir
    ok = filelib:ensure_path(NodeConfigDir),
    ConfigFile = filename:join(NodeConfigDir, "emqx.conf"),
    ok = append_format(ConfigFile, "node.config_files = [~p]~n", [ConfigFile]),
    ok = append_format(ConfigFile, "node.data_dir = ~p~n", [NodeDataDir]),
    application:set_env(emqx, config_files, [ConfigFile]),
    %% application:set_env(emqx, data_dir, Node),
    %% We set env both cluster.hocon and cluster-override.conf, but only one will be used
    application:set_env(
        emqx,
        cluster_hocon_file,
        filename:join([NodeDataDir, "configs", "cluster.hocon"])
    ),
    application:set_env(
        emqx,
        cluster_override_conf_file,
        filename:join([NodeDataDir, "configs", "cluster-override.conf"])
    ),
    ok.

append_format(Filename, Fmt, Args) ->
    ok = file:write_file(Filename, io_lib:format(Fmt, Args), [append]).

assert_data_copy_done([_First | Rest], File) ->
    FirstDataDir = filename:dirname(filename:dirname(File)),
    {ok, FakeCertFile} = file:read_file(FirstDataDir ++ "/certs/fake-cert"),
    {ok, FakeAuthzFile} = file:read_file(FirstDataDir ++ "/authz/fake-authz"),
    {ok, FakeOverrideFile} = file:read_file(File),
    {ok, ExpectFake} = hocon:binary(FakeOverrideFile),
    lists:foreach(
        fun(Node0) ->
            NodeDataDir = erpc:call(Node0, emqx, data_dir, []),
            ?assertEqual(
                {ok, FakeCertFile},
                file:read_file(NodeDataDir ++ "/certs/fake-cert"),
                #{node => Node0}
            ),
            ?assertEqual(
                {ok, ExpectFake},
                hocon:files([File]),
                #{node => Node0}
            ),
            ?assertEqual(
                {ok, FakeAuthzFile},
                file:read_file(NodeDataDir ++ "/authz/fake-authz"),
                #{node => Node0}
            )
        end,
        Rest
    ).

assert_no_cluster_conf_copied([], _) ->
    ok;
assert_no_cluster_conf_copied([Node | Nodes], File) ->
    NodeStr = atom_to_list(Node),
    ?assertEqual(
        {error, enoent},
        file:read_file(NodeStr ++ File),
        #{node => Node}
    ),
    assert_no_cluster_conf_copied(Nodes, File).

assert_config_load_done(Nodes) ->
    lists:foreach(
        fun(Node) ->
            Done = rpc:call(Node, emqx_app, init_load_done, []),
            ?assert(Done, #{node => Node})
        end,
        Nodes
    ).

stop_cluster(Nodes) ->
    emqx_cth_cluster:stop(Nodes).

start_cluster(Specs) ->
    emqx_cth_cluster:start(Specs).

restart_cluster_async(Specs) ->
    [
        begin
            _Pid = spawn_link(emqx_cth_cluster, restart, [Spec]),
            timer:sleep(1_000)
        end
     || Spec <- Specs
    ].

cluster(TC, Specs, Config) ->
    Apps = [
        {emqx, #{override_env => [{boot_modules, [broker]}]}},
        {emqx_conf, #{}}
    ],
    emqx_cth_cluster:mk_nodespecs(
        [{Name, #{apps => Apps}} || Name <- Specs],
        #{work_dir => emqx_cth_suite:work_dir(TC, Config)}
    ).

cluster_spec(Num) ->
    list_to_atom(atom_to_list(?MODULE) ++ integer_to_list(Num)).

sort_highest_uptime(Nodes) ->
    Ranking = lists:sort([{-get_node_uptime(N), N} || N <- Nodes]),
    element(2, lists:unzip(Ranking)).

get_node_uptime(Node) ->
    {Milliseconds, _} = erpc:call(Node, erlang, statistics, [wall_clock]),
    Milliseconds.
