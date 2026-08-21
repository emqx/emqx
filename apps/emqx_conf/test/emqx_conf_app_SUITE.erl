%%--------------------------------------------------------------------
%% Copyright (c) 2022-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_conf_app_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("stdlib/include/assert.hrl").

-define(ON(NODE, BODY), erpc:call(NODE, fun() -> BODY end)).

suite() ->
    %% Enables the flaky-test hook to clean up leftover state (e.g. the work
    %% dir) between the retry attempts declared in `flaky_tests/0`.
    [{ct_hooks, [emqx_cth_ct_hook_flaky]}].

all() ->
    emqx_common_test_helpers:all(?MODULE).

flaky_tests() ->
    #{
        t_copy_new_data_dir => 3,
        t_copy_deprecated_data_dir => 3
    }.

t_copy_conf_override_on_restarts(Config) ->
    ct:timetrap({seconds, 120}),
    Cluster = cluster(
        ?FUNCTION_NAME,
        [cluster_spec(1), cluster_spec(2), cluster_spec(3)],
        Config
    ),

    %% 1. Start all nodes
    ct:pal("starting cluster"),
    Nodes = start_cluster(Cluster),
    try
        ct:pal("checking state"),
        assert_config_load_done(Nodes),

        %% 2. Stop each in order.
        ct:pal("stopping cluster"),
        stop_cluster(Nodes),

        %% 3. Restart nodes in the same order.  This should not
        %% crash and eventually all nodes should be ready.
        ct:pal("restarting cluster"),
        restart_cluster_async(Cluster),

        ct:pal("waiting for stabilization"),
        PrevFlag = process_flag(trap_exit, true),
        wait_emqx_conf_started(Nodes, 35_000),
        process_flag(trap_exit, PrevFlag),

        ct:pal("checking state"),
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
        ct:sleep(500),
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
        ct:pal("stopping emqx_conf application on all nodes"),
        {[ok, ok, ok], []} = rpc:multicall(Nodes, application, stop, [emqx_conf]),
        {[ok, ok, ok], []} = rpc:multicall(Nodes, ?MODULE, set_data_dir_env, []),
        ct:pal("starting emqx_conf application on ~p", [First]),
        ok = rpc:call(First, application, start, [emqx_conf]),
        ct:pal("starting emqx_conf application on ~p", [Rest]),
        {[ok, ok], []} = rpc:multicall(Rest, application, start, [emqx_conf]),
        ct:pal("checking state", []),
        ?retry(200, 10, ok = assert_data_copy_done(Nodes, File)),
        ct:pal("state ok", [])
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

%% Checks that a node may join another even if the former has bad/broken symlinks in the
%% data directories that are zipped and synced from it.
t_sync_data_from_node_with_bad_symlinks(Config) ->
    Names = [cluster_spec(13), cluster_spec(14)],
    Nodes = lists:map(fun emqx_cth_cluster:node_name/1, Names),
    [N1Spec, N2Spec] = cluster(?FUNCTION_NAME, Names, Config),
    try
        [N1] = start_cluster([N1Spec]),
        OkFiles = ?ON(N1, begin
            DataDir = emqx:data_dir(),
            AuthzDir = filename:join(DataDir, "authz"),
            CertsDir = filename:join(DataDir, "certs"),
            ok = filelib:ensure_path(AuthzDir),
            ok = filelib:ensure_path(CertsDir),
            BadAuthzSymlink = filename:join(AuthzDir, "badlink"),
            BadCertsSymlink = filename:join(CertsDir, "badlink"),
            ok = file:make_symlink("idontexistandneverwill", BadAuthzSymlink),
            ok = file:make_symlink("idontexistandneverwill", BadCertsSymlink),
            OkAuthzFile = filename:join(AuthzDir, "ok"),
            OkCertsFile = filename:join(CertsDir, "ok"),
            ok = file:write_file(OkAuthzFile, <<"">>),
            ok = file:write_file(OkCertsFile, <<"">>),
            [OkAuthzFile, OkCertsFile]
        end),
        [N2] = start_cluster([N2Spec]),
        %% Should have copied the ok files
        lists:foreach(
            fun(F) ->
                ?ON(N2, ?assertMatch({ok, _}, file:read_file(F)))
            end,
            OkFiles
        ),
        ok
    after
        stop_cluster(Nodes)
    end.

t_init_load_with_changed_max_packet_size_before_listeners_start(Config) ->
    [NodeSpec] = cluster(?FUNCTION_NAME, [cluster_spec(15)], Config),
    [Node] = start_cluster([NodeSpec]),
    try
        ?ON(Node, begin
            MaxPacketSizePath = [mqtt, max_packet_size],
            MaxPacketSize0 = emqx_config:get_zone_conf(default, MaxPacketSizePath),
            ?assertNotEqual(4194304, MaxPacketSize0),

            RawConf0 = emqx_config_handler:get_raw_cluster_override_conf(),
            RawConf1 = emqx_utils_maps:deep_put(
                [<<"mqtt">>, <<"max_packet_size">>],
                RawConf0,
                <<"4MB">>
            ),
            RawConf2 = emqx_utils_maps:deep_put(
                [<<"node">>, <<"cookie">>],
                RawConf1,
                atom_to_binary(erlang:get_cookie())
            ),
            RawConf = emqx_utils_maps:deep_put(
                [<<"node">>, <<"data_dir">>],
                RawConf2,
                unicode:characters_to_binary(emqx:data_dir())
            ),
            ok = emqx_config:save_to_override_conf(
                emqx_config:has_deprecated_file(),
                RawConf,
                #{override_to => cluster}
            ),
            ?assertEqual(MaxPacketSize0, emqx_config:get_zone_conf(default, MaxPacketSizePath)),

            ok = application:stop(emqx),
            ok = application:stop(esockd),
            ok = emqx_config:init_load(emqx_conf:schema_module()),
            ?assertEqual(4194304, emqx_config:get_zone_conf(default, MaxPacketSizePath)),

            {ok, _} = application:ensure_all_started(emqx),
            ?assert(emqx:is_running()),
            ok
        end)
    after
        stop_cluster([Node])
    end.

%% Verify that changes to `base.hocon` configuration values after initial rolling
%% cluster restart take effect afterwards.
%% Essentially:
%% 1. Start a cluster with identical `base.hocon` on each node containing
%%    `mqtt.max_topic_levels = 100`.
%% 2. Perform a runtime update of `alarm.size_limit = 2000`.
%% 3. Do a rolling restart.
%% 4. Update `base.hocon` on each node to `mqtt.max_topic_levels = 200`.
%% 5. Restart a node and observe it runs with `mqtt.max_topic_levels` updated
%%    to 200.
%%
%% Note that most of the runtime operations introducing configuration changes use
%% schema-root-level granularity, so for example a runtime update in (2) targeting
%% `mqtt.max_clientid_len = 256` makes the testcase fail consistently.
t_base_hocon_change_after_rolling_restart(Config) ->
    test_base_hocon_change_after_rolling_restart(
        ?FUNCTION_NAME,
        {[mqtt, max_topic_levels], 100, 200},
        {[alarm, size_limit], 1000, 2000},
        Config
    ).

test_base_hocon_change_after_rolling_restart(
    TCName,
    {ConfPath, V0, V1},
    {RuntimeConfPath, _RV0, RV1},
    Config
) ->
    ct:timetrap({seconds, 120}),
    Apps = [
        %% Start `emqx_conf` first.
        %% Set up few `emqx` application environment variables that are usually set up
        %% through schema-generated app file.
        {emqx_conf, #{
            config => false,
            before_start => fun(_App, _Opts) ->
                {ok, WorkDir} = file:get_cwd(),
                ClusterHoconFile = filename:join([WorkDir, "configs", "cluster.hocon"]),
                ok = application:set_env(emqx, data_dir, WorkDir),
                ok = application:set_env(emqx, config_files, ["etc/emqx.conf"]),
                ok = application:set_env(emqx, cluster_hocon_file, ClusterHoconFile)
            end
        }},
        %% Start `emqx` explicitly.
        %% It owns `emqx_config_handler` needed for cluster config sync.
        {emqx, #{
            config => false,
            before_start => false
        }}
    ],
    Cluster =
        [NodeSpec1, NodeSpec2] = emqx_cth_cluster:mk_nodespecs(
            [
                {base_hocon_change_after_rolling_restart1, #{apps => Apps, work_dir_dirty => true}},
                {base_hocon_change_after_rolling_restart2, #{apps => Apps, work_dir_dirty => true}}
            ],
            #{work_dir => emqx_cth_suite:work_dir(TCName, Config)}
        ),
    %% Put node-specific configuration in `emqx.conf`, similar to what `emqx_cth_cluster` sets up:
    lists:foreach(fun write_node_config/1, Cluster),
    %% Put initial value for `mqtt.max_topic_levels` in `base.hocon`:
    lists:foreach(
        fun(Spec) -> write_base_hocon(Spec, nest_config_value(ConfPath, V0)) end,
        Cluster
    ),
    ok = emqx_common_test_helpers:copy_acl_conf(),
    try
        %% 1. Start the first node.
        [Node1] = start_cluster([NodeSpec1]),
        %% 2. Change unrelated configuration through cluster RPC mechanism:
        ?assertMatch(
            {ok, #{}},
            ?ON(Node1, emqx_conf:update(RuntimeConfPath, RV1, #{override_to => cluster}))
        ),
        %% 3. Start the second node.
        %% Performs an initial configuration sync from an already running cluster member.
        [Node2] = start_cluster([NodeSpec2]),
        %% 4. Verify the second node intantiated its `cluster.hocon` configuration file.
        assert_config_load_done([Node2]),
        NodeClusterHocon = filename:join([maps:get(work_dir, NodeSpec2), "configs", "cluster.hocon"]),
        ?assertEqual(NodeClusterHocon, ?ON(Node2, emqx_config:cluster_hocon_file())),
        ?assert(filelib:is_regular(NodeClusterHocon)),
        ?assertEqual(V0, ?ON(Node1, emqx:get_config(ConfPath))),
        ?assertEqual(V0, ?ON(Node2, emqx:get_config(ConfPath))),
        %% 5. Initiate the rolling restart.
        %% Similarly, the first node performs configuration sync from the second node.
        [Node1] = emqx_cth_cluster:restart(NodeSpec1),
        [Node2] = emqx_cth_cluster:restart(NodeSpec2),
        %% 6. Change `base.hocon` on each node after initial rolling restart.
        ok = write_base_hocon(NodeSpec1, nest_config_value(ConfPath, V1)),
        ok = write_base_hocon(NodeSpec2, nest_config_value(ConfPath, V1)),
        ?assertEqual(V1, read_base_hocon(Node1, ConfPath)),
        ?assertEqual(V1, read_base_hocon(Node2, ConfPath)),
        %% 7. Restart the second node.
        [Node2] = emqx_cth_cluster:restart(NodeSpec2),
        %% 8. Configuration on `base.hocon` level should still take effect.
        ?assertEqual(V1, read_base_hocon(Node2, ConfPath)),
        ?assertEqual(
            V1,
            ?ON(Node2, emqx:get_config(ConfPath)),
            #{reason => base_hocon_change_should_survive_rolling_restart}
        )
    after
        stop_cluster(Cluster)
    end.

nest_config_value([K | ConfPath], V) ->
    #{K => nest_config_value(ConfPath, V)};
nest_config_value([], V) ->
    V.

%%------------------------------------------------------------------------------
%% Helper functions
%%------------------------------------------------------------------------------

write_node_config(
    #{
        name := Node,
        role := Role,
        base_port := BasePort,
        work_dir := WorkDir,
        core_nodes := CoreNodes
    }
) ->
    NodeConfig = #{
        node => #{
            name => Node,
            role => Role,
            cookie => erlang:get_cookie(),
            data_dir => unicode:characters_to_binary(WorkDir)
        },
        cluster => #{
            discovery_strategy => static,
            static => #{seeds => CoreNodes}
        },
        rpc => #{
            protocol => tcp,
            tcp_server_port => BasePort - 1,
            port_discovery => manual
        },
        log => #{file => #{enable => true, level => debug}},
        listeners => #{
            tcp => #{default => #{enable => false}},
            ssl => #{default => #{enable => false}},
            ws => #{default => #{enable => false}},
            wss => #{default => #{enable => false}}
        }
    },
    NodeHocon = hocon_pp:do(NodeConfig, #{}),
    FilePath = filename:join([WorkDir, "etc", "emqx.conf"]),
    ok = filelib:ensure_dir(FilePath),
    ok = file:write_file(FilePath, unicode:characters_to_binary(NodeHocon)).

write_base_hocon(#{work_dir := WorkDir}, Config) ->
    FilePath = filename:join([WorkDir, "etc", "base.hocon"]),
    ok = filelib:ensure_dir(FilePath),
    BaseHocon = hocon_pp:do(Config, #{}),
    file:write_file(FilePath, unicode:characters_to_binary(BaseHocon)).

read_base_hocon(Node, ConfigPath) ->
    ?ON(Node, begin
        File = emqx_config:base_hocon_file(),
        {ok, RawConfig} = hocon:files([File]),
        emqx_utils_maps:deep_get([emqx_utils_conv:bin(X) || X <- ConfigPath], RawConfig)
    end).

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
            %% The cert/authz files are zipped and sent from the first node and
            %% extracted on this node after the local `emqx_conf` app start
            %% completes. Under heavy CI load the assertions can race with the
            %% sync; allow up to 10s for the data to land. The unzip is atomic,
            %% so retrying any one of the files is enough to gate the others.
            ?retry(
                200,
                50,
                ?assertEqual(
                    {ok, FakeCertFile},
                    file:read_file(NodeDataDir ++ "/certs/fake-cert"),
                    #{node => Node0}
                )
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
    ok = emqx_cth_cluster:stop(Nodes).

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
        emqx,
        emqx_conf
    ],
    emqx_cth_cluster:mk_nodespecs(
        [{Name, #{apps => Apps}} || Name <- Specs],
        #{
            work_dir => emqx_cth_suite:work_dir(TC, Config),
            start_opts => #{
                %% Call `init:stop' instead of `erlang:halt/0' for clean mnesia
                %% shutdown and restart
                shutdown => 5_000
            }
        }
    ).

cluster_spec(Num) ->
    list_to_atom(atom_to_list(?MODULE) ++ integer_to_list(Num)).

sort_highest_uptime(Nodes) ->
    Ranking = lists:sort([{-get_node_uptime(N), N} || N <- Nodes]),
    ct:pal("nodes sorted by uptime: ~p", [Ranking]),
    element(2, lists:unzip(Ranking)).

get_node_uptime(Node) ->
    {Milliseconds, _} = erpc:call(Node, erlang, statistics, [wall_clock]),
    Milliseconds.

now_ms() ->
    erlang:system_time(millisecond).

wait_emqx_conf_started([], _RemainingTime) ->
    ok;
wait_emqx_conf_started(Nodes, RemainingTime) when RemainingTime =< 0 ->
    error({timed_out_waiting_for_emqx_conf, #{remaining_nodes => Nodes}});
wait_emqx_conf_started([Node | Nodes], RemainingTime0) ->
    T0 = now_ms(),
    try erpc:call(Node, application, which_applications, [1_000]) of
        StartedApps ->
            case lists:keyfind(emqx_conf, 1, StartedApps) of
                {emqx_conf, _, _} ->
                    T1 = now_ms(),
                    wait_emqx_conf_started(Nodes, RemainingTime0 - (T1 - T0));
                false ->
                    ct:sleep(200),
                    T1 = now_ms(),
                    wait_emqx_conf_started([Node | Nodes], RemainingTime0 - (T1 - T0))
            end
    catch
        exit:{timeout, _} ->
            T1 = now_ms(),
            wait_emqx_conf_started([Node | Nodes], RemainingTime0 - (T1 - T0));
        error:{erpc, noconnection} ->
            ct:sleep(200),
            T1 = now_ms(),
            wait_emqx_conf_started([Node | Nodes], RemainingTime0 - (T1 - T0))
    end.
