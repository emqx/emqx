%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_plugins_pinned_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("emqx_plugins/include/emqx_plugins.hrl").

-define(APP_NAME, my_emqx_plugin).
-define(RELEASE_NAME, "my_emqx_plugin").
-define(TEMPLATE_URL, "https://github.com/emqx/emqx-plugin-template/releases/download/").
-define(VSN, "5.9.0-beta.3").
-define(OLD_VSN, "5.1.0").
-define(NEW_VSN, "5.9.0-beta.1").
-define(ALARM(NameVsn), <<"pinned_plugin_unavailable:", NameVsn/binary>>).

-define(ON(NODE, BODY), erpc:call(NODE, fun() -> BODY end)).

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    WorkDir = emqx_cth_suite:work_dir(Config),
    InstallDir = filename:join([WorkDir, "plugins"]),
    Apps = emqx_cth_suite:start(
        [
            emqx_conf,
            emqx_ctl,
            {emqx_plugins, #{config => #{plugins => #{install_dir => InstallDir}}}}
        ],
        #{work_dir => WorkDir}
    ),
    ok = filelib:ensure_path(InstallDir),
    %% The package file stays outside the install dir: a pinned plugin runs from
    %% the extracted directory only.
    #{name_vsn := NameVsn, package := Package} = get_package(?VSN, packages_dir(WorkDir, "suite")),
    [
        {suite_apps, Apps},
        {install_dir, InstallDir},
        {name_vsn, NameVsn},
        {package, Package}
        | Config
    ].

end_per_suite(Config) ->
    ok = emqx_cth_suite:stop(?config(suite_apps, Config)).

init_per_testcase(_TestCase, Config) ->
    ok = extract(?config(package, Config), ?config(install_dir, Config)),
    Config.

end_per_testcase(_TestCase, Config) ->
    NameVsn = ?config(name_vsn, Config),
    _ = application:stop(?APP_NAME),
    set_pinned([]),
    emqx_plugins:put_configured([]),
    _ = emqx_plugins:purge(NameVsn),
    _ = file:delete(emqx_plugins_pinned:override_file_path(NameVsn)),
    emqx_common_test_helpers:call_janitor(),
    ok.

%%--------------------------------------------------------------------
%% Test cases
%%--------------------------------------------------------------------

-doc "The setting accepts a comma-separated string or an array, and rejects bad entries.".
t_setting_parse(_Config) ->
    Convert = fun(V) -> emqx_conf_schema:pinned_plugins_converter(V, #{}) end,
    ?assertEqual(
        [<<"a-1.0">>, <<"b_c-2.0.1">>],
        Convert(<<" a-1.0 , b_c-2.0.1,">>)
    ),
    ?assertEqual([<<"a-1.0">>, <<"b-2">>], Convert([<<" a-1.0 ">>, <<"b-2">>])),
    ?assertEqual([], Convert(<<"">>)),
    Validate = fun emqx_plugins_utils:validate_pinned_plugins/1,
    ?assertEqual(ok, Validate([])),
    ?assertEqual(ok, Validate([<<"a-1.0">>, <<"b-1.0">>])),
    ?assertMatch(
        {error, #{reason := duplicate_plugin_names, names := [<<"a">>]}},
        Validate([<<"a-1.0">>, <<"b-1.0">>, <<"a-2.0">>])
    ),
    lists:foreach(
        fun(Bad) ->
            ?assertMatch({error, #{reason := bad_plugin_name_vsn}}, Validate([Bad]))
        end,
        [<<"a">>, <<"a-">>, <<"1a-1.0">>, <<"../a-1.0">>, <<"a-1.0/b">>]
    ),
    Check = fun(Value) ->
        Raw = #{
            <<"node">> => #{
                <<"cookie">> => <<"cookie">>,
                <<"data_dir">> => <<"data">>,
                <<"pinned_plugins">> => Value
            }
        },
        hocon_tconf:check_plain(emqx_conf_schema, Raw, #{atom_key => true, required => false}, [
            "node"
        ])
    end,
    ?assertMatch(
        #{node := #{pinned_plugins := [<<"a-1.0">>, <<"b-2.0">>]}},
        Check(<<"a-1.0, b-2.0">>)
    ),
    ?assertMatch(
        #{node := #{pinned_plugins := [<<"a-1.0">>, <<"b-2.0">>]}},
        Check([<<"a-1.0">>, <<"b-2.0">>])
    ),
    ?assertThrow(_, Check(<<"a-1.0,a-2.0">>)),
    ok.

-doc "A pinned plugin starts at boot without a `plugins.states` entry and writes no cluster config.".
t_boot_starts_without_states(Config) ->
    NameVsn = ?config(name_vsn, Config),
    set_pinned([NameVsn]),
    ok = boot(),
    ?assert(is_app_running(?APP_NAME)),
    ?assertMatch(
        {ok, #{pinned := true, config_status := enabled, running_status := running}},
        emqx_plugins:describe(NameVsn)
    ),
    ?assertMatch([#{pinned := true}], emqx_plugins:list()),
    ?assertEqual([], emqx:get_config([plugins, states])),
    %% The config comes from the package default, not from the data dir.
    ?assertMatch(
        #{<<"hostname">> := <<"localhost">>, <<"port">> := 3306},
        emqx_plugins:get_config(NameVsn)
    ),
    ?assertNot(filelib:is_regular(emqx_plugins_fs:config_file_path(NameVsn))),
    ok.

-doc "A pinned plugin that is not extracted raises an alarm, is never fetched from peers, and the boot continues.".
t_missing_package_no_peer_fetch(Config) ->
    NameVsn = ?config(name_vsn, Config),
    ok = emqx_plugins:purge(NameVsn),
    ok = meck:new(emqx_plugins_proto_v2, [passthrough, no_history]),
    ok = meck:expect(emqx_plugins_proto_v2, get_tar, fun(_, _, _) -> {badrpc, nodedown} end),
    ok = meck:expect(emqx_plugins_proto_v2, get_config, fun(_, _, _, _, _) ->
        {badrpc, nodedown}
    end),
    on_exit(fun() -> meck:unload(emqx_plugins_proto_v2) end),
    set_pinned([NameVsn]),
    %% A peer is available, so a fetch would be attempted if the pinned path used it.
    ok = meck:new(mria, [passthrough, no_history]),
    try
        ok = meck:expect(mria, running_nodes, fun() -> [node(), 'peer@127.0.0.1'] end),
        ok = boot()
    after
        meck:unload(mria)
    end,
    ?assertEqual(0, meck:num_calls(emqx_plugins_proto_v2, get_tar, '_')),
    ?assertEqual(0, meck:num_calls(emqx_plugins_proto_v2, get_config, '_')),
    ?assertNot(is_app_running(?APP_NAME)),
    ?assert(is_alarm_active(?ALARM(NameVsn))),
    %% The extracted directory is back: the next start succeeds and clears the alarm.
    ok = extract(?config(package, Config), ?config(install_dir, Config)),
    ok = boot(),
    ?assert(is_app_running(?APP_NAME)),
    ?assertNot(is_alarm_active(?ALARM(NameVsn))),
    ok.

-doc "The file `etc/plugins/<name>.hocon` overrides the package default config.".
t_config_override_file(Config) ->
    NameVsn = ?config(name_vsn, Config),
    Override = emqx_plugins_pinned:override_file_path(NameVsn),
    ok = filelib:ensure_dir(Override),
    ok = file:write_file(Override, <<"port = 3307\n">>),
    set_pinned([NameVsn]),
    ok = boot(),
    ?assert(is_app_running(?APP_NAME)),
    ?assertMatch(
        #{<<"hostname">> := <<"localhost">>, <<"port">> := 3307},
        emqx_plugins:get_config(NameVsn)
    ),
    %% A broken override file fails the start and raises the alarm.
    ok = emqx_plugins:stop_pinned(NameVsn),
    ok = file:write_file(Override, <<"port = {\n">>),
    ?assertMatch(
        {error, #{msg := "bad_pinned_plugin_config_file"}},
        emqx_plugins:start_pinned(NameVsn)
    ),
    ok = boot(),
    ?assertNot(is_app_running(?APP_NAME)),
    ?assert(is_alarm_active(?ALARM(NameVsn))),
    ok.

-doc "Lifecycle operations on a pinned name are refused, and calls from peers are skipped.".
t_lifecycle_refusals(Config) ->
    NameVsn = ?config(name_vsn, Config),
    OtherVsn = <<?RELEASE_NAME, "-9.9.9">>,
    set_pinned([NameVsn]),
    ok = boot(),
    Refused = [
        fun() -> emqx_plugins:ensure_installed(NameVsn) end,
        fun() -> emqx_plugins:ensure_installed(NameVsn, ?fresh_install) end,
        fun() -> emqx_plugins:ensure_uninstalled(NameVsn) end,
        fun() -> emqx_plugins:ensure_enabled(NameVsn) end,
        fun() -> emqx_plugins:ensure_enabled(OtherVsn, front, global) end,
        fun() -> emqx_plugins:ensure_disabled(NameVsn) end,
        fun() -> emqx_plugins:purge_other_versions(NameVsn) end,
        fun() -> emqx_plugins:update_config(NameVsn, #{}) end,
        fun() -> emqx_plugins:safe_delete_package(NameVsn) end,
        fun() -> emqx_plugins:start_pinned(OtherVsn) end
    ],
    lists:foreach(
        fun(F) -> ?assertMatch({error, #{msg := "plugin_pinned"}}, F()) end,
        Refused
    ),
    %% Calls that peers make as part of cluster-wide operations are no-ops.
    ?assertEqual(ok, emqx_plugins:ensure_stopped(NameVsn)),
    ?assertEqual(ok, emqx_plugins:ensure_stopped(OtherVsn)),
    ?assertEqual(ok, emqx_plugins:restart(NameVsn)),
    ?assertEqual({ok, running}, emqx_plugins:validate_start(OtherVsn)),
    ?assertEqual(ok, emqx_plugins:ensure_start_package(OtherVsn)),
    ?assertEqual(ok, emqx_plugins:ensure_started(OtherVsn)),
    ?assertEqual(ok, emqx_plugins:install_package(OtherVsn, <<"not a package">>)),
    ?assertEqual(ok, emqx_plugins:allow_installation(OtherVsn)),
    ?assertNot(emqx_plugins:is_allowed_installation(OtherVsn)),
    ?assertMatch({error, #{msg := "plugin_pinned"}}, emqx_plugins:get_tar(NameVsn)),
    ?assertEqual({ok, default}, emqx_plugins:get_config(NameVsn, ?CONFIG_FORMAT_MAP, default)),
    %% The CLI refuses too.
    lists:foreach(
        fun(Cmd) ->
            Output = cli(Cmd),
            ct:pal("~p: ~s", [Cmd, Output]),
            ?assertNotEqual(nomatch, binary:match(Output, <<"plugin_pinned">>))
        end,
        [
            ["allow", binary_to_list(NameVsn)],
            ["install", binary_to_list(NameVsn)],
            ["install", binary_to_list(NameVsn), "--cluster"],
            ["uninstall", binary_to_list(NameVsn)],
            ["enable", binary_to_list(NameVsn)],
            ["disable", binary_to_list(NameVsn)]
        ]
    ),
    ?assert(is_app_running(?APP_NAME)),
    ?assertEqual([], emqx:get_config([plugins, states])),
    ok.

-doc """
The CLI stops and starts a pinned plugin on this node. A stop lasts until the
node starts all plugins again.
""".
t_cli_stop_start(Config) ->
    NameVsn = ?config(name_vsn, Config),
    Name = binary_to_list(NameVsn),
    set_pinned([NameVsn]),
    ok = boot(),
    _ = cli(["stop", Name]),
    ?assertNot(is_app_running(?APP_NAME)),
    %% Still visible and still reported as enabled.
    {ok, Stopped} = emqx_plugins:describe(NameVsn),
    ?assertMatch(#{pinned := true, config_status := enabled}, Stopped),
    ?assertNotEqual(running, maps:get(running_status, Stopped)),
    %% A replicated `plugins.states' change for the same name does not start it.
    emqx_plugins:put_configured([#{name_vsn => NameVsn, enable => false}]),
    emqx_plugins:put_configured([#{name_vsn => NameVsn, enable => true}]),
    ?assertNot(is_app_running(?APP_NAME)),
    _ = cli(["start", Name]),
    ?assert(is_app_running(?APP_NAME)),
    _ = cli(["restart", Name]),
    ?assert(is_app_running(?APP_NAME)),
    %% The next start of all plugins, at boot or after a cluster join, starts it again.
    _ = cli(["stop", Name]),
    ?assertNot(is_app_running(?APP_NAME)),
    ok = emqx_plugins:ensure_started(),
    ?assert(is_app_running(?APP_NAME)),
    ok.

-doc "`plugins.states` entries for a pinned name are ignored without errors, whatever their version.".
t_ignores_states_for_pinned_name(Config) ->
    NameVsn = ?config(name_vsn, Config),
    set_pinned([NameVsn]),
    emqx_plugins:put_configured([
        #{name_vsn => <<?RELEASE_NAME, "-1.0.0">>, enable => true},
        #{name_vsn => NameVsn, enable => false}
    ]),
    ?check_trace(
        begin
            ok = emqx_plugins:ensure_installed(),
            ok = emqx_plugins:log_unconfigured_plugins(),
            ok = emqx_plugins:ensure_started()
        end,
        fun(Trace) ->
            ?assertEqual([], ?of_kind(for_plugins_action_error_occurred, Trace)),
            ?assertMatch([_], ?of_kind(pinned_plugin_started, Trace))
        end
    ),
    ?assert(is_app_running(?APP_NAME)),
    ?assertMatch({ok, #{config_status := enabled}}, emqx_plugins:describe(NameVsn)),
    ok.

-doc """
Node 2 pins another version of the plugin that the cluster config names.
Join, cluster-wide stop and start from node 1, and a restart of node 2 leave
both nodes running their own version, with cluster RPC in sync and the cluster
config unchanged by node 2.
""".
t_cluster_mixed_versions(Config) ->
    WorkDir = emqx_cth_suite:work_dir(?FUNCTION_NAME, Config),
    OldNameVsn = <<?RELEASE_NAME, "-", ?OLD_VSN>>,
    NewNameVsn = <<?RELEASE_NAME, "-", ?NEW_VSN>>,
    %% The install dirs are outside the node work dirs, which must start empty.
    InstallDir1 = packages_dir(WorkDir, "node1"),
    InstallDir2 = packages_dir(WorkDir, "node2"),
    #{name_vsn := OldNameVsn} = get_package(?OLD_VSN, InstallDir1),
    #{name_vsn := NewNameVsn, package := NewPackage} =
        get_package(?NEW_VSN, packages_dir(WorkDir, "pkgs")),
    ok = extract(NewPackage, InstallDir2),
    Apps = fun(InstallDir, Pinned) ->
        [
            emqx,
            {emqx_conf, #{config => #{node => #{pinned_plugins => Pinned}}}},
            emqx_ctl,
            {emqx_plugins, #{
                config => #{
                    plugins => #{
                        install_dir => bin(InstallDir),
                        states => [#{name_vsn => OldNameVsn, enable => true}]
                    }
                }
            }}
        ]
    end,
    [_Spec1, Spec2] =
        Specs = emqx_cth_cluster:mk_nodespecs(
            [
                {pinned_cluster1, #{role => core, apps => Apps(InstallDir1, <<>>)}},
                {pinned_cluster2, #{role => core, apps => Apps(InstallDir2, NewNameVsn)}}
            ],
            #{work_dir => WorkDir}
        ),
    [N1, N2] = Nodes = emqx_cth_cluster:start(Specs),
    on_exit(fun() -> emqx_cth_cluster:stop(Nodes) end),
    lists:foreach(fun(N) -> ok = ?ON(N, emqx_plugins:ensure_started()) end, Nodes),
    States = ?ON(N1, emqx:get_raw_config([plugins, states])),
    AssertHealthy = fun(Label) ->
        ct:pal("checking: ~p", [Label]),
        ?assertEqual([OldNameVsn], ?ON(N1, emqx_plugins:list_active())),
        ?assertEqual([NewNameVsn], ?ON(N2, emqx_plugins:list_active())),
        ?retry(200, 50, assert_cluster_rpc_in_sync(N1, Nodes))
    end,
    AssertHealthy(joined),
    ?assertEqual(States, ?ON(N1, emqx:get_raw_config([plugins, states]))),

    %% The list shows which node pins which version.
    {200, Listed} = ?ON(N1, emqx_mgmt_api_plugins:list_plugins(get, #{})),
    ?assertMatch(
        [
            #{
                rel_vsn := <<?OLD_VSN>>,
                pinned := false,
                running_status := [#{node := N1, pinned := false, status := running}]
            },
            #{
                rel_vsn := <<?NEW_VSN>>,
                pinned := true,
                running_status := [#{node := N2, pinned := true, status := running}]
            }
        ],
        lists:sort(fun(#{rel_vsn := A}, #{rel_vsn := B}) -> A =< B end, Listed)
    ),
    %% A plugin that only node 2 runs is managed on node 2.
    ?assertMatch(
        {409, #{code := 'PLUGIN_PINNED'}},
        ?ON(
            N1,
            emqx_mgmt_api_plugins:update_plugin(
                put, #{bindings => #{name => NewNameVsn, action => stop}}
            )
        )
    ),

    %% Cluster-wide stop and start of the cluster-managed version.
    UpdatePlugin = fun(Action) ->
        ?ON(
            N1,
            emqx_mgmt_api_plugins:update_plugin(
                put, #{bindings => #{name => OldNameVsn, action => Action}}
            )
        )
    end,
    ?assertEqual({204}, UpdatePlugin(stop)),
    ?assertEqual([], ?ON(N1, emqx_plugins:list_active())),
    ?assertEqual([NewNameVsn], ?ON(N2, emqx_plugins:list_active())),
    ?retry(200, 50, assert_cluster_rpc_in_sync(N1, Nodes)),
    %% Node 2 mirrors the `plugins.states' change, so both copies stay equal.
    ?retry(
        200,
        50,
        ?assertEqual(
            ?ON(N1, emqx:get_raw_config([plugins, states])),
            ?ON(N2, emqx:get_raw_config([plugins, states]))
        )
    ),
    ?assertEqual({204}, UpdatePlugin(start)),
    AssertHealthy(started),

    %% A CLI stop on node 2 ends when node 2 restarts.
    ok = ?ON(N2, emqx_plugins:stop_pinned(NewNameVsn)),
    ?assertEqual([], ?ON(N2, emqx_plugins:list_active())),
    [N2] = emqx_cth_cluster:restart(Spec2),
    ok = ?ON(N2, emqx_plugins:ensure_started()),
    AssertHealthy(restarted),
    [#{<<"name_vsn">> := StateNameVsn, <<"enable">> := true}] =
        ?ON(N1, emqx:get_raw_config([plugins, states])),
    ?assertEqual(OldNameVsn, bin(StateNameVsn)),
    ok.

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

extract(Package, InstallDir) ->
    erl_tar:extract(Package, [compressed, {cwd, InstallDir}]).

get_package(Vsn, Dir) ->
    emqx_plugins_test_helpers:get_demo_plugin_package(#{
        release_name => ?RELEASE_NAME,
        git_url => ?TEMPLATE_URL,
        vsn => Vsn,
        tag => Vsn,
        shdir => Dir
    }).

packages_dir(WorkDir, Name) ->
    Dir = filename:join([WorkDir, "packages", Name]),
    ok = filelib:ensure_path(Dir),
    Dir.

set_pinned(NameVsns) ->
    emqx_config:put([node, pinned_plugins], [bin(NV) || NV <- NameVsns]).

%% The plugin part of a node boot: `emqx_plugins_app' installs, and
%% `emqx_machine_boot' starts after all EMQX applications are up.
boot() ->
    ok = emqx_plugins:ensure_installed(),
    ok = emqx_plugins:ensure_started().

is_app_running(App) ->
    lists:keymember(App, 1, application:which_applications()).

is_alarm_active(Name) ->
    lists:any(fun(#{name := N}) -> N =:= Name end, emqx_alarm:get_alarms(activated)).

%% Every node has applied the latest cluster RPC transaction.
assert_cluster_rpc_in_sync(_Node, Nodes) ->
    lists:foreach(
        fun(N) ->
            #{my_id := MyId, latest := Latest} = ?ON(N, emqx_cluster_rpc:get_commit_lag()),
            ?assertEqual(Latest, MyId, N)
        end,
        Nodes
    ).

cli(Args) ->
    Self = self(),
    Ref = make_ref(),
    LogFun = fun(Fmt, FmtArgs) -> Self ! {Ref, iolist_to_binary(io_lib:format(Fmt, FmtArgs))} end,
    _ = cli(Args, LogFun),
    iolist_to_binary(collect(Ref)).

cli(["allow", NameVsn], LogFun) ->
    emqx_plugins_cli_utils:allow_installation(NameVsn, LogFun);
cli(["install", NameVsn], LogFun) ->
    emqx_plugins_cli_utils:ensure_installed(NameVsn, LogFun);
cli(["install", NameVsn, "--cluster"], LogFun) ->
    emqx_plugins_cli_utils:ensure_installed_cluster(NameVsn, LogFun);
cli(["uninstall", NameVsn], LogFun) ->
    emqx_plugins_cli_utils:ensure_uninstalled(NameVsn, LogFun);
cli(["enable", NameVsn], LogFun) ->
    emqx_plugins_cli_utils:ensure_enabled(NameVsn, no_move, LogFun);
cli(["disable", NameVsn], LogFun) ->
    emqx_plugins_cli_utils:ensure_disabled(NameVsn, LogFun);
cli(["start", NameVsn], LogFun) ->
    emqx_plugins_cli_utils:ensure_started(NameVsn, LogFun);
cli(["stop", NameVsn], LogFun) ->
    emqx_plugins_cli_utils:ensure_stopped(NameVsn, LogFun);
cli(["restart", NameVsn], LogFun) ->
    emqx_plugins_cli_utils:restart(NameVsn, LogFun).

collect(Ref) ->
    receive
        {Ref, Line} -> [Line | collect(Ref)]
    after 0 -> []
    end.

on_exit(Fun) ->
    emqx_common_test_helpers:on_exit(Fun).

bin(A) when is_atom(A) -> atom_to_binary(A, utf8);
bin(L) when is_list(L) -> unicode:characters_to_binary(L, utf8);
bin(B) when is_binary(B) -> B.
