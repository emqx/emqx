%%--------------------------------------------------------------------
%% Copyright (c) 2019-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_plugins_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("emqx_plugins/include/emqx_plugins.hrl").

-define(EMQX_PLUGIN_APP_NAME, my_emqx_plugin).
-define(EMQX_PLUGIN_APP_NAME_BIN, <<"my_emqx_plugin">>).
-define(EMQX_PLUGIN_TEMPLATE_RELEASE_NAME, atom_to_list(?EMQX_PLUGIN_APP_NAME)).
-define(EMQX_PLUGIN_TEMPLATE_URL,
    "https://github.com/emqx/emqx-plugin-template/releases/download/"
).
-define(EMQX_PLUGIN_TEMPLATE_VSN, "5.9.0-beta.3").
-define(EMQX_PLUGIN_TEMPLATE_TAG, "5.9.0-beta.3").

-define(EMQX_PLUGIN_TEMPLATES_LEGACY, [
    #{
        vsn => "5.0.0",
        tag => "5.0.0",
        release_name => "emqx_plugin_template",
        app_name => emqx_plugin_template
    }
]).

-define(EMQX_ELIXIR_PLUGIN_TEMPLATE_RELEASE_NAME, "elixir_plugin_template").
-define(EMQX_ELIXIR_PLUGIN_TEMPLATE_URL,
    "https://github.com/emqx/emqx-elixir-plugin/releases/download/"
).
-define(EMQX_ELIXIR_PLUGIN_TEMPLATE_VSN, "0.1.0").
-define(EMQX_ELIXIR_PLUGIN_TEMPLATE_TAG, "0.1.0-2").
-define(PACKAGE_SUFFIX, ".tar.gz").

-define(ON(NODE, BODY), erpc:call(NODE, fun() -> BODY end)).

all() ->
    [
        {group, copy_plugin},
        {group, create_tar_copy_plugin},
        {group, beam_preflight},
        emqx_common_test_helpers:all(?MODULE)
    ].

groups() ->
    [
        {copy_plugin, [sequence], [
            group_t_copy_plugin_to_a_new_node,
            group_t_copy_plugin_to_a_new_node_single_node,
            group_t_cluster_leave,
            group_t_cluster_force_sync_vsn,
            group_t_cluster_install
        ]},
        {create_tar_copy_plugin, [sequence], [group_t_copy_plugin_to_a_new_node]},
        {beam_preflight, [sequence], [group_t_beam_preflight]}
    ].

init_per_group(copy_plugin, Config) ->
    Config;
init_per_group(create_tar_copy_plugin, Config) ->
    [{remove_tar, true} | Config];
init_per_group(beam_preflight, Config) ->
    Config.

end_per_group(_Group, _Config) ->
    ok.

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
    [{suite_apps, Apps}, {install_dir, InstallDir} | Config].

end_per_suite(Config) ->
    ok = emqx_cth_suite:stop(?config(suite_apps, Config)).

init_per_testcase(TestCase, Config) ->
    emqx_plugins_test_helpers:purge_plugins(),
    ?MODULE:TestCase({init, Config}).

end_per_testcase(TestCase, Config) ->
    emqx_plugins:put_configured([]),
    ?MODULE:TestCase({'end', Config}).

get_demo_plugin_package() ->
    get_demo_plugin_package(emqx_plugins_fs:install_dir()).

get_demo_plugin_package(#{} = Opts) ->
    emqx_plugins_test_helpers:get_demo_plugin_package(Opts);
get_demo_plugin_package(Dir) ->
    get_demo_plugin_package(
        #{
            release_name => ?EMQX_PLUGIN_TEMPLATE_RELEASE_NAME,
            git_url => ?EMQX_PLUGIN_TEMPLATE_URL,
            vsn => ?EMQX_PLUGIN_TEMPLATE_VSN,
            tag => ?EMQX_PLUGIN_TEMPLATE_TAG,
            shdir => Dir
        }
    ).

hookpoints() ->
    [
        'client.connect',
        'client.connack',
        'client.connected',
        'client.disconnected',
        'client.authenticate',
        'client.authorize',
        'client.subscribe',
        'client.unsubscribe',
        'session.created',
        'session.subscribed',
        'session.unsubscribed',
        'session.resumed',
        'session.discarded',
        'session.takenover',
        'session.terminated',
        'message.publish',
        'message.puback',
        'message.delivered',
        'message.acked',
        'message.dropped'
    ].

get_hook_modules() ->
    lists:flatmap(
        fun(HookPoint) ->
            CBs = emqx_hooks:lookup(HookPoint),
            [Mod || {callback, {Mod, _Fn, _Args}, _Filter, _Prio} <- CBs]
        end,
        hookpoints()
    ).

t_demo_install_start_stop_uninstall({init, Config}) ->
    Opts = #{package := Package} = get_demo_plugin_package(),
    NameVsn = filename:basename(Package, ?PACKAGE_SUFFIX),
    [
        {name_vsn, NameVsn},
        {plugin_opts, Opts}
        | Config
    ];
t_demo_install_start_stop_uninstall({'end', _Config}) ->
    ok;
t_demo_install_start_stop_uninstall(Config) ->
    NameVsn = proplists:get_value(name_vsn, Config),
    NameVsnBin = bin(NameVsn),
    #{
        release_name := ReleaseName,
        vsn := PluginVsn
    } = proplists:get_value(plugin_opts, Config),
    ok = emqx_plugins:ensure_installed(NameVsn),
    %% idempotent
    ok = emqx_plugins:ensure_installed(NameVsn),
    ?assert(is_app_loaded(?EMQX_PLUGIN_APP_NAME)),
    ?assert(is_app_loaded(map_sets)),
    {ok, Info} = emqx_plugins:describe(NameVsn),
    ?assertEqual([maps:without([readme], Info)], emqx_plugins:list()),
    %% start
    ok = emqx_plugins:ensure_started(NameVsn),
    ?assert(is_app_running(?EMQX_PLUGIN_APP_NAME)),
    ?assert(is_app_running(map_sets)),
    %% start (idempotent)
    ok = emqx_plugins:ensure_started(NameVsnBin),
    ?assert(is_app_running(?EMQX_PLUGIN_APP_NAME)),
    ?assert(is_app_running(map_sets)),
    ?assertEqual([NameVsnBin], emqx_plugins:list_active()),

    %% running app can not be un-installed
    ?assertMatch(
        {error, _},
        emqx_plugins:ensure_uninstalled(NameVsn)
    ),

    %% stop
    ok = emqx_plugins:ensure_stopped(NameVsn),
    ?assertNot(is_app_running(?EMQX_PLUGIN_APP_NAME)),
    ?assertNot(is_app_running(map_sets)),
    ?assert(is_app_loaded(?EMQX_PLUGIN_APP_NAME)),
    ?assert(is_app_loaded(map_sets)),
    %% stop (idempotent)
    ok = emqx_plugins:ensure_stopped(NameVsnBin),
    ?assertNot(is_app_running(?EMQX_PLUGIN_APP_NAME)),
    ?assertNot(is_app_running(map_sets)),
    ?assert(is_app_loaded(?EMQX_PLUGIN_APP_NAME)),
    ?assert(is_app_loaded(map_sets)),
    %% still listed after stopped
    ReleaseNameBin = list_to_binary(ReleaseName),
    PluginVsnBin = list_to_binary(PluginVsn),
    ?assertMatch(
        [
            #{
                name := ReleaseNameBin,
                rel_vsn := PluginVsnBin
            }
        ],
        emqx_plugins:list()
    ),
    ok = emqx_plugins:ensure_uninstalled(NameVsn),
    ?assertNot(is_app_loaded(?EMQX_PLUGIN_APP_NAME)),
    ?assertNot(is_app_loaded(map_sets)),
    ?assertEqual([], emqx_plugins:list()),
    ?assertEqual([], emqx_plugins:list_active()),
    ?assertMatch([<<"[]">>], emqx_plugins_cli_utils:list(fun(_, L) -> L end)),
    ok.

-doc """
`ensure_started/0' runs on the boot path
(tail of `emqx_machine_boot:ensure_apps_started/0').
A broken configured plugin must be collected and logged, not raised,
so it cannot fail the node boot, and the plugins configured after it
must still start.  The call must also survive the cluster-join cycle:
a join runs `stop_apps/0' (`emqx_plugins:ensure_stopped/0') and then
re-runs `ensure_apps_started/0'.
""".
t_boot_start_tolerates_broken_plugin({init, Config}) ->
    #{package := Package} = get_demo_plugin_package(),
    NameVsn = filename:basename(Package, ?PACKAGE_SUFFIX),
    [{name_vsn, NameVsn} | Config];
t_boot_start_tolerates_broken_plugin({'end', Config}) ->
    NameVsn = proplists:get_value(name_vsn, Config),
    _ = emqx_plugins:ensure_stopped(NameVsn),
    ok;
t_boot_start_tolerates_broken_plugin(Config) ->
    NameVsn = proplists:get_value(name_vsn, Config),
    ok = emqx_plugins:ensure_installed(NameVsn),
    Broken = #{name_vsn => <<"missing_plugin-1.0.0">>, enable => true},
    Good = #{name_vsn => bin(NameVsn), enable => true},
    ok = emqx_plugins:put_configured([Broken, Good]),
    ?assertEqual(ok, emqx_plugins:ensure_started()),
    ?assert(is_app_running(?EMQX_PLUGIN_APP_NAME)),
    %% Re-running with plugins already up must be a no-op.
    ?assertEqual(ok, emqx_plugins:ensure_started()),
    ?assert(is_app_running(?EMQX_PLUGIN_APP_NAME)),
    %% A cluster join runs `stop_apps/0' (which calls
    %% `emqx_plugins:ensure_stopped/0') and then re-runs
    %% `ensure_apps_started/0'.  Simulate the plugin-relevant part of that
    %% cycle; calling `stop_apps/0' itself would stop this CT node's apps.
    %% The real join path is covered by `t_start_node_with_plugin_enabled'.
    ok = emqx_plugins:ensure_stopped(),
    ?assertNot(is_app_running(?EMQX_PLUGIN_APP_NAME)),
    ?assertEqual(ok, emqx_plugins:ensure_started()),
    ?assert(is_app_running(?EMQX_PLUGIN_APP_NAME)),
    ok.

%% help function to create a info file.
%% The file is in JSON format when built
%% but since we are using hocon:load to load it
%% ad-hoc test files can be in hocon format
write_info_file(Config, NameVsn, Content) ->
    WorkDir = proplists:get_value(install_dir, Config),
    InfoFile = filename:join([WorkDir, NameVsn, "release.json"]),
    ok = filelib:ensure_dir(InfoFile),
    ok = file:write_file(InfoFile, Content).

t_position({init, Config}) ->
    #{package := Package} = get_demo_plugin_package(),
    NameVsn = filename:basename(Package, ?PACKAGE_SUFFIX),
    [{name_vsn, NameVsn} | Config];
t_position({'end', _Config}) ->
    ok;
t_position(Config) ->
    NameVsn = proplists:get_value(name_vsn, Config),
    ok = emqx_plugins:ensure_installed(NameVsn),
    ok = emqx_plugins:ensure_enabled(NameVsn),
    FakeInfo =
        "name=position, rel_vsn=\"2\", rel_apps=[\"position-9\"],"
        "description=\"desc fake position app\"",
    PosApp2 = <<"position-2">>,
    ok = write_info_file(Config, PosApp2, FakeInfo),
    %% fake a disabled plugin in config
    ok = ensure_state(PosApp2, {before, NameVsn}, false),
    ListFun = fun() ->
        lists:map(
            fun(
                #{name := Name, rel_vsn := Vsn}
            ) ->
                <<Name/binary, "-", Vsn/binary>>
            end,
            emqx_plugins:list()
        )
    end,
    ?assertEqual([PosApp2, list_to_binary(NameVsn)], ListFun()),
    emqx_plugins:ensure_enabled(PosApp2, {behind, NameVsn}),
    ?assertEqual([list_to_binary(NameVsn), PosApp2], ListFun()),

    ok = emqx_plugins:ensure_stopped(),
    ok = emqx_plugins:ensure_disabled(NameVsn),
    ok = emqx_plugins:ensure_disabled(PosApp2),
    ok = emqx_plugins:ensure_uninstalled(NameVsn),
    ok = emqx_plugins:ensure_uninstalled(PosApp2),
    ?assertEqual([], emqx_plugins:list()),
    ok.

t_resolve_active_name_vsn({init, Config}) ->
    Config;
t_resolve_active_name_vsn({'end', _Config}) ->
    ok;
t_resolve_active_name_vsn(_Config) ->
    ?assertEqual(
        {ok, <<"emqx_plugins_fake-1.0.0">>},
        emqx_plugins:resolve_active_name_vsn(<<"emqx_plugins_fake">>, [
            <<"emqx_plugins_fake-1.0.0">>
        ])
    ),
    ?assertEqual(
        {error, not_found},
        emqx_plugins:resolve_active_name_vsn(<<"unknown_plugin">>, [
            <<"emqx_plugins_fake-1.0.0">>
        ])
    ),
    ?assertEqual(
        {ok, <<"emqx_plugins_fake-1.0.0">>},
        emqx_plugins:resolve_active_name_vsn(<<"emqx_plugins_fake-1.0.0">>, [
            <<"emqx_plugins_fake-1.0.0">>
        ])
    ).

t_start_restart_and_stop({init, Config}) ->
    #{package := Package} = get_demo_plugin_package(),
    NameVsn = filename:basename(Package, ?PACKAGE_SUFFIX),
    [{name_vsn, NameVsn} | Config];
t_start_restart_and_stop({'end', _Config}) ->
    ok;
t_start_restart_and_stop(Config) ->
    %% pre-condition
    Hooks0 = get_hook_modules(),
    ?assertNot(lists:member(?EMQX_PLUGIN_APP_NAME, Hooks0), #{hooks => Hooks0}),

    NameVsn = proplists:get_value(name_vsn, Config),
    ok = emqx_plugins:ensure_installed(NameVsn),
    ok = emqx_plugins:ensure_enabled(NameVsn),

    %% Application is not yet started.
    Hooks1 = get_hook_modules(),
    ?assertNot(lists:member(?EMQX_PLUGIN_APP_NAME, Hooks1), #{hooks => Hooks1}),

    FakeInfo =
        "name=bar, rel_vsn=\"2\", rel_apps=[\"bar-9\"],"
        "description=\"desc bar\"",
    Bar2 = <<"bar-2">>,
    ok = write_info_file(Config, Bar2, FakeInfo),
    %% fake a disabled plugin in config
    ok = ensure_state(Bar2, front, false),

    ?assertNot(is_app_running(?EMQX_PLUGIN_APP_NAME)),
    ok = emqx_plugins:ensure_started(),
    ?assert(is_app_running(?EMQX_PLUGIN_APP_NAME)),

    %% Should have called the application start callback, which in turn adds hooks.
    Hooks2 = get_hook_modules(),
    ?assert(lists:member(?EMQX_PLUGIN_APP_NAME, Hooks2), #{hooks => Hooks2}),

    %% fake enable bar-2
    ok = ensure_state(Bar2, rear, true),
    %% should cause an error
    ?check_trace(
        emqx_plugins:ensure_started(),
        fun(Trace) ->
            ?assertMatch(
                [#{function := _, errors := [_ | _]}],
                ?of_kind(for_plugins_action_error_occurred, Trace)
            ),
            ok
        end
    ),
    %% but demo plugin should still be running
    ?assert(is_app_running(?EMQX_PLUGIN_APP_NAME)),

    %% stop all
    ok = emqx_plugins:ensure_stopped(),
    ?assertNot(is_app_running(?EMQX_PLUGIN_APP_NAME)),
    %% `bar-2' is not a usable installation: it declares an application that was
    %% never unpacked, and there is no package to unpack it from.  Starting it
    %% has purged those leftovers, so re-create the fake metadata in order to be
    %% able to disable it again.
    ?assertEqual({error, enoent}, file:read_file_info(emqx_plugins_fs:plugin_dir(Bar2))),
    ok = write_info_file(Config, Bar2, FakeInfo),
    ok = ensure_state(Bar2, rear, false),

    %% wait for plugin application to remove hooks
    timer:sleep(1000),
    %% Should have called the application stop callback, which removes the hooks.
    Hooks3 = get_hook_modules(),
    ?assertNot(lists:member(?EMQX_PLUGIN_APP_NAME, Hooks3), #{hooks => Hooks3}),

    ok = emqx_plugins:restart(NameVsn),
    ?assert(is_app_running(?EMQX_PLUGIN_APP_NAME)),
    %% repeat
    ok = emqx_plugins:restart(NameVsn),
    ?assert(is_app_running(?EMQX_PLUGIN_APP_NAME)),

    ok = emqx_plugins:ensure_stopped(),
    ok = emqx_plugins:ensure_disabled(NameVsn),
    ok = emqx_plugins:ensure_uninstalled(NameVsn),
    ok = emqx_plugins:ensure_uninstalled(Bar2),
    ?assertEqual([], emqx_plugins:list()),
    ok.

t_start_preinstalled_plugin_inits_config_cache({init, Config}) ->
    #{package := Package} = get_demo_plugin_package(),
    NameVsn = filename:basename(Package, ?PACKAGE_SUFFIX),
    [{name_vsn, NameVsn} | Config];
t_start_preinstalled_plugin_inits_config_cache({'end', _Config}) ->
    ok;
t_start_preinstalled_plugin_inits_config_cache(Config) ->
    NameVsn = proplists:get_value(name_vsn, Config),
    ok = emqx_plugins_fs:ensure_installed_from_tar(NameVsn, fun() -> ok end),
    ?assertEqual(plugin_conf_not_found, emqx_plugins:get_config(NameVsn, plugin_conf_not_found)),
    ok = emqx_plugins:ensure_started(NameVsn),
    Config0 = emqx_plugins:get_config(NameVsn, plugin_conf_not_found),
    ?assert(is_map(Config0), #{config => Config0}),
    ok = emqx_plugins:ensure_stopped(NameVsn),
    ok = emqx_plugins:ensure_uninstalled(NameVsn).

t_legacy_plugins({init, Config}) ->
    Config;
t_legacy_plugins({'end', _Config}) ->
    ok;
t_legacy_plugins(Config) ->
    lists:foreach(
        fun(LegacyPlugin) ->
            test_legacy_plugin(LegacyPlugin, Config)
        end,
        ?EMQX_PLUGIN_TEMPLATES_LEGACY
    ).

test_legacy_plugin(#{app_name := AppName} = LegacyPlugin, _Config) ->
    #{package := Package} = get_demo_plugin_package(LegacyPlugin#{
        shdir => emqx_plugins_fs:install_dir(), git_url => ?EMQX_PLUGIN_TEMPLATE_URL
    }),
    NameVsn = filename:basename(Package, ?PACKAGE_SUFFIX),
    ok = emqx_plugins:ensure_installed(NameVsn),
    %% start
    ok = emqx_plugins:ensure_started(NameVsn),
    ?assert(is_app_running(AppName)),
    ?assert(is_app_running(map_sets)),
    %% stop
    ok = emqx_plugins:ensure_stopped(NameVsn),
    ?assertNot(is_app_running(AppName)),
    ?assert(is_app_loaded(map_sets)),
    ok = emqx_plugins:ensure_uninstalled(NameVsn),
    ?assertEqual([], emqx_plugins:list()),
    ok.

t_enable_disable({init, Config}) ->
    #{package := Package} = get_demo_plugin_package(),
    NameVsn = filename:basename(Package, ?PACKAGE_SUFFIX),
    [{name_vsn, NameVsn} | Config];
t_enable_disable({'end', Config}) ->
    ok = emqx_plugins:ensure_uninstalled(proplists:get_value(name_vsn, Config));
t_enable_disable(Config) ->
    NameVsn = proplists:get_value(name_vsn, Config),
    ok = emqx_plugins:ensure_installed(NameVsn),

    ?assertEqual([#{name_vsn => NameVsn, enable => false}], emqx_plugins:configured()),
    ok = emqx_plugins:ensure_enabled(NameVsn),
    ?assertEqual([#{name_vsn => NameVsn, enable => true}], emqx_plugins:configured()),
    ok = emqx_plugins:ensure_disabled(NameVsn),
    ?assertEqual([#{name_vsn => NameVsn, enable => false}], emqx_plugins:configured()),
    ok = emqx_plugins:ensure_enabled(bin(NameVsn)),
    ?assertEqual([#{name_vsn => NameVsn, enable => true}], emqx_plugins:configured()),
    ?assertMatch(
        {error, #{
            msg := "bad_plugin_config_status",
            hint := "disable_the_plugin_first"
        }},
        emqx_plugins:ensure_uninstalled(NameVsn)
    ),
    ok = emqx_plugins:ensure_disabled(bin(NameVsn)),
    ok = emqx_plugins:ensure_uninstalled(NameVsn),
    ?assertMatch({error, _}, emqx_plugins:ensure_enabled(NameVsn)),
    ?assertMatch({error, _}, emqx_plugins:ensure_disabled(NameVsn)),
    ok.

%% The applications of the plugin that are running from its install directory.
%% `emqx_plugins_apps:running_status/1' with a name-vsn compares the release vsn
%% with the application vsn, which differ for the demo package, so check the
%% directory instead.
plugin_is_running(NameVsn) ->
    emqx_plugins_apps:running_apps_from(emqx_plugins_fs:plugin_dir(NameVsn)) =/= [].

is_app_running(Name) ->
    AllApps = application:which_applications(),
    lists:keyfind(Name, 1, AllApps) /= false.

is_app_loaded(Name) ->
    AllApps = application:loaded_applications(),
    lists:keyfind(Name, 1, AllApps) /= false.

assert_started_and_hooks_loaded() ->
    PluginConfig = emqx_plugins:list(),
    ct:pal("plugin config:\n  ~p", [PluginConfig]),
    ?assertMatch([_], PluginConfig),
    ?assert(is_app_running(?EMQX_PLUGIN_APP_NAME)),
    Hooks = get_hook_modules(),
    ?assert(lists:member(?EMQX_PLUGIN_APP_NAME, Hooks), #{hooks => Hooks}),
    ok.

t_bad_tar_gz({init, Config}) ->
    Config;
t_bad_tar_gz({'end', _Config}) ->
    ok;
t_bad_tar_gz(Config) ->
    WorkDir = proplists:get_value(install_dir, Config),
    FakeTarTz = filename:join([WorkDir, "fake-vsn.tar.gz"]),
    ok = file:write_file(FakeTarTz, "a\n"),
    ?assertMatch(
        {error, #{
            msg := "bad_plugin_package",
            reason := eof
        }},
        emqx_plugins:ensure_installed("fake-vsn")
    ),
    %% the plugin tarball can not be found on any nodes
    ?assertMatch(
        {error, #{
            msg := "no_nodes_to_copy_plugin_from",
            reason := plugin_not_found
        }},
        emqx_plugins:ensure_installed("nonexisting")
    ),
    ?assertEqual([], emqx_plugins:list()),
    ok = emqx_plugins:delete_package("fake-vsn"),
    %% idempotent
    ok = emqx_plugins:delete_package("fake-vsn").

%% create with incomplete info file
%% failed install attempts should not leave behind extracted dir
t_bad_tar_gz2({init, Config}) ->
    WorkDir = proplists:get_value(install_dir, Config),
    NameVsn = "foo-0.2",
    %% this an invalid info file content (description missing)
    BadInfo = "name=foo, rel_vsn=\"0.2\", rel_apps=[foo]",
    ok = write_info_file(Config, NameVsn, BadInfo),
    TarGz = filename:join([WorkDir, NameVsn ++ ".tar.gz"]),
    ok = make_tar(WorkDir, NameVsn),
    [{tar_gz, TarGz}, {name_vsn, NameVsn} | Config];
t_bad_tar_gz2({'end', Config}) ->
    NameVsn = ?config(name_vsn, Config),
    ok = emqx_plugins:delete_package(NameVsn),
    ok;
t_bad_tar_gz2(Config) ->
    TarGz = ?config(tar_gz, Config),
    NameVsn = ?config(name_vsn, Config),
    ?assert(filelib:is_regular(TarGz)),
    %% failed to install, it also cleans up the bad content of .tar.gz file
    ?assertMatch({error, _}, emqx_plugins:ensure_installed(NameVsn)),
    ?assertEqual({error, enoent}, file:read_file_info(emqx_plugins_fs:plugin_dir(NameVsn))),
    %% but the tar.gz file is still around
    ?assert(filelib:is_regular(TarGz)),
    ok.

t_rejects_invalid_schema({init, Config}) ->
    NameVsn = "invalid_plugin-1.0.0",
    ok = make_plugin_tar(NameVsn),
    ok = replace_tar_entry(NameVsn, "config_schema.avsc", <<"not an avro schema">>),
    [{name_vsn, NameVsn} | Config];
t_rejects_invalid_schema({'end', Config}) ->
    cleanup_invalid_plugin(?config(name_vsn, Config));
t_rejects_invalid_schema(Config) ->
    assert_invalid_plugin_package(?config(name_vsn, Config)).

t_rejects_invalid_application({init, Config}) ->
    NameVsn = "invalid_plugin-1.0.0",
    ok = make_plugin_tar(NameVsn),
    ok = replace_tar_entry(NameVsn, "invalid_plugin.app", <<"invalid app">>),
    [{name_vsn, NameVsn} | Config];
t_rejects_invalid_application({'end', Config}) ->
    cleanup_invalid_plugin(?config(name_vsn, Config));
t_rejects_invalid_application(Config) ->
    assert_invalid_plugin_package(?config(name_vsn, Config)).

t_rejects_application_version_mismatch({init, Config}) ->
    NameVsn = "invalid_plugin-1.0.0",
    ok = make_plugin_tar(NameVsn),
    ok = replace_tar_entry(
        NameVsn,
        "invalid_plugin.app",
        <<"{application, invalid_plugin, [{vsn, \"2.0.0\"}]}.\n">>
    ),
    [{name_vsn, NameVsn} | Config];
t_rejects_application_version_mismatch({'end', Config}) ->
    cleanup_invalid_plugin(?config(name_vsn, Config));
t_rejects_application_version_mismatch(Config) ->
    assert_invalid_plugin_package(?config(name_vsn, Config)).

t_rejects_invalid_application_version({init, Config}) ->
    NameVsn = "invalid_plugin-1.0.0",
    ok = make_plugin_tar(NameVsn),
    ok = replace_tar_entry(
        NameVsn,
        "invalid_plugin.app",
        <<"{application, invalid_plugin, [{vsn, {invalid}}]}.\n">>
    ),
    [{name_vsn, NameVsn} | Config];
t_rejects_invalid_application_version({'end', Config}) ->
    cleanup_invalid_plugin(?config(name_vsn, Config));
t_rejects_invalid_application_version(Config) ->
    assert_invalid_plugin_package(?config(name_vsn, Config)).

t_rejects_externally_loaded_application({init, Config}) ->
    NameVsn = "invalid_plugin-1.0.0",
    ok = make_plugin_tar(NameVsn),
    [{name_vsn, NameVsn} | Config];
t_rejects_externally_loaded_application({'end', Config}) ->
    cleanup_invalid_plugin(?config(name_vsn, Config));
t_rejects_externally_loaded_application(Config) ->
    ok = application:load({application, invalid_plugin, [{vsn, "0.1.0"}]}),
    assert_invalid_plugin_package(?config(name_vsn, Config)).

t_rejects_invalid_default_config({init, Config}) ->
    NameVsn = "invalid_plugin-1.0.0",
    ok = make_plugin_tar(NameVsn),
    ok = replace_tar_entry(NameVsn, "config.hocon", <<"foo = {">>),
    [{name_vsn, NameVsn} | Config];
t_rejects_invalid_default_config({'end', Config}) ->
    cleanup_invalid_plugin(?config(name_vsn, Config));
t_rejects_invalid_default_config(Config) ->
    assert_invalid_plugin_package(?config(name_vsn, Config)).

t_allows_missing_default_config({init, Config}) ->
    NameVsn = "invalid_plugin-1.0.0",
    ok = make_plugin_tar(NameVsn),
    ok = remove_tar_entry(NameVsn, "config.hocon"),
    [{name_vsn, NameVsn} | Config];
t_allows_missing_default_config({'end', Config}) ->
    cleanup_invalid_plugin(?config(name_vsn, Config));
t_allows_missing_default_config(Config) ->
    NameVsn = ?config(name_vsn, Config),
    ok = emqx_plugins:ensure_installed(NameVsn, ?fresh_install),
    ?assertMatch({ok, #{config_status := disabled}}, emqx_plugins:describe(NameVsn)),
    ok = emqx_plugins:ensure_started(NameVsn),
    ?assert(is_app_running(invalid_plugin)),
    ok = emqx_plugins:ensure_stopped(NameVsn).

%% A plugin declaring emqx_plugins in its applications list must still load
%% and start: the dependency is dropped from the app spec at load time.
%% Waiting for it deadlocks plugin start during node boot.
t_ignores_emqx_plugins_dependency({init, Config}) ->
    NameVsn = "invalid_plugin-1.0.0",
    ok = make_plugin_tar(NameVsn),
    ok = replace_tar_entry(
        NameVsn,
        "invalid_plugin.app",
        <<
            "{application, invalid_plugin, [{vsn, \"0.1.0\"},"
            " {applications, [kernel, stdlib, emqx_plugins]}]}.\n"
        >>
    ),
    [{name_vsn, NameVsn} | Config];
t_ignores_emqx_plugins_dependency({'end', Config}) ->
    _ = emqx_plugins:ensure_stopped(?config(name_vsn, Config)),
    cleanup_invalid_plugin(?config(name_vsn, Config));
t_ignores_emqx_plugins_dependency(Config) ->
    NameVsn = ?config(name_vsn, Config),
    ok = emqx_plugins:ensure_installed(NameVsn, ?fresh_install),
    ok = emqx_plugins:ensure_started(NameVsn),
    ?assert(is_app_running(invalid_plugin)),
    ?assertEqual({ok, [kernel, stdlib]}, application:get_key(invalid_plugin, applications)),
    ok = emqx_plugins:ensure_stopped(NameVsn).

t_rejects_invalid_schema_on_reconfigure({init, Config}) ->
    NameVsn = "invalid_plugin-1.0.0",
    ok = make_plugin_tar(NameVsn),
    ok = emqx_plugins:ensure_installed(NameVsn, ?fresh_install),
    ok = emqx_plugins:ensure_started(NameVsn),
    ok = file:write_file(
        filename:join([filename:dirname(plugin_ebin_dir(NameVsn)), "priv", "config_schema.avsc"]),
        <<"not an avro schema">>
    ),
    [{name_vsn, NameVsn} | Config];
t_rejects_invalid_schema_on_reconfigure({'end', Config}) ->
    _ = emqx_plugins:ensure_stopped(?config(name_vsn, Config)),
    cleanup_invalid_plugin(?config(name_vsn, Config));
t_rejects_invalid_schema_on_reconfigure(Config) ->
    ?assertMatch({error, _}, emqx_plugins:ensure_installed(?config(name_vsn, Config))).

t_rejects_invalid_local_config_on_start({init, Config}) ->
    NameVsn = "invalid_plugin-1.0.0",
    ok = make_plugin_tar(NameVsn),
    ok = emqx_plugins:ensure_installed(NameVsn, ?fresh_install),
    ok = emqx_plugins:ensure_started(NameVsn),
    ok = emqx_plugins:ensure_stopped(NameVsn),
    ok = file:write_file(emqx_plugins_fs:config_file_path(NameVsn), <<"foo = 42\n">>),
    [{name_vsn, NameVsn} | Config];
t_rejects_invalid_local_config_on_start({'end', Config}) ->
    NameVsn = ?config(name_vsn, Config),
    ok = file:delete(emqx_plugins_fs:config_file_path(NameVsn)),
    cleanup_invalid_plugin(NameVsn);
t_rejects_invalid_local_config_on_start(Config) ->
    NameVsn = ?config(name_vsn, Config),
    ?assertMatch({error, _}, emqx_plugins:ensure_installed(NameVsn)),
    LoadedApps = lists:sort(application:loaded_applications()),
    Serdes = lists:sort(ets:tab2list(?PLUGIN_SERDE_TAB)),
    CachedConfig = emqx_plugins:get_config(NameVsn, not_found),
    {ok, ConfigBin} = file:read_file(emqx_plugins_fs:config_file_path(NameVsn)),
    ?assertMatch(
        {error, #{
            msg := "invalid_plugin_config",
            reason := #{
                reason := invalid_type,
                path := <<"foo">>,
                expected := <<"string">>,
                actual := <<"integer">>
            }
        }},
        emqx_plugins:validate_start(NameVsn)
    ),
    ?assertEqual(LoadedApps, lists:sort(application:loaded_applications())),
    ?assertEqual(Serdes, lists:sort(ets:tab2list(?PLUGIN_SERDE_TAB))),
    ?assertEqual(CachedConfig, emqx_plugins:get_config(NameVsn, not_found)),
    ?assertEqual(
        {ok, ConfigBin},
        file:read_file(emqx_plugins_fs:config_file_path(NameVsn))
    ),
    ?assertMatch({error, _}, emqx_plugins:ensure_started(NameVsn)),
    ?assertNot(is_app_running(invalid_plugin)).

t_validate_start_does_not_start({init, Config}) ->
    NameVsn = "invalid_plugin-1.0.0",
    ok = make_plugin_tar(NameVsn),
    ok = emqx_plugins:ensure_installed(NameVsn, ?fresh_install),
    [{name_vsn, NameVsn} | Config];
t_validate_start_does_not_start({'end', Config}) ->
    cleanup_invalid_plugin(?config(name_vsn, Config));
t_validate_start_does_not_start(Config) ->
    NameVsn = ?config(name_vsn, Config),
    LoadedApps = lists:sort(application:loaded_applications()),
    Serdes = lists:sort(ets:tab2list(?PLUGIN_SERDE_TAB)),
    CachedConfig = emqx_plugins:get_config(NameVsn, not_found),
    {ok, ConfigBin} = file:read_file(emqx_plugins_fs:config_file_path(NameVsn)),
    ?assertEqual({ok, not_running}, emqx_plugins:validate_start(NameVsn)),
    ?assertNot(is_app_running(invalid_plugin)),
    ?assertEqual(LoadedApps, lists:sort(application:loaded_applications())),
    ?assertEqual(Serdes, lists:sort(ets:tab2list(?PLUGIN_SERDE_TAB))),
    ?assertEqual(CachedConfig, emqx_plugins:get_config(NameVsn, not_found)),
    ?assertEqual(
        {ok, ConfigBin},
        file:read_file(emqx_plugins_fs:config_file_path(NameVsn))
    ).

t_ensure_start_package_only_materializes_files({init, Config}) ->
    NameVsn = "invalid_plugin-1.0.0",
    ok = make_plugin_tar(NameVsn),
    [{name_vsn, NameVsn} | Config];
t_ensure_start_package_only_materializes_files({'end', Config}) ->
    cleanup_invalid_plugin(?config(name_vsn, Config));
t_ensure_start_package_only_materializes_files(Config) ->
    NameVsn = ?config(name_vsn, Config),
    LoadedApps = lists:sort(application:loaded_applications()),
    Serdes = lists:sort(ets:tab2list(?PLUGIN_SERDE_TAB)),
    CachedConfig = emqx_plugins:get_config(NameVsn, not_found),
    ?assertNot(filelib:is_dir(emqx_plugins_fs:plugin_dir(NameVsn))),
    ok = emqx_plugins:ensure_start_package(NameVsn),
    ?assert(filelib:is_dir(emqx_plugins_fs:plugin_dir(NameVsn))),
    ?assertNot(is_app_running(invalid_plugin)),
    ?assertEqual(LoadedApps, lists:sort(application:loaded_applications())),
    ?assertEqual(Serdes, lists:sort(ets:tab2list(?PLUGIN_SERDE_TAB))),
    ?assertEqual(CachedConfig, emqx_plugins:get_config(NameVsn, not_found)),
    ?assertEqual({ok, not_running}, emqx_plugins:validate_start(NameVsn)).

t_restart_rejects_invalid_local_config_without_stopping({init, Config}) ->
    NameVsn = "invalid_plugin-1.0.0",
    ok = make_plugin_tar(NameVsn),
    ok = emqx_plugins:ensure_installed(NameVsn, ?fresh_install),
    ok = emqx_plugins:ensure_started(NameVsn),
    ok = file:write_file(emqx_plugins_fs:config_file_path(NameVsn), <<"foo = 42\n">>),
    [{name_vsn, NameVsn} | Config];
t_restart_rejects_invalid_local_config_without_stopping({'end', Config}) ->
    NameVsn = ?config(name_vsn, Config),
    _ = emqx_plugins:ensure_stopped(NameVsn),
    ok = file:delete(emqx_plugins_fs:config_file_path(NameVsn)),
    cleanup_invalid_plugin(NameVsn);
t_restart_rejects_invalid_local_config_without_stopping(Config) ->
    NameVsn = ?config(name_vsn, Config),
    ?assertMatch(
        {error, #{msg := "invalid_plugin_config", reason := #{reason := invalid_type}}},
        emqx_plugins:restart(NameVsn)
    ),
    ?assert(is_app_running(invalid_plugin)).

t_start_revalidates_after_validation({init, Config}) ->
    NameVsn = "invalid_plugin-1.0.0",
    ok = make_plugin_tar(NameVsn),
    ok = emqx_plugins:ensure_installed(NameVsn, ?fresh_install),
    ok = file:write_file(emqx_plugins_fs:config_file_path(NameVsn), <<"foo = \"prepared\"\n">>),
    [{name_vsn, NameVsn} | Config];
t_start_revalidates_after_validation({'end', Config}) ->
    NameVsn = ?config(name_vsn, Config),
    _ = emqx_plugins:ensure_stopped(NameVsn),
    ok = file:delete(emqx_plugins_fs:config_file_path(NameVsn)),
    cleanup_invalid_plugin(NameVsn);
t_start_revalidates_after_validation(Config) ->
    NameVsn = ?config(name_vsn, Config),
    {ok, not_running} = emqx_plugins:validate_start(NameVsn),
    ok = file:write_file(emqx_plugins_fs:config_file_path(NameVsn), <<"foo = 42\n">>),
    ?assertMatch(
        {error, #{msg := "invalid_plugin_config", reason := #{reason := invalid_type}}},
        emqx_plugins:ensure_started(NameVsn)
    ),
    ?assertNot(is_app_running(invalid_plugin)).

t_start_revalidates_cached_config_after_validation({init, Config}) ->
    NameVsn = "invalid_plugin-1.0.0",
    ok = make_plugin_tar(NameVsn),
    ok = emqx_plugins:ensure_installed(NameVsn, ?fresh_install),
    ValidConfig = emqx_plugins:get_config(NameVsn),
    ok = file:delete(emqx_plugins_fs:config_file_path(NameVsn)),
    [{name_vsn, NameVsn}, {valid_config, ValidConfig} | Config];
t_start_revalidates_cached_config_after_validation({'end', Config}) ->
    cleanup_invalid_plugin(?config(name_vsn, Config));
t_start_revalidates_cached_config_after_validation(Config) ->
    NameVsn = ?config(name_vsn, Config),
    ConfigKey = {emqx_plugins, list_to_binary(NameVsn)},
    {ok, not_running} = emqx_plugins:validate_start(NameVsn),
    persistent_term:put(ConfigKey, #{<<"foo">> => 42}),
    try
        ?assertMatch(
            {error, #{msg := "invalid_plugin_config", reason := #{reason := invalid_type}}},
            emqx_plugins:ensure_started(NameVsn)
        ),
        ?assertNot(is_app_running(invalid_plugin))
    after
        persistent_term:put(ConfigKey, ?config(valid_config, Config))
    end.

t_start_is_noop_when_already_running({init, Config}) ->
    NameVsn = "invalid_plugin-1.0.0",
    ok = make_plugin_tar(NameVsn),
    ok = emqx_plugins:ensure_installed(NameVsn, ?fresh_install),
    ok = emqx_plugins:ensure_started(NameVsn),
    ok = file:write_file(
        emqx_plugins_fs:config_file_path(NameVsn),
        <<"foo = \"from-file\"\n">>
    ),
    [{name_vsn, NameVsn} | Config];
t_start_is_noop_when_already_running({'end', Config}) ->
    NameVsn = ?config(name_vsn, Config),
    _ = emqx_plugins:ensure_stopped(NameVsn),
    ok = file:delete(emqx_plugins_fs:config_file_path(NameVsn)),
    cleanup_invalid_plugin(NameVsn);
t_start_is_noop_when_already_running(Config) ->
    NameVsn = ?config(name_vsn, Config),
    CachedConfig = emqx_plugins:get_config(NameVsn),
    {ok, running} = emqx_plugins:validate_start(NameVsn),
    ok = emqx_plugins:ensure_started(NameVsn),
    ?assert(is_app_running(invalid_plugin)),
    ?assertEqual(CachedConfig, emqx_plugins:get_config(NameVsn)).

t_temporary_serde_reports_structured_type_errors({init, Config}) ->
    Config;
t_temporary_serde_reports_structured_type_errors({'end', _Config}) ->
    ok;
t_temporary_serde_reports_structured_type_errors(_Config) ->
    Name = <<"validation_plugin-1.0.0">>,
    NestedSchema = <<
        "{\"type\":\"record\",\"name\":\"validation_plugin\",\"fields\":["
        "{\"name\":\"items\",\"type\":{\"type\":\"array\",\"items\":"
        "{\"type\":\"record\",\"name\":\"item\",\"fields\":["
        "{\"name\":\"name\",\"type\":\"string\"}]}}}]}"
    >>,
    ?assertMatch(
        {error, #{
            reason := invalid_type,
            path := <<"items.0.name">>,
            expected := <<"string">>,
            actual := <<"integer">>
        }},
        emqx_plugins_serde:decode(
            Name,
            NestedSchema,
            emqx_utils_json:encode(#{<<"items">> => [#{<<"name">> => 42}]})
        )
    ),
    ?assertMatch(
        {error, #{
            reason := invalid_type,
            path := <<"$">>,
            expected := <<"validation_plugin">>,
            actual := <<"integer">>
        }},
        emqx_plugins_serde:decode(Name, NestedSchema, <<"42">>)
    ),
    UnionSchema = <<
        "{\"type\":\"record\",\"name\":\"validation_plugin\",\"fields\":["
        "{\"name\":\"value\",\"type\":[\"null\",\"string\"]}]}"
    >>,
    ?assertMatch(
        {error, #{
            reason := invalid_union_member,
            path := <<"value.integer">>,
            actual := <<"integer">>
        }},
        emqx_plugins_serde:decode(
            Name,
            UnionSchema,
            emqx_utils_json:encode(#{<<"value">> => #{<<"integer">> => 42}})
        )
    ),
    EnumSchema = <<
        "{\"type\":\"record\",\"name\":\"validation_plugin\",\"fields\":["
        "{\"name\":\"mode\",\"type\":{\"type\":\"enum\",\"name\":\"mode_enum\","
        "\"symbols\":[\"on\",\"off\"]}}]}"
    >>,
    ?assertMatch(
        {error, #{
            reason := invalid_type,
            path := <<"mode">>,
            expected := <<"mode_enum">>,
            actual := <<"integer">>
        }},
        emqx_plugins_serde:decode(
            Name,
            EnumSchema,
            emqx_utils_json:encode(#{<<"mode">> => 42})
        )
    ),
    RootFixedSchema = <<
        "{\"type\":\"fixed\",\"name\":\"validation_plugin\",\"size\":4}"
    >>,
    ?assertMatch(
        {error, #{
            reason := invalid_type,
            path := <<"$">>,
            expected := <<"validation_plugin">>,
            actual := <<"integer">>
        }},
        emqx_plugins_serde:decode(Name, RootFixedSchema, <<"42">>)
    ),
    NestedFixedSchema = <<
        "{\"type\":\"record\",\"name\":\"validation_plugin\",\"fields\":["
        "{\"name\":\"token\",\"type\":{\"type\":\"fixed\",\"name\":\"token_fixed\","
        "\"size\":4}}]}"
    >>,
    ?assertMatch(
        {error, #{
            reason := invalid_type,
            path := <<"token">>,
            expected := <<"token_fixed">>,
            actual := <<"integer">>
        }},
        emqx_plugins_serde:decode(
            Name,
            NestedFixedSchema,
            emqx_utils_json:encode(#{<<"token">> => 42})
        )
    ).

assert_invalid_plugin_package(NameVsn) ->
    Result = emqx_plugins:ensure_installed(NameVsn, ?fresh_install),
    ?assertMatch({error, _}, Result),
    ?assertEqual({error, enoent}, file:read_file_info(emqx_plugins_fs:plugin_dir(NameVsn))),
    ?assertNot(lists:member(plugin_ebin_dir(NameVsn), code:get_path())),
    Result.

cleanup_invalid_plugin(NameVsn) ->
    _ = application:unload(invalid_plugin),
    _ = code:del_path(plugin_ebin_dir(NameVsn)),
    ok = emqx_plugins:purge(NameVsn),
    ok = emqx_plugins:delete_package(NameVsn).

plugin_ebin_dir(NameVsn) ->
    filename:join([emqx_plugins_fs:lib_dir(NameVsn), "invalid_plugin-0.1.0", "ebin"]).

make_plugin_tar(NameVsn) ->
    PluginApp = "invalid_plugin-0.1.0",
    PrivDir = filename:join([NameVsn, PluginApp, "priv"]),
    Tar = emqx_plugins_fs:tar_file_path(NameVsn),
    Info = <<
        "{\"name\":\"invalid_plugin\",\"rel_vsn\":\"1.0.0\","
        "\"rel_apps\":[\"invalid_plugin-0.1.0\"],\"description\":\"test\","
        "\"with_config_schema\":true}"
    >>,
    Schema =
        <<"{\"type\":\"record\",\"name\":\"invalid_plugin\",\"fields\":[{\"name\":\"foo\",\"type\":\"string\"}]}">>,
    erl_tar:create(
        Tar,
        [
            {filename:join(NameVsn, "release.json"), Info},
            {
                filename:join([NameVsn, PluginApp, "ebin", "invalid_plugin.app"]),
                <<"{application, invalid_plugin, [{vsn, \"0.1.0\"}]}.\n">>
            },
            {filename:join(PrivDir, "config_schema.avsc"), Schema},
            {filename:join(PrivDir, "config.hocon"), <<"foo = \"bar\"\n">>}
        ],
        [compressed]
    ).

replace_tar_entry(NameVsn, Filename, Content) ->
    Tar = emqx_plugins_fs:tar_file_path(NameVsn),
    {ok, TarContent} = erl_tar:extract(Tar, [compressed, memory]),
    {NewTarContent, true} = lists:mapfoldl(
        fun({Path, _} = Entry, Found) ->
            case filename:basename(Path) of
                Filename -> {{Path, Content}, true};
                _ -> {Entry, Found}
            end
        end,
        false,
        TarContent
    ),
    erl_tar:create(Tar, NewTarContent, [compressed]).

remove_tar_entry(NameVsn, Filename) ->
    Tar = emqx_plugins_fs:tar_file_path(NameVsn),
    {ok, TarContent} = erl_tar:extract(Tar, [compressed, memory]),
    {NewTarContent, [_]} = lists:partition(
        fun({Path, _}) -> filename:basename(Path) =/= Filename end, TarContent
    ),
    erl_tar:create(Tar, NewTarContent, [compressed]).

%% Add an entry (with its full path inside the package) to an existing tarball.
add_tar_entry(NameVsn, EntryName, Content) ->
    Tar = emqx_plugins_fs:tar_file_path(NameVsn),
    {ok, TarContent} = erl_tar:extract(Tar, [compressed, memory]),
    erl_tar:create(Tar, TarContent ++ [{EntryName, Content}], [compressed]).

%%--------------------------------------------------------------------
%% Beam preflight and atomic loading
%%
%% These cases go through the real installation API, and overlap with the
%% `emqx_plugins_apps_tests' unit tests.
%%--------------------------------------------------------------------

group_t_beam_preflight({init, Config}) ->
    Config;
group_t_beam_preflight({'end', _Config}) ->
    ok;
group_t_beam_preflight(_Config) ->
    ok = bp_rejects_corrupt_beam(),
    ok = bp_rejected_package_does_not_run_on_load(),
    ok = bp_rejects_emqx_module_replacement(),
    ok = bp_install_rolls_back_on_application_load_failure(),
    ok = bp_install_failure_handler_still_restores_package(),
    ok = bp_valid_package_still_installs_and_starts(),
    ok = bp_install_new_version_while_old_loaded(),
    ok.

bp_rejects_corrupt_beam() ->
    NameVsn = "invalid_plugin-1.0.0",
    ok = make_plugin_tar(NameVsn),
    Bin = bp_compile(ghost, bp_plain_src(ghost, ok)),
    ok = bp_add_beam(NameVsn, ghost, bp_truncate(Bin)),
    ok = replace_tar_entry(NameVsn, "invalid_plugin.app", bp_app_spec([ghost])),
    Result = assert_invalid_plugin_package(NameVsn),
    {error, #{msg := Msg}} = Result,
    ?assert(lists:member(Msg, ["plugin_beam_truncated", "plugin_beam_not_loadable"])),
    ?assertNot(code:is_loaded(ghost)),
    ok.

%% The regression which motivated the fix: an illegal package must not run any
%% of its code, not even an early module's `-on_load'.
bp_rejected_package_does_not_run_on_load() ->
    NameVsn = "invalid_plugin-1.0.0",
    Marker = {a_onload, on_load_ran},
    persistent_term:erase(Marker),
    ok = make_plugin_tar(NameVsn),
    OnLoad = bp_compile(a_onload, bp_onload_src(a_onload)),
    Bad = bp_compile(z_bad, bp_plain_src(z_bad, ok)),
    ok = bp_add_beam(NameVsn, a_onload, OnLoad),
    ok = bp_add_beam(NameVsn, z_bad, bp_truncate(Bad)),
    ok = replace_tar_entry(NameVsn, "invalid_plugin.app", bp_app_spec([a_onload, z_bad])),
    Path0 = code:get_path(),
    try
        _ = assert_invalid_plugin_package(NameVsn),
        ?assertEqual(undefined, persistent_term:get(Marker, undefined)),
        ?assertNot(code:is_loaded(a_onload)),
        ?assertEqual(Path0, code:get_path())
    after
        persistent_term:erase(Marker)
    end,
    ok.

bp_rejects_emqx_module_replacement() ->
    NameVsn = "invalid_plugin-1.0.0",
    Which0 = code:which(emqx_plugins_apps),
    ok = make_plugin_tar(NameVsn),
    Bin = bp_compile(emqx_plugins_apps, bp_plain_src(emqx_plugins_apps, ok)),
    ok = bp_add_beam(NameVsn, emqx_plugins_apps, Bin),
    ok = replace_tar_entry(
        NameVsn, "invalid_plugin.app", bp_app_spec([emqx_plugins_apps])
    ),
    Result = assert_invalid_plugin_package(NameVsn),
    ?assertMatch(
        {error, #{msg := "plugin_beam_load_conflict", module := emqx_plugins_apps}},
        Result
    ),
    {error, Conflict} = Result,
    %% Under cover, `code:which/1' reports `cover_compiled' instead of the beam
    %% path, so the same protection is reported with a different label.
    ?assert(
        lists:member(
            maps:get(conflict, Conflict),
            [loaded_outside_plugins, cover_compiled_module]
        )
    ),
    ?assertEqual(Which0, code:which(emqx_plugins_apps)),
    ok.

bp_install_rolls_back_on_application_load_failure() ->
    NameVsn = "invalid_plugin-1.0.0",
    Marker = {a_onload, on_load_ran},
    persistent_term:erase(Marker),
    ok = make_plugin_tar(NameVsn),
    OnLoad = bp_compile(a_onload, bp_onload_src(a_onload)),
    ok = bp_add_beam(NameVsn, a_onload, OnLoad),
    ok = replace_tar_entry(
        NameVsn,
        "invalid_plugin.app",
        %% `kernel' is loaded and running: the rollback must survive it.
        bp_app_spec([a_onload], [{included_applications, [kernel, nonexistent_app_xyz]}])
    ),
    Path0 = code:get_path(),
    try
        Result = emqx_plugins:ensure_installed(NameVsn, ?fresh_install),
        ?assertMatch({error, #{msg := "failed_to_load_plugin_app"}}, Result),
        ?assertEqual(undefined, persistent_term:get(Marker, undefined)),
        ?assertNot(code:is_loaded(a_onload)),
        ?assertEqual(Path0, code:get_path()),
        ?assertNot(is_app_loaded(invalid_plugin)),
        ?assertNot(lists:member(plugin_ebin_dir(NameVsn), code:get_path())),
        ?assert(is_app_loaded(kernel)),
        ?assertMatch([_ | _], application:which_applications())
    after
        persistent_term:erase(Marker),
        cleanup_invalid_plugin(NameVsn)
    end,
    ok.

%% The failure handler of the install API snapshots the package which is being
%% replaced and puts it back when the installation fails.
bp_install_failure_handler_still_restores_package() ->
    NameVsn = "invalid_plugin-1.0.0",
    ok = make_plugin_tar(NameVsn),
    {ok, PreviousPackage} = emqx_plugins:backup_package(NameVsn),
    try
        ok = make_plugin_tar(NameVsn),
        Bin = bp_compile(ghost, bp_plain_src(ghost, ok)),
        ok = bp_add_beam(NameVsn, ghost, bp_truncate(Bin)),
        ok = replace_tar_entry(NameVsn, "invalid_plugin.app", bp_app_spec([ghost])),
        Path0 = code:get_path(),
        ?assertMatch({error, _}, emqx_plugins:ensure_installed(NameVsn, ?fresh_install)),
        ok = emqx_plugins:restore_package(NameVsn, PreviousPackage),
        ?assertNot(code:is_loaded(ghost)),
        ?assertEqual(Path0, code:get_path()),
        ?assert(filelib:is_regular(emqx_plugins_fs:tar_file_path(NameVsn)))
    after
        _ = emqx_plugins:purge(NameVsn),
        _ = emqx_plugins:delete_package(NameVsn)
    end,
    ok.

%% A package built by the regular toolchain must still install and start.
bp_valid_package_still_installs_and_starts() ->
    #{package := Package} = get_demo_plugin_package(),
    NameVsn = filename:basename(Package, ?PACKAGE_SUFFIX),
    try
        ok = emqx_plugins:ensure_installed(NameVsn),
        ok = emqx_plugins:ensure_started(NameVsn),
        ?assert(is_app_running(?EMQX_PLUGIN_APP_NAME)),
        ok = emqx_plugins:ensure_stopped(NameVsn)
    after
        _ = emqx_plugins:ensure_stopped(NameVsn),
        _ = emqx_plugins:ensure_uninstalled(NameVsn)
    end,
    ok.

%% Upgrading a plugin leaves the modules of the old version in the code server
%% (`unload' only soft purges), so the preflight must let the new version
%% replace them.
bp_install_new_version_while_old_loaded() ->
    OldNameVsn = "invalid_plugin-1.0.0",
    NewNameVsn = "invalid_plugin-2.0.0",
    ok = make_plugin_tar(OldNameVsn),
    OldBin = bp_compile(invalid_plugin, bp_plain_src(invalid_plugin, v1)),
    ok = bp_add_beam(OldNameVsn, invalid_plugin, OldBin),
    ok = replace_tar_entry(OldNameVsn, "invalid_plugin.app", bp_app_spec([invalid_plugin])),
    ok = make_plugin_tar(NewNameVsn),
    NewBin = bp_compile(invalid_plugin, bp_plain_src(invalid_plugin, v2)),
    ok = bp_add_beam(NewNameVsn, invalid_plugin, NewBin),
    ok = replace_tar_entry(NewNameVsn, "invalid_plugin.app", bp_app_spec([invalid_plugin])),
    ok = replace_tar_entry(NewNameVsn, "release.json", bp_release_json("2.0.0")),
    OldEbin = plugin_ebin_dir(OldNameVsn),
    NewEbin = plugin_ebin_dir(NewNameVsn),
    OldBeam = filename:join(OldEbin, "invalid_plugin.beam"),
    NewBeam = filename:join(NewEbin, "invalid_plugin.beam"),
    try
        ok = emqx_plugins:ensure_installed(OldNameVsn, ?fresh_install),
        ?assertEqual(v1, invalid_plugin:ping()),
        ok = emqx_plugins:ensure_uninstalled(OldNameVsn),
        ?assertNot(is_app_loaded(invalid_plugin)),
        ?assertEqual(OldBeam, code:which(invalid_plugin)),
        ok = emqx_plugins:ensure_installed(NewNameVsn, ?fresh_install),
        ?assertEqual(NewBeam, code:which(invalid_plugin)),
        ?assertEqual(v2, invalid_plugin:ping())
    after
        _ = emqx_plugins:ensure_stopped(NewNameVsn),
        _ = emqx_plugins:ensure_uninstalled(NewNameVsn),
        _ = emqx_plugins:ensure_uninstalled(OldNameVsn),
        _ = emqx_plugins:purge(OldNameVsn),
        _ = emqx_plugins:purge(NewNameVsn),
        _ = emqx_plugins:delete_package(OldNameVsn),
        _ = emqx_plugins:delete_package(NewNameVsn)
    end,
    ok.

bp_app_spec(Modules) ->
    bp_app_spec(Modules, []).

bp_app_spec(Modules, Extra) ->
    iolist_to_binary(
        io_lib:format(
            "~p.~n",
            [{application, invalid_plugin, [{vsn, "0.1.0"}, {modules, Modules} | Extra]}]
        )
    ).

bp_release_json(RelVsn) ->
    emqx_utils_json:encode(#{
        <<"name">> => <<"invalid_plugin">>,
        <<"rel_vsn">> => list_to_binary(RelVsn),
        <<"rel_apps">> => [<<"invalid_plugin-0.1.0">>],
        <<"description">> => <<"test">>,
        <<"with_config_schema">> => true
    }).

bp_beam_entry(NameVsn, Mod) ->
    filename:join([NameVsn, "invalid_plugin-0.1.0", "ebin", atom_to_list(Mod) ++ ".beam"]).

bp_add_beam(NameVsn, Mod, Bin) ->
    add_tar_entry(NameVsn, bp_beam_entry(NameVsn, Mod), Bin).

bp_compile(Mod, Src) ->
    Dir = filename:join(emqx_plugins_fs:install_dir(), "bp_src"),
    ok = filelib:ensure_dir(filename:join(Dir, "dummy")),
    SrcFile = filename:join(Dir, atom_to_list(Mod) ++ ".erl"),
    ok = file:write_file(SrcFile, Src),
    try
        {ok, Forms} = epp:parse_file(SrcFile, [], []),
        {ok, Mod, Bin} = compile:forms(Forms, [binary, return_errors]),
        Bin
    after
        _ = file:delete(SrcFile)
    end.

bp_plain_src(Mod, Value) ->
    lists:flatten(
        io_lib:format("-module(~s).~n-export([ping/0]).~nping() -> ~p.~n", [Mod, Value])
    ).

bp_onload_src(Mod) ->
    lists:flatten(
        io_lib:format(
            "-module(~s).~n-export([ping/0]).~n-on_load(init/0).~n"
            "init() -> persistent_term:put({~s, on_load_ran}, true), ok.~n"
            "ping() -> ok.~n",
            [Mod, Mod]
        )
    ).

bp_truncate(Bin) ->
    binary:part(Bin, 0, byte_size(Bin) div 2).

%% test that we even cleanup content that doesn't match the expected name-vsn
%% pattern
t_tar_vsn_content_mismatch({init, Config}) ->
    WorkDir = proplists:get_value(install_dir, Config),
    NameVsn = "bad_tar-0.2",
    %% this an invalid info file content
    BadInfo = "name=foo, rel_vsn=\"0.2\", rel_apps=[\"foo-0.2\"], description=\"lorem ipsum\"",
    ok = write_info_file(Config, "foo-0.2", BadInfo),
    TarGz = filename:join([WorkDir, "bad_tar-0.2.tar.gz"]),
    ok = make_tar(WorkDir, "foo-0.2", NameVsn),
    file:delete(filename:join([WorkDir, "foo-0.2", "release.json"])),
    [{tar_gz, TarGz}, {name_vsn, NameVsn} | Config];
t_tar_vsn_content_mismatch({'end', Config}) ->
    NameVsn = ?config(name_vsn, Config),
    ok = emqx_plugins:delete_package(NameVsn),
    ok;
t_tar_vsn_content_mismatch(Config) ->
    TarGz = ?config(tar_gz, Config),
    NameVsn = ?config(name_vsn, Config),
    ?assert(filelib:is_regular(TarGz)),
    %% failed to install, it also cleans up content of the bad .tar.gz file even
    %% if in other directory
    ?assertMatch({error, _}, emqx_plugins:ensure_installed(NameVsn)),
    ?assertEqual({error, enoent}, file:read_file_info(emqx_plugins_fs:plugin_dir(NameVsn))),
    ?assertEqual({error, enoent}, file:read_file_info(emqx_plugins_fs:plugin_dir("foo-0.2"))),
    %% the tar.gz file is still around
    ?assert(filelib:is_regular(TarGz)),
    ok.

%% An interrupted or failed installation can leave a plugin directory behind
%% that has no readable `release.json'.  Such leftovers do not count as an
%% installation: installing again must purge them and unpack the package from
%% scratch.
t_install_recovers_from_leftover_dir({init, Config}) ->
    #{package := Package} = get_demo_plugin_package(),
    NameVsn = filename:basename(Package, ?PACKAGE_SUFFIX),
    ok = emqx_plugins:ensure_uninstalled(NameVsn),
    %% simulate an interrupted unpack: the package file is there, and so is the
    %% plugin directory, but the directory has no `release.json'
    ok = erl_tar:extract(Package, [compressed, {cwd, emqx_plugins_fs:install_dir()}]),
    ok = file:delete(emqx_plugins_fs:info_file_path(NameVsn)),
    StaleFile = filename:join(emqx_plugins_fs:plugin_dir(NameVsn), "stale.txt"),
    ok = file:write_file(StaleFile, <<"stale">>),
    [{name_vsn, NameVsn}, {stale_file, StaleFile} | Config];
t_install_recovers_from_leftover_dir({'end', Config}) ->
    NameVsn = ?config(name_vsn, Config),
    _ = emqx_plugins:ensure_uninstalled(NameVsn),
    _ = emqx_plugins:delete_package(NameVsn),
    ok;
t_install_recovers_from_leftover_dir(Config) ->
    NameVsn = ?config(name_vsn, Config),
    StaleFile = ?config(stale_file, Config),
    %% the package file is still around, as in the bug report
    ?assert(filelib:is_regular(emqx_plugins_fs:tar_file_path(NameVsn))),
    ?assertMatch(
        {error, #{msg := "bad_info_file", reason := {enoent, _}}},
        emqx_plugins:describe(NameVsn, #{})
    ),
    ok = emqx_plugins:ensure_installed(NameVsn, ?fresh_install),
    ?assertMatch({ok, #{name := <<"my_emqx_plugin">>}}, emqx_plugins:describe(NameVsn, #{})),
    %% the leftovers of the interrupted installation are gone
    ?assertEqual({error, enoent}, file:read_file_info(StaleFile)),
    ok.

%% An unpack can stop right after `release.json' has been written, so the
%% directory holds a perfectly readable manifest without the applications it
%% declares.  That is not an installation either: the application files must be
%% unpacked from the package.
t_install_recovers_from_incomplete_install({init, Config}) ->
    #{package := Package} = get_demo_plugin_package(),
    NameVsn = filename:basename(Package, ?PACKAGE_SUFFIX),
    ok = emqx_plugins:ensure_uninstalled(NameVsn),
    ok = erl_tar:extract(Package, [compressed, {cwd, emqx_plugins_fs:install_dir()}]),
    %% keep `release.json' only, as if the unpack had been interrupted there
    ok = emqx_plugins_test_helpers:delete_plugin_app_dirs(NameVsn),
    [{name_vsn, NameVsn} | Config];
t_install_recovers_from_incomplete_install({'end', Config}) ->
    NameVsn = ?config(name_vsn, Config),
    _ = emqx_plugins:ensure_uninstalled(NameVsn),
    _ = emqx_plugins:delete_package(NameVsn),
    ok;
t_install_recovers_from_incomplete_install(Config) ->
    NameVsn = ?config(name_vsn, Config),
    %% the metadata is readable, but the applications it declares are not there
    ?assertMatch({ok, _}, emqx_plugins:describe(NameVsn, #{})),
    ?assertEqual([], emqx_plugins_test_helpers:plugin_app_files(NameVsn)),
    ok = emqx_plugins:ensure_installed(NameVsn, ?fresh_install),
    ?assertMatch({ok, #{name := <<"my_emqx_plugin">>}}, emqx_plugins:describe(NameVsn, #{})),
    ?assertMatch([_ | _], emqx_plugins_test_helpers:plugin_app_files(NameVsn)),
    ?assertEqual(installed, emqx_plugins:install_state(NameVsn)),
    ok.

%% A failed metadata read must not make the recovery delete the files of a
%% plugin whose applications are still loaded: the plugin would keep running
%% from replaced or missing code.
t_install_refuses_in_use_plugin({init, Config}) ->
    #{package := Package} = get_demo_plugin_package(),
    NameVsn = filename:basename(Package, ?PACKAGE_SUFFIX),
    ok = emqx_plugins:ensure_uninstalled(NameVsn),
    ok = emqx_plugins:ensure_installed(NameVsn),
    ok = emqx_plugins:ensure_started(NameVsn),
    ?assert(is_app_running(?EMQX_PLUGIN_APP_NAME)),
    %% the metadata of the running plugin stops being readable
    ok = file:write_file(emqx_plugins_fs:info_file_path(NameVsn), <<"not json">>),
    [{name_vsn, NameVsn}, {package, Package} | Config];
t_install_refuses_in_use_plugin({'end', Config}) ->
    NameVsn = ?config(name_vsn, Config),
    %% restore the metadata, otherwise the plugin can not be stopped
    _ = emqx_plugins_test_helpers:restore_info_file_from_package(?config(package, Config), NameVsn),
    _ = emqx_plugins:ensure_stopped(NameVsn),
    _ = emqx_plugins:ensure_uninstalled(NameVsn),
    _ = emqx_plugins:delete_package(NameVsn),
    ok;
t_install_refuses_in_use_plugin(Config) ->
    NameVsn = ?config(name_vsn, Config),
    ?assertMatch({error, _}, emqx_plugins:describe(NameVsn, #{})),
    ?assert(plugin_is_running(NameVsn)),
    AppFiles = emqx_plugins_test_helpers:plugin_app_files(NameVsn),
    ?assertMatch([_ | _], AppFiles),
    ?assertMatch(
        {error, #{msg := "plugin_is_in_use"}},
        emqx_plugins:ensure_installed(NameVsn, ?fresh_install)
    ),
    %% the running plugin has been left alone
    ?assert(plugin_is_running(NameVsn)),
    emqx_plugins_test_helpers:assert_files_exist(AppFiles),
    %% the plugin can still be stopped: its applications are found in the
    %% install directory even though `release.json' is unreadable
    ok = emqx_plugins:ensure_stopped(NameVsn),
    ?assertNot(plugin_is_running(NameVsn)),
    %% and then the installation is replaced by the package
    ok = emqx_plugins:ensure_installed(NameVsn, ?fresh_install),
    ?assertEqual(installed, emqx_plugins:install_state(NameVsn)),
    ok.

t_bad_info_json({init, Config}) ->
    Config;
t_bad_info_json({'end', _}) ->
    ok;
t_bad_info_json(Config) ->
    NameVsn = "test-2",
    ok = write_info_file(Config, NameVsn, "bad-syntax"),
    ?assertMatch(
        {error, #{
            msg := "bad_info_file",
            reason := {parse_error, _}
        }},
        emqx_plugins:describe(NameVsn)
    ),
    ok = write_info_file(Config, NameVsn, "{\"bad\": \"obj\"}"),
    ?assertMatch(
        {error, #{
            msg := "bad_info_file_content",
            mandatory_fields := _
        }},
        emqx_plugins:describe(NameVsn)
    ),
    ?assertEqual([], emqx_plugins:list()),
    emqx_plugins:purge(NameVsn),
    ok.

t_elixir_plugin({init, Config}) ->
    Opts0 =
        #{
            release_name => ?EMQX_ELIXIR_PLUGIN_TEMPLATE_RELEASE_NAME,
            git_url => ?EMQX_ELIXIR_PLUGIN_TEMPLATE_URL,
            vsn => ?EMQX_ELIXIR_PLUGIN_TEMPLATE_VSN,
            tag => ?EMQX_ELIXIR_PLUGIN_TEMPLATE_TAG,
            shdir => emqx_plugins_fs:install_dir()
        },
    Opts = #{package := Package} = get_demo_plugin_package(Opts0),
    NameVsn = filename:basename(Package, ?PACKAGE_SUFFIX),
    [
        {name_vsn, NameVsn},
        {plugin_opts, Opts}
        | Config
    ];
t_elixir_plugin({'end', _Config}) ->
    ok;
t_elixir_plugin(Config) ->
    NameVsn = proplists:get_value(name_vsn, Config),
    #{
        release_name := ReleaseName,
        vsn := PluginVsn
    } = proplists:get_value(plugin_opts, Config),
    ok = emqx_plugins:ensure_installed(NameVsn),
    %% idempotent
    ok = emqx_plugins:ensure_installed(NameVsn),
    {ok, Info} = emqx_plugins:read_plugin_info(NameVsn, #{}),
    ?assertEqual([Info], emqx_plugins:list()),
    %% start
    ok = emqx_plugins:ensure_started(NameVsn),
    ?assert(is_app_running(elixir_plugin_template)),
    ?assert(is_app_running(hallux)),
    %% start (idempotent)
    ok = emqx_plugins:ensure_started(bin(NameVsn)),
    ?assert(is_app_running(elixir_plugin_template)),
    ?assert(is_app_running(hallux)),

    %% call an elixir function
    1 = 'Elixir.ElixirPluginTemplate':ping(),
    3 = 'Elixir.Kernel':'+'(1, 2),

    %% running app can not be un-installed
    ?assertMatch(
        {error, _},
        emqx_plugins:ensure_uninstalled(NameVsn)
    ),

    %% stop
    ok = emqx_plugins:ensure_stopped(NameVsn),
    ?assertNot(is_app_running(elixir_plugin_template)),
    ?assertNot(is_app_running(hallux)),
    %% stop (idempotent)
    ok = emqx_plugins:ensure_stopped(bin(NameVsn)),
    ?assertNot(is_app_running(elixir_plugin_template)),
    ?assertNot(is_app_running(hallux)),
    %% still listed after stopped
    ReleaseNameBin = list_to_binary(ReleaseName),
    PluginVsnBin = list_to_binary(PluginVsn),
    ?assertMatch(
        [
            #{
                name := ReleaseNameBin,
                rel_vsn := PluginVsnBin
            }
        ],
        emqx_plugins:list()
    ),
    ok = emqx_plugins:ensure_uninstalled(NameVsn),
    ?assertEqual([], emqx_plugins:list()),
    ok.

t_load_config_from_cli({init, Config}) ->
    #{package := Package} = get_demo_plugin_package(),
    NameVsn = filename:basename(Package, ?PACKAGE_SUFFIX),
    [{name_vsn, NameVsn} | Config];
t_load_config_from_cli({'end', Config}) ->
    NameVsn = ?config(name_vsn, Config),
    ok = emqx_plugins:ensure_stopped(NameVsn),
    ok = emqx_plugins:ensure_uninstalled(NameVsn),
    ok;
t_load_config_from_cli(Config) when is_list(Config) ->
    NameVsn = ?config(name_vsn, Config),
    ok = emqx_plugins:ensure_installed(NameVsn),
    ?assertEqual([#{name_vsn => NameVsn, enable => false}], emqx_plugins:configured()),
    ok = emqx_plugins:ensure_enabled(NameVsn),
    ok = emqx_plugins:ensure_started(NameVsn),
    Params0 = unused,
    ?assertMatch(
        {200, [#{running_status := [#{status := running}]}]},
        emqx_mgmt_api_plugins:list_plugins(get, Params0)
    ),

    %% Now we disable it via CLI loading
    Conf0 = emqx_config:get([plugins]),
    ?assertMatch(
        #{states := [#{enable := true}]},
        Conf0
    ),
    #{states := [Plugin0]} = Conf0,
    Conf1 = Conf0#{states := [Plugin0#{enable := false}]},
    Filename = filename:join(["/tmp", [?FUNCTION_NAME, ".hocon"]]),
    ok = file:write_file(Filename, hocon_pp:do(#{plugins => Conf1}, #{})),
    ok = emqx_conf_cli:conf(["load", Filename]),

    Conf2 = emqx_config:get([plugins]),
    ?assertMatch(
        #{states := [#{enable := false}]},
        Conf2
    ),
    ?assertMatch(
        {200, [#{running_status := [#{status := stopped}]}]},
        emqx_mgmt_api_plugins:list_plugins(get, Params0)
    ),

    %% Re-enable it via CLI loading
    ok = file:write_file(Filename, hocon_pp:do(#{plugins => Conf0}, #{})),
    ok = emqx_conf_cli:conf(["load", Filename]),

    Conf3 = emqx_config:get([plugins]),
    ?assertMatch(
        #{states := [#{enable := true}]},
        Conf3
    ),
    ?assertMatch(
        {200, [#{running_status := [#{status := running}]}]},
        emqx_mgmt_api_plugins:list_plugins(get, Params0)
    ),

    ok.

group_t_copy_plugin_to_a_new_node({init, Config}) ->
    FromInstallDir = filename:join(emqx_cth_suite:work_dir(?FUNCTION_NAME, Config), from),
    ok = filelib:ensure_path(FromInstallDir),
    ToInstallDir = filename:join(emqx_cth_suite:work_dir(?FUNCTION_NAME, Config), to),
    ok = filelib:ensure_path(ToInstallDir),
    #{package := Package, release_name := PluginName} = get_demo_plugin_package(FromInstallDir),
    Apps = [
        emqx,
        emqx_conf,
        emqx_ctl,
        emqx_plugins
    ],
    [SpecCopyFrom, SpecCopyTo] =
        emqx_cth_cluster:mk_nodespecs(
            [
                {plugins_copy_from, #{role => core, apps => Apps}},
                {plugins_copy_to, #{role => core, apps => Apps}}
            ],
            #{
                work_dir => emqx_cth_suite:work_dir(?FUNCTION_NAME, Config)
            }
        ),
    [CopyFromNode] = emqx_cth_cluster:start([SpecCopyFrom#{join_to => undefined}]),
    ok = rpc:call(CopyFromNode, emqx_plugins, put_config_internal, [install_dir, FromInstallDir]),
    [CopyToNode] = emqx_cth_cluster:start([SpecCopyTo#{join_to => undefined}]),
    ok = rpc:call(CopyToNode, emqx_plugins, put_config_internal, [install_dir, ToInstallDir]),
    NameVsn = filename:basename(Package, ?PACKAGE_SUFFIX),
    ok = rpc:call(CopyFromNode, emqx_plugins, ensure_installed, [NameVsn]),
    ok = rpc:call(CopyFromNode, emqx_plugins, ensure_started, [NameVsn]),
    ok = rpc:call(CopyFromNode, emqx_plugins, ensure_enabled, [NameVsn]),
    case proplists:get_bool(remove_tar, Config) of
        true ->
            %% Test the case when a plugin is installed, but its original tar file is removed
            %% and must be re-created
            ok = file:delete(filename:join(FromInstallDir, NameVsn ++ ?PACKAGE_SUFFIX));
        false ->
            ok
    end,
    [
        {from_install_dir, FromInstallDir},
        {to_install_dir, ToInstallDir},
        {copy_from_node, CopyFromNode},
        {copy_to_node, CopyToNode},
        {name_vsn, NameVsn},
        {plugin_name, PluginName}
        | Config
    ];
group_t_copy_plugin_to_a_new_node({'end', Config}) ->
    CopyFromNode = ?config(copy_from_node, Config),
    CopyToNode = ?config(copy_to_node, Config),
    ok = emqx_cth_cluster:stop([CopyFromNode, CopyToNode]);
group_t_copy_plugin_to_a_new_node(Config) ->
    CopyFromNode = proplists:get_value(copy_from_node, Config),
    CopyToNode = proplists:get_value(copy_to_node, Config),
    CopyToDir = proplists:get_value(to_install_dir, Config),
    CopyFromPluginsState = rpc:call(CopyFromNode, emqx_plugins, get_config_internal, [[states], []]),
    NameVsn = proplists:get_value(name_vsn, Config),
    PluginName = proplists:get_value(plugin_name, Config),
    PluginApp = list_to_atom(PluginName),
    ?assertMatch([#{enable := true, name_vsn := NameVsn}], CopyFromPluginsState),
    ?assert(
        proplists:is_defined(
            PluginApp,
            rpc:call(CopyFromNode, application, which_applications, [])
        )
    ),
    ?assertEqual([], filelib:wildcard(filename:join(CopyToDir, "**"))),
    %% Check that a new node doesn't have this plugin before it joins the cluster
    ?assertEqual([], rpc:call(CopyToNode, emqx_conf, get, [[plugins, states], []])),
    ?assertMatch({error, _}, rpc:call(CopyToNode, emqx_plugins, describe, [NameVsn])),
    ?assertNot(
        proplists:is_defined(
            PluginApp,
            rpc:call(CopyToNode, application, which_applications, [])
        )
    ),
    ok = rpc:call(CopyToNode, ekka, join, [CopyFromNode]),
    %% Mimic cluster-override conf copying
    ok = rpc:call(CopyToNode, emqx_plugins, put_config_internal, [[states], CopyFromPluginsState]),
    %% Plugin copying is triggered upon app restart on a new node.
    %% This is similar to emqx_conf, which copies cluster-override conf upon start,
    %% see: emqx_conf_app:init_conf/0
    ok = rpc:call(CopyToNode, application, stop, [emqx_plugins]),
    {ok, _} = rpc:call(CopyToNode, application, ensure_all_started, [emqx_plugins]),

    %% Plugin config should be synced from `CopyFromNode`
    %% by application `emqx` and `emqx_conf`
    %% FIXME: in test case, we manually do it here
    ok = rpc:call(CopyToNode, emqx_plugins, put_config_internal, [[states], CopyFromPluginsState]),
    ok = rpc:call(CopyToNode, emqx_plugins, ensure_installed, []),
    ok = rpc:call(CopyToNode, emqx_plugins, ensure_started, []),

    ?assertMatch(
        {ok, #{running_status := running, config_status := enabled}},
        rpc:call(CopyToNode, emqx_plugins, describe, [NameVsn])
    ).

%% checks that we can start a cluster with a lone node.
group_t_copy_plugin_to_a_new_node_single_node({init, Config}) ->
    ToInstallDir = emqx_cth_suite:work_dir(?FUNCTION_NAME, Config),
    file:del_dir_r(ToInstallDir),
    ok = filelib:ensure_path(ToInstallDir),
    #{package := Package, release_name := PluginName} = get_demo_plugin_package(ToInstallDir),
    NameVsn = filename:basename(Package, ?PACKAGE_SUFFIX),
    Apps = [
        emqx,
        emqx_conf,
        emqx_ctl,
        {emqx_plugins, #{
            config => #{
                plugins => #{
                    install_dir => ToInstallDir,
                    states => [#{name_vsn => NameVsn, enable => true}]
                }
            }
        }}
    ],
    [CopyToNode] = emqx_cth_cluster:start(
        [{plugins_copy_to, #{role => core, apps => Apps}}],
        #{work_dir => emqx_cth_suite:work_dir(?FUNCTION_NAME, Config)}
    ),
    [
        {to_install_dir, ToInstallDir},
        {copy_to_node, CopyToNode},
        {name_vsn, NameVsn},
        {plugin_name, PluginName}
        | Config
    ];
group_t_copy_plugin_to_a_new_node_single_node({'end', Config}) ->
    CopyToNode = proplists:get_value(copy_to_node, Config),
    ok = emqx_cth_cluster:stop([CopyToNode]);
group_t_copy_plugin_to_a_new_node_single_node(Config) ->
    CopyToNode = ?config(copy_to_node, Config),
    ToInstallDir = ?config(to_install_dir, Config),
    NameVsn = proplists:get_value(name_vsn, Config),
    %% Start the node for the first time. The plugin should start
    %% successfully even if it's not extracted yet.  Simply starting
    %% the node would crash if not working properly.
    ct:pal("~p config:\n  ~p", [
        CopyToNode, erpc:call(CopyToNode, emqx_plugins, get_config_internal, [[], #{}])
    ]),
    ct:pal("~p install_dir:\n  ~p", [
        CopyToNode, erpc:call(CopyToNode, file, list_dir, [ToInstallDir])
    ]),

    %% Plugin config should be synced from `CopyFromNode`
    %% by application `emqx` and `emqx_conf`
    %% FIXME: in test case, we manually do it here
    ok = rpc:call(CopyToNode, emqx_plugins, put_config_internal, [
        [states], [#{enable => true, name_vsn => NameVsn}]
    ]),
    ok = rpc:call(CopyToNode, emqx_plugins, ensure_installed, []),
    ok = rpc:call(CopyToNode, emqx_plugins, ensure_started, []),

    ?assertMatch(
        {ok, #{running_status := running, config_status := enabled}},
        rpc:call(CopyToNode, emqx_plugins, describe, [NameVsn])
    ),
    ok.

group_t_cluster_leave({init, Config}) ->
    Specs = emqx_cth_cluster:mk_nodespecs(
        [
            {group_t_cluster_leave1, #{role => core, apps => [emqx, emqx_conf, emqx_ctl]}},
            {group_t_cluster_leave2, #{role => core, apps => [emqx, emqx_conf, emqx_ctl]}}
        ],
        #{work_dir => emqx_cth_suite:work_dir(?FUNCTION_NAME, Config)}
    ),
    Nodes = emqx_cth_cluster:start(Specs),
    InstallRelDir = "plugins_copy_to",
    InstallDirs = [filename:join(WD, InstallRelDir) || #{work_dir := WD} <- Specs],
    ok = lists:foreach(fun filelib:ensure_path/1, InstallDirs),
    #{package := Package, release_name := PluginName} = get_demo_plugin_package(hd(InstallDirs)),
    NameVsn = filename:basename(Package, ?PACKAGE_SUFFIX),
    [{ok, _}, {ok, _}] = erpc:multicall(Nodes, emqx_cth_suite, start_app, [
        emqx_plugins,
        #{
            config => #{
                plugins => #{
                    install_dir => InstallRelDir,
                    states => [#{name_vsn => NameVsn, enable => true}]
                }
            }
        }
    ]),
    [
        {nodes, Nodes},
        {name_vsn, NameVsn},
        {plugin_name, PluginName}
        | Config
    ];
group_t_cluster_leave({'end', Config}) ->
    Nodes = ?config(nodes, Config),
    ok = emqx_cth_cluster:stop(Nodes);
group_t_cluster_leave(Config) ->
    [N1, N2] = ?config(nodes, Config),
    NameVsn = proplists:get_value(name_vsn, Config),
    ok = erpc:call(N1, emqx_plugins, ensure_installed, [NameVsn]),
    ok = erpc:call(N1, emqx_plugins, ensure_started, [NameVsn]),
    ok = erpc:call(N1, emqx_plugins, ensure_enabled, [NameVsn]),

    ok = erpc:call(N2, emqx_plugins, ensure_installed, [NameVsn]),
    ok = erpc:call(N2, emqx_plugins, ensure_started, [NameVsn]),
    ok = erpc:call(N2, emqx_plugins, ensure_enabled, [NameVsn]),

    Params = unused,
    %% 2 nodes running
    ?assertMatch(
        {200, [#{running_status := [#{status := running}, #{status := running}]}]},
        erpc:call(N1, emqx_mgmt_api_plugins, list_plugins, [get, Params])
    ),
    ?assertMatch(
        {200, [#{running_status := [#{status := running}, #{status := running}]}]},
        erpc:call(N2, emqx_mgmt_api_plugins, list_plugins, [get, Params])
    ),

    %% Now, one node leaves the cluster.
    ok = erpc:call(N2, ekka, leave, []),

    %% Each node will no longer ask the plugin status to the other.
    ?assertMatch(
        {200, [#{running_status := [#{node := N1, status := running}]}]},
        erpc:call(N1, emqx_mgmt_api_plugins, list_plugins, [get, Params])
    ),
    ?assertMatch(
        {200, [#{running_status := [#{node := N2, status := running}]}]},
        erpc:call(N2, emqx_mgmt_api_plugins, list_plugins, [get, Params])
    ),
    ok.

group_t_cluster_force_sync_vsn({init, Config}) ->
    OldInstallDir = filename:join(emqx_cth_suite:work_dir(?FUNCTION_NAME, Config), old),
    ok = filelib:ensure_path(OldInstallDir),
    NewInstallDir = filename:join(emqx_cth_suite:work_dir(?FUNCTION_NAME, Config), new),
    ok = filelib:ensure_path(NewInstallDir),
    #{package := OldPackage, release_name := OldPluginName} =
        get_demo_plugin_package(#{
            release_name => ?EMQX_PLUGIN_TEMPLATE_RELEASE_NAME,
            git_url => ?EMQX_PLUGIN_TEMPLATE_URL,
            vsn => "5.1.0",
            tag => "5.1.0",
            shdir => OldInstallDir
        }),

    #{package := NewPackage, release_name := NewPluginName} =
        get_demo_plugin_package(#{
            release_name => ?EMQX_PLUGIN_TEMPLATE_RELEASE_NAME,
            git_url => ?EMQX_PLUGIN_TEMPLATE_URL,
            vsn => "5.9.0-beta.1",
            tag => "5.9.0-beta.1",
            shdir => NewInstallDir
        }),
    Apps = [
        emqx,
        emqx_conf,
        emqx_ctl,
        emqx_plugins
    ],
    [SpecWithOld, SpecWithNew] =
        emqx_cth_cluster:mk_nodespecs(
            [
                {node_with_old, #{role => core, apps => Apps}},
                {node_with_new, #{role => core, apps => Apps}}
            ],
            #{
                work_dir => emqx_cth_suite:work_dir(?FUNCTION_NAME, Config)
            }
        ),
    %% Start two nodes
    [NodeWithOld] = emqx_cth_cluster:start([SpecWithOld#{join_to => undefined}]),
    ok = rpc:call(NodeWithOld, emqx_plugins, put_config_internal, [install_dir, OldInstallDir]),
    [NodeWithNew] = emqx_cth_cluster:start([SpecWithNew#{join_to => undefined}]),
    ok = rpc:call(NodeWithNew, emqx_plugins, put_config_internal, [install_dir, NewInstallDir]),

    OldNameVsn = filename:basename(OldPackage, ?PACKAGE_SUFFIX),
    NewNameVsn = filename:basename(NewPackage, ?PACKAGE_SUFFIX),
    ?assertEqual(OldPluginName, NewPluginName),

    lists:foreach(
        fun({Node, NameVsn}) ->
            ok = rpc:call(Node, emqx_plugins, ensure_installed, [NameVsn]),
            ok = rpc:call(Node, emqx_plugins, ensure_started, [NameVsn]),
            ok = rpc:call(Node, emqx_plugins, ensure_enabled, [NameVsn])
        end,
        [
            {NodeWithOld, OldNameVsn},
            {NodeWithNew, NewNameVsn}
        ]
    ),
    [
        {old_install_dir, OldInstallDir},
        {new_install_dir, NewInstallDir},
        {node_with_old, NodeWithOld},
        {node_with_new, NodeWithNew},
        {old_name_vsn, OldNameVsn},
        {new_name_vsn, NewNameVsn},
        {old_plugin_name, OldPluginName},
        {new_plugin_name, NewPluginName}
        | Config
    ];
group_t_cluster_force_sync_vsn({'end', Config}) ->
    NodeWithOld = ?config(node_with_old, Config),
    NodeWithNew = ?config(node_with_new, Config),
    ok = emqx_cth_cluster:stop([NodeWithOld, NodeWithNew]);
group_t_cluster_force_sync_vsn(Config) ->
    NodeWithOld = ?config(node_with_old, Config),
    NodeWithNew = ?config(node_with_new, Config),

    Params = unused,
    ?assertMatch(
        {200, [
            #{
                name := ?EMQX_PLUGIN_APP_NAME_BIN,
                rel_vsn := <<"5.1.0">>,
                running_status := [#{node := NodeWithOld, status := running}]
            }
        ]},
        erpc:call(NodeWithOld, emqx_mgmt_api_plugins, list_plugins, [get, Params])
    ),
    ?assertMatch(
        {200, [
            #{
                name := ?EMQX_PLUGIN_APP_NAME_BIN,
                rel_vsn := <<"5.9.0-beta.1">>,
                running_status := [#{node := NodeWithNew, status := running}]
            }
        ]},
        erpc:call(NodeWithNew, emqx_mgmt_api_plugins, list_plugins, [get, Params])
    ),

    ok = erpc:call(NodeWithNew, ekka, join, [NodeWithOld]),
    %% After `NodeWithNew` joined `NodeWithOld`,
    %% The node: `NodeWithNew` should have the same plugin version as `NodeWithOld`
    %% but the new version plugin directory should still exist
    %% aka: the node will have the same plugin version as the node it joined
    %% and it will keep the plugin directory before it joined, untill run `force_sync` action

    %% expected: the new version plugin directory should still exist
    %% list_plugins api will simply list the plugin directory and merge the result
    %% both nodes running the old version plugin
    %% note thet they have same app name `my_emqx_plugin`
    %% so the running status all be `running`
    lists:foreach(
        fun(Node) ->
            ?assertMatch(
                {200, [
                    #{
                        name := ?EMQX_PLUGIN_APP_NAME_BIN,
                        rel_vsn := <<"5.1.0">>,
                        running_status := [#{node := NodeWithOld, status := running}]
                    },
                    #{
                        name := ?EMQX_PLUGIN_APP_NAME_BIN,
                        rel_vsn := <<"5.9.0-beta.1">>,
                        running_status := [#{node := NodeWithNew, status := running}]
                    }
                ]},
                erpc:call(Node, emqx_mgmt_api_plugins, list_plugins, [get, Params])
            )
        end,
        [NodeWithOld, NodeWithNew]
    ),

    {204} = erpc:call(NodeWithNew, emqx_mgmt_api_plugins, sync_plugin, [
        post, #{body => #{<<"name">> => <<"my_emqx_plugin-5.9.0-beta.1">>}}
    ]),
    %% After `force_sync` action, the node should have the new version plugin
    %% and the old version plugin directory should be removed
    %% User should be able to start the new version plugin manually

    NewNameVsn = ?config(new_name_vsn, Config),
    {204} = erpc:call(NodeWithNew, emqx_mgmt_api_plugins, update_plugin, [
        put, #{bindings => #{name => NewNameVsn, action => start}}
    ]),

    %% two nodes all running the new version plugin
    lists:foreach(
        fun(Node) ->
            ?assertMatch(
                {200, [
                    #{
                        name := ?EMQX_PLUGIN_APP_NAME_BIN,
                        rel_vsn := <<"5.9.0-beta.1">>,
                        running_status := [
                            #{status := running},
                            #{status := running}
                        ]
                    }
                ]},
                erpc:call(Node, emqx_mgmt_api_plugins, list_plugins, [get, Params])
            )
        end,
        [NodeWithOld, NodeWithNew]
    ),

    ok.

%% Checks that starting a node with a plugin enabled starts it correctly, and that the
%% hooks added by the plugin's `application:start/2' callback are indeed in place.
%% See also: https://github.com/emqx/emqx/issues/13378
t_start_node_with_plugin_enabled({init, Config}) ->
    #{package := Package} = get_demo_plugin_package(),
    Basename = filename:basename(Package),
    NameVsn = filename:basename(Package, ?PACKAGE_SUFFIX),
    AppSpecs = [
        emqx,
        emqx_conf,
        emqx_ctl,
        {emqx_plugins, #{
            config =>
                #{
                    plugins =>
                        #{
                            install_dir => <<"plugins">>,
                            states =>
                                [
                                    #{
                                        enable => true,
                                        name_vsn => NameVsn
                                    }
                                ]
                        }
                }
        }}
    ],
    Name1 = t_cluster_start_enabled1,
    Name2 = t_cluster_start_enabled2,
    Specs = emqx_cth_cluster:mk_nodespecs(
        [
            {Name1, #{role => core, apps => AppSpecs, join_to => undefined}},
            {Name2, #{role => core, apps => AppSpecs, join_to => undefined}}
        ],
        #{work_dir => emqx_cth_suite:work_dir(?FUNCTION_NAME, Config)}
    ),
    lists:foreach(
        fun(#{work_dir := WorkDir}) ->
            Destination = filename:join([WorkDir, "plugins", Basename]),
            ok = filelib:ensure_dir(Destination),
            {ok, _} = file:copy(Package, Destination)
        end,
        Specs
    ),
    Names = [Name1, Name2],
    Nodes = [emqx_cth_cluster:node_name(N) || N <- Names],
    [
        {node_specs, Specs},
        {nodes, Nodes},
        {name_vsn, NameVsn}
        | Config
    ];
t_start_node_with_plugin_enabled({'end', Config}) ->
    Nodes = ?config(nodes, Config),
    ok = emqx_cth_cluster:stop(Nodes),
    ok;
t_start_node_with_plugin_enabled(Config) when is_list(Config) ->
    NodeSpecs = ?config(node_specs, Config),
    ?check_trace(
        #{timetrap => 30_000},
        begin
            ct:pal("restarting nodes"),
            %% Hack: we use `restart' here to disable the clean state verification, as we
            %% just created and populated the `plugins' directory...
            [N1, N2 | _] = lists:flatmap(fun emqx_cth_cluster:restart/1, NodeSpecs),
            %% `emqx_cth_cluster' starts applications individually and never runs
            %% `emqx_machine_boot:ensure_apps_started/0', which starts plugin apps at
            %% the end of the real node boot.  Emulate that boot tail here.
            ok = ?ON(N1, emqx_plugins:ensure_started()),
            ok = ?ON(N2, emqx_plugins:ensure_started()),
            ct:pal("checking N1 state"),
            ?ON(N1, assert_started_and_hooks_loaded()),
            ct:pal("checking N2 state"),
            ?ON(N2, assert_started_and_hooks_loaded()),
            %% Now make them join.
            %% N.B.: We need to start autocluster so that applications are restarted in
            %% order, and also we need to override the config loader to emulate what
            %% `emqx_cth_cluster' does and avoid the node crashing due to lack of config
            %% keys.
            ok = ?ON(N2, emqx_machine_boot:start_autocluster()),
            ?ON(N2, begin
                StartCallback0 =
                    case ekka:env({callback, start}) of
                        {ok, SC0} -> SC0;
                        _ -> fun() -> ok end
                    end,
                StartCallback = fun() ->
                    ok = emqx_app:set_config_loader(emqx_cth_suite),
                    StartCallback0()
                end,
                ekka:callback(start, StartCallback)
            end),
            %% `emqx_machine_boot_apps_started' fires after the boot tail has
            %% started the plugin apps, so the assertions below cannot race
            %% with the plugin start.
            {ok, {ok, _}} =
                ?wait_async_action(
                    ?ON(N2, emqx_cluster:join(N1)),
                    #{?snk_kind := emqx_machine_boot_apps_started}
                ),
            ct:pal("checking N1 state after join"),
            ?ON(N1, assert_started_and_hooks_loaded()),
            ct:pal("checking N2 state after join"),
            ?ON(N2, assert_started_and_hooks_loaded()),
            ok
        end,
        []
    ),
    ok.

make_tar(Cwd, NameWithVsn) ->
    make_tar(Cwd, NameWithVsn, NameWithVsn).

make_tar(Cwd0, NameWithVsn, TarfileVsn) ->
    %% absolute output path: a bare relative name would land wherever
    %% the cwd happens to point if set_cwd is skipped or misdirected
    Cwd = filename:absname(Cwd0),
    TarFile = filename:join(Cwd, TarfileVsn ++ ".tar.gz"),
    {ok, OriginalCwd} = file:get_cwd(),
    %% archive entries must stay relative to Cwd
    ok = file:set_cwd(Cwd),
    try
        Files = filelib:wildcard(NameWithVsn ++ "/**"),
        ok = erl_tar:create(TarFile, Files, [compressed])
    after
        file:set_cwd(OriginalCwd)
    end.

ensure_state(NameVsn, Position, Enabled) ->
    %% NOTE: this is an internal function that is (legacy) exported in test builds only...
    emqx_plugins:ensure_state(NameVsn, Position, Enabled, _ConfLocation = local).

bin(A) when is_atom(A) -> atom_to_binary(A, utf8);
bin(L) when is_list(L) -> unicode:characters_to_binary(L, utf8);
bin(B) when is_binary(B) -> B.

%%--------------------------------------------------------------------
%% allow_installation TTL + sha256 binding
%%--------------------------------------------------------------------

%% Allow entry expires after the configured TTL and is purged on the next access.
t_allow_ttl_expires({init, Config}) ->
    application:set_env(emqx_plugins, allow_ttl_ms, 50),
    Config;
t_allow_ttl_expires({'end', _Config}) ->
    application:unset_env(emqx_plugins, allow_ttl_ms),
    application:unset_env(emqx_plugins, allowed_installations),
    ok;
t_allow_ttl_expires(_Config) ->
    NameVsn = <<"foo-1.0.0">>,
    ok = emqx_plugins:allow_installation(NameVsn),
    ?assert(emqx_plugins:is_allowed_installation(NameVsn)),
    timer:sleep(150),
    ?assertNot(emqx_plugins:is_allowed_installation(NameVsn)),
    %% Lazy purge: the entry should be gone from the env after the read.
    Allowed = application:get_env(emqx_plugins, allowed_installations, #{}),
    ?assertEqual(#{}, Allowed),
    ok.

%% is_allowed_installation/2 accepts bytes whose sha256 matches the bound hash.
t_allow_sha256_match({init, Config}) ->
    Config;
t_allow_sha256_match({'end', _Config}) ->
    application:unset_env(emqx_plugins, allowed_installations),
    ok;
t_allow_sha256_match(_Config) ->
    NameVsn = <<"foo-1.0.0">>,
    Bin = <<"hello world">>,
    Sha = binary:encode_hex(crypto:hash(sha256, Bin), lowercase),
    ok = emqx_plugins:allow_installation(NameVsn, Sha),
    ?assertEqual(ok, emqx_plugins:is_allowed_installation(NameVsn, Bin)),
    ok.

%% is_allowed_installation/2 rejects bytes whose sha256 does not match.
t_allow_sha256_mismatch({init, Config}) ->
    Config;
t_allow_sha256_mismatch({'end', _Config}) ->
    application:unset_env(emqx_plugins, allowed_installations),
    ok;
t_allow_sha256_mismatch(_Config) ->
    NameVsn = <<"foo-1.0.0">>,
    Allowed = <<"hello world">>,
    Tampered = <<"goodbye world">>,
    Sha = binary:encode_hex(crypto:hash(sha256, Allowed), lowercase),
    ok = emqx_plugins:allow_installation(NameVsn, Sha),
    ?assertEqual(
        {error, sha256_mismatch},
        emqx_plugins:is_allowed_installation(NameVsn, Tampered)
    ),
    ?assertEqual(
        {error, not_allowed},
        emqx_plugins:is_allowed_installation(<<"other-1.0.0">>, Allowed)
    ),
    ok.

%% When no sha256 is bound, is_allowed_installation/2 accepts any bytes (legacy path).
t_allow_sha256_undefined_accepts_any({init, Config}) ->
    Config;
t_allow_sha256_undefined_accepts_any({'end', _Config}) ->
    application:unset_env(emqx_plugins, allowed_installations),
    ok;
t_allow_sha256_undefined_accepts_any(_Config) ->
    NameVsn = <<"foo-1.0.0">>,
    ok = emqx_plugins:allow_installation(NameVsn),
    ?assertEqual(ok, emqx_plugins:is_allowed_installation(NameVsn, <<"any bytes">>)),
    ?assertEqual(ok, emqx_plugins:is_allowed_installation(NameVsn, <<"other bytes">>)),
    ok.

%% A tar entry whose name escapes the install dir (../../../tmp/pwned) must be
%% rejected, and no part of the tarball may land on disk outside the install dir.
t_tar_path_traversal({init, Config}) ->
    InstallDir = ?config(install_dir, Config),
    NameVsn = "evil-1.0.0",
    %% Mimic the PoC from the spec: one legitimate entry plus a traversal entry.
    LegitEntry = filename:join(NameVsn, "release.json"),
    EvilTarget = filename:join(
        "/tmp", "pwned-by-emqx-zipslip-" ++ integer_to_list(erlang:unique_integer([positive]))
    ),
    EvilEntry = "../../../../tmp/" ++ filename:basename(EvilTarget),
    TarGz = filename:join(InstallDir, NameVsn ++ ".tar.gz"),
    ok = erl_tar:create(
        TarGz,
        [
            {LegitEntry, <<"{}">>},
            {EvilEntry, <<"pwned\n">>}
        ],
        [compressed]
    ),
    [{tar_gz, TarGz}, {name_vsn, NameVsn}, {evil_target, EvilTarget} | Config];
t_tar_path_traversal({'end', Config}) ->
    %% Be paranoid in case the test failed and the file actually got written.
    file:delete(?config(evil_target, Config)),
    ok = emqx_plugins:delete_package(?config(name_vsn, Config));
t_tar_path_traversal(Config) ->
    NameVsn = ?config(name_vsn, Config),
    EvilTarget = ?config(evil_target, Config),
    %% Pre-condition: the target must not exist before install.
    ?assertEqual({error, enoent}, file:read_file_info(EvilTarget)),
    ?assertMatch(
        {error, #{msg := "unsafe_tar_entry_path"}},
        emqx_plugins:ensure_installed(NameVsn)
    ),
    %% Post-condition: the malicious file must NOT have been written.
    ?assertEqual({error, enoent}, file:read_file_info(EvilTarget)),
    %% And no plugin dir should have been created either.
    ?assertEqual({error, enoent}, file:read_file_info(emqx_plugins_fs:plugin_dir(NameVsn))),
    ok.

%%--------------------------------------------------------------------
%% Phase 1: CLI install uses ?fresh_install — no cluster config lookup
%%--------------------------------------------------------------------

t_cli_install_no_warning({init, Config}) ->
    Config;
t_cli_install_no_warning({'end', _Config}) ->
    ok;
t_cli_install_no_warning(_Config) ->
    #{name_vsn := NameVsn} = get_demo_plugin_package(),
    ok = emqx_plugins:ensure_installed(NameVsn, ?fresh_install),
    ?assertMatch(
        {ok, #{config_status := disabled}},
        emqx_plugins:describe(NameVsn)
    ),
    ok = emqx_plugins:ensure_uninstalled(NameVsn),
    ok.

t_install_package_rpc({init, Config}) ->
    Config;
t_install_package_rpc({'end', _Config}) ->
    ok;
t_install_package_rpc(_Config) ->
    #{name_vsn := NameVsn} = get_demo_plugin_package(),
    TarPath = emqx_plugins_fs:tar_file_path(NameVsn),
    {ok, TarBin} = file:read_file(TarPath),
    ok = emqx_plugins:delete_package(NameVsn),
    ok = emqx_plugins:purge(NameVsn),
    ok = emqx_plugins:install_package(NameVsn, TarBin),
    ?assertMatch(
        {ok, #{config_status := disabled}},
        emqx_plugins:describe(NameVsn)
    ),
    ok = emqx_plugins:ensure_uninstalled(NameVsn),
    ok.

t_fresh_install_skips_peer_config({init, Config}) ->
    Config;
t_fresh_install_skips_peer_config({'end', _Config}) ->
    ok;
t_fresh_install_skips_peer_config(_Config) ->
    #{name_vsn := NameVsn} = get_demo_plugin_package(),
    ?check_trace(
        emqx_plugins:ensure_installed(NameVsn, ?fresh_install),
        fun(Trace) ->
            ?assertMatch(
                [],
                ?of_kind(failed_to_get_plugin_config_from_cluster, Trace)
            ),
            ok
        end
    ),
    ok = emqx_plugins:ensure_uninstalled(NameVsn),
    ok.

%%--------------------------------------------------------------------
%% Phase 2: cluster install via --cluster flag
%%--------------------------------------------------------------------

group_t_cluster_install({init, Config}) ->
    Specs = emqx_cth_cluster:mk_nodespecs(
        [
            {group_t_cluster_install1, #{
                role => core, apps => [emqx, emqx_conf, emqx_ctl]
            }},
            {group_t_cluster_install2, #{
                role => core, apps => [emqx, emqx_conf, emqx_ctl]
            }}
        ],
        #{work_dir => emqx_cth_suite:work_dir(?FUNCTION_NAME, Config)}
    ),
    Nodes = emqx_cth_cluster:start(Specs),
    InstallRelDir = "plugins_cluster_install",
    InstallDirs = [filename:join(WD, InstallRelDir) || #{work_dir := WD} <- Specs],
    ok = lists:foreach(fun filelib:ensure_path/1, InstallDirs),
    #{package := Package, name_vsn := NameVsn0} =
        emqx_plugins_test_helpers:get_demo_plugin_package(#{
            release_name => ?EMQX_PLUGIN_TEMPLATE_RELEASE_NAME,
            git_url => ?EMQX_PLUGIN_TEMPLATE_URL,
            vsn => ?EMQX_PLUGIN_TEMPLATE_VSN,
            tag => ?EMQX_PLUGIN_TEMPLATE_TAG,
            shdir => hd(InstallDirs)
        }),
    NameVsn = bin(NameVsn0),
    {ok, TarBin} = file:read_file(Package),
    [{ok, _}, {ok, _}] = erpc:multicall(Nodes, emqx_cth_suite, start_app, [
        emqx_plugins,
        #{config => #{plugins => #{install_dir => InstallRelDir}}}
    ]),
    [
        {nodes, Nodes},
        {name_vsn, NameVsn},
        {tar_bin, TarBin}
        | Config
    ];
group_t_cluster_install({'end', Config}) ->
    Nodes = ?config(nodes, Config),
    ok = emqx_cth_cluster:stop(Nodes);
group_t_cluster_install(Config) ->
    [N1, N2] = ?config(nodes, Config),
    NameVsn = ?config(name_vsn, Config),
    TarBin = ?config(tar_bin, Config),

    %% Verify both nodes start with no plugins
    ?assertEqual([], erpc:call(N1, emqx_plugins, list, [])),
    ?assertEqual([], erpc:call(N2, emqx_plugins, list, [])),

    %% Install on both nodes via RPC (simulates --cluster)
    Results = emqx_plugins_proto_v5:install_package([N1, N2], NameVsn, TarBin),
    ?assertMatch([{ok, ok}, {ok, ok}], lists:sort(Results)),

    %% Both nodes should have the plugin
    ?assertMatch(
        [#{config_status := disabled}],
        erpc:call(N1, emqx_plugins, list, [])
    ),
    ?assertMatch(
        [#{config_status := disabled}],
        erpc:call(N2, emqx_plugins, list, [])
    ),

    %% Cleanup
    ok = erpc:call(N1, emqx_plugins, ensure_uninstalled, [NameVsn]),
    ok = erpc:call(N2, emqx_plugins, ensure_uninstalled, [NameVsn]),
    ok.
