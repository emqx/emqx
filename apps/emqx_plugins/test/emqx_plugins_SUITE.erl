%%--------------------------------------------------------------------
%% Copyright (c) 2019-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_plugins_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-define(EMQX_PLUGIN_APP_NAME, my_emqx_plugin).
-define(EMQX_PLUGIN_TEMPLATE_RELEASE_NAME, atom_to_list(?EMQX_PLUGIN_APP_NAME)).
-define(EMQX_PLUGIN_TEMPLATE_URL,
    "https://github.com/emqx/emqx-plugin-template/releases/download/"
).
-define(EMQX_PLUGIN_TEMPLATE_VSN, "5.1.0").
-define(EMQX_PLUGIN_TEMPLATE_TAG, "5.1.0").

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

all() ->
    [
        {group, copy_plugin},
        {group, create_tar_copy_plugin},
        emqx_common_test_helpers:all(?MODULE)
    ].

groups() ->
    [
        {copy_plugin, [sequence], [
            group_t_copy_plugin_to_a_new_node,
            group_t_copy_plugin_to_a_new_node_single_node,
            group_t_cluster_leave
        ]},
        {create_tar_copy_plugin, [sequence], [group_t_copy_plugin_to_a_new_node]}
    ].

init_per_group(copy_plugin, Config) ->
    Config;
init_per_group(create_tar_copy_plugin, Config) ->
    [{remove_tar, true} | Config].

end_per_group(_Group, _Config) ->
    ok.

init_per_suite(Config) ->
    WorkDir = proplists:get_value(data_dir, Config),
    filelib:ensure_path(WorkDir),
    OrigInstallDir = emqx_plugins:get_config(install_dir, undefined),
    emqx_common_test_helpers:start_apps([emqx_conf, emqx_plugins]),
    emqx_plugins:put_config(install_dir, WorkDir),
    [{orig_install_dir, OrigInstallDir} | Config].

end_per_suite(Config) ->
    emqx_common_test_helpers:boot_modules(all),
    emqx_config:erase(plugins),
    %% restore config
    case proplists:get_value(orig_install_dir, Config) of
        undefined -> ok;
        OrigInstallDir -> emqx_plugins:put_config(install_dir, OrigInstallDir)
    end,
    emqx_common_test_helpers:stop_apps([emqx_plugins, emqx_conf]),
    ok.

init_per_testcase(TestCase, Config) ->
    emqx_plugins:put_configured([]),
    lists:foreach(
        fun(#{<<"name">> := Name, <<"rel_vsn">> := Vsn}) ->
            emqx_plugins:purge(bin([Name, "-", Vsn]))
        end,
        emqx_plugins:list()
    ),
    ?MODULE:TestCase({init, Config}).

end_per_testcase(TestCase, Config) ->
    emqx_plugins:put_configured([]),
    ?MODULE:TestCase({'end', Config}).

get_demo_plugin_package() ->
    get_demo_plugin_package(emqx_plugins:install_dir()).

get_demo_plugin_package(
    #{
        release_name := ReleaseName,
        git_url := GitUrl,
        vsn := PluginVsn,
        tag := ReleaseTag,
        shdir := WorkDir
    } = Opts
) ->
    TargetName = lists:flatten([ReleaseName, "-", PluginVsn, ?PACKAGE_SUFFIX]),
    FileURI = lists:flatten(lists:join("/", [GitUrl, ReleaseTag, TargetName])),
    {ok, {_, _, PluginBin}} = httpc:request(FileURI),
    Pkg = filename:join([
        WorkDir,
        TargetName
    ]),
    ok = file:write_file(Pkg, PluginBin),
    Opts#{package => Pkg};
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

bin(A) when is_atom(A) -> atom_to_binary(A, utf8);
bin(L) when is_list(L) -> unicode:characters_to_binary(L, utf8);
bin(B) when is_binary(B) -> B.

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
    #{
        release_name := ReleaseName,
        vsn := PluginVsn
    } = proplists:get_value(plugin_opts, Config),
    ok = emqx_plugins:ensure_installed(NameVsn),
    %% idempotent
    ok = emqx_plugins:ensure_installed(NameVsn),
    {ok, Info} = emqx_plugins:describe(NameVsn),
    ?assertEqual([maps:without([readme], Info)], emqx_plugins:list()),
    %% start
    ok = emqx_plugins:ensure_started(NameVsn),
    ok = assert_app_running(?EMQX_PLUGIN_APP_NAME, true),
    ok = assert_app_running(map_sets, true),
    %% start (idempotent)
    ok = emqx_plugins:ensure_started(bin(NameVsn)),
    ok = assert_app_running(?EMQX_PLUGIN_APP_NAME, true),
    ok = assert_app_running(map_sets, true),

    %% running app can not be un-installed
    ?assertMatch(
        {error, _},
        emqx_plugins:ensure_uninstalled(NameVsn)
    ),

    %% stop
    ok = emqx_plugins:ensure_stopped(NameVsn),
    ok = assert_app_running(?EMQX_PLUGIN_APP_NAME, false),
    ok = assert_app_running(map_sets, false),
    %% stop (idempotent)
    ok = emqx_plugins:ensure_stopped(bin(NameVsn)),
    ok = assert_app_running(?EMQX_PLUGIN_APP_NAME, false),
    ok = assert_app_running(map_sets, false),
    %% still listed after stopped
    ReleaseNameBin = list_to_binary(ReleaseName),
    PluginVsnBin = list_to_binary(PluginVsn),
    ?assertMatch(
        [
            #{
                <<"name">> := ReleaseNameBin,
                <<"rel_vsn">> := PluginVsnBin
            }
        ],
        emqx_plugins:list()
    ),
    ok = emqx_plugins:ensure_uninstalled(NameVsn),
    ?assertEqual([], emqx_plugins:list()),
    ok.

%% help function to create a info file.
%% The file is in JSON format when built
%% but since we are using hocon:load to load it
%% ad-hoc test files can be in hocon format
write_info_file(Config, NameVsn, Content) ->
    WorkDir = proplists:get_value(data_dir, Config),
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
                #{<<"name">> := Name, <<"rel_vsn">> := Vsn}
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

t_start_restart_and_stop({init, Config}) ->
    #{package := Package} = get_demo_plugin_package(),
    NameVsn = filename:basename(Package, ?PACKAGE_SUFFIX),
    [{name_vsn, NameVsn} | Config];
t_start_restart_and_stop({'end', _Config}) ->
    ok;
t_start_restart_and_stop(Config) ->
    NameVsn = proplists:get_value(name_vsn, Config),
    ok = emqx_plugins:ensure_installed(NameVsn),
    ok = emqx_plugins:ensure_enabled(NameVsn),
    FakeInfo =
        "name=bar, rel_vsn=\"2\", rel_apps=[\"bar-9\"],"
        "description=\"desc bar\"",
    Bar2 = <<"bar-2">>,
    ok = write_info_file(Config, Bar2, FakeInfo),
    %% fake a disabled plugin in config
    ok = ensure_state(Bar2, front, false),

    assert_app_running(?EMQX_PLUGIN_APP_NAME, false),
    ok = emqx_plugins:ensure_started(),
    assert_app_running(?EMQX_PLUGIN_APP_NAME, true),

    %% fake enable bar-2
    ok = ensure_state(Bar2, rear, true),
    %% should cause an error
    ?assertError(
        #{function := _, errors := [_ | _]},
        emqx_plugins:ensure_started()
    ),
    %% but demo plugin should still be running
    assert_app_running(?EMQX_PLUGIN_APP_NAME, true),

    %% stop all
    ok = emqx_plugins:ensure_stopped(),
    assert_app_running(?EMQX_PLUGIN_APP_NAME, false),
    ok = ensure_state(Bar2, rear, false),

    ok = emqx_plugins:restart(NameVsn),
    assert_app_running(?EMQX_PLUGIN_APP_NAME, true),
    %% repeat
    ok = emqx_plugins:restart(NameVsn),
    assert_app_running(?EMQX_PLUGIN_APP_NAME, true),

    ok = emqx_plugins:ensure_stopped(),
    ok = emqx_plugins:ensure_disabled(NameVsn),
    ok = emqx_plugins:ensure_uninstalled(NameVsn),
    ok = emqx_plugins:ensure_uninstalled(Bar2),
    ?assertEqual([], emqx_plugins:list()),
    ok.

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
        shdir => emqx_plugins:install_dir(), git_url => ?EMQX_PLUGIN_TEMPLATE_URL
    }),
    NameVsn = filename:basename(Package, ?PACKAGE_SUFFIX),
    ok = emqx_plugins:ensure_installed(NameVsn),
    %% start
    ok = emqx_plugins:ensure_started(NameVsn),
    ok = assert_app_running(AppName, true),
    ok = assert_app_running(map_sets, true),
    %% stop
    ok = emqx_plugins:ensure_stopped(NameVsn),
    ok = assert_app_running(AppName, false),
    ok = assert_app_running(map_sets, false),
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
    ?assertEqual([], emqx_plugins:configured()),
    ok = emqx_plugins:ensure_enabled(NameVsn),
    ?assertEqual([#{name_vsn => NameVsn, enable => true}], emqx_plugins:configured()),
    ok = emqx_plugins:ensure_disabled(NameVsn),
    ?assertEqual([#{name_vsn => NameVsn, enable => false}], emqx_plugins:configured()),
    ok = emqx_plugins:ensure_enabled(bin(NameVsn)),
    ?assertEqual([#{name_vsn => NameVsn, enable => true}], emqx_plugins:configured()),
    ?assertMatch(
        {error, #{
            reason := "bad_plugin_config_status",
            hint := "disable_the_plugin_first"
        }},
        emqx_plugins:ensure_uninstalled(NameVsn)
    ),
    ok = emqx_plugins:ensure_disabled(bin(NameVsn)),
    ok = emqx_plugins:ensure_uninstalled(NameVsn),
    ?assertMatch({error, _}, emqx_plugins:ensure_enabled(NameVsn)),
    ?assertMatch({error, _}, emqx_plugins:ensure_disabled(NameVsn)),
    ok.

assert_app_running(Name, true) ->
    AllApps = application:which_applications(),
    ?assertMatch({Name, _, _}, lists:keyfind(Name, 1, AllApps));
assert_app_running(Name, false) ->
    AllApps = application:which_applications(),
    ?assertEqual(false, lists:keyfind(Name, 1, AllApps)).

t_bad_tar_gz({init, Config}) ->
    Config;
t_bad_tar_gz({'end', _Config}) ->
    ok;
t_bad_tar_gz(Config) ->
    WorkDir = proplists:get_value(data_dir, Config),
    FakeTarTz = filename:join([WorkDir, "fake-vsn.tar.gz"]),
    ok = file:write_file(FakeTarTz, "a\n"),
    ?assertMatch(
        {error, #{
            reason := "bad_plugin_package",
            return := eof
        }},
        emqx_plugins:ensure_installed("fake-vsn")
    ),
    ?assertMatch(
        {error, #{
            reason := "failed_to_extract_plugin_package",
            return := not_found
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
    WorkDir = proplists:get_value(data_dir, Config),
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
    ?assertEqual({error, enoent}, file:read_file_info(emqx_plugins:dir(NameVsn))),
    %% but the tar.gz file is still around
    ?assert(filelib:is_regular(TarGz)),
    ok.

%% test that we even cleanup content that doesn't match the expected name-vsn
%% pattern
t_tar_vsn_content_mismatch({init, Config}) ->
    WorkDir = proplists:get_value(data_dir, Config),
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
    ?assertEqual({error, enoent}, file:read_file_info(emqx_plugins:dir(NameVsn))),
    ?assertEqual({error, enoent}, file:read_file_info(emqx_plugins:dir("foo-0.2"))),
    %% the tar.gz file is still around
    ?assert(filelib:is_regular(TarGz)),
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
            error := "bad_info_file",
            return := {parse_error, _}
        }},
        emqx_plugins:describe(NameVsn)
    ),
    ok = write_info_file(Config, NameVsn, "{\"bad\": \"obj\"}"),
    ?assertMatch(
        {error, #{
            error := "bad_info_file_content",
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
            shdir => emqx_plugins:install_dir()
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
    {ok, Info} = emqx_plugins:read_plugin(NameVsn, #{}),
    ?assertEqual([Info], emqx_plugins:list()),
    %% start
    ok = emqx_plugins:ensure_started(NameVsn),
    ok = assert_app_running(elixir_plugin_template, true),
    ok = assert_app_running(hallux, true),
    %% start (idempotent)
    ok = emqx_plugins:ensure_started(bin(NameVsn)),
    ok = assert_app_running(elixir_plugin_template, true),
    ok = assert_app_running(hallux, true),

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
    ok = assert_app_running(elixir_plugin_template, false),
    ok = assert_app_running(hallux, false),
    %% stop (idempotent)
    ok = emqx_plugins:ensure_stopped(bin(NameVsn)),
    ok = assert_app_running(elixir_plugin_template, false),
    ok = assert_app_running(hallux, false),
    %% still listed after stopped
    ReleaseNameBin = list_to_binary(ReleaseName),
    PluginVsnBin = list_to_binary(PluginVsn),
    ?assertMatch(
        [
            #{
                <<"name">> := ReleaseNameBin,
                <<"rel_vsn">> := PluginVsnBin
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
    ?assertEqual([], emqx_plugins:configured()),
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
    WorkDir = proplists:get_value(data_dir, Config),
    FromInstallDir = filename:join(WorkDir, atom_to_list(plugins_copy_from)),
    file:del_dir_r(FromInstallDir),
    ok = filelib:ensure_path(FromInstallDir),
    ToInstallDir = filename:join(WorkDir, atom_to_list(plugins_copy_to)),
    file:del_dir_r(ToInstallDir),
    ok = filelib:ensure_path(ToInstallDir),
    #{package := Package, release_name := PluginName} = get_demo_plugin_package(FromInstallDir),
    [{CopyFrom, CopyFromOpts}, {CopyTo, CopyToOpts}] =
        emqx_common_test_helpers:emqx_cluster(
            [
                {core, plugins_copy_from},
                {core, plugins_copy_to}
            ],
            #{
                apps => [emqx_conf, emqx_plugins],
                env => [
                    {emqx, boot_modules, []}
                ],
                load_schema => false
            }
        ),
    CopyFromNode = emqx_common_test_helpers:start_peer(
        CopyFrom, maps:remove(join_to, CopyFromOpts)
    ),
    ok = rpc:call(CopyFromNode, emqx_plugins, put_config, [install_dir, FromInstallDir]),
    CopyToNode = emqx_common_test_helpers:start_peer(CopyTo, maps:remove(join_to, CopyToOpts)),
    ok = rpc:call(CopyToNode, emqx_plugins, put_config, [install_dir, ToInstallDir]),
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
    CopyFromNode = proplists:get_value(copy_from_node, Config),
    CopyToNode = proplists:get_value(copy_to_node, Config),
    ok = rpc:call(CopyFromNode, emqx_config, delete_override_conf_files, []),
    ok = rpc:call(CopyToNode, emqx_config, delete_override_conf_files, []),
    rpc:call(CopyToNode, ekka, leave, []),
    rpc:call(CopyFromNode, ekka, leave, []),
    ok = emqx_common_test_helpers:stop_peer(CopyToNode),
    ok = emqx_common_test_helpers:stop_peer(CopyFromNode),
    ok = file:del_dir_r(proplists:get_value(to_install_dir, Config)),
    ok = file:del_dir_r(proplists:get_value(from_install_dir, Config));
group_t_copy_plugin_to_a_new_node(Config) ->
    CopyFromNode = proplists:get_value(copy_from_node, Config),
    CopyToNode = proplists:get_value(copy_to_node, Config),
    CopyToDir = proplists:get_value(to_install_dir, Config),
    CopyFromPluginsState = rpc:call(CopyFromNode, emqx_plugins, get_config, [[states], []]),
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
    ok = rpc:call(CopyToNode, emqx_plugins, put_config, [[states], CopyFromPluginsState]),
    %% Plugin copying is triggered upon app restart on a new node.
    %% This is similar to emqx_conf, which copies cluster-override conf upon start,
    %% see: emqx_conf_app:init_conf/0
    ok = rpc:call(CopyToNode, application, stop, [emqx_plugins]),
    {ok, _} = rpc:call(CopyToNode, application, ensure_all_started, [emqx_plugins]),
    ?assertMatch(
        {ok, #{running_status := running, config_status := enabled}},
        rpc:call(CopyToNode, emqx_plugins, describe, [NameVsn])
    ).

%% checks that we can start a cluster with a lone node.
group_t_copy_plugin_to_a_new_node_single_node({init, Config}) ->
    PrivDataDir = ?config(priv_dir, Config),
    ToInstallDir = filename:join(PrivDataDir, "plugins_copy_to"),
    file:del_dir_r(ToInstallDir),
    ok = filelib:ensure_path(ToInstallDir),
    #{package := Package, release_name := PluginName} = get_demo_plugin_package(ToInstallDir),
    NameVsn = filename:basename(Package, ?PACKAGE_SUFFIX),
    [{CopyTo, CopyToOpts}] =
        emqx_common_test_helpers:emqx_cluster(
            [
                {core, plugins_copy_to}
            ],
            #{
                apps => [emqx_conf, emqx_plugins],
                env => [
                    {emqx, boot_modules, []}
                ],
                env_handler => fun
                    (emqx_plugins) ->
                        ok = emqx_plugins:put_config(install_dir, ToInstallDir),
                        %% this is to simulate an user setting the state
                        %% via environment variables before starting the node
                        ok = emqx_plugins:put_config(
                            states,
                            [#{name_vsn => NameVsn, enable => true}]
                        ),
                        ok;
                    (_) ->
                        ok
                end,
                priv_data_dir => PrivDataDir,
                schema_mod => emqx_conf_schema,
                load_schema => true
            }
        ),
    [
        {to_install_dir, ToInstallDir},
        {copy_to_node_name, CopyTo},
        {copy_to_opts, CopyToOpts},
        {name_vsn, NameVsn},
        {plugin_name, PluginName}
        | Config
    ];
group_t_copy_plugin_to_a_new_node_single_node({'end', Config}) ->
    CopyToNode = proplists:get_value(copy_to_node_name, Config),
    ok = emqx_common_test_helpers:stop_peer(CopyToNode),
    ok = file:del_dir_r(proplists:get_value(to_install_dir, Config)),
    ok;
group_t_copy_plugin_to_a_new_node_single_node(Config) ->
    CopyTo = ?config(copy_to_node_name, Config),
    CopyToOpts = ?config(copy_to_opts, Config),
    ToInstallDir = ?config(to_install_dir, Config),
    NameVsn = proplists:get_value(name_vsn, Config),
    %% Start the node for the first time. The plugin should start
    %% successfully even if it's not extracted yet.  Simply starting
    %% the node would crash if not working properly.
    CopyToNode = emqx_common_test_helpers:start_peer(CopyTo, CopyToOpts),
    ct:pal("~p config:\n  ~p", [
        CopyToNode, erpc:call(CopyToNode, emqx_plugins, get_config, [[], #{}])
    ]),
    ct:pal("~p install_dir:\n  ~p", [
        CopyToNode, erpc:call(CopyToNode, file, list_dir, [ToInstallDir])
    ]),
    ?assertMatch(
        {ok, #{running_status := running, config_status := enabled}},
        rpc:call(CopyToNode, emqx_plugins, describe, [NameVsn])
    ),
    ok.

group_t_cluster_leave({init, Config}) ->
    PrivDataDir = ?config(priv_dir, Config),
    ToInstallDir = filename:join(PrivDataDir, "plugins_copy_to"),
    file:del_dir_r(ToInstallDir),
    ok = filelib:ensure_path(ToInstallDir),
    #{package := Package, release_name := PluginName} = get_demo_plugin_package(ToInstallDir),
    NameVsn = filename:basename(Package, ?PACKAGE_SUFFIX),
    Cluster =
        emqx_common_test_helpers:emqx_cluster(
            [core, core],
            #{
                apps => [emqx_conf, emqx_plugins],
                env => [
                    {emqx, boot_modules, []}
                ],
                env_handler => fun
                    (emqx_plugins) ->
                        ok = emqx_plugins:put_config(install_dir, ToInstallDir),
                        %% this is to simulate an user setting the state
                        %% via environment variables before starting the node
                        ok = emqx_plugins:put_config(
                            states,
                            [#{name_vsn => NameVsn, enable => true}]
                        ),
                        ok;
                    (_) ->
                        ok
                end,
                priv_data_dir => PrivDataDir,
                schema_mod => emqx_conf_schema,
                load_schema => true
            }
        ),
    Nodes = [emqx_common_test_helpers:start_peer(Name, Opts) || {Name, Opts} <- Cluster],
    [
        {to_install_dir, ToInstallDir},
        {cluster, Cluster},
        {nodes, Nodes},
        {name_vsn, NameVsn},
        {plugin_name, PluginName}
        | Config
    ];
group_t_cluster_leave({'end', Config}) ->
    Nodes = proplists:get_value(nodes, Config),
    [ok = emqx_common_test_helpers:stop_peer(N) || N <- Nodes],
    ok = file:del_dir_r(proplists:get_value(to_install_dir, Config)),
    ok;
group_t_cluster_leave(Config) ->
    [N1, N2] = ?config(nodes, Config),
    NameVsn = proplists:get_value(name_vsn, Config),
    ok = erpc:call(N1, emqx_plugins, ensure_installed, [NameVsn]),
    ok = erpc:call(N1, emqx_plugins, ensure_started, [NameVsn]),
    ok = erpc:call(N1, emqx_plugins, ensure_enabled, [NameVsn]),
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

make_tar(Cwd, NameWithVsn) ->
    make_tar(Cwd, NameWithVsn, NameWithVsn).

make_tar(Cwd, NameWithVsn, TarfileVsn) ->
    {ok, OriginalCwd} = file:get_cwd(),
    ok = file:set_cwd(Cwd),
    try
        Files = filelib:wildcard(NameWithVsn ++ "/**"),
        TarFile = TarfileVsn ++ ".tar.gz",
        ok = erl_tar:create(TarFile, Files, [compressed])
    after
        file:set_cwd(OriginalCwd)
    end.

ensure_state(NameVsn, Position, Enabled) ->
    %% NOTE: this is an internal function that is (legacy) exported in test builds only...
    emqx_plugins:ensure_state(NameVsn, Position, Enabled, _ConfLocation = local).
