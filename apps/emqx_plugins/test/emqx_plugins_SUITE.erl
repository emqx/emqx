%%--------------------------------------------------------------------
%% Copyright (c) 2019-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-include_lib("emqx/include/emqx.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(EMQX_PLUGIN_TEMPLATE_VSN, "5.0.0-rc.3").
-define(EMQX_ELIXIR_PLUGIN_TEMPLATE_VSN, "0.1.0").
-define(PACKAGE_SUFFIX, ".tar.gz").

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    WorkDir = proplists:get_value(data_dir, Config),
    OrigInstallDir = emqx_plugins:get_config(install_dir, undefined),
    emqx_common_test_helpers:start_apps([emqx_conf]),
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
    emqx_common_test_helpers:stop_apps([emqx_conf]),
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

build_demo_plugin_package() ->
    build_demo_plugin_package(
        #{
            target_path => "_build/default/emqx_plugrel",
            release_name => "emqx_plugin_template",
            git_url => "https://github.com/emqx/emqx-plugin-template.git",
            vsn => ?EMQX_PLUGIN_TEMPLATE_VSN,
            workdir => "demo_src",
            shdir => emqx_plugins:install_dir()
        }
    ).

build_demo_plugin_package(
    #{
        target_path := TargetPath,
        release_name := ReleaseName,
        git_url := GitUrl,
        vsn := PluginVsn,
        workdir := DemoWorkDir,
        shdir := WorkDir
    } = Opts
) ->
    BuildSh = filename:join([WorkDir, "build-demo-plugin.sh"]),
    Cmd = string:join(
        [
            BuildSh,
            PluginVsn,
            TargetPath,
            ReleaseName,
            GitUrl,
            DemoWorkDir
        ],
        " "
    ),
    case emqx_run_sh:do(Cmd, [{cd, WorkDir}]) of
        {ok, _} ->
            Pkg = filename:join([
                WorkDir,
                ReleaseName ++ "-" ++
                    PluginVsn ++
                    ?PACKAGE_SUFFIX
            ]),
            case filelib:is_regular(Pkg) of
                true -> Opts#{package => Pkg};
                false -> error(#{reason => unexpected_build_result, not_found => Pkg})
            end;
        {error, {Rc, Output}} ->
            io:format(user, "failed_to_build_demo_plugin, Exit = ~p, Output:~n~ts\n", [Rc, Output]),
            error(failed_to_build_demo_plugin)
    end.

bin(A) when is_atom(A) -> atom_to_binary(A, utf8);
bin(L) when is_list(L) -> unicode:characters_to_binary(L, utf8);
bin(B) when is_binary(B) -> B.

t_demo_install_start_stop_uninstall({init, Config}) ->
    Opts = #{package := Package} = build_demo_plugin_package(),
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
    ok = assert_app_running(emqx_plugin_template, true),
    ok = assert_app_running(map_sets, true),
    %% start (idempotent)
    ok = emqx_plugins:ensure_started(bin(NameVsn)),
    ok = assert_app_running(emqx_plugin_template, true),
    ok = assert_app_running(map_sets, true),

    %% running app can not be un-installed
    ?assertMatch(
        {error, _},
        emqx_plugins:ensure_uninstalled(NameVsn)
    ),

    %% stop
    ok = emqx_plugins:ensure_stopped(NameVsn),
    ok = assert_app_running(emqx_plugin_template, false),
    ok = assert_app_running(map_sets, false),
    %% stop (idempotent)
    ok = emqx_plugins:ensure_stopped(bin(NameVsn)),
    ok = assert_app_running(emqx_plugin_template, false),
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
    #{package := Package} = build_demo_plugin_package(),
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
    ok = emqx_plugins:ensure_state(PosApp2, {before, NameVsn}, false),
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
    #{package := Package} = build_demo_plugin_package(),
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
    ok = emqx_plugins:ensure_state(Bar2, front, false),

    assert_app_running(emqx_plugin_template, false),
    ok = emqx_plugins:ensure_started(),
    assert_app_running(emqx_plugin_template, true),

    %% fake enable bar-2
    ok = emqx_plugins:ensure_state(Bar2, rear, true),
    %% should cause an error
    ?assertError(
        #{function := _, errors := [_ | _]},
        emqx_plugins:ensure_started()
    ),
    %% but demo plugin should still be running
    assert_app_running(emqx_plugin_template, true),

    %% stop all
    ok = emqx_plugins:ensure_stopped(),
    assert_app_running(emqx_plugin_template, false),
    ok = emqx_plugins:ensure_state(Bar2, rear, false),

    ok = emqx_plugins:restart(NameVsn),
    assert_app_running(emqx_plugin_template, true),
    %% repeat
    ok = emqx_plugins:restart(NameVsn),
    assert_app_running(emqx_plugin_template, true),

    ok = emqx_plugins:ensure_stopped(),
    ok = emqx_plugins:ensure_disabled(NameVsn),
    ok = emqx_plugins:ensure_uninstalled(NameVsn),
    ok = emqx_plugins:ensure_uninstalled(Bar2),
    ?assertEqual([], emqx_plugins:list()),
    ok.

t_enable_disable({init, Config}) ->
    #{package := Package} = build_demo_plugin_package(),
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

%% create a corrupted .tar.gz
%% failed install attempts should not leave behind extracted dir
t_bad_tar_gz2({init, Config}) ->
    Config;
t_bad_tar_gz2({'end', _Config}) ->
    ok;
t_bad_tar_gz2(Config) ->
    WorkDir = proplists:get_value(data_dir, Config),
    NameVsn = "foo-0.2",
    %% this an invalid info file content
    BadInfo = "name=foo, rel_vsn=\"0.2\", rel_apps=[foo]",
    ok = write_info_file(Config, NameVsn, BadInfo),
    TarGz = filename:join([WorkDir, NameVsn ++ ".tar.gz"]),
    ok = make_tar(WorkDir, NameVsn),
    ?assert(filelib:is_regular(TarGz)),
    %% failed to install, it also cleans up the bad .tar.gz file
    ?assertMatch({error, _}, emqx_plugins:ensure_installed(NameVsn)),
    %% the tar.gz file is still around
    ?assert(filelib:is_regular(TarGz)),
    ?assertEqual({error, enoent}, file:read_file_info(emqx_plugins:dir(NameVsn))),
    ok = emqx_plugins:delete_package(NameVsn).

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
            target_path => "_build/prod/plugrelex/elixir_plugin_template",
            release_name => "elixir_plugin_template",
            git_url => "https://github.com/emqx/emqx-elixir-plugin.git",
            vsn => ?EMQX_ELIXIR_PLUGIN_TEMPLATE_VSN,
            workdir => "demo_src_elixir",
            shdir => emqx_plugins:install_dir()
        },
    Opts = #{package := Package} = build_demo_plugin_package(Opts0),
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

make_tar(Cwd, NameWithVsn) ->
    {ok, OriginalCwd} = file:get_cwd(),
    ok = file:set_cwd(Cwd),
    try
        Files = filelib:wildcard(NameWithVsn ++ "/**"),
        TarFile = NameWithVsn ++ ".tar.gz",
        ok = erl_tar:create(TarFile, Files, [compressed])
    after
        file:set_cwd(OriginalCwd)
    end.
