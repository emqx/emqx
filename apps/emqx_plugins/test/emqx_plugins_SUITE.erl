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

-define(EMQX_PLUGIN_TEMPLATE_RELEASE_NAME, "emqx_plugin_template").
-define(EMQX_PLUGIN_TEMPLATE_URL,
    "https://github.com/emqx/emqx-plugin-template/releases/download/"
).
-define(EMQX_PLUGIN_TEMPLATE_VSN, "5.0.0").
-define(EMQX_PLUGIN_TEMPLATE_TAG, "5.0.0").
-define(EMQX_ELIXIR_PLUGIN_TEMPLATE_RELEASE_NAME, "elixir_plugin_template").
-define(EMQX_ELIXIR_PLUGIN_TEMPLATE_URL,
    "https://github.com/emqx/emqx-elixir-plugin/releases/download/"
).
-define(EMQX_ELIXIR_PLUGIN_TEMPLATE_VSN, "0.1.0").
-define(EMQX_ELIXIR_PLUGIN_TEMPLATE_TAG, "0.1.0-2").
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

get_demo_plugin_package() ->
    get_demo_plugin_package(
        #{
            release_name => ?EMQX_PLUGIN_TEMPLATE_RELEASE_NAME,
            git_url => ?EMQX_PLUGIN_TEMPLATE_URL,
            vsn => ?EMQX_PLUGIN_TEMPLATE_VSN,
            tag => ?EMQX_PLUGIN_TEMPLATE_TAG,
            shdir => emqx_plugins:install_dir()
        }
    ).

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
    Opts#{package => Pkg}.

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
