%%--------------------------------------------------------------------
%% Copyright (c) 2020-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_mgmt_api_plugins_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-define(EMQX_PLUGIN_TEMPLATE_NAME, "my_emqx_plugin").
-define(EMQX_PLUGIN_TEMPLATE_APP_NAME, my_emqx_plugin).
-define(EMQX_PLUGIN_TEMPLATE_VSN, "5.9.0-beta.3").
-define(EMQX_PLUGIN_TEMPLATE_TAG, "5.9.0-beta.3").
-define(EMQX_PLUGIN_TEMPLATE_URL,
    "https://github.com/emqx/emqx-plugin-template/releases/download/"
).
-define(PACKAGE_SUFFIX, ".tar.gz").

-define(EMQX_PLUGIN, #{
    release_name => ?EMQX_PLUGIN_TEMPLATE_NAME,
    app_name => ?EMQX_PLUGIN_TEMPLATE_APP_NAME,
    git_url => ?EMQX_PLUGIN_TEMPLATE_URL,
    vsn => ?EMQX_PLUGIN_TEMPLATE_VSN,
    tag => ?EMQX_PLUGIN_TEMPLATE_TAG
}).

-define(CLUSTER_API_SERVER(PORT), ("http://127.0.0.1:" ++ (integer_to_list(PORT)))).

-define(ON(NODE, BODY), erpc:call(NODE, fun() -> BODY end)).

-import(emqx_common_test_helpers, [on_exit/1]).

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [
            emqx_conf,
            emqx_plugins,
            {emqx_management, #{
                after_start => fun emqx_mgmt_cli:load/0
            }},
            emqx_mgmt_api_test_util:emqx_dashboard()
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    ok = emqx_cth_suite:stop(?config(apps, Config)).

init_per_testcase(t_cluster_update_order = TestCase, Config0) ->
    Config = [{api_port, 18085} | Config0],
    Cluster = [Node1 | _] = cluster(TestCase, Config),
    {ok, API} = init_api(Node1),
    [
        {api, API},
        {cluster, Cluster}
        | Config
    ];
init_per_testcase(TestCase, Config) ->
    ToInstallDir = filename:join(emqx_cth_suite:work_dir(TestCase, Config), "emqx_plugins"),
    emqx_plugins:put_config_internal(install_dir, ToInstallDir),
    Config.

end_per_testcase(t_cluster_update_order, Config) ->
    Cluster = ?config(cluster, Config),
    emqx_cth_cluster:stop(Cluster),
    end_per_testcase(common, Config);
end_per_testcase(_TestCase, _Config) ->
    emqx_common_test_helpers:call_janitor(),
    ok.

t_plugins(_Config) ->
    PackagePath = get_demo_plugin_package(),
    NameVsn = filename:basename(PackagePath, ?PACKAGE_SUFFIX),
    ok = emqx_plugins:ensure_uninstalled(NameVsn),
    ok = emqx_plugins:delete_package(NameVsn),
    %% Must allow via CLI first.
    ?assertMatch({ok, {{_, 403, _}, _, _}}, install_plugin(PackagePath)),
    ok = allow_installation(NameVsn),
    %% Test disallow
    ok = disallow_installation(NameVsn),
    ?assertMatch({ok, {{_, 403, _}, _, _}}, install_plugin(PackagePath)),
    %% Now really allow it.
    ok = allow_installation(NameVsn),
    ok = install_plugin(PackagePath),
    Node = atom_to_binary(node()),
    ?assertMatch(
        #{
            <<"running_status">> := [
                #{<<"node">> := Node, <<"status">> := <<"stopped">>}
            ]
        },
        describe_plugin(NameVsn)
    ),
    {ok, StopRes1} = update_plugin(NameVsn, "start"),
    ?assertEqual([], StopRes1),
    ?assertMatch(
        #{
            <<"running_status">> := [
                #{<<"node">> := Node, <<"status">> := <<"running">>}
            ]
        },
        describe_plugin(NameVsn)
    ),
    {ok, []} = update_plugin(NameVsn, "stop"),
    ?assertMatch(
        #{
            <<"running_status">> := [
                #{<<"node">> := Node, <<"status">> := <<"stopped">>}
            ]
        },
        describe_plugin(NameVsn)
    ),
    {ok, []} = uninstall_plugin(NameVsn),
    %% Should forget that we allowed installation after uninstall
    ?assertMatch({ok, {{_, 403, _}, _, _}}, install_plugin(PackagePath)),
    ok.

t_update_config(_Config) ->
    PackagePath = get_demo_plugin_package(),
    NameVsn = filename:basename(PackagePath, ?PACKAGE_SUFFIX),
    ok = emqx_plugins:ensure_uninstalled(NameVsn),
    ok = emqx_plugins:delete_package(NameVsn),
    ok = allow_installation(NameVsn),
    ok = install_plugin(PackagePath),
    OldConfig = emqx_plugins:get_config(NameVsn),
    %% Check config update when plugin is not started
    ?assertMatch(
        {ok, 400, _},
        update_plugin_config(NameVsn, OldConfig#{<<"hostname">> => <<"bad.host">>})
    ),
    ?assertMatch(
        {ok, 204, _},
        update_plugin_config(NameVsn, OldConfig#{<<"hostname">> => <<"localhost">>})
    ),
    %% Check config update when plugin is started
    {ok, _} = update_plugin(NameVsn, "start"),
    ?assertMatch(
        {ok, 400, _},
        update_plugin_config(NameVsn, OldConfig#{<<"hostname">> => <<"bad.host">>})
    ),
    ?assertMatch(
        {ok, 204, _},
        update_plugin_config(NameVsn, OldConfig#{<<"hostname">> => <<"localhost">>})
    ),
    {ok, []} = update_plugin(NameVsn, "stop"),
    %% Check config update when plugin is stopped
    ?assertMatch(
        {ok, 400, _},
        update_plugin_config(NameVsn, OldConfig#{<<"hostname">> => <<"bad.host">>})
    ),
    ?assertMatch(
        {ok, 204, _},
        update_plugin_config(NameVsn, OldConfig#{<<"hostname">> => <<"localhost">>})
    ),
    %% Clean up
    {ok, []} = uninstall_plugin(NameVsn).

t_upload_download_config(_Config) ->
    PackagePath = get_demo_plugin_package(),
    NameVsn = filename:basename(PackagePath, ?PACKAGE_SUFFIX),
    ok = emqx_plugins:ensure_uninstalled(NameVsn),
    ok = emqx_plugins:delete_package(NameVsn),
    ok = allow_installation(NameVsn),
    ok = install_plugin(PackagePath),
    DownloadPath = emqx_mgmt_api_test_util:api_path(["plugins", NameVsn, "config", "download"]),
    ?assertMatch(
        {ok, _},
        emqx_mgmt_api_test_util:request_api(get, DownloadPath)
    ),
    OldConfig = emqx_plugins:get_config(NameVsn),
    JSONData = emqx_utils_json:encode(OldConfig),
    UploadPath = emqx_mgmt_api_test_util:api_path(["plugins", NameVsn, "config", "upload"]),
    ?assertMatch(
        {ok, 204, _},
        emqx_dashboard_api_test_helpers:multipart_formdata_request(UploadPath, [], [
            {config, "config.json", JSONData}
        ])
    ),
    %% Clean up
    {ok, []} = uninstall_plugin(NameVsn).

t_health_status(_Config) ->
    PackagePath = get_demo_plugin_package(),
    NameVsn = filename:basename(PackagePath, ?PACKAGE_SUFFIX),
    ok = emqx_plugins:ensure_uninstalled(NameVsn),
    ok = emqx_plugins:delete_package(NameVsn),
    ok = allow_installation(NameVsn),
    ok = install_plugin(PackagePath),
    OldConfig = emqx_plugins:get_config(NameVsn),
    Node = atom_to_binary(node()),
    %% No health status for stopped plugin
    ?assertNotMatch(
        #{
            <<"running_status">> := [
                #{<<"node">> := Node, <<"health_status">> := _}
            ]
        },
        describe_plugin(NameVsn)
    ),
    %% Check health status when plugin is started
    {ok, _} = update_plugin(NameVsn, "start"),
    ?assertMatch(
        #{
            <<"running_status">> := [
                #{<<"node">> := Node, <<"health_status">> := #{<<"status">> := <<"ok">>}}
            ]
        },
        describe_plugin(NameVsn)
    ),
    %% Check health status in /plugins
    ?assertMatch(
        [
            #{
                <<"running_status">> := [
                    #{<<"node">> := Node, <<"health_status">> := #{<<"status">> := <<"ok">>}}
                ]
            }
        ],
        list_plugins()
    ),
    %% Change config to make plugin unhealthy. Unhealthines for ports other than
    %% 3306 is baked in the plugin.
    ?assertMatch(
        {ok, 204, _},
        update_plugin_config(NameVsn, OldConfig#{<<"port">> => 3307})
    ),
    ?assertMatch(
        #{
            <<"running_status">> := [
                #{<<"node">> := Node, <<"health_status">> := #{<<"status">> := <<"error">>}}
            ]
        },
        describe_plugin(NameVsn)
    ),
    {ok, []} = update_plugin(NameVsn, "stop"),
    %% Check health status when plugin is stopped
    ?assertNotMatch(
        #{
            <<"running_status">> := [
                #{<<"node">> := Node, <<"health_status">> := _}
            ]
        },
        describe_plugin(NameVsn)
    ),
    %% Clean up
    {ok, []} = uninstall_plugin(NameVsn).

t_install_plugin_sha256_match(_Config) ->
    PackagePath = get_demo_plugin_package(),
    NameVsn = filename:basename(PackagePath, ?PACKAGE_SUFFIX),
    ok = emqx_plugins:ensure_uninstalled(NameVsn),
    ok = emqx_plugins:delete_package(NameVsn),
    {ok, Bin} = file:read_file(PackagePath),
    Sha = binary_to_list(binary:encode_hex(crypto:hash(sha256, Bin), lowercase)),
    ok = allow_installation(NameVsn, Sha),
    ok = install_plugin(PackagePath),
    _ = describe_plugin(NameVsn),
    {ok, []} = uninstall_plugin(NameVsn),
    ok.

t_install_plugin_sha256_mismatch(_Config) ->
    PackagePath = get_demo_plugin_package(),
    NameVsn = filename:basename(PackagePath, ?PACKAGE_SUFFIX),
    ok = emqx_plugins:ensure_uninstalled(NameVsn),
    ok = emqx_plugins:delete_package(NameVsn),
    %% Bind a hash that intentionally doesn't match the file bytes.
    BogusSha = string:copies("a", 64),
    ok = allow_installation(NameVsn, BogusSha),
    ?assertMatch({ok, {{_, 403, _}, _, _}}, install_plugin(PackagePath)),
    %% Clean up the leftover allow entry so other cases aren't affected.
    ok = disallow_installation(NameVsn),
    ok.

%% A tarball sitting in the install dir without having been unpacked (e.g.
%% copied by hand, or left behind by a CLI install that the allow gate
%% refused) is not an installed plugin.  Uploading it must not short-circuit
%% to ALREADY_INSTALLED before the allow gate: without a grant the upload is
%% rejected with the actionable 403, and with a grant it installs.
t_install_orphan_package_requires_allow(_Config) ->
    PackagePath = get_demo_plugin_package(),
    NameVsn = filename:basename(PackagePath, ?PACKAGE_SUFFIX),
    ok = emqx_plugins:ensure_uninstalled(NameVsn),
    ok = emqx_plugins:delete_package(NameVsn),
    on_exit(fun() ->
        _ = emqx_plugins:ensure_stopped(NameVsn),
        _ = emqx_plugins:ensure_uninstalled(NameVsn),
        _ = emqx_plugins:delete_package(NameVsn),
        _ = disallow_installation(NameVsn)
    end),
    %% Simulate the orphan tarball: bytes present, nothing unpacked.
    {ok, Bin} = file:read_file(PackagePath),
    ok = emqx_plugins:write_package(NameVsn, Bin),
    ?assertMatch({error, #{msg := "bad_info_file"}}, emqx_plugins:describe(NameVsn, #{})),
    ?assertMatch({true, [_]}, emqx_plugins:is_package_present(NameVsn)),
    %% Not allowed yet: the upload must ask for `plugins allow', not claim
    %% that the package is already installed.
    {ok, {{_, 403, _}, _, Body}} = install_plugin(PackagePath),
    #{<<"code">> := <<"FORBIDDEN">>, <<"message">> := Msg} = emqx_utils_json:decode(Body),
    ?assertNotEqual(nomatch, binary:match(Msg, <<"plugins allow">>)),
    %% The rejected upload left no installation behind, and did not consume
    %% the orphan tarball.
    ?assertMatch({error, #{msg := "bad_info_file"}}, emqx_plugins:describe(NameVsn, #{})),
    ?assertMatch({true, [_]}, emqx_plugins:is_package_present(NameVsn)),
    %% Once allowed, the very same upload installs the package.
    ok = allow_installation(NameVsn),
    ok = install_plugin(PackagePath),
    ?assertMatch(#{<<"name">> := <<"my_emqx_plugin">>}, describe_plugin(NameVsn)),
    {ok, []} = uninstall_plugin(NameVsn),
    ok.

%% A plugin directory left behind by an interrupted installation (here: the
%% directory is there, but it has no `release.json') is not an installation
%% either.  The upload must go through the allow gate, and once allowed it must
%% purge the leftovers and unpack the package into a clean directory, instead
%% of failing after having deleted the uploaded package.
t_install_over_leftover_install_dir(_Config) ->
    PackagePath = get_demo_plugin_package(),
    NameVsn = filename:basename(PackagePath, ?PACKAGE_SUFFIX),
    ok = emqx_plugins:ensure_uninstalled(NameVsn),
    ok = emqx_plugins:delete_package(NameVsn),
    on_exit(fun() ->
        _ = emqx_plugins:ensure_stopped(NameVsn),
        _ = emqx_plugins:ensure_uninstalled(NameVsn),
        _ = emqx_plugins:delete_package(NameVsn),
        _ = disallow_installation(NameVsn)
    end),
    %% Simulate the leftovers of an interrupted unpack.
    StaleFile = filename:join(emqx_plugins_fs:plugin_dir(NameVsn), "stale.txt"),
    ok = filelib:ensure_dir(StaleFile),
    ok = file:write_file(StaleFile, <<"stale">>),
    ?assertMatch(
        {error, #{msg := "bad_info_file", reason := {enoent, _}}},
        emqx_plugins:describe(NameVsn, #{})
    ),
    %% Not allowed yet: the upload must ask for `plugins allow'.
    {ok, {{_, 403, _}, _, Body}} = install_plugin(PackagePath),
    #{<<"code">> := <<"FORBIDDEN">>, <<"message">> := Msg} = emqx_utils_json:decode(Body),
    ?assertNotEqual(nomatch, binary:match(Msg, <<"plugins allow">>)),
    %% The rejected upload has no side effect on the leftovers.
    ?assertMatch({error, #{msg := "bad_info_file"}}, emqx_plugins:describe(NameVsn, #{})),
    ?assert(filelib:is_regular(StaleFile)),
    %% Once allowed, the leftovers are purged and the package is installed.
    ok = allow_installation(NameVsn),
    ok = install_plugin(PackagePath),
    ?assertMatch(#{<<"name">> := <<"my_emqx_plugin">>}, describe_plugin(NameVsn)),
    ?assertEqual({error, enoent}, file:read_file_info(StaleFile)),
    {ok, []} = uninstall_plugin(NameVsn),
    ok.

%% Same as above, but the leftover directory even has an unreadable
%% `release.json', so that `describe/2' fails with something else than
%% `enoent'.  The upload must not crash (it used to return 500) and must
%% repair the installation once allowed.
t_install_over_broken_install_dir(_Config) ->
    PackagePath = get_demo_plugin_package(),
    NameVsn = filename:basename(PackagePath, ?PACKAGE_SUFFIX),
    ok = emqx_plugins:ensure_uninstalled(NameVsn),
    ok = emqx_plugins:delete_package(NameVsn),
    on_exit(fun() ->
        _ = emqx_plugins:ensure_stopped(NameVsn),
        _ = emqx_plugins:ensure_uninstalled(NameVsn),
        _ = emqx_plugins:delete_package(NameVsn),
        _ = disallow_installation(NameVsn)
    end),
    InfoFile = emqx_plugins_fs:info_file_path(NameVsn),
    ok = filelib:ensure_dir(InfoFile),
    BrokenInfo = emqx_utils_json:encode(#{
        name => <<"other_plugin">>,
        rel_vsn => <<"1.0.0">>,
        rel_apps => [<<"other_plugin-1.0.0">>],
        description => <<"stale">>
    }),
    ok = file:write_file(InfoFile, BrokenInfo),
    ?assertMatch({error, #{msg := "name_vsn_mismatch"}}, emqx_plugins:describe(NameVsn, #{})),
    ok = allow_installation(NameVsn),
    ok = install_plugin(PackagePath),
    ?assertMatch(#{<<"name">> := <<"my_emqx_plugin">>}, describe_plugin(NameVsn)),
    {ok, []} = uninstall_plugin(NameVsn),
    ok.

%% A complete installation is still an installation: uploading the same package
%% again is refused with ALREADY_INSTALLED and the installed files are kept.
t_install_over_complete_install_dir(_Config) ->
    PackagePath = get_demo_plugin_package(),
    NameVsn = filename:basename(PackagePath, ?PACKAGE_SUFFIX),
    ok = emqx_plugins:ensure_uninstalled(NameVsn),
    ok = emqx_plugins:delete_package(NameVsn),
    on_exit(fun() ->
        _ = emqx_plugins:ensure_stopped(NameVsn),
        _ = emqx_plugins:ensure_uninstalled(NameVsn),
        _ = emqx_plugins:delete_package(NameVsn),
        _ = disallow_installation(NameVsn)
    end),
    ok = allow_installation(NameVsn),
    ok = install_plugin(PackagePath),
    %% the applications declared by the package have all been unpacked
    ?assertEqual(installed, emqx_plugins:install_state(NameVsn)),
    AppFiles = emqx_plugins_test_helpers:plugin_app_files(NameVsn),
    ?assertMatch([_ | _], AppFiles),
    {ok, {{_, 400, _}, _, Body}} = install_plugin(PackagePath),
    #{<<"code">> := <<"ALREADY_INSTALLED">>} = emqx_utils_json:decode(Body),
    ?assertEqual(installed, emqx_plugins:install_state(NameVsn)),
    emqx_plugins_test_helpers:assert_files_exist(AppFiles),
    {ok, []} = uninstall_plugin(NameVsn),
    ok.

%% An unpack can stop right after `release.json' has been written, so the
%% directory holds a readable manifest without the applications it declares.
%% That is not an installation: the upload must go through the allow gate and
%% unpack the package instead of replying ALREADY_INSTALLED.
t_install_over_manifest_only_install_dir(_Config) ->
    PackagePath = get_demo_plugin_package(),
    NameVsn = filename:basename(PackagePath, ?PACKAGE_SUFFIX),
    ok = emqx_plugins:ensure_uninstalled(NameVsn),
    ok = emqx_plugins:delete_package(NameVsn),
    on_exit(fun() ->
        _ = emqx_plugins:ensure_stopped(NameVsn),
        _ = emqx_plugins:ensure_uninstalled(NameVsn),
        _ = emqx_plugins:delete_package(NameVsn),
        _ = disallow_installation(NameVsn)
    end),
    %% keep `release.json' only, as if the unpack had been interrupted there
    ok = erl_tar:extract(PackagePath, [compressed, {cwd, emqx_plugins_fs:install_dir()}]),
    ok = emqx_plugins_test_helpers:delete_plugin_app_dirs(NameVsn),
    ?assertMatch({ok, _}, emqx_plugins:describe(NameVsn, #{})),
    ?assertEqual([], emqx_plugins_test_helpers:plugin_app_files(NameVsn)),
    %% Not allowed yet: the upload must ask for `plugins allow', not claim
    %% that the package is already installed.
    {ok, {{_, 403, _}, _, Body}} = install_plugin(PackagePath),
    #{<<"code">> := <<"FORBIDDEN">>, <<"message">> := Msg} = emqx_utils_json:decode(Body),
    ?assertNotEqual(nomatch, binary:match(Msg, <<"plugins allow">>)),
    %% Once allowed, the very same upload unpacks the package.
    ok = allow_installation(NameVsn),
    ok = install_plugin(PackagePath),
    ?assertMatch(#{<<"name">> := <<"my_emqx_plugin">>}, describe_plugin(NameVsn)),
    ?assertMatch([_ | _], emqx_plugins_test_helpers:plugin_app_files(NameVsn)),
    {ok, []} = uninstall_plugin(NameVsn),
    ok.

%% The files of a plugin whose applications are running must not be replaced by
%% an upload, even when its metadata has become unreadable: the upload is
%% refused and the running plugin is left untouched.
t_install_running_plugin_broken_metadata(_Config) ->
    PackagePath = get_demo_plugin_package(),
    NameVsn = filename:basename(PackagePath, ?PACKAGE_SUFFIX),
    ok = emqx_plugins:ensure_uninstalled(NameVsn),
    ok = emqx_plugins:delete_package(NameVsn),
    on_exit(fun() ->
        %% restore the metadata, otherwise the plugin can not be stopped
        _ = emqx_plugins_test_helpers:restore_info_file_from_package(PackagePath, NameVsn),
        _ = emqx_plugins:ensure_stopped(NameVsn),
        _ = emqx_plugins:ensure_uninstalled(NameVsn),
        _ = emqx_plugins:delete_package(NameVsn),
        _ = disallow_installation(NameVsn)
    end),
    ok = allow_installation(NameVsn),
    ok = install_plugin(PackagePath),
    ok = emqx_plugins:ensure_started(NameVsn),
    ?assertEqual(running, emqx_plugins_apps:running_status(NameVsn)),
    AppFiles = emqx_plugins_test_helpers:plugin_app_files(NameVsn),
    ?assertMatch([_ | _], AppFiles),
    %% the metadata of the running plugin becomes unreadable
    ok = file:write_file(emqx_plugins_fs:info_file_path(NameVsn), <<"not json">>),
    ?assertMatch({error, _}, emqx_plugins:describe(NameVsn, #{})),
    %% the package that is installed now
    TarFile = emqx_plugins_fs:tar_file_path(NameVsn),
    {ok, InstalledPackage} = file:read_file(TarFile),
    {ok, InstalledChecksum} = file:read_file(TarFile ++ ".md5sum"),
    ok = allow_installation(NameVsn),
    %% an upload which is refused must not destroy it: it may be the only
    %% local copy the broken installation can be repaired from
    RefusedPackage = create_modified_package(PackagePath),
    {ok, {{_, 400, _}, _, Body}} = install_plugin(RefusedPackage),
    #{<<"code">> := <<"BAD_PLUGIN_INFO">>, <<"message">> := Msg} = emqx_utils_json:decode(Body),
    ?assertNotEqual(nomatch, binary:match(Msg, <<"plugin_is_in_use">>)),
    %% the message tells the user how to get out of the situation
    ?assertNotEqual(nomatch, binary:match(Msg, <<"stop the plugin first">>)),
    ?assertEqual({ok, InstalledPackage}, file:read_file(TarFile)),
    ?assertEqual({ok, InstalledChecksum}, file:read_file(TarFile ++ ".md5sum")),
    %% the running plugin is still running, from the same files
    ?assertEqual(running, emqx_plugins_apps:running_status(NameVsn)),
    emqx_plugins_test_helpers:assert_files_exist(AppFiles),
    %% the plugin can be stopped even though its metadata is unreadable, and
    %% the upload then replaces the broken installation
    ok = emqx_plugins:ensure_stopped(NameVsn),
    ?assertNotEqual(running, emqx_plugins_apps:running_status(NameVsn)),
    ok = install_plugin(PackagePath),
    ?assertEqual(installed, emqx_plugins:install_state(NameVsn)),
    ok.

%% A package which does not contain the applications its own metadata
%% declares must be rejected instead of being reported as installed (the next
%% state check would classify it as incomplete again).
t_install_incomplete_package_returns_error(_Config) ->
    PackagePath = get_demo_plugin_package(),
    NameVsn = filename:basename(PackagePath, ?PACKAGE_SUFFIX),
    ok = emqx_plugins:ensure_uninstalled(NameVsn),
    ok = emqx_plugins:delete_package(NameVsn),
    on_exit(fun() ->
        _ = emqx_plugins:ensure_stopped(NameVsn),
        _ = emqx_plugins:ensure_uninstalled(NameVsn),
        _ = emqx_plugins:delete_package(NameVsn),
        _ = disallow_installation(NameVsn)
    end),
    %% a beam file which the application resource file declares is missing
    IncompletePackage = strip_declared_beam(PackagePath),
    ok = allow_installation(NameVsn),
    {ok, {{_, 400, _}, _, Body}} = install_plugin(IncompletePackage),
    #{<<"code">> := <<"BAD_PLUGIN_INFO">>, <<"message">> := Msg} = emqx_utils_json:decode(Body),
    ?assertNotEqual(nomatch, binary:match(Msg, <<"incomplete_plugin_package">>)),
    %% the message tells the user what to do
    ?assertNotEqual(nomatch, binary:match(Msg, <<"rebuild or reinstall">>)),
    %% nothing of the refused package has been installed
    ?assertEqual(absent, emqx_plugins:install_state(NameVsn)),
    ok.

t_install_plugin_matching_exisiting_name(_Config) ->
    PackagePath = get_demo_plugin_package(),
    NameVsn = filename:basename(PackagePath, ?PACKAGE_SUFFIX),
    ok = emqx_plugins:ensure_uninstalled(NameVsn),
    ok = emqx_plugins:delete_package(NameVsn),
    NameVsn1 = ?EMQX_PLUGIN_TEMPLATE_NAME ++ "_a" ++ "-" ++ ?EMQX_PLUGIN_TEMPLATE_VSN,
    PackagePath1 = create_renamed_package(PackagePath, NameVsn1),
    NameVsn1 = filename:basename(PackagePath1, ?PACKAGE_SUFFIX),
    ok = emqx_plugins:ensure_uninstalled(NameVsn1),
    ok = emqx_plugins:delete_package(NameVsn1),
    %% First, install plugin "emqx_plugin_template_a", then:
    %% "emqx_plugin_template" which matches the beginning
    %% of the previously installed plugin name
    ok = allow_installation(NameVsn),
    ok = allow_installation(NameVsn1),
    ok = install_plugin(PackagePath1),
    ok = install_plugin(PackagePath),
    _ = describe_plugin(NameVsn),
    _ = describe_plugin(NameVsn1),
    _ = uninstall_plugin(NameVsn),
    _ = uninstall_plugin(NameVsn1).

t_install_plugin_with_bad_form_data(_Config) ->
    AuthHeader = emqx_common_test_http:default_auth_header(),
    Path = emqx_mgmt_api_test_util:api_path(["plugins", "install"]),
    {error, {"HTTP/1.1", 400, "Bad Request"}} = emqx_mgmt_api_test_util:request_api(
        post, Path, "", AuthHeader, #{}
    ).

t_bad_plugin(_Config) ->
    PackagePathOrig = get_demo_plugin_package(),
    BackupPath = filename:join(["/tmp", [filename:basename(PackagePathOrig), ".backup"]]),
    {ok, _} = file:copy(PackagePathOrig, BackupPath),
    on_exit(fun() -> {ok, _} = file:rename(BackupPath, PackagePathOrig) end),
    PackagePath = filename:join([
        filename:dirname(PackagePathOrig),
        "bad_plugin-1.0.0.tar.gz"
    ]),
    on_exit(fun() -> file:delete(PackagePath) end),
    ct:pal("package_location:~p orig:~p", [PackagePath, PackagePathOrig]),
    %% rename plugin tarball
    file:copy(PackagePathOrig, PackagePath),
    file:delete(PackagePathOrig),
    NameVsn = filename:basename(PackagePath, ?PACKAGE_SUFFIX),
    ok = allow_installation(NameVsn),
    {ok, {{"HTTP/1.1", 400, "Bad Request"}, _, _}} = install_plugin(PackagePath),
    ?assertEqual(
        {error, enoent},
        file:delete(
            filename:join([
                emqx_plugins_fs:install_dir(),
                filename:basename(PackagePath)
            ])
        )
    ).

t_delete_non_existing(_Config) ->
    Path = emqx_mgmt_api_test_util:api_path(["plugins", "non_exists-1.0.0"]),
    ?assertMatch(
        {error, {_, 404, _}},
        emqx_mgmt_api_test_util:request_api(delete, Path)
    ),
    ok.

t_cluster_update_order(Config) ->
    [N1 | _] = ?config(cluster, Config),
    PackagePath1 = get_demo_plugin_package(),
    NameVsn1 = filename:basename(PackagePath1, ?PACKAGE_SUFFIX),
    Name2Str = ?EMQX_PLUGIN_TEMPLATE_NAME ++ "_a",
    NameVsn2 = Name2Str ++ "-" ++ ?EMQX_PLUGIN_TEMPLATE_VSN,
    PackagePath2 = create_renamed_package(PackagePath1, NameVsn2),
    Name1 = list_to_binary(?EMQX_PLUGIN_TEMPLATE_NAME),
    Name2 = list_to_binary(Name2Str),

    ?ON(N1, begin
        ok = allow_installation(NameVsn1),
        ok = allow_installation(NameVsn2)
    end),
    ok = install_plugin_into_cluster(Config, PackagePath1),
    ok = install_plugin_into_cluster(Config, PackagePath2),
    %% to get them configured...
    {ok, _} = update_plugin_in_cluster(Config, NameVsn1, "start"),
    {ok, _} = update_plugin_in_cluster(Config, NameVsn2, "start"),

    ?assertMatch(
        [
            #{<<"name">> := Name1},
            #{<<"name">> := Name2}
        ],
        list_plugins_from_cluster(Config)
    ),

    ct:pal("moving to rear"),
    ?assertMatch({ok, _}, update_boot_order(NameVsn1, #{position => rear}, Config)),
    ?assertMatch(
        [
            #{<<"name">> := Name2},
            #{<<"name">> := Name1}
        ],
        list_plugins_from_cluster(Config)
    ),

    ct:pal("moving to front"),
    ?assertMatch({ok, _}, update_boot_order(NameVsn1, #{position => front}, Config)),
    ?assertMatch(
        [
            #{<<"name">> := Name1},
            #{<<"name">> := Name2}
        ],
        list_plugins_from_cluster(Config)
    ),

    ct:pal("moving after"),
    NameVsn2Bin = list_to_binary(NameVsn2),
    ?assertMatch(
        {ok, _},
        update_boot_order(NameVsn1, #{position => <<"after:", NameVsn2Bin/binary>>}, Config)
    ),
    ?assertMatch(
        [
            #{<<"name">> := Name2},
            #{<<"name">> := Name1}
        ],
        list_plugins_from_cluster(Config)
    ),

    ct:pal("moving before"),
    ?assertMatch(
        {ok, _},
        update_boot_order(NameVsn1, #{position => <<"before:", NameVsn2Bin/binary>>}, Config)
    ),
    ?assertMatch(
        [
            #{<<"name">> := Name1},
            #{<<"name">> := Name2}
        ],
        list_plugins_from_cluster(Config)
    ),

    ok.

list_plugins_from_cluster(Config) ->
    #{host := Host, auth := Auth} = get_host_and_auth(Config),
    Path = emqx_mgmt_api_test_util:api_path(Host, ["plugins"]),
    case emqx_mgmt_api_test_util:request_api(get, Path, Auth) of
        {ok, Apps} -> emqx_utils_json:decode(Apps);
        {error, Reason} -> error(Reason)
    end.

list_plugins() ->
    Path = emqx_mgmt_api_test_util:api_path(["plugins"]),
    case emqx_mgmt_api_test_util:request_api(get, Path) of
        {ok, Apps} -> emqx_utils_json:decode(Apps);
        {error, Reason} -> error(Reason)
    end.

describe_plugin(Name) ->
    Path = emqx_mgmt_api_test_util:api_path(["plugins", Name]),
    case emqx_mgmt_api_test_util:request_api(get, Path) of
        {ok, Res} -> emqx_utils_json:decode(Res);
        {error, Reason} -> error(Reason)
    end.

update_plugin_config(Name, Config) ->
    Path = emqx_mgmt_api_test_util:api_path(["plugins", Name, "config"]),
    case emqx_mgmt_api_test_util:request_api_with_body(put, Path, Config) of
        {ok, Res} -> {ok, emqx_utils_json:decode(Res)};
        Error -> Error
    end.

install_plugin(FilePath) ->
    {ok, #{token := Token}} = emqx_dashboard_admin:sign_token(<<"admin">>, <<"public">>),
    Path = emqx_mgmt_api_test_util:api_path(["plugins", "install"]),
    case
        emqx_mgmt_api_test_util:upload_request(
            Path,
            FilePath,
            "plugin",
            <<"application/gzip">>,
            [],
            Token
        )
    of
        {ok, {{"HTTP/1.1", 204, "No Content"}, _Headers, <<>>}} -> ok;
        Error -> Error
    end.

install_plugin_into_cluster(Config, FilePath) ->
    #{host := Host, auth := Auth} = get_host_and_auth(Config),
    Path = emqx_mgmt_api_test_util:api_path(Host, ["plugins", "install"]),
    case
        emqx_mgmt_api_test_util:upload_request(
            Path,
            FilePath,
            "plugin",
            <<"application/gzip">>,
            [],
            Auth
        )
    of
        {ok, {{"HTTP/1.1", 204, "No Content"}, _Headers, <<>>}} -> ok;
        Error -> Error
    end.

update_plugin(Name, Action) ->
    Path = emqx_mgmt_api_test_util:api_path(["plugins", Name, Action]),
    emqx_mgmt_api_test_util:request_api(put, Path).

update_plugin_in_cluster(Config, Name, Action) when is_list(Config) ->
    #{host := Host, auth := Auth} = get_host_and_auth(Config),
    Path = emqx_mgmt_api_test_util:api_path(Host, ["plugins", Name, Action]),
    emqx_mgmt_api_test_util:request_api(put, Path, Auth).

update_boot_order(Name, MoveBody, Config) ->
    #{host := Host, auth := Auth} = get_host_and_auth(Config),
    Path = emqx_mgmt_api_test_util:api_path(Host, ["plugins", Name, "move"]),
    Opts = #{return_all => true},
    case emqx_mgmt_api_test_util:request_api(post, Path, "", Auth, MoveBody, Opts) of
        {ok, Res} ->
            Resp =
                case emqx_utils_json:safe_decode(Res) of
                    {ok, Decoded} -> Decoded;
                    {error, _} -> Res
                end,
            ct:pal("update_boot_order response:\n  ~p", [Resp]),
            {ok, Resp};
        Error ->
            Error
    end.

uninstall_plugin(Name) ->
    DeletePath = emqx_mgmt_api_test_util:api_path(["plugins", Name]),
    emqx_mgmt_api_test_util:request_api(delete, DeletePath).

get_demo_plugin_package() ->
    #{package := Pkg} = emqx_plugins_test_helpers:get_demo_plugin_package(
        maps:merge(?EMQX_PLUGIN, #{shdir => "./"})
    ),
    true = filelib:is_regular(Pkg),
    Pkg.

%% The same plugin package with different bytes: a file is added, the name-vsn
%% stays the same, so an upload of it can only be told apart from the installed
%% package by comparing the bytes.
create_modified_package(PackagePath) ->
    {ok, Content} = erl_tar:extract(PackagePath, [compressed, memory]),
    NameVsn = filename:basename(PackagePath, ?PACKAGE_SUFFIX),
    ModifiedPath = filename:join([upload_scratch_dir(), NameVsn ++ ?PACKAGE_SUFFIX]),
    ok = filelib:ensure_dir(ModifiedPath),
    ok = erl_tar:create(
        ModifiedPath,
        [{filename:join(NameVsn, "REVIEW_EXTRA.md"), <<"different bytes">>} | Content],
        [compressed]
    ),
    ModifiedPath.

%% The same plugin package with one beam file removed: the application resource
%% file still declares the module.
strip_declared_beam(PackagePath) ->
    {ok, Content} = erl_tar:extract(PackagePath, [compressed, memory]),
    NameVsn = filename:basename(PackagePath, ?PACKAGE_SUFFIX),
    AppFile = find_app_file(Content),
    [Module | _] = declared_modules(AppFile, Content),
    MissingBeam = filename:join(filename:dirname(AppFile), atom_to_list(Module) ++ ".beam"),
    StrippedPath = filename:join([
        upload_scratch_dir(), "incomplete", NameVsn ++ ?PACKAGE_SUFFIX
    ]),
    ok = filelib:ensure_dir(StrippedPath),
    ok = erl_tar:create(
        StrippedPath,
        [{File, Bin} || {File, Bin} <- Content, File =/= MissingBeam],
        [compressed]
    ),
    StrippedPath.

%% A directory for the packages the tests upload: outside of the directories of
%% the plugins themselves, so that they do not look like installations.
upload_scratch_dir() ->
    filename:join([emqx_plugins_fs:install_dir(), "upload"]).

find_app_file(Content) ->
    [AppFile | _] = [
        File
     || {File, _Bin} <- Content,
        filename:extension(File) =:= ".app"
    ],
    AppFile.

declared_modules(AppFile, Content) ->
    AppFileBin = proplists:get_value(AppFile, Content),
    TmpFile = filename:join([upload_scratch_dir(), "declared_modules.app"]),
    ok = filelib:ensure_dir(TmpFile),
    ok = file:write_file(TmpFile, AppFileBin),
    {ok, [{application, _AppName, Props}]} = file:consult(TmpFile),
    proplists:get_value(modules, Props).

create_renamed_package(PackagePath, NewNameVsn) ->
    {ok, Content} = erl_tar:extract(PackagePath, [compressed, memory]),
    {NewName, _Vsn} = emqx_plugins_utils:parse_name_vsn(NewNameVsn),
    NewNameB = atom_to_binary(NewName, utf8),
    Content1 = lists:map(
        fun({F, B}) ->
            [_ | PathPart] = filename:split(F),
            B1 = update_release_json(PathPart, B, NewNameB),
            {filename:join([NewNameVsn | PathPart]), B1}
        end,
        Content
    ),
    NewPackagePath = filename:join(filename:dirname(PackagePath), NewNameVsn ++ ?PACKAGE_SUFFIX),
    ok = erl_tar:create(NewPackagePath, Content1, [compressed]),
    NewPackagePath.

update_release_json(["release.json"], FileContent, NewName) ->
    ContentMap = emqx_utils_json:decode(FileContent),
    emqx_utils_json:encode(ContentMap#{<<"name">> => NewName});
update_release_json(_FileName, FileContent, _NewName) ->
    FileContent.

cluster(TestCase, Config) ->
    APIPort = ?config(api_port, Config),
    AppSpecs = app_specs(Config),
    Node1Apps = AppSpecs ++ [app_spec_dashboard(APIPort)],
    Node2Apps = AppSpecs,
    Node1Name = emqx_mgmt_api_plugins_SUITE1,
    Node1 = emqx_cth_cluster:node_name(Node1Name),
    emqx_cth_cluster:start(
        [
            {Node1Name, #{role => core, apps => Node1Apps, join_to => Node1}},
            {emqx_mgmt_api_plugins_SUITE2, #{role => core, apps => Node2Apps, join_to => Node1}}
        ],
        #{work_dir => emqx_cth_suite:work_dir(TestCase, Config)}
    ).

app_specs(_Config) ->
    [
        emqx,
        emqx_conf,
        {emqx_management, #{
            after_start => fun emqx_mgmt_cli:load/0
        }},
        emqx_plugins
    ].

app_spec_dashboard(APIPort) ->
    {emqx_dashboard, #{
        config =>
            #{
                dashboard =>
                    #{
                        listeners =>
                            #{
                                http =>
                                    #{bind => APIPort}
                            }
                    }
            }
    }}.

init_api(Node) ->
    erpc:call(Node, emqx_common_test_http, create_default_app, []).

get_host_and_auth(Config) when is_list(Config) ->
    API = ?config(api, Config),
    APIPort = ?config(api_port, Config),
    Host = ?CLUSTER_API_SERVER(APIPort),
    Auth = emqx_common_test_http:auth_header(API),
    #{host => Host, auth => Auth}.

allow_installation(NameVsn) ->
    emqx_ctl:run_command(["plugins", "allow", NameVsn]).

allow_installation(NameVsn, Sha256Hex) ->
    emqx_ctl:run_command(["plugins", "allow", NameVsn, "sha256:" ++ Sha256Hex]).

disallow_installation(NameVsn) ->
    emqx_ctl:run_command(["plugins", "disallow", NameVsn]).
