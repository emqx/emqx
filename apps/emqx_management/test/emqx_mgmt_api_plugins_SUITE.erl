%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

disallow_installation(NameVsn) ->
    emqx_ctl:run_command(["plugins", "disallow", NameVsn]).
