%%--------------------------------------------------------------------
%% Copyright (c) 2020-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-define(EMQX_PLUGIN_TEMPLATE_VSN, "5.1.0").
-define(PACKAGE_SUFFIX, ".tar.gz").

-define(CLUSTER_API_SERVER(PORT), ("http://127.0.0.1:" ++ (integer_to_list(PORT)))).

-import(emqx_common_test_helpers, [on_exit/1]).

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    WorkDir = proplists:get_value(data_dir, Config),
    DemoShDir1 = string:replace(WorkDir, "emqx_mgmt_api_plugins", "emqx_plugins"),
    DemoShDir = lists:flatten(string:replace(DemoShDir1, "emqx_management", "emqx_plugins")),
    Apps = emqx_cth_suite:start(
        [
            emqx_conf,
            emqx_plugins,
            emqx_management,
            emqx_mgmt_api_test_util:emqx_dashboard()
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    ok = filelib:ensure_dir(DemoShDir),
    emqx_plugins:put_config_internal(install_dir, DemoShDir),
    [{apps, Apps}, {demo_sh_dir, DemoShDir} | Config].

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
init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(t_cluster_update_order, Config) ->
    Cluster = ?config(cluster, Config),
    emqx_cth_cluster:stop(Cluster),
    end_per_testcase(common, Config);
end_per_testcase(_TestCase, _Config) ->
    emqx_common_test_helpers:call_janitor(),
    ok.

t_plugins(Config) ->
    DemoShDir = proplists:get_value(demo_sh_dir, Config),
    PackagePath = get_demo_plugin_package(DemoShDir),
    ct:pal("package_location:~p install dir:~p", [PackagePath, emqx_plugins:install_dir()]),
    NameVsn = filename:basename(PackagePath, ?PACKAGE_SUFFIX),
    ok = emqx_plugins:ensure_uninstalled(NameVsn),
    ok = emqx_plugins:delete_package(NameVsn),
    ok = install_plugin(PackagePath),
    {ok, StopRes} = describe_plugins(NameVsn),
    Node = atom_to_binary(node()),
    ?assertMatch(
        #{
            <<"running_status">> := [
                #{<<"node">> := Node, <<"status">> := <<"stopped">>}
            ]
        },
        StopRes
    ),
    {ok, StopRes1} = update_plugin(NameVsn, "start"),
    ?assertEqual([], StopRes1),
    {ok, StartRes} = describe_plugins(NameVsn),
    ?assertMatch(
        #{
            <<"running_status">> := [
                #{<<"node">> := Node, <<"status">> := <<"running">>}
            ]
        },
        StartRes
    ),
    {ok, []} = update_plugin(NameVsn, "stop"),
    {ok, StopRes2} = describe_plugins(NameVsn),
    ?assertMatch(
        #{
            <<"running_status">> := [
                #{<<"node">> := Node, <<"status">> := <<"stopped">>}
            ]
        },
        StopRes2
    ),
    {ok, []} = uninstall_plugin(NameVsn),
    ok.

t_install_plugin_matching_exisiting_name(Config) ->
    DemoShDir = proplists:get_value(demo_sh_dir, Config),
    PackagePath = get_demo_plugin_package(DemoShDir),
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
    ok = install_plugin(PackagePath1),
    ok = install_plugin(PackagePath),
    {ok, _} = describe_plugins(NameVsn),
    {ok, _} = describe_plugins(NameVsn1),
    {ok, _} = uninstall_plugin(NameVsn),
    {ok, _} = uninstall_plugin(NameVsn1).

t_bad_plugin(Config) ->
    DemoShDir = proplists:get_value(demo_sh_dir, Config),
    PackagePathOrig = get_demo_plugin_package(DemoShDir),
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
    {ok, {{"HTTP/1.1", 400, "Bad Request"}, _, _}} = install_plugin(PackagePath),
    ?assertEqual(
        {error, enoent},
        file:delete(
            filename:join([
                emqx_plugins:install_dir(),
                filename:basename(PackagePath)
            ])
        )
    ).

t_delete_non_existing(_Config) ->
    Path = emqx_mgmt_api_test_util:api_path(["plugins", "non_exists"]),
    ?assertMatch(
        {error, {_, 404, _}},
        emqx_mgmt_api_test_util:request_api(delete, Path)
    ),
    ok.

t_cluster_update_order(Config) ->
    DemoShDir = proplists:get_value(demo_sh_dir, Config),
    PackagePath1 = get_demo_plugin_package(DemoShDir),
    NameVsn1 = filename:basename(PackagePath1, ?PACKAGE_SUFFIX),
    Name2Str = ?EMQX_PLUGIN_TEMPLATE_NAME ++ "_a",
    NameVsn2 = Name2Str ++ "-" ++ ?EMQX_PLUGIN_TEMPLATE_VSN,
    PackagePath2 = create_renamed_package(PackagePath1, NameVsn2),
    Name1 = list_to_binary(?EMQX_PLUGIN_TEMPLATE_NAME),
    Name2 = list_to_binary(Name2Str),

    ok = install_plugin(Config, PackagePath1),
    ok = install_plugin(Config, PackagePath2),
    %% to get them configured...
    {ok, _} = update_plugin(Config, NameVsn1, "start"),
    {ok, _} = update_plugin(Config, NameVsn2, "start"),

    ?assertMatch(
        {ok, [
            #{<<"name">> := Name1},
            #{<<"name">> := Name2}
        ]},
        list_plugins(Config)
    ),

    ct:pal("moving to rear"),
    ?assertMatch({ok, _}, update_boot_order(NameVsn1, #{position => rear}, Config)),
    ?assertMatch(
        {ok, [
            #{<<"name">> := Name2},
            #{<<"name">> := Name1}
        ]},
        list_plugins(Config)
    ),

    ct:pal("moving to front"),
    ?assertMatch({ok, _}, update_boot_order(NameVsn1, #{position => front}, Config)),
    ?assertMatch(
        {ok, [
            #{<<"name">> := Name1},
            #{<<"name">> := Name2}
        ]},
        list_plugins(Config)
    ),

    ct:pal("moving after"),
    NameVsn2Bin = list_to_binary(NameVsn2),
    ?assertMatch(
        {ok, _},
        update_boot_order(NameVsn1, #{position => <<"after:", NameVsn2Bin/binary>>}, Config)
    ),
    ?assertMatch(
        {ok, [
            #{<<"name">> := Name2},
            #{<<"name">> := Name1}
        ]},
        list_plugins(Config)
    ),

    ct:pal("moving before"),
    ?assertMatch(
        {ok, _},
        update_boot_order(NameVsn1, #{position => <<"before:", NameVsn2Bin/binary>>}, Config)
    ),
    ?assertMatch(
        {ok, [
            #{<<"name">> := Name1},
            #{<<"name">> := Name2}
        ]},
        list_plugins(Config)
    ),

    ok.

list_plugins(Config) ->
    #{host := Host, auth := Auth} = get_host_and_auth(Config),
    Path = emqx_mgmt_api_test_util:api_path(Host, ["plugins"]),
    case emqx_mgmt_api_test_util:request_api(get, Path, Auth) of
        {ok, Apps} -> {ok, emqx_utils_json:decode(Apps, [return_maps])};
        Error -> Error
    end.

describe_plugins(Name) ->
    Path = emqx_mgmt_api_test_util:api_path(["plugins", Name]),
    case emqx_mgmt_api_test_util:request_api(get, Path) of
        {ok, Res} -> {ok, emqx_utils_json:decode(Res, [return_maps])};
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

install_plugin(Config, FilePath) ->
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

update_plugin(Config, Name, Action) when is_list(Config) ->
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
                case emqx_utils_json:safe_decode(Res, [return_maps]) of
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

get_demo_plugin_package(Dir) ->
    #{package := Pkg} = emqx_plugins_SUITE:get_demo_plugin_package(),
    FileName = ?EMQX_PLUGIN_TEMPLATE_NAME ++ "-" ++ ?EMQX_PLUGIN_TEMPLATE_VSN ++ ?PACKAGE_SUFFIX,
    PluginPath = "./" ++ FileName,
    Pkg = filename:join([Dir, FileName]),
    _ = os:cmd("cp " ++ Pkg ++ " " ++ PluginPath),
    true = filelib:is_regular(PluginPath),
    PluginPath.

create_renamed_package(PackagePath, NewNameVsn) ->
    {ok, Content} = erl_tar:extract(PackagePath, [compressed, memory]),
    {ok, NewName, _Vsn} = emqx_plugins:parse_name_vsn(NewNameVsn),
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
    ContentMap = emqx_utils_json:decode(FileContent, [return_maps]),
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
        emqx_management,
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
