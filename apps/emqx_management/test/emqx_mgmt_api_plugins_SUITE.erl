%%--------------------------------------------------------------------
%% Copyright (c) 2020-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-define(EMQX_PLUGIN_TEMPLATE_VSN, "5.0-rc.1").
-define(PACKAGE_SUFFIX, ".tar.gz").

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    WorkDir = proplists:get_value(data_dir, Config),
    ok = filelib:ensure_dir(WorkDir),
    DemoShDir1 = string:replace(WorkDir, "emqx_mgmt_api_plugins", "emqx_plugins"),
    DemoShDir = string:replace(DemoShDir1, "emqx_management", "emqx_plugins"),
    OrigInstallDir = emqx_plugins:get_config(install_dir, undefined),
    ok = filelib:ensure_dir(DemoShDir),
    emqx_mgmt_api_test_util:init_suite([emqx_conf, emqx_plugins]),
    emqx_plugins:put_config(install_dir, DemoShDir),

    [{demo_sh_dir, DemoShDir}, {orig_install_dir, OrigInstallDir} | Config].

end_per_suite(Config) ->
    emqx_common_test_helpers:boot_modules(all),
    %% restore config
    case proplists:get_value(orig_install_dir, Config) of
        undefined -> ok;
        OrigInstallDir -> emqx_plugins:put_config(install_dir, OrigInstallDir)
    end,
    emqx_mgmt_api_test_util:end_suite([emqx_plugins, emqx_conf]),
    ok.

todo_t_plugins(Config) ->
    DemoShDir = proplists:get_value(demo_sh_dir, Config),
    PackagePath = build_demo_plugin_package(DemoShDir),
    ct:pal("package_location:~p install dir:~p", [PackagePath, emqx_plugins:install_dir()]),
    NameVsn = filename:basename(PackagePath, ?PACKAGE_SUFFIX),
    ok = emqx_plugins:delete_package(NameVsn),
    ok = install_plugin(PackagePath),
    {ok, StopRes} = describe_plugins(NameVsn),
    ?assertMatch(
        #{
            <<"running_status">> := [
                #{<<"node">> := <<"test@127.0.0.1">>, <<"status">> := <<"stopped">>}
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
                #{<<"node">> := <<"test@127.0.0.1">>, <<"status">> := <<"running">>}
            ]
        },
        StartRes
    ),
    {ok, []} = update_plugin(NameVsn, "stop"),
    {ok, StopRes2} = describe_plugins(NameVsn),
    ?assertMatch(
        #{
            <<"running_status">> := [
                #{<<"node">> := <<"test@127.0.0.1">>, <<"status">> := <<"stopped">>}
            ]
        },
        StopRes2
    ),
    {ok, []} = uninstall_plugin(NameVsn),
    ok.

list_plugins() ->
    Path = emqx_mgmt_api_test_util:api_path(["plugins"]),
    case emqx_mgmt_api_test_util:request_api(get, Path) of
        {ok, Apps} -> {ok, emqx_json:decode(Apps, [return_maps])};
        Error -> Error
    end.

describe_plugins(Name) ->
    Path = emqx_mgmt_api_test_util:api_path(["plugins", Name]),
    case emqx_mgmt_api_test_util:request_api(get, Path) of
        {ok, Res} -> {ok, emqx_json:decode(Res, [return_maps])};
        Error -> Error
    end.

install_plugin(FilePath) ->
    {ok, Token} = emqx_dashboard_admin:sign_token(<<"admin">>, <<"public">>),
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
        {ok, {{"HTTP/1.1", 200, "OK"}, _Headers, <<>>}} -> ok;
        Error -> Error
    end.

update_plugin(Name, Action) ->
    Path = emqx_mgmt_api_test_util:api_path(["plugins", Name, Action]),
    emqx_mgmt_api_test_util:request_api(put, Path).

update_boot_order(Name, MoveBody) ->
    Auth = emqx_mgmt_api_test_util:auth_header_(),
    Path = emqx_mgmt_api_test_util:api_path(["plugins", Name, "move"]),
    case emqx_mgmt_api_test_util:request_api(post, Path, "", Auth, MoveBody) of
        {ok, Res} -> {ok, emqx_json:decode(Res, [return_maps])};
        Error -> Error
    end.

uninstall_plugin(Name) ->
    DeletePath = emqx_mgmt_api_test_util:api_path(["plugins", Name]),
    emqx_mgmt_api_test_util:request_api(delete, DeletePath).

build_demo_plugin_package(Dir) ->
    #{package := Pkg} = emqx_plugins_SUITE:build_demo_plugin_package(),
    FileName = "emqx_plugin_template-" ++ ?EMQX_PLUGIN_TEMPLATE_VSN ++ ?PACKAGE_SUFFIX,
    PluginPath = "./" ++ FileName,
    Pkg = filename:join([Dir, FileName]),
    _ = os:cmd("cp " ++ Pkg ++ " " ++ PluginPath),
    true = filelib:is_regular(PluginPath),
    PluginPath.
