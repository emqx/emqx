%%--------------------------------------------------------------------
%% Copyright (c) 2019-2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

all() -> emqx_ct:all(?MODULE).

init_per_suite(Config) ->

    %% Compile extra plugin code

    DataPath = proplists:get_value(data_dir, Config),
    AppPath = filename:join([DataPath, "emqx_mini_plugin"]),
    HoconPath = filename:join([DataPath, "emqx_hocon_plugin"]),
    Cmd = lists:flatten(io_lib:format("cd ~s && make", [AppPath])),
    CmdPath = lists:flatten(io_lib:format("cd ~s && make", [HoconPath])),

    ct:pal("Executing ~s~n", [Cmd]),
    ct:pal("~n ~s~n", [os:cmd(Cmd)]),

    ct:pal("Executing ~s~n", [CmdPath]),
    ct:pal("~n ~s~n", [os:cmd(CmdPath)]),

    put(loaded_file, filename:join([DataPath, "loaded_plugins"])),
    emqx_ct_helpers:boot_modules([]),
    emqx_ct_helpers:start_apps([], fun(_) -> set_special_cfg(DataPath) end),

    Config.

set_special_cfg(PluginsDir) ->
    application:set_env(emqx, plugins_loaded_file, get(loaded_file)),
    application:set_env(emqx, expand_plugins_dir, PluginsDir),
    ok.

end_per_suite(_Config) ->
    emqx_ct_helpers:stop_apps([]).

t_load(_) ->
    ?assertEqual(ok, emqx_plugins:load()),
    ?assertEqual(ok, emqx_plugins:unload()),

    ?assertEqual({error, not_found}, emqx_plugins:load(not_existed_plugin)),
    ?assertEqual({error, not_started}, emqx_plugins:unload(emqx_mini_plugin)),
    ?assertEqual({error, not_started}, emqx_plugins:unload(emqx_hocon_plugin)),

    application:set_env(emqx, expand_plugins_dir, undefined),
    application:set_env(emqx, plugins_loaded_file, undefined).

t_load_ext_plugin(_) ->
    ?assertError({plugin_app_file_not_found, _},
                 emqx_plugins:load_ext_plugin("./not_existed_path/")).

t_list(_) ->
    ?assertMatch([{plugin, _, _, _, _, _, _} | _ ], emqx_plugins:list()).

t_find_plugin(_) ->
    ?assertMatch({plugin, emqx_mini_plugin, _, _, _, _, _}, emqx_plugins:find_plugin(emqx_mini_plugin)),
    ?assertMatch({plugin, emqx_hocon_plugin, _, _, _, _, _}, emqx_plugins:find_plugin(emqx_hocon_plugin)).

t_plugin(_) ->
    try
        emqx_plugins:plugin(not_existed_plugin)
    catch
        _Error:Reason:_Stacktrace ->
            ?assertEqual({plugin_not_found,not_existed_plugin}, Reason)
    end,
    ?assertMatch({plugin, emqx_mini_plugin, _, _, _, _, _}, emqx_plugins:plugin(emqx_mini_plugin)),
    ?assertMatch({plugin, emqx_hocon_plugin, _, _, _, _, _}, emqx_plugins:plugin(emqx_hocon_plugin)).

t_load_plugin(_) ->
    ok = meck:new(application, [unstick, non_strict, passthrough, no_history]),
    ok = meck:expect(application, load, fun(already_loaded_app) -> {error, {already_loaded, already_loaded_app}};
                                           (error_app) -> {error, error};
                                           (_) -> ok end),
    ok = meck:expect(application, ensure_all_started, fun(already_loaded_app) -> {error, {already_loaded_app, already_loaded}};
                                                         (error_app) -> {error, error};
                                                         (App) -> {ok, App} end),
    ok = meck:new(emqx_plugins, [unstick, non_strict, passthrough, no_history]),
    ok = meck:expect(emqx_plugins, generate_configs, fun(_) -> ok end),
    ok = meck:expect(emqx_plugins, apply_configs, fun(_) -> ok end),
    ?assertMatch({error, _}, emqx_plugins:load_plugin(already_loaded_app)),
    ?assertMatch(ok, emqx_plugins:load_plugin(normal)),
    ?assertMatch({error,_}, emqx_plugins:load_plugin(error_app)),

    ok = meck:unload(emqx_plugins),
    ok = meck:unload(application).

t_unload_plugin(_) ->
    ok = meck:new(application, [unstick, non_strict, passthrough, no_history]),
    ok = meck:expect(application, stop, fun(not_started_app) -> {error, {not_started, not_started_app}};
                                           (error_app) -> {error, error};
                                           (_) -> ok end),

    ?assertEqual(ok, emqx_plugins:unload_plugin(not_started_app)),
    ?assertEqual(ok, emqx_plugins:unload_plugin(normal)),
    ?assertEqual({error,error}, emqx_plugins:unload_plugin(error_app)),

    ok = meck:unload(application).
