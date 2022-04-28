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
-include_lib("common_test/include/ct.hrl").

all() -> emqx_ct:all(?MODULE).

init_per_suite(Config) ->
    %% Compile extra plugin code

    DataPath = proplists:get_value(data_dir, Config),
    AppPath = filename:join([DataPath, "emqx_mini_plugin"]),
    Cmd = lists:flatten(io_lib:format("cd ~s && make", [AppPath])),

    ct:pal("Executing ~s~n", [Cmd]),
    ct:pal("~n ~s~n", [os:cmd(Cmd)]),

    put(loaded_file, filename:join([DataPath, "loaded_plugins"])),
    emqx_ct_helpers:boot_modules([]),
    emqx_ct_helpers:start_apps([], fun(_) -> set_special_cfg(DataPath) end),

    Config.

set_special_cfg(PluginsDir) ->
    application:set_env(emqx, plugins_loaded_file, get(loaded_file)),
    application:set_env(emqx, expand_plugins_dir, PluginsDir),
    ok.

end_per_suite(_Config) ->
    emqx_ct_helpers:stop_apps([]),
    file:delete(get(loaded_file)).

init_per_testcase(t_ensure_default_loaded_plugins_file, Config) ->
    {ok, LoadedPluginsFilepath} = application:get_env(emqx, plugins_loaded_file),
    TmpFilepath = filename:join(["/", "tmp", "loaded_plugins_tmp"]),
    case file:delete(TmpFilepath) of
        ok -> ok;
        {error, enoent} -> ok
    end,
    application:set_env(emqx, plugins_loaded_file, TmpFilepath),
    [ {loaded_plugins_filepath, LoadedPluginsFilepath}
    , {tmp_filepath, TmpFilepath}
    | Config];
init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(t_ensure_default_loaded_plugins_file, Config) ->
    LoadedPluginsFilepath = ?config(loaded_plugins_filepath, Config),
    TmpFilepath = ?config(tmp_filepath, Config),
    file:delete(TmpFilepath),
    emqx_plugins:unload(),
    application:set_env(emqx, plugins_loaded_file, LoadedPluginsFilepath),
    %% need to purge the plugin to avoid inter-testcase dependencies.
    code:purge(emqx_mini_plugin_app),
    ok;
end_per_testcase(_TestCase, _Config) ->
    emqx_plugins:unload(),
    ok.

t_load(_) ->
    ?assertEqual(ok, emqx_plugins:load()),
    ?assertEqual(ok, emqx_plugins:unload()),

    ?assertEqual({error, not_found}, emqx_plugins:load(not_existed_plugin)),
    ?assertEqual({error, not_started}, emqx_plugins:unload(emqx_mini_plugin)),

    application:set_env(emqx, expand_plugins_dir, undefined),
    application:set_env(emqx, plugins_loaded_file, undefined),
    ?assertEqual(ignore, emqx_plugins:load()),
    ?assertEqual(ignore, emqx_plugins:unload()).

t_ensure_default_loaded_plugins_file(Config) ->
    %% this will trigger it to write the default plugins to the
    %% inexistent file; but it won't truly load them in this test
    %% because there are no config files in `expand_plugins_dir'.
    TmpFilepath = ?config(tmp_filepath, Config),
    ok = emqx_plugins:load(),
    {ok, Contents} = file:consult(TmpFilepath),
    ?assertEqual(
       [ {emqx_bridge_mqtt, false}
       , {emqx_dashboard, true}
       , {emqx_eviction_agent, true}
       , {emqx_management, true}
       , {emqx_modules, false}
       , {emqx_node_rebalance, true}
       , {emqx_recon, true}
       , {emqx_retainer, true}
       , {emqx_rule_engine, true}
       , {emqx_telemetry, true}
       ],
       lists:sort(Contents)),
    ok.

t_init_config(_) ->
    ConfFile = "emqx_mini_plugin.config",
    Data = "[{emqx_mini_plugin,[{mininame ,test}]}].",
    file:write_file(ConfFile, list_to_binary(Data)),
    ?assertEqual(ok, emqx_plugins:init_config(ConfFile)),
    file:delete(ConfFile),
    ?assertEqual({ok,test}, application:get_env(emqx_mini_plugin, mininame)).

t_load_ext_plugin(_) ->
    ?assertError({plugin_app_file_not_found, _},
                 emqx_plugins:load_ext_plugin("./not_existed_path/")).

t_list(_) ->
    ?assertMatch([{plugin, _, _, _, _, _, _, _} | _ ], emqx_plugins:list()).

t_find_plugin(_) ->
    ?assertMatch({plugin, emqx_mini_plugin, _, _, _, _, _, _}, emqx_plugins:find_plugin(emqx_mini_plugin)).

t_plugin_type(_) ->
    ?assertEqual(auth, emqx_plugins:plugin_type(auth)),
    ?assertEqual(protocol, emqx_plugins:plugin_type(protocol)),
    ?assertEqual(backend, emqx_plugins:plugin_type(backend)),
    ?assertEqual(bridge, emqx_plugins:plugin_type(bridge)),
    ?assertEqual(feature, emqx_plugins:plugin_type(undefined)).

t_with_loaded_file(_) ->
    ?assertMatch({error, _}, emqx_plugins:with_loaded_file("./not_existed_path/", fun(_) -> ok end)).

t_plugin_loaded(_) ->
    ?assertEqual(ok, emqx_plugins:plugin_loaded(emqx_mini_plugin, false)),
    ?assertEqual(ok, emqx_plugins:plugin_loaded(emqx_mini_plugin, true)).

t_plugin_unloaded(_) ->
    ?assertEqual(ok, emqx_plugins:plugin_unloaded(emqx_mini_plugin, false)),
    ?assertEqual(ok, emqx_plugins:plugin_unloaded(emqx_mini_plugin, true)).

t_plugin(_) ->
    try
        emqx_plugins:plugin(not_existed_plugin, undefined)
    catch
        _Error:Reason:_Stacktrace ->
            ?assertEqual({plugin_not_found,not_existed_plugin}, Reason)
    end,
    ?assertMatch({plugin, emqx_mini_plugin, _, _, _, _, _, _}, emqx_plugins:plugin(emqx_mini_plugin, undefined)).

t_filter_plugins(_) ->
    ?assertEqual([name1, name2], emqx_plugins:filter_plugins([name1, {name2,true}, {name3, false}])).

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
    ?assertMatch({error, _}, emqx_plugins:load_plugin(already_loaded_app, true)),
    ?assertMatch(ok, emqx_plugins:load_plugin(normal, true)),
    ?assertMatch({error,_}, emqx_plugins:load_plugin(error_app, true)),

    ok = meck:unload(emqx_plugins),
    ok = meck:unload(application).

t_unload_plugin(_) ->
    ok = meck:new(application, [unstick, non_strict, passthrough, no_history]),
    ok = meck:expect(application, stop, fun(not_started_app) -> {error, {not_started, not_started_app}};
                                           (error_app) -> {error, error};
                                           (_) -> ok end),

    ?assertEqual(ok, emqx_plugins:unload_plugin(not_started_app, true)),
    ?assertEqual(ok, emqx_plugins:unload_plugin(normal, true)),
    ?assertEqual({error,error}, emqx_plugins:unload_plugin(error_app, true)),

    ok = meck:unload(application).
