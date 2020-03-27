%%--------------------------------------------------------------------
%% Copyright (c) 2020 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_plugins).

-include("emqx.hrl").
-include("logger.hrl").

-logger_header("[Plugins]").

-export([init/0]).

-export([ load/0
        , load/1
        , unload/0
        , unload/1
        , reload/1
        , list/0
        , find_plugin/1
        , generate_configs/1
        , apply_configs/1
        ]).

-ifdef(TEST).
-compile(export_all).
-compile(nowarn_export_all).
-endif.
%%--------------------------------------------------------------------
%% APIs
%%--------------------------------------------------------------------

%% @doc Init plugins' config
-spec(init() -> ok).
init() ->
    case emqx:get_env(plugins_etc_dir) of
        undefined  -> ok;
        PluginsEtc ->
            CfgFiles = [filename:join(PluginsEtc, File) ||
                        File <- filelib:wildcard("*.config", PluginsEtc)],
            lists:foreach(fun init_config/1, CfgFiles)
    end.

%% @doc Load all plugins when the broker started.
-spec(load() -> list() | {error, term()}).
load() ->
    load_expand_plugins(),
    case emqx:get_env(plugins_loaded_file) of
        undefined -> ignore; %% No plugins available
        File ->
            ensure_file(File),
            with_loaded_file(File, fun(Names) -> load_plugins(Names, false) end)
    end.

%% @doc Load a Plugin
-spec(load(atom()) -> ok | {error, term()}).
load(PluginName) when is_atom(PluginName) ->
    case {lists:member(PluginName, names(plugin)), lists:member(PluginName, names(started_app))} of
        {false, _} ->
            ?LOG(alert, "Plugin ~s not found, cannot load it", [PluginName]),
            {error, not_found};
        {_, true} ->
            ?LOG(notice, "Plugin ~s is already started", [PluginName]),
            {error, already_started};
        {_, false} ->
            load_plugin(PluginName, true)
    end.

%% @doc Unload all plugins before broker stopped.
-spec(unload() -> list() | {error, term()}).
unload() ->
    case emqx:get_env(plugins_loaded_file) of
        undefined -> ignore;
        File ->
            with_loaded_file(File, fun stop_plugins/1)
    end.

%% @doc UnLoad a Plugin
-spec(unload(atom()) -> ok | {error, term()}).
unload(PluginName) when is_atom(PluginName) ->
    case {lists:member(PluginName, names(plugin)), lists:member(PluginName, names(started_app))} of
        {false, _} ->
            ?LOG(error, "Plugin ~s is not found, cannot unload it", [PluginName]),
            {error, not_found};
        {_, false} ->
            ?LOG(error, "Plugin ~s is not started", [PluginName]),
            {error, not_started};
        {_, _} ->
            unload_plugin(PluginName, true)            
    end.

reload(PluginName) when is_atom(PluginName)->
    case {lists:member(PluginName, names(plugin)), lists:member(PluginName, names(started_app))} of
        {false, _} ->
            ?LOG(error, "Plugin ~s is not found, cannot reload it", [PluginName]),
            {error, not_found};
        {_, false} ->
            load(PluginName);
        {_, true} ->
            case unload(PluginName) of
                ok -> load(PluginName);
                {error, Reason} -> {error, Reason}
            end
    end.

%% @doc List all available plugins
-spec(list() -> [emqx_types:plugin()]).
list() ->
    StartedApps = names(started_app),
    lists:map(fun({Name, _, [Type| _]}) ->
        Plugin = plugin(Name, Type),
        case lists:member(Name, StartedApps) of
            true  -> Plugin#plugin{active = true};
            false -> Plugin
        end
    end, lists:sort(ekka_boot:all_module_attributes(emqx_plugin))).

find_plugin(Name) ->
    find_plugin(Name, list()).

find_plugin(Name, Plugins) ->
    lists:keyfind(Name, 2, Plugins).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

init_config(CfgFile) ->
    {ok, [AppsEnv]} = file:consult(CfgFile),
    lists:foreach(fun({App, Envs}) ->
                      [application:set_env(App, Par, Val) || {Par, Val} <- Envs]
                  end, AppsEnv).

load_expand_plugins() ->
    case emqx:get_env(expand_plugins_dir) of
        undefined -> ok;
        ExpandPluginsDir ->
            Plugins = filelib:wildcard("*", ExpandPluginsDir),
            lists:foreach(fun(Plugin) ->
                PluginDir = filename:join(ExpandPluginsDir, Plugin),
                case filelib:is_dir(PluginDir) of
                    true  -> load_expand_plugin(PluginDir);
                    false -> ok
                end
            end, Plugins)
    end.

load_expand_plugin(PluginDir) ->
    init_expand_plugin_config(PluginDir),
    Ebin = filename:join([PluginDir, "ebin"]),
    code:add_patha(Ebin),
    Modules = filelib:wildcard(filename:join([Ebin, "*.beam"])),
    lists:foreach(fun(Mod) ->
        Module = list_to_atom(filename:basename(Mod, ".beam")),
        code:load_file(Module)
    end, Modules),
    case filelib:wildcard(Ebin ++ "/*.app") of
        [App|_] -> application:load(list_to_atom(filename:basename(App, ".app")));
        _ -> ?LOG(alert, "Plugin not found."),
             {error, load_app_fail}
    end.

init_expand_plugin_config(PluginDir) ->
    Priv = PluginDir ++ "/priv",
    Etc  = PluginDir ++ "/etc",
    Schema = filelib:wildcard(Priv ++ "/*.schema"),
    Conf = case filelib:wildcard(Etc ++ "/*.conf") of
        [] -> [];
        [Conf1] -> cuttlefish_conf:file(Conf1)
    end,
    AppsEnv = cuttlefish_generator:map(cuttlefish_schema:files(Schema), Conf),
    lists:foreach(fun({AppName, Envs}) ->
        [application:set_env(AppName, Par, Val) || {Par, Val} <- Envs]
    end, AppsEnv).

ensure_file(File) ->
    case filelib:is_file(File) of false -> write_loaded([]); true -> ok end.

with_loaded_file(File, SuccFun) ->
    case read_loaded(File) of
        {ok, Names0} ->
            Names = filter_plugins(Names0),
            SuccFun(Names);
        {error, Error} ->
            ?LOG(alert, "Failed to read: ~p, error: ~p", [File, Error]),
            {error, Error}
    end.

filter_plugins(Names) ->
    lists:filtermap(fun(Name1) when is_atom(Name1) -> {true, Name1};
                       ({Name1, true}) -> {true, Name1};
                       ({_Name1, false}) -> false
                    end, Names).

load_plugins(Names, Persistent) ->
    Plugins = list(), NotFound = Names -- names(Plugins),
    case NotFound of
        []       -> ok;
        NotFound -> ?LOG(alert, "Cannot find plugins: ~p", [NotFound])
    end,
    NeedToLoad = Names -- NotFound -- names(started_app),
    lists:foreach(fun(Name) ->
                      Plugin = find_plugin(Name, Plugins),
                      load_plugin(Plugin#plugin.name, Persistent)
                  end, NeedToLoad).

generate_configs(App) ->
    ConfigFile = filename:join([emqx:get_env(plugins_etc_dir), App]) ++ ".config",
    ConfFile = filename:join([emqx:get_env(plugins_etc_dir), App]) ++ ".conf",
    SchemaFile = filename:join([code:priv_dir(App), App]) ++ ".schema",
    case {filelib:is_file(ConfigFile), filelib:is_file(ConfFile) andalso filelib:is_file(SchemaFile)} of
        {true, _} ->
            {ok, [Configs]} = file:consult(ConfigFile),
            Configs;
        {_, true} ->
            Schema = cuttlefish_schema:files([SchemaFile]),
            Conf = cuttlefish_conf:file(ConfFile),
            cuttlefish_generator:map(Schema, Conf);
        {false, false} ->
            error(no_avaliable_configuration)
    end.

apply_configs([]) ->
    ok;
apply_configs([{App, Config} | More]) ->
    lists:foreach(fun({Key, _}) -> application:unset_env(App, Key) end, application:get_all_env(App)),
    lists:foreach(fun({Key, Val}) -> application:set_env(App, Key, Val) end, Config),
    apply_configs(More).

%% Stop plugins
stop_plugins(Names) ->
    [stop_app(App) || App <- Names],
    ok.

plugin(AppName, Type) ->
    case application:get_all_key(AppName) of
        {ok, Attrs} ->
            Descr = proplists:get_value(description, Attrs, ""),
            #plugin{name = AppName, descr = Descr, type = plugin_type(Type)};
        undefined -> error({plugin_not_found, AppName})
    end.

load_plugin(Name, Persistent) ->
    try
        Configs = ?MODULE:generate_configs(Name),
        ?MODULE:apply_configs(Configs),
        case load_app(Name) of
            ok ->
                start_app(Name, fun(App) -> plugin_loaded(App, Persistent) end);
            {error, Error0} ->
                {error, Error0}
        end
    catch _ : Error : Stacktrace ->
        ?LOG(alert, "Plugin ~s load failed with ~p", [Name, {Error, Stacktrace}]),
        {error, parse_config_file_failed}
    end.

load_app(App) ->
    case application:load(App) of
        ok ->
            ok;
        {error, {already_loaded, App}} ->
            ok;
        {error, Error} ->
            {error, Error}
    end.

start_app(App, SuccFun) ->
    case application:ensure_all_started(App) of
        {ok, Started} ->
            ?LOG(info, "Started plugins: ~p", [Started]),
            ?LOG(info, "Load plugin ~s successfully", [App]),
            SuccFun(App),
            ok;
        {error, {ErrApp, Reason}} ->
            ?LOG(error, "Load plugin ~s failed, cannot start plugin ~s for ~0p", [App, ErrApp, Reason]),
            {error, {ErrApp, Reason}}
    end.

unload_plugin(App, Persistent) ->
    case stop_app(App) of
        ok ->
            plugin_unloaded(App, Persistent), ok;
        {error, Reason} ->
            {error, Reason}
    end.

stop_app(App) ->
    case application:stop(App) of
        ok ->
            ?LOG(info, "Stop plugin ~s successfully", [App]), ok;
        {error, {not_started, App}} ->
            ?LOG(error, "Plugin ~s is not started", [App]), ok;
        {error, Reason} ->
            ?LOG(error, "Stop plugin ~s error: ~p", [App]), {error, Reason}
    end.

names(plugin) ->
    names(list());

names(started_app) ->
    [Name || {Name, _Descr, _Ver} <- application:which_applications()];

names(Plugins) ->
    [Name || #plugin{name = Name} <- Plugins].

plugin_loaded(_Name, false) ->
    ok;
plugin_loaded(Name, true) ->
    case read_loaded() of
        {ok, Names} ->
            case lists:member(Name, Names) of
                false ->
                    %% write file if plugin is loaded
                    write_loaded(lists:append(Names, [{Name, true}]));
                true ->
                    ignore
            end;
        {error, Error} ->
            ?LOG(error, "Cannot read loaded plugins: ~p", [Error])
    end.

plugin_unloaded(_Name, false) ->
    ok;
plugin_unloaded(Name, true) ->
    case read_loaded() of
        {ok, Names0} ->
            Names = filter_plugins(Names0),
            case lists:member(Name, Names) of
                true ->
                    write_loaded(lists:delete(Name, Names));
                false ->
                    ?LOG(error, "Cannot find ~s in loaded_file", [Name])
            end;
        {error, Error} ->
            ?LOG(error, "Cannot read loaded_plugins: ~p", [Error])
    end.

read_loaded() ->
    case emqx:get_env(plugins_loaded_file) of
        undefined -> {error, not_found};
        File      -> read_loaded(File)
    end.

read_loaded(File) -> file:consult(File).

write_loaded(AppNames) ->
    FilePath = emqx:get_env(plugins_loaded_file),
    case file:write_file(FilePath, [io_lib:format("~p.~n", [Name]) || Name <- AppNames]) of
        ok -> ok;
        {error, Error} ->
            ?LOG(error, "Write File ~p Error: ~p", [FilePath, Error]),
            {error, Error}
    end.

plugin_type(auth) -> auth;
plugin_type(protocol) -> protocol;
plugin_type(backend) -> backend;
plugin_type(bridge) -> bridge;
plugin_type(_) -> feature.
