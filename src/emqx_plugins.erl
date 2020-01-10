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
        , list/0
        , find_plugin/1
        , load_expand_plugin/1
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

init_config(CfgFile) ->
    {ok, [AppsEnv]} = file:consult(CfgFile),
    lists:foreach(fun({App, Envs}) ->
                      [application:set_env(App, Par, Val) || {Par, Val} <- Envs]
                  end, AppsEnv).

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
    [load_plugin(find_plugin(Name, Plugins), Persistent) || Name <- NeedToLoad].

%% @doc Unload all plugins before broker stopped.
-spec(unload() -> list() | {error, term()}).
unload() ->
    case emqx:get_env(plugins_loaded_file) of
        undefined -> ignore;
        File ->
            with_loaded_file(File, fun stop_plugins/1)
    end.

%% Stop plugins
stop_plugins(Names) ->
    [stop_app(App) || App <- Names].

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

plugin(AppName, Type) ->
    case application:get_all_key(AppName) of
        {ok, Attrs} ->
            Ver = proplists:get_value(vsn, Attrs, "0"),
            Descr = proplists:get_value(description, Attrs, ""),
            #plugin{name = AppName, version = Ver, descr = Descr, type = plugin_type(Type)};
        undefined -> error({plugin_not_found, AppName})
    end.

%% @doc Load a Plugin
-spec(load(atom()) -> ok | {error, term()}).
load(PluginName) when is_atom(PluginName) ->
    case lists:member(PluginName, names(started_app)) of
        true ->
            ?LOG(notice, "Plugin ~s is already started", [PluginName]),
            {error, already_started};
        false ->
            case find_plugin(PluginName) of
                false ->
                    ?LOG(alert, "Plugin ~s not found", [PluginName]),
                    {error, not_found};
                Plugin ->
                    load_plugin(Plugin, true)
            end
    end.

load_plugin(#plugin{name = Name}, Persistent) ->
    case load_app(Name) of
        ok ->
            start_app(Name, fun(App) -> plugin_loaded(App, Persistent) end);
        {error, Error} ->
            {error, Error}
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
            ?LOG(error, "Load plugin ~s failed, cannot start plugin ~s for ~p", [App, ErrApp, Reason]),
            {error, {ErrApp, Reason}}
    end.

find_plugin(Name) ->
    find_plugin(Name, list()).

find_plugin(Name, Plugins) ->
    lists:keyfind(Name, 2, Plugins).

%% @doc UnLoad a Plugin
-spec(unload(atom()) -> ok | {error, term()}).
unload(PluginName) when is_atom(PluginName) ->
    case {lists:member(PluginName, names(started_app)), lists:member(PluginName, names(plugin))} of
        {true, true} ->
            unload_plugin(PluginName, true);
        {false, _} ->
            ?LOG(error, "Plugin ~s is not started", [PluginName]),
            {error, not_started};
        {true, false} ->
            ?LOG(error, "~s is not a plugin, cannot unload it", [PluginName]),
            {error, not_found}
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

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------
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
