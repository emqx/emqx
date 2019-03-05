%% Copyright (c) 2013-2019 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_plugins).

-include("emqx.hrl").

-export([init/0]).

-export([load/0, unload/0]).

-export([load/1, unload/1]).

-export([list/0]).

-export([load_expand_plugin/1]).

%% @doc Init plugins' config
-spec(init() -> ok).
init() ->
    case emqx_config:get_env(plugins_etc_dir) of
        undefined  -> ok;
        PluginsEtc ->
            CfgFiles = [filename:join(PluginsEtc, File) ||
                        File <- filelib:wildcard("*.config", PluginsEtc)],
            lists:foreach(fun init_config/1, CfgFiles)
    end.

init_config(CfgFile) ->
    {ok, [AppsEnv]} = file:consult(CfgFile),
    lists:foreach(fun({AppName, Envs}) ->
                      [application:set_env(AppName, Par, Val) || {Par, Val} <- Envs]
                  end, AppsEnv).

%% @doc Load all plugins when the broker started.
-spec(load() -> list() | {error, term()}).
load() ->
    load_expand_plugins(),
    case emqx_config:get_env(plugins_loaded_file) of
        undefined -> %% No plugins available
            ignore;
        File ->
            ensure_file(File),
            with_loaded_file(File, fun(Names) -> load_plugins(Names, false) end)
    end.

load_expand_plugins() ->
    case emqx_config:get_env(expand_plugins_dir) of
        undefined -> ok;
        Dir ->
            PluginsDir = filelib:wildcard("*", Dir),
            lists:foreach(fun(PluginDir) ->
                case filelib:is_dir(Dir ++ PluginDir) of
                    true  -> load_expand_plugin(Dir ++ PluginDir);
                    false -> ok
                end
            end, PluginsDir)
    end.

load_expand_plugin(PluginDir) ->
    init_expand_plugin_config(PluginDir),
    Ebin = PluginDir ++ "/ebin",
    code:add_patha(Ebin),
    Modules = filelib:wildcard(Ebin ++ "/*.beam"),
    lists:foreach(fun(Mod) ->
        Module = list_to_atom(filename:basename(Mod, ".beam")),
        code:load_file(Module)
    end, Modules),
    case filelib:wildcard(Ebin ++ "/*.app") of
        [App|_] -> application:load(list_to_atom(filename:basename(App, ".app")));
        _ -> emqx_logger:error("App file cannot be found."),
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

get_expand_plugin_config() ->
    case emqx_config:get_env(expand_plugins_dir) of
        undefined -> ok;
        Dir ->
            PluginsDir = filelib:wildcard("*", Dir),
            lists:foldl(fun(PluginDir, Acc) ->
                case filelib:is_dir(Dir ++ PluginDir) of
                    true  ->
                        Etc  = Dir ++ PluginDir ++ "/etc",
                        case filelib:wildcard("*.{conf,config}", Etc) of
                            [] -> Acc;
                            [Conf] -> [Conf | Acc]
                        end;
                    false ->
                        Acc
                end
            end, [], PluginsDir)
    end.

ensure_file(File) ->
    case filelib:is_file(File) of false -> write_loaded([]); true -> ok end.

with_loaded_file(File, SuccFun) ->
    case read_loaded(File) of
        {ok, Names} ->
            SuccFun(Names);
        {error, Error} ->
            emqx_logger:error("[Plugins] Failed to read: ~p, error: ~p", [File, Error]),
            {error, Error}
    end.

load_plugins(Names, Persistent) ->
    Plugins = list(), NotFound = Names -- names(Plugins),
    case NotFound of
        []       -> ok;
        NotFound -> emqx_logger:error("[Plugins] Cannot find plugins: ~p", [NotFound])
    end,
    NeedToLoad = Names -- NotFound -- names(started_app),
    [load_plugin(find_plugin(Name, Plugins), Persistent) || Name <- NeedToLoad].

%% @doc Unload all plugins before broker stopped.
-spec(unload() -> list() | {error, term()}).
unload() ->
    case emqx_config:get_env(plugins_loaded_file) of
        undefined ->
            ignore;
        File ->
            with_loaded_file(File, fun stop_plugins/1)
    end.

%% Stop plugins
stop_plugins(Names) ->
    [stop_app(App) || App <- Names].

%% @doc List all available plugins
-spec(list() -> [emqx_types:plugin()]).
list() ->
    case emqx_config:get_env(plugins_etc_dir) of
        undefined ->
            [];
        PluginsEtc ->
            CfgFiles = filelib:wildcard("*.{conf,config}", PluginsEtc) ++ get_expand_plugin_config(),
            Plugins = [plugin(CfgFile) || CfgFile <- CfgFiles],
            StartedApps = names(started_app),
            lists:map(fun(Plugin = #plugin{name = Name}) ->
                          case lists:member(Name, StartedApps) of
                              true  -> Plugin#plugin{active = true};
                              false -> Plugin
                          end
                      end, Plugins)
    end.

plugin(CfgFile) ->
    AppName = app_name(CfgFile),
    case application:get_all_key(AppName) of
        {ok, Attrs} ->
            Ver = proplists:get_value(vsn, Attrs, "0"),
            Descr = proplists:get_value(description, Attrs, ""),
            #plugin{name = AppName, version = Ver, descr = Descr};
        undefined -> error({plugin_not_found, AppName})
    end.

%% @doc Load a Plugin
-spec(load(atom()) -> ok | {error, term()}).
load(PluginName) when is_atom(PluginName) ->
    case lists:member(PluginName, names(started_app)) of
        true ->
            emqx_logger:error("[Plugins] Plugin ~s is already started", [PluginName]),
            {error, already_started};
        false ->
            case find_plugin(PluginName) of
                false ->
                    emqx_logger:error("[Plugins] Plugin ~s not found", [PluginName]),
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
            emqx_logger:info("Started Apps: ~p", [Started]),
            emqx_logger:info("Load plugin ~s successfully", [App]),
            SuccFun(App),
            {ok, Started};
        {error, {ErrApp, Reason}} ->
            emqx_logger:error("Load plugin ~s error, cannot start app ~s for ~p", [App, ErrApp, Reason]),
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
            emqx_logger:error("Plugin ~s is not started", [PluginName]),
            {error, not_started};
        {true, false} ->
            emqx_logger:error("~s is not a plugin, cannot unload it", [PluginName]),
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
            emqx_logger:info("Stop plugin ~s successfully", [App]), ok;
        {error, {not_started, App}} ->
            emqx_logger:error("Plugin ~s is not started", [App]), ok;
        {error, Reason} ->
            emqx_logger:error("Stop plugin ~s error: ~p", [App]), {error, Reason}
    end.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

app_name(File) ->
    [AppName | _] = string:tokens(File, "."), list_to_atom(AppName).

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
                    write_loaded(lists:append(Names, [Name]));
                true ->
                    ignore
            end;
        {error, Error} ->
            emqx_logger:error("Cannot read loaded plugins: ~p", [Error])
    end.

plugin_unloaded(_Name, false) ->
    ok;
plugin_unloaded(Name, true) ->
    case read_loaded() of
        {ok, Names} ->
            case lists:member(Name, Names) of
                true ->
                    write_loaded(lists:delete(Name, Names));
                false ->
                    emqx_logger:error("Cannot find ~s in loaded_file", [Name])
            end;
        {error, Error} ->
            emqx_logger:error("Cannot read loaded_plugins: ~p", [Error])
    end.

read_loaded() ->
    case emqx_config:get_env(plugins_loaded_file) of
        undefined -> {error, not_found};
        File      -> read_loaded(File)
    end.

read_loaded(File) -> file:consult(File).

write_loaded(AppNames) ->
    File = emqx_config:get_env(plugins_loaded_file),
    case file:open(File, [binary, write]) of
        {ok, Fd} ->
            lists:foreach(fun(Name) ->
                file:write(Fd, iolist_to_binary(io_lib:format("~s.~n", [Name])))
            end, AppNames);
        {error, Error} ->
            emqx_logger:error("Open File ~p Error: ~p", [File, Error]),
            {error, Error}
    end.
