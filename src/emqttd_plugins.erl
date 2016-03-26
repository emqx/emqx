%%--------------------------------------------------------------------
%% Copyright (c) 2012-2016 Feng Lee <feng@emqtt.io>.
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

-module(emqttd_plugins).

-include("emqttd.hrl").

-export([load/0, unload/0]).

-export([load/1, unload/1]).

-export([list/0]).

%% @doc Load all plugins when the broker started.
-spec(load() -> list() | {error, any()}).
load() ->
    case env(loaded_file) of
        {ok, File} ->
            with_loaded_file(File, fun(Names) -> load_plugins(Names, false) end);
        undefined ->
            %% No plugins available
            ignore
    end.

with_loaded_file(File, SuccFun) ->
    case read_loaded(File) of
        {ok, Names} ->
            SuccFun(Names);
        {error, Error} ->
            lager:error("Failed to read: ~p, error: ~p", [File, Error]),
            {error, Error}
    end.

load_plugins(Names, Persistent) ->
    Plugins = list(), NotFound = Names -- names(Plugins),
    case NotFound of
        []       -> ok;
        NotFound -> lager:error("Cannot find plugins: ~p", [NotFound])
    end,
    NeedToLoad = Names -- NotFound -- names(started_app),
    [load_plugin(find_plugin(Name, Plugins), Persistent) || Name <- NeedToLoad].

%% @doc Unload all plugins before broker stopped.
-spec(unload() -> list() | {error, any()}).
unload() ->
    case env(loaded_file) of
        {ok, File} ->
            with_loaded_file(File, fun stop_plugins/1);
        undefined ->
            ignore
    end.

%% stop plugins
stop_plugins(Names) ->
    [stop_app(App) || App <- Names].

%% @doc List all available plugins
-spec(list() -> [mqtt_plugin()]).
list() ->
    case env(plugins_dir) of
        {ok, PluginsDir} -> 
            AppFiles = filelib:wildcard("*/ebin/*.app", PluginsDir),
            Plugins = [plugin(PluginsDir, AppFile) || AppFile <- AppFiles],
            StartedApps = names(started_app),
            lists:map(fun(Plugin = #mqtt_plugin{name = Name}) ->
                          case lists:member(Name, StartedApps) of
                              true  -> Plugin#mqtt_plugin{active = true};
                              false -> Plugin
                          end
                      end, Plugins);
        undefined ->
            []
    end.

plugin(PluginsDir, AppFile0) ->
    AppFile = filename:join(PluginsDir, AppFile0),
    {ok, [{application, Name, Attrs}]} = file:consult(AppFile),
    CfgFile = filename:join([PluginsDir, Name, "etc/plugin.config"]),
    AppsEnv1 =
    case filelib:is_file(CfgFile) of
        true ->
            {ok, [AppsEnv]} = file:consult(CfgFile),
            AppsEnv;
        false ->
            []
    end,
    Ver = proplists:get_value(vsn, Attrs, "0"),
    Descr = proplists:get_value(description, Attrs, ""),
    #mqtt_plugin{name = Name, version = Ver, config = AppsEnv1, descr = Descr}.

%% @doc Load a Plugin
-spec(load(atom()) -> ok | {error, any()}).
load(PluginName) when is_atom(PluginName) ->
    case lists:member(PluginName, names(started_app)) of
        true ->
            lager:error("Plugin ~p is already started", [PluginName]),
            {error, already_started};
        false ->
            case find_plugin(PluginName) of
                false ->
                    lager:error("Plugin ~s not found", [PluginName]),
                    {error, not_found};
                Plugin ->
                    load_plugin(Plugin, true)
            end
    end.

load_plugin(#mqtt_plugin{name = Name, config = Config}, Persistent) ->
    case load_app(Name, Config) of
        ok ->
            start_app(Name, fun(App) -> plugin_loaded(App, Persistent) end);
        {error, Error} ->
            {error, Error}
    end.

load_app(App, Config) ->
    case application:load(App) of
        ok ->
            set_config(Config);
        {error, {already_loaded, App}} ->
            set_config(Config);
        {error, Error} ->
            {error, Error}
    end.

%% This trick is awesome:)
set_config([]) ->
    ok;
set_config([{AppName, Envs} | Config]) ->
    [application:set_env(AppName, Par, Val) || {Par, Val} <- Envs],
    set_config(Config).

start_app(App, SuccFun) ->
    case application:ensure_all_started(App) of
        {ok, Started} ->
            lager:info("started Apps: ~p", [Started]),
            lager:info("load plugin ~p successfully", [App]),
            SuccFun(App),
            {ok, Started};
        {error, {ErrApp, Reason}} ->
            lager:error("load plugin ~p error, cannot start app ~s for ~p", [App, ErrApp, Reason]),
            {error, {ErrApp, Reason}}
    end.

find_plugin(Name) ->
    find_plugin(Name, list()).

find_plugin(Name, Plugins) ->
    lists:keyfind(Name, 2, Plugins). 

%% @doc UnLoad a Plugin
-spec(unload(atom()) -> ok | {error, any()}).
unload(PluginName) when is_atom(PluginName) ->
    case {lists:member(PluginName, names(started_app)), lists:member(PluginName, names(plugin))} of
        {true, true} ->
            unload_plugin(PluginName, true);
        {false, _} ->
            lager:error("Plugin ~p is not started", [PluginName]),
            {error, not_started};
        {true, false} ->
            lager:error("~s is not a plugin, cannot unload it", [PluginName]),
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
            lager:info("stop plugin ~p successfully~n", [App]), ok;
        {error, {not_started, App}} ->
            lager:error("plugin ~p is not started~n", [App]), ok;
        {error, Reason} ->
            lager:error("stop plugin ~p error: ~p", [App]), {error, Reason}
    end.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

names(plugin) ->
    names(list());

names(started_app) ->
    [Name || {Name, _Descr, _Ver} <- application:which_applications()];

names(Plugins) ->
    [Name || #mqtt_plugin{name = Name} <- Plugins].

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
            lager:error("Cannot read loaded plugins: ~p", [Error])
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
                    lager:error("Cannot find ~s in loaded_file", [Name])
            end;
        {error, Error} ->
            lager:error("Cannot read loaded_plugins: ~p", [Error])
    end.

read_loaded() ->
    {ok, File} = env(loaded_file),
    read_loaded(File). 

read_loaded(File) ->
    file:consult(File).

write_loaded(AppNames) ->
    {ok, File} = env(loaded_file),
    case file:open(File, [binary, write]) of
        {ok, Fd} ->
            lists:foreach(fun(Name) ->
                file:write(Fd, iolist_to_binary(io_lib:format("~s.~n", [Name])))
            end, AppNames);
        {error, Error} ->
            lager:error("Open File ~p Error: ~p", [File, Error]),
            {error, Error}
    end.

env(Name) ->
    case application:get_env(emqttd, plugins) of
        {ok, PluginsEnv} ->
            case proplists:get_value(Name, PluginsEnv) of
                undefined ->
                    undefined;
                Val ->
                    {ok, Val}
            end;
        undefined ->
            undefined
    end.

