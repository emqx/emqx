%%%-----------------------------------------------------------------------------
%%% Copyright (c) 2012-2015 eMQTT.IO, All Rights Reserved.
%%%
%%% Permission is hereby granted, free of charge, to any person obtaining a copy
%%% of this software and associated documentation files (the "Software"), to deal
%%% in the Software without restriction, including without limitation the rights
%%% to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
%%% copies of the Software, and to permit persons to whom the Software is
%%% furnished to do so, subject to the following conditions:
%%%
%%% The above copyright notice and this permission notice shall be included in all
%%% copies or substantial portions of the Software.
%%%
%%% THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
%%% IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
%%% FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
%%% AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
%%% LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
%%% OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
%%% SOFTWARE.
%%%-----------------------------------------------------------------------------
%%% @doc
%%% emqttd plugin manager.
%%%
%%% @end
%%%-----------------------------------------------------------------------------

-module(emqttd_plugin_mgr).

-author("Feng Lee <feng@emqtt.io>").

-include("emqttd.hrl").

-export([start/0, list/0, load/1, unload/1, stop/0]).

%%------------------------------------------------------------------------------
%% @doc Load all plugins
%% @end
%%------------------------------------------------------------------------------
-spec start() -> ok | {error, any()}.
start() ->
    case read_loaded() of
        {ok, AppNames} ->
            NotFound = AppNames -- apps(plugin),
            case NotFound of
                [] -> ok;
                NotFound -> lager:error("Cannot find plugins: ~p", [NotFound])
            end,
            {ok, start_apps(AppNames -- NotFound -- apps(started))};
        {error, Error} ->
            lager:error("Read loaded_plugins file error: ~p", [Error]),
            {error, Error}
    end.

%%------------------------------------------------------------------------------
%% @doc List all available plugins
%% @end
%%------------------------------------------------------------------------------
list() ->
    {ok, PluginEnv} = application:get_env(emqttd, plugins),
    PluginsDir = proplists:get_value(dir, PluginEnv, "./plugins"),
    AppFiles = filelib:wildcard("*/ebin/*.app", PluginsDir),
    Plugins = [plugin(filename:join(PluginsDir, AppFile)) || AppFile <- AppFiles],
    StartedApps = [Name || {Name, _Descr, _Ver} <- application:which_applications()],
    lists:map(fun(Plugin = #mqtt_plugin{name = Name}) ->
                      case lists:member(Name, StartedApps) of
                          true  -> Plugin#mqtt_plugin{active = true};
                          false -> Plugin
                      end
              end, Plugins).

plugin(AppFile) ->
    {ok, [{application, Name, Attrs}]} = file:consult(AppFile),
    Ver = proplists:get_value(vsn, Attrs, "0"),
    Descr = proplists:get_value(description, Attrs, ""),
    #mqtt_plugin{name = Name, version = Ver, descr = Descr}.

%%------------------------------------------------------------------------------
%% @doc Load Plugin
%% @end
%%------------------------------------------------------------------------------
-spec load(atom()) -> ok | {error, any()}.
load(PluginName) when is_atom(PluginName) ->
    case lists:member(PluginName, apps(started)) of
        true ->
            lager:info("plugin ~p is started", [PluginName]),
            {error, already_started};
        false ->
            case lists:member(PluginName, apps(plugin)) of
                true ->
                    load_plugin(PluginName);
                false ->
                    lager:info("plugin ~p is not found", [PluginName]),
                    {error, not_foun}
            end
    end.
    
-spec load_plugin(App :: atom()) -> {ok, list()} | {error, any()}.
load_plugin(PluginName) ->
    case start_app(PluginName) of
        {ok, Started} ->
            plugin_loaded(PluginName),
            {ok, Started};
        {error, Error} ->
            {error, Error}
    end.

start_app(App) ->
    case application:ensure_all_started(App) of
        {ok, Started} ->
            lager:info("started apps: ~p, load plugin ~p successfully", [Started, App]),
            {ok, Started};
        {error, {ErrApp, Reason}} ->
            lager:error("load plugin ~p error, cannot start app ~s for ~p", [App, ErrApp, Reason]),
            {error, {ErrApp, Reason}}
    end.

%%------------------------------------------------------------------------------
%% @doc UnLoad Plugin
%% @end
%%------------------------------------------------------------------------------
-spec unload(atom()) -> ok | {error, any()}.
unload(PluginName) when is_atom(PluginName) ->
    %% stop plugin
    %% write file if plugin is loaded
    ok.

-spec unload_plugin(App :: atom()) -> ok | {error, any()}.
unload_plugin(App) ->
    case stop_app(App) of
        ok ->
            unload_app(App);
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

unload_app(App) ->
    case application:unload(App) of
        ok ->
            lager:info("unload plugin ~p successfully~n", [App]), ok;
        {error, {not_loaded, App}} ->
            lager:info("load plugin ~p is not loaded~n", [App]), ok;
        {error, Reason} ->
            lager:error("unload plugin ~p error: ~p", [App, Reason]), {error, Reason}
    end.

stop() ->
    %% stop all plugins
    PluginApps = application:get_env(emqttd, plugins, []),
    %%[{App, unload_plugin(App)} || App <- PluginApps].
    ok.

%%%=============================================================================
%%% Internal functions
%%%=============================================================================

start_apps(Apps) ->
    [start_app(App) || App <- Apps].

stop_apps(Apps) ->
    [stop_app(App) || App <- Apps].

apps(plugin) ->
    [Name || #mqtt_plugin{name = Name} <- list()];

apps(started) ->
    [Name || {Name, _Descr, _Ver} <- application:which_applications()].

plugin_loaded(Name) ->
    case read_loaded() of
        {ok, Names} ->
            case lists:member(Name, Names) of
                true ->
                    ok;
                false ->
                    %% write file if plugin is loaded
                    write_loaded(lists:append(Names, Name))
            end;
        {error, Error} ->
            lager:error("Cannot read loaded plugins: ~p", [Error])
    end.



read_loaded() ->
    {ok, PluginEnv} = application:get_env(emqttd, plugins),
    LoadedFile = proplists:get_value(loaded_file, PluginEnv, "./data/loaded_plugins"),
    file:consult(LoadedFile).

write_loaded(AppNames) ->
    {ok, PluginEnv} = application:get_env(emqttd, plugins),
    LoadedFile = proplists:get_value(loaded_file, PluginEnv, "./data/loaded_plugins"),
    case file:open(LoadedFile, [binary, write]) of
        {ok, Fd} ->
            Line = list_to_binary(io_lib:format("~w.~n", [AppNames])),
            file:write(Fd, Line);
        {error, Error} ->
            {error, Error}
    end.

