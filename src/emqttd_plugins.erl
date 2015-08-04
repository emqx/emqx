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
%%% emqttd plugin admin.
%%%
%%% @end
%%%-----------------------------------------------------------------------------

-module(emqttd_plugins).

-author("Feng Lee <feng@emqtt.io>").

-include("emqttd.hrl").

-export([load/0, unload/0]).

-export([list/0, load/1, unload/1]).

%%------------------------------------------------------------------------------
%% @doc Load all plugins when the broker started.
%% @end
%%------------------------------------------------------------------------------
-spec load() -> list() | {error, any()}.
load() ->
    case read_loaded() of
        {ok, LoadNames} ->
            NotFound = LoadNames -- apps(plugin),
            case NotFound of
                [] -> ok;
                NotFound -> lager:error("Cannot find plugins: ~p", [NotFound])
            end,
            start_apps(LoadNames -- NotFound -- apps(started));
        {error, Error} ->
            lager:error("Read loaded_plugins file error: ~p", [Error]),
            {error, Error}
    end.

start_apps(Apps) ->
    [start_app(App) || App <- Apps].

%%------------------------------------------------------------------------------
%% @doc Unload all plugins before broker stopped.
%% @end
%%------------------------------------------------------------------------------
-spec unload() -> list() | {error, any()}.
unload() ->
    case read_loaded() of
        {ok, LoadNames} ->
            stop_apps(LoadNames);
        {error, Error} ->
            lager:error("Read loaded_plugins file error: ~p", [Error]),
            {error, Error}
    end.

stop_apps(Apps) ->
    [stop_app(App) || App <- Apps].

%%------------------------------------------------------------------------------
%% @doc List all available plugins
%% @end
%%------------------------------------------------------------------------------
-spec list() -> [mqtt_plugin()].
list() ->
    PluginsDir = env(dir),
    AppFiles = filelib:wildcard("*/ebin/*.app", PluginsDir),
    Plugins = [plugin(filename:join(PluginsDir, AppFile)) || AppFile <- AppFiles],
    StartedApps = apps(started),
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
%% @doc Load One Plugin
%% @end
%%------------------------------------------------------------------------------
-spec load(atom()) -> ok | {error, any()}.
load(PluginName) when is_atom(PluginName) ->
    case {lists:member(PluginName, apps(started)), lists:member(PluginName, apps(plugin))} of
        {true, _} ->
            lager:error("plugin ~p is started", [PluginName]),
            {error, already_started};
        {false, true} ->
            load_plugin(PluginName);
        {false, false} ->
            lager:error("plugin ~p is not found", [PluginName]),
            {error, not_found}
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
%% @doc UnLoad One Plugin
%% @end
%%------------------------------------------------------------------------------
-spec unload(atom()) -> ok | {error, any()}.
unload(PluginName) when is_atom(PluginName) ->
    case {lists:member(PluginName, apps(started)), lists:member(PluginName, apps(plugin))} of
        {false, _} ->
            lager:error("plugin ~p is not started", [PluginName]),
            {error, not_started};
        {true, true} ->
            unload_plugin(PluginName);
        {true, false} ->
            lager:error("~s is not a plugin, cannot unload it", [PluginName]),
            {error, not_found}
    end.

-spec unload_plugin(App :: atom()) -> ok | {error, any()}.
unload_plugin(App) ->
    case stop_app(App) of
        ok ->
            plugin_unloaded(App), ok;
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

%%%=============================================================================
%%% Internal functions
%%%=============================================================================

apps(plugin) ->
    [Name || #mqtt_plugin{name = Name} <- list()];

apps(started) ->
    [Name || {Name, _Descr, _Ver} <- application:which_applications()].

plugin_loaded(Name) ->
    case read_loaded() of
        {ok, Names} ->
            case lists:member(Name, Names) of
                true ->
                    ignore;
                false ->
                    %% write file if plugin is loaded
                    write_loaded(lists:append(Names, Name))
            end;
        {error, Error} ->
            lager:error("Cannot read loaded plugins: ~p", [Error])
    end.

plugin_unloaded(Name) ->
    case read_loaded() of
        {ok, Names} ->
            case lists:member(Name, Names) of
                true ->
                    write_loaded(lists:delete(Name, Names));
                false ->
                    lager:error("Cannot find ~s in loaded_file", [Name])
            end;
        {error, Error} ->
            lager:error("Cannot read loaded plugins: ~p", [Error])
    end.

read_loaded() ->
    file:consult(env(loaded_file)).

write_loaded(AppNames) ->
    case file:open(env(loaded_file), [binary, write]) of
        {ok, Fd} ->
            Line = list_to_binary(io_lib:format("~w.~n", [AppNames])),
            file:write(Fd, Line);
        {error, Error} ->
            {error, Error}
    end.

env(dir) ->
    proplists:get_value(dir, env(), "./plugins");

env(loaded_file) ->
    proplists:get_value(loaded_file, env(), "./data/loaded_plugins").

env() ->
    {ok, PluginsEnv} = application:get_env(emqttd, plugins), PluginsEnv.

