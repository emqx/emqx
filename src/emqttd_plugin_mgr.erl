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

-module(emqttd_plugin_manager).

-author("Feng Lee <feng@emqtt.io>").

-include("emqttd.hrl").

-export([start/0, list/0, load/1, unload/1, stop/0]).

start() ->
    %% start all plugins
    %%
    ok.

%%------------------------------------------------------------------------------
%% @doc Load all plugins
%% @end
%%------------------------------------------------------------------------------
-spec load_all_plugins() -> [{App :: atom(), ok | {error, any()}}].
load_all_plugins() ->
    %% save first
    case file:consult("etc/plugins.config") of
        {ok, [PluginApps]} ->
            ok;
            %% application:set_env(emqttd, plugins, [App || {App, _Env} <- PluginApps]),
            %% [{App, load_plugin(App)} || {App, _Env} <- PluginApps];
        {error, enoent} ->
            lager:error("etc/plugins.config not found!");
        {error, Error} ->
            lager:error("Load etc/plugins.config error: ~p", [Error])
    end.

%%------------------------------------------------------------------------------
%% List all available plugins
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
    Ver = proplists:get_value(vsn, Attrs),
    Descr = proplists:get_value(description, Attrs, ""),
    #mqtt_plugin{name = Name, version = Ver, descr = Descr}.

%%------------------------------------------------------------------------------
%% @doc Load Plugin
%% @end
%%------------------------------------------------------------------------------
-spec load(atom()) -> ok | {error, any()}.
load(PluginName) when is_atom(PluginName) ->
    %% start plugin
    %% write file if plugin is loaded
    ok.

-spec load_plugin(App :: atom()) -> ok | {error, any()}.
load_plugin(App) ->
    case load_app(App) of
        ok ->
            start_app(App);
        {error, Reason} ->
            {error, Reason}
    end.

load_app(App) ->
    case application:load(App) of
        ok ->
            lager:info("load plugin ~p successfully", [App]), ok;
        {error, {already_loaded, App}} ->
            lager:info("load plugin ~p is already loaded", [App]), ok;
        {error, Reason} ->
            lager:error("load plugin ~p error: ~p", [App, Reason]), {error, Reason}
    end.

start_app(App) ->
    case application:start(App) of
        ok ->
            lager:info("start plugin ~p successfully", [App]), ok;
        {error, {already_started, App}} ->
            lager:error("plugin ~p is already started", [App]), ok;
        {error, Reason} ->
            lager:error("start plugin ~p error: ~p", [App, Reason]), {error, Reason}
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
    ok.

%%------------------------------------------------------------------------------
%% @doc Unload all plugins
%% @end
%%------------------------------------------------------------------------------
-spec unload_all_plugins() -> [{App :: atom(), ok | {error, any()}}].
unload_all_plugins() ->
    PluginApps = application:get_env(emqttd, plugins, []).
    %%[{App, unload_plugin(App)} || App <- PluginApps].

