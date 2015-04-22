%%%-----------------------------------------------------------------------------
%%% @Copyright (C) 2012-2015, Feng Lee <feng@emqtt.io>
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

%%TODO: rewrite this module later...

-author("Feng Lee <feng@emqtt.io>").

-export([load_all_plugins/0, unload_all_plugins/0]).

-export([load_plugin/1, unload_plugin/1]).

-export([loaded_plugins/0]).

%%------------------------------------------------------------------------------
%% @doc Load all plugins
%% @end
%%------------------------------------------------------------------------------
load_all_plugins() ->
    {ok, [PluginApps]} = file:consult("etc/plugins.config"),
    LoadedPlugins = load_plugins(PluginApps),
    RunningPlugins = start_plugins(lists:reverse(LoadedPlugins)),
    %% save to application env?
    application:set_env(emqttd, loaded_plugins, lists:reverse(RunningPlugins)).

load_plugins(PluginApps) ->
    lists:foldl(fun({App, _Env}, Acc) ->
                    case application:load(App) of
                        ok ->
                            io:format("load plugin ~p successfully~n", [App]),
                            [App | Acc];
                        {error, {already_loaded, App}} ->
                            io:format("load plugin ~p is already loaded~n", [App]),
                            [App | Acc];
                        {error, Reason} ->
                            io:format("load plugin ~p error: ~p~n", [App, Reason]),
                            Acc
                    end
                end, [], PluginApps).

start_plugins(PluginApps) ->
    lists:foldl(fun(App, Acc) ->
                    case application:start(App) of
                        ok ->
                            io:format("start plugin ~p successfully~n", [App]),
                            [App | Acc];
                        {error, {already_started, App}} ->
                            io:format("plugin ~p is already started~n", [App]),
                            [App | Acc];
                        {error, Reason} ->
                            io:format("start plugin ~p error: ~p~n", [App, Reason]),
                            Acc
                    end
                end, [], PluginApps).

%%------------------------------------------------------------------------------
%% @doc Loaded plugins
%% @end
%%------------------------------------------------------------------------------
loaded_plugins() ->
    LoadedPluginApps = application:get_env(emqttd, loaded_plugins, []),
    lager:info("loaded plugins: ~p", [LoadedPluginApps]),
    [App || App = {Name, _Descr, _Vsn} <- application:which_applications(),
                                            lists:member(Name, LoadedPluginApps)].
    
%%------------------------------------------------------------------------------
%% @doc Unload all plugins
%% @end
%%------------------------------------------------------------------------------
unload_all_plugins() ->
    LoadedPluginApps = application:get_env(emqttd, loaded_plugins, []),
    StoppedApps = stop_plugins(lists:reverse(LoadedPluginApps)),
    UnloadedApps = unload_plugins(lists:reverse(StoppedApps)),
    case LoadedPluginApps -- UnloadedApps of
        [] -> 
            application:unset_env(emqttd, loaded_plugins);
        LeftApps -> 
            lager:error("cannot unload plugins: ~p", [LeftApps]),
            application:set_env(emqttd, loaded_plugins, LeftApps)
    end.

stop_plugins(PluginApps) ->
    lists:foldl(fun(App, Acc) ->
                    case application:stop(App) of
                        ok ->
                            io:format("stop plugin ~p successfully~n", [App]),
                            [App | Acc];
                        {error, {not_started, App}} ->
                            io:format("plugin ~p is not started~n", [App]),
                            [App | Acc];
                        {error, Reason} ->
                            io:format("stop plugin ~p error: ~p~n", [App, Reason]),
                            Acc
                    end
                end, [], PluginApps).

unload_plugins(PluginApps) ->
    lists:foldl(fun({App, _Env}, Acc) ->
                    case application:unload(App) of
                        ok ->
                            io:format("unload plugin ~p successfully~n", [App]),
                            [App | Acc];
                        {error, {not_loaded, App}} ->
                            io:format("load plugin ~p is not loaded~n", [App]),
                            [App | Acc];
                        {error, Reason} ->
                            io:format("unload plugin ~p error: ~p~n", [App, Reason]),
                            Acc
                    end
                end, [], PluginApps).

%%------------------------------------------------------------------------------
%% @doc Load Plugin
%% @end
%%------------------------------------------------------------------------------
load_plugin(Name) when is_atom(Name) ->
    %% load app
    %% start app
    %% set env...
    application:start(Name).

%%------------------------------------------------------------------------------
%% @doc Unload Plugin
%% @end
%%------------------------------------------------------------------------------
unload_plugin(Name) when is_atom(Name) ->
    %% stop app
    %% unload app
    %% set env
    application:stop(Name),
    application:unload(Name).

