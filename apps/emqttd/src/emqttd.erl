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
%%% emqttd main module.
%%%
%%% @end
%%%-----------------------------------------------------------------------------
-module(emqttd).

-author("Feng Lee <feng@emqtt.io>").

-export([start/0, env/1,
         open_listeners/1, close_listeners/1,
         load_all_plugins/0, unload_all_plugins/0,
         load_plugin/1, unload_plugin/1,
         loaded_plugins/0,
         is_running/1]).

-define(MQTT_SOCKOPTS, [
	binary,
	{packet,    raw},
	{reuseaddr, true},
	{backlog,   512},
	{nodelay,   true}
]).

-type listener() :: {atom(), inet:port_number(), [esockd:option()]}. 

%%------------------------------------------------------------------------------
%% @doc Start emqttd application.
%% @end
%%------------------------------------------------------------------------------
-spec start() -> ok | {error, any()}.
start() ->
    application:start(emqttd).

%%------------------------------------------------------------------------------
%% @doc Get mqtt environment
%% @end
%%------------------------------------------------------------------------------
-spec env(atom()) -> undefined | any().
env(Name) ->
    proplists:get_value(Name, application:get_env(emqttd, mqtt, [])).

%%------------------------------------------------------------------------------
%% @doc Open Listeners
%% @end
%%------------------------------------------------------------------------------
-spec open_listeners([listener()]) -> any().
open_listeners(Listeners) when is_list(Listeners) ->
    [open_listener(Listener) || Listener <- Listeners].

%% open mqtt port
open_listener({mqtt, Port, Options}) ->
    open_listener(mqtt, Port, Options);

%% open mqtt(SSL) port
open_listener({mqtts, Port, Options}) ->
    open_listener(mqtts, Port, Options);

%% open http port
open_listener({http, Port, Options}) ->
    MFArgs = {emqttd_http, handle_request, []},
	mochiweb:start_http(Port, Options, MFArgs).

open_listener(Protocol, Port, Options) ->
    MFArgs = {emqttd_client, start_link, [env(packet)]},
    esockd:open(Protocol, Port, merge_sockopts(Options) , MFArgs).

merge_sockopts(Options) ->
    SockOpts = emqttd_opts:merge(?MQTT_SOCKOPTS,
                                 proplists:get_value(sockopts, Options, [])),
    emqttd_opts:merge(Options, [{sockopts, SockOpts}]).

%%------------------------------------------------------------------------------
%% @doc Close Listeners
%% @end
%%------------------------------------------------------------------------------
-spec close_listeners([listener()]) -> any().
close_listeners(Listeners) when is_list(Listeners) ->
    [close_listener(Listener) || Listener <- Listeners].

close_listener({Protocol, Port, _Options}) ->
    esockd:close({Protocol, Port}).

%%------------------------------------------------------------------------------
%% @doc Load all plugins
%% @end
%%------------------------------------------------------------------------------
-spec load_all_plugins() -> [{App :: atom(), ok | {error, any()}}].
load_all_plugins() ->
    %% save first
    {ok, [PluginApps]} = file:consult("etc/plugins.config"),
    application:set_env(emqttd, plugins, [App || {App, _Env} <- PluginApps]),
    [{App, load_plugin(App)} || {App, _Env} <- PluginApps].

%%------------------------------------------------------------------------------
%% @doc Load plugin
%% @end
%%------------------------------------------------------------------------------
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
%% @doc Loaded plugins
%% @end
%%------------------------------------------------------------------------------
loaded_plugins() ->
    PluginApps = application:get_env(emqttd, plugins, []),
    [App || App = {Name, _Descr, _Vsn} <- application:which_applications(),
                                            lists:member(Name, PluginApps)].
    
%%------------------------------------------------------------------------------
%% @doc Unload all plugins
%% @end
%%------------------------------------------------------------------------------
-spec unload_all_plugins() -> [{App :: atom(), ok | {error, any()}}].
unload_all_plugins() ->
    PluginApps = application:get_env(emqttd, plugins, []),
    [{App, unload_plugin(App)} || {App, _Env} <- PluginApps].


%%------------------------------------------------------------------------------
%% @doc Unload plugin
%% @end
%%------------------------------------------------------------------------------
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

%%------------------------------------------------------------------------------
%% @doc Is running?
%% @end
%%------------------------------------------------------------------------------
is_running(Node) ->
    case rpc:call(Node, erlang, whereis, [emqttd]) of
        {badrpc, _}          -> false;
        undefined            -> false;
        Pid when is_pid(Pid) -> true
    end.


