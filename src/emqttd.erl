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
%%% @doc emqttd main module.
%%%
%%% @author Feng Lee <feng@emqtt.io>
%%%-----------------------------------------------------------------------------
-module(emqttd).

-export([start/0, env/1, env/2,
         start_listeners/0, stop_listeners/0,
         load_all_mods/0, is_mod_enabled/1,
         is_running/1]).

-define(MQTT_SOCKOPTS, [
	binary,
	{packet,    raw},
	{reuseaddr, true},
	{backlog,   512},
	{nodelay,   true}
]).

-define(APP, ?MODULE).

-type listener() :: {atom(), inet:port_number(), [esockd:option()]}.

%%------------------------------------------------------------------------------
%% @doc Start emqttd application.
%% @end
%%------------------------------------------------------------------------------
-spec start() -> ok | {error, any()}.
start() ->
    application:start(emqttd).

%%------------------------------------------------------------------------------
%% @doc Get environment
%% @end
%%------------------------------------------------------------------------------
-spec env(atom()) -> list().
env(Group) ->
    application:get_env(emqttd, Group, []).

-spec env(atom(), atom()) -> undefined | any().
env(Group, Name) ->
    proplists:get_value(Name, env(Group)).

%%------------------------------------------------------------------------------
%% @doc Start Listeners
%% @end
%%------------------------------------------------------------------------------
-spec start_listeners() -> any().
start_listeners() ->
    {ok, Listeners} = application:get_env(?APP, listeners),
    lists:foreach(fun start_listener/1, Listeners).

%% Start mqtt listener
-spec start_listener(listener()) -> any().
start_listener({mqtt, Port, Options}) ->
    start_listener(mqtt, Port, Options);

%% Start mqtt(SSL) listener
start_listener({mqtts, Port, Options}) ->
    start_listener(mqtts, Port, Options);

%% Start http listener
start_listener({http, Port, Options}) ->
    MFArgs = {emqttd_http, handle_request, []},
	mochiweb:start_http(Port, Options, MFArgs);

%% Start https listener
start_listener({https, Port, Options}) ->
    MFArgs = {emqttd_http, handle_request, []},
	mochiweb:start_http(Port, Options, MFArgs).

start_listener(Protocol, Port, Options) ->
    MFArgs = {emqttd_client, start_link, [env(mqtt)]},
    esockd:open(Protocol, Port, merge_sockopts(Options) , MFArgs).

merge_sockopts(Options) ->
    SockOpts = emqttd_opts:merge(?MQTT_SOCKOPTS,
                                 proplists:get_value(sockopts, Options, [])),
    emqttd_opts:merge(Options, [{sockopts, SockOpts}]).

%%------------------------------------------------------------------------------
%% @doc Stop Listeners
%% @end
%%------------------------------------------------------------------------------
stop_listeners() ->
    {ok, Listeners} = application:get_env(?APP, listeners),
    lists:foreach(fun stop_listener/1, Listeners).

stop_listener({Protocol, Port, _Options}) ->
    esockd:close({Protocol, Port}).

load_all_mods() ->
    lists:foreach(fun({Name, Opts}) ->
        Mod = list_to_atom("emqttd_mod_" ++ atom_to_list(Name)),
        Mod:load(Opts),
        lager:info("load module ~s successfully", [Name])
    end, env(modules)).

is_mod_enabled(Name) ->
    env(modules, Name) =/= undefined.

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

