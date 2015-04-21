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
%%% emqttd main module.
%%%
%%% @end
%%%-----------------------------------------------------------------------------
-module(emqttd).

-author("Feng Lee <feng@emqtt.io>").

-export([start/0, open/1, close/1, is_running/1]).

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
%% @doc Open Listeners
%% @end
%%------------------------------------------------------------------------------
-spec open([listener()] | listener()) -> any().
open(Listeners) when is_list(Listeners) ->
    [open(Listener) || Listener <- Listeners];

%% open mqtt port
open({mqtt, Port, Options}) ->
    open(mqtt, Port, Options);

%% open mqtt(SSL) port
open({mqtts, Port, Options}) ->
    open(mqtts, Port, Options);

%% open http port
open({http, Port, Options}) ->
    MFArgs = {emqttd_http, handle, []},
	mochiweb:start_http(Port, Options, MFArgs).

open(Protocol, Port, Options) ->
    {ok, PktOpts} = application:get_env(emqttd, mqtt_packet),
    MFArgs = {emqttd_client, start_link, [PktOpts]},
    esockd:open(Protocol, Port, emqttd_opts:merge(?MQTT_SOCKOPTS, Options) , MFArgs).

%%------------------------------------------------------------------------------
%% @doc Close Listeners
%% @end
%%------------------------------------------------------------------------------
-spec close([listener()] | listener()) -> any().
close(Listeners) when is_list(Listeners) ->
    [close(Listener) || Listener <- Listeners];

close({Protocol, Port, _Options}) ->
    esockd:close({Protocol, Port}).

is_running(Node) ->
    case rpc:call(Node, erlang, whereis, [emqttd]) of
        {badrpc, _}          -> false;
        undefined            -> false;
        Pid when is_pid(Pid) -> true
    end.


