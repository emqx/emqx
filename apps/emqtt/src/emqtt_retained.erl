%%-----------------------------------------------------------------------------
%% Copyright (c) 2014, Feng Lee <feng@slimchat.io>
%% 
%% Permission is hereby granted, free of charge, to any person obtaining a copy
%% of this software and associated documentation files (the "Software"), to deal
%% in the Software without restriction, including without limitation the rights
%% to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
%% copies of the Software, and to permit persons to whom the Software is
%% furnished to do so, subject to the following conditions:
%% 
%% The above copyright notice and this permission notice shall be included in all
%% copies or substantial portions of the Software.
%% 
%% THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
%% IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
%% FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
%% AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
%% LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
%% OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
%% SOFTWARE.
%%------------------------------------------------------------------------------

-module(emqtt_retained).

-author('feng@slimchat.io').

%%TODO: FIXME Later...

%%
%% <<MQTT_V3.1_Protocol_Specific>>

%% RETAIN
%% Position: byte 1, bit 0.

%% This flag is only used on PUBLISH messages. When a client sends a PUBLISH to a server, if the Retain flag is set (1), the server should hold on to the message after it has been delivered to the current subscribers.

%% When a new subscription is established on a topic, the last retained message on that topic should be sent to the subscriber with the Retain flag set. If there is no retained message, nothing is sent

%% This is useful where publishers send messages on a "report by exception" basis, where it might be some time between messages. This allows new subscribers to instantly receive data with the retained, or Last Known Good, value.

%% When a server sends a PUBLISH to a client as a result of a subscription that already existed when the original PUBLISH arrived, the Retain flag should not be set, regardless of the Retain flag of the original PUBLISH. This allows a client to distinguish messages that are being received because they were retained and those that are being received "live".

%% Retained messages should be kept over restarts of the server.

%% A server may delete a retained message if it receives a message with a zero-length payload and the Retain flag set on the same topic.

-include("emqtt.hrl").

-export([start_link/0,
		lookup/1,
		insert/2,
		delete/1,
		send/2]).

-behaviour(gen_server).

-export([init/1,
		handle_call/3,
		handle_cast/2,
		handle_info/2,
		terminate/2,
		code_change/3]).

-record(state, {}).

start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

lookup(Topic) ->
	ets:lookup(retained_msg, Topic).

insert(Topic, Msg) ->
	gen_server:cast(?MODULE, {insert, Topic, Msg}).

delete(Topic) ->
	gen_server:cast(?MODULE, {delete, Topic}).

send(Topic, Client) ->
	[Client ! {dispatch, Msg} ||{_, Msg} <- lookup(Topic)].

init([]) ->
	ets:new(retained_msg, [set, protected, named_table]),
	{ok, #state{}}.

handle_call(Req, _From, State) ->
	{stop, {badreq,Req}, State}.

handle_cast({insert, Topic, Msg}, State) ->
	ets:insert(retained_msg, {Topic, Msg}),
	{noreply, State};

handle_cast({delete, Topic}, State) ->
	ets:delete(retained_msg, Topic),
	{noreply, State};

handle_cast(Msg, State) ->
	{stop, {badmsg, Msg}, State}.

handle_info(Info, State) ->
	{stop, {badinfo, Info}, State}.

terminate(_Reason, _State) ->
	ok.

code_change(_OldVsn, State, _Extra) ->
	{ok, State}.


