%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% Developer of the eMQTT Code is <ery.lee@gmail.com>
%% Copyright (c) 2012 Ery Lee.  All rights reserved.
%%

-module(emqtt_retained).

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

-include_lib("elog/include/elog.hrl").

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
	gen_server2:start_link({local, ?MODULE}, ?MODULE, [], []).

lookup(Topic) ->
	ets:lookup(retained_msg, Topic).

insert(Topic, Msg) ->
	gen_server2:cast(?MODULE, {insert, Topic, Msg}).

delete(Topic) ->
	gen_server2:cast(?MODULE, {delete, Topic}).

send(Topic, Client) ->
	[Client ! {route, Msg} ||{_, Msg} <- lookup(Topic)].

init([]) ->
	ets:new(retained_msg, [set, protected, named_table]),
	?INFO("~p is started.", [?MODULE]),
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


