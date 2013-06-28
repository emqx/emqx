%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License at
%% http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%% License for the specific language governing rights and limitations
%% under the License.
%%
%% The Original Code is eMQTT
%%
%% The Initial Developer of the Original Code is <ery.lee at gmail dot com>
%% Copyright (C) 2012 Ery Lee All Rights Reserved.

-module(emqtt_registry).

-include("emqtt.hrl").

-include_lib("elog/include/elog.hrl").

-export([start_link/0, 
		size/0,
		register/2,
		unregister/1]).

-behaviour(gen_server).

-export([init/1,
		 handle_call/3,
		 handle_cast/2,
		 handle_info/2,
         terminate/2,
		 code_change/3]).

-record(state, {}).

-define(SERVER, ?MODULE).

%%----------------------------------------------------------------------------

start_link() ->
    gen_server2:start_link({local, ?SERVER}, ?MODULE, [], []).

size() ->
	ets:info(client, size).

register(ClientId, Pid) ->
    gen_server2:cast(?SERVER, {register, ClientId, Pid}).

unregister(ClientId) ->
    gen_server2:cast(?SERVER, {unregister, ClientId}).

%%----------------------------------------------------------------------------

init([]) ->
	ets:new(client, [set, protected, named_table]),
	?INFO("~p is started.", [?MODULE]),
    {ok, #state{}}. % clientid -> {pid, monitor}

%%--------------------------------------------------------------------------
handle_call(Req, _From, State) ->
    {stop, {badreq, Req}, State}.

handle_cast({register, ClientId, Pid}, State) ->
	case ets:lookup(client, ClientId) of
	[{_, {OldPid, MRef}}] ->
		catch gen_server2:call(OldPid, duplicate_id),
		erlang:demonitor(MRef);
	[] ->
		ignore
	end,
	ets:insert(client, {ClientId, {Pid, erlang:monitor(process, Pid)}}),
    {noreply, State};

handle_cast({unregister, ClientId}, State) ->
	case ets:lookup(client, ClientId) of
	[{_, {_Pid, MRef}}] ->
		erlang:demonitor(MRef),
		ets:delete(client, ClientId);
	[] ->
		ignore
	end,
	{noreply, State};

handle_cast(Msg, State) ->
    {stop, {badmsg, Msg}, State}.

handle_info({'DOWN', MRef, process, DownPid, _Reason}, State) ->
	ets:match_delete(client, {'_', {DownPid, MRef}}),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

