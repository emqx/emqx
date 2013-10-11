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

-module(emqtt_client_monitor).

-include("emqtt.hrl").

-include("emqtt_internal.hrl").

-include_lib("elog/include/elog.hrl").

-export([start_link/0, mon/1]).

-behaviour(gen_server).

-export([init/1,
		 handle_call/3,
		 handle_cast/2,
		 handle_info/2,
         terminate/2,
		 code_change/3]).

-record(state, {}).

mon(Client) when is_pid(Client) ->
	gen_server2:cast(?MODULE, {monitor, Client}).

%%----------------------------------------------------------------------------

start_link() ->
    gen_server2:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
	ets:new(clientmon, [set, protected, named_table]),
	ets:new(clientmon_reverse, [set, protected, named_table]),
	?INFO("~p is started.", [?MODULE]),
    {ok, #state{}}.

handle_call(Req, _From, State) ->
	{stop, {badreq, Req}, State}.

handle_cast({monitor, Client}, State) ->
	case ets:lookup(clientmon, Client) of
		[] ->
			Ref = erlang:monitor(process, Client),
			ets:insert(clientmon, {Client, Ref}),
			ets:insert(clientmon_reverse, {Ref, Client});
		[_] ->
			ok
	end,
	{noreply, State};

handle_cast(Msg, State) ->
	{stop, {badmsg, Msg}, State}.

handle_info({'DOWN', MRef, _Type, _Object, _Info}, State) ->
	case ets:lookup(clientmon_reverse, MRef) of
	[{_, Client}] ->
		emqtt_router:down(Client),
		ets:delete(clientmon, Client),
		ets:delete(clientmon_reverse, MRef);
	[] ->
		ignore
	end,
	{noreply, State};

handle_info(Info, State) ->
	{stop, {badinfo, Info},State}.


terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.



