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
%%% MQTT Client Manager
%%%
%%% @end
%%%-----------------------------------------------------------------------------
-module(emqttd_cm).

-author("Feng Lee <feng@emqtt.io>").

-behaviour(gen_server).

-define(SERVER, ?MODULE).

%% API Exports 
-export([start_link/3]).

-export([lookup/1, register/1, unregister/1]).

%% gen_server Function Exports
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {tab, statsfun}).

-define(POOL, cm_pool).

%%%=============================================================================
%%% API
%%%=============================================================================

%%------------------------------------------------------------------------------
%% @doc Start client manager
%% @end
%%------------------------------------------------------------------------------
-spec start_link(Id, TabId, StatsFun) -> {ok, pid()} | ignore | {error, any()} when
        Id :: pos_integer(),
        TabId :: ets:tid(),
        StatsFun :: fun().
start_link(Id, TabId, StatsFun) ->
    gen_server:start_link(?MODULE, [Id, TabId, StatsFun], []).

%%------------------------------------------------------------------------------
%% @doc Lookup client pid with clientId
%% @end
%%------------------------------------------------------------------------------
-spec lookup(ClientId :: binary()) -> pid() | undefined.
lookup(ClientId) when is_binary(ClientId) ->
    case ets:lookup(emqttd_cm_sup:table(), ClientId) of
	[{_, Pid, _}] -> Pid;
	[] -> undefined
	end.

%%------------------------------------------------------------------------------
%% @doc Register clientId with pid.
%% @end
%%------------------------------------------------------------------------------
-spec register(ClientId :: binary()) -> ok.
register(ClientId) when is_binary(ClientId) ->
    CmPid = gproc_pool:pick_worker(?POOL, ClientId),
    gen_server:call(CmPid, {register, ClientId, self()}, infinity).

%%------------------------------------------------------------------------------
%% @doc Unregister clientId with pid.
%% @end
%%------------------------------------------------------------------------------
-spec unregister(ClientId :: binary()) -> ok.
unregister(ClientId) when is_binary(ClientId) ->
    CmPid = gproc_pool:pick_worker(?POOL, ClientId),
    gen_server:cast(CmPid, {unregister, ClientId, self()}).

%%%=============================================================================
%%% gen_server callbacks
%%%=============================================================================

init([Id, TabId, StatsFun]) ->
    gproc_pool:connect_worker(?POOL, {?MODULE, Id}),
    {ok, #state{tab = TabId, statsfun = StatsFun}}.

handle_call({register, ClientId, Pid}, _From, State = #state{tab = Tab}) ->
	case ets:lookup(Tab, ClientId) of
        [{_, Pid, _}] ->
			lager:error("clientId '~s' has been registered with ~p", [ClientId, Pid]),
            ignore;
		[{_, OldPid, MRef}] ->
			lager:error("clientId '~s' is duplicated: pid=~p, oldpid=~p", [ClientId, Pid, OldPid]),
			OldPid ! {stop, duplicate_id, Pid},
			erlang:demonitor(MRef),
            ets:insert(Tab, {ClientId, Pid, erlang:monitor(process, Pid)});
		[] -> 
            ets:insert(Tab, {ClientId, Pid, erlang:monitor(process, Pid)})
	end,
    {reply, ok, setstats(State)};

handle_call(Req, _From, State) ->
    lager:error("unexpected request: ~p", [Req]),
    {reply, {error, badreq}, State}.

handle_cast({unregister, ClientId, Pid}, State = #state{tab = TabId}) ->
	case ets:lookup(TabId, ClientId) of
	[{_, Pid, MRef}] ->
		erlang:demonitor(MRef, [flush]),
		ets:delete(TabId, ClientId);
	[_] -> 
		ignore;
	[] ->
		lager:error("cannot find clientId '~s' with ~p", [ClientId, Pid])
	end,
	{noreply, setstats(State)};

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({'DOWN', MRef, process, DownPid, _Reason}, State = #state{tab = TabId}) ->
	ets:match_delete(TabId, {'_', DownPid, MRef}),
    {noreply, setstats(State)};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%=============================================================================
%%% Internal functions
%%%=============================================================================

setstats(State = #state{tab = TabId, statsfun = StatsFun}) ->
    StatsFun(ets:info(TabId, size)), State.

