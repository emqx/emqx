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

-include("emqttd.hrl").

-behaviour(gen_server).

-define(SERVER, ?MODULE).

%% API Exports 
-export([start_link/2, pool/0, table/0]).

-export([lookup/1, register/1, unregister/1]).

%% gen_server Function Exports
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {id, tab, statsfun}).

-define(CM_POOL, cm_pool).

-define(CLIENT_TAB, mqtt_client).

%%%=============================================================================
%%% API
%%%=============================================================================

%%------------------------------------------------------------------------------
%% @doc Start client manager
%% @end
%%------------------------------------------------------------------------------
-spec start_link(Id, StatsFun) -> {ok, pid()} | ignore | {error, any()} when
        Id :: pos_integer(),
        StatsFun :: fun().
start_link(Id, StatsFun) ->
    gen_server:start_link(?MODULE, [Id, StatsFun], []).

pool() -> ?CM_POOL.

table() -> ?CLIENT_TAB.

%%------------------------------------------------------------------------------
%% @doc Lookup client pid with clientId
%% @end
%%------------------------------------------------------------------------------
-spec lookup(ClientId :: binary()) -> mqtt_client() | undefined.
lookup(ClientId) when is_binary(ClientId) ->
    case ets:lookup(?CLIENT_TAB, ClientId) of
	[Client] -> Client;
	[] -> undefined
	end.

%%------------------------------------------------------------------------------
%% @doc Register clientId with pid.
%% @end
%%------------------------------------------------------------------------------
-spec register(Client :: mqtt_client()) -> ok.
register(Client = #mqtt_client{clientid = ClientId}) ->
    CmPid = gproc_pool:pick_worker(?CM_POOL, ClientId),
    gen_server:call(CmPid, {register, Client}, infinity).

%%------------------------------------------------------------------------------
%% @doc Unregister clientId with pid.
%% @end
%%------------------------------------------------------------------------------
-spec unregister(ClientId :: binary()) -> ok.
unregister(ClientId) when is_binary(ClientId) ->
    CmPid = gproc_pool:pick_worker(?CM_POOL, ClientId),
    gen_server:cast(CmPid, {unregister, ClientId, self()}).

%%%=============================================================================
%%% gen_server callbacks
%%%=============================================================================

init([Id, StatsFun]) ->
    gproc_pool:connect_worker(?CM_POOL, {?MODULE, Id}),
    {ok, #state{id = Id, statsfun = StatsFun}}.

handle_call({register, Client = #mqtt_client{clientid = ClientId, client_pid = Pid}}, _From, State) ->
	case ets:lookup(?CLIENT_TAB, ClientId) of
        [#mqtt_client{client_pid = Pid}] ->
			lager:error("clientId '~s' has been registered with ~p", [ClientId, Pid]),
            ignore;
		[#mqtt_client{client_pid = OldPid, client_mon = MRef}] ->
			lager:error("clientId '~s' is duplicated: pid=~p, oldpid=~p", [ClientId, Pid, OldPid]),
			OldPid ! {stop, duplicate_id, Pid},
			erlang:demonitor(MRef),
            ets:insert(?CLIENT_TAB, Client#mqtt_client{client_mon = erlang:monitor(process, Pid)});
		[] -> 
            ets:insert(?CLIENT_TAB, Client#mqtt_client{client_mon = erlang:monitor(process, Pid)})
	end,
    {reply, ok, setstats(State)};

handle_call(Req, _From, State) ->
    lager:error("unexpected request: ~p", [Req]),
    {reply, {error, badreq}, State}.

handle_cast({unregister, ClientId, Pid}, State) ->
	case ets:lookup(?CLIENT_TAB, ClientId) of
	[#mqtt_client{client_pid = Pid, client_mon = MRef}] ->
		erlang:demonitor(MRef, [flush]),
		ets:delete(?CLIENT_TAB, ClientId);
	[_] -> 
		ignore;
	[] ->
		lager:error("cannot find clientId '~s' with ~p", [ClientId, Pid])
	end,
	{noreply, setstats(State)};

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({'DOWN', MRef, process, DownPid, Reason}, State) ->
    case ets:match_object(?CLIENT_TAB, {mqtt_client, '$1', '_', '_', DownPid, MRef, '_', '_'}) of
        [] ->
            ignore;
        Clients ->
            lists:foreach(
                fun(Client = #mqtt_client{clientid = ClientId}) ->
                        ets:delete_object(?CLIENT_TAB, Client),
                        lager:error("Client ~s is Down: ~p", [ClientId, Reason]),
                        emqttd_broker:foreach_hooks(client_disconnected, [Reason, ClientId])
                end, Clients)
    end,
    {noreply, setstats(State)};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{id = Id}) ->
    gproc_pool:disconnect_worker(?CM_POOL, {?MODULE, Id}), ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%=============================================================================
%%% Internal functions
%%%=============================================================================

setstats(State = #state{statsfun = StatsFun}) ->
    StatsFun(ets:info(?CLIENT_TAB, size)), State.


