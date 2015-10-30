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

%% API Exports 
-export([start_link/2, pool/0]).

-export([lookup/1, register/1, unregister/1]).

-behaviour(gen_server2).

%% gen_server Function Exports
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {id, statsfun}).

-define(CM_POOL, ?MODULE).

-define(LOG(Level, Format, Args, Client),
            lager:Level("CM(~s): " ++ Format, [Client#mqtt_client.client_id|Args])).

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
    gen_server2:start_link(?MODULE, [Id, StatsFun], []).

pool() -> ?CM_POOL.

%%------------------------------------------------------------------------------
%% @doc Lookup client pid with clientId
%% @end
%%------------------------------------------------------------------------------
-spec lookup(ClientId :: binary()) -> mqtt_client() | undefined.
lookup(ClientId) when is_binary(ClientId) ->
    case ets:lookup(mqtt_client, ClientId) of
	[Client] -> Client;
	[] -> undefined
	end.

%%------------------------------------------------------------------------------
%% @doc Register clientId with pid.
%% @end
%%------------------------------------------------------------------------------
-spec register(Client :: mqtt_client()) -> ok.
register(Client = #mqtt_client{client_id = ClientId}) ->
    CmPid = gproc_pool:pick_worker(?CM_POOL, ClientId),
    gen_server2:cast(CmPid, {register, Client}).

%%------------------------------------------------------------------------------
%% @doc Unregister clientId with pid.
%% @end
%%------------------------------------------------------------------------------
-spec unregister(ClientId :: binary()) -> ok.
unregister(ClientId) when is_binary(ClientId) ->
    CmPid = gproc_pool:pick_worker(?CM_POOL, ClientId),
    gen_server2:cast(CmPid, {unregister, ClientId, self()}).

%%%=============================================================================
%%% gen_server callbacks
%%%=============================================================================

init([Id, StatsFun]) ->
    gproc_pool:connect_worker(?CM_POOL, {?MODULE, Id}),
    {ok, #state{id = Id, statsfun = StatsFun}}.

handle_call(Req, _From, State) ->
    lager:error("unexpected request: ~p", [Req]),
    {reply, {error, unsupported_req}, State}.

handle_cast({register, Client = #mqtt_client{client_id  = ClientId,
                                             client_pid = Pid}}, State) ->
	case ets:lookup(mqtt_client, ClientId) of
        [#mqtt_client{client_pid = Pid}] ->
            ignore;
		[#mqtt_client{client_pid = OldPid}] ->
            %% TODO: should cancel monitor
            ?LOG(warning, "client ~p conflict with ~p", [Pid, OldPid], Client);
		[] -> 
            ok
	end,
    ets:insert(mqtt_client, Client),
    {noreply, setstats(State)};

handle_cast({unregister, ClientId, Pid}, State) ->
	case ets:lookup(mqtt_client, ClientId) of
	[#mqtt_client{client_pid = Pid}] ->
		ets:delete(mqtt_client, ClientId);
	[_] ->
		ignore;
	[] ->
        ?LOG(error, "Cannot find registered: ~p", [Pid], State)
	end,
	{noreply, setstats(State)};

handle_cast(Msg, State) ->
    lager:error("Unexpected Msg: ~p", [Msg]),
    {noreply, State}.

handle_info(Info, State) ->
    lager:error("Unexpected Msg: ~p", [Info]),
    {noreply, State}.

terminate(_Reason, #state{id = Id}) ->
    gproc_pool:disconnect_worker(?CM_POOL, {?MODULE, Id}),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%=============================================================================
%%% Internal functions
%%%=============================================================================

setstats(State = #state{statsfun = StatsFun}) ->
    StatsFun(ets:info(mqtt_client, size)), State.

