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
%%% @doc MQTT Client Manager
%%%  
%%% @author Feng Lee <feng@emqtt.io>
%%%-----------------------------------------------------------------------------
-module(emqttd_cm).

-include("emqttd.hrl").

-include("emqttd_internal.hrl").

%% API Exports 
-export([start_link/3]).

-export([lookup/1, lookup_proc/1, register/1, unregister/1]).

-behaviour(gen_server2).

%% gen_server Function Exports
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%% gen_server2 priorities
-export([prioritise_call/4, prioritise_cast/3, prioritise_info/3]).

-record(state, {pool, id, statsfun, monitors}).

-define(POOL, ?MODULE).

%%%=============================================================================
%%% API
%%%=============================================================================

%%------------------------------------------------------------------------------
%% @doc Start Client Manager
%% @end
%%------------------------------------------------------------------------------
-spec start_link(Pool, Id, StatsFun) -> {ok, pid()} | ignore | {error, any()} when
        Pool :: atom(),
        Id   :: pos_integer(),
        StatsFun :: fun().
start_link(Pool, Id, StatsFun) ->
    gen_server2:start_link(?MODULE, [Pool, Id, StatsFun], []).

%%------------------------------------------------------------------------------
%% @doc Lookup Client by ClientId
%% @end
%%------------------------------------------------------------------------------
-spec lookup(ClientId :: binary()) -> mqtt_client() | undefined.
lookup(ClientId) when is_binary(ClientId) ->
    case ets:lookup(mqtt_client, ClientId) of
        [Client] -> Client;
        [] -> undefined
    end.

%%------------------------------------------------------------------------------
%% @doc Lookup client pid by clientId
%% @end
%%------------------------------------------------------------------------------
-spec lookup_proc(ClientId :: binary()) -> pid() | undefined.
lookup_proc(ClientId) when is_binary(ClientId) ->
    try ets:lookup_element(mqtt_client, ClientId, #mqtt_client.client_pid)
    catch
        error:badarg -> undefined
    end.

%%------------------------------------------------------------------------------
%% @doc Register ClientId with Pid.
%% @end
%%------------------------------------------------------------------------------
-spec register(Client :: mqtt_client()) -> ok.
register(Client = #mqtt_client{client_id = ClientId}) ->
    CmPid = gproc_pool:pick_worker(?POOL, ClientId),
    gen_server2:cast(CmPid, {register, Client}).

%%------------------------------------------------------------------------------
%% @doc Unregister clientId with pid.
%% @end
%%------------------------------------------------------------------------------
-spec unregister(ClientId :: binary()) -> ok.
unregister(ClientId) when is_binary(ClientId) ->
    CmPid = gproc_pool:pick_worker(?POOL, ClientId),
    gen_server2:cast(CmPid, {unregister, ClientId, self()}).

%%%=============================================================================
%%% gen_server callbacks
%%%=============================================================================

init([Pool, Id, StatsFun]) ->
    ?GPROC_POOL(join, Pool, Id),
    {ok, #state{pool = Pool, id = Id,
                statsfun = StatsFun,
                monitors = dict:new()}}.

prioritise_call(_Req, _From, _Len, _State) ->
    1.

prioritise_cast(Msg, _Len, _State) ->
    case Msg of
        {register,   _Client}         -> 2;
        {unregister, _ClientId, _Pid} -> 9;
        _                             -> 1
    end.

prioritise_info(_Msg, _Len, _State) ->
    3.

handle_call(Req, _From, State) ->
    ?UNEXPECTED_REQ(Req, State).

handle_cast({register, Client = #mqtt_client{client_id  = ClientId,
                                             client_pid = Pid}}, State) ->
    case lookup_proc(ClientId) of
        Pid ->
            {noreply, State};
        _ ->
            ets:insert(mqtt_client, Client),
            {noreply, setstats(monitor_client(ClientId, Pid, State))}
    end;

handle_cast({unregister, ClientId, Pid}, State) ->
    case lookup_proc(ClientId) of
        Pid ->
            ets:delete(mqtt_client, ClientId),
            {noreply, setstats(State)};
        _ ->
            {noreply, State}
    end;

handle_cast(Msg, State) ->
    ?UNEXPECTED_MSG(Msg, State).

handle_info({'DOWN', MRef, process, DownPid, _Reason}, State) ->
    case dict:find(MRef, State#state.monitors) of
        {ok, {ClientId, DownPid}} ->
            case lookup_proc(ClientId) of
                DownPid ->
                    ets:delete(mqtt_client, ClientId);
                _ ->
                    ignore
            end,
            {noreply, setstats(erase_monitor(MRef, State))};
        error ->
            lager:error("MRef of client ~p not found", [DownPid]),
            {noreply, State}
    end;

handle_info(Info, State) ->
    ?UNEXPECTED_INFO(Info, State).

terminate(_Reason, #state{pool = Pool, id = Id}) ->
    ?GPROC_POOL(leave, Pool, Id), ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%=============================================================================
%%% Internal functions
%%%=============================================================================

monitor_client(ClientId, Pid, State = #state{monitors = Monitors}) ->
    MRef = erlang:monitor(process, Pid),
    State#state{monitors = dict:store(MRef, {ClientId, Pid}, Monitors)}.

erase_monitor(MRef, State = #state{monitors = Monitors}) ->
    State#state{monitors = dict:erase(MRef, Monitors)}.

setstats(State = #state{statsfun = StatsFun}) ->
    StatsFun(ets:info(mqtt_client, size)), State.

