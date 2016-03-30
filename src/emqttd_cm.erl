%%--------------------------------------------------------------------
%% Copyright (c) 2012-2016 Feng Lee <feng@emqtt.io>.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

%% @doc MQTT Client Manager
-module(emqttd_cm).

-behaviour(gen_server2).

-include("emqttd.hrl").

-include("emqttd_internal.hrl").

%% API Exports 
-export([start_link/3]).

-export([lookup/1, lookup_proc/1, register/1, unregister/1]).

%% gen_server Function Exports
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%% gen_server2 priorities
-export([prioritise_call/4, prioritise_cast/3, prioritise_info/3]).

-record(state, {pool, id, statsfun, monitors}).

-define(POOL, ?MODULE).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

%% @doc Start Client Manager
-spec(start_link(Pool, Id, StatsFun) -> {ok, pid()} | ignore | {error, any()} when
      Pool :: atom(),
      Id   :: pos_integer(),
      StatsFun :: fun()).
start_link(Pool, Id, StatsFun) ->
    gen_server2:start_link(?MODULE, [Pool, Id, StatsFun], []).

%% @doc Lookup Client by ClientId
-spec(lookup(ClientId :: binary()) -> mqtt_client() | undefined).
lookup(ClientId) when is_binary(ClientId) ->
    case ets:lookup(mqtt_client, ClientId) of
        [Client] -> Client;
        [] -> undefined
    end.

%% @doc Lookup client pid by clientId
-spec(lookup_proc(ClientId :: binary()) -> pid() | undefined).
lookup_proc(ClientId) when is_binary(ClientId) ->
    try ets:lookup_element(mqtt_client, ClientId, #mqtt_client.client_pid)
    catch
        error:badarg -> undefined
    end.

%% @doc Register ClientId with Pid.
-spec(register(Client :: mqtt_client()) -> ok).
register(Client = #mqtt_client{client_id = ClientId}) ->
    gen_server2:call(pick(ClientId), {register, Client}, 120000).

%% @doc Unregister clientId with pid.
-spec(unregister(ClientId :: binary()) -> ok).
unregister(ClientId) when is_binary(ClientId) ->
    gen_server2:cast(pick(ClientId), {unregister, ClientId, self()}).

pick(ClientId) -> gproc_pool:pick_worker(?POOL, ClientId).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([Pool, Id, StatsFun]) ->
    ?GPROC_POOL(join, Pool, Id),
    {ok, #state{pool = Pool, id = Id, statsfun = StatsFun, monitors = dict:new()}}.

prioritise_call(Req, _From, _Len, _State) ->
    case Req of
        {register,   _Client} -> 2;
        _                     -> 1
    end.

prioritise_cast(Msg, _Len, _State) ->
    case Msg of
        {unregister, _ClientId, _Pid} -> 9;
        _                             -> 1
    end.

prioritise_info(_Msg, _Len, _State) ->
    3.

handle_call({register, Client = #mqtt_client{client_id  = ClientId,
                                             client_pid = Pid}}, _From, State) ->
    case lookup_proc(ClientId) of
        Pid ->
            {reply, ok, State};
        _ ->
            ets:insert(mqtt_client, Client),
            {reply, ok, setstats(monitor_client(ClientId, Pid, State))}
    end;

handle_call(Req, _From, State) ->
    ?UNEXPECTED_REQ(Req, State).

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

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

monitor_client(ClientId, Pid, State = #state{monitors = Monitors}) ->
    MRef = erlang:monitor(process, Pid),
    State#state{monitors = dict:store(MRef, {ClientId, Pid}, Monitors)}.

erase_monitor(MRef, State = #state{monitors = Monitors}) ->
    State#state{monitors = dict:erase(MRef, Monitors)}.

setstats(State = #state{statsfun = StatsFun}) ->
    StatsFun(ets:info(mqtt_client, size)), State.

