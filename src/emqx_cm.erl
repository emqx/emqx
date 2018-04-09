%%--------------------------------------------------------------------
%% Copyright (c) 2013-2018 EMQ Inc. All rights reserved.
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

-module(emqx_cm).

-behaviour(gen_server).

-include("emqx.hrl").

-export([start_link/1]).

-export([lookup/1, reg/1, unreg/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {stats_fun, stats_timer, monitors}).

-define(SERVER, ?MODULE).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

%% @doc Start the client manager
-spec(start_link(fun()) -> {ok, pid()} | ignore | {error, term()}).
start_link(StatsFun) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [StatsFun], []).

%% @doc Lookup ClientPid by ClientId
-spec(lookup(client_id()) -> pid() | undefined).
lookup(ClientId) when is_binary(ClientId) ->
    try ets:lookup_element(client, ClientId, 2)
    catch
        error:badarg -> undefined
    end.

%% @doc Register a clientId 
-spec(reg(client_id()) -> ok).
reg(ClientId) ->
    gen_server:cast(?SERVER, {reg, ClientId, self()}).

%% @doc Unregister clientId with pid.
-spec(unreg(client_id()) -> ok).
unreg(ClientId) ->
    gen_server:cast(?SERVER, {unreg, ClientId, self()}).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([]) ->
    _ = emqx_tables:create(client, [public, set, {keypos, 2},
                                    {read_concurrency, true},
                                    {write_concurrency, true}]),
    _ = emqx_tables:create(client_attrs, [public, set,
                                          {write_concurrency, true}]),
    {ok, #state{monitors = dict:new()}}.

handle_call(Req, _From, State) ->
    emqx_log:error("[CM] Unexpected request: ~p", [Req]),
    {reply, ignore, State}.

handle_cast({reg, ClientId, Pid}, State) ->
    _ = ets:insert(client, {ClientId, Pid}),
    {noreply, monitor_client(ClientId, Pid, State)};

handle_cast({unreg, ClientId, Pid}, State) ->
    case lookup(ClientId) of
        Pid -> remove_client({ClientId, Pid});
        _   -> ok
    end,
    {noreply, State};

handle_cast(Msg, State) ->
    emqx_log:error("[CM] Unexpected msg: ~p", [Msg]),
    {noreply, State}.

handle_info({'DOWN', MRef, process, DownPid, _Reason}, State) ->
    case dict:find(MRef, State#state.monitors) of
        {ok, ClientId} ->
            case lookup(ClientId) of
                DownPid -> remove_client({ClientId, DownPid});
                _       -> ok
            end,
            {noreply, erase_monitor(MRef, State)};
        error ->
            emqx_log:error("[CM] down client ~p not found", [DownPid]),
            {noreply, State}
    end;

handle_info(stats, State) ->
    {noreply, setstats(State), hibernate};

handle_info(Info, State) ->
    emqx_log:error("[CM] Unexpected info: ~p", [Info]),
    {noreply, State}.

terminate(_Reason, _State = #state{stats_timer = TRef}) ->
    timer:cancel(TRef).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

remove_client(Client) ->
    ets:delete_object(client, Client),
    ets:delete(client_stats, Client),
    ets:delete(client_attrs, Client).

monitor_client(ClientId, Pid, State = #state{monitors = Monitors}) ->
    MRef = erlang:monitor(process, Pid),
    State#state{monitors = dict:store(MRef, ClientId, Monitors)}.

erase_monitor(MRef, State = #state{monitors = Monitors}) ->
    erlang:demonitor(MRef),
    State#state{monitors = dict:erase(MRef, Monitors)}.

setstats(State = #state{stats_fun = StatsFun}) ->
    StatsFun(ets:info(client, size)), State.

