%% Copyright (c) 2018 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_cm).

-behaviour(gen_server).

-include("emqx.hrl").

-export([start_link/0]).

-export([lookup_client/1]).
-export([register_client/1, register_client/2, unregister_client/1]).

-export([get_client_attrs/1, lookup_client_pid/1]).
-export([get_client_stats/1, set_client_stats/2]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-record(state, {client_pmon}).

-define(CM, ?MODULE).
%% ETS Tables.
-define(CLIENT,       emqx_client).
-define(CLIENT_ATTRS, emqx_client_attrs).
-define(CLIENT_STATS, emqx_client_stats).

%% @doc Start the client manager.
-spec(start_link() -> {ok, pid()} | ignore | {error, term()}).
start_link() ->
    gen_server:start_link({local, ?CM}, ?MODULE, [], []).

%% @doc Lookup a client.
-spec(lookup_client(client_id()) -> list({client_id(), pid()})).
lookup_client(ClientId) when is_binary(ClientId) ->
    ets:lookup(?CLIENT, ClientId).

%% @doc Register a client.
-spec(register_client(client_id() | {client_id(), pid()}) -> ok).
register_client(ClientId) when is_binary(ClientId) ->
    register_client({ClientId, self()});

register_client({ClientId, ClientPid}) when is_binary(ClientId), is_pid(ClientPid) ->
    register_client({ClientId, ClientPid}, []).

-spec(register_client({client_id(), pid()}, list()) -> ok).
register_client(CObj = {ClientId, ClientPid}, Attrs) when is_binary(ClientId), is_pid(ClientPid) ->
    _ = ets:insert(?CLIENT, CObj),
    _ = ets:insert(?CLIENT_ATTRS, {CObj, Attrs}),
    notify({registered, ClientId, ClientPid}).

%% @doc Get client attrs
-spec(get_client_attrs({client_id(), pid()}) -> list()).
get_client_attrs(CObj = {ClientId, ClientPid}) when is_binary(ClientId), is_pid(ClientPid) ->
    try
        ets:lookup_element(?CLIENT_ATTRS, CObj, 2)
    catch
        error:badarg -> []
    end.

%% @doc Unregister a client.
-spec(unregister_client(client_id() | {client_id(), pid()}) -> ok).
unregister_client(ClientId) when is_binary(ClientId) ->
    unregister_client({ClientId, self()});

unregister_client(CObj = {ClientId, ClientPid}) when is_binary(ClientId), is_pid(ClientPid) ->
    _ = ets:delete(?CLIENT_STATS, CObj),
    _ = ets:delete(?CLIENT_ATTRS, CObj),
    _ = ets:delete_object(?CLIENT, CObj),
    notify({unregistered, ClientId, ClientPid}).

%% @doc Lookup client pid
-spec(lookup_client_pid(client_id()) -> pid() | undefined).
lookup_client_pid(ClientId) when is_binary(ClientId) ->
    case lookup_client_pid(ClientId) of
        [] -> undefined;
        [{_, Pid}] -> Pid
    end.

%% @doc Get client stats
-spec(get_client_stats({client_id(), pid()}) -> list(emqx_stats:stats())).
get_client_stats(CObj = {ClientId, ClientPid}) when is_binary(ClientId), is_pid(ClientPid) ->
    try ets:lookup_element(?CLIENT_STATS, CObj, 2)
    catch
        error:badarg -> []
    end.

%% @doc Set client stats.
-spec(set_client_stats(client_id(), list(emqx_stats:stats())) -> boolean()).
set_client_stats(ClientId, Stats) when is_binary(ClientId) ->
    set_client_stats({ClientId, self()}, Stats);

set_client_stats(CObj = {ClientId, ClientPid}, Stats) when is_binary(ClientId), is_pid(ClientPid) ->
    ets:insert(?CLIENT_STATS, {CObj, Stats}).

notify(Msg) ->
    gen_server:cast(?CM, {notify, Msg}).

%%-----------------------------------------------------------------------------
%% gen_server callbacks
%%-----------------------------------------------------------------------------

init([]) ->
    TabOpts = [public, set, {write_concurrency, true}],
    _ = emqx_tables:new(?CLIENT, [{read_concurrency, true} | TabOpts]),
    _ = emqx_tables:new(?CLIENT_ATTRS, TabOpts),
    _ = emqx_tables:new(?CLIENT_STATS, TabOpts),
    ok = emqx_stats:update_interval(cm_stats, fun update_client_stats/0),
    {ok, #state{client_pmon = emqx_pmon:new()}}.

handle_call(Req, _From, State) ->
    emqx_logger:error("[CM] unexpected call: ~p", [Req]),
    {reply, ignored, State}.

handle_cast({notify, {registered, ClientId, Pid}}, State = #state{client_pmon = PMon}) ->
    {noreply, State#state{client_pmon = emqx_pmon:monitor(Pid, ClientId, PMon)}};

handle_cast({notify, {unregistered, _ClientId, Pid}}, State = #state{client_pmon = PMon}) ->
    {noreply, State#state{client_pmon = emqx_pmon:demonitor(Pid, PMon)}};

handle_cast(Msg, State) ->
    emqx_logger:error("[CM] unexpected cast: ~p", [Msg]),
    {noreply, State}.

handle_info({'DOWN', _MRef, process, DownPid, _Reason}, State = #state{client_pmon = PMon}) ->
    case emqx_pmon:find(DownPid, PMon) of
        undefined -> {noreply, State};
        ClientId  ->
            unregister_client({ClientId, DownPid}),
            {noreply, State#state{client_pmon = emqx_pmon:erase(DownPid, PMon)}}
    end;

handle_info(Info, State) ->
    emqx_logger:error("[CM] unexpected info: ~p", [Info]),
    {noreply, State}.

terminate(_Reason, _State = #state{}) ->
    emqx_stats:cancel_update(cm_stats).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%-----------------------------------------------------------------------------
%% Internal functions
%%-----------------------------------------------------------------------------

update_client_stats() ->
    case ets:info(?CLIENT, size) of
        undefined -> ok;
        Size ->
            emqx_stats:setstat('clients/count', 'clients/max', Size)
    end.

