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

-export([lookup_connection/1]).
-export([register_connection/1, register_connection/2]).
-export([unregister_connection/1]).
-export([get_conn_attrs/1, set_conn_attrs/2]).
-export([get_conn_stats/1, set_conn_stats/2]).
-export([lookup_conn_pid/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

%% internal export
-export([update_conn_stats/0]).

-define(CM, ?MODULE).

%% ETS Tables.
-define(CONN_TAB,       emqx_conn).
-define(CONN_ATTRS_TAB, emqx_conn_attrs).
-define(CONN_STATS_TAB, emqx_conn_stats).

%% @doc Start the connection manager.
-spec(start_link() -> emqx_types:startlink_ret()).
start_link() ->
    gen_server:start_link({local, ?CM}, ?MODULE, [], []).

%% @doc Lookup a connection.
-spec(lookup_connection(emqx_types:client_id()) -> list({emqx_types:client_id(), pid()})).
lookup_connection(ClientId) when is_binary(ClientId) ->
    ets:lookup(?CONN_TAB, ClientId).

%% @doc Register a connection.
-spec(register_connection(emqx_types:client_id() | {emqx_types:client_id(), pid()}) -> ok).
register_connection(ClientId) when is_binary(ClientId) ->
    register_connection({ClientId, self()});

register_connection(Conn = {ClientId, ConnPid}) when is_binary(ClientId), is_pid(ConnPid) ->
    _ = ets:insert(?CONN_TAB, Conn),
    notify({registered, ClientId, ConnPid}).

-spec(register_connection(emqx_types:client_id() | {emqx_types:client_id(), pid()}, list()) -> ok).
register_connection(ClientId, Attrs) when is_binary(ClientId) ->
    register_connection({ClientId, self()}, Attrs);
register_connection(Conn = {ClientId, ConnPid}, Attrs) when is_binary(ClientId), is_pid(ConnPid) ->
    set_conn_attrs(Conn, Attrs),
    register_connection(Conn).

%% @doc Get conn attrs
-spec(get_conn_attrs({emqx_types:client_id(), pid()}) -> list()).
get_conn_attrs(Conn = {ClientId, ConnPid}) when is_binary(ClientId), is_pid(ConnPid) ->
    try
        ets:lookup_element(?CONN_ATTRS_TAB, Conn, 2)
    catch
        error:badarg -> []
    end.

%% @doc Set conn attrs
set_conn_attrs(ClientId, Attrs) when is_binary(ClientId) ->
    set_conn_attrs({ClientId, self()}, Attrs);
set_conn_attrs(Conn = {ClientId, ConnPid}, Attrs) when is_binary(ClientId), is_pid(ConnPid) ->
    ets:insert(?CONN_ATTRS_TAB, {Conn, Attrs}).

%% @doc Unregister a conn.
-spec(unregister_connection(emqx_types:client_id() | {emqx_types:client_id(), pid()}) -> ok).
unregister_connection(ClientId) when is_binary(ClientId) ->
    unregister_connection({ClientId, self()});

unregister_connection(Conn = {ClientId, ConnPid}) when is_binary(ClientId), is_pid(ConnPid) ->
    _ = ets:delete(?CONN_STATS_TAB, Conn),
    _ = ets:delete(?CONN_ATTRS_TAB, Conn),
    _ = ets:delete_object(?CONN_TAB, Conn),
    notify({unregistered, ClientId, ConnPid}).

%% @doc Lookup connection pid
-spec(lookup_conn_pid(emqx_types:client_id()) -> pid() | undefined).
lookup_conn_pid(ClientId) when is_binary(ClientId) ->
    case ets:lookup(?CONN_TAB, ClientId) of
        [] -> undefined;
        [{_, Pid}] -> Pid
    end.

%% @doc Get conn stats
-spec(get_conn_stats({emqx_types:client_id(), pid()}) -> list(emqx_stats:stats())).
get_conn_stats(Conn = {ClientId, ConnPid}) when is_binary(ClientId), is_pid(ConnPid) ->
    try ets:lookup_element(?CONN_STATS_TAB, Conn, 2)
    catch
        error:badarg -> []
    end.

%% @doc Set conn stats.
-spec(set_conn_stats(emqx_types:client_id(), list(emqx_stats:stats())) -> boolean()).
set_conn_stats(ClientId, Stats) when is_binary(ClientId) ->
    set_conn_stats({ClientId, self()}, Stats);

set_conn_stats(Conn = {ClientId, ConnPid}, Stats) when is_binary(ClientId), is_pid(ConnPid) ->
    ets:insert(?CONN_STATS_TAB, {Conn, Stats}).

notify(Msg) ->
    gen_server:cast(?CM, {notify, Msg}).

%%-----------------------------------------------------------------------------
%% gen_server callbacks
%%-----------------------------------------------------------------------------

init([]) ->
    TabOpts = [public, set, {write_concurrency, true}],
    ok = emqx_tables:new(?CONN_TAB, [{read_concurrency, true} | TabOpts]),
    ok = emqx_tables:new(?CONN_ATTRS_TAB, TabOpts),
    ok = emqx_tables:new(?CONN_STATS_TAB, TabOpts),
    ok = emqx_stats:update_interval(cm_stats, fun ?MODULE:update_conn_stats/0),
    {ok, #{conn_pmon => emqx_pmon:new()}}.

handle_call(Req, _From, State) ->
    emqx_logger:error("[CM] unexpected call: ~p", [Req]),
    {reply, ignored, State}.

handle_cast({notify, {registered, ClientId, ConnPid}}, State = #{conn_pmon := PMon}) ->
    {noreply, State#{conn_pmon := emqx_pmon:monitor(ConnPid, ClientId, PMon)}};

handle_cast({notify, {unregistered, _ClientId, ConnPid}}, State = #{conn_pmon := PMon}) ->
    {noreply, State#{conn_pmon := emqx_pmon:demonitor(ConnPid, PMon)}};

handle_cast(Msg, State) ->
    emqx_logger:error("[CM] unexpected cast: ~p", [Msg]),
    {noreply, State}.

handle_info({'DOWN', _MRef, process, ConnPid, _Reason}, State = #{conn_pmon := PMon}) ->
    case emqx_pmon:find(ConnPid, PMon) of
        undefined ->
            {noreply, State};
        ClientId ->
            unregister_connection({ClientId, ConnPid}),
            {noreply, State#{conn_pmon := emqx_pmon:erase(ConnPid, PMon)}}
    end;

handle_info(Info, State) ->
    emqx_logger:error("[CM] unexpected info: ~p", [Info]),
    {noreply, State}.

terminate(_Reason, _State) ->
    emqx_stats:cancel_update(cm_stats).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------

update_conn_stats() ->
    case ets:info(?CONN_TAB, size) of
        undefined -> ok;
        Size -> emqx_stats:setstat('connections/count', 'connections/max', Size)
    end.

