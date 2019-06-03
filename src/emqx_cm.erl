%% Copyright (c) 2013-2019 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-include("logger.hrl").
-include("types.hrl").

-export([start_link/0]).

-export([ register_connection/1
        , register_connection/2
        , unregister_connection/1
        , unregister_connection/2
        ]).

-export([ get_conn_attrs/1
        , get_conn_attrs/2
        , set_conn_attrs/2
        , set_conn_attrs/3
        ]).

-export([ get_conn_stats/1
        , get_conn_stats/2
        , set_conn_stats/2
        , set_conn_stats/3
        ]).

-export([lookup_conn_pid/1]).

%% gen_server callbacks
-export([ init/1
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , terminate/2
        , code_change/3
        ]).

%% internal export
-export([stats_fun/0]).

-define(CM, ?MODULE).

%% ETS tables for connection management.
-define(CONN_TAB, emqx_conn).
-define(CONN_ATTRS_TAB, emqx_conn_attrs).
-define(CONN_STATS_TAB, emqx_conn_stats).

-define(BATCH_SIZE, 100000).

%% @doc Start the connection manager.
-spec(start_link() -> startlink_ret()).
start_link() ->
    gen_server:start_link({local, ?CM}, ?MODULE, [], []).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

%% @doc Register a connection.
-spec(register_connection(emqx_types:client_id()) -> ok).
register_connection(ClientId) when is_binary(ClientId) ->
    register_connection(ClientId, self()).

-spec(register_connection(emqx_types:client_id(), pid()) -> ok).
register_connection(ClientId, ConnPid) when is_binary(ClientId), is_pid(ConnPid) ->
    true = ets:insert(?CONN_TAB, {ClientId, ConnPid}),
    notify({registered, ClientId, ConnPid}).

%% @doc Unregister a connection.
-spec(unregister_connection(emqx_types:client_id()) -> ok).
unregister_connection(ClientId) when is_binary(ClientId) ->
    unregister_connection(ClientId, self()).

-spec(unregister_connection(emqx_types:client_id(), pid()) -> ok).
unregister_connection(ClientId, ConnPid) when is_binary(ClientId), is_pid(ConnPid) ->
    true = do_unregister_connection({ClientId, ConnPid}),
    notify({unregistered, ConnPid}).

do_unregister_connection(Conn) ->
    true = ets:delete(?CONN_STATS_TAB, Conn),
    true = ets:delete(?CONN_ATTRS_TAB, Conn),
    true = ets:delete_object(?CONN_TAB, Conn).

%% @doc Get conn attrs
-spec(get_conn_attrs(emqx_types:client_id()) -> list()).
get_conn_attrs(ClientId) when is_binary(ClientId) ->
    ConnPid = lookup_conn_pid(ClientId),
    get_conn_attrs(ClientId, ConnPid).

-spec(get_conn_attrs(emqx_types:client_id(), pid()) -> list()).
get_conn_attrs(ClientId, ConnPid) when is_binary(ClientId) ->
    emqx_tables:lookup_value(?CONN_ATTRS_TAB, {ClientId, ConnPid}, []).

%% @doc Set conn attrs
-spec(set_conn_attrs(emqx_types:client_id(), list()) -> true).
set_conn_attrs(ClientId, Attrs) when is_binary(ClientId) ->
    set_conn_attrs(ClientId, self(), Attrs).

-spec(set_conn_attrs(emqx_types:client_id(), pid(), list()) -> true).
set_conn_attrs(ClientId, ConnPid, Attrs) when is_binary(ClientId), is_pid(ConnPid) ->
    Conn = {ClientId, ConnPid},
    ets:insert(?CONN_ATTRS_TAB, {Conn, Attrs}).

%% @doc Get conn stats
-spec(get_conn_stats(emqx_types:client_id()) -> list(emqx_stats:stats())).
get_conn_stats(ClientId) when is_binary(ClientId) ->
    ConnPid = lookup_conn_pid(ClientId),
    get_conn_stats(ClientId, ConnPid).

-spec(get_conn_stats(emqx_types:client_id(), pid()) -> list(emqx_stats:stats())).
get_conn_stats(ClientId, ConnPid) when is_binary(ClientId) ->
    Conn = {ClientId, ConnPid},
    emqx_tables:lookup_value(?CONN_STATS_TAB, Conn, []).

%% @doc Set conn stats.
-spec(set_conn_stats(emqx_types:client_id(), list(emqx_stats:stats())) -> true).
set_conn_stats(ClientId, Stats) when is_binary(ClientId) ->
    set_conn_stats(ClientId, self(), Stats).

-spec(set_conn_stats(emqx_types:client_id(), pid(), list(emqx_stats:stats())) -> true).
set_conn_stats(ClientId, ConnPid, Stats) when is_binary(ClientId), is_pid(ConnPid) ->
    Conn = {ClientId, ConnPid},
    ets:insert(?CONN_STATS_TAB, {Conn, Stats}).

%% @doc Lookup connection pid.
-spec(lookup_conn_pid(emqx_types:client_id()) -> maybe(pid())).
lookup_conn_pid(ClientId) when is_binary(ClientId) ->
    emqx_tables:lookup_value(?CONN_TAB, ClientId).

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
    ok = emqx_stats:update_interval(conn_stats, fun ?MODULE:stats_fun/0),
    {ok, #{conn_pmon => emqx_pmon:new()}}.

handle_call(Req, _From, State) ->
    ?LOG(error, "[CM] Unexpected call: ~p", [Req]),
    {reply, ignored, State}.

handle_cast({notify, {registered, ClientId, ConnPid}}, State = #{conn_pmon := PMon}) ->
    {noreply, State#{conn_pmon := emqx_pmon:monitor(ConnPid, ClientId, PMon)}};

handle_cast({notify, {unregistered, ConnPid}}, State = #{conn_pmon := PMon}) ->
    {noreply, State#{conn_pmon := emqx_pmon:demonitor(ConnPid, PMon)}};

handle_cast(Msg, State) ->
    ?LOG(error, "[CM] Unexpected cast: ~p", [Msg]),
    {noreply, State}.

handle_info({'DOWN', _MRef, process, Pid, _Reason}, State = #{conn_pmon := PMon}) ->
    ConnPids = [Pid | emqx_misc:drain_down(?BATCH_SIZE)],
    {Items, PMon1} = emqx_pmon:erase_all(ConnPids, PMon),
    ok = emqx_pool:async_submit(
           fun lists:foreach/2, [fun clean_down/1, Items]),
    {noreply, State#{conn_pmon := PMon1}};

handle_info(Info, State) ->
    ?LOG(error, "[CM] Unexpected info: ~p", [Info]),
    {noreply, State}.

terminate(_Reason, _State) ->
    emqx_stats:cancel_update(conn_stats).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------

clean_down({Pid, ClientId}) ->
    Conn = {ClientId, Pid},
    case ets:member(?CONN_TAB, ClientId)
         orelse ets:member(?CONN_ATTRS_TAB, Conn) of
        true ->
            do_unregister_connection(Conn);
        false -> false
    end.

stats_fun() ->
    case ets:info(?CONN_TAB, size) of
        undefined -> ok;
        Size -> emqx_stats:setstat('connections.count', 'connections.max', Size)
    end.
