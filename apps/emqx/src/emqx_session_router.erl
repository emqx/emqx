%%--------------------------------------------------------------------
%% Copyright (c) 2021-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_session_router).

-behaviour(gen_server).

-include("emqx.hrl").
-include("logger.hrl").
-include("types.hrl").

-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-export([
    create_init_tab/0,
    create_router_tab/1,
    start_link/2
]).

%% Route APIs
-export([
    delete_routes/2,
    do_add_route/2,
    do_delete_route/2,
    match_routes/1
]).

-export([
    buffer/3,
    pending/2,
    resume_begin/2,
    resume_end/2
]).

-export([print_routes/1]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-type group() :: binary().

-type dest() :: node() | {group(), node()}.

-define(ROUTE_RAM_TAB, emqx_session_route_ram).
-define(ROUTE_DISC_TAB, emqx_session_route_disc).

-define(SESSION_INIT_TAB, session_init_tab).

%%--------------------------------------------------------------------
%% Mnesia bootstrap
%%--------------------------------------------------------------------

create_router_tab(disc) ->
    ok = mria:create_table(?ROUTE_DISC_TAB, [
        {type, bag},
        {rlog_shard, ?ROUTE_SHARD},
        {storage, disc_copies},
        {record_name, route},
        {attributes, record_info(fields, route)},
        {storage_properties, [
            {ets, [
                {read_concurrency, true},
                {write_concurrency, true}
            ]}
        ]}
    ]);
create_router_tab(ram) ->
    ok = mria:create_table(?ROUTE_RAM_TAB, [
        {type, bag},
        {rlog_shard, ?ROUTE_SHARD},
        {storage, ram_copies},
        {record_name, route},
        {attributes, record_info(fields, route)},
        {storage_properties, [
            {ets, [
                {read_concurrency, true},
                {write_concurrency, true}
            ]}
        ]}
    ]).

%%--------------------------------------------------------------------
%% Start a router
%%--------------------------------------------------------------------

create_init_tab() ->
    emqx_tables:new(?SESSION_INIT_TAB, [
        public,
        {read_concurrency, true},
        {write_concurrency, true}
    ]).

-spec start_link(atom(), pos_integer()) -> startlink_ret().
start_link(Pool, Id) ->
    gen_server:start_link(
        {local, emqx_misc:proc_name(?MODULE, Id)},
        ?MODULE,
        [Pool, Id],
        [{hibernate_after, 1000}]
    ).

%%--------------------------------------------------------------------
%% Route APIs
%%--------------------------------------------------------------------

-spec do_add_route(emqx_topic:topic(), dest()) -> ok | {error, term()}.
do_add_route(Topic, SessionID) when is_binary(Topic) ->
    Route = #route{topic = Topic, dest = SessionID},
    case lists:member(Route, lookup_routes(Topic)) of
        true ->
            ok;
        false ->
            case emqx_topic:wildcard(Topic) of
                true ->
                    Fun = fun emqx_router_utils:insert_session_trie_route/2,
                    emqx_router_utils:maybe_trans(
                        Fun,
                        [route_tab(), Route],
                        ?PERSISTENT_SESSION_SHARD
                    );
                false ->
                    emqx_router_utils:insert_direct_route(route_tab(), Route)
            end
    end.

%% @doc Match routes
-spec match_routes(emqx_topic:topic()) -> [emqx_types:route()].
match_routes(Topic) when is_binary(Topic) ->
    case match_trie(Topic) of
        [] -> lookup_routes(Topic);
        Matched -> lists:append([lookup_routes(To) || To <- [Topic | Matched]])
    end.

%% Optimize: routing table will be replicated to all router nodes.
match_trie(Topic) ->
    case emqx_trie:empty_session() of
        true -> [];
        false -> emqx_trie:match_session(Topic)
    end.

%% Async
delete_routes(SessionID, Subscriptions) ->
    cast(pick(SessionID), {delete_routes, SessionID, Subscriptions}).

-spec do_delete_route(emqx_topic:topic(), dest()) -> ok | {error, term()}.
do_delete_route(Topic, SessionID) ->
    Route = #route{topic = Topic, dest = SessionID},
    case emqx_topic:wildcard(Topic) of
        true ->
            Fun = fun emqx_router_utils:delete_session_trie_route/2,
            emqx_router_utils:maybe_trans(Fun, [route_tab(), Route], ?PERSISTENT_SESSION_SHARD);
        false ->
            emqx_router_utils:delete_direct_route(route_tab(), Route)
    end.

%% @doc Print routes to a topic
-spec print_routes(emqx_topic:topic()) -> ok.
print_routes(Topic) ->
    lists:foreach(
        fun(#route{topic = To, dest = SessionID}) ->
            io:format("~s -> ~p~n", [To, SessionID])
        end,
        match_routes(Topic)
    ).

%%--------------------------------------------------------------------
%% Session APIs
%%--------------------------------------------------------------------

pending(SessionID, MarkerIDs) ->
    call(pick(SessionID), {pending, SessionID, MarkerIDs}).

buffer(SessionID, STopic, Msg) ->
    case emqx_tables:lookup_value(?SESSION_INIT_TAB, SessionID) of
        undefined -> ok;
        Worker -> emqx_session_router_worker:buffer(Worker, STopic, Msg)
    end.

-spec resume_begin(pid(), binary()) -> [{node(), emqx_guid:guid()}].
resume_begin(From, SessionID) when is_pid(From), is_binary(SessionID) ->
    call(pick(SessionID), {resume_begin, From, SessionID}).

-spec resume_end(pid(), binary()) ->
    {'ok', [emqx_types:message()]} | {'error', term()}.
resume_end(From, SessionID) when is_pid(From), is_binary(SessionID) ->
    case emqx_tables:lookup_value(?SESSION_INIT_TAB, SessionID) of
        undefined ->
            ?tp(ps_session_not_found, #{sid => SessionID}),
            {error, not_found};
        Pid ->
            Res = emqx_session_router_worker:resume_end(From, Pid, SessionID),
            cast(pick(SessionID), {resume_end, SessionID, Pid}),
            Res
    end.

%%--------------------------------------------------------------------
%% Worker internals
%%--------------------------------------------------------------------

call(Router, Msg) ->
    gen_server:call(Router, Msg, infinity).

cast(Router, Msg) ->
    gen_server:cast(Router, Msg).

pick(#route{dest = SessionID}) ->
    gproc_pool:pick_worker(session_router_pool, SessionID);
pick(SessionID) when is_binary(SessionID) ->
    gproc_pool:pick_worker(session_router_pool, SessionID).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([Pool, Id]) ->
    true = gproc_pool:connect_worker(Pool, {Pool, Id}),
    {ok, #{pool => Pool, id => Id, pmon => emqx_pmon:new()}}.

handle_call({resume_begin, RemotePid, SessionID}, _From, State) ->
    case init_resume_worker(RemotePid, SessionID, State) of
        error ->
            {reply, error, State};
        {ok, Pid, State1} ->
            ets:insert(?SESSION_INIT_TAB, {SessionID, Pid}),
            MarkerID = emqx_persistent_session:mark_resume_begin(SessionID),
            {reply, {ok, MarkerID}, State1}
    end;
handle_call({pending, SessionID, MarkerIDs}, _From, State) ->
    Res = emqx_persistent_session:pending_messages_in_db(SessionID, MarkerIDs),
    {reply, Res, State};
handle_call(Req, _From, State) ->
    ?SLOG(error, #{msg => "unexpected_call", req => Req}),
    {reply, ignored, State}.

handle_cast({delete_routes, SessionID, Subscriptions}, State) ->
    %% TODO: Make a batch for deleting all routes.
    Fun = fun({Topic, _}) -> do_delete_route(Topic, SessionID) end,
    ok = lists:foreach(Fun, maps:to_list(Subscriptions)),
    {noreply, State};
handle_cast({resume_end, SessionID, Pid}, State) ->
    case emqx_tables:lookup_value(?SESSION_INIT_TAB, SessionID) of
        undefined -> skip;
        P when P =:= Pid -> ets:delete(?SESSION_INIT_TAB, SessionID);
        P when is_pid(P) -> skip
    end,
    Pmon = emqx_pmon:demonitor(Pid, maps:get(pmon, State)),
    _ = emqx_session_router_worker_sup:abort_worker(Pid),
    {noreply, State#{pmon => Pmon}};
handle_cast(Msg, State) ->
    ?SLOG(error, #{msg => "unexpected_cast", cast => Msg}),
    {noreply, State}.

handle_info(Info, State) ->
    ?SLOG(error, #{msg => "unexpected_info", info => Info}),
    {noreply, State}.

terminate(_Reason, #{pool := Pool, id := Id}) ->
    gproc_pool:disconnect_worker(Pool, {Pool, Id}).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% Resume worker. A process that buffers the persisted messages during
%% initialisation of a resuming session.
%%--------------------------------------------------------------------

init_resume_worker(RemotePid, SessionID, #{pmon := Pmon} = State) ->
    case emqx_session_router_worker_sup:start_worker(SessionID, RemotePid) of
        {error, What} ->
            ?SLOG(error, #{msg => "failed_to_start_resume_worker", reason => What}),
            error;
        {ok, Pid} ->
            Pmon1 = emqx_pmon:monitor(Pid, Pmon),
            case emqx_tables:lookup_value(?SESSION_INIT_TAB, SessionID) of
                undefined ->
                    {ok, Pid, State#{pmon => Pmon1}};
                {_, OldPid} ->
                    Pmon2 = emqx_pmon:demonitor(OldPid, Pmon1),
                    _ = emqx_session_router_worker_sup:abort_worker(OldPid),
                    {ok, Pid, State#{pmon => Pmon2}}
            end
    end.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

lookup_routes(Topic) ->
    ets:lookup(route_tab(), Topic).

route_tab() ->
    case emqx_persistent_session:storage_type() of
        disc -> ?ROUTE_DISC_TAB;
        ram -> ?ROUTE_RAM_TAB
    end.
