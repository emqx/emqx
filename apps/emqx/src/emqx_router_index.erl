%%--------------------------------------------------------------------
%% Copyright (c) 2017-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_router_index).

-behaviour(gen_server).

-include("emqx.hrl").
-include("emqx_router.hrl").
-include("types.hrl").
-include("logger.hrl").

-define(ROUTE_INDEX_TRIE, emqx_route_index_trie).

-define(INDEXER, ?MODULE).

-type update() :: {write | delete, emqx_types:topic(), emqx_router:dest()}.
-type state() :: #{last_update := maybe(update())}.

-export([
    start_link/0,
    enabled/0
]).

-export([match/1]).

%% test only
-export([peek_last_update/0]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2
]).

-spec start_link() -> startlink_ret().
start_link() ->
    case emqx_config:get([broker, perf, trie_local_async]) of
        true ->
            gen_server:start_link({local, ?INDEXER}, ?MODULE, [], []);
        false ->
            ignore
    end.

-spec enabled() -> boolean().
enabled() ->
    erlang:whereis(?INDEXER) =/= undefined.

-spec match(emqx_types:topic()) -> [emqx_types:route()].
match(Topic) ->
    match_entries(Topic).

-spec peek_last_update() -> maybe(update()).
peek_last_update() ->
    gen_server:call(?INDEXER, peek_last_update).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

-spec init(_) -> {ok, state()}.
init([]) ->
    _ = emqx_trie:create_local(?ROUTE_INDEX_TRIE, [public, named_table]),
    ok = mria:wait_for_tables([?ROUTE_TAB]),
    % NOTE
    % Must subscribe to the events before indexing the routes to make no record is missed.
    {ok, _} = mnesia:subscribe({table, ?ROUTE_TAB, simple}),
    NRoutes = emqx_router:info(size),
    ?SLOG(info, #{
        msg => "started_route_index_building",
        num_routes => NRoutes
    }),
    TS1 = erlang:monotonic_time(millisecond),
    ok = build_index(),
    TS2 = erlang:monotonic_time(millisecond),
    ?SLOG(info, #{
        msg => "finished_route_index_building",
        num_routes => NRoutes,
        num_entries => ets:info(?ROUTE_INDEX_TRIE, size),
        time_spent_ms => TS2 - TS1
    }),
    {ok, #{last_update => undefined}}.

-spec handle_call(_Call, _From, state()) -> {reply, _Reply, state()}.
handle_call(peek_last_update, _From, State) ->
    {reply, maps:get(last_update, State, undefined), State};
handle_call(Req, {Pid, _Ref}, State) ->
    ?SLOG(error, #{msg => "unexpected_call", call => Req, from => Pid}),
    {reply, ignored, State}.

-spec handle_cast(_Msg, state()) -> {noreply, state()}.
handle_cast(Msg, State) ->
    ?SLOG(error, #{msg => "unexpected_cast", cast => Msg}),
    {noreply, State}.

-spec handle_info(_Info, state()) -> {noreply, state()}.
handle_info({mnesia_table_event, {Op, Record, _}}, State) ->
    {noreply, handle_table_event(Op, Record, State)};
handle_info(Info, State) ->
    ?SLOG(error, #{msg => "unexpected_info", info => Info}),
    {noreply, State}.

handle_table_event(write, {?ROUTE_TAB, Topic, Dest}, State) ->
    ok = insert_entry(Topic, Dest),
    State#{last_update => {write, Topic, Dest}};
handle_table_event(delete_object, {?ROUTE_TAB, Topic, Dest}, State) ->
    ok = delete_entry(Topic, Dest),
    State#{last_update => {delete, Topic, Dest}};
%% TODO
%% handle_table_event(delete, {?ROUTE_TAB, Topic}, State) ->
%%     ok = delete_topic(Topic),
%%     State;
handle_table_event(write, {schema, ?ROUTE_TAB, _}, State) ->
    State;
handle_table_event(delete, {schema, ?ROUTE_TAB}, State) ->
    ok = clean_index(),
    State.

match_entries(Topic) ->
    emqx_trie:match(Topic, ?ROUTE_INDEX_TRIE).

build_index() ->
    ok = emqx_router:foldl_routes(fun(Route, _) -> insert_entry(Route) end, ok).

insert_entry(#route{topic = Topic, dest = Dest}) ->
    insert_entry(Topic, Dest).

insert_entry(Topic, Dest) ->
    case emqx_topic:wildcard(Topic) of
        true ->
            emqx_trie:insert_local(Topic, Dest, ?ROUTE_INDEX_TRIE);
        false ->
            ok
    end.

delete_entry(Topic, Dest) ->
    case emqx_topic:wildcard(Topic) of
        true ->
            emqx_trie:delete_local(Topic, Dest, ?ROUTE_INDEX_TRIE);
        false ->
            ok
    end.

clean_index() ->
    emqx_trie:clear_local(?ROUTE_INDEX_TRIE).
