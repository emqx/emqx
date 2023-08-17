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

-module(emqx_router_indexer).

-behaviour(gen_server).

-include("emqx.hrl").
-include("emqx_router.hrl").
-include("types.hrl").
-include("logger.hrl").

-define(ROUTE_INDEX, emqx_route_topic_index).

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
    _ = ets:new(?ROUTE_INDEX, [public, named_table, ordered_set, {read_concurrency, true}]),
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
        num_entries => ets:info(?ROUTE_INDEX, size),
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

build_index() ->
    ok = emqx_router:foldl_routes(fun(Route, _) -> insert_entry(Route) end, ok).

%% Route topic index
%%
%% We maintain "compacted" index here, this is why index entries has no relevant IDs
%% associated with them. Records are mapsets of route destinations. Basically:
%% ```
%% {<<"t/route/topic/#">>, _ID = []} => #{'node1@emqx' => [], 'node2@emqx' => [], ...}
%% ```
%%
%% This layout implies that we cannot make changes concurrently, since inserting and
%% deleting entries are not atomic operations.

match_entries(Topic) ->
    Matches = emqx_topic_index:matches(Topic, ?ROUTE_INDEX, []),
    lists:flatmap(fun expand_match/1, Matches).

expand_match(Match) ->
    Topic = emqx_topic_index:get_topic(Match),
    [
        #route{topic = Topic, dest = Dest}
     || Ds <- emqx_topic_index:get_record(Match, ?ROUTE_INDEX),
        Dest <- maps:keys(Ds)
    ].

insert_entry(#route{topic = Topic, dest = Dest}) ->
    insert_entry(Topic, Dest).

insert_entry(Topic, Dest) ->
    Words = emqx_topic_index:words(Topic),
    case emqx_topic:wildcard(Words) of
        true ->
            case emqx_topic_index:lookup(Words, ID = [], ?ROUTE_INDEX) of
                [Ds = #{}] ->
                    true = emqx_topic_index:insert(Words, ID, Ds#{Dest => []}, ?ROUTE_INDEX);
                [] ->
                    true = emqx_topic_index:insert(Words, ID, #{Dest => []}, ?ROUTE_INDEX)
            end,
            ok;
        false ->
            ok
    end.

delete_entry(Topic, Dest) ->
    Words = emqx_topic_index:words(Topic),
    case emqx_topic:wildcard(Words) of
        true ->
            case emqx_topic_index:lookup(Words, ID = [], ?ROUTE_INDEX) of
                [Ds = #{Dest := _}] when map_size(Ds) =:= 1 ->
                    true = emqx_topic_index:delete(Words, ID, ?ROUTE_INDEX);
                [Ds = #{Dest := _}] ->
                    NDs = maps:remove(Dest, Ds),
                    true = emqx_topic_index:insert(Words, ID, NDs, ?ROUTE_INDEX);
                [] ->
                    true
            end,
            ok;
        false ->
            ok
    end.

clean_index() ->
    true = emqx_topic_index:clean(?ROUTE_INDEX),
    ok.
