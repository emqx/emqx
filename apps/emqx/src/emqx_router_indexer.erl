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
    emqx_route_index:match(Topic, ?ROUTE_INDEX).

-spec peek_last_update() -> maybe(update()).
peek_last_update() ->
    gen_server:call(?INDEXER, peek_last_update).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

-spec init(_) -> {ok, state()}.
init([]) ->
    ok = emqx_route_index:init(?ROUTE_INDEX),
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
    _ = build_index(),
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
    _ = insert_unsafe(Topic, Dest),
    State#{last_update => {write, Topic, Dest}};
handle_table_event(delete_object, {?ROUTE_TAB, Topic, Dest}, State) ->
    _ = delete_unsafe(Topic, Dest),
    State#{last_update => {delete, Topic, Dest}};
%% TODO
%% handle_table_event(delete, {?ROUTE_TAB, Topic}, State) ->
%%     _ = delete_topic(Topic),
%%     State;
handle_table_event(write, {schema, ?ROUTE_TAB, _}, State) ->
    State;
handle_table_event(delete, {schema, ?ROUTE_TAB}, State) ->
    true = emqx_topic_index:clean(?ROUTE_INDEX),
    State.

-define(BATCH_SIZE, 1000).

build_index() ->
    AccIn = #{n => 1, count => 0, batch => [], refs => []},
    AccOut = submit_batch(emqx_router:foldl_routes(fun submit_batches/2, AccIn)),
    wait_batch_completions(maps:get(refs, AccOut)).

submit_batches(Route, Acc = #{count := ?BATCH_SIZE}) ->
    submit_batches(Route, submit_batch(Acc));
submit_batches(Route, #{count := Count, batch := Batch} = Acc) ->
    Acc#{count => Count + 1, batch => [Route | Batch]}.

submit_batch(#{n := N, count := Count, batch := Batch, refs := Refs}) when Count > 0 ->
    Ref = {batch, N},
    ok = emqx_pool:async_submit(fun insert_batch/3, [Ref, Batch, self()]),
    #{n => N + 1, count => 0, batch => [], refs => [Ref | Refs]};
submit_batch(Acc) ->
    Acc.

insert_batch(Ref, Batch, ReplyTo) ->
    ok = lists:foreach(fun insert_transactional/1, Batch),
    ReplyTo ! {Ref, ok}.

wait_batch_completions([Ref | Rest]) ->
    receive
        {Ref, ok} ->
            wait_batch_completions(Rest);
        {Ref, Error} ->
            ?SLOG(error, #{msg => "batch_insert_failed", error => Error}),
            wait_batch_completions(Rest)
    end;
wait_batch_completions([]) ->
    ok.

insert_transactional(Route) ->
    emqx_route_index:insert(Route, ?ROUTE_INDEX, transactional).

insert_unsafe(Topic, Dest) ->
    emqx_route_index:insert(Topic, Dest, ?ROUTE_INDEX, unsafe).

delete_unsafe(Topic, Dest) ->
    emqx_route_index:delete(Topic, Dest, ?ROUTE_INDEX, unsafe).
