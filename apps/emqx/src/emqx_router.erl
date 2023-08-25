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

-module(emqx_router).

-behaviour(gen_server).

-include("emqx.hrl").
-include("logger.hrl").
-include("types.hrl").
-include_lib("emqx/include/emqx_router.hrl").

%% Mnesia bootstrap
-export([mnesia/1]).

-boot_mnesia({mnesia, [boot]}).

-export([start_link/2]).

%% Route APIs
-export([
    add_route/1,
    add_route/2,
    do_add_route/1,
    do_add_route/2
]).

-export([
    delete_route/1,
    delete_route/2,
    do_delete_route/1,
    do_delete_route/2
]).

-export([cleanup_routes/1]).

-export([
    match_routes/1,
    lookup_routes/1,
    has_routes/1
]).

-export([print_routes/1]).

-export([
    foldl_routes/2,
    foldr_routes/2
]).

-export([topics/0]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

%% test / debugging purposes
-export([is_unified_table_active/0]).

-type group() :: binary().

-type dest() :: node() | {group(), node()}.

-record(routeidx, {
    entry :: emqx_topic_index:key(dest()),
    unused = [] :: nil()
}).

-dialyzer({nowarn_function, [cleanup_routes_regular/1]}).

%%--------------------------------------------------------------------
%% Mnesia bootstrap
%%--------------------------------------------------------------------

mnesia(boot) ->
    mria_config:set_dirty_shard(?ROUTE_SHARD, true),
    ok = mria:create_table(?ROUTE_TAB, [
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
    ]),
    ok = mria:create_table(?ROUTE_TAB_UNIFIED, [
        {type, ordered_set},
        {rlog_shard, ?ROUTE_SHARD},
        {storage, ram_copies},
        {record_name, routeidx},
        {attributes, record_info(fields, routeidx)},
        {storage_properties, [
            {ets, [
                {read_concurrency, true},
                {write_concurrency, auto}
            ]}
        ]}
    ]).

%%--------------------------------------------------------------------
%% Start a router
%%--------------------------------------------------------------------

-spec start_link(atom(), pos_integer()) -> startlink_ret().
start_link(Pool, Id) ->
    gen_server:start_link(
        {local, emqx_utils:proc_name(?MODULE, Id)},
        ?MODULE,
        [Pool, Id],
        [{hibernate_after, 1000}]
    ).

%%--------------------------------------------------------------------
%% Route APIs
%%--------------------------------------------------------------------

-spec add_route(emqx_types:topic()) -> ok | {error, term()}.
add_route(Topic) when is_binary(Topic) ->
    add_route(Topic, node()).

-spec add_route(emqx_types:topic(), dest()) -> ok | {error, term()}.
add_route(Topic, Dest) when is_binary(Topic) ->
    call(pick(Topic), {add_route, Topic, Dest}).

-spec do_add_route(emqx_types:topic()) -> ok | {error, term()}.
do_add_route(Topic) when is_binary(Topic) ->
    do_add_route(Topic, node()).

-spec do_add_route(emqx_types:topic(), dest()) -> ok | {error, term()}.
do_add_route(Topic, Dest) when is_binary(Topic) ->
    case has_route(Topic, Dest) of
        true ->
            ok;
        false ->
            ok = emqx_router_helper:monitor(Dest),
            mria_insert_route(is_unified_table_active(), Topic, Dest)
    end.

mria_insert_route(_Unified = true, Topic, Dest) ->
    mria_insert_route_unified(Topic, Dest);
mria_insert_route(_Unified = false, Topic, Dest) ->
    Route = #route{topic = Topic, dest = Dest},
    case emqx_topic:wildcard(Topic) of
        true ->
            mria_insert_route_update_trie(Route);
        false ->
            mria_insert_route(Route)
    end.

mria_insert_route_unified(Topic, Dest) ->
    K = emqx_topic_index:make_key(Topic, Dest),
    mria:dirty_write(?ROUTE_TAB_UNIFIED, #routeidx{entry = K}).

mria_insert_route_update_trie(Route) ->
    emqx_router_utils:maybe_trans(
        fun emqx_router_utils:insert_trie_route/2,
        [?ROUTE_TAB, Route],
        ?ROUTE_SHARD
    ).

mria_insert_route(Route) ->
    mria:dirty_write(?ROUTE_TAB, Route).

%% @doc Match routes
-spec match_routes(emqx_types:topic()) -> [emqx_types:route()].
match_routes(Topic) when is_binary(Topic) ->
    match_routes(is_unified_table_active(), Topic).

match_routes(_Unified = true, Topic) ->
    [match_to_route(M) || M <- match_unified(Topic)];
match_routes(_Unified = false, Topic) ->
    lookup_routes_regular(Topic) ++
        lists:flatmap(fun lookup_routes_regular/1, match_global_trie(Topic)).

match_unified(Topic) ->
    emqx_topic_index:matches(Topic, ?ROUTE_TAB_UNIFIED, []).

match_global_trie(Topic) ->
    case emqx_trie:empty() of
        true -> [];
        false -> emqx_trie:match(Topic)
    end.

-spec lookup_routes(emqx_types:topic()) -> [emqx_types:route()].
lookup_routes(Topic) ->
    case is_unified_table_active() of
        true ->
            lookup_routes_unified(Topic);
        false ->
            lookup_routes_regular(Topic)
    end.

lookup_routes_unified(Topic) ->
    Pat = #routeidx{entry = emqx_topic_index:make_key(Topic, '$1')},
    [Dest || [Dest] <- ets:match(?ROUTE_TAB_UNIFIED, Pat)].

lookup_routes_regular(Topic) ->
    ets:lookup(?ROUTE_TAB, Topic).

match_to_route(M) ->
    #route{topic = emqx_topic_index:get_topic(M), dest = emqx_topic_index:get_id(M)}.

-spec has_routes(emqx_types:topic()) -> boolean().
has_routes(Topic) when is_binary(Topic) ->
    case is_unified_table_active() of
        true ->
            has_routes_unified(Topic);
        false ->
            has_routes_regular(Topic)
    end.

has_routes_unified(Topic) ->
    Pat = #routeidx{entry = emqx_topic_index:mk_key(Topic, '$1'), _ = '_'},
    case ets:match(?ROUTE_TAB_UNIFIED, Pat, 1) of
        {[_], _} ->
            true;
        _ ->
            false
    end.

has_routes_regular(Topic) ->
    ets:member(?ROUTE_TAB, Topic).

-spec has_route(emqx_types:topic(), dest()) -> boolean().
has_route(Topic, Dest) ->
    case is_unified_table_active() of
        true ->
            has_route_unified(Topic, Dest);
        false ->
            has_route_regular(Topic, Dest)
    end.

has_route_unified(Topic, Dest) ->
    ets:member(?ROUTE_TAB_UNIFIED, emqx_topic_index:make_key(Topic, Dest)).

has_route_regular(Topic, Dest) ->
    lists:any(fun(Route) -> Route#route.dest =:= Dest end, ets:lookup(?ROUTE_TAB, Topic)).

-spec delete_route(emqx_types:topic()) -> ok | {error, term()}.
delete_route(Topic) when is_binary(Topic) ->
    delete_route(Topic, node()).

-spec delete_route(emqx_types:topic(), dest()) -> ok | {error, term()}.
delete_route(Topic, Dest) when is_binary(Topic) ->
    call(pick(Topic), {delete_route, Topic, Dest}).

-spec do_delete_route(emqx_types:topic()) -> ok | {error, term()}.
do_delete_route(Topic) when is_binary(Topic) ->
    do_delete_route(Topic, node()).

-spec do_delete_route(emqx_types:topic(), dest()) -> ok | {error, term()}.
do_delete_route(Topic, Dest) ->
    mria_delete_route(is_unified_table_active(), Topic, Dest).

mria_delete_route(_Unified = true, Topic, Dest) ->
    mria_delete_route_unified(Topic, Dest);
mria_delete_route(_Unified = false, Topic, Dest) ->
    Route = #route{topic = Topic, dest = Dest},
    case emqx_topic:wildcard(Topic) of
        true ->
            mria_delete_route_update_trie(Route);
        false ->
            mria_delete_route(Route)
    end.

mria_delete_route_unified(Topic, Dest) ->
    K = emqx_topic_index:make_key(Topic, Dest),
    mria:dirty_delete(?ROUTE_TAB_UNIFIED, K).

mria_delete_route_update_trie(Route) ->
    emqx_router_utils:maybe_trans(
        fun emqx_router_utils:delete_trie_route/2,
        [?ROUTE_TAB, Route],
        ?ROUTE_SHARD
    ).

mria_delete_route(Route) ->
    mria:dirty_delete_object(?ROUTE_TAB, Route).

-spec is_unified_table_active() -> boolean().
is_unified_table_active() ->
    is_empty(?ROUTE_TAB) andalso
        ((not is_empty(?ROUTE_TAB_UNIFIED)) orelse
            emqx_config:get([broker, unified_routing_table])).

is_empty(Tab) ->
    % NOTE
    % Supposedly, should be better than `ets:info(Tab, size)` because the latter suffers
    % from `{decentralized_counters, true}` which is default when `write_concurrency` is
    % either `auto` or `true`.
    ets:first(Tab) =:= '$end_of_table'.

-spec topics() -> list(emqx_types:topic()).
topics() ->
    topics(is_unified_table_active()).

topics(_Unified = true) ->
    Pat = #routeidx{entry = '$1'},
    [emqx_topic_index:get_topic(K) || [K] <- ets:match(?ROUTE_TAB_UNIFIED, Pat)];
topics(_Unified = false) ->
    mnesia:dirty_all_keys(?ROUTE_TAB).

%% @doc Print routes to a topic
-spec print_routes(emqx_types:topic()) -> ok.
print_routes(Topic) ->
    lists:foreach(
        fun(#route{topic = To, dest = Dest}) ->
            io:format("~ts -> ~ts~n", [To, Dest])
        end,
        match_routes(Topic)
    ).

-spec cleanup_routes(node()) -> ok.
cleanup_routes(Node) ->
    case is_unified_table_active() of
        true ->
            cleanup_routes_unified(Node);
        false ->
            cleanup_routes_regular(Node)
    end.

cleanup_routes_unified(Node) ->
    % NOTE
    % No point in transaction here because all the operations on unified routing table
    % are dirty.
    ets:foldl(
        fun(#routeidx{entry = K}, ok) ->
            case emqx_topic_index:get_id(K) of
                Node ->
                    mria:dirty_delete(?ROUTE_TAB_UNIFIED, K);
                _ ->
                    ok
            end
        end,
        ok,
        ?ROUTE_TAB_UNIFIED
    ).

cleanup_routes_regular(Node) ->
    Patterns = [
        #route{_ = '_', dest = Node},
        #route{_ = '_', dest = {'_', Node}}
    ],
    mria:transaction(?ROUTE_SHARD, fun() ->
        [
            mnesia:delete_object(?ROUTE_TAB, Route, write)
         || Pat <- Patterns,
            Route <- mnesia:match_object(?ROUTE_TAB, Pat, write)
        ]
    end).

-spec foldl_routes(fun((emqx_types:route(), Acc) -> Acc), Acc) -> Acc.
foldl_routes(FoldFun, AccIn) ->
    case is_unified_table_active() of
        true ->
            ets:foldl(mk_fold_fun_unified(FoldFun), AccIn, ?ROUTE_TAB_UNIFIED);
        false ->
            ets:foldl(FoldFun, AccIn, ?ROUTE_TAB)
    end.

-spec foldr_routes(fun((emqx_types:route(), Acc) -> Acc), Acc) -> Acc.
foldr_routes(FoldFun, AccIn) ->
    case is_unified_table_active() of
        true ->
            ets:foldr(mk_fold_fun_unified(FoldFun), AccIn, ?ROUTE_TAB_UNIFIED);
        false ->
            ets:foldr(FoldFun, AccIn, ?ROUTE_TAB)
    end.

mk_fold_fun_unified(FoldFun) ->
    fun(#routeidx{entry = K}, Acc) -> FoldFun(match_to_route(K), Acc) end.

call(Router, Msg) ->
    gen_server:call(Router, Msg, infinity).

pick(Topic) ->
    gproc_pool:pick_worker(router_pool, Topic).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([Pool, Id]) ->
    true = gproc_pool:connect_worker(Pool, {Pool, Id}),
    {ok, #{pool => Pool, id => Id}}.

handle_call({add_route, Topic, Dest}, _From, State) ->
    Ok = do_add_route(Topic, Dest),
    {reply, Ok, State};
handle_call({delete_route, Topic, Dest}, _From, State) ->
    Ok = do_delete_route(Topic, Dest),
    {reply, Ok, State};
handle_call(Req, _From, State) ->
    ?SLOG(error, #{msg => "unexpected_call", call => Req}),
    {reply, ignored, State}.

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
