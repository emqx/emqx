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
    has_route/2
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

-export([
    get_schema_vsn/0,
    init_schema/0,
    deinit_schema/0
]).

-type group() :: binary().

-type dest() :: node() | {group(), node()}.

-record(routeidx, {
    entry :: emqx_topic_index:key(dest()),
    unused = [] :: nil()
}).

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
    ok = mria:create_table(?ROUTE_TAB_FILTERS, [
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
            mria_insert_route(get_schema_vsn(), Topic, Dest)
    end.

mria_insert_route(v2, Topic, Dest) ->
    mria_insert_route_v2(Topic, Dest);
mria_insert_route(v1, Topic, Dest) ->
    mria_insert_route_v1(Topic, Dest).

%% @doc Match routes
-spec match_routes(emqx_types:topic()) -> [emqx_types:route()].
match_routes(Topic) when is_binary(Topic) ->
    match_routes(get_schema_vsn(), Topic).

match_routes(v2, Topic) ->
    match_routes_v2(Topic);
match_routes(v1, Topic) ->
    match_routes_v1(Topic).

-spec lookup_routes(emqx_types:topic()) -> [emqx_types:route()].
lookup_routes(Topic) ->
    lookup_routes(get_schema_vsn(), Topic).

lookup_routes(v2, Topic) ->
    lookup_routes_v2(Topic);
lookup_routes(v1, Topic) ->
    lookup_routes_v1(Topic).

-spec has_route(emqx_types:topic(), dest()) -> boolean().
has_route(Topic, Dest) ->
    has_route(get_schema_vsn(), Topic, Dest).

has_route(v2, Topic, Dest) ->
    has_route_v2(Topic, Dest);
has_route(v1, Topic, Dest) ->
    has_route_v1(Topic, Dest).

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
    mria_delete_route(get_schema_vsn(), Topic, Dest).

mria_delete_route(v2, Topic, Dest) ->
    mria_delete_route_v2(Topic, Dest);
mria_delete_route(v1, Topic, Dest) ->
    mria_delete_route_v1(Topic, Dest).

-spec topics() -> list(emqx_types:topic()).
topics() ->
    topics(get_schema_vsn()).

topics(v2) ->
    list_topics_v2();
topics(v1) ->
    list_topics_v1().

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
    cleanup_routes(get_schema_vsn(), Node).

cleanup_routes(v2, Node) ->
    cleanup_routes_v2(Node);
cleanup_routes(v1, Node) ->
    cleanup_routes_v1(Node).

-spec foldl_routes(fun((emqx_types:route(), Acc) -> Acc), Acc) -> Acc.
foldl_routes(FoldFun, AccIn) ->
    fold_routes(get_schema_vsn(), foldl, FoldFun, AccIn).

-spec foldr_routes(fun((emqx_types:route(), Acc) -> Acc), Acc) -> Acc.
foldr_routes(FoldFun, AccIn) ->
    fold_routes(get_schema_vsn(), foldr, FoldFun, AccIn).

fold_routes(v2, FunName, FoldFun, AccIn) ->
    fold_routes_v2(FunName, FoldFun, AccIn);
fold_routes(v1, FunName, FoldFun, AccIn) ->
    fold_routes_v1(FunName, FoldFun, AccIn).

call(Router, Msg) ->
    gen_server:call(Router, Msg, infinity).

pick(Topic) ->
    gproc_pool:pick_worker(router_pool, Topic).

%%--------------------------------------------------------------------
%% Schema v1
%% --------------------------------------------------------------------

-dialyzer({nowarn_function, [cleanup_routes_v1/1]}).

mria_insert_route_v1(Topic, Dest) ->
    Route = #route{topic = Topic, dest = Dest},
    case emqx_topic:wildcard(Topic) of
        true ->
            mria_route_tab_insert_update_trie(Route);
        false ->
            mria_route_tab_insert(Route)
    end.

mria_route_tab_insert_update_trie(Route) ->
    emqx_router_utils:maybe_trans(
        fun emqx_router_utils:insert_trie_route/2,
        [?ROUTE_TAB, Route],
        ?ROUTE_SHARD
    ).

mria_route_tab_insert(Route) ->
    mria:dirty_write(?ROUTE_TAB, Route).

mria_delete_route_v1(Topic, Dest) ->
    Route = #route{topic = Topic, dest = Dest},
    case emqx_topic:wildcard(Topic) of
        true ->
            mria_route_tab_delete_update_trie(Route);
        false ->
            mria_route_tab_delete(Route)
    end.

mria_route_tab_delete_update_trie(Route) ->
    emqx_router_utils:maybe_trans(
        fun emqx_router_utils:delete_trie_route/2,
        [?ROUTE_TAB, Route],
        ?ROUTE_SHARD
    ).

mria_route_tab_delete(Route) ->
    mria:dirty_delete_object(?ROUTE_TAB, Route).

match_routes_v1(Topic) ->
    lookup_route_tab(Topic) ++
        lists:flatmap(fun lookup_route_tab/1, match_global_trie(Topic)).

match_global_trie(Topic) ->
    case emqx_trie:empty() of
        true -> [];
        false -> emqx_trie:match(Topic)
    end.

lookup_routes_v1(Topic) ->
    lookup_route_tab(Topic).

lookup_route_tab(Topic) ->
    ets:lookup(?ROUTE_TAB, Topic).

has_route_v1(Topic, Dest) ->
    has_route_tab_entry(Topic, Dest).

has_route_tab_entry(Topic, Dest) ->
    [] =/= ets:match(?ROUTE_TAB, #route{topic = Topic, dest = Dest}).

cleanup_routes_v1(Node) ->
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

list_topics_v1() ->
    list_route_tab_topics().

list_route_tab_topics() ->
    mnesia:dirty_all_keys(?ROUTE_TAB).

fold_routes_v1(FunName, FoldFun, AccIn) ->
    ets:FunName(FoldFun, AccIn, ?ROUTE_TAB).

%%--------------------------------------------------------------------
%% Schema v2
%% One bag table exclusively for regular, non-filter subscription
%% topics, and one `emqx_topic_index` table exclusively for wildcard
%% topics. Writes go to only one of the two tables at a time.
%% --------------------------------------------------------------------

mria_insert_route_v2(Topic, Dest) ->
    case emqx_trie_search:filter(Topic) of
        Words when is_list(Words) ->
            K = emqx_topic_index:make_key(Words, Dest),
            mria:dirty_write(?ROUTE_TAB_FILTERS, #routeidx{entry = K});
        false ->
            mria_route_tab_insert(#route{topic = Topic, dest = Dest})
    end.

mria_delete_route_v2(Topic, Dest) ->
    case emqx_trie_search:filter(Topic) of
        Words when is_list(Words) ->
            K = emqx_topic_index:make_key(Words, Dest),
            mria:dirty_delete(?ROUTE_TAB_FILTERS, K);
        false ->
            mria_route_tab_delete(#route{topic = Topic, dest = Dest})
    end.

match_routes_v2(Topic) ->
    lookup_route_tab(Topic) ++
        [match_to_route(M) || M <- match_filters(Topic)].

match_filters(Topic) ->
    emqx_topic_index:matches(Topic, ?ROUTE_TAB_FILTERS, []).

lookup_routes_v2(Topic) ->
    case emqx_topic:wildcard(Topic) of
        true ->
            Pat = #routeidx{entry = emqx_topic_index:make_key(Topic, '$1')},
            [Dest || [Dest] <- ets:match(?ROUTE_TAB_FILTERS, Pat)];
        false ->
            lookup_route_tab(Topic)
    end.

has_route_v2(Topic, Dest) ->
    case emqx_topic:wildcard(Topic) of
        true ->
            ets:member(?ROUTE_TAB_FILTERS, emqx_topic_index:make_key(Topic, Dest));
        false ->
            has_route_tab_entry(Topic, Dest)
    end.

cleanup_routes_v2(Node) ->
    % NOTE
    % No point in transaction here because all the operations on unified routing table
    % are dirty.
    ok = ets:foldl(
        fun(#routeidx{entry = K}, ok) ->
            case get_dest_node(emqx_topic_index:get_id(K)) of
                Node ->
                    mria:dirty_delete(?ROUTE_TAB_FILTERS, K);
                _ ->
                    ok
            end
        end,
        ok,
        ?ROUTE_TAB_FILTERS
    ),
    ok = ets:foldl(
        fun(#route{dest = Dest} = Route, ok) ->
            case get_dest_node(Dest) of
                Node ->
                    mria:dirty_delete_object(?ROUTE_TAB, Route);
                _ ->
                    ok
            end
        end,
        ok,
        ?ROUTE_TAB
    ).

get_dest_node({_, Node}) ->
    Node;
get_dest_node(Node) ->
    Node.

list_topics_v2() ->
    Pat = #routeidx{entry = '$1'},
    Filters = [emqx_topic_index:get_topic(K) || [K] <- ets:match(?ROUTE_TAB_FILTERS, Pat)],
    list_route_tab_topics() ++ Filters.

fold_routes_v2(FunName, FoldFun, AccIn) ->
    FilterFoldFun = mk_filtertab_fold_fun(FoldFun),
    Acc = ets:FunName(FoldFun, AccIn, ?ROUTE_TAB),
    ets:FunName(FilterFoldFun, Acc, ?ROUTE_TAB_FILTERS).

mk_filtertab_fold_fun(FoldFun) ->
    fun(#routeidx{entry = K}, Acc) -> FoldFun(match_to_route(K), Acc) end.

match_to_route(M) ->
    #route{topic = emqx_topic_index:get_topic(M), dest = emqx_topic_index:get_id(M)}.

%%--------------------------------------------------------------------
%% Routing table type
%% --------------------------------------------------------------------

-define(PT_SCHEMA_VSN, {?MODULE, schemavsn}).

-type schemavsn() :: v1 | v2.

-spec get_schema_vsn() -> schemavsn().
get_schema_vsn() ->
    persistent_term:get(?PT_SCHEMA_VSN).

-spec init_schema() -> ok.
init_schema() ->
    ConfSchema = emqx_config:get([broker, routing, storage_schema]),
    Schema = choose_schema_vsn(ConfSchema),
    ok = persistent_term:put(?PT_SCHEMA_VSN, Schema),
    case Schema of
        ConfSchema ->
            ?SLOG(info, #{
                msg => "routing_schema_used",
                schema => Schema
            });
        _ ->
            ?SLOG(notice, #{
                msg => "configured_routing_schema_unacceptable",
                schema => Schema,
                configured => ConfSchema,
                reason =>
                    "Could not use configured routing storage schema because "
                    "there are already non-empty routing tables pertaining to "
                    "another schema."
            })
    end.

-spec deinit_schema() -> ok.
deinit_schema() ->
    _ = persistent_term:erase(?PT_SCHEMA_VSN),
    ok.

-spec choose_schema_vsn(schemavsn()) -> schemavsn().
choose_schema_vsn(ConfType) ->
    IsEmptyIndex = emqx_trie:empty(),
    IsEmptyFilters = is_empty(?ROUTE_TAB_FILTERS),
    case {IsEmptyIndex, IsEmptyFilters} of
        {true, true} ->
            ConfType;
        {false, true} ->
            v1;
        {true, false} ->
            v2
    end.

is_empty(Tab) ->
    ets:first(Tab) =:= '$end_of_table'.

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
