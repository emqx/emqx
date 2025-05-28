%%--------------------------------------------------------------------
%% Copyright (c) 2017-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_router).

-behaviour(gen_server).

-include("emqx.hrl").
-include("logger.hrl").
-include("types.hrl").
-include_lib("emqx/include/emqx_router.hrl").

%% Mnesia bootstrap
-export([create_tables/0]).

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

%% Mria Activity RPC targets
-export([
    mria_batch_run/2
]).

-export([do_batch/1]).

-export([cleanup_routes/1]).

-export([
    match_routes/1,
    lookup_routes/1
]).

%% Topics API
-export([stream/1]).

-export([print_routes/1]).

-export([
    foldl_routes/2,
    foldr_routes/2
]).

-export([
    topics/0,
    stats/1
]).

%% Exported for tests
-export([has_route/2]).

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
    init_schema/0
]).

-export_type([dest/0]).
-export_type([schemavsn/0]).

-type group() :: binary().
-type dest() :: node() | {group(), node()}.

%% Storage schema.
%% * `v2` is current implementation.
%% * `v1` is no longer supported since 5.10.
-type schemavsn() :: v1 | v2.

%% Operation :: {add, ...} | {delete, ...}.
-type batch() :: #{batch_route() => _Operation :: tuple()}.
-type batch_route() :: {emqx_types:topic(), dest()}.

-record(routeidx, {
    entry :: '$1' | emqx_topic_index:key(dest()),
    unused = [] :: nil()
}).

-define(dest_patterns(NodeOrExtDest),
    case is_atom(NodeOrExtDest) of
        %% node
        true -> [NodeOrExtDest, {'_', NodeOrExtDest}];
        %% external destination
        false -> [NodeOrExtDest]
    end
).

-define(UNSUPPORTED, unsupported).

-define(with_fallback(Expr, FallbackExpr),
    try
        Expr
    catch
        throw:?UNSUPPORTED -> FallbackExpr
    end
).

%%--------------------------------------------------------------------
%% Mnesia bootstrap
%%--------------------------------------------------------------------

create_tables() ->
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
                {write_concurrency, true},
                {decentralized_counters, true}
            ]}
        ]}
    ]),
    [?ROUTE_TAB, ?ROUTE_TAB_FILTERS].

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
    ok = emqx_router_helper:monitor(Dest),
    mria_insert_route_v2(Topic, Dest, single).

%% @doc Take a real topic (not filter) as input, return the matching topics and topic
%% filters associated with route destination.
-spec match_routes(emqx_types:topic()) -> [emqx_types:route()].
match_routes(Topic) when is_binary(Topic) ->
    match_routes_v2(Topic).

%% @doc Take a topic or filter as input, and return the existing routes with exactly
%% this topic or filter.
-spec lookup_routes(emqx_types:topic()) -> [emqx_types:route()].
lookup_routes(Topic) ->
    lookup_routes_v2(Topic).

-spec has_route(emqx_types:topic(), dest()) -> boolean().
has_route(Topic, Dest) ->
    has_route_v2(Topic, Dest).

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
    mria_delete_route_v2(Topic, Dest, single).

-spec do_batch(batch()) -> #{batch_route() => _Error}.
do_batch(Batch) ->
    mria_batch_v2(Batch).

mria_batch_v2(Batch) ->
    mria:async_dirty(?ROUTE_SHARD, fun ?MODULE:mria_batch_run/2, [v2, Batch]).

batch_get_action(Op) ->
    element(1, Op).

-spec stream(_Spec :: {_TopicPat, _DestPat}) ->
    emqx_utils_stream:stream(emqx_types:route()).
stream(MatchSpec) ->
    stream_v2(MatchSpec).

-spec topics() -> list(emqx_types:topic()).
topics() ->
    list_topics_v2().

-spec stats(n_routes) -> non_neg_integer().
stats(Item) ->
    get_stats_v2(Item).

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
cleanup_routes(NodeOrExtDest) ->
    cleanup_routes_v2(NodeOrExtDest).

-spec foldl_routes(fun((emqx_types:route(), Acc) -> Acc), Acc) -> Acc.
foldl_routes(FoldFun, AccIn) ->
    fold_routes_v2(foldl, FoldFun, AccIn).

-spec foldr_routes(fun((emqx_types:route(), Acc) -> Acc), Acc) -> Acc.
foldr_routes(FoldFun, AccIn) ->
    fold_routes_v2(foldr, FoldFun, AccIn).

call(Router, Msg) ->
    gen_server:call(Router, Msg, infinity).

pick(Topic) ->
    gproc_pool:pick_worker(router_pool, Topic).

%%--------------------------------------------------------------------
%% Route batch RPC targets
%%--------------------------------------------------------------------

-spec mria_batch_run(schemavsn(), batch()) -> #{batch_route() => _Error}.
mria_batch_run(v2, Batch) ->
    maps:fold(
        fun({Topic, Dest}, Op, Errors) ->
            ok = mria_batch_operation_v2(batch_get_action(Op), Topic, Dest),
            Errors
        end,
        #{},
        Batch
    ).

mria_batch_operation_v2(add, Topic, Dest) ->
    mria_insert_route_v2(Topic, Dest, batch);
mria_batch_operation_v2(delete, Topic, Dest) ->
    mria_delete_route_v2(Topic, Dest, batch).

%%--------------------------------------------------------------------
%% Schema v2
%% One bag table exclusively for regular, non-filter subscription
%% topics, and one `emqx_topic_index` table exclusively for wildcard
%% topics. Writes go to only one of the two tables at a time.
%% --------------------------------------------------------------------

mria_insert_route_v2(Topic, Dest, Ctx) ->
    case emqx_trie_search:filter(Topic) of
        Words when is_list(Words) ->
            K = emqx_topic_index:make_key(Words, Dest),
            mria_filter_tab_insert(K, Ctx);
        false ->
            mria_route_tab_insert(#route{topic = Topic, dest = Dest}, Ctx)
    end.

mria_filter_tab_insert(K, single) ->
    mria:dirty_write(?ROUTE_TAB_FILTERS, #routeidx{entry = K});
mria_filter_tab_insert(K, batch) ->
    mnesia:write(?ROUTE_TAB_FILTERS, #routeidx{entry = K}, write).

mria_delete_route_v2(Topic, Dest, Ctx) ->
    case emqx_trie_search:filter(Topic) of
        Words when is_list(Words) ->
            K = emqx_topic_index:make_key(Words, Dest),
            mria_filter_tab_delete(K, Ctx);
        false ->
            mria_route_tab_delete(#route{topic = Topic, dest = Dest}, Ctx)
    end.

mria_route_tab_insert(Route, single) ->
    mria:dirty_write(?ROUTE_TAB, Route);
mria_route_tab_insert(Route, batch) ->
    mnesia:write(?ROUTE_TAB, Route, write).

mria_route_tab_delete(Route, single) ->
    mria:dirty_delete_object(?ROUTE_TAB, Route);
mria_route_tab_delete(Route, batch) ->
    mnesia:delete_object(?ROUTE_TAB, Route, write).

mria_filter_tab_delete(K, single) ->
    mria:dirty_delete(?ROUTE_TAB_FILTERS, K);
mria_filter_tab_delete(K, batch) ->
    mnesia:delete(?ROUTE_TAB_FILTERS, K, write).

match_routes_v2(Topic) ->
    lookup_route_tab(Topic) ++
        [match_to_route(M) || M <- match_filters(Topic)].

lookup_route_tab(Topic) ->
    ets:lookup(?ROUTE_TAB, Topic).

match_filters(Topic) ->
    emqx_topic_index:matches(Topic, ?ROUTE_TAB_FILTERS, []).

lookup_routes_v2(Topic) ->
    case emqx_topic:wildcard(Topic) of
        true ->
            Pat = #routeidx{entry = emqx_topic_index:make_key(Topic, '$1')},
            [#route{topic = Topic, dest = Dest} || [Dest] <- ets:match(?ROUTE_TAB_FILTERS, Pat)];
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

has_route_tab_entry(Topic, Dest) ->
    [] =/= ets:match(?ROUTE_TAB, #route{topic = Topic, dest = Dest}).

cleanup_routes_v2(NodeOrExtDest) ->
    ?with_fallback(
        lists:foreach(
            fun(Pattern) ->
                _ = throw_unsupported(
                    mria:match_delete(
                        ?ROUTE_TAB_FILTERS,
                        #routeidx{entry = emqx_trie_search:make_pat('_', Pattern)}
                    )
                ),
                throw_unsupported(mria:match_delete(?ROUTE_TAB, make_route_rec_pat(Pattern)))
            end,
            ?dest_patterns(NodeOrExtDest)
        ),
        cleanup_routes_v2_fallback(NodeOrExtDest)
    ).

cleanup_routes_v2_fallback(NodeOrExtDest) ->
    %% NOTE
    %% No point in transaction here because all the operations on filters table are dirty.
    ok = ets:foldl(
        fun(#routeidx{entry = K}, ok) ->
            case get_dest_node(emqx_topic_index:get_id(K)) of
                NodeOrExtDest ->
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
                NodeOrExtDest ->
                    mria:dirty_delete_object(?ROUTE_TAB, Route);
                _ ->
                    ok
            end
        end,
        ok,
        ?ROUTE_TAB
    ).

get_dest_node({external, _} = ExtDest) ->
    ExtDest;
get_dest_node({_, Node}) ->
    Node;
get_dest_node(Node) ->
    Node.

throw_unsupported({error, unsupported_otp_version}) ->
    throw(?UNSUPPORTED);
throw_unsupported(Other) ->
    Other.

%% Make Dialyzer happy
make_route_rec_pat(DestPattern) ->
    erlang:make_tuple(
        record_info(size, route),
        '_',
        [{1, route}, {#route.dest, DestPattern}]
    ).

stream_v2(Spec) ->
    emqx_utils_stream:chain(
        mk_route_stream(?ROUTE_TAB, Spec),
        mk_route_stream(?ROUTE_TAB_FILTERS, Spec)
    ).

mk_route_stream(Tab = ?ROUTE_TAB, {MTopic, MDest}) ->
    emqx_utils_stream:ets(fun
        (undefined) ->
            ets:match_object(Tab, #route{topic = MTopic, dest = MDest}, 1);
        (Cont) ->
            ets:match_object(Cont)
    end);
mk_route_stream(Tab = ?ROUTE_TAB_FILTERS, {MTopic, MDest}) ->
    emqx_utils_stream:map(
        fun routeidx_to_route/1,
        emqx_utils_stream:ets(
            fun
                (undefined) ->
                    MatchSpec = #routeidx{entry = emqx_trie_search:make_pat(MTopic, MDest)},
                    ets:match_object(Tab, MatchSpec, 1);
                (Cont) ->
                    ets:match_object(Cont)
            end
        )
    ).

list_topics_v2() ->
    Pat = #routeidx{entry = '$1'},
    Filters = [emqx_topic_index:get_topic(K) || [K] <- ets:match(?ROUTE_TAB_FILTERS, Pat)],
    list_route_tab_topics() ++ Filters.

list_route_tab_topics() ->
    mnesia:dirty_all_keys(?ROUTE_TAB).

get_stats_v2(n_routes) ->
    NTopics = emqx_maybe:define(ets:info(?ROUTE_TAB, size), 0),
    NWildcards = emqx_maybe:define(ets:info(?ROUTE_TAB_FILTERS, size), 0),
    NTopics + NWildcards.

fold_routes_v2(FunName, FoldFun, AccIn) ->
    FilterFoldFun = mk_filtertab_fold_fun(FoldFun),
    Acc = ets:FunName(FoldFun, AccIn, ?ROUTE_TAB),
    ets:FunName(FilterFoldFun, Acc, ?ROUTE_TAB_FILTERS).

mk_filtertab_fold_fun(FoldFun) ->
    fun(#routeidx{entry = K}, Acc) -> FoldFun(match_to_route(K), Acc) end.

routeidx_to_route(#routeidx{entry = M}) ->
    match_to_route(M).

match_to_route(M) ->
    #route{topic = emqx_topic_index:get_topic(M), dest = emqx_topic_index:get_id(M)}.

%%--------------------------------------------------------------------
%% Legacy routing schema
%% --------------------------------------------------------------------

%% @doc Get the schema version in use.
%% BPAPI RPC Target @ emqx_router_proto
-spec get_schema_vsn() -> schemavsn().
get_schema_vsn() ->
    v2.

-spec init_schema() -> ok.
init_schema() ->
    ClusterState = discover_cluster_schema_vsn(),
    verify_cluster_schema_vsn(ClusterState).

-spec discover_cluster_schema_vsn() ->
    _ClusterState :: [{node(), schemavsn() | unknown, _Details}].
discover_cluster_schema_vsn() ->
    discover_cluster_schema_vsn(emqx:running_nodes() -- [node()]).

discover_cluster_schema_vsn([]) ->
    [];
discover_cluster_schema_vsn(Nodes) ->
    lists:zipwith(
        fun
            (Node, {ok, Schema}) ->
                {Node, Schema, configured};
            (Node, {error, {exception, badarg, _Stacktrace}}) ->
                %% Likely, persistent term is not defined yet.
                {Node, unknown, starting};
            (Node, Error) ->
                {Node, unknown, Error}
        end,
        Nodes,
        emqx_router_proto_v1:get_routing_schema_vsn(Nodes)
    ).

-spec verify_cluster_schema_vsn(_ClusterState :: [{node(), schemavsn() | unknown, _Details}]) ->
    ok.
verify_cluster_schema_vsn(ClusterState) ->
    Versions = lists:usort([Vsn || {_Node, Vsn, _} <- ClusterState, Vsn /= unknown]),
    verify_cluster_schema_vsn(Versions, ClusterState).

verify_cluster_schema_vsn([], []) ->
    %% Only this node in the cluster.
    ok;
verify_cluster_schema_vsn([v2], _) ->
    %% Every other node uses v2 schema.
    ok;
verify_cluster_schema_vsn([], ClusterState = [_ | _]) ->
    ?SLOG(warning, #{
        msg => "cluster_routing_schema_discovery_failed",
        responses => ClusterState,
        reason => "Could not determine configured routing storage schema in peer nodes."
    }),
    %% Ok to start?
    %% The only risk is full cluster restart, other nodes are < 5.10 *and* have
    %% storage schema force configured to v1.
    ok;
verify_cluster_schema_vsn([v1], ClusterState) ->
    %% Every other node uses v1 schema.
    Desc = unsupported_schema_reason(ClusterState),
    io:format(standard_error, "Error: ~ts~n", [Desc]),
    ?SLOG(critical, #{
        msg => "unsupported_routing_schemas_in_cluster",
        responses => ClusterState,
        description => Desc
    }),
    error(conflicting_routing_schemas_configured_in_cluster);
verify_cluster_schema_vsn([_ | _], ClusterState) ->
    Desc = schema_conflict_reason(ClusterState),
    io:format(standard_error, "Error: ~ts~n", [Desc]),
    ?SLOG(critical, #{
        msg => "conflicting_routing_schemas_in_cluster",
        responses => ClusterState,
        description => Desc
    }),
    error(conflicting_routing_schemas_configured_in_cluster).

unsupported_schema_reason(_State) ->
    "Peer nodes are running unsupported legacy (v1) route storage schema."
    "\nThis node cannot boot before the cluster is upgraded to use current (v2) "
    "storage schema."
    "\n"
    "\nSituation requires manual intervention:"
    "\n1. Upgrade all nodes to 5.10.0 or newer."
    "\n2. Remove `broker.routing.storage_schema` option from applicable "
    "configuration files, if present."
    "\n3. Stop ALL running nodes, then restart them one by one.".

schema_conflict_reason(State) ->
    Cause =
        "Peer nodes have route storage schema resolved into conflicting versions.\n"
        "\nThis was caused by a race-condition when the cluster was rolling upgraded "
        "from an older version to 5.4.0, 5.4.1, 5.5.0 or 5.5.1."
        "\nThis node cannot boot before the conflicts are resolved.\n",
    NodesV1 = [Node || {Node, v1, _} <- State],
    NodesUnknown = [Node || {Node, unknown, _} <- State],
    Format =
        "There are two ways to resolve the conflict:"
        "\n"
        "\nA: Full cluster restart: stop ALL running nodes one by one "
        "and restart them in the reversed order."
        "\n"
        "\nB: Force v1 nodes to clean up their routes."
        "\n   Following EMQX nodes are running with v1 schema: ~0p."
        "\n   1. Stop listeners with command \"emqx eval 'emqx_listener:stop()'\" in all v1 nodes"
        "\n   2. Wait until they are safe to restart."
        "\n      This could take some time, depending on the number of clients and their subscriptions."
        "\n      Below conditions should be true for each of the nodes in order to proceed:"
        "\n     a) Command 'ets:info(emqx_subscriber, size)' prints `0`."
        "\n     b) Command 'emqx ctl topics list' prints No topics.`"
        "\n   3. Upgrade the nodes to 5.6.0 or newer.",
    FormatUnkown =
        "Additionally, the following nodes were unreachable during startup: ~0p."
        "It is strongly advised to include them in the manual resolution procedure as well.",
    Message = io_lib:format(Format, [NodesV1]),
    MessageUnknown = [io_lib:format(FormatUnkown, [NodesUnknown]) || NodesUnknown =/= []],
    Cause ++ unicode:characters_to_list([Message, "\n", MessageUnknown]).

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
