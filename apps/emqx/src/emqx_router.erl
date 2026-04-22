%%--------------------------------------------------------------------
%% Copyright (c) 2017-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-elvis([{elvis_style, dont_repeat_yourself, disable}]).

-type group() :: binary().
-type dest() :: node() | {group(), node()}.

%% Storage schema.
%% * `v3' current implementation.
%% * `v2' similar to the current implementation, but used regular mria tables
%% * `v1' is no longer supported since 5.10.
-type schemavsn() :: v1 | v2 | v3.

%% Operation :: {add, ...} | {delete, ...}.
-type batch() :: #{batch_route() => _Operation :: tuple()}.
-type batch_route() :: {emqx_types:topic(), dest()}.

-record(routeidx, {
    entry :: emqx_topic_index:key(dest()) | {'_', {'$1' | {'_', '$1'}}} | '$1',
    unused = [] :: _
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

-type cluster_schema_versions() :: #{schemavsn() => [node(), ...], unknown => [_, ...]}.

-define(PT_SCHEMA_VSN, emqx_router_pt_schema_vsn).

%%--------------------------------------------------------------------
%% Mnesia bootstrap
%%--------------------------------------------------------------------

create_tables() ->
    case get_schema_vsn() of
        v2 -> create_tables_v2();
        v3 -> create_tables_v3()
    end.

create_tables_v3() ->
    %% Note: routes look like this:
    %%
    %% Examples:
    %% ````
    %% #route{topic = <<"foo/bar/baz">>,  dest = 'emqx@127.0.0.1'}               % foo/bar/baz
    %% #route{topic = <<"bar/baz">>,      dest = {<<"foo">>,'emqx@127.0.0.1'}}   % $share/foo/bar/baz
    %% ```
    ok = mria:create_table(?ROUTE_TAB_V3, [
        {merge_table, true},
        {auto_clean, true},
        {node_pattern, [
            #route{dest = '$1', _ = '_'},
            #route{dest = {'_', '$1'}, _ = '_'}
        ]},
        {type, bag},
        {rlog_shard, ?ROUTE_SHARD_V3},
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
    %% Note: filters look like this:
    %%
    %% {binary() | [word()], {ID}} where ID :: node() | {group(), node()}
    %%
    %% Examples:
    %% ```
    %% {routeidx, {[<<"foo">>,'+',<<"bar">>],    {'emqx@127.0.0.1'}},             []}   % foo/+/bar
    %% {routeidx, {[<<"foo">>,'#'],              {'emqx@127.0.0.1'}},             []}   % foo/#
    %% {routeidx, {['#'],                        {{<<"foo">>,'emqx@127.0.0.1'}}}, []}   % $share/foo/#
    %% {routeidx, {['+',<<"bar">>],              {{<<"foo">>,'emqx@127.0.0.1'}}}, []}}  % $share/foo/+/bar
    %% '''
    ok = mria:create_table(?ROUTE_TAB_FILTERS_V3, [
        {merge_table, true},
        {auto_clean, true},
        {node_pattern, [
            #routeidx{entry = {'_', {'$1'}}, _ = '_'},
            #routeidx{entry = {'_', {{'_', '$1'}}}, _ = '_'}
        ]},
        {type, ordered_set},
        {rlog_shard, ?ROUTE_SHARD_V3},
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
    [?ROUTE_TAB_V3, ?ROUTE_TAB_FILTERS_V3].

create_tables_v2() ->
    mria_config:set_dirty_shard(?ROUTE_SHARD_V2, true),
    ok = mria:create_table(?ROUTE_TAB_V2, [
        {type, bag},
        {rlog_shard, ?ROUTE_SHARD_V2},
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
    ok = mria:create_table(?ROUTE_TAB_FILTERS_V2, [
        {type, ordered_set},
        {rlog_shard, ?ROUTE_SHARD_V2},
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
    [?ROUTE_TAB_V2, ?ROUTE_TAB_FILTERS_V2].

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
    mria:async_dirty(?ROUTE_SHARD_V2, fun ?MODULE:mria_batch_run/2, [v2, Batch]).

batch_get_action(Op) ->
    element(1, Op).

-spec stream(_Spec :: {_TopicPat, _DestPat}) ->
    emqx_utils_stream:stream(emqx_types:route()).
stream(MatchSpec) ->
    stream_v2(MatchSpec).

-spec topics() -> list(emqx_types:topic()).
topics() ->
    Pat = #routeidx{entry = '$1'},
    Filters = [emqx_topic_index:get_topic(K) || [K] <- ets:match(route_filters_table(), Pat)],
    list_route_tab_topics() ++ Filters.

-spec stats(n_routes) -> non_neg_integer().
stats(n_routes) ->
    NTopics = emqx_maybe:define(ets:info(route_table(), size), 0),
    NWildcards = emqx_maybe:define(ets:info(route_filters_table(), size), 0),
    NTopics + NWildcards.

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
    case get_schema_vsn() of
        v3 ->
            %% Mria does the job automatically, since tables are
            %% created with `auto_clean' = `true':
            ok;
        v2 ->
            cleanup_routes_v2(NodeOrExtDest)
    end.

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
%% Schema v2 and v3
%%
%% One bag table exclusively for regular, non-filter subscription
%% topics, and one `emqx_topic_index` table exclusively for wildcard
%% topics. Writes go to only one of the two tables at a time.
%%
%% v3 schema uses mria merged tables
%%--------------------------------------------------------------------

mria_insert_route_v2(Topic, Dest, Ctx) ->
    case emqx_trie_search:filter(Topic) of
        Words when is_list(Words) ->
            K = emqx_topic_index:make_key(Words, Dest),
            mria_filter_tab_insert(K, Ctx);
        false ->
            mria_route_tab_insert(#route{topic = Topic, dest = Dest}, Ctx)
    end.

mria_filter_tab_insert(K, single) ->
    mria:dirty_write(route_filters_table(), #routeidx{entry = K});
mria_filter_tab_insert(K, batch) ->
    mnesia:write(route_filters_table(), #routeidx{entry = K}, write).

mria_delete_route_v2(Topic, Dest, Ctx) ->
    case emqx_trie_search:filter(Topic) of
        Words when is_list(Words) ->
            K = emqx_topic_index:make_key(Words, Dest),
            mria_filter_tab_delete(K, Ctx);
        false ->
            mria_route_tab_delete(#route{topic = Topic, dest = Dest}, Ctx)
    end.

mria_route_tab_insert(Route, single) ->
    mria:dirty_write(route_table(), Route);
mria_route_tab_insert(Route, batch) ->
    mnesia:write(route_table(), Route, write).

mria_route_tab_delete(Route, single) ->
    mria:dirty_delete_object(route_table(), Route);
mria_route_tab_delete(Route, batch) ->
    mnesia:delete_object(route_table(), Route, write).

mria_filter_tab_delete(K, single) ->
    mria:dirty_delete(route_filters_table(), K);
mria_filter_tab_delete(K, batch) ->
    mnesia:delete(route_filters_table(), K, write).

match_routes_v2(Topic) ->
    lookup_route_tab(Topic) ++
        [match_to_route(M) || M <- match_filters(Topic)].

lookup_route_tab(Topic) ->
    ets:lookup(route_table(), Topic).

match_filters(Topic) ->
    emqx_topic_index:matches(Topic, route_filters_table(), []).

lookup_routes_v2(Topic) ->
    case emqx_topic:wildcard(Topic) of
        true ->
            Pat = #routeidx{entry = emqx_topic_index:make_key(Topic, '$1')},
            [#route{topic = Topic, dest = Dest} || [Dest] <- ets:match(route_filters_table(), Pat)];
        false ->
            lookup_route_tab(Topic)
    end.

has_route_v2(Topic, Dest) ->
    case emqx_topic:wildcard(Topic) of
        true ->
            ets:member(route_filters_table(), emqx_topic_index:make_key(Topic, Dest));
        false ->
            has_route_tab_entry(Topic, Dest)
    end.

has_route_tab_entry(Topic, Dest) ->
    [] =/= ets:match(route_table(), #route{topic = Topic, dest = Dest}).

cleanup_routes_v2(NodeOrExtDest) ->
    ?with_fallback(
        lists:foreach(
            fun(Pattern) ->
                _ = throw_unsupported(
                    mria:match_delete(
                        ?ROUTE_TAB_FILTERS_V2,
                        #routeidx{entry = emqx_trie_search:make_pat('_', Pattern)}
                    )
                ),
                throw_unsupported(mria:match_delete(?ROUTE_TAB_V2, make_route_rec_pat(Pattern)))
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
                    mria:dirty_delete(route_filters_table(), K);
                _ ->
                    ok
            end
        end,
        ok,
        route_filters_table()
    ),
    ok = ets:foldl(
        fun(#route{dest = Dest} = Route, ok) ->
            case get_dest_node(Dest) of
                NodeOrExtDest ->
                    mria:dirty_delete_object(route_table(), Route);
                _ ->
                    ok
            end
        end,
        ok,
        route_table()
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
        mk_route_stream(route_table(), Spec),
        mk_route_stream(route_filters_table(), Spec)
    ).

mk_route_stream(Tab, {MTopic, MDest}) when
    Tab =:= ?ROUTE_TAB_V2;
    Tab =:= ?ROUTE_TAB_V3
->
    emqx_utils_stream:ets(fun
        (undefined) ->
            ets:match_object(Tab, #route{topic = MTopic, dest = MDest}, 1);
        (Cont) ->
            ets:match_object(Cont)
    end);
mk_route_stream(Tab, {MTopic, MDest}) when
    Tab =:= ?ROUTE_TAB_FILTERS_V2;
    Tab =:= ?ROUTE_TAB_FILTERS_V3
->
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

list_route_tab_topics() ->
    mnesia:dirty_all_keys(route_table()).

fold_routes_v2(FunName, FoldFun, AccIn) ->
    FilterFoldFun = mk_filtertab_fold_fun(FoldFun),
    Acc = ets:FunName(FoldFun, AccIn, route_table()),
    ets:FunName(FilterFoldFun, Acc, route_filters_table()).

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
-compile({inline, get_schema_vsn/0}).
-spec get_schema_vsn() -> schemavsn().
get_schema_vsn() ->
    persistent_term:get(?PT_SCHEMA_VSN).

-spec init_schema() -> ok.
init_schema() ->
    ClusterState = discover_cluster_schema_vsn(),
    SchemaVsn = cluster_preferred_api_version(ClusterState),
    SchemaVsn =:= v2 andalso
        begin
            io:put_chars(standard_error, v2_warning()),
            ?SLOG(warning, #{msg => v2_warning()})
        end,
    verify_cluster_schema_vsn(ClusterState),
    persistent_term:put(?PT_SCHEMA_VSN, SchemaVsn).

%% Find if there are known peers that are still running EMQX version
%% that doesn't support v3:
%%
%% Note: do not confuse BPAPI versions with the schema version.
-spec cluster_preferred_api_version(#{schemavsn() => [node()], unknown => _}) -> schemavsn().
cluster_preferred_api_version(#{v2 := [_ | _]}) ->
    v2;
cluster_preferred_api_version(_) ->
    case has_v2_tables() of
        true ->
            %% Old table is present in the cluster, use old schema
            v2;
        false ->
            %% There are no signs of the old routing tables. This is
            %% likely a newly deployed cluster. Use configured value:
            emqx_config:get([broker, routing, storage_schema])
    end.

-spec discover_cluster_schema_vsn() -> cluster_schema_versions().
discover_cluster_schema_vsn() ->
    RawResult = discover_cluster_schema_vsn(emqx:running_nodes() -- [node()]),
    maps:groups_from_list(
        fun({_Node, Version, _Details}) -> Version end,
        fun
            ({Node, unknown, Details}) ->
                {Node, Details};
            ({Node, _Vsn, _Details}) ->
                Node
        end,
        RawResult
    ).

-spec discover_cluster_schema_vsn([node()]) -> [Result] when
    Result ::
        {node(), v2 | v3, configured}
        | {node(), unknown, starting | _}.
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

-spec verify_cluster_schema_vsn(cluster_schema_versions()) -> ok.
verify_cluster_schema_vsn(ClusterView) ->
    case maps:get(v1, ClusterView, []) of
        [] ->
            ok;
        NodesV1 ->
            MsgV1 = unsupported_schema_reason(NodesV1),
            io:format(standard_error, "Error: ~ts", [MsgV1]),
            ?SLOG(critical, #{
                msg => "unsupported_routing_schemas_in_cluster",
                responses => ClusterView,
                description => MsgV1
            }),
            error(conflicting_routing_schemas_configured_in_cluster)
    end,
    case ClusterView of
        #{v3 := [_ | _], v2 := [_ | _]} ->
            MsgV2 = schema_conflict_reason(ClusterView),
            io:format(standard_error, "Error: ~ts", [MsgV2]),
            ?SLOG(critical, #{
                msg => "conflicting_routing_schemas_in_cluster",
                responses => ClusterView,
                description => MsgV2
            }),
            error(conflicting_routing_schemas_configured_in_cluster);
        #{} ->
            ok
    end.

%% erlfmt-ignore
unsupported_schema_reason(NodesV1) ->
    io_lib:format(
"Peer nodes are running unsupported legacy (v1) route storage schema.
This node cannot boot before the cluster is upgraded to use current (v3)
storage schema.

Nodes using V1 schema: ~0p

Situation requires manual intervention:
1. Upgrade all nodes to 5.10.4 or newer.
2. Remove `broker.routing.storage_schema` option from applicable configuration files, if present.
3. Stop ALL running nodes, then restart them one by one.
",
      [NodesV1]).

%% erlfmt-ignore
schema_conflict_reason(#{v2 := V2, v3 := V3}) ->
    io_lib:format(
"Peer nodes have route storage schema resolved into conflicting versions.

Nodes using V3 schema: ~0p
Nodes using V2 schema: ~0p

This node cannot boot before the conflicts are resolved.

Situation requires manual intervention:
1. Upgrade all nodes to 5.10.4 or newer.
2. Remove `broker.routing.storage_schema` option from applicable configuration files, if present.
3. Stop ALL running nodes, then restart them one by one.
",
      [V3, V2]).

%% erlfmt-ignore
v2_warning() ->
    "This node is running in compatibility mode with v2 routing schema.

To complete upgade to the current (v3) schema, the following steps are required:

1. Upgrade all nodes to EMQX version 5.10.4 or later
2. Perform full restart of the cluster.
".

has_v2_tables() ->
    %% Run this in a transaction to make sure replicants don't take
    %% decisions while disconnected from core node:
    Result = mria:ro_transaction(
        '$mria_meta_shard',
        fun() ->
            case mnesia:read(mria_schema, ?ROUTE_TAB_V2) of
                [_] ->
                    true;
                [] ->
                    false
            end
        end
    ),
    case Result of
        {atomic, Val} ->
            Val;
        {aborted, Reason} ->
            ?SLOG(info, #{
                msg => "Waiting for core node to get decision which routing schema to use",
                reason => Reason
            }),
            timer:sleep(5_000),
            has_v2_tables()
    end.

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

-compile({inline, route_filters_table/0}).
route_filters_table() ->
    case get_schema_vsn() of
        v3 -> ?ROUTE_TAB_FILTERS_V3;
        v2 -> ?ROUTE_TAB_FILTERS_V2
    end.

-compile({inline, route_table/0}).
route_table() ->
    case get_schema_vsn() of
        v3 -> ?ROUTE_TAB_V3;
        v2 -> ?ROUTE_TAB_V2
    end.
