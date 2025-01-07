%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_cluster_link_router_bootstrap).

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_shared_sub.hrl").
-include_lib("emqx/src/emqx_persistent_session_ds/emqx_ps_ds_int.hrl").

-include("emqx_cluster_link.hrl").

-export([
    init/3,
    next_batch/1
]).

-define(MAX_BATCH_SIZE, 4000).

-record(bootstrap, {
    target :: _ClusterName :: binary(),
    filters :: [emqx_types:topic()],
    stash :: [{emqx_types:topic(), _RouteID}],
    max_batch_size :: non_neg_integer(),
    is_persistent_route :: boolean()
}).

%%

init(TargetCluster, LinkFilters, Options) ->
    IsPersistentRoute = maps:get(is_persistent_route, Options, false),
    #bootstrap{
        target = TargetCluster,
        filters = LinkFilters,
        stash = [],
        max_batch_size = maps:get(max_batch_size, Options, ?MAX_BATCH_SIZE),
        is_persistent_route = IsPersistentRoute
    }.

next_batch(B = #bootstrap{stash = S0 = [_ | _], max_batch_size = MBS}) ->
    {Batch, Stash} = mk_batch(S0, MBS),
    {Batch, B#bootstrap{stash = Stash}};
next_batch(B0 = #bootstrap{filters = Filters = [_ | _], stash = [], is_persistent_route = false}) ->
    next_batch(B0#bootstrap{filters = [], stash = routes_by_wildcards(Filters)});
next_batch(B0 = #bootstrap{filters = Filters = [_ | _], stash = [], is_persistent_route = true}) ->
    next_batch(B0#bootstrap{filters = [], stash = ps_routes_by_wildcards(Filters)});
next_batch(#bootstrap{filters = [], stash = []}) ->
    done.

mk_batch(Stash, MaxBatchSize) when length(Stash) =< MaxBatchSize ->
    {Stash, []};
mk_batch(Stash, MaxBatchSize) ->
    {Batch, Rest} = lists:split(MaxBatchSize, Stash),
    {Batch, Rest}.

%%

routes_by_wildcards(Wildcards) ->
    Routes = select_routes_by_wildcards(Wildcards),
    SharedRoutes = select_shared_sub_routes_by_wildcards(Wildcards),
    Routes ++ SharedRoutes.

ps_routes_by_wildcards(Wildcards) ->
    emqx_persistent_session_ds_router:foldl_routes(
        fun(#ps_route{topic = Topic} = PSRoute, Acc) ->
            Intersections = emqx_cluster_link_router:compute_intersections(Topic, Wildcards),
            [encode_route(I, ps_route_id(PSRoute)) || I <- Intersections] ++ Acc
        end,
        []
    ).

ps_route_id(#ps_route{topic = T, dest = #share_dest{group = Group, session_id = SessionId}}) ->
    ?PERSISTENT_SHARED_ROUTE_ID(T, Group, SessionId);
ps_route_id(#ps_route{topic = T, dest = SessionId}) ->
    ?PERSISTENT_ROUTE_ID(T, SessionId).

select_routes_by_wildcards(Wildcards) ->
    emqx_broker:foldl_topics(
        fun(Topic, Acc) ->
            Intersections = emqx_cluster_link_router:compute_intersections(Topic, Wildcards),
            [encode_route(I, Topic) || I <- Intersections] ++ Acc
        end,
        []
    ).

select_shared_sub_routes_by_wildcards(Wildcards) ->
    emqx_utils_ets:keyfoldl(
        fun({Group, Topic}, Acc) ->
            Intersections = emqx_cluster_link_router:compute_intersections(Topic, Wildcards),
            [encode_route(I, ?SHARED_ROUTE_ID(Topic, Group)) || I <- Intersections] ++ Acc
        end,
        [],
        ?SHARED_SUBSCRIBER
    ).

encode_route(Topic, RouteID) ->
    emqx_cluster_link_mqtt:encode_field(route, {add, {Topic, RouteID}}).
