%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_cluster_link_router_bootstrap).

-include_lib("emqx/include/emqx_router.hrl").
-include_lib("emqx/include/emqx_shared_sub.hrl").
-include_lib("emqx/src/emqx_persistent_session_ds/emqx_ps_ds_int.hrl").

-include("emqx_cluster_link.hrl").

-export([
    init/2,
    next_batch/1
]).

-define(MAX_BATCH_SIZE, 4000).

-record(bootstrap, {
    target :: _ClusterName :: binary(),
    wildcards :: [emqx_types:topic()],
    topics :: [emqx_types:topic()],
    stash :: [{emqx_types:topic(), _RouteID}],
    max_batch_size :: non_neg_integer(),
    is_persistent_route :: boolean()
}).

%%

init(TargetCluster, Options) ->
    LinkFilters = emqx_cluster_link_config:topic_filters(TargetCluster),
    {Wildcards, Topics} = lists:partition(fun emqx_topic:wildcard/1, LinkFilters),
    IsPersistentRoute = maps:get(is_persistent_route, Options, false),
    #bootstrap{
        target = TargetCluster,
        wildcards = Wildcards,
        topics = Topics,
        stash = [],
        max_batch_size = maps:get(max_batch_size, Options, ?MAX_BATCH_SIZE),
        is_persistent_route = IsPersistentRoute
    }.

next_batch(B = #bootstrap{stash = S0 = [_ | _], max_batch_size = MBS}) ->
    {Batch, Stash} = mk_batch(S0, MBS),
    {Batch, B#bootstrap{stash = Stash}};
next_batch(B = #bootstrap{topics = Topics = [_ | _], stash = [], is_persistent_route = IsPs}) ->
    next_batch(B#bootstrap{topics = [], stash = routes_by_topic(Topics, IsPs)});
next_batch(
    B0 = #bootstrap{wildcards = Wildcards = [_ | _], stash = [], is_persistent_route = IsPs}
) ->
    next_batch(B0#bootstrap{wildcards = [], stash = routes_by_wildcards(Wildcards, IsPs)});
next_batch(#bootstrap{topics = [], wildcards = [], stash = []}) ->
    done.

mk_batch(Stash, MaxBatchSize) when length(Stash) =< MaxBatchSize ->
    {Stash, []};
mk_batch(Stash, MaxBatchSize) ->
    {Batch, Rest} = lists:split(MaxBatchSize, Stash),
    {Batch, Rest}.

%%

routes_by_topic(Topics, _IsPersistentRoute = false) ->
    Routes = select_routes_by_topics(Topics),
    SharedRoutes = select_shared_sub_routes_by_topics(Topics),
    Routes ++ SharedRoutes;
routes_by_topic(Topics, _IsPersistentRoute = true) ->
    lists:foldl(
        fun(T, Acc) ->
            Routes = emqx_persistent_session_ds_router:lookup_routes(T),
            [encode_route(T, ?PERSISTENT_ROUTE_ID(T, D)) || #ps_route{dest = D} <- Routes] ++ Acc
        end,
        [],
        Topics
    ).

routes_by_wildcards(Wildcards, _IsPersistentRoute = false) ->
    Routes = select_routes_by_wildcards(Wildcards),
    SharedRoutes = select_shared_sub_routes_by_wildcards(Wildcards),
    Routes ++ SharedRoutes;
routes_by_wildcards(Wildcards, _IsPersistentRoute = true) ->
    emqx_persistent_session_ds_router:foldl_routes(
        fun(#ps_route{dest = D, topic = T}, Acc) ->
            case topic_intersect_any(T, Wildcards) of
                false ->
                    Acc;
                Intersec ->
                    [encode_route(Intersec, ?PERSISTENT_ROUTE_ID(T, D)) | Acc]
            end
        end,
        []
    ).

select_routes_by_topics(Topics) ->
    [encode_route(Topic, Topic) || Topic <- Topics, emqx_broker:subscribers(Topic) =/= []].

select_routes_by_wildcards(Wildcards) ->
    emqx_utils_ets:keyfoldl(
        fun(Topic, Acc) -> intersecting_route(Topic, Wildcards) ++ Acc end,
        [],
        ?SUBSCRIBER
    ).

select_shared_sub_routes_by_topics([T | Topics]) ->
    select_shared_sub_routes(T) ++ select_shared_sub_routes_by_topics(Topics);
select_shared_sub_routes_by_topics([]) ->
    [].

select_shared_sub_routes_by_wildcards(Wildcards) ->
    emqx_utils_ets:keyfoldl(
        fun({Group, Topic}, Acc) ->
            RouteID = ?SHARED_ROUTE_ID(Topic, Group),
            intersecting_route(Topic, RouteID, Wildcards) ++ Acc
        end,
        [],
        ?SHARED_SUBSCRIBER
    ).

select_shared_sub_routes(Topic) ->
    LocalGroups = lists:usort(ets:select(?SHARED_SUBSCRIBER, [{{{'$1', Topic}, '_'}, [], ['$1']}])),
    [encode_route(Topic, ?SHARED_ROUTE_ID(Topic, G)) || G <- LocalGroups].

intersecting_route(Topic, Wildcards) ->
    intersecting_route(Topic, Topic, Wildcards).

intersecting_route(Topic, RouteID, Wildcards) ->
    %% TODO: probably nice to validate cluster link topic filters
    %% to have no intersections between each other?
    case topic_intersect_any(Topic, Wildcards) of
        false -> [];
        Intersection -> [encode_route(Intersection, RouteID)]
    end.

topic_intersect_any(Topic, [LinkFilter | T]) ->
    case emqx_topic:intersection(Topic, LinkFilter) of
        false -> topic_intersect_any(Topic, T);
        TopicOrFilter -> TopicOrFilter
    end;
topic_intersect_any(_Topic, []) ->
    false.

encode_route(Topic, RouteID) ->
    emqx_cluster_link_mqtt:encode_field(route, {add, {Topic, RouteID}}).
