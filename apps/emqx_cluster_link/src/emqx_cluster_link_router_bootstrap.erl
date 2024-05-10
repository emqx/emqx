%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_cluster_link_router_bootstrap).

-include_lib("emqx/include/emqx_router.hrl").

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
    max_batch_size :: non_neg_integer()
}).

%%

init(TargetCluster, Options) ->
    LinkFilters = emqx_cluster_link_config:topic_filters(TargetCluster),
    {Wildcards, Topics} = lists:partition(fun emqx_topic:wildcard/1, LinkFilters),
    #bootstrap{
        target = TargetCluster,
        wildcards = Wildcards,
        topics = Topics,
        stash = [],
        max_batch_size = maps:get(max_batch_size, Options, ?MAX_BATCH_SIZE)
    }.

next_batch(B = #bootstrap{stash = S0 = [_ | _], max_batch_size = MBS}) ->
    {Batch, Stash} = mk_batch(S0, MBS),
    {Batch, B#bootstrap{stash = Stash}};
next_batch(B = #bootstrap{topics = Topics = [_ | _], stash = []}) ->
    Routes = select_routes_by_topics(Topics),
    next_batch(B#bootstrap{topics = [], stash = Routes});
next_batch(B0 = #bootstrap{wildcards = Wildcards = [_ | _], stash = []}) ->
    Routes = select_routes_by_wildcards(Wildcards),
    next_batch(B0#bootstrap{wildcards = [], stash = Routes});
next_batch(#bootstrap{topics = [], wildcards = [], stash = []}) ->
    done.

mk_batch(Stash, MaxBatchSize) when length(Stash) =< MaxBatchSize ->
    {Stash, []};
mk_batch(Stash, MaxBatchSize) ->
    {Batch, Rest} = lists:split(MaxBatchSize, Stash),
    {Batch, Rest}.

%%

select_routes_by_topics(Topics) ->
    [encode_route(Topic, Topic) || Topic <- Topics, emqx_broker:subscribers(Topic) =/= []].

select_routes_by_wildcards(Wildcards) ->
    emqx_utils_ets:keyfoldl(
        fun(Topic, Acc) -> intersecting_route(Topic, Wildcards) ++ Acc end,
        [],
        ?SUBSCRIBER
    ).

intersecting_route(Topic, Wildcards) ->
    %% TODO: probably nice to validate cluster link topic filters
    %% to have no intersections between each other?
    case topic_intersect_any(Topic, Wildcards) of
        false -> [];
        Intersection -> [encode_route(Intersection, Topic)]
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
