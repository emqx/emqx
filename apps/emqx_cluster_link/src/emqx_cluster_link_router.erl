%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_cluster_link_router).

-export([
    push_update/3,
    push_update_persistent/3,
    compute_intersections/2
]).

%% Test exports
-export([push_update/5]).

%%--------------------------------------------------------------------

push_update(Op, Topic, RouteID) ->
    push_update(Op, Topic, RouteID, fun emqx_cluster_link_router_syncer:push/4).

push_update_persistent(Op, Topic, RouteID) ->
    push_update(Op, Topic, RouteID, fun emqx_cluster_link_router_syncer:push_persistent_route/4).

push_update(Op, Topic, RouteID, PushFun) ->
    push_update(Op, Topic, RouteID, PushFun, emqx_cluster_link_config:enabled_links()).

push_update(Op, Topic, RouteID, PushFun, [#{name := Cluster, topics := Filters} | Rest]) ->
    Intersections = compute_intersections(Topic, Filters),
    ok = push_intersections(Cluster, Op, RouteID, PushFun, Intersections),
    push_update(Op, Topic, RouteID, PushFun, Rest);
push_update(_Op, _Topic, _RouteID, _PushFun, []) ->
    ok.

push_intersections(Cluster, Op, RouteID, PushFun, [Intersection | Rest]) ->
    _ = PushFun(Cluster, Op, Intersection, RouteID),
    push_intersections(Cluster, Op, RouteID, PushFun, Rest);
push_intersections(_Cluster, _Op, _RouteID, _PushFun, []) ->
    ok.

compute_intersections(Topic, Filters) ->
    compute_intersections(Topic, Filters, []).

compute_intersections(Topic, [Filter | Rest], Acc) ->
    case emqx_topic:intersection(Topic, Filter) of
        false ->
            compute_intersections(Topic, Rest, Acc);
        Intersection ->
            compute_intersections(Topic, Rest, unionify(Intersection, Acc))
    end;
compute_intersections(_Topic, [], Acc) ->
    Acc.

unionify(Filter, Union) ->
    %% NOTE: See also `emqx_topic:union/1` implementation.
    %% Drop filters completely covered by `Filter`.
    Disjoint = [F || F <- Union, not emqx_topic:is_subset(F, Filter)],
    %% Drop `Filter` if completely covered by another filter.
    Head = [Filter || not lists:any(fun(F) -> emqx_topic:is_subset(Filter, F) end, Disjoint)],
    Head ++ Disjoint.
