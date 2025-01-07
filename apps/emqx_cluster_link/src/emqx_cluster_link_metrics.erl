%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_cluster_link_metrics).

-include("emqx_cluster_link.hrl").

%% API
-export([
    maybe_create_metrics/1,
    drop_metrics/1,

    get_metrics/1,
    reset_metrics/1,
    routes_set/2
]).

%%--------------------------------------------------------------------
%% Type definitions
%%--------------------------------------------------------------------

-define(METRICS, [
    ?route_metric
]).
-define(RATE_METRICS, []).

%%--------------------------------------------------------------------
%% metrics API
%%--------------------------------------------------------------------

get_metrics(ClusterName) ->
    Nodes = emqx:running_nodes(),
    Timeout = 15_000,
    RouterResults = emqx_metrics_proto_v2:get_metrics(Nodes, ?METRIC_NAME, ClusterName, Timeout),
    ResourceId = emqx_cluster_link_mqtt:resource_id(ClusterName),
    ResourceResults = emqx_metrics_proto_v2:get_metrics(
        Nodes, resource_metrics, ResourceId, Timeout
    ),
    lists:zip3(Nodes, RouterResults, ResourceResults).

maybe_create_metrics(ClusterName) ->
    case emqx_metrics_worker:has_metrics(?METRIC_NAME, ClusterName) of
        true ->
            ok = emqx_metrics_worker:reset_metrics(?METRIC_NAME, ClusterName);
        false ->
            ok = emqx_metrics_worker:create_metrics(
                ?METRIC_NAME, ClusterName, ?METRICS, ?RATE_METRICS
            )
    end.

reset_metrics(ClusterName) ->
    Nodes = emqx:running_nodes(),
    Timeout = 15_000,
    RouterResults = emqx_metrics_proto_v2:reset_metrics(Nodes, ?METRIC_NAME, ClusterName, Timeout),
    ResourceId = emqx_cluster_link_mqtt:resource_id(ClusterName),
    ResourceResults = emqx_metrics_proto_v2:reset_metrics(
        Nodes, resource_metrics, ResourceId, Timeout
    ),
    lists:zip3(Nodes, RouterResults, ResourceResults).

drop_metrics(ClusterName) ->
    ok = emqx_metrics_worker:clear_metrics(?METRIC_NAME, ClusterName).

routes_set(ClusterName, Val) ->
    catch emqx_metrics_worker:set_gauge(
        ?METRIC_NAME, ClusterName, <<"singleton">>, ?route_metric, Val
    ).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------
