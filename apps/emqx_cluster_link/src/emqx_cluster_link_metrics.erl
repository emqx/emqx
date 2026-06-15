%%--------------------------------------------------------------------
%% Copyright (c) 2024-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_cluster_link_metrics).

-include("emqx_cluster_link.hrl").

%% API
-export([
    %% Lifecycle
    child_spec/0,
    child_spec_link/1,
    %% Reporting
    set_num_external_routes/2,
    %% Cluster-wide operations
    get_cluster_metrics/1,
    reset_cluster_metrics/1
]).

%%--------------------------------------------------------------------
%% Type definitions
%%--------------------------------------------------------------------

-define(METRICS, [
    ?route_metric
]).

-define(RATE_METRICS, []).

-define(METRICS_RPC_TIMEOUT, 15_000).

%%--------------------------------------------------------------------
%% metrics API
%%--------------------------------------------------------------------

child_spec() ->
    emqx_metrics_worker:child_spec(metrics, ?METRIC_NAME).

child_spec_link(ClusterName) ->
    emqx_metrics_installer:child_spec(
        metrics_installer,
        ?METRIC_NAME,
        ClusterName,
        ?METRICS,
        ?RATE_METRICS
    ).

%%--------------------------------------------------------------------

-spec set_num_external_routes(emqx_cluster_link_schema:cluster(), non_neg_integer()) ->
    ok.
set_num_external_routes(ClusterName, N) ->
    emqx_metrics_worker:set_gauge(?METRIC_NAME, ClusterName, routerepl, ?route_metric, N).

%%--------------------------------------------------------------------

-spec get_cluster_metrics(emqx_cluster_link_schema:cluster()) ->
    [
        {
            node(),
            emqx_rpc:erpc(_Router :: emqx_metrics_worker:metrics()),
            emqx_rpc:erpc(_Resource :: emqx_metrics_worker:metrics())
        }
    ].
get_cluster_metrics(ClusterName) ->
    Nodes = emqx:running_nodes(),
    RouterResults = emqx_metrics_proto_v2:get_metrics(
        Nodes, ?METRIC_NAME, ClusterName, ?METRICS_RPC_TIMEOUT
    ),
    ResourceId = emqx_cluster_link_mqtt:resource_id(ClusterName),
    ResourceResults = emqx_metrics_proto_v2:get_metrics(
        Nodes, resource_metrics, ResourceId, ?METRICS_RPC_TIMEOUT
    ),
    lists:zip3(Nodes, RouterResults, ResourceResults).

-spec reset_cluster_metrics(emqx_cluster_link_schema:cluster()) ->
    [{node(), emqx_rpc:erpc(ok), emqx_rpc:erpc(ok)}].
reset_cluster_metrics(ClusterName) ->
    Nodes = emqx:running_nodes(),
    RouterResults = emqx_metrics_proto_v2:reset_metrics(
        Nodes, ?METRIC_NAME, ClusterName, ?METRICS_RPC_TIMEOUT
    ),
    ResourceId = emqx_cluster_link_mqtt:resource_id(ClusterName),
    ResourceResults = emqx_metrics_proto_v2:reset_metrics(
        Nodes, resource_metrics, ResourceId, ?METRICS_RPC_TIMEOUT
    ),
    lists:zip3(Nodes, RouterResults, ResourceResults).
