%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_metrics_proto_v2).

-behaviour(emqx_bpapi).

-export([
    introduced_in/0,

    get_metrics/4,

    %% introduced in v2
    reset_metrics/4
]).

-include("bpapi.hrl").

introduced_in() ->
    "5.7.0".

-spec get_metrics(
    [node()],
    emqx_metrics_worker:handler_name(),
    emqx_metrics_worker:metric_id(),
    timeout()
) -> emqx_rpc:erpc_multicall(emqx_metrics_worker:metrics()).
get_metrics(Nodes, HandlerName, MetricId, Timeout) ->
    erpc:multicall(Nodes, emqx_metrics_worker, get_metrics, [HandlerName, MetricId], Timeout).

%%--------------------------------------------------------------------------------
%% Introduced in V2
%%--------------------------------------------------------------------------------

-spec reset_metrics(
    [node()],
    emqx_metrics_worker:handler_name(),
    emqx_metrics_worker:metric_id(),
    timeout()
) -> emqx_rpc:erpc_multicall(emqx_metrics_worker:metrics()).
reset_metrics(Nodes, HandlerName, MetricId, Timeout) ->
    erpc:multicall(Nodes, emqx_metrics_worker, reset_metrics, [HandlerName, MetricId], Timeout).
