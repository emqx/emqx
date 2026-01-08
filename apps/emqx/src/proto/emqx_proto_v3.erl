%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_proto_v3).

-behaviour(emqx_bpapi).

-include("bpapi.hrl").

-export([
    introduced_in/0,

    get_metrics/3,
    get_metrics_cluster/3
]).

introduced_in() ->
    "6.1.0".

-spec get_metrics(node(), emqx_config:maybe_namespace(), timeout()) ->
    [{emqx_metrics:metric_name(), non_neg_integer()}].
get_metrics(Node, Namespace, Timeout) ->
    erpc:call(Node, emqx_metrics, all_v3, [Namespace], Timeout).

-spec get_metrics_cluster([node()], emqx_config:maybe_namespace(), timeout()) ->
    emqx_rpc:erpc_multicall([{emqx_metrics:metric_name(), non_neg_integer()}]).
get_metrics_cluster(Nodes, Namespace, Timeout) ->
    erpc:multicall(Nodes, emqx_metrics, all_v3, [Namespace], Timeout).
