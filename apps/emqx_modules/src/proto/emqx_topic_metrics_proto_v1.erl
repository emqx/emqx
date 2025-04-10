%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_topic_metrics_proto_v1).

-behaviour(emqx_bpapi).

-export([
    introduced_in/0,

    metrics/1,
    metrics/2,
    reset/1,
    reset/2
]).

-include_lib("emqx/include/bpapi.hrl").

introduced_in() ->
    "5.0.0".

-spec metrics([node()]) -> emqx_rpc:multicall_result().
metrics(Nodes) ->
    emqx_rpc:multicall(Nodes, emqx_topic_metrics, metrics, []).

-spec metrics([node()], emqx_types:topic()) -> emqx_rpc:multicall_result().
metrics(Nodes, Topic) ->
    emqx_rpc:multicall(Nodes, emqx_topic_metrics, metrics, [Topic]).

-spec reset([node()]) -> emqx_rpc:multicall_result().
reset(Nodes) ->
    emqx_rpc:multicall(Nodes, emqx_topic_metrics, reset, []).

-spec reset([node()], emqx_types:topic()) -> emqx_rpc:multicall_result().
reset(Nodes, Topic) ->
    emqx_rpc:multicall(Nodes, emqx_topic_metrics, reset, [Topic]).
