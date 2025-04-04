%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_prometheus_proto_v1).

-behaviour(emqx_bpapi).

-export([
    introduced_in/0,
    deprecated_since/0,
    start/1,
    stop/1
]).

-include_lib("emqx/include/bpapi.hrl").

deprecated_since() -> "5.0.10".

introduced_in() ->
    "5.0.0".

-spec start([node()]) -> emqx_rpc:multicall_result().
start(Nodes) ->
    rpc:multicall(Nodes, emqx_prometheus, do_start, [], 5000).

-spec stop([node()]) -> emqx_rpc:multicall_result().
stop(Nodes) ->
    rpc:multicall(Nodes, emqx_prometheus, do_stop, [], 5000).
