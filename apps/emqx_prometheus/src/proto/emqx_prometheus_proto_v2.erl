%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_prometheus_proto_v2).

-behaviour(emqx_bpapi).

-export([
    introduced_in/0,
    start/1,
    stop/1,

    raw_prom_data/4
]).

-include_lib("emqx/include/bpapi.hrl").

introduced_in() ->
    "5.5.0".

-spec start([node()]) -> emqx_rpc:multicall_result().
start(Nodes) ->
    rpc:multicall(Nodes, emqx_prometheus, do_start, [], 5000).

-spec stop([node()]) -> emqx_rpc:multicall_result().
stop(Nodes) ->
    rpc:multicall(Nodes, emqx_prometheus, do_stop, [], 5000).

-type key() :: atom().
-type arg() :: list(term()).

-spec raw_prom_data([node()], key(), key(), arg()) -> emqx_rpc:erpc_multicall(term()).
raw_prom_data(Nodes, M, F, A) ->
    erpc:multicall(
        Nodes,
        emqx_prometheus_api,
        lookup_from_local_nodes,
        [M, F, A],
        5000
    ).
