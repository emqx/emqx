%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_prometheus_proto_v3).

-behaviour(emqx_bpapi).

-export([
    introduced_in/0,

    raw_prom_data/5
]).

-include_lib("emqx/include/bpapi.hrl").

-type key() :: atom().
-type arg() :: list(term()).

introduced_in() ->
    "6.1.0".

-spec raw_prom_data([node()], key(), key(), arg(), timeout()) -> emqx_rpc:erpc_multicall(term()).
raw_prom_data(Nodes, M, F, A, Timeout) ->
    erpc:multicall(
        Nodes,
        emqx_prometheus_api,
        lookup_from_local_nodes,
        [M, F, A],
        Timeout
    ).
