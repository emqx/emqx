%%--------------------------------------------------------------------
%% Copyright (c) 2022-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_prometheus_proto_v2).

-behaviour(emqx_bpapi).

-export([
    introduced_in/0,

    raw_prom_data/4
]).

-include_lib("emqx/include/bpapi.hrl").

introduced_in() ->
    "5.5.0".

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
