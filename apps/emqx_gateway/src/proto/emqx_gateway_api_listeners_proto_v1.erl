%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_gateway_api_listeners_proto_v1).

-behaviour(emqx_bpapi).

-export([
    introduced_in/0,
    listeners_cluster_status/2
]).

-include_lib("emqx/include/bpapi.hrl").

introduced_in() ->
    "5.0.0".

-spec listeners_cluster_status([node()], list()) ->
    emqx_rpc:multicall_result([map()]).
listeners_cluster_status(Nodes, Listeners) ->
    rpc:multicall(Nodes, emqx_gateway_api_listeners, do_listeners_cluster_status, [Listeners]).
