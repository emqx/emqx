%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_gateway_http_proto_v1).

-behaviour(emqx_bpapi).

-export([
    introduced_in/0,
    get_cluster_status/2
]).

-include_lib("emqx/include/bpapi.hrl").

introduced_in() ->
    "5.0.0".

-spec get_cluster_status([node()], emqx_gateway_cm:gateway_name()) ->
    emqx_rpc:multicall_result([map()]).
get_cluster_status(Nodes, GwName) ->
    rpc:multicall(Nodes, emqx_gateway_http, gateway_status, [GwName]).
