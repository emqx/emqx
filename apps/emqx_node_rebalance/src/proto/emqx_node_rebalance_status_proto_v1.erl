%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_node_rebalance_status_proto_v1).

-behaviour(emqx_bpapi).

-export([
    introduced_in/0,

    local_status/1,
    rebalance_status/1,
    evacuation_status/1
]).

-include_lib("emqx/include/bpapi.hrl").
-include_lib("emqx/include/types.hrl").

introduced_in() ->
    "5.0.22".

-spec local_status(node()) ->
    emqx_rpc:badrpc() | disabled | {evacuation, map()} | {rebalance, map()}.
local_status(Node) ->
    rpc:call(Node, emqx_node_rebalance_status, local_status, []).

-spec rebalance_status([node()]) ->
    emqx_rpc:multicall_result({node(), map()}).
rebalance_status(Nodes) ->
    rpc:multicall(Nodes, emqx_node_rebalance_status, rebalance_status, []).

-spec evacuation_status([node()]) ->
    emqx_rpc:multicall_result({node(), map()}).
evacuation_status(Nodes) ->
    rpc:multicall(Nodes, emqx_node_rebalance_status, evacuation_status, []).
