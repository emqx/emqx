%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mgmt_cluster_proto_v3).

-behaviour(emqx_bpapi).

-export([
    introduced_in/0,
    invite_node/3,
    connected_replicants/1
]).

-include_lib("emqx/include/bpapi.hrl").

introduced_in() ->
    "5.5.0".

-spec invite_node(node(), node(), timeout()) -> ok | ignore | {error, term()} | emqx_rpc:badrpc().
invite_node(Node, Self, Timeout) when is_integer(Timeout); Timeout =:= infinity ->
    rpc:call(Node, emqx_mgmt_api_cluster, join, [Self], Timeout).

-spec connected_replicants([node()]) -> emqx_rpc:multicall_result().
connected_replicants(Nodes) ->
    rpc:multicall(Nodes, emqx_mgmt_api_cluster, connected_replicants, [], 30_000).
