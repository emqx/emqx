%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_authz_proto_v1).

-behaviour(emqx_bpapi).

-export([
    introduced_in/0,
    lookup_from_all_nodes/2
]).

-include_lib("emqx/include/bpapi.hrl").

-define(TIMEOUT, 15000).

introduced_in() ->
    "5.0.0".

-spec lookup_from_all_nodes([node()], atom()) ->
    emqx_rpc:erpc_multicall(term()).
lookup_from_all_nodes(Nodes, Type) ->
    erpc:multicall(Nodes, emqx_authz_api_sources, lookup_from_local_node, [Type], ?TIMEOUT).
