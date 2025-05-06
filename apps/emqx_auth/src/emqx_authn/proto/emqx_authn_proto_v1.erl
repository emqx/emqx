%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_authn_proto_v1).

-behaviour(emqx_bpapi).

-export([
    introduced_in/0,
    lookup_from_all_nodes/3
]).

-include_lib("emqx/include/bpapi.hrl").

-define(TIMEOUT, 15000).

introduced_in() ->
    "5.0.0".

-spec lookup_from_all_nodes([node()], atom(), binary()) ->
    emqx_rpc:erpc_multicall(term()).
lookup_from_all_nodes(Nodes, ChainName, AuthenticatorID) ->
    erpc:multicall(
        Nodes, emqx_authn_api, lookup_from_local_node, [ChainName, AuthenticatorID], ?TIMEOUT
    ).
