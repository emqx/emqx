%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_connector_proto_v2).

-behaviour(emqx_bpapi).

-export([
    introduced_in/0,

    lookup/4,
    list/2,
    start/4
]).

-include_lib("emqx/include/bpapi.hrl").

-type key() :: atom() | binary() | [byte()].
-type maybe_namespace() :: undefined | binary().

-define(TIMEOUT, 15_000).

introduced_in() ->
    "6.0.0".

-spec lookup([node()], maybe_namespace(), key(), key()) ->
    emqx_rpc:erpc_multicall(term()).
lookup(Nodes, Namespace, ConnectorType, ConnectorName) ->
    erpc:multicall(
        Nodes,
        emqx_connector_api,
        v2_lookup,
        [Namespace, ConnectorType, ConnectorName],
        ?TIMEOUT
    ).

-spec list([node()], maybe_namespace()) ->
    emqx_rpc:erpc_multicall([emqx_resource:resource_data()]).
list(Nodes, Namespace) ->
    erpc:multicall(Nodes, emqx_connector, list, [Namespace], ?TIMEOUT).

-spec start([node()], maybe_namespace(), key(), key()) ->
    emqx_rpc:erpc_multicall(term()).
start(Nodes, Namespace, ConnectorType, ConnectorName) ->
    erpc:multicall(
        Nodes,
        emqx_connector_resource,
        start,
        [Namespace, ConnectorType, ConnectorName],
        ?TIMEOUT
    ).
