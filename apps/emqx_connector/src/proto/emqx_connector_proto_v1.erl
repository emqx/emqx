%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_connector_proto_v1).

-behaviour(emqx_bpapi).

-export([
    introduced_in/0,
    deprecated_since/0,

    list_connectors_on_nodes/1,
    lookup_from_all_nodes/3,
    start_connector_to_node/3,
    start_connectors_to_all_nodes/3
]).

-include_lib("emqx/include/bpapi.hrl").

-define(TIMEOUT, 15000).

introduced_in() ->
    "5.3.1".

deprecated_since() ->
    "6.0.0".

-spec list_connectors_on_nodes([node()]) ->
    emqx_rpc:erpc_multicall([emqx_resource:resource_data()]).
list_connectors_on_nodes(Nodes) ->
    erpc:multicall(Nodes, emqx_connector, list, [], ?TIMEOUT).

-type key() :: atom() | binary() | [byte()].

-spec lookup_from_all_nodes([node()], key(), key()) ->
    emqx_rpc:erpc_multicall(term()).
lookup_from_all_nodes(Nodes, ConnectorType, ConnectorName) ->
    erpc:multicall(
        Nodes,
        emqx_connector_api,
        lookup_from_local_node,
        [ConnectorType, ConnectorName],
        ?TIMEOUT
    ).

-spec start_connector_to_node(node(), key(), key()) ->
    term().
start_connector_to_node(Node, ConnectorType, ConnectorName) ->
    rpc:call(
        Node,
        emqx_connector_resource,
        start,
        [ConnectorType, ConnectorName],
        ?TIMEOUT
    ).

-spec start_connectors_to_all_nodes([node()], key(), key()) ->
    emqx_rpc:erpc_multicall(term()).
start_connectors_to_all_nodes(Nodes, ConnectorType, ConnectorName) ->
    erpc:multicall(
        Nodes,
        emqx_connector_resource,
        start,
        [ConnectorType, ConnectorName],
        ?TIMEOUT
    ).
