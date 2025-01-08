%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(emqx_connector_proto_v1).

-behaviour(emqx_bpapi).

-export([
    introduced_in/0,

    list_connectors_on_nodes/1,
    lookup_from_all_nodes/3,
    start_connector_to_node/3,
    start_connectors_to_all_nodes/3
]).

-include_lib("emqx/include/bpapi.hrl").

-define(TIMEOUT, 15000).

introduced_in() ->
    "5.3.1".

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
