%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_schema_registry_http_api_proto_v1).

-behaviour(emqx_bpapi).

-include_lib("emqx/include/bpapi.hrl").

-export([
    introduced_in/0,

    lookup_resource_from_all_nodes/3
]).

-define(LOOKUP_TIMEOUT, 15_000).

introduced_in() ->
    "5.9.0".

-spec lookup_resource_from_all_nodes(
    [node()],
    emqx_schema_registry:serde_type(),
    emqx_schema_registry:schema_name()
) ->
    emqx_rpc:erpc_multicall({ok, map()} | {error, not_found}).
lookup_resource_from_all_nodes(Nodes, Type, Name) ->
    erpc:multicall(
        Nodes,
        emqx_schema_registry_http_api,
        lookup_resource_from_local_node_v1,
        [Type, Name],
        ?LOOKUP_TIMEOUT
    ).
