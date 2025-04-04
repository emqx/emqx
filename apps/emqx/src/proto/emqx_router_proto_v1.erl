%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_router_proto_v1).

-behaviour(emqx_bpapi).

-export([introduced_in/0]).

-export([
    get_routing_schema_vsn/1
]).

-include_lib("emqx/include/bpapi.hrl").

-define(TIMEOUT, 3_000).

introduced_in() ->
    "5.6.0".

-spec get_routing_schema_vsn([node()]) ->
    [emqx_rpc:erpc(emqx_router:schemavsn())].
get_routing_schema_vsn(Nodes) ->
    erpc:multicall(Nodes, emqx_router, get_schema_vsn, [], ?TIMEOUT).
