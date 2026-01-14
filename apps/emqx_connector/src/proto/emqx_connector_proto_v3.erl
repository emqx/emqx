%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_connector_proto_v3).

-behaviour(emqx_bpapi).

-export([
    introduced_in/0,

    list/3
]).

-include_lib("emqx/include/bpapi.hrl").
-include_lib("emqx/include/emqx_config.hrl").

-type maybe_namespace() :: ?global_ns | binary().

introduced_in() ->
    "6.1.0".

%% Difference from v2: accepts `all` as "namespace".
-spec list([node()], all | maybe_namespace(), timeout()) ->
    emqx_rpc:erpc_multicall([emqx_resource:resource_data()]).
list(Nodes, Namespace, Timeout) ->
    erpc:multicall(Nodes, emqx_connector, list, [Namespace], Timeout).
