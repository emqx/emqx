%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_proto_v9).

-behaviour(emqx_bpapi).

-export([
    introduced_in/0,

    list/4,
    summary/4
]).

-include_lib("emqx/include/bpapi.hrl").

-include_lib("emqx/include/emqx_config.hrl").

-type maybe_namespace() :: emqx_config:maybe_namespace().

introduced_in() ->
    "6.1.0".

%% Difference from previous versions: accepts `all` as a "namespace"
-spec list([node()], all | maybe_namespace(), emqx_bridge_v2:root_cfg_key(), timeout()) ->
    emqx_rpc:erpc_multicall([emqx_resource:resource_data()]).
list(Nodes, Namespace, ConfRootKey, Timeout) ->
    erpc:multicall(Nodes, emqx_bridge_v2, list, [Namespace, ConfRootKey], Timeout).

%% Difference from previous versions: accepts `all` as a "namespace"
-spec summary([node()], all | maybe_namespace(), emqx_bridge_v2:root_cfg_key(), timeout()) ->
    emqx_rpc:erpc_multicall([emqx_resource:resource_data()]).
summary(Nodes, Namespace, ConfRootKey, Timeout) ->
    erpc:multicall(
        Nodes,
        emqx_bridge_v2_api,
        summary_v8,
        [Namespace, ConfRootKey],
        Timeout
    ).
