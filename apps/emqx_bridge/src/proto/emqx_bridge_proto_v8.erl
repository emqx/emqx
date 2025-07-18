%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_proto_v8).

-behaviour(emqx_bpapi).

-export([
    introduced_in/0,

    %% introduced in v8
    list/3,
    lookup/5,
    summary/3,
    wait_for_ready/6,
    start/5,
    get_metrics/5
]).

-include_lib("emqx/include/bpapi.hrl").

-include_lib("emqx/include/emqx_config.hrl").

-define(TIMEOUT, 15_000).

-type key() :: atom() | binary() | [byte()].
-type maybe_namespace() :: ?global_ns | binary().

introduced_in() ->
    "6.0.0".

%%--------------------------------------------------------------------------------
%% introduced in v8
%%--------------------------------------------------------------------------------

-spec list([node()], maybe_namespace(), emqx_bridge_v2:root_cfg_key()) ->
    emqx_rpc:erpc_multicall([emqx_resource:resource_data()]).
list(Nodes, Namespace, ConfRootKey) ->
    erpc:multicall(Nodes, emqx_bridge_v2, list, [Namespace, ConfRootKey], ?TIMEOUT).

-spec lookup([node()], maybe_namespace(), emqx_bridge_v2:root_cfg_key(), key(), key()) ->
    emqx_rpc:erpc_multicall(term()).
lookup(Nodes, Namespace, ConfRootKey, BridgeType, BridgeName) ->
    erpc:multicall(
        Nodes,
        emqx_bridge_v2_api,
        lookup_v8,
        [Namespace, ConfRootKey, BridgeType, BridgeName],
        ?TIMEOUT
    ).

-spec summary([node()], maybe_namespace(), emqx_bridge_v2:root_cfg_key()) ->
    emqx_rpc:erpc_multicall([emqx_resource:resource_data()]).
summary(Nodes, Namespace, ConfRootKey) ->
    erpc:multicall(
        Nodes,
        emqx_bridge_v2_api,
        summary_v8,
        [Namespace, ConfRootKey],
        ?TIMEOUT
    ).

-spec wait_for_ready(
    [node()],
    maybe_namespace(),
    emqx_bridge_v2:root_cfg_key(),
    atom(),
    binary(),
    integer()
) ->
    emqx_rpc:erpc_multicall([_]).
wait_for_ready(Nodes, Namespace, ConfRootKey, Type, Name, RPCTimeout) ->
    erpc:multicall(
        Nodes,
        emqx_bridge_v2_api,
        wait_for_ready_v8,
        [Namespace, ConfRootKey, Type, Name],
        RPCTimeout
    ).

-spec start([node()], maybe_namespace(), emqx_bridge_v2:root_cfg_key(), key(), key()) ->
    emqx_rpc:erpc_multicall(ok).
start(Nodes, Namespace, ConfRootKey, BridgeType, BridgeName) ->
    erpc:multicall(
        Nodes,
        emqx_bridge_v2,
        start,
        [Namespace, ConfRootKey, BridgeType, BridgeName],
        ?TIMEOUT
    ).

-spec get_metrics([node()], maybe_namespace(), emqx_bridge_v2:root_cfg_key(), key(), key()) ->
    emqx_rpc:erpc_multicall(term()).
get_metrics(Nodes, Namespace, ConfRootKey, ActionType, ActionName) ->
    erpc:multicall(
        Nodes,
        emqx_bridge_v2_api,
        get_metrics_v8,
        [Namespace, ConfRootKey, ActionType, ActionName],
        ?TIMEOUT
    ).
