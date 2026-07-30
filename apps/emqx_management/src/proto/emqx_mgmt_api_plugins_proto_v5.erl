%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%-------------------------------------------------------------------
-module(emqx_mgmt_api_plugins_proto_v5).

-behaviour(emqx_bpapi).

-export([
    introduced_in/0,
    ensure_start_package/2,
    validate_start/2,
    ensure_started/2,
    ensure_stopped/2
]).

-include_lib("emqx/include/bpapi.hrl").

-define(TIMEOUT, 10000).

introduced_in() ->
    "6.0.4".

-spec ensure_start_package([node()], binary() | string()) -> emqx_rpc:multicall_result().
ensure_start_package(Nodes, Name) ->
    rpc:multicall(Nodes, emqx_plugins, ensure_start_package, [Name], ?TIMEOUT).

-spec validate_start([node()], binary() | string()) -> emqx_rpc:multicall_result().
validate_start(Nodes, Name) ->
    rpc:multicall(Nodes, emqx_plugins, validate_start, [Name], ?TIMEOUT).

-spec ensure_started([node()], binary() | string()) -> emqx_rpc:multicall_result().
ensure_started(Nodes, Name) ->
    rpc:multicall(Nodes, emqx_plugins, ensure_started, [Name], ?TIMEOUT).

-spec ensure_stopped([node()], binary() | string()) -> emqx_rpc:multicall_result().
ensure_stopped(Nodes, Name) ->
    rpc:multicall(Nodes, emqx_plugins, ensure_stopped, [Name], ?TIMEOUT).
