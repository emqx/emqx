%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_auth_cache_proto_v1).

-behaviour(emqx_bpapi).

-export([
    introduced_in/0,
    metrics/2,
    reset/2
]).

-include_lib("emqx/include/bpapi.hrl").

-define(TIMEOUT, 15000).

introduced_in() ->
    "5.9.0".

-spec metrics([node()], emqx_auth_cache:name()) ->
    emqx_rpc:erpc_multicall({node(), map()}).
metrics(Nodes, Name) ->
    erpc:multicall(Nodes, emqx_auth_cache, metrics_v1, [Name], ?TIMEOUT).

-spec reset([node()], emqx_auth_cache:name()) ->
    emqx_rpc:erpc_multicall(ok).
reset(Nodes, Name) ->
    erpc:multicall(Nodes, emqx_auth_cache, reset_v1, [Name], ?TIMEOUT).
