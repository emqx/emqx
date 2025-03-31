%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_plugins_proto_v3).

-behaviour(emqx_bpapi).

-export([
    introduced_in/0,

    allow_installation/2,
    disallow_installation/2
]).

-include_lib("emqx/include/bpapi.hrl").

-define(TIMEOUT, 25_000).

introduced_in() ->
    "5.8.6".

-spec allow_installation([node()], binary() | string()) ->
    emqx_rpc:erpc_multicall(ok | {error, term()}).
allow_installation(Nodes, NameVsn) ->
    erpc:multicall(Nodes, emqx_plugins, allow_installation, [NameVsn], ?TIMEOUT).

-spec disallow_installation([node()], binary() | string()) ->
    emqx_rpc:erpc_multicall(ok | {error, term()}).
disallow_installation(Nodes, NameVsn) ->
    erpc:multicall(Nodes, emqx_plugins, forget_allowed_installation, [NameVsn], ?TIMEOUT).
