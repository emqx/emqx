%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_exhook_proto_v1).

-behaviour(emqx_bpapi).

-export([
    introduced_in/0,

    all_servers_info/1,
    server_info/2,
    server_hooks_metrics/2
]).

-include_lib("emqx/include/bpapi.hrl").

introduced_in() ->
    "5.0.0".

-spec all_servers_info([node()]) ->
    emqx_rpc:erpc_multicall(map()).
all_servers_info(Nodes) ->
    erpc:multicall(Nodes, emqx_exhook_mgr, all_servers_info, []).

-spec server_info([node()], emqx_exhook_mgr:server_name()) ->
    emqx_rpc:erpc_multicall(map()).
server_info(Nodes, Name) ->
    erpc:multicall(Nodes, emqx_exhook_mgr, server_info, [Name]).

-spec server_hooks_metrics([node()], emqx_exhook_mgr:server_name()) ->
    emqx_rpc:erpc_multicall(emqx_exhook_metrics:hooks_metrics()).
server_hooks_metrics(Nodes, Name) ->
    erpc:multicall(Nodes, emqx_exhook_mgr, server_hooks_metrics, [Name]).
