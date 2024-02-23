%%--------------------------------------------------------------------
%% Copyright (c) 2022-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_node_rebalance_purge_proto_v1).

-behaviour(emqx_bpapi).

-export([
    introduced_in/0,

    start/2,
    stop/1
]).

-include_lib("emqx/include/bpapi.hrl").

introduced_in() ->
    "5.2.1".

-spec start([node()], emqx_node_rebalance_purge:start_opts()) ->
    emqx_rpc:erpc_multicall(ok | {error, emqx_node_rebalance_purge:start_error()}).
start(Nodes, Opts) ->
    erpc:multicall(Nodes, emqx_node_rebalance_purge, start, [Opts]).

-spec stop([node()]) ->
    emqx_rpc:erpc_multicall(ok | {error, emqx_node_rebalance_purge:stop_error()}).
stop(Nodes) ->
    erpc:multicall(Nodes, emqx_node_rebalance_purge, stop, []).
