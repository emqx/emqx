%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_dashboard_proto_v2).

-behaviour(emqx_bpapi).

-export([
    introduced_in/0,
    do_sample/2,
    clear_table/1,
    current_rate/1
]).

-include("emqx_dashboard.hrl").
-include_lib("emqx/include/bpapi.hrl").

introduced_in() ->
    "5.8.4".

-spec do_sample(node(), Latest :: pos_integer() | infinity) -> list(map()) | emqx_rpc:badrpc().
do_sample(Node, Latest) ->
    erpc:call(Node, emqx_dashboard_monitor, do_sample, [Node, Latest], ?RPC_TIMEOUT).

-spec clear_table(Nodes :: [node()]) -> emqx_rpc:erpc_multicall(ok).
clear_table(Nodes) ->
    erpc:multicall(Nodes, emqx_dashboard_monitor, clear_table, [], ?RPC_TIMEOUT).

-spec current_rate(node()) -> {ok, map()} | emqx_rpc:badrpc().
current_rate(Node) ->
    erpc:call(Node, emqx_dashboard_monitor, current_rate, [Node], ?RPC_TIMEOUT).
