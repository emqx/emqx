%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_node_rebalance_api_proto_v1).

-behaviour(emqx_bpapi).

-export([
    introduced_in/0,

    node_rebalance_evacuation_start/2,
    node_rebalance_evacuation_stop/1,

    node_rebalance_start/2,
    node_rebalance_stop/1
]).

-include_lib("emqx/include/bpapi.hrl").
-include_lib("emqx/include/types.hrl").

introduced_in() ->
    "5.0.22".

-spec node_rebalance_evacuation_start(node(), emqx_node_rebalance_evacuation:start_opts()) ->
    emqx_rpc:badrpc() | ok_or_error(emqx_node_rebalance_evacuation:start_error()).
node_rebalance_evacuation_start(Node, #{} = Opts) ->
    rpc:call(Node, emqx_node_rebalance_evacuation, start, [Opts]).

-spec node_rebalance_evacuation_stop(node()) ->
    emqx_rpc:badrpc() | ok_or_error(not_started).
node_rebalance_evacuation_stop(Node) ->
    rpc:call(Node, emqx_node_rebalance_evacuation, stop, []).

-spec node_rebalance_start(node(), emqx_node_rebalance:start_opts()) ->
    emqx_rpc:badrpc() | ok_or_error(emqx_node_rebalance:start_error()).
node_rebalance_start(Node, Opts) ->
    rpc:call(Node, emqx_node_rebalance, start, [Opts]).

-spec node_rebalance_stop(node()) ->
    emqx_rpc:badrpc() | ok_or_error(not_started).
node_rebalance_stop(Node) ->
    rpc:call(Node, emqx_node_rebalance, stop, []).
