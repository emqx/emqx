%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_retainer_proto_v2).

-behaviour(emqx_bpapi).

-include_lib("emqx/include/bpapi.hrl").

-export([
    introduced_in/0,
    wait_dispatch_complete/2,
    active_mnesia_indices/1
]).

-define(TIMEOUT, 5000).

introduced_in() ->
    "5.0.13".

-spec wait_dispatch_complete(list(node()), timeout()) -> emqx_rpc:multicall_result(ok).
wait_dispatch_complete(Nodes, Timeout) ->
    rpc:multicall(Nodes, emqx_retainer_dispatcher, wait_dispatch_complete, [Timeout]).

-spec active_mnesia_indices(list(node())) ->
    emqx_rpc:multicall_result({list(emqx_retainer_index:index()), list(emqx_retainer_index:index())}).
active_mnesia_indices(Nodes) ->
    rpc:multicall(Nodes, emqx_retainer_mnesia, active_indices, [], ?TIMEOUT).
