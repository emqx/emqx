%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_retainer_proto_v1).

-behaviour(emqx_bpapi).

-include_lib("emqx/include/bpapi.hrl").

-export([
    introduced_in/0,
    wait_dispatch_complete/2
]).

introduced_in() ->
    "5.0.0".

-spec wait_dispatch_complete(list(node()), timeout()) -> emqx_rpc:multicall_result(ok).
wait_dispatch_complete(Nodes, Timeout) ->
    rpc:multicall(Nodes, emqx_retainer_dispatcher, wait_dispatch_complete, [Timeout]).
