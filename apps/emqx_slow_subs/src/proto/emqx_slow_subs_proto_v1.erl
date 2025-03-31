%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_slow_subs_proto_v1).

-behaviour(emqx_bpapi).

-export([introduced_in/0]).

-export([clear_history/1, get_history/1]).

-include_lib("emqx/include/bpapi.hrl").

introduced_in() ->
    "5.0.0".

-spec clear_history([node()]) -> emqx_rpc:erpc_multicall(map()).
clear_history(Nodes) ->
    erpc:multicall(Nodes, emqx_slow_subs, clear_history, []).

-spec get_history([node()]) -> emqx_rpc:erpc_multicall(map()).
get_history(Nodes) ->
    erpc:multicall(Nodes, emqx_slow_subs_api, get_history, []).
