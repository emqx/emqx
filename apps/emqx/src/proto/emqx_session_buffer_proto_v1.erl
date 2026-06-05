%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_session_buffer_proto_v1).

-behaviour(emqx_bpapi).

-export([
    introduced_in/0,
    local_top/4
]).

-include_lib("emqx/include/bpapi.hrl").

introduced_in() ->
    "6.0.3".

-spec local_top([node()], pos_integer(), emqx_session_buffer_mon:sort_by(), timeout()) ->
    emqx_rpc:multicall_result([emqx_session_buffer_mon:row()]).
local_top(Nodes, Count, SortBy, Timeout) ->
    rpc:multicall(Nodes, emqx_session_buffer_mon, local_top, [Count, SortBy], Timeout).
