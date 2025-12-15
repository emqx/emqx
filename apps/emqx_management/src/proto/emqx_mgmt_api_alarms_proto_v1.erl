%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_mgmt_api_alarms_proto_v1).

-behaviour(emqx_bpapi).

-export([
    introduced_in/0,

    safe_deactivate/3
]).

-include_lib("emqx/include/bpapi.hrl").

introduced_in() ->
    "6.1.0".

-spec safe_deactivate([node()], emqx_alarm:name(), timeout()) ->
    emqx_rpc:erpc_multicall([ok | {error, not_found}]).
safe_deactivate(Nodes, Name, Timeout) ->
    erpc:multicall(Nodes, emqx_mgmt_api_alarms, safe_deactivate_v1, [Name], Timeout).
