%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_security_profile_proto_v1).

-behaviour(emqx_bpapi).

-include("bpapi.hrl").

-export([
    introduced_in/0,

    get_profile/2
]).

introduced_in() ->
    "6.3.0".

-spec get_profile([node()], timeout()) ->
    emqx_rpc:erpc_multicall(emqx_security_profile:profile()).
get_profile(Nodes, Timeout) ->
    erpc:multicall(Nodes, emqx_security_profile, profile, [], Timeout).
