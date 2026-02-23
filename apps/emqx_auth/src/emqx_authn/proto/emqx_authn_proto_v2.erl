%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_authn_proto_v2).

-behaviour(emqx_bpapi).

-export([
    introduced_in/0,
    reset_metrics/3
]).

-include_lib("emqx/include/bpapi.hrl").

-define(TIMEOUT, 15000).

introduced_in() ->
    "6.2.0".

-spec reset_metrics([node()], atom(), binary()) ->
    emqx_rpc:erpc_multicall(ok).
reset_metrics(Nodes, ChainName, AuthenticatorID) ->
    erpc:multicall(
        Nodes, emqx_authn_api, reset_metrics_local, [ChainName, AuthenticatorID], ?TIMEOUT
    ).
