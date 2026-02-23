%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_authz_proto_v2).

-behaviour(emqx_bpapi).

-export([
    introduced_in/0,
    reset_metrics/2
]).

-include_lib("emqx/include/bpapi.hrl").

-define(TIMEOUT, 5000).

introduced_in() ->
    "6.2.0".

-spec reset_metrics([node()], atom()) ->
    emqx_rpc:erpc_multicall(ok).
reset_metrics(Nodes, Type) ->
    erpc:multicall(Nodes, emqx_authz_api_sources, reset_metrics_local, [Type], ?TIMEOUT).
