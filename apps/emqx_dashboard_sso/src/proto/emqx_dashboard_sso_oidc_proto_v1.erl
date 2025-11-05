%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_dashboard_sso_oidc_proto_v1).

-behaviour(emqx_bpapi).

-include_lib("emqx/include/bpapi.hrl").

-export([
    introduced_in/0,

    lookup/3,
    delete/3
]).

introduced_in() ->
    "5.8.9".

-spec lookup([node()], binary(), pos_integer()) ->
    emqx_rpc:erpc_multicall({ok, term()} | undefined).
lookup(Nodes, State, Timeout) ->
    erpc:multicall(Nodes, emqx_dashboard_sso_oidc_session, lookup, [State], Timeout).

-spec delete([node()], binary(), pos_integer()) -> emqx_rpc:erpc_multicall(true).
delete(Nodes, State, Timeout) ->
    erpc:multicall(Nodes, emqx_dashboard_sso_oidc_session, delete, [State], Timeout).
