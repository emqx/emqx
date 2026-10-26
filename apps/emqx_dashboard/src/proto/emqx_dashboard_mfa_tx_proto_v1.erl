%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% Transaction funs of emqx_dashboard_mfa. A replicant passes them to
%% mria by name and a core node runs them, see apps/emqx_bpapi/README.md.
-module(emqx_dashboard_mfa_tx_proto_v1).

-behaviour(emqx_bpapi).

-include("emqx_dashboard.hrl").
-include_lib("emqx/include/bpapi.hrl").

-export([introduced_in/0]).

-export([
    verify_temp_token/2,
    record_temp_token_failure/2
]).

introduced_in() ->
    "6.3.3".

-spec verify_temp_token(dashboard_username(), binary()) ->
    {ok, dashboard_username(), emqx_dashboard_mfa:temp_token_purpose()} | {error, term()}.
verify_temp_token(SsoUsername, Token) ->
    emqx_dashboard_mfa:verify_temp_token_tx(SsoUsername, Token).

-spec record_temp_token_failure(dashboard_username(), binary()) -> ok | {error, term()}.
record_temp_token_failure(SsoUsername, Token) ->
    emqx_dashboard_mfa:record_temp_token_failure_tx(SsoUsername, Token).
