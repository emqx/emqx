%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% Transaction funs of emqx_dashboard_login. A replicant passes them to
%% mria by name and a core node runs them, see apps/emqx_bpapi/README.md.
-module(emqx_dashboard_login_tx_proto_v1).

-behaviour(emqx_bpapi).

-include("emqx_dashboard.hrl").
-include_lib("emqx/include/bpapi.hrl").

-export([introduced_in/0]).

-export([
    put_challenge/2,
    take_challenge/1,
    ensure_mock_salt_secret/0,
    delete_challenges/1
]).

introduced_in() ->
    "6.3.3".

-spec put_challenge(binary(), #emqx_dashboard_scram_challenge{}) -> ok.
put_challenge(Id, Challenge) ->
    emqx_dashboard_login:put_challenge_tx(Id, Challenge).

-spec take_challenge(binary()) -> {ok, #emqx_dashboard_scram_challenge{}} | {error, not_found}.
take_challenge(Id) ->
    emqx_dashboard_login:take_challenge_tx(Id).

-spec ensure_mock_salt_secret() -> ok.
ensure_mock_salt_secret() ->
    emqx_dashboard_login:ensure_mock_salt_secret_tx().

-spec delete_challenges([#emqx_dashboard_scram_challenge{}]) -> ok.
delete_challenges(Challenges) ->
    emqx_dashboard_login:delete_challenges_tx(Challenges).
