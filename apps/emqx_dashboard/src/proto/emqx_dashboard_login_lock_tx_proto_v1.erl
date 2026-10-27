%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% Transaction funs of emqx_dashboard_login_lock. A replicant passes them
%% to mria by name and a core node runs them, see apps/emqx_bpapi/README.md.
-module(emqx_dashboard_login_lock_tx_proto_v1).

-behaviour(emqx_bpapi).

-include_lib("emqx/include/bpapi.hrl").

-export([introduced_in/0]).

-export([delete_attempts/1]).

introduced_in() ->
    "6.3.3".

-spec delete_attempts([{binary(), non_neg_integer()}]) -> ok.
delete_attempts(Keys) ->
    emqx_dashboard_login_lock:delete_attempts_tx(Keys).
