%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% Transaction funs of emqx_dashboard_token. A replicant passes them to
%% mria by name and a core node runs them, see apps/emqx_bpapi/README.md.
-module(emqx_dashboard_token_tx_proto_v1).

-behaviour(emqx_bpapi).

-include("emqx_dashboard.hrl").
-include_lib("emqx/include/bpapi.hrl").

-export([introduced_in/0]).

-export([destroy_by_username/1]).

introduced_in() ->
    "6.3.3".

-spec destroy_by_username(dashboard_username()) -> ok.
destroy_by_username(Username) ->
    emqx_dashboard_token:destroy_by_username_tx(Username).
