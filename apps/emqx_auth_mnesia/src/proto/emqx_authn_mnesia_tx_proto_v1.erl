%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% Transaction funs of emqx_authn_mnesia. A replicant passes them to
%% mria by name and a core node runs them, see apps/emqx_bpapi/README.md.
-module(emqx_authn_mnesia_tx_proto_v1).

-behaviour(emqx_bpapi).

-include_lib("emqx/include/bpapi.hrl").

-export([introduced_in/0]).

-export([import_users/2]).

introduced_in() ->
    "6.3.3".

-spec import_users([map()], map()) -> map().
import_users(Users, Opts) ->
    emqx_authn_mnesia:import_users_tx(Users, Opts).
