%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% Transaction funs of emqx_banned. A replicant passes them to mria by
%% name and a core node runs them, see apps/emqx_bpapi/README.md.
-module(emqx_banned_tx_proto_v1).

-behaviour(emqx_bpapi).

-include_lib("emqx/include/bpapi.hrl").

-export([introduced_in/0]).

-export([import_rules/2]).

introduced_in() ->
    "6.3.3".

-spec import_rules(mria:table(), [tuple()]) -> ok.
import_rules(Tab, Items) ->
    emqx_banned:import_rules_tx(Tab, Items).
