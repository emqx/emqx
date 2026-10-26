%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% Transaction funs of emqx_license_session_hwm. A replicant passes them
%% to mria by name and a core node runs them, see apps/emqx_bpapi/README.md.
-module(emqx_license_session_hwm_tx_proto_v1).

-behaviour(emqx_bpapi).

-include_lib("emqx/include/bpapi.hrl").

-export([introduced_in/0]).

-export([maybe_update/3]).

introduced_in() ->
    "6.3.3".

-spec maybe_update(integer(), integer(), non_neg_integer()) -> term().
maybe_update(Period, Timestamp, Count) ->
    emqx_license_session_hwm:maybe_update_in_transaction(Period, Timestamp, Count).
