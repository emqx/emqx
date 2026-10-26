%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% Transaction funs of emqx_mt_state. A replicant passes them to mria by
%% name and a core node runs them, see apps/emqx_bpapi/README.md.
-module(emqx_mt_state_tx_proto_v1).

-behaviour(emqx_bpapi).

-include_lib("emqx/include/bpapi.hrl").

-export([introduced_in/0]).

-export([
    create_managed_ns/1,
    delete_ns/1
]).

introduced_in() ->
    "6.3.3".

-spec create_managed_ns(binary()) -> term().
create_managed_ns(Ns) ->
    emqx_mt_state:create_managed_ns_txn(Ns).

-spec delete_ns(binary()) -> term().
delete_ns(Ns) ->
    emqx_mt_state:delete_ns_txn(Ns).
