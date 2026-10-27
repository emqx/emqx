%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% Transaction funs of emqx_retainer_mnesia. A replicant passes them to
%% mria by name and a core node runs them, see apps/emqx_bpapi/README.md.
-module(emqx_retainer_mnesia_tx_proto_v1).

-behaviour(emqx_bpapi).

-include_lib("emqx/include/bpapi.hrl").

-export([introduced_in/0]).

-export([
    store_retained_indices/3,
    try_start_reindex/2,
    finalize_reindex/0
]).

introduced_in() ->
    "6.3.3".

-spec store_retained_indices(
    [emqx_retainer_index:index()], emqx_types:words(), non_neg_integer()
) -> ok.
store_retained_indices(Indices, TopicTokens, ExpiryTime) ->
    emqx_retainer_mnesia:store_retained_indices_tx(Indices, TopicTokens, ExpiryTime).

-spec try_start_reindex([emqx_retainer_index:index()], boolean()) ->
    ok | {error, already_started}.
try_start_reindex(NewIndices, Force) ->
    emqx_retainer_mnesia:try_start_reindex_tx(NewIndices, Force).

-spec finalize_reindex() -> ok.
finalize_reindex() ->
    emqx_retainer_mnesia:finalize_reindex_tx().
