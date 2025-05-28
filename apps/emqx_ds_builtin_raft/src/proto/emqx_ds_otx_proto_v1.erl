%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_ds_otx_proto_v1).

-behavior(emqx_bpapi).

-include_lib("emqx_utils/include/bpapi.hrl").
%% API:
-export([
    new_kv_tx_ctx/5
]).

%% behavior callbacks:
-export([introduced_in/0]).

%%================================================================================
%% API functions
%%================================================================================

-spec new_kv_tx_ctx(
    node(), emqx_ds:db(), emqx_ds:shard(), emqx_ds:generation(), emqx_ds:transaction_opts()
) ->
    {ok, emqx_ds_replication_layer:tx_context()} | emqx_ds:error(_).
new_kv_tx_ctx(Node, DB, Shard, Generation, Options) ->
    erpc:call(Node, emqx_ds_replication_layer, do_new_kv_tx_ctx_v1, [
        DB, Shard, Generation, Options
    ]).

%%================================================================================
%% behavior callbacks
%%================================================================================

introduced_in() ->
    "5.10.0".
