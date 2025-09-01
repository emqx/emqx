%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_ds_builtin_raft_proto_v1).
-moduledoc """
This module defines a protocol for RPC to the remote builtin_raft shards.
""".

-behavior(emqx_bpapi).

%% API:
-export([
    drop_db/2,
    get_streams/6,
    make_iterator/6,
    list_slabs/3,
    drop_slab/4,
    next/5,
    new_kv_tx_ctx/5
]).

%% = Changelog =
%% == v1 ==
%%
%% This protocol has been previous known as `emqx_ds_proto`.
%%
%% === poll ===
%%
%% API has been removed.
%%
%% === update_iterator ===
%%
%% API has been removed.
%%
%% === get_streams ===
%%
%% 1. StartTime is passed as is, without conversion to microseconds.
%%
%% 2. Added support for `generation_min'
%%
%% === make_iterator/6 ===
%%
%% StartTime is passed as is, without conversion to microseconds.
%%
%% === make_iterator_ttv/6 ===
%%
%% Added new function.
%%
%% === next/6 ===
%%
%% No longer returns the keys
%%
%% === get_delete_streams, delete_next, make_delete_iterator ===
%%
%% APIs have been removed

%% behavior callbacks:
-export([introduced_in/0]).

-include_lib("emqx_utils/include/bpapi.hrl").

-define(mod, emqx_ds_builtin_raft).

%%================================================================================
%% API functions
%%================================================================================

-spec drop_db([node()], emqx_ds:db()) ->
    [{ok, ok} | {error, _}].
drop_db(Node, DB) ->
    erpc:multicall(Node, ?mod, do_drop_db_v1, [DB]).

-spec get_streams(
    node(),
    emqx_ds:db(),
    emqx_ds:shard(),
    emqx_ds:topic_filter(),
    emqx_ds:time(),
    emqx_ds:generation()
) ->
    [{integer(), emqx_ds_storage_layer:stream()}].
get_streams(Node, DB, Shard, TopicFilter, Time, MinGeneration) ->
    erpc:call(Node, ?mod, do_get_streams_v1, [
        DB, Shard, TopicFilter, Time, MinGeneration
    ]).

-spec make_iterator(
    node(),
    emqx_ds:db(),
    emqx_ds:shard(),
    emqx_ds_storage_layer_ttv:stream(),
    emqx_ds:topic_filter(),
    emqx_ds:time()
) ->
    emqx_ds:make_iterator_result().
make_iterator(Node, DB, Shard, Stream, TopicFilter, StartTime) ->
    erpc:call(Node, ?mod, do_make_iterator_v1, [
        DB, Shard, Stream, TopicFilter, StartTime
    ]).

-spec next(
    node(),
    emqx_ds:db(),
    emqx_ds:shard(),
    emqx_ds_storage_layer_ttv:iterator(),
    emqx_ds:next_limit()
) ->
    emqx_rpc:call_result(emqx_ds:next_result()).
next(Node, DB, Shard, Iter, NextLimit) ->
    emqx_rpc:call(Shard, Node, ?mod, do_next_v1, [DB, Iter, NextLimit]).

-spec list_slabs(
    node(),
    emqx_ds:db(),
    emqx_ds:shard()
) ->
    #{emqx_ds:generation() => emqx_ds:slab_info()}.
list_slabs(Node, DB, Shard) ->
    erpc:call(Node, ?mod, do_list_slabs_v1, [DB, Shard]).

-spec drop_slab(
    node(),
    emqx_ds:db(),
    emqx_ds:shard(),
    emqx_ds_storage_layer:gen_id()
) ->
    ok | {error, _}.
drop_slab(Node, DB, Shard, GenId) ->
    erpc:call(Node, ?mod, do_drop_slab_v1, [DB, Shard, GenId]).

-spec new_kv_tx_ctx(
    node(), emqx_ds:db(), emqx_ds:shard(), emqx_ds:generation(), emqx_ds:transaction_opts()
) ->
    {ok, emqx_ds_builtin_raft:tx_context()} | emqx_ds:error(_).
new_kv_tx_ctx(Node, DB, Shard, Generation, Options) ->
    erpc:call(Node, emqx_ds_builtin_raft, do_new_kv_tx_ctx_v1, [
        DB, Shard, Generation, Options
    ]).

%%================================================================================
%% behavior callbacks
%%================================================================================

introduced_in() -> "6.0.0".
