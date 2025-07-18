%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_ds_proto_v6).

-behavior(emqx_bpapi).

%% API:
-export([
    drop_db/2,
    store_batch/5,
    get_streams/6,
    make_iterator/6,
    make_iterator_ttv/6,
    add_generation/2,
    list_generations_with_lifetimes/3,
    drop_generation/4,

    %% introduced in v4
    get_delete_streams/5,
    make_delete_iterator/6,
    delete_next/6,

    %% Changed in v6: return results without keys.
    next/5,
    next_ttv/5
]).

%% = Changelog =
%% == v6 ==
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

%% behavior callbacks:
-export([introduced_in/0]).

-include_lib("emqx_utils/include/bpapi.hrl").

%%================================================================================
%% API functions
%%================================================================================

-spec drop_db([node()], emqx_ds:db()) ->
    [{ok, ok} | {error, _}].
drop_db(Node, DB) ->
    erpc:multicall(Node, emqx_ds_replication_layer, do_drop_db_v1, [DB]).

-spec get_streams(
    node(),
    emqx_ds:db(),
    emqx_ds_replication_layer:shard_id(),
    emqx_ds:topic_filter(),
    emqx_ds:time(),
    emqx_ds:generation()
) ->
    [{integer(), emqx_ds_storage_layer:stream()}].
get_streams(Node, DB, Shard, TopicFilter, Time, MinGeneration) ->
    erpc:call(Node, emqx_ds_replication_layer, do_get_streams_v3, [
        DB, Shard, TopicFilter, Time, MinGeneration
    ]).

-spec make_iterator(
    node(),
    emqx_ds:db(),
    emqx_ds_replication_layer:shard_id(),
    emqx_ds_storage_layer:stream(),
    emqx_ds:topic_filter(),
    emqx_ds:time()
) ->
    emqx_ds:make_iterator_result().
make_iterator(Node, DB, Shard, Stream, TopicFilter, StartTime) ->
    erpc:call(Node, emqx_ds_replication_layer, do_make_iterator_v3, [
        DB, Shard, Stream, TopicFilter, StartTime
    ]).

-spec make_iterator_ttv(
    node(),
    emqx_ds:db(),
    emqx_ds_replication_layer:shard_id(),
    emqx_ds_storage_layer_ttv:stream(),
    emqx_ds:topic_filter(),
    emqx_ds:time()
) ->
    emqx_ds:make_iterator_result().
make_iterator_ttv(Node, DB, Shard, Stream, TopicFilter, StartTime) ->
    erpc:call(Node, emqx_ds_replication_layer, do_make_iterator_ttv_v1, [
        DB, Shard, Stream, TopicFilter, StartTime
    ]).

-spec next(
    node(),
    emqx_ds:db(),
    emqx_ds_replication_layer:shard_id(),
    emqx_ds_storage_layer:iterator(),
    emqx_ds:next_limit()
) ->
    emqx_rpc:call_result(emqx_ds:next_result()).
next(Node, DB, Shard, Iter, NextLimit) ->
    emqx_rpc:call(Shard, Node, emqx_ds_replication_layer, do_next_v2, [DB, Shard, Iter, NextLimit]).

-spec next_ttv(
    node(),
    emqx_ds:db(),
    emqx_ds:shard(),
    emqx_ds_storage_layer_ttv:iterator(),
    emqx_ds:next_limit()
) ->
    emqx_rpc:call_result(emqx_ds:next_result()).
next_ttv(Node, DB, Shard, Iter, NextLimit) ->
    emqx_rpc:call(Shard, Node, emqx_ds_replication_layer, do_next_ttv, [DB, Iter, NextLimit]).

-spec store_batch(
    node(),
    emqx_ds:db(),
    emqx_ds_replication_layer:shard_id(),
    emqx_ds_replication_layer:batch(),
    emqx_ds:message_store_opts()
) ->
    emqx_ds:store_batch_result().
store_batch(Node, DB, Shard, Batch, Options) ->
    emqx_rpc:call(Shard, Node, emqx_ds_replication_layer, do_store_batch_v1, [
        DB, Shard, Batch, Options
    ]).

-spec add_generation([node()], emqx_ds:db()) ->
    [{ok, ok} | {error, _}].
add_generation(Node, DB) ->
    erpc:multicall(Node, emqx_ds_replication_layer, do_add_generation_v2, [DB]).

-spec list_generations_with_lifetimes(
    node(),
    emqx_ds:db(),
    emqx_ds_replication_layer:shard_id()
) ->
    #{
        emqx_ds:generation() => emqx_ds:slab_info()
    }.
list_generations_with_lifetimes(Node, DB, Shard) ->
    erpc:call(Node, emqx_ds_replication_layer, do_list_generations_with_lifetimes_v3, [DB, Shard]).

-spec drop_generation(
    node(),
    emqx_ds:db(),
    emqx_ds_replication_layer:shard_id(),
    emqx_ds_storage_layer:gen_id()
) ->
    ok | {error, _}.
drop_generation(Node, DB, Shard, GenId) ->
    erpc:call(Node, emqx_ds_replication_layer, do_drop_generation_v3, [DB, Shard, GenId]).

%%--------------------------------------------------------------------------------
%% Introduced in V4
%%--------------------------------------------------------------------------------

-spec get_delete_streams(
    node(),
    emqx_ds:db(),
    emqx_ds_replication_layer:shard_id(),
    emqx_ds:topic_filter(),
    emqx_ds:time()
) ->
    [emqx_ds_storage_layer:delete_stream()].
get_delete_streams(Node, DB, Shard, TopicFilter, Time) ->
    erpc:call(Node, emqx_ds_replication_layer, do_get_delete_streams_v4, [
        DB, Shard, TopicFilter, Time
    ]).

-spec make_delete_iterator(
    node(),
    emqx_ds:db(),
    emqx_ds_replication_layer:shard_id(),
    emqx_ds_storage_layer:delete_stream(),
    emqx_ds:topic_filter(),
    emqx_ds:time()
) ->
    {ok, emqx_ds_storage_layer:delete_iterator()} | {error, _}.
make_delete_iterator(Node, DB, Shard, Stream, TopicFilter, StartTime) ->
    erpc:call(Node, emqx_ds_replication_layer, do_make_delete_iterator_v4, [
        DB, Shard, Stream, TopicFilter, StartTime
    ]).

-spec delete_next(
    node(),
    emqx_ds:db(),
    emqx_ds_replication_layer:shard_id(),
    emqx_ds_storage_layer:delete_iterator(),
    emqx_ds:delete_selector(),
    pos_integer()
) ->
    {ok, emqx_ds_storage_layer:delete_iterator(), non_neg_integer()}
    | {ok, end_of_stream}
    | {error, _}.
delete_next(Node, DB, Shard, Iter, Selector, BatchSize) ->
    erpc:call(
        Node,
        emqx_ds_replication_layer,
        do_delete_next_v4,
        [DB, Shard, Iter, Selector, BatchSize]
    ).

%%================================================================================
%% behavior callbacks
%%================================================================================

introduced_in() -> "6.0.0".
