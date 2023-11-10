%%--------------------------------------------------------------------
%% Copyright (c) 2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

%% @doc Replication layer for DS backends that don't support
%% replication on their own.
-module(emqx_ds_replication_layer).

-behaviour(emqx_ds).

-export([
    list_shards/1,
    open_db/2,
    drop_db/1,
    store_batch/3,
    get_streams/3,
    make_iterator/4,
    next/3
]).

%% internal exports:
-export([
    do_open_shard_v1/3,
    do_drop_shard_v1/2,
    do_store_batch_v1/4,
    do_get_streams_v1/4,
    do_make_iterator_v1/5,
    do_next_v1/4
]).

-export_type([shard_id/0, stream/0, iterator/0, message_id/0]).

%%================================================================================
%% Type declarations
%%================================================================================

%% # "Record" integer keys.  We use maps with integer keys to avoid persisting and sending
%% records over the wire.

%% tags:
-define(stream, stream).
-define(it, it).

%% keys:
-define(tag, 1).
-define(shard, 2).
-define(enc, 3).

-type db() :: emqx_ds:db().

-type shard_id() :: atom().

%% This enapsulates the stream entity from the replication level.
%%
%% TODO: currently the stream is hardwired to only support the
%% internal rocksdb storage. In the future we want to add another
%% implementations for emqx_ds, so this type has to take this into
%% account.
-opaque stream() ::
    #{
        ?tag := ?stream,
        ?shard := emqx_ds_replication_layer:shard_id(),
        ?enc := emqx_ds_storage_layer:stream()
    }.

-opaque iterator() ::
    #{
        ?tag := ?it,
        ?shard := emqx_ds_replication_layer:shard_id(),
        ?enc := emqx_ds_storage_layer:iterator()
    }.

-type message_id() :: emqx_ds_storage_layer:message_id().

%%================================================================================
%% API functions
%%================================================================================

-spec list_shards(db()) -> [shard_id()].
list_shards(_DB) ->
    %% TODO: milestone 5
    list_nodes().

-spec open_db(db(), emqx_ds:create_db_opts()) -> ok | {error, _}.
open_db(DB, Opts) ->
    %% TODO: improve error reporting, don't just crash
    lists:foreach(
        fun(Shard) ->
            Node = node_of_shard(DB, Shard),
            ok = emqx_ds_proto_v1:open_shard(Node, DB, Shard, Opts)
        end,
        list_shards(DB)
    ).

-spec drop_db(db()) -> ok | {error, _}.
drop_db(DB) ->
    lists:foreach(
        fun(Shard) ->
            Node = node_of_shard(DB, Shard),
            ok = emqx_ds_proto_v1:drop_shard(Node, DB, Shard)
        end,
        list_shards(DB)
    ).

-spec store_batch(db(), [emqx_types:message()], emqx_ds:message_store_opts()) ->
    emqx_ds:store_batch_result().
store_batch(DB, Batch, Opts) ->
    %% TODO: Currently we store messages locally.
    Shard = node(),
    Node = node_of_shard(DB, Shard),
    emqx_ds_proto_v1:store_batch(Node, DB, Shard, Batch, Opts).

-spec get_streams(db(), emqx_ds:topic_filter(), emqx_ds:time()) ->
    [{emqx_ds:stream_rank(), stream()}].
get_streams(DB, TopicFilter, StartTime) ->
    Shards = list_shards(DB),
    lists:flatmap(
        fun(Shard) ->
            Node = node_of_shard(DB, Shard),
            Streams = emqx_ds_proto_v1:get_streams(Node, DB, Shard, TopicFilter, StartTime),
            lists:map(
                fun({RankY, Stream}) ->
                    RankX = Shard,
                    Rank = {RankX, RankY},
                    {Rank, #{
                        ?tag => ?stream,
                        ?shard => Shard,
                        ?enc => Stream
                    }}
                end,
                Streams
            )
        end,
        Shards
    ).

-spec make_iterator(emqx_ds:db(), stream(), emqx_ds:topic_filter(), emqx_ds:time()) ->
    emqx_ds:make_iterator_result(iterator()).
make_iterator(DB, Stream, TopicFilter, StartTime) ->
    #{?tag := ?stream, ?shard := Shard, ?enc := StorageStream} = Stream,
    Node = node_of_shard(DB, Shard),
    case emqx_ds_proto_v1:make_iterator(Node, DB, Shard, StorageStream, TopicFilter, StartTime) of
        {ok, Iter} ->
            {ok, #{?tag => ?it, ?shard => Shard, ?enc => Iter}};
        Err = {error, _} ->
            Err
    end.

-spec next(emqx_ds:db(), iterator(), pos_integer()) -> emqx_ds:next_result(iterator()).
next(DB, Iter0, BatchSize) ->
    #{?tag := ?it, ?shard := Shard, ?enc := StorageIter0} = Iter0,
    Node = node_of_shard(DB, Shard),
    %% TODO: iterator can contain information that is useful for
    %% reconstructing messages sent over the network. For example,
    %% when we send messages with the learned topic index, we could
    %% send the static part of topic once, and append it to the
    %% messages on the receiving node, hence saving some network.
    %%
    %% This kind of trickery should be probably done here in the
    %% replication layer. Or, perhaps, in the logic layer.
    case emqx_ds_proto_v1:next(Node, DB, Shard, StorageIter0, BatchSize) of
        {ok, StorageIter, Batch} ->
            Iter = Iter0#{?enc := StorageIter},
            {ok, Iter, Batch};
        Other ->
            Other
    end.

%%================================================================================
%% behavior callbacks
%%================================================================================

%%================================================================================
%% Internal exports (RPC targets)
%%================================================================================

-spec do_open_shard_v1(db(), emqx_ds_replication_layer:shard_id(), emqx_ds:create_db_opts()) ->
    ok | {error, _}.
do_open_shard_v1(DB, Shard, Opts) ->
    emqx_ds_storage_layer:open_shard({DB, Shard}, Opts).

-spec do_drop_shard_v1(db(), emqx_ds_replication_layer:shard_id()) -> ok | {error, _}.
do_drop_shard_v1(DB, Shard) ->
    emqx_ds_storage_layer:drop_shard({DB, Shard}).

-spec do_store_batch_v1(
    db(), emqx_ds_replication_layer:shard_id(), [emqx_types:message()], emqx_ds:message_store_opts()
) ->
    emqx_ds:store_batch_result().
do_store_batch_v1(DB, Shard, Batch, Options) ->
    emqx_ds_storage_layer:store_batch({DB, Shard}, Batch, Options).

-spec do_get_streams_v1(
    emqx_ds:db(), emqx_ds_replicationi_layer:shard_id(), emqx_ds:topic_filter(), emqx_ds:time()
) ->
    [{integer(), emqx_ds_storage_layer:stream()}].
do_get_streams_v1(DB, Shard, TopicFilter, StartTime) ->
    emqx_ds_storage_layer:get_streams({DB, Shard}, TopicFilter, StartTime).

-spec do_make_iterator_v1(
    emqx_ds:db(),
    emqx_ds_storage_layer:shard_id(),
    emqx_ds_storage_layer:stream(),
    emqx_ds:topic_filter(),
    emqx_ds:time()
) ->
    {ok, emqx_ds_storage_layer:iterator()} | {error, _}.
do_make_iterator_v1(DB, Shard, Stream, TopicFilter, StartTime) ->
    emqx_ds_storage_layer:make_iterator({DB, Shard}, Stream, TopicFilter, StartTime).

-spec do_next_v1(
    emqx_ds:db(),
    emqx_ds_replication_layer:shard_id(),
    emqx_ds_storage_layer:iterator(),
    pos_integer()
) ->
    emqx_ds:next_result(emqx_ds_storage_layer:iterator()).
do_next_v1(DB, Shard, Iter, BatchSize) ->
    emqx_ds_storage_layer:next({DB, Shard}, Iter, BatchSize).

%%================================================================================
%% Internal functions
%%================================================================================

-spec node_of_shard(emqx_ds:db(), shard_id()) -> node().
node_of_shard(_DB, Node) ->
    Node.

list_nodes() ->
    mria:running_nodes().
