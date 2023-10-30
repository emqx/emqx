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

-export([
    list_shards/1,
    open_db/2,
    drop_db/1,
    store_batch/3,
    get_streams/3,
    make_iterator/3,
    next/2,
    save_iterator/3,
    get_iterator/2
]).

%% internal exports:
-export([
    do_open_shard_v1/2,
    do_drop_shard_v1/1,
    do_get_streams_v1/3,
    do_make_iterator_v1/4,
    do_next_v1/3
]).

-export_type([shard_id/0, stream/0, iterator/0, message_id/0]).

%%================================================================================
%% Type declarations
%%================================================================================

-type db() :: emqx_ds:db().

-type shard_id() :: {db(), atom()}.

%% This record enapsulates the stream entity from the replication
%% level.
%%
%% TODO: currently the stream is hardwired to only support the
%% internal rocksdb storage. In the future we want to add another
%% implementations for emqx_ds, so this type has to take this into
%% account.
-record(stream, {
    shard :: emqx_ds_replication_layer:shard_id(),
    enc :: emqx_ds_storage_layer:stream()
}).

-opaque stream() :: #stream{}.

-record(iterator, {
    shard :: emqx_ds_replication_layer:shard_id(),
    enc :: enqx_ds_storage_layer:iterator()
}).

-opaque iterator() :: #iterator{}.

-type message_id() :: emqx_ds_storage_layer:message_id().

%%================================================================================
%% API functions
%%================================================================================

-spec list_shards(db()) -> [shard_id()].
list_shards(DB) ->
    %% TODO: milestone 5
    lists:map(
        fun(Node) ->
            shard_id(DB, Node)
        end,
        list_nodes()
    ).

-spec open_db(db(), emqx_ds:create_db_opts()) -> ok | {error, _}.
open_db(DB, Opts) ->
    %% TODO: improve error reporting, don't just crash
    lists:foreach(
        fun(Node) ->
            Shard = shard_id(DB, Node),
            ok = emqx_ds_proto_v1:open_shard(Node, Shard, Opts)
        end,
        list_nodes()
    ).

-spec drop_db(db()) -> ok | {error, _}.
drop_db(DB) ->
    lists:foreach(
        fun(Node) ->
            Shard = shard_id(DB, Node),
            ok = emqx_ds_proto_v1:drop_shard(Node, Shard)
        end,
        list_nodes()
    ).

-spec store_batch(db(), [emqx_types:message()], emqx_ds:message_store_opts()) ->
    emqx_ds:store_batch_result().
store_batch(DB, Msg, Opts) ->
    %% TODO: Currently we store messages locally.
    Shard = shard_id(DB, node()),
    emqx_ds_storage_layer:store_batch(Shard, Msg, Opts).

-spec get_streams(db(), emqx_ds:topic_filter(), emqx_ds:time()) ->
    [{emqx_ds:stream_rank(), stream()}].
get_streams(DB, TopicFilter, StartTime) ->
    Shards = list_shards(DB),
    lists:flatmap(
        fun(Shard) ->
            Node = node_of_shard(Shard),
            Streams = emqx_ds_proto_v1:get_streams(Node, Shard, TopicFilter, StartTime),
            lists:map(
                fun({RankY, Stream}) ->
                    RankX = Shard,
                    Rank = {RankX, RankY},
                    {Rank, #stream{
                        shard = Shard,
                        enc = Stream
                    }}
                end,
                Streams
            )
        end,
        Shards
    ).

-spec make_iterator(stream(), emqx_ds:topic_filter(), emqx_ds:time()) ->
    emqx_ds:make_iterator_result(iterator()).
make_iterator(Stream, TopicFilter, StartTime) ->
    #stream{shard = Shard, enc = StorageStream} = Stream,
    Node = node_of_shard(Shard),
    case emqx_ds_proto_v1:make_iterator(Node, Shard, StorageStream, TopicFilter, StartTime) of
        {ok, Iter} ->
            {ok, #iterator{shard = Shard, enc = Iter}};
        Err = {error, _} ->
            Err
    end.

-spec next(iterator(), pos_integer()) -> emqx_ds:next_result(iterator()).
next(Iter0, BatchSize) ->
    #iterator{shard = Shard, enc = StorageIter0} = Iter0,
    Node = node_of_shard(Shard),
    %% TODO: iterator can contain information that is useful for
    %% reconstructing messages sent over the network. For example,
    %% when we send messages with the learned topic index, we could
    %% send the static part of topic once, and append it to the
    %% messages on the receiving node, hence saving some network.
    %%
    %% This kind of trickery should be probably done here in the
    %% replication layer. Or, perhaps, in the logic layer.
    case emqx_ds_proto_v1:next(Node, Shard, StorageIter0, BatchSize) of
        {ok, StorageIter, Batch} ->
            Iter = #iterator{shard = Shard, enc = StorageIter},
            {ok, Iter, Batch};
        Other ->
            Other
    end.

-spec save_iterator(db(), emqx_ds:iterator_id(), iterator()) -> ok.
save_iterator(_DB, _ITRef, _Iterator) ->
    error(todo).

-spec get_iterator(db(), emqx_ds:iterator_id()) -> emqx_ds:get_iterator_result(iterator()).
get_iterator(_DB, _ITRef) ->
    error(todo).

%%================================================================================
%% behavior callbacks
%%================================================================================

%%================================================================================
%% Internal exports (RPC targets)
%%================================================================================

-spec do_open_shard_v1(shard_id(), emqx_ds:create_db_opts()) -> ok.
do_open_shard_v1(Shard, Opts) ->
    emqx_ds_storage_layer:open_shard(Shard, Opts).

-spec do_drop_shard_v1(shard_id()) -> ok.
do_drop_shard_v1(Shard) ->
    emqx_ds_storage_layer:drop_shard(Shard).

-spec do_get_streams_v1(shard_id(), emqx_ds:topic_filter(), emqx_ds:time()) ->
    [{integer(), _Stream}].
do_get_streams_v1(Shard, TopicFilter, StartTime) ->
    emqx_ds_storage_layer:get_streams(Shard, TopicFilter, StartTime).

-spec do_make_iterator_v1(shard_id(), _Stream, emqx_ds:topic_filter(), emqx_ds:time()) ->
    {ok, iterator()} | {error, _}.
do_make_iterator_v1(Shard, Stream, TopicFilter, StartTime) ->
    emqx_ds_storage_layer:make_iterator(Shard, Stream, TopicFilter, StartTime).

-spec do_next_v1(shard_id(), Iter, pos_integer()) -> emqx_ds:next_result(Iter).
do_next_v1(Shard, Iter, BatchSize) ->
    emqx_ds_storage_layer:next(Shard, Iter, BatchSize).

%%================================================================================
%% Internal functions
%%================================================================================

shard_id(DB, Node) ->
    %% TODO: don't bake node name into the schema, don't repeat the
    %% Mnesia's 1M$ mistake.
    {DB, Node}.

-spec node_of_shard(shard_id()) -> node().
node_of_shard({_DB, Node}) ->
    Node.

list_nodes() ->
    mria:running_nodes().
