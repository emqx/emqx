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
    update_iterator/3,
    next/3
]).

%% internal exports:
-export([
    do_drop_db_v1/1,
    do_store_batch_v1/4,
    do_get_streams_v1/4,
    do_make_iterator_v1/5,
    do_update_iterator_v2/4,
    do_next_v1/4
]).

-export_type([shard_id/0, builtin_db_opts/0, stream/0, iterator/0, message_id/0, batch/0]).

-include_lib("emqx_utils/include/emqx_message.hrl").

%%================================================================================
%% Type declarations
%%================================================================================

%% # "Record" integer keys.  We use maps with integer keys to avoid persisting and sending
%% records over the wire.

%% tags:
-define(STREAM, 1).
-define(IT, 2).
-define(BATCH, 3).

%% keys:
-define(tag, 1).
-define(shard, 2).
-define(enc, 3).

-type shard_id() :: binary().

-type builtin_db_opts() ::
    #{
        backend := builtin,
        storage := emqx_ds_storage_layer:prototype(),
        n_shards => pos_integer(),
        replication_factor => pos_integer()
    }.

%% This enapsulates the stream entity from the replication level.
%%
%% TODO: currently the stream is hardwired to only support the
%% internal rocksdb storage. In the future we want to add another
%% implementations for emqx_ds, so this type has to take this into
%% account.
-opaque stream() ::
    #{
        ?tag := ?STREAM,
        ?shard := emqx_ds_replication_layer:shard_id(),
        ?enc := emqx_ds_storage_layer:stream()
    }.

-opaque iterator() ::
    #{
        ?tag := ?IT,
        ?shard := emqx_ds_replication_layer:shard_id(),
        ?enc := emqx_ds_storage_layer:iterator()
    }.

-type message_id() :: emqx_ds:message_id().

-define(batch_messages, 2).

-type batch() :: #{
    ?tag := ?BATCH,
    ?batch_messages := [emqx_types:message()]
}.

%%================================================================================
%% API functions
%%================================================================================

-spec list_shards(emqx_ds:db()) -> [shard_id()].
list_shards(DB) ->
    emqx_ds_replication_layer_meta:shards(DB).

-spec open_db(emqx_ds:db(), builtin_db_opts()) -> ok | {error, _}.
open_db(DB, CreateOpts) ->
    Opts = emqx_ds_replication_layer_meta:open_db(DB, CreateOpts),
    MyShards = emqx_ds_replication_layer_meta:my_shards(DB),
    lists:foreach(
        fun(Shard) ->
            emqx_ds_storage_layer:open_shard({DB, Shard}, Opts),
            maybe_set_myself_as_leader(DB, Shard)
        end,
        MyShards
    ).

-spec drop_db(emqx_ds:db()) -> ok | {error, _}.
drop_db(DB) ->
    Nodes = list_nodes(),
    _ = emqx_ds_proto_v1:drop_db(Nodes, DB),
    _ = emqx_ds_replication_layer_meta:drop_db(DB),
    ok.

-spec store_batch(emqx_ds:db(), [emqx_types:message(), ...], emqx_ds:message_store_opts()) ->
    emqx_ds:store_batch_result().
store_batch(DB, Messages, Opts) ->
    Shard = shard_of_messages(DB, Messages),
    Node = node_of_shard(DB, Shard),
    Batch = #{?tag => ?BATCH, ?batch_messages => Messages},
    emqx_ds_proto_v1:store_batch(Node, DB, Shard, Batch, Opts).

-spec get_streams(emqx_ds:db(), emqx_ds:topic_filter(), emqx_ds:time()) ->
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
                        ?tag => ?STREAM,
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
    #{?tag := ?STREAM, ?shard := Shard, ?enc := StorageStream} = Stream,
    Node = node_of_shard(DB, Shard),
    case emqx_ds_proto_v1:make_iterator(Node, DB, Shard, StorageStream, TopicFilter, StartTime) of
        {ok, Iter} ->
            {ok, #{?tag => ?IT, ?shard => Shard, ?enc => Iter}};
        Err = {error, _} ->
            Err
    end.

-spec update_iterator(
    emqx_ds:db(),
    iterator(),
    emqx_ds:message_key()
) ->
    emqx_ds:make_iterator_result(iterator()).
update_iterator(DB, OldIter, DSKey) ->
    #{?tag := ?IT, ?shard := Shard, ?enc := StorageIter} = OldIter,
    Node = node_of_shard(DB, Shard),
    case
        emqx_ds_proto_v2:update_iterator(
            Node,
            DB,
            Shard,
            StorageIter,
            DSKey
        )
    of
        {ok, Iter} ->
            {ok, #{?tag => ?IT, ?shard => Shard, ?enc => Iter}};
        Err = {error, _} ->
            Err
    end.

-spec next(emqx_ds:db(), iterator(), pos_integer()) -> emqx_ds:next_result(iterator()).
next(DB, Iter0, BatchSize) ->
    #{?tag := ?IT, ?shard := Shard, ?enc := StorageIter0} = Iter0,
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

-spec do_drop_db_v1(emqx_ds:db()) -> ok | {error, _}.
do_drop_db_v1(DB) ->
    MyShards = emqx_ds_replication_layer_meta:my_shards(DB),
    lists:foreach(
        fun(Shard) ->
            emqx_ds_storage_layer:drop_shard({DB, Shard})
        end,
        MyShards
    ).

-spec do_store_batch_v1(
    emqx_ds:db(),
    emqx_ds_replication_layer:shard_id(),
    batch(),
    emqx_ds:message_store_opts()
) ->
    emqx_ds:store_batch_result().
do_store_batch_v1(DB, Shard, #{?tag := ?BATCH, ?batch_messages := Messages}, Options) ->
    emqx_ds_storage_layer:store_batch({DB, Shard}, Messages, Options).

-spec do_get_streams_v1(
    emqx_ds:db(), emqx_ds_replication_layer:shard_id(), emqx_ds:topic_filter(), emqx_ds:time()
) ->
    [{integer(), emqx_ds_storage_layer:stream()}].
do_get_streams_v1(DB, Shard, TopicFilter, StartTime) ->
    emqx_ds_storage_layer:get_streams({DB, Shard}, TopicFilter, StartTime).

-spec do_make_iterator_v1(
    emqx_ds:db(),
    emqx_ds_replication_layer:shard_id(),
    emqx_ds_storage_layer:stream(),
    emqx_ds:topic_filter(),
    emqx_ds:time()
) ->
    {ok, emqx_ds_storage_layer:iterator()} | {error, _}.
do_make_iterator_v1(DB, Shard, Stream, TopicFilter, StartTime) ->
    emqx_ds_storage_layer:make_iterator({DB, Shard}, Stream, TopicFilter, StartTime).

-spec do_update_iterator_v2(
    emqx_ds:db(),
    emqx_ds_storage_layer:shard_id(),
    emqx_ds_storage_layer:iterator(),
    emqx_ds:message_key()
) ->
    {ok, emqx_ds_storage_layer:iterator()} | {error, _}.
do_update_iterator_v2(DB, Shard, OldIter, DSKey) ->
    emqx_ds_storage_layer:update_iterator(
        {DB, Shard}, OldIter, DSKey
    ).

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

%% TODO: there's no real leader election right now
-spec maybe_set_myself_as_leader(emqx_ds:db(), shard_id()) -> ok.
maybe_set_myself_as_leader(DB, Shard) ->
    Site = emqx_ds_replication_layer_meta:this_site(),
    case emqx_ds_replication_layer_meta:in_sync_replicas(DB, Shard) of
        [Site | _] ->
            %% Currently the first in-sync replica always becomes the
            %% leader
            ok = emqx_ds_replication_layer_meta:set_leader(DB, Shard, node());
        _Sites ->
            ok
    end.

-spec node_of_shard(emqx_ds:db(), shard_id()) -> node().
node_of_shard(DB, Shard) ->
    case emqx_ds_replication_layer_meta:shard_leader(DB, Shard) of
        {ok, Leader} ->
            Leader;
        {error, no_leader_for_shard} ->
            %% TODO: use optvar
            timer:sleep(500),
            node_of_shard(DB, Shard)
    end.

%% Here we assume that all messages in the batch come from the same client
shard_of_messages(DB, [#message{from = From} | _]) ->
    N = emqx_ds_replication_layer_meta:n_shards(DB),
    integer_to_binary(erlang:phash2(From, N)).

list_nodes() ->
    mria:running_nodes().
