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
-module(emqx_ds_replication_layer).

-export([
          list_shards/1,
          create_db/2,
          message_store/3,
          get_streams/3,
          open_iterator/3,
          next/3
        ]).


%% internal exports:
-export([ do_create_shard_v1/2,
          do_get_streams_v1/3,
          do_open_iterator_v1/3,
          do_next_v1/3
        ]).

-export_type([shard/0, stream/0, iterator/0, message_id/0]).

%%================================================================================
%% Type declarations
%%================================================================================

-opaque stream() :: emqx_ds_storage_layer:stream().

-type shard() :: binary().

-opaque iterator() :: emqx_ds_storage_layer:iterator().

-type message_id() :: emqx_ds_storage_layer:message_id().

%%================================================================================
%% API functions
%%================================================================================

-spec list_shards(emqx_ds:db()) -> [shard()].
list_shards(DB) ->
    %% TODO: milestone 5
    lists:map(
      fun(Node) ->
              term_to_binary({DB, Node})
      end,
      list_nodes()).

-spec create_db(emqx_ds:db(), emqx_ds:create_db_opts()) -> ok.
create_db(DB, Opts) ->
    lists:foreach(
      fun(Node) ->
              Shard = term_to_binary({DB, Node}),
              emqx_ds_proto_v1:create_shard(Node, Shard, Opts)
      end,
      list_nodes()).

-spec message_store(emqx_ds:db(), [emqx_types:message()], emqx_ds:message_store_opts()) ->
    {ok, [message_id()]} | {error, _}.
message_store(DB, Msg, Opts) ->
    %% TODO: milestone 5. Currently we store messages locally.
    Shard = term_to_binary({DB, node()}),
    emqx_ds_storage_layer:message_store(Shard, Msg, Opts).

-spec get_streams(shard(), emqx_ds:topic_filter(), emqx_ds:time()) -> [{emqx_ds:stream_rank(), stream()}].
get_streams(Shard, TopicFilter, StartTime) ->
    Node = node_of_shard(Shard),
    emqx_ds_proto_v1:get_streams(Node, Shard, TopicFilter, StartTime).

-spec open_iterator(shard(), stream(), emqx_ds:time()) -> {ok, iterator()} | {error, _}.
open_iterator(Shard, Stream, StartTime) ->
    Node = node_of_shard(Shard),
    emqx_ds_proto_v1:open_iterator(Node, Shard, Stream, StartTime).

-spec next(shard(), iterator(), pos_integer()) ->
          {ok, iterator(), [emqx_types:message()]} | end_of_stream.
next(Shard, Iter, BatchSize) ->
    Node = node_of_shard(Shard),
    %% TODO: iterator can contain information that is useful for
    %% reconstructing messages sent over the network. For example,
    %% when we send messages with the learned topic index, we could
    %% send the static part of topic once, and append it to the
    %% messages on the receiving node, hence saving some network.
    %%
    %% This kind of trickery should be probably done here in the
    %% replication layer. Or, perhaps, in the logic lary.
    emqx_ds_proto_v1:next(Node, Shard, Iter, BatchSize).

%%================================================================================
%% behavior callbacks
%%================================================================================

%%================================================================================
%% Internal exports (RPC targets)
%%================================================================================

-spec do_create_shard_v1(shard(), emqx_ds:create_db_opts()) -> ok.
do_create_shard_v1(Shard, Opts) ->
    error({todo, Shard, Opts}).

-spec do_get_streams_v1(shard(), emqx_ds:topic_filter(), emqx_ds:time()) ->
          [{emqx_ds:stream_rank(), stream()}].
do_get_streams_v1(Shard, TopicFilter, StartTime) ->
    error({todo, Shard, TopicFilter, StartTime}).

-spec do_open_iterator_v1(shard(), stream(), emqx_ds:time()) -> iterator().
do_open_iterator_v1(Shard, Stream, StartTime) ->
    error({todo, Shard, Stream, StartTime}).

-spec do_next_v1(shard(), iterator(), non_neg_integer()) ->
          {ok, iterator(), [emqx_types:message()]} | end_of_stream.
do_next_v1(Shard, Iter, BatchSize) ->
    error({todo, Shard, Iter, BatchSize}).

%%================================================================================
%% Internal functions
%%================================================================================

-spec node_of_shard(shard()) -> node().
node_of_shard(ShardId) ->
    {_DB, Node} = binary_to_term(ShardId),
    Node.

list_nodes() ->
    mria:running_nodes().
