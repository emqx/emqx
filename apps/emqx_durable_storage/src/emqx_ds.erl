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
-module(emqx_ds).

%% Management API:
-export([open_db/2]).

%% Message storage API:
-export([message_store/1, message_store/2, message_store/3]).

%% Message replay API:
-export([get_streams/3, open_iterator/2, next/2]).

%% internal exports:
-export([]).

-export_type([db/0, time/0, topic_filter/0, topic/0]).

%%================================================================================
%% Type declarations
%%================================================================================

%% Different DBs are completely independent from each other. They
%% could represent something like different tenants.
%%
%% Topics stored in different DBs aren't necesserily disjoint.
-type db() :: binary().

%% Parsed topic.
-type topic() :: list(binary()).

%% Parsed topic filter.
-type topic_filter() :: list(binary() | '+' | '#' | '').

%% This record enapsulates the stream entity from the replication
%% level.
%%
%% TODO: currently the stream is hardwired to only support the
%% internal rocksdb storage. In t he future we want to add another
%% implementations for emqx_ds, so this type has to take this into
%% account.
-record(stream,
        { shard :: emqx_ds_replication_layer:shard()
        , enc :: emqx_ds_replication_layer:stream()
        }).

-type stream_rank() :: {integer(), integer()}.

-opaque stream() :: #stream{}.

%% This record encapsulates the iterator entity from the replication
%% level.
-record(iterator,
        { shard :: emqx_ds_replication_layer:shard()
        , enc :: enqx_ds_replication_layer:iterator()
        }).

-opaque iterator() :: #iterator{}.

%% Timestamp
%% Earliest possible timestamp is 0.
%% TODO granularity?  Currently, we should always use micro second, as that's the unit we
%% use in emqx_guid.  Otherwise, the iterators won't match the message timestamps.
-type time() :: non_neg_integer().

-type message_store_opts() :: #{}.

-type create_db_opts() :: #{}.

-type message_id() :: emqx_ds_replication_layer:message_id().

-define(DEFAULT_DB, <<"default">>).

%%================================================================================
%% API funcions
%%================================================================================

-spec open_db(db(), create_db_opts()) -> ok.
open_db(DB, Opts) ->
    emqx_ds_replication_layer:open_db(DB, Opts).

-spec message_store([emqx_types:message()]) ->
          {ok, [message_id()]} | {error, _}.
message_store(Msgs) ->
    message_store(?DEFAULT_DB, Msgs, #{}).

-spec message_store(db(), [emqx_types:message()], message_store_opts()) ->
    {ok, [message_id()]} | {error, _}.
message_store(DB, Msgs, Opts) ->
    emqx_ds_replication_layer:message_store(DB, Msgs, Opts).

%% TODO: Do we really need to return message IDs? It's extra work...
-spec message_store(db(), [emqx_types:message()]) -> {ok, [message_id()]} | {error, _}.
message_store(DB, Msgs) ->
    message_store(DB, Msgs, #{}).

%% @doc Get a list of streams needed for replaying a topic filter.
%%
%% Motivation: under the hood, EMQX may store different topics at
%% different locations or even in different databases. A wildcard
%% topic filter may require pulling data from any number of locations.
%%
%% Stream is an abstraction exposed by `emqx_ds' that reflects the
%% notion that different topics can be stored differently, but hides
%% the implementation details.
%%
%% Rules:
%%
%% 1. New streams matching the topic filter can appear without notice,
%% so the replayer must periodically call this function to get the
%% updated list of streams.
%%
%% 2. Streams may depend on one another. Therefore, care should be
%% taken while replaying them in parallel to avoid out-of-order
%% replay. This function returns stream together with its
%% "coordinates": `{X, T, Stream}'. If X coordinate of two streams is
%% different, then they can be replayed in parallel.  If it's the
%% same, then the stream with smaller T coordinate should be replayed
%% first.
-spec get_streams(db(), topic_filter(), time()) -> [{stream_rank(), stream()}].
get_streams(DB, TopicFilter, StartTime) ->
    Shards = emqx_ds_replication_layer:list_shards(DB),
    lists:flatmap(
      fun(Shard) ->
              Streams = emqx_ds_replication_layer:get_streams(Shard, TopicFilter, StartTime),
              [{Rank, #stream{ shard = Shard
                             , enc = I
                             }} || {Rank, I} <- Streams]
      end,
      Shards).

-spec open_iterator(stream(), time()) -> {ok, iterator()} | {error, _}.
open_iterator(#stream{shard = Shard, enc = Stream}, StartTime) ->
    case emqx_ds_replication_layer:open_iterator(Shard, Stream, StartTime) of
        {ok, Iter} ->
            {ok, #iterator{shard = Shard, enc = Iter}};
        Err = {error, _} ->
            Err
    end.

-spec next(iterator(), pos_integer()) -> {ok, iterator(), [emqx_types:message()]} | end_of_stream.
next(#iterator{shard = Shard, enc = Iter0}, BatchSize) ->
    case emqx_ds_replication_layer:next(Shard, Iter0, BatchSize) of
        {ok, Iter, Batch} ->
            {ok, #iterator{shard = Shard, enc = Iter}, Batch};
        end_of_stream ->
            end_of_stream
    end.

%%================================================================================
%% behavior callbacks
%%================================================================================

%%================================================================================
%% Internal exports
%%================================================================================

%%================================================================================
%% Internal functions
%%================================================================================
