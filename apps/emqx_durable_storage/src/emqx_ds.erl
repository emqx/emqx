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

%% @doc Main interface module for `emqx_durable_storage' application.
%%
%% It takes care of forwarding calls to the underlying DBMS. Currently
%% only the embedded `emqx_ds_replication_layer' storage is supported,
%% so all the calls are simply passed through.
-module(emqx_ds).

%% Management API:
-export([open_db/2, drop_db/1]).

%% Message storage API:
-export([store_batch/2, store_batch/3]).

%% Message replay API:
-export([get_streams/3, make_iterator/3, next/2]).

%% Iterator storage API:
-export([save_iterator/3, get_iterator/2]).

%% Misc. API:
-export([]).

-export_type([
    create_db_opts/0,
    builtin_db_opts/0,
    db/0,
    time/0,
    topic_filter/0,
    topic/0,
    stream/0,
    stream_rank/0,
    iterator/0,
    message_id/0,
    next_result/1, next_result/0,
    store_batch_result/0,
    make_iterator_result/1, make_iterator_result/0,
    get_iterator_result/1
]).

%%================================================================================
%% Type declarations
%%================================================================================

-type db() :: atom().

%% Parsed topic.
-type topic() :: list(binary()).

%% Parsed topic filter.
-type topic_filter() :: list(binary() | '+' | '#' | '').

-type stream_rank() :: {term(), integer()}.

-opaque stream() :: emqx_ds_replication_layer:stream().

-opaque iterator() :: emqx_ds_replication_layer:iterator().

-type store_batch_result() :: ok | {error, _}.

-type make_iterator_result(Iterator) :: {ok, Iterator} | {error, _}.

-type make_iterator_result() :: make_iterator_result(iterator()).

-type next_result(Iterator) ::
    {ok, Iterator, [emqx_types:message()]} | {ok, end_of_stream} | {error, _}.

-type next_result() :: next_result(iterator()).

%% Timestamp
%% Earliest possible timestamp is 0.
%% TODO granularity?  Currently, we should always use micro second, as that's the unit we
%% use in emqx_guid.  Otherwise, the iterators won't match the message timestamps.
-type time() :: non_neg_integer().

-type message_store_opts() :: #{}.

-type builtin_db_opts() ::
    #{
        backend := builtin,
        storage := emqx_ds_storage_layer:prototype()
    }.

-type create_db_opts() ::
    builtin_db_opts().

-type message_id() :: emqx_ds_replication_layer:message_id().

-type iterator_id() :: term().

-type get_iterator_result(Iterator) :: {ok, Iterator} | undefined.

%%================================================================================
%% API funcions
%%================================================================================

%% @doc Different DBs are completely independent from each other. They
%% could represent something like different tenants.
-spec open_db(db(), create_db_opts()) -> ok.
open_db(DB, Opts = #{backend := builtin}) ->
    emqx_ds_replication_layer:open_db(DB, Opts).

%% @doc TODO: currently if one or a few shards are down, they won't be
%% deleted.
-spec drop_db(db()) -> ok.
drop_db(DB) ->
    emqx_ds_replication_layer:drop_db(DB).

-spec store_batch(db(), [emqx_types:message()], message_store_opts()) -> store_batch_result().
store_batch(DB, Msgs, Opts) ->
    emqx_ds_replication_layer:store_batch(DB, Msgs, Opts).

-spec store_batch(db(), [emqx_types:message()]) -> store_batch_result().
store_batch(DB, Msgs) ->
    store_batch(DB, Msgs, #{}).

%% @doc Get a list of streams needed for replaying a topic filter.
%%
%% Motivation: under the hood, EMQX may store different topics at
%% different locations or even in different databases. A wildcard
%% topic filter may require pulling data from any number of locations.
%%
%% Stream is an abstraction exposed by `emqx_ds' that, on one hand,
%% reflects the notion that different topics can be stored
%% differently, but hides the implementation details.
%%
%% While having to work with multiple iterators to replay a topic
%% filter may be cumbersome, it opens up some possibilities:
%%
%% 1. It's possible to parallelize replays
%%
%% 2. Streams can be shared between different clients to implement
%% shared subscriptions
%%
%% IMPORTANT RULES:
%%
%% 0. There is no 1-to-1 mapping between MQTT topics and streams. One
%% stream can contain any number of MQTT topics.
%%
%% 1. New streams matching the topic filter and start time can appear
%% without notice, so the replayer must periodically call this
%% function to get the updated list of streams.
%%
%% 2. Streams may depend on one another. Therefore, care should be
%% taken while replaying them in parallel to avoid out-of-order
%% replay. This function returns stream together with its
%% "coordinate": `stream_rank()'.
%%
%% Stream rank is a tuple of two integers, let's call them X and Y. If
%% X coordinate of two streams is different, they are independent and
%% can be replayed in parallel. If it's the same, then the stream with
%% smaller Y coordinate should be replayed first. If Y coordinates are
%% equal, then the streams are independent.
%%
%% Stream is fully consumed when `next/3' function returns
%% `end_of_stream'. Then and only then the client can proceed to
%% replaying streams that depend on the given one.
-spec get_streams(db(), topic_filter(), time()) -> [{stream_rank(), stream()}].
get_streams(DB, TopicFilter, StartTime) ->
    emqx_ds_replication_layer:get_streams(DB, TopicFilter, StartTime).

-spec make_iterator(stream(), topic_filter(), time()) -> make_iterator_result().
make_iterator(Stream, TopicFilter, StartTime) ->
    emqx_ds_replication_layer:make_iterator(Stream, TopicFilter, StartTime).

-spec next(iterator(), pos_integer()) -> next_result().
next(Iter, BatchSize) ->
    emqx_ds_replication_layer:next(Iter, BatchSize).

-spec save_iterator(db(), iterator_id(), iterator()) -> ok.
save_iterator(DB, ITRef, Iterator) ->
    emqx_ds_replication_layer:save_iterator(DB, ITRef, Iterator).

-spec get_iterator(db(), iterator_id()) -> get_iterator_result(iterator()).
get_iterator(DB, ITRef) ->
    emqx_ds_replication_layer:get_iterator(DB, ITRef).

%%================================================================================
%% Internal exports
%%================================================================================

%%================================================================================
%% Internal functions
%%================================================================================
