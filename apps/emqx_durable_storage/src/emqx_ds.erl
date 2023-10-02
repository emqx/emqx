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

-include_lib("stdlib/include/ms_transform.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

%% API:
-export([ensure_shard/2]).
%%   Messages:
-export([message_store/2, message_store/1, message_stats/0]).
%%   Iterator:
-export([iterator_update/2, iterator_next/1, iterator_stats/0]).

%% internal exports:
-export([]).

-export_type([
    keyspace/0,
    message_id/0,
    message_stats/0,
    message_store_opts/0,
    replay/0,
    replay_id/0,
    iterator_id/0,
    iterator/0,
    shard/0,
    shard_id/0,
    topic/0,
    topic_filter/0,
    time/0
]).

%%================================================================================
%% Type declarations
%%================================================================================

-type iterator() :: term().

-type iterator_id() :: binary().

-type message_store_opts() :: #{}.

-type message_stats() :: #{}.

-type message_id() :: binary().

%% Parsed topic.
-type topic() :: list(binary()).

%% Parsed topic filter.
-type topic_filter() :: list(binary() | '+' | '#' | '').

-type keyspace() :: atom().
-type shard_id() :: binary().
-type shard() :: {keyspace(), shard_id()}.

%% Timestamp
%% Earliest possible timestamp is 0.
%% TODO granularity?  Currently, we should always use micro second, as that's the unit we
%% use in emqx_guid.  Otherwise, the iterators won't match the message timestamps.
-type time() :: non_neg_integer().

-type replay_id() :: binary().

-type replay() :: {
    _TopicFilter :: topic_filter(),
    _StartTime :: time()
}.

%%================================================================================
%% API funcions
%%================================================================================

-spec ensure_shard(shard(), emqx_ds_storage_layer:options()) ->
    ok | {error, _Reason}.
ensure_shard(Shard, Options) ->
    case emqx_ds_storage_layer_sup:start_shard(Shard, Options) of
        {ok, _Pid} ->
            ok;
        {error, {already_started, _Pid}} ->
            ok;
        {error, Reason} ->
            {error, Reason}
    end.

%%--------------------------------------------------------------------------------
%% Message
%%--------------------------------------------------------------------------------
-spec message_store([emqx_types:message()], message_store_opts()) ->
    {ok, [message_id()]} | {error, _}.
message_store(_Msg, _Opts) ->
    %% TODO
    {error, not_implemented}.

-spec message_store([emqx_types:message()]) -> {ok, [message_id()]} | {error, _}.
message_store(Msg) ->
    %% TODO
    message_store(Msg, #{}).

-spec message_stats() -> message_stats().
message_stats() ->
    #{}.

%%--------------------------------------------------------------------------------
%% Session
%%--------------------------------------------------------------------------------

%%--------------------------------------------------------------------------------
%% Iterator (pull API)
%%--------------------------------------------------------------------------------

%% @doc Called when a client acks a message
-spec iterator_update(iterator_id(), iterator()) -> ok.
iterator_update(_IterId, _Iter) ->
    %% TODO
    ok.

%% @doc Called when a client acks a message
-spec iterator_next(iterator()) -> {value, emqx_types:message(), iterator()} | none | {error, _}.
iterator_next(_Iter) ->
    %% TODO
    none.

-spec iterator_stats() -> #{}.
iterator_stats() ->
    #{}.

%%================================================================================
%% Internal functions
%%================================================================================
