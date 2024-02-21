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

%% @doc Reference implementation of the storage.
%%
%% Trivial, extremely slow and inefficient. It also doesn't handle
%% restart of the Erlang node properly, so obviously it's only to be
%% used for testing.
-module(emqx_ds_storage_reference).

-behaviour(emqx_ds_storage_layer).

%% API:
-export([]).

%% behavior callbacks:
-export([
    create/4,
    open/5,
    drop/5,
    store_batch/4,
    get_streams/4,
    make_iterator/5,
    update_iterator/4,
    next/4
]).

%% internal exports:
-export([]).

-export_type([options/0]).

-include_lib("emqx_utils/include/emqx_message.hrl").

%%================================================================================
%% Type declarations
%%================================================================================

-type options() :: #{}.

%% Permanent state:
-record(schema, {}).

%% Runtime state:
-record(s, {
    db :: rocksdb:db_handle(),
    cf :: rocksdb:cf_handle()
}).

-record(stream, {}).

-record(it, {
    topic_filter :: emqx_ds:topic_filter(),
    start_time :: emqx_ds:time(),
    last_seen_message_key = first :: binary() | first
}).

%%================================================================================
%% API funcions
%%================================================================================

%%================================================================================
%% behavior callbacks
%%================================================================================

create(_ShardId, DBHandle, GenId, _Options) ->
    CFName = data_cf(GenId),
    {ok, CFHandle} = rocksdb:create_column_family(DBHandle, CFName, []),
    Schema = #schema{},
    {Schema, [{CFName, CFHandle}]}.

open(_Shard, DBHandle, GenId, CFRefs, #schema{}) ->
    {_, CF} = lists:keyfind(data_cf(GenId), 1, CFRefs),
    #s{db = DBHandle, cf = CF}.

drop(_ShardId, DBHandle, _GenId, _CFRefs, #s{cf = CFHandle}) ->
    ok = rocksdb:drop_column_family(DBHandle, CFHandle),
    ok.

store_batch(_ShardId, #s{db = DB, cf = CF}, Messages, _Options) ->
    lists:foreach(
        fun(Msg) ->
            Id = erlang:unique_integer([monotonic]),
            Key = <<Id:64>>,
            Val = term_to_binary(Msg),
            rocksdb:put(DB, CF, Key, Val, [])
        end,
        Messages
    ).

get_streams(_Shard, _Data, _TopicFilter, _StartTime) ->
    [#stream{}].

make_iterator(_Shard, _Data, #stream{}, TopicFilter, StartTime) ->
    {ok, #it{
        topic_filter = TopicFilter,
        start_time = StartTime
    }}.

update_iterator(_Shard, _Data, OldIter, DSKey) ->
    #it{
        topic_filter = TopicFilter,
        start_time = StartTime
    } = OldIter,
    {ok, #it{
        topic_filter = TopicFilter,
        start_time = StartTime,
        last_seen_message_key = DSKey
    }}.

next(_Shard, #s{db = DB, cf = CF}, It0, BatchSize) ->
    #it{topic_filter = TopicFilter, start_time = StartTime, last_seen_message_key = Key0} = It0,
    {ok, ITHandle} = rocksdb:iterator(DB, CF, []),
    Action =
        case Key0 of
            first ->
                first;
            _ ->
                _ = rocksdb:iterator_move(ITHandle, Key0),
                next
        end,
    {Key, Messages} = do_next(TopicFilter, StartTime, ITHandle, Action, BatchSize, Key0, []),
    rocksdb:iterator_close(ITHandle),
    It = It0#it{last_seen_message_key = Key},
    {ok, It, lists:reverse(Messages)}.

%%================================================================================
%% Internal functions
%%================================================================================

do_next(_, _, _, _, 0, Key, Acc) ->
    {Key, Acc};
do_next(TopicFilter, StartTime, IT, Action, NLeft, Key0, Acc) ->
    case rocksdb:iterator_move(IT, Action) of
        {ok, Key, Blob} ->
            Msg = #message{topic = Topic, timestamp = TS} = binary_to_term(Blob),
            TopicWords = emqx_topic:words(Topic),
            case emqx_topic:match(TopicWords, TopicFilter) andalso TS >= StartTime of
                true ->
                    do_next(TopicFilter, StartTime, IT, next, NLeft - 1, Key, [{Key, Msg} | Acc]);
                false ->
                    do_next(TopicFilter, StartTime, IT, next, NLeft, Key, Acc)
            end;
        {error, invalid_iterator} ->
            {Key0, Acc}
    end.

%% @doc Generate a column family ID for the MQTT messages
-spec data_cf(emqx_ds_storage_layer:gen_id()) -> [char()].
data_cf(GenId) ->
    "emqx_ds_storage_reference" ++ integer_to_list(GenId).
