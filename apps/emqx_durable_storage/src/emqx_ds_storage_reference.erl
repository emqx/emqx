%%--------------------------------------------------------------------
%% Copyright (c) 2023-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
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
    prepare_batch/4,
    commit_batch/3,
    get_streams/4,
    get_delete_streams/4,
    make_iterator/5,
    make_delete_iterator/5,
    update_iterator/4,
    next/5,
    delete_next/6
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

-record(delete_stream, {}).

-record(it, {
    topic_filter :: emqx_ds:topic_filter(),
    start_time :: emqx_ds:time(),
    last_seen_message_key = first :: binary() | first
}).

-record(delete_it, {
    topic_filter :: emqx_ds:topic_filter(),
    start_time :: emqx_ds:time(),
    last_seen_message_key = first :: binary() | first
}).

%%================================================================================
%% API functions
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

prepare_batch(_ShardId, _Data, Messages, _Options) ->
    {ok, Messages}.

commit_batch(_ShardId, #s{db = DB, cf = CF}, Messages) ->
    {ok, Batch} = rocksdb:batch(),
    lists:foreach(
        fun({TS, Msg}) ->
            Key = <<TS:64>>,
            Val = term_to_binary(Msg),
            rocksdb:batch_put(Batch, CF, Key, Val)
        end,
        Messages
    ),
    Res = rocksdb:write_batch(DB, Batch, _WriteOptions = []),
    rocksdb:release_batch(Batch),
    Res.

get_streams(_Shard, _Data, _TopicFilter, _StartTime) ->
    [#stream{}].

get_delete_streams(_Shard, _Data, _TopicFilter, _StartTime) ->
    [#delete_stream{}].

make_iterator(_Shard, _Data, #stream{}, TopicFilter, StartTime) ->
    {ok, #it{
        topic_filter = TopicFilter,
        start_time = StartTime
    }}.

make_delete_iterator(_Shard, _Data, #delete_stream{}, TopicFilter, StartTime) ->
    {ok, #delete_it{
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

next(_Shard, #s{db = DB, cf = CF}, It0, BatchSize, _Now) ->
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

delete_next(_Shard, #s{db = DB, cf = CF}, It0, Selector, BatchSize, _Now) ->
    #delete_it{
        topic_filter = TopicFilter,
        start_time = StartTime,
        last_seen_message_key = Key0
    } = It0,
    {ok, ITHandle} = rocksdb:iterator(DB, CF, []),
    Action =
        case Key0 of
            first ->
                first;
            _ ->
                _ = rocksdb:iterator_move(ITHandle, Key0),
                next
        end,
    {Key, {NumDeleted, NumIterated}} = do_delete_next(
        TopicFilter,
        StartTime,
        DB,
        CF,
        ITHandle,
        Action,
        Selector,
        BatchSize,
        Key0,
        {0, 0}
    ),
    rocksdb:iterator_close(ITHandle),
    It = It0#delete_it{last_seen_message_key = Key},
    {ok, It, NumDeleted, NumIterated}.

%%================================================================================
%% Internal functions
%%================================================================================

do_next(_, _, _, _, 0, Key, Acc) ->
    {Key, Acc};
do_next(TopicFilter, StartTime, IT, Action, NLeft, Key0, Acc) ->
    case rocksdb:iterator_move(IT, Action) of
        {ok, Key = <<TS:64>>, Blob} ->
            Msg = #message{topic = Topic} = binary_to_term(Blob),
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

%% TODO: use a context map...
do_delete_next(_, _, _, _, _, _, _, 0, Key, Acc) ->
    {Key, Acc};
do_delete_next(
    TopicFilter, StartTime, DB, CF, IT, Action, Selector, NLeft, Key0, {AccDel, AccIter}
) ->
    case rocksdb:iterator_move(IT, Action) of
        {ok, Key, Blob} ->
            Msg = #message{topic = Topic, timestamp = TS} = binary_to_term(Blob),
            TopicWords = emqx_topic:words(Topic),
            case emqx_topic:match(TopicWords, TopicFilter) andalso TS >= StartTime of
                true ->
                    case Selector(Msg) of
                        true ->
                            ok = rocksdb:delete(DB, CF, Key, _WriteOpts = []),
                            do_delete_next(
                                TopicFilter,
                                StartTime,
                                DB,
                                CF,
                                IT,
                                next,
                                Selector,
                                NLeft - 1,
                                Key,
                                {AccDel + 1, AccIter + 1}
                            );
                        false ->
                            do_delete_next(
                                TopicFilter,
                                StartTime,
                                DB,
                                CF,
                                IT,
                                next,
                                Selector,
                                NLeft - 1,
                                Key,
                                {AccDel, AccIter + 1}
                            )
                    end;
                false ->
                    do_delete_next(
                        TopicFilter,
                        StartTime,
                        DB,
                        CF,
                        IT,
                        next,
                        Selector,
                        NLeft,
                        Key,
                        {AccDel, AccIter + 1}
                    )
            end;
        {error, invalid_iterator} ->
            {Key0, {AccDel, AccIter}}
    end.

%% @doc Generate a column family ID for the MQTT messages
-spec data_cf(emqx_ds_storage_layer:gen_id()) -> [char()].
data_cf(GenId) ->
    "emqx_ds_storage_reference" ++ integer_to_list(GenId).
