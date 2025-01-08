%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-include("emqx_ds.hrl").

-behaviour(emqx_ds_storage_layer).

%% API:
-export([]).

%% behavior callbacks:
-export([
    create/6,
    open/5,
    drop/5,
    prepare_batch/4,
    commit_batch/4,
    get_streams/4,
    get_delete_streams/4,
    make_iterator/5,
    make_delete_iterator/5,
    update_iterator/4,
    next/6,
    delete_next/7,
    lookup_message/3,

    unpack_iterator/3,
    scan_stream/8,
    message_matcher/3,
    batch_events/3
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
    cf :: rocksdb:cf_handle(),
    topic_hash_bytes :: non_neg_integer()
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

create(_ShardId, DBHandle, GenId, _Options, _SPrev, DBOpts) ->
    CFName = data_cf(GenId),
    {ok, CFHandle} = rocksdb:create_column_family(DBHandle, CFName, []),
    case DBOpts of
        #{append_only := true} -> THB = 0;
        _NonAppendOnly -> THB = 8
    end,
    Schema = #{
        topic_hash_bytes => THB
    },
    {Schema, [{CFName, CFHandle}]}.

open({DB, _}, DBHandle, GenId, CFRefs, Schema) ->
    {_, CF} = lists:keyfind(data_cf(GenId), 1, CFRefs),
    emqx_ds_new_streams:notify_new_stream(DB, ['#']),
    #s{db = DBHandle, cf = CF, topic_hash_bytes = schema_topic_hash_bytes(Schema)}.

schema_topic_hash_bytes(#{topic_hash_bytes := THB}) ->
    THB;
schema_topic_hash_bytes(#schema{}) ->
    0.

drop(_ShardId, DBHandle, _GenId, _CFRefs, #s{cf = CFHandle}) ->
    ok = rocksdb:drop_column_family(DBHandle, CFHandle),
    ok.

prepare_batch(_ShardId, _Data, Batch, _Options) ->
    {ok, Batch}.

commit_batch(_ShardId, S = #s{db = DB}, Batch, Options) ->
    {ok, BatchHandle} = rocksdb:batch(),
    lists:foreach(fun(Op) -> process_batch_operation(S, Op, BatchHandle) end, Batch),
    Res = rocksdb:write_batch(DB, BatchHandle, write_batch_opts(Options)),
    rocksdb:release_batch(BatchHandle),
    Res.

process_batch_operation(S, {TS, Msg = #message{topic = Topic}}, BatchHandle) ->
    Val = encode_message(Msg),
    rocksdb:batch_put(BatchHandle, S#s.cf, db_key(S, TS, Topic), Val);
process_batch_operation(S, {delete, #message_matcher{timestamp = TS, topic = Topic}}, BatchHandle) ->
    rocksdb:batch_delete(BatchHandle, S#s.cf, db_key(S, TS, Topic)).

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

next(_Shard, #s{db = DB, cf = CF}, It0, BatchSize, _Now, IsCurrent) ->
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
    case Messages of
        [] when not IsCurrent ->
            {ok, end_of_stream};
        _ ->
            {ok, It, lists:reverse(Messages)}
    end.

delete_next(_Shard, #s{db = DB, cf = CF}, It0, Selector, BatchSize, _Now, IsCurrent) ->
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
    case IsCurrent of
        false when NumDeleted =:= 0, NumIterated =:= 0 ->
            {ok, end_of_stream};
        _ ->
            {ok, It, NumDeleted, NumIterated}
    end.

lookup_message(_ShardId, S = #s{db = DB, cf = CF}, #message_matcher{timestamp = TS, topic = Topic}) ->
    case rocksdb:get(DB, CF, db_key(S, TS, Topic), _ReadOpts = []) of
        {ok, Val} ->
            decode_message(Val);
        not_found ->
            not_found;
        {error, Reason} ->
            {error, unrecoverable, Reason}
    end.

unpack_iterator(_Shard, _S, #it{topic_filter = TopicFilter, last_seen_message_key = LSK}) ->
    Stream = #stream{},
    case LSK of
        first -> Timestamp = 0;
        <<Timestamp:64>> -> ok
    end,
    {Stream, TopicFilter, LSK, Timestamp}.

scan_stream(Shard, S, _Stream, TopicFilter, LastSeenKey, BatchSize, TMax, IsCurrent) ->
    It0 = #it{topic_filter = TopicFilter, start_time = 0, last_seen_message_key = LastSeenKey},
    case next(Shard, S, It0, BatchSize, TMax, IsCurrent) of
        {ok, #it{last_seen_message_key = LSK}, Batch} ->
            {ok, LSK, Batch};
        Other ->
            Other
    end.

message_matcher(_Shard, _S, #it{
    start_time = StartTime, topic_filter = TF, last_seen_message_key = LSK
}) ->
    fun(MsgKey = <<TS:64>>, #message{topic = Topic}) ->
        MsgKey > LSK andalso TS >= StartTime andalso emqx_topic:match(Topic, TF)
    end.

batch_events(_Shard, _, Messages) ->
    Topics = lists:foldl(
        fun
            ({_TS, #message{topic = Topic}}, Acc) ->
                Acc#{Topic => 1};
            ({delete, _Msg}, Acc) ->
                Acc
        end,
        #{},
        Messages
    ),
    [{#stream{}, T} || T <- maps:keys(Topics)].

%%================================================================================
%% Internal functions
%%================================================================================

do_next(_, _, _, _, 0, Key, Acc) ->
    {Key, Acc};
do_next(TopicFilter, StartTime, IT, Action, NLeft, Key0, Acc) ->
    case rocksdb:iterator_move(IT, Action) of
        {ok, Key = <<TS:64, _TopicHashOpt/bytes>>, Blob} ->
            Msg = #message{topic = Topic} = decode_message(Blob),
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
            Msg = #message{topic = Topic, timestamp = TS} = decode_message(Blob),
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

db_key(#s{topic_hash_bytes = 0}, TS, _Topic) ->
    <<TS:64>>;
db_key(#s{topic_hash_bytes = THB}, TS, Topic) ->
    <<Hash:THB/bytes, _/bytes>> = erlang:md5(Topic),
    <<TS:64, Hash/bytes>>.

encode_message(Msg) ->
    term_to_binary(Msg).

decode_message(Val) ->
    binary_to_term(Val).

%% @doc Generate a column family ID for the MQTT messages
-spec data_cf(emqx_ds_storage_layer:gen_id()) -> [char()].
data_cf(GenId) ->
    "emqx_ds_storage_reference" ++ integer_to_list(GenId).

-spec write_batch_opts(emqx_ds_storage_layer:batch_store_opts()) ->
    _RocksDBOpts :: [{atom(), _}].
write_batch_opts(#{durable := false}) ->
    [{disable_wal, true}];
write_batch_opts(#{}) ->
    [].
