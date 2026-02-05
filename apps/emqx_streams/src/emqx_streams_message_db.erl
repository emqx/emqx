%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_streams_message_db).

-moduledoc """
Facade for all operations with the message database.
""".

-behaviour(emqx_mq_quota_buffer).

-include_lib("emqx_durable_storage/include/emqx_ds.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("emqx/include/logger.hrl").
-include("emqx_streams_internal.hrl").

-export([
    open/0,
    close/0,
    wait_readiness/1
]).

-export([
    insert/2,
    suback/3,
    create_client/1,
    subscribe/4,
    drop/1,
    partitions/1,
    find_generations/2,
    find_generation/3,
    slab_of_dsstream/2
]).

-export([
    encode_message/1,
    decode_message/1
]).

-export([
    add_regular_db_generation/0,
    delete_expired_data/1,
    regular_db_slab_info/0,
    drop_regular_db_slab/1,
    initial_generations/1
]).

-export([
    quota_buffer_max_size/0,
    quota_buffer_flush_interval/0,
    quota_buffer_notify_queue_size/2,
    quota_buffer_flush_transaction/2,
    quota_buffer_flush_tx_read/3,
    quota_buffer_flush_tx_write/3
]).

%% For testing/maintenance
-export([
    delete_all/0,
    dirty_read_all/1,
    dirty_index/2
]).

-define(STREAMS_MESSAGE_DB_APPEND_RETRY, 3).
-define(STREAMS_MESSAGE_DB_QUOTA_INDEX_APPEND_RETRY, 3).
-define(STREAMS_MESSAGE_DB_DELETE_RETRY, 1).
-define(STREAMS_MESSAGE_DB_DELETE_RETRY_DELAY, 1000).
-define(STREAMS_INDEX_TOPIC(STREAM_TOPIC, STREAM_ID), [
    <<"topic">>, STREAM_TOPIC, STREAM_ID, <<"index">>
]).
-define(STREAMS_DIRTY_APPEND_TIMEOUT, 5000).
-define(STREAMS_MESSAGE_LASTVALUE_DB_INSERT_GENERATION, 1).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec open() -> ok.
open() ->
    Config = maps:merge(emqx_ds_schema:db_config_streams_messages(), #{
        storage => {emqx_ds_storage_skipstream_lts_v2, ?STREAMS_MESSAGE_DB_LTS_SETTINGS}
    }),
    maybe
        ok ?= emqx_ds:open_db(?STREAMS_MESSAGE_LASTVALUE_DB, Config),
        ok ?= emqx_ds:open_db(?STREAMS_MESSAGE_REGULAR_DB, Config)
    else
        _ -> error(failed_to_open_streams_databases)
    end.

-spec close() -> ok.
close() ->
    ok = emqx_ds:close_db(?STREAMS_MESSAGE_LASTVALUE_DB),
    ok = emqx_ds:close_db(?STREAMS_MESSAGE_REGULAR_DB).

-spec wait_readiness(timeout()) -> ok | timeout.
wait_readiness(Timeout) ->
    maybe
        ok ?= emqx_ds:wait_db(?STREAMS_MESSAGE_LASTVALUE_DB, all, Timeout),
        ok ?= emqx_ds:wait_db(?STREAMS_MESSAGE_REGULAR_DB, all, Timeout)
    end.

-spec insert(emqx_streams_types:stream(), emqx_types:message()) ->
    ok | {error, term()}.
insert(Stream, Message) ->
    case key(Stream, Message) of
        {error, _} = Error ->
            Error;
        {ok, Key} ->
            insert(Stream, emqx_streams_prop:is_limited(Stream), Key, Message)
    end.

insert(#{is_lastvalue := true, id := Id} = Stream, IsLimited, Key, Message) ->
    DB = ?STREAMS_MESSAGE_LASTVALUE_DB,
    Shard = emqx_ds:shard_of(DB, Id),
    Topic = stream_message_topic(Stream, Key),
    MessageBin = encode_message(Message),
    Gen = ?STREAMS_MESSAGE_LASTVALUE_DB_INSERT_GENERATION,
    TxOpts = #{
        db => DB,
        shard => Shard,
        generation => Gen,
        sync => true,
        retries => ?STREAMS_MESSAGE_DB_APPEND_RETRY,
        retry_interval => retry_interval(DB)
    },
    ?tp(debug, streams_message_db_insert, #{
        stream => Stream,
        topic => Topic,
        key => Key,
        shard => Shard,
        tx_opts => TxOpts
    }),
    TxFun = fun() ->
        MaybeOldMessage =
            case IsLimited of
                true ->
                    emqx_ds:tx_read(Topic);
                false ->
                    []
            end,
        emqx_ds:tx_del_topic(Topic),
        emqx_ds:tx_write({Topic, ?ds_tx_ts_monotonic, MessageBin}),
        MaybeOldMessage
    end,
    {Time, Result} = timer:tc(fun() -> emqx_ds:trans(TxOpts, TxFun) end),
    case Result of
        {atomic, _Serial, MaybeOldMessage} ->
            emqx_streams_metrics:observe_hist_stream(
                Stream, insert_latency_ms, us_to_ms(Time)
            ),
            emqx_streams_metrics:inc_stream(Stream, insert_ok),
            IsLimited andalso
                update_quota(
                    Stream,
                    {Shard, Gen},
                    old_message_quota_info(MaybeOldMessage),
                    #{message => Message, message_size => byte_size(MessageBin)}
                ),
            ok;
        {error, IsRecoverable, Reason} ->
            emqx_streams_metrics:inc_stream(Stream, insert_errors),
            {error, {IsRecoverable, Reason}}
    end;
insert(#{is_lastvalue := false, id := Id} = Stream, true = _IsLimited, Key, Message) ->
    DB = ?STREAMS_MESSAGE_LASTVALUE_DB,
    Shard = emqx_ds:shard_of(DB, Id),
    Topic = stream_message_topic(Stream, Key),
    MessageBin = encode_message(Message),
    Gen = ?STREAMS_MESSAGE_LASTVALUE_DB_INSERT_GENERATION,
    TxOpts = #{
        db => DB,
        shard => Shard,
        generation => Gen,
        sync => true,
        retries => ?STREAMS_MESSAGE_DB_APPEND_RETRY,
        retry_interval => retry_interval(DB)
    },
    ?tp(debug, streams_message_db_insert, #{
        stream => Stream,
        topic => Topic,
        tx_opts => TxOpts
    }),
    TxFun = fun() ->
        emqx_ds:tx_write({Topic, ?ds_tx_ts_monotonic, MessageBin})
    end,
    {Time, Result} = timer:tc(fun() -> emqx_ds:trans(TxOpts, TxFun) end),
    case Result of
        {atomic, _Serial, ok} ->
            emqx_streams_metrics:observe_hist_stream(
                Stream, insert_latency_ms, us_to_ms(Time)
            ),
            emqx_streams_metrics:inc_stream(Stream, insert_ok),
            ok = update_quota(
                Stream,
                {Shard, Gen},
                undefined,
                #{message => Message, message_size => byte_size(MessageBin)}
            );
        {error, IsRecoverable, Reason} ->
            emqx_streams_metrics:inc_stream(Stream, insert_errors),
            {error, {IsRecoverable, Reason}}
    end;
insert(#{is_lastvalue := false, id := Id} = Stream, false = _IsLimited, Key, Message) ->
    DB = ?STREAMS_MESSAGE_REGULAR_DB,
    Shard = emqx_ds:shard_of(DB, Id),
    Topic = stream_message_topic(Stream, Key),
    MessageBin = encode_message(Message),
    NeedReply = need_reply(Message),
    DirtyOpts = #{
        db => DB,
        shard => Shard,
        reply => NeedReply
    },
    ?tp(debug, streams_message_db_insert, #{
        stream => Stream,
        topic => Topic,
        dirty_opts => DirtyOpts
    }),
    TimeStartMs = erlang:monotonic_time(millisecond),
    case emqx_ds:dirty_append(DirtyOpts, [{Topic, ?ds_tx_ts_monotonic, MessageBin}]) of
        noreply ->
            ok;
        {error, IsRecoverable, Reason} ->
            {error, {IsRecoverable, Reason}};
        Ref when is_reference(Ref) ->
            receive
                ?ds_tx_commit_reply(Ref, Reply) ->
                    case emqx_ds:dirty_append_outcome(Ref, Reply) of
                        {ok, _Serial} ->
                            ElapsedMs = erlang:monotonic_time(millisecond) - TimeStartMs,
                            emqx_streams_metrics:observe_hist_stream(
                                Stream, insert_latency_ms, ElapsedMs
                            ),
                            emqx_streams_metrics:inc_stream(Stream, insert_ok),
                            ok;
                        {error, IsRecoverable, Reason} ->
                            emqx_streams_metrics:inc_stream(Stream, insert_errors),
                            {error, {IsRecoverable, Reason}}
                    end
            after ?STREAMS_DIRTY_APPEND_TIMEOUT ->
                {error, dirty_append_timeout}
            end
    end.

-spec drop(emqx_streams_types:stream()) -> ok | {error, term()}.
drop(Stream) ->
    DB = db(Stream),
    delete(DB, delete_topics(DB, Stream)).

-spec delete_all() -> ok.
delete_all() ->
    ok = delete(?STREAMS_MESSAGE_LASTVALUE_DB, [['#']]),
    ok = delete(?STREAMS_MESSAGE_REGULAR_DB, [['#']]).

-spec create_client(module()) -> emqx_ds_client:t().
create_client(Module) ->
    emqx_ds_client:new(Module, #{}).

-spec subscribe(
    emqx_streams_types:stream(),
    emqx_ds_client:t(),
    emqx_ds_client:sub_id(),
    State
) ->
    {ok, emqx_ds_client:t(), State}.
subscribe(Stream, DSClient0, SubId, State0) ->
    StreamMaxUnacked = emqx_streams_prop:max_unacked(Stream),
    SubOpts = #{
        db => db(Stream),
        id => SubId,
        topic => stream_message_topic(Stream, '#'),
        ds_sub_opts => #{
            max_unacked => StreamMaxUnacked
        }
    },
    ?tp_debug(streams_message_db_subscribe, #{
        stream => Stream,
        sub_id => SubId,
        sub_opts => SubOpts
    }),
    {ok, DSClient, State} = emqx_ds_client:subscribe(DSClient0, SubOpts, State0),
    {ok, DSClient, State}.

-spec find_generation(emqx_streams_types:stream(), emqx_ds:shard(), emqx_ds:time()) ->
    {ok, emqx_ds:generation()} | {error, term()}.
find_generation(Stream, Shard, TimestampUs) ->
    case emqx_ds:list_slabs(db(Stream), #{shard => Shard}) of
        {SlabInfo, []} ->
            TimestampMs = us_to_ms(TimestampUs),
            {ok, do_find_generation(TimestampMs, lists:sort(maps:to_list(SlabInfo)))};
        {_, Errors} ->
            {error, {cannot_list_slabs, Errors}}
    end.

find_generations(Stream, TimestampUs) ->
    Shards = emqx_ds:list_shards(db(Stream)),
    try
        Generations = lists:map(
            fun(Shard) ->
                case find_generation(Stream, Shard, TimestampUs) of
                    {ok, Generation} ->
                        {Shard, Generation};
                    {error, Reason} ->
                        throw(Reason)
                end
            end,
            Shards
        ),
        {ok, maps:from_list(Generations)}
    catch
        throw:Reason ->
            {error, Reason}
    end.

do_find_generation(_TimestampMs, [{{_Shard, Generation} = _Slab, _SlabInfo}]) ->
    Generation;
do_find_generation(TimestampMs, [{{_Shard, Generation} = _Slab, SlabInfo} | Rest]) ->
    case SlabInfo of
        #{until := Until} when TimestampMs =< Until orelse Until =:= undefined ->
            Generation;
        _ ->
            do_find_generation(TimestampMs, Rest)
    end.

-spec suback(
    emqx_streams_types:stream() | emqx_ds:db(), emqx_ds:subscription_handle(), emqx_ds:sub_seqno()
) -> ok.
suback(Stream, SubHandle, SeqNo) when is_map(Stream) ->
    ?tp_debug(streams_message_db_suback, #{
        stream => Stream,
        sub_handle => SubHandle,
        seqno => SeqNo
    }),
    emqx_ds:suback(db(Stream), SubHandle, SeqNo).

-spec encode_message(emqx_types:message()) -> binary().
encode_message(Message) ->
    emqx_ds_msg_serializer:serialize(asn1, Message).

-spec decode_message(binary()) -> emqx_types:message().
decode_message(Bin) ->
    emqx_ds_msg_serializer:deserialize(asn1, Bin).

-spec add_regular_db_generation() -> ok.
add_regular_db_generation() ->
    ok = emqx_ds:add_generation(?STREAMS_MESSAGE_REGULAR_DB).

-spec delete_expired_data([emqx_streams_types:stream()]) -> ok.
delete_expired_data(Streams) ->
    Shards = emqx_ds:list_shards(?STREAMS_MESSAGE_LASTVALUE_DB),
    Refs = lists:filtermap(
        fun(Shard) ->
            TxOpts = #{
                db => ?STREAMS_MESSAGE_LASTVALUE_DB,
                shard => Shard,
                generation => ?STREAMS_MESSAGE_LASTVALUE_DB_INSERT_GENERATION,
                sync => false
            },
            Indices = [dirty_index(Stream, Shard) || Stream <- Streams],
            Res = emqx_ds:trans(TxOpts, fun() ->
                lists:foreach(
                    fun({Stream, Index}) -> tx_stream_delete_expired_data(Stream, Index) end,
                    lists:zip(Streams, Indices)
                )
            end),
            case Res of
                {async, Ref, _} ->
                    {true, Ref};
                {nop, ok} ->
                    false;
                {error, IsRecoverable, Reason} ->
                    ?tp(error, streams_message_db_delete_expired_error, #{
                        is_recoverable => IsRecoverable,
                        reason => Reason,
                        shard => Shard
                    }),
                    false
            end
        end,
        Shards
    ),
    lists:foreach(
        fun(Ref) ->
            receive
                ?ds_tx_commit_reply(Ref, Reply) ->
                    case emqx_ds:tx_commit_outcome(?STREAMS_MESSAGE_LASTVALUE_DB, Ref, Reply) of
                        {ok, _} ->
                            ok;
                        {error, IsRecoverable, Reason} ->
                            ?tp(error, streams_message_db_delete_expired_error, #{
                                streams => Streams,
                                is_recoverable => IsRecoverable,
                                reason => Reason
                            })
                    end
            end
        end,
        Refs
    ).

tx_stream_delete_expired_data(Stream, Index) ->
    DataRetentionPeriod = emqx_streams_prop:data_retention_period(Stream),
    TimeRetentionDeadline = max(now_ms() - DataRetentionPeriod, 0),
    LimitsDeadline =
        case Index of
            undefined ->
                0;
            _ ->
                emqx_mq_quota_index:deadline(Index)
        end,
    DeleteTill = max(TimeRetentionDeadline, LimitsDeadline),
    Topic = stream_message_topic(Stream, '#'),
    emqx_ds:tx_del_topic(Topic, 0, DeleteTill).

-spec regular_db_slab_info() -> #{emqx_ds:slab() => emqx_ds:slab_info()}.
regular_db_slab_info() ->
    emqx_ds:list_slabs(?STREAMS_MESSAGE_REGULAR_DB).

-spec initial_generations(emqx_streams_types:stream()) ->
    #{emqx_ds:slab() => emqx_ds:generation()}.
initial_generations(Stream) ->
    SlabInfo = emqx_ds:list_slabs(db(Stream)),
    maps:from_list(lists:reverse(lists:sort(maps:keys(SlabInfo)))).

-spec drop_regular_db_slab(emqx_ds:slab()) -> ok.
drop_regular_db_slab(Slab) ->
    ok = emqx_ds:drop_slab(?STREAMS_MESSAGE_REGULAR_DB, Slab).

-spec dirty_read_all(emqx_streams_types:stream()) ->
    [emqx_ds:ttv()].
dirty_read_all(Stream) ->
    emqx_ds:dirty_read(db(Stream), stream_message_topic(Stream, '#')).

-spec partitions(emqx_streams_types:stream()) ->
    [emqx_streams_types:partition()].
partitions(Stream) ->
    emqx_ds:list_shards(db(Stream)).

-spec slab_of_dsstream(emqx_streams_types:stream(), emqx_ds:stream()) ->
    {ok, emqx_ds:slab()} | emqx_ds:error(_).
slab_of_dsstream(Stream, DSStream) ->
    emqx_ds:slab_of_stream(db(Stream), DSStream).

%%--------------------------------------------------------------------
%% QuotaBuffer CBM callbacks
%%--------------------------------------------------------------------

quota_buffer_max_size() ->
    emqx_streams_config:quota_option(buffer_max_size).

quota_buffer_flush_interval() ->
    emqx_streams_config:quota_option(buffer_flush_interval).

quota_buffer_notify_queue_size(WorkerId, QueueSize) ->
    emqx_streams_metrics:set_quota_buffer_inbox_size(WorkerId, QueueSize).

quota_buffer_flush_transaction({DB, {Shard, Generation}} = _TxKey, TxFun) ->
    TxOpts = #{
        db => DB,
        shard => Shard,
        generation => Generation,
        sync => true,
        retries => ?STREAMS_MESSAGE_DB_QUOTA_INDEX_APPEND_RETRY,
        retry_interval => retry_interval(DB)
    },
    {Time, Result} = timer:tc(fun() -> emqx_ds:trans(TxOpts, TxFun) end),
    case Result of
        {atomic, _Serial, ok} ->
            emqx_streams_metrics:observe_hist(flush_quota_index, flush_latency_ms, us_to_ms(Time)),
            ok;
        {error, IsRecoverable, Reason} ->
            emqx_streams_metrics:inc(flush_quota_index, flush_errors),
            ?tp(error, mq_message_db_flush_quota_index_error, #{
                shard => Shard,
                generation => Generation,
                is_recoverable => IsRecoverable,
                reason => Reason
            }),
            ok
    end.

quota_buffer_flush_tx_read(_TxKey, IndexTopic = _Key, QuotaIndexOpts) ->
    case emqx_ds:tx_read(IndexTopic) of
        [] ->
            undefined;
        [{_, ?QUOTA_INDEX_TS, IndexBin}] ->
            emqx_mq_quota_index:decode(QuotaIndexOpts, IndexBin)
    end.

quota_buffer_flush_tx_write(_TxKey, IndexTopic = _Key, Index) ->
    IndexBin = emqx_mq_quota_index:encode(Index),
    emqx_ds:tx_write({IndexTopic, ?QUOTA_INDEX_TS, IndexBin}).

%%--------------------------------------------------------------------
%% Quota index utilities
%%--------------------------------------------------------------------

-spec dirty_index(
    emqx_streams_types:stream(), emqx_ds:shard()
) ->
    emqx_mq_quota_index:t() | undefined.
dirty_index(Stream, Shard) ->
    dirty_index(Stream, Shard, emqx_streams_prop:is_limited(Stream)).

dirty_index(_StreanHandle, _Shard, false = _IsLimited) ->
    undefined;
dirty_index(Stream, Shard, true = _IsLimited) ->
    DB = ?STREAMS_MESSAGE_LASTVALUE_DB,
    case
        emqx_ds:dirty_read(
            #{
                db => DB,
                shard => Shard,
                generation => ?STREAMS_MESSAGE_LASTVALUE_DB_INSERT_GENERATION
            },
            stream_index_topic(Stream)
        )
    of
        [] ->
            undefined;
        [{_, ?QUOTA_INDEX_TS, IndexBin}] ->
            Opts = emqx_streams_prop:quota_index_opts(Stream),
            emqx_mq_quota_index:decode(Opts, IndexBin)
    end.

old_message_quota_info([]) ->
    undefined;
old_message_quota_info([{_, _, OldMessageBin}]) ->
    OldMessage = decode_message(OldMessageBin),
    #{message => OldMessage, message_size => byte_size(OldMessageBin)}.

update_quota(Stream, Slab, OldMessageInfo, NewMessageInfo) ->
    emqx_mq_quota_buffer:add(
        ?STREAMS_QUOTA_BUFFER,
        {db(Stream), Slab},
        stream_index_topic(Stream),
        emqx_streams_prop:quota_index_opts(Stream),
        OldMessageInfo,
        NewMessageInfo
    ).

stream_index_topic(#{topic_filter := TopicFilter, id := Id} = _Stream) ->
    ?STREAMS_INDEX_TOPIC(TopicFilter, Id).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

key(#{key_expression := KeyExpression} = Stream, Message) ->
    Bindings = #{message => message_to_map(Message)},
    case emqx_variform:render(KeyExpression, Bindings, #{eval_as_string => true}) of
        {error, Reason} ->
            ?SLOG_THROTTLE(
                warning,
                #{
                    msg => streams_message_db_key_expression_error,
                    stream => Stream,
                    reason => Reason,
                    key_expression => emqx_variform:decompile(KeyExpression),
                    bindings => Bindings
                },
                #{tag => "STREAM"}
            ),
            {error, Reason};
        {ok, Key} ->
            {ok, Key}
    end.

need_reply(Message) ->
    case emqx_message:qos(Message) of
        ?QOS_0 -> false;
        _ -> true
    end.

delete(DB, Topics) ->
    {_Time, Errors} = timer:tc(fun() ->
        %% NOTE
        %% This is a temporary workaround for the behavior of the ds tx manager.
        %% Simultaneous asycnhronous txs to the same shard having a new generation
        %% may result to a conflict.
        %% So we delete in groups with all different shards.
        lists:flatmap(
            fun(Slabs) ->
                do_delete(DB, Topics, ?STREAMS_MESSAGE_DB_DELETE_RETRY + 1, Slabs, [])
            end,
            slabs_by_generation(DB)
        )
    end),
    ?tp_debug(streams_message_db_delete, #{
        topic => Topics,
        time => us_to_ms(_Time),
        errors => Errors
    }),
    case Errors of
        [] ->
            ok;
        _ ->
            {error, Errors}
    end.

slabs_by_generation(DB) ->
    ByGeneration = lists:foldl(
        fun({_Shard, Generation} = Slab, Acc) ->
            maps:update_with(Generation, fun(Slabs) -> [Slab | Slabs] end, [Slab], Acc)
        end,
        #{},
        maps:keys(emqx_ds:list_slabs(DB))
    ),
    maps:values(ByGeneration).

do_delete(_DB, _Topics, 0, _Slabs, Errors) ->
    Errors;
do_delete(DB, Topics, Retries, Slabs0, _Errors0) ->
    Refs = lists:map(
        fun({Shard, Generation} = Slab) ->
            TxOpts = #{
                db => DB,
                shard => Shard,
                generation => Generation,
                sync => false
            },
            {async, Ref, _} = emqx_ds:trans(TxOpts, fun() ->
                lists:foreach(
                    fun(Topic) ->
                        emqx_ds:tx_del_topic(Topic)
                    end,
                    Topics
                )
            end),
            {Ref, Slab}
        end,
        Slabs0
    ),
    Errors = lists:filtermap(
        fun({Ref, Slab}) ->
            receive
                ?ds_tx_commit_reply(Ref, Reply) ->
                    case emqx_ds:tx_commit_outcome(DB, Ref, Reply) of
                        {ok, _} ->
                            false;
                        {error, IsRecoverable, Reason} ->
                            ?tp(warning, streams_message_db_delete_error, #{
                                topics => Topics,
                                is_recoverable => IsRecoverable,
                                reason => Reason,
                                slab => Slab
                            }),
                            {true, {Slab, {IsRecoverable, Reason}}}
                    end
            end
        end,
        Refs
    ),
    case Errors of
        [] ->
            [];
        _ ->
            {Slabs, _} = lists:unzip(Errors),
            timer:sleep(?STREAMS_MESSAGE_DB_DELETE_RETRY_DELAY),
            do_delete(DB, Topics, Retries - 1, Slabs, Errors)
    end.

stream_message_topic(#{topic_filter := TopicFilter, id := Id} = _Stream, Key) ->
    ?STREAMS_MESSAGE_DB_TOPIC(TopicFilter, Id, Key).

db(Stream) ->
    case emqx_streams_prop:is_append_only(Stream) of
        true ->
            ?STREAMS_MESSAGE_REGULAR_DB;
        false ->
            ?STREAMS_MESSAGE_LASTVALUE_DB
    end.

retry_interval(?STREAMS_MESSAGE_LASTVALUE_DB) ->
    emqx_streams_config:message_db_tx_retry_interval().

message_to_map(Message) ->
    convert([user_property, peername, peerhost], emqx_message:to_map(Message)).

convert(
    [user_property | Rest],
    #{headers := #{properties := #{'User-Property' := UserProperty}} = Headers} = Map
) ->
    convert(Rest, Map#{
        headers => Headers#{properties => #{'User-Property' => maps:from_list(UserProperty)}}
    });
convert([peername | Rest], #{headers := #{peername := {_Host, _Port} = Peername} = Headers} = Map) ->
    convert(Rest, Map#{headers => Headers#{peername => ntoa(Peername)}});
convert([peerhost | Rest], #{headers := #{peerhost := Peerhost} = Headers} = Map) ->
    convert(Rest, Map#{headers => Headers#{peerhost => ntoa(Peerhost)}});
convert(_, Map) ->
    Map.

ntoa(Addr) ->
    list_to_binary(emqx_utils:ntoa(Addr)).

us_to_ms(Us) ->
    erlang:convert_time_unit(Us, microsecond, millisecond).

delete_topics(?STREAMS_MESSAGE_REGULAR_DB, Stream) ->
    [stream_message_topic(Stream, '#')];
delete_topics(?STREAMS_MESSAGE_LASTVALUE_DB, Stream) ->
    [stream_index_topic(Stream), stream_message_topic(Stream, '#')].

now_ms() ->
    erlang:monotonic_time(millisecond).
