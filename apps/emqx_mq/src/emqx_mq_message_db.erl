%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mq_message_db).

-moduledoc """
Facade for all operations with the message database.
""".

-behaviour(emqx_mq_quota_buffer).

-include_lib("emqx_durable_storage/include/emqx_ds.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("emqx/include/logger.hrl").
-include("emqx_mq_internal.hrl").

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
    purge/1,
    purge/2
]).

-export([
    encode_message/1,
    decode_message/1
]).

-export([
    add_regular_db_generation/0,
    gc_info/2,
    regular_db_slab_info/0,
    drop_regular_db_slab/1,
    initial_generations/1
]).

%% QuotaBuffer CBM callbacks
-export([
    quota_buffer_max_size/0,
    quota_buffer_flush_interval/0,
    quota_buffer_notify_queue_size/2,
    quota_buffer_flush_transaction/2,
    quota_buffer_flush_tx_update/5
]).

%% For testing/maintenance
-export([
    delete_all/0,
    dirty_read_all/1
]).

-define(MQ_MESSAGE_DB_APPEND_RETRY, 3).
-define(MQ_MESSAGE_DB_INDEX_APPEND_RETRY, 3).
%% How many times a full purge writes a new fence when a flush made the previous one unusable.
-define(MQ_MESSAGE_DB_PURGE_FENCE_ATTEMPTS, 3).
-define(MQ_MESSAGE_DB_LTS_SETTINGS, #{
    %% "topic/MQ_TOPIC/MQ_ID/key/СOMPACTION_KEY"
    lts_threshold_spec => {simple, {100, 0, 0, 100, 0}}
}).
-define(MQ_MESSAGE_DB_TOPIC(MQ_TOPIC, MQ_ID, KEY), [
    <<"topic">>, MQ_TOPIC, MQ_ID, <<"key">>, KEY
]).
-define(MQ_INDEX_TOPIC(MQ_TOPIC, MQ_ID), [
    <<"topic">>, MQ_TOPIC, MQ_ID, <<"index">>
]).
-define(MQ_FENCE_TOPIC(MQ_TOPIC, MQ_ID), [
    <<"topic">>, MQ_TOPIC, MQ_ID, <<"fence">>
]).
-define(MQ_DIRTY_APPEND_TIMEOUT, 5000).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec open() -> ok.
open() ->
    Config = maps:merge(emqx_ds_schema:db_config_mq_messages(), #{
        storage => {emqx_ds_storage_skipstream_lts_v2, ?MQ_MESSAGE_DB_LTS_SETTINGS}
    }),
    ok = open_db(?MQ_MESSAGE_LASTVALUE_DB, Config),
    ok = open_db(?MQ_MESSAGE_REGULAR_DB, Config).

open_db(DB, Config) ->
    case emqx_ds:open_db(DB, Config) of
        ok ->
            ok;
        {error, Reason} ->
            error({failed_to_open_mq_database, DB, Reason})
    end.

-spec close() -> ok | {error, term()}.
close() ->
    LastValueErrors =
        case emqx_ds:close_db(?MQ_MESSAGE_LASTVALUE_DB) of
            ok ->
                [];
            LVError ->
                [{?MQ_MESSAGE_LASTVALUE_DB, LVError}]
        end,
    RegularErrors =
        case emqx_ds:close_db(?MQ_MESSAGE_REGULAR_DB) of
            ok ->
                [];
            RegularError ->
                [{?MQ_MESSAGE_REGULAR_DB, RegularError}]
        end,
    case LastValueErrors ++ RegularErrors of
        [] ->
            ok;
        Errors ->
            {error, Errors}
    end.

-spec wait_readiness(timeout()) -> ok | timeout.
wait_readiness(Timeout) ->
    maybe
        ok ?= emqx_ds:wait_db(?MQ_MESSAGE_LASTVALUE_DB, all, Timeout),
        ok ?= emqx_ds:wait_db(?MQ_MESSAGE_REGULAR_DB, all, Timeout)
    end.

-spec insert(emqx_mq_types:mq_handle(), emqx_types:message()) ->
    ok | {error, term()}.
insert(MQHandle, Message) ->
    insert(MQHandle, emqx_mq_prop:is_limited(MQHandle), Message).

insert(#{is_lastvalue := true} = MQHandle, IsLimited, Message) ->
    DB = db(MQHandle),
    case key(MQHandle, Message) of
        {error, _} = Error ->
            Error;
        {ok, Key} ->
            Shard = emqx_ds:shard_of(DB, Key),
            Topic = mq_message_topic(MQHandle, Key),
            MessageBin = encode_message(Message),
            Gen = insert_generation(DB),
            TxOpts = #{
                db => DB,
                shard => Shard,
                generation => Gen,
                sync => true,
                retries => ?MQ_MESSAGE_DB_APPEND_RETRY,
                retry_interval => retry_interval(DB)
            },
            ?tp(debug, mq_message_db_insert, #{
                mq => MQHandle,
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
                {atomic, Serial, MaybeOldMessage} ->
                    emqx_mq_metrics:observe_hist_mq(MQHandle, insert_latency_ms, us_to_ms(Time)),
                    IsLimited andalso
                        update_quota(
                            MQHandle,
                            {Shard, Gen},
                            old_message_quota_info(MaybeOldMessage),
                            #{
                                message => Message,
                                message_size => byte_size(MessageBin),
                                serial => Serial
                            }
                        ),
                    ok;
                {error, IsRecoverable, Reason} ->
                    emqx_mq_metrics:inc_mq(MQHandle, insert_errors),
                    {error, {IsRecoverable, Reason}}
            end
    end;
insert(#{is_lastvalue := false} = MQHandle, true = _IsLimited, Message) ->
    DB = db(MQHandle),
    ClientId = emqx_message:from(Message),
    Shard = emqx_ds:shard_of(DB, ClientId),
    Topic = mq_message_topic(MQHandle, ClientId),
    MessageBin = encode_message(Message),
    Gen = insert_generation(DB),
    TxOpts = #{
        db => DB,
        shard => Shard,
        generation => Gen,
        sync => true,
        retries => ?MQ_MESSAGE_DB_APPEND_RETRY,
        retry_interval => retry_interval(DB)
    },
    ?tp(debug, mq_message_db_insert, #{
        mq => MQHandle,
        topic => Topic,
        tx_opts => TxOpts
    }),
    TxFun = fun() ->
        emqx_ds:tx_write({Topic, ?ds_tx_ts_monotonic, MessageBin})
    end,
    {Time, Result} = timer:tc(fun() -> emqx_ds:trans(TxOpts, TxFun) end),
    case Result of
        {atomic, Serial, ok} ->
            emqx_mq_metrics:observe_hist_mq(MQHandle, insert_latency_ms, us_to_ms(Time)),
            ok = update_quota(
                MQHandle,
                {Shard, Gen},
                undefined,
                #{message => Message, message_size => byte_size(MessageBin), serial => Serial}
            );
        {error, IsRecoverable, Reason} ->
            emqx_mq_metrics:inc_mq(MQHandle, insert_errors),
            {error, {IsRecoverable, Reason}}
    end;
insert(#{is_lastvalue := false} = MQHandle, false = _IsLimited, Message) ->
    DB = db(MQHandle),
    ClientId = emqx_message:from(Message),
    Shard = emqx_ds:shard_of(DB, ClientId),
    Topic = mq_message_topic(MQHandle, ClientId),
    MessageBin = encode_message(Message),
    NeedReply = need_reply(Message),
    DirtyOpts = #{
        db => DB,
        shard => Shard,
        reply => NeedReply
    },
    ?tp(debug, mq_message_db_insert, #{
        mq => MQHandle,
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
                            emqx_mq_metrics:observe_hist_mq(MQHandle, insert_latency_ms, ElapsedMs),
                            ok;
                        {error, IsRecoverable, Reason} ->
                            emqx_mq_metrics:inc_mq(MQHandle, insert_errors),
                            {error, {IsRecoverable, Reason}}
                    end
            after ?MQ_DIRTY_APPEND_TIMEOUT ->
                {error, dirty_append_timeout}
            end
    end.

-spec drop(emqx_mq_types:mq_handle() | emqx_mq_types:mq()) -> ok | {error, term()}.
drop(MQHandle) ->
    DB = db(MQHandle),
    delete(DB, delete_topics(DB, MQHandle)).

-doc """
Delete all messages of the queue that were stored before the purge. Keep the queue.

In each slab:
1. Write a fresh fence. DS gives it the serial S and the timestamp T of its transaction.
2. Limited queues: rotate the quota index at the fence.
   The index starts a new bucket at (S, T) and drops all older buckets.
   If a flush made the fence unusable, go back to step 1.
3. Delete the messages stored before T.

Messages stored after the fence survive, and the index counts them.
Unlimited queues have no index. They use the fence only to get T, and delete it after step 3.

Return the delivery cutoff: the smallest T of all slabs, in milliseconds.
Every message stored before it is deleted.
""".
-spec purge(emqx_mq_types:mq()) ->
    {ok, emqx_utils_calendar:epoch_millisecond()} | {error, term()}.
purge(MQ) ->
    maybe
        {ok, BoundariesUs} ?= purge_slabs(MQ, fun purge_all_slab/2),
        case BoundariesUs of
            [] -> {ok, 0};
            _ -> {ok, us_to_ms(lists:min(BoundariesUs))}
        end
    end.

-doc """
Delete messages stored before BeforeMs, an epoch millisecond, as far as the quota index allows.

Limited queues delete the messages before the newest index bucket boundary at or before BeforeMs.
The index drops the buckets before that boundary first, in its own transaction.
When there is no such boundary, the messages stay.
Unlimited queues have no index, so they delete the messages stored before BeforeMs.
""".
-spec purge(emqx_mq_types:mq(), emqx_utils_calendar:epoch_millisecond()) ->
    ok | {error, term()}.
purge(MQ, BeforeMs) when is_integer(BeforeMs), BeforeMs >= 0 ->
    BeforeUs = erlang:convert_time_unit(BeforeMs, millisecond, microsecond),
    maybe
        {ok, _} ?= purge_slabs(MQ, fun(TxOpts, M) -> purge_slab_before(TxOpts, M, BeforeUs) end),
        ok
    end.

-spec delete_all() -> ok.
delete_all() ->
    ok = delete(?MQ_MESSAGE_LASTVALUE_DB, [['#']]),
    ok = delete(?MQ_MESSAGE_REGULAR_DB, [['#']]).

-spec create_client(module()) -> emqx_ds_client:t().
create_client(Module) ->
    emqx_ds_client:new(Module, #{}).

-spec subscribe(
    emqx_mq_types:mq(),
    emqx_ds_client:t(),
    emqx_ds_client:sub_id(),
    State
) ->
    {ok, emqx_ds_client:t(), State}.
subscribe(#{stream_max_unacked := StreamMaxUnacked} = MQ, DSClient0, SubId, State0) ->
    SubOpts = #{
        db => db(MQ),
        id => SubId,
        topic => mq_message_topic(MQ, '#'),
        start_time => emqx_mq_prop:delivery_start_time(MQ),
        ds_sub_opts => #{
            max_unacked => StreamMaxUnacked
        }
    },
    {ok, DSClient, State} = emqx_ds_client:subscribe(DSClient0, SubOpts, State0),
    {ok, DSClient, State}.

-spec suback(
    emqx_mq_types:mq() | emqx_ds:db(), emqx_ds:subscription_handle(), emqx_ds:sub_seqno()
) -> ok.
suback(MQ, SubHandle, SeqNo) when is_map(MQ) ->
    emqx_ds:suback(db(MQ), SubHandle, SeqNo);
suback(DB, SubHandle, SeqNo) when is_atom(DB) ->
    emqx_ds:suback(DB, SubHandle, SeqNo).

-spec encode_message(emqx_types:message()) -> binary().
encode_message(Message) ->
    emqx_ds_msg_serializer:serialize(asn1, Message).

-spec decode_message(binary()) -> emqx_types:message().
decode_message(Bin) ->
    emqx_ds_msg_serializer:deserialize(asn1, Bin).

-spec add_regular_db_generation() -> ok.
add_regular_db_generation() ->
    ok = emqx_ds:add_generation(?MQ_MESSAGE_REGULAR_DB).

-doc "Compute this queue's retention cutoff for one last-value slab".
-spec gc_info(emqx_mq_types:mq(), emqx_ds:slab()) ->
    skip | #{topic := emqx_ds:topic_filter(), cutoff := emqx_ds:time()}.
gc_info(#{data_retention_period := DataRetentionPeriod} = MQ, {Shard, _}) ->
    Index = dirty_index(MQ, Shard),
    DataRetentionPeriodUs = erlang:convert_time_unit(DataRetentionPeriod, millisecond, microsecond),
    TimeRetentionDeadline = max(now_us() - DataRetentionPeriodUs, 0),
    LimitsDeadline =
        case Index of
            undefined ->
                0;
            _ ->
                emqx_mq_quota_index:deadline(Index)
        end,
    DeleteTill = max(TimeRetentionDeadline, LimitsDeadline),
    Topic = mq_message_topic(MQ, '#'),
    %% Dirty GC assumes this past cutoff precedes leader-assigned monotonic insert timestamps.
    case DeleteTill of
        0 -> skip;
        _ -> #{topic => Topic, cutoff => DeleteTill}
    end.

-spec regular_db_slab_info() ->
    {ok, #{emqx_ds:slab() => emqx_ds:slab_info()}}
    | {error, [{emqx_ds:shard(), emqx_ds:error(_)}]}.
regular_db_slab_info() ->
    case emqx_ds:list_slabs(?MQ_MESSAGE_REGULAR_DB, #{}) of
        {SlabInfo, []} ->
            {ok, SlabInfo};
        {_SlabInfo, Errors} ->
            {error, Errors}
    end.

-spec initial_generations(emqx_mq_types:mq() | emqx_mq_types:mq_handle()) ->
    #{emqx_ds:slab() => emqx_ds:generation()}.
initial_generations(MQ) ->
    SlabInfo = emqx_ds:list_slabs(db(MQ)),
    maps:from_list(lists:reverse(lists:sort(maps:keys(SlabInfo)))).

-spec drop_regular_db_slab(emqx_ds:slab()) -> ok.
drop_regular_db_slab(Slab) ->
    ok = emqx_ds:drop_slab(?MQ_MESSAGE_REGULAR_DB, Slab).

-spec dirty_read_all(emqx_mq_types:mq()) -> [emqx_ds:ttv()].
dirty_read_all(MQ) ->
    emqx_ds:dirty_read(db(MQ), mq_message_topic(MQ, '#')).

%%--------------------------------------------------------------------
%% QuotaBuffer CBM callbacks
%%--------------------------------------------------------------------

quota_buffer_max_size() ->
    emqx:get_config([mq, quota, buffer_max_size], ?DEFAULT_QUOTA_BUFFER_MAX_SIZE).

quota_buffer_flush_interval() ->
    emqx:get_config([mq, quota, buffer_flush_interval], ?DEFAULT_QUOTA_BUFFER_FLUSH_INTERVAL).

quota_buffer_notify_queue_size(WorkerId, QueueSize) ->
    emqx_mq_metrics:set_quota_buffer_inbox_size(WorkerId, QueueSize).

quota_buffer_flush_transaction({DB, {Shard, Generation}} = _TxKey, TxFun) ->
    TxOpts = #{
        db => DB,
        shard => Shard,
        generation => Generation,
        sync => true,
        retries => ?MQ_MESSAGE_DB_INDEX_APPEND_RETRY,
        retry_interval => retry_interval(DB)
    },
    {Time, Result} = timer:tc(fun() -> emqx_ds:trans(TxOpts, TxFun) end),
    case Result of
        {atomic, _Serial, ok} ->
            emqx_mq_metrics:observe_hist(flush_quota_index, flush_latency_ms, us_to_ms(Time)),
            ok;
        {error, IsRecoverable, Reason} ->
            emqx_mq_metrics:inc(flush_quota_index, flush_errors),
            ?tp(error, mq_message_db_flush_quota_index_error, #{
                shard => Shard,
                generation => Generation,
                is_recoverable => IsRecoverable,
                reason => Reason
            }),
            ok
    end.

quota_buffer_flush_tx_update(
    _TxKey, {IndexTopic, FenceTopic} = _Key, QuotaIndexOpts, Updates, NeedLegacy
) ->
    emqx_mq_quota_index_tx:update_tx(IndexTopic, FenceTopic, QuotaIndexOpts, Updates, NeedLegacy).

%%--------------------------------------------------------------------
%% Quota index utilities
%%--------------------------------------------------------------------

dirty_index(MQHandle, Shard) ->
    dirty_index(MQHandle, Shard, emqx_mq_prop:is_limited(MQHandle)).

dirty_index(_MQHandle, _Shard, false = _IsLimited) ->
    undefined;
dirty_index(MQHandle, Shard, true = _IsLimited) ->
    DB = db(MQHandle),
    case
        emqx_ds:dirty_read(
            #{db => DB, shard => Shard, generation => insert_generation(DB)},
            mq_index_topic(MQHandle)
        )
    of
        [] ->
            undefined;
        [{_, ?QUOTA_INDEX_TS, IndexBin}] ->
            Opts = emqx_mq_prop:quota_index_opts(MQHandle),
            emqx_mq_quota_index:decode(Opts, IndexBin)
    end.

old_message_quota_info([]) ->
    undefined;
old_message_quota_info([{_, DsTsUs, OldMessageBin}]) ->
    OldMessage = decode_message(OldMessageBin),
    #{message => OldMessage, message_size => byte_size(OldMessageBin), ds_ts_us => DsTsUs}.

update_quota(MQHandle, Slab, OldMessageInfo, NewMessageInfo) ->
    emqx_mq_quota_buffer:add(
        ?MQ_QUOTA_BUFFER,
        {db(MQHandle), Slab},
        {mq_index_topic(MQHandle), mq_fence_topic(MQHandle)},
        emqx_mq_prop:quota_index_opts(MQHandle),
        OldMessageInfo,
        NewMessageInfo
    ).

mq_index_topic(#{topic_filter := TopicFilter, id := Id} = _MQ) ->
    ?MQ_INDEX_TOPIC(TopicFilter, Id).

mq_fence_topic(#{topic_filter := TopicFilter, id := Id} = _MQ) ->
    ?MQ_FENCE_TOPIC(TopicFilter, Id).

%%--------------------------------------------------------------------
%% Purge
%%--------------------------------------------------------------------

%% Run SlabFun in every slab of the queue's DB. Collect its results, or the errors of all slabs.
purge_slabs(MQ, SlabFun) ->
    DB = db(MQ),
    case emqx_ds:list_slabs(DB, #{}) of
        {Slabs, []} ->
            Results = [{Slab, SlabFun(purge_tx_opts(DB, Slab), MQ)} || Slab <- maps:keys(Slabs)],
            case [{Slab, Reason} || {Slab, {error, Reason}} <- Results] of
                [] -> {ok, [Value || {_Slab, {ok, Value}} <- Results]};
                Errors -> {error, Errors}
            end;
        {_Slabs, Errors} ->
            {error, Errors}
    end.

%% Purge transactions conflict with quota index flushes, so retry like a flush.
purge_tx_opts(DB, {Shard, Generation}) ->
    #{
        db => DB,
        shard => Shard,
        generation => Generation,
        sync => true,
        retries => ?MQ_MESSAGE_DB_INDEX_APPEND_RETRY,
        retry_interval => retry_interval(DB)
    }.

%% Delete all messages stored before a fresh fence. Return the fence timestamp.
purge_all_slab(TxOpts, MQ) ->
    case emqx_mq_prop:is_limited(MQ) of
        true -> rotate_and_delete(TxOpts, MQ, ?MQ_MESSAGE_DB_PURGE_FENCE_ATTEMPTS);
        false -> fence_and_delete(TxOpts, MQ)
    end.

rotate_and_delete(#{shard := Shard} = TxOpts, MQ, Attempts) ->
    IndexTopic = mq_index_topic(MQ),
    FenceTopic = mq_fence_topic(MQ),
    Opts = emqx_mq_prop:quota_index_opts(MQ),
    maybe
        {ok, FenceSerial, ok} ?=
            purge_tx(TxOpts, fun() -> emqx_mq_quota_index_tx:write_fence_tx(FenceTopic) end),
        ?tp(mq_message_db_purge_fence_written, #{shard => Shard, fence_serial => FenceSerial}),
        ?tp(mq_message_db_purge_rotate, #{shard => Shard, fence_serial => FenceSerial}),
        {ok, _, Rotated} ?=
            purge_tx(TxOpts, fun() ->
                emqx_mq_quota_index_tx:rotate_tx(IndexTopic, FenceTopic, Opts, FenceSerial)
            end),
        case Rotated of
            {ok, #{ts_us := BoundaryUs}} ->
                ?tp(mq_message_db_purge_rotated, #{mq => MQ, boundary_us => BoundaryUs}),
                delete_messages_before(TxOpts, MQ, BoundaryUs);
            {error, stale_fence} when Attempts > 1 ->
                ?tp(mq_message_db_purge_stale_fence, #{shard => Shard, fence_serial => FenceSerial}),
                rotate_and_delete(TxOpts, MQ, Attempts - 1);
            {error, stale_fence} = Error ->
                Error
        end
    end.

%% A queue without an index uses the fence only to get a DS timestamp.
fence_and_delete(TxOpts, MQ) ->
    FenceTopic = mq_fence_topic(MQ),
    maybe
        {ok, _, ok} ?=
            purge_tx(TxOpts, fun() -> emqx_mq_quota_index_tx:write_fence_tx(FenceTopic) end),
        {ok, _, BoundaryUs} ?=
            purge_tx(TxOpts, fun() ->
                #{ts_us := TsUs} = emqx_mq_quota_index_tx:read_fence_tx(FenceTopic),
                emqx_ds:tx_del_topic(FenceTopic),
                emqx_ds:tx_del_topic(mq_message_topic(MQ, '#'), 0, TsUs),
                TsUs
            end),
        {ok, BoundaryUs}
    end.

%% Delete messages stored before BeforeUs, as far as the index allows.
purge_slab_before(TxOpts, MQ, BeforeUs) ->
    case emqx_mq_prop:is_limited(MQ) of
        true ->
            IndexTopic = mq_index_topic(MQ),
            Opts = emqx_mq_prop:quota_index_opts(MQ),
            maybe
                {ok, _, CutoffUs} ?=
                    purge_tx(TxOpts, fun() ->
                        emqx_mq_quota_index_tx:cutoff_tx(IndexTopic, Opts, BeforeUs)
                    end),
                ?tp(mq_message_db_purge_cut, #{mq => MQ, cutoff_us => CutoffUs}),
                delete_messages_before(TxOpts, MQ, CutoffUs)
            end;
        false ->
            delete_messages_before(TxOpts, MQ, BeforeUs)
    end.

delete_messages_before(_TxOpts, _MQ, 0) ->
    {ok, 0};
delete_messages_before(TxOpts, MQ, TsUs) ->
    maybe
        {ok, _, _} ?=
            purge_tx(TxOpts, fun() -> emqx_ds:tx_del_topic(mq_message_topic(MQ, '#'), 0, TsUs) end),
        {ok, TsUs}
    end.

purge_tx(TxOpts, Fun) ->
    case emqx_ds:trans(TxOpts, Fun) of
        {atomic, SerialBin, Result} -> {ok, binary:decode_unsigned(SerialBin), Result};
        {error, IsRecoverable, Reason} -> {error, {IsRecoverable, Reason}}
    end.

%%--------------------------------------------------------------------
%% Other helper functions
%%--------------------------------------------------------------------

key(#{is_lastvalue := true, key_expression := KeyExpression} = MQ, Message) ->
    Bindings = #{message => message_to_map(Message)},
    case emqx_variform:render(KeyExpression, Bindings, #{eval_as_string => true}) of
        {error, Reason} ->
            ?SLOG_THROTTLE(
                warning,
                #{
                    msg => mq_message_db_key_expression_error,
                    mq => MQ,
                    reason => Reason,
                    key_expression => emqx_variform:decompile(KeyExpression),
                    bindings => Bindings
                },
                #{tag => "MQ"}
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
    emqx_mq_ds_queue_deleter:delete(DB, Topics).

mq_message_topic(#{topic_filter := TopicFilter, id := Id} = _MQ, Key) ->
    ?MQ_MESSAGE_DB_TOPIC(TopicFilter, Id, Key).

db(MQ) ->
    case emqx_mq_prop:is_append_only(MQ) of
        true ->
            ?MQ_MESSAGE_REGULAR_DB;
        false ->
            ?MQ_MESSAGE_LASTVALUE_DB
    end.

insert_generation(?MQ_MESSAGE_LASTVALUE_DB) ->
    1.

%% Both message DBs use the mq_messages durable storage settings.
retry_interval(_DB) ->
    emqx:get_config([durable_storage, mq_messages, transaction, flush_interval], 100) * 2.

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

delete_topics(?MQ_MESSAGE_REGULAR_DB, MQHandle) ->
    %% A purge writes a fence here even for unlimited queues.
    [mq_fence_topic(MQHandle), mq_message_topic(MQHandle, '#')];
delete_topics(?MQ_MESSAGE_LASTVALUE_DB, MQHandle) ->
    [mq_index_topic(MQHandle), mq_fence_topic(MQHandle), mq_message_topic(MQHandle, '#')].

now_us() ->
    erlang:system_time(microsecond).
