%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mq_message_db).

-moduledoc "Facade for all operations with the message database".

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
    rotate_quota_indexes/1,
    truncate_quota_indexes/2,
    calc_constant_bounds/2,
    delete_before/2,
    release_before/3,
    occupancy/2
]).
-export_type([bounds/0]).

%% For each slab, the DS timestamp before which purged messages can be deleted.
-type bounds() :: #{emqx_ds:slab() => emqx_ds:time()}.

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
            {Time, Result} = timer:tc(fun() ->
                admit_and_insert(MQHandle, Shard, MessageBin, TxOpts, TxFun)
            end),
            case Result of
                {error, queue_full} ->
                    {error, queue_full};
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
    {Time, Result} = timer:tc(fun() ->
        admit_and_insert(MQHandle, Shard, MessageBin, TxOpts, TxFun)
    end),
    case Result of
        {error, queue_full} ->
            {error, queue_full};
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
Rotate the quota index of a limited queue in every slab, and return the boundaries.

The boundaries map each slab to its boundary.
This function does not delete messages. Pass the boundaries to `delete_before/2`.
""".
-spec rotate_quota_indexes(emqx_mq_types:mq()) -> {ok, bounds()} | {error, term()}.
rotate_quota_indexes(MQ) ->
    map_slabs(MQ, fun rotate_quota_index/2).

-doc """
Truncate the quota index of a limited queue in every slab, and return the boundaries.

In each slab, the index drops the buckets that end at or before BeforeUs.
Every slab's boundary is BeforeUs.
This function does not delete messages. Pass the boundaries to `delete_before/2`.
""".
-spec truncate_quota_indexes(emqx_mq_types:mq(), emqx_ds:time()) ->
    {ok, bounds()} | {error, term()}.
truncate_quota_indexes(MQ, BeforeUs) ->
    map_slabs(MQ, fun(TxOpts, M) -> truncate_quota_index(TxOpts, M, BeforeUs) end).

-doc "Return the same boundary for every slab of the queue's DB. Unlimited queues have no index to limit it".
-spec calc_constant_bounds(emqx_mq_types:mq(), emqx_ds:time()) -> {ok, bounds()} | {error, term()}.
calc_constant_bounds(MQ, BeforeUs) ->
    case emqx_ds:list_slabs(db(MQ), #{}) of
        {Slabs, []} -> {ok, maps:map(fun(_Slab, _Info) -> BeforeUs end, Slabs)};
        {_Slabs, Errors} -> {error, Errors}
    end.

-doc "Delete the queue messages stored before the given per-slab bounds".
-spec delete_before(emqx_mq_types:mq(), bounds()) -> ok | {error, term()}.
delete_before(MQ, Bounds) ->
    emqx_mq_ds_queue_deleter:delete(db(MQ), [mq_message_topic(MQ, '#')], Bounds).

-spec delete_all() -> ok.
delete_all() ->
    ok = delete(?MQ_MESSAGE_LASTVALUE_DB, [['#']]),
    ok = delete(?MQ_MESSAGE_REGULAR_DB, [['#']]).

-spec occupancy(emqx_mq_types:mq_handle(), emqx_ds:shard()) ->
    {ok, #{bytes := integer(), count := integer(), buckets := non_neg_integer()}}
    | {error, term()}.
occupancy(MQ, Shard) ->
    try dirty_index(MQ, Shard) of
        undefined -> {ok, #{bytes => 0, count => 0, buckets => 0}};
        Index -> {ok, emqx_mq_quota_index:occupancy(Index)}
    catch
        Class:Reason -> {error, {quota_index_read_failed, Class, Reason}}
    end.

-doc """
Release the quota of the records of a reject queue shard stored before `Boundary`.
Every such record must be consumed or expired.

1. Close a full index head. When the stored fence is already seen, write a fresh fence,
   then close the head at it in a second transaction.
2. Pick the newest index boundary at or before `Boundary`.
   Pick the next boundary instead when no record is stored between `Boundary` and it.
3. Delete the records stored before the picked boundary.
4. Drop the index buckets before the picked boundary.

Quota is released only in step 4, after the records are deleted.
Return the released boundary, or 0 when no bucket retired.
""".
-spec release_before(emqx_mq_types:mq(), emqx_ds:shard(), emqx_ds:time()) ->
    {ok, emqx_ds:time()} | {error, term()}.
release_before(MQ, Shard, Boundary) ->
    DB = db(MQ),
    Slab = {Shard, insert_generation(DB)},
    TxOpts = index_tx_opts(DB, Slab),
    case refresh_quota_index(TxOpts, MQ) of
        {ok, undefined} ->
            {ok, 0};
        {ok, Index} ->
            case release_boundary(MQ, Slab, Index, Boundary) of
                0 -> {ok, 0};
                Released -> release(MQ, Slab, TxOpts, Released)
            end;
        {error, _} = Error ->
            Error
    end.

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

-doc """
Compute this queue's retention cutoff for one last-value slab.

The cutoff is the latest of the retention start, the delivery cutoff,
and, for limited queues, the quota index deadline.
Messages before the retention start or the delivery cutoff are never delivered.
""".
-spec gc_info(emqx_mq_types:mq(), emqx_ds:slab()) ->
    skip | #{topic := emqx_ds:topic_filter(), cutoff := emqx_ds:time()}.
gc_info(MQ, {Shard, _}) ->
    Index = limited_index(MQ, Shard),
    DeleteTill = lists:max([
        emqx_mq_prop:retention_start_time(MQ),
        emqx_mq_prop:deliver_cutoff_time(MQ),
        index_deadline(Index)
    ]),
    Topic = mq_message_topic(MQ, '#'),
    %% Dirty GC assumes this past cutoff precedes leader-assigned monotonic insert timestamps.
    case DeleteTill of
        0 ->
            skip;
        _ ->
            ok = maybe_report_expired(MQ, Shard, Index, DeleteTill),
            #{topic => Topic, cutoff => DeleteTill}
    end.

%% Expired records of reject queues keep their quota until the cleanup pool releases it.
%% The pool releases whole buckets, so report only when the cutoff reaches the end of the oldest one.
maybe_report_expired(MQ, Shard, Index, DeleteTill) when Index =/= undefined ->
    case emqx_mq_prop:on_full(MQ) of
        reject ->
            case emqx_mq_quota_index:next_boundary(Index) of
                Next when is_integer(Next), Next =< DeleteTill ->
                    _ = emqx_mq_cleanup_worker:report_expired(MQ, Shard, DeleteTill),
                    ok;
                _ ->
                    ok
            end;
        evict ->
            ok
    end;
maybe_report_expired(_MQ, _Shard, undefined, _DeleteTill) ->
    ok.

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

%% The quota index of a limited queue in the shard, or `undefined`.
limited_index(MQ, Shard) ->
    case emqx_mq_prop:is_limited(MQ) of
        true -> dirty_index(MQ, Shard);
        false -> undefined
    end.

index_deadline(undefined) -> 0;
index_deadline(Index) -> emqx_mq_quota_index:deadline(Index).

dirty_index(MQHandle, Shard) ->
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
%% Admission and cleanup
%%--------------------------------------------------------------------

admit_and_insert(MQ, Shard, MessageBin, TxOpts, TxFun) ->
    case admit(MQ, Shard, byte_size(MessageBin)) of
        ok ->
            emqx_ds:trans(TxOpts, TxFun);
        {error, queue_full} = Error ->
            emqx_mq_metrics:inc_mq(MQ, queue_full),
            Error;
        {error, Reason} ->
            {error, recoverable, Reason}
    end.

admit(MQ, Shard, Size) ->
    case emqx_mq_prop:on_full(MQ) of
        evict ->
            ok;
        reject ->
            #{limits := #{max_shard_message_count := MaxCount, max_shard_message_bytes := MaxBytes}} =
                MQ,
            case ?MODULE:occupancy(MQ, Shard) of
                %% Cleanup never releases the head bucket. Admit writes into a single bucket,
                %% so it fills, closes at a fence, and its consumed records can be released.
                {ok, #{buckets := 1}} ->
                    ok;
                {ok, #{count := Count, bytes := Bytes}} ->
                    case max(0, Count) + 1 > MaxCount orelse max(0, Bytes) + Size > MaxBytes of
                        true -> {error, queue_full};
                        false -> ok
                    end;
                {error, _} = Error ->
                    Error
            end
    end.

%% A full head closes at a stored fence on the next quota flush. A full reject queue gets no
%% flushes, so close the head here. This does not change the quota.
refresh_quota_index(TxOpts, MQ) ->
    maybe
        {ok, {_, true}} ?= refresh_quota_index_tx(TxOpts, MQ),
        ?tp(mq_cleanup_fence_written, #{id => emqx_mq_prop:id(MQ)}),
        {ok, {Index1, _}} ?= refresh_quota_index_tx(TxOpts, MQ),
        {ok, Index1}
    else
        {ok, {Index2, false}} -> {ok, Index2};
        {ok, undefined} -> {ok, undefined};
        {error, _} = Error -> Error
    end.

refresh_quota_index_tx(TxOpts, MQ) ->
    case
        emqx_ds:trans(TxOpts, fun() ->
            emqx_mq_quota_index_tx:refresh_tx(
                mq_index_topic(MQ), mq_fence_topic(MQ), emqx_mq_prop:quota_index_opts(MQ)
            )
        end)
    of
        {atomic, _Serial, Result} -> {ok, Result};
        {nop, Result} -> {ok, Result};
        {error, IsRecoverable, Reason} -> {error, {IsRecoverable, Reason}}
    end.

release_boundary(MQ, Slab, Index, Boundary) ->
    {Passed, Ahead} = lists:splitwith(
        fun(B) -> B =< Boundary end, emqx_mq_quota_index:boundaries(Index)
    ),
    Newest =
        case Passed of
            [] -> 0;
            _ -> lists:last(Passed)
        end,
    case Ahead of
        [Next | _] ->
            case has_records(MQ, Slab, Boundary, Next) of
                false -> Next;
                true -> Newest
            end;
        [] ->
            Newest
    end.

%% Writes after a stored fence get later timestamps, so no record can appear before `To`.
%% Stop at the first record found.
has_records(MQ, {Shard, Generation}, From, To) ->
    Opts = #{
        db => db(MQ),
        shard => Shard,
        generation => Generation,
        start_time => From,
        end_time => To,
        batch_size => 1
    },
    try
        emqx_ds:fold_topic(
            fun(_Slab, _Stream, _TTV, _Acc) -> throw(record_found) end,
            false,
            mq_message_topic(MQ, '#'),
            Opts
        )
    catch
        throw:record_found -> true
    end.

release(MQ, Slab, TxOpts, Released) ->
    ?tp(mq_cleanup_delete, #{id => emqx_mq_prop:id(MQ), slab => Slab, boundary => Released}),
    maybe
        ok ?= delete_before(MQ, #{Slab => Released}),
        {ok, Released} ?= truncate_quota_index(TxOpts, MQ, Released),
        ?tp(mq_cleanup_released, #{id => emqx_mq_prop:id(MQ), slab => Slab, boundary => Released}),
        {ok, Released}
    end.

%%--------------------------------------------------------------------
%% Purge
%%--------------------------------------------------------------------

%% Run SlabFun in every slab of the queue's DB. Collect its results by slab, or the errors of all slabs.
map_slabs(MQ, SlabFun) ->
    DB = db(MQ),
    case emqx_ds:list_slabs(DB, #{}) of
        {Slabs, []} ->
            Results = [{Slab, SlabFun(index_tx_opts(DB, Slab), MQ)} || Slab <- maps:keys(Slabs)],
            case [{Slab, Reason} || {Slab, {error, Reason}} <- Results] of
                [] -> {ok, maps:from_list([{Slab, Value} || {Slab, {ok, Value}} <- Results])};
                Errors -> {error, Errors}
            end;
        {_Slabs, Errors} ->
            {error, Errors}
    end.

%% Index transactions conflict with quota index flushes, so retry like a flush.
index_tx_opts(DB, {Shard, Generation}) ->
    #{
        db => DB,
        shard => Shard,
        generation => Generation,
        sync => true,
        retries => ?MQ_MESSAGE_DB_INDEX_APPEND_RETRY,
        retry_interval => retry_interval(DB)
    }.

rotate_quota_index(#{shard := Shard} = TxOpts, MQ) ->
    IndexTopic = mq_index_topic(MQ),
    FenceTopic = mq_fence_topic(MQ),
    Opts = emqx_mq_prop:quota_index_opts(MQ),
    maybe
        {ok, FenceSerial, ok} ?=
            run_transaction(TxOpts, fun() -> emqx_mq_quota_index_tx:write_fence_tx(FenceTopic) end),
        ?tp(mq_message_db_purge_fence_written, #{shard => Shard, fence_serial => FenceSerial}),
        ?tp(mq_message_db_purge_rotate, #{shard => Shard, fence_serial => FenceSerial}),
        {ok, _, {ok, #{ts_us := BoundaryUs}}} ?=
            run_transaction(TxOpts, fun() ->
                emqx_mq_quota_index_tx:rotate_tx(IndexTopic, FenceTopic, Opts)
            end),
        ?tp(mq_message_db_purge_rotated, #{mq => MQ, boundary_us => BoundaryUs}),
        {ok, BoundaryUs}
    else
        {ok, _, {error, no_fence} = Error} -> Error;
        {error, _} = Error -> Error
    end.

%% Drop the buckets that end at or before BeforeUs. Return BeforeUs as the slab's boundary.
truncate_quota_index(TxOpts, MQ, BeforeUs) ->
    IndexTopic = mq_index_topic(MQ),
    Opts = emqx_mq_prop:quota_index_opts(MQ),
    case
        emqx_ds:trans(TxOpts, fun() ->
            emqx_mq_quota_index_tx:truncate_tx(IndexTopic, Opts, BeforeUs)
        end)
    of
        {atomic, _Serial, ok} ->
            ?tp(mq_message_db_purge_truncated, #{mq => MQ, boundary_us => BeforeUs}),
            {ok, BeforeUs};
        {error, IsRecoverable, Reason} ->
            {error, {IsRecoverable, Reason}}
    end.

run_transaction(TxOpts, Fun) ->
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
    [mq_message_topic(MQHandle, '#')];
delete_topics(?MQ_MESSAGE_LASTVALUE_DB, MQHandle) ->
    [mq_index_topic(MQHandle), mq_fence_topic(MQHandle), mq_message_topic(MQHandle, '#')].
