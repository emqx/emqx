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
    drop/1
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

%% QuotaBuffer CBM callbacks
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
    dirty_read_all/1
]).

-define(MQ_MESSAGE_DB_APPEND_RETRY, 3).
-define(MQ_MESSAGE_DB_INDEX_APPEND_RETRY, 3).
-define(MQ_MESSAGE_DB_DELETE_RETRY, 1).
-define(MQ_MESSAGE_DB_DELETE_RETRY_DELAY, 1000).
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
-define(MQ_DIRTY_APPEND_TIMEOUT, 5000).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec open() -> ok.
open() ->
    Config = maps:merge(emqx_ds_schema:db_config_mq_messages(), #{
        storage => {emqx_ds_storage_skipstream_lts_v2, ?MQ_MESSAGE_DB_LTS_SETTINGS}
    }),
    maybe
        ok ?= emqx_ds:open_db(?MQ_MESSAGE_LASTVALUE_DB, Config),
        ok ?= emqx_ds:open_db(?MQ_MESSAGE_REGULAR_DB, Config)
    else
        _ -> error(failed_to_open_mq_databases)
    end.

-spec close() -> ok.
close() ->
    ok = emqx_ds:close_db(?MQ_MESSAGE_LASTVALUE_DB),
    ok = emqx_ds:close_db(?MQ_MESSAGE_REGULAR_DB).

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
                {atomic, _Serial, MaybeOldMessage} ->
                    emqx_mq_metrics:observe_hist_mq(MQHandle, insert_latency_ms, us_to_ms(Time)),
                    IsLimited andalso
                        update_quota(
                            MQHandle,
                            {Shard, Gen},
                            old_message_quota_info(MaybeOldMessage),
                            #{message => Message, message_size => byte_size(MessageBin)}
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
        {atomic, _Serial, ok} ->
            emqx_mq_metrics:observe_hist_mq(MQHandle, insert_latency_ms, us_to_ms(Time)),
            ok = update_quota(
                MQHandle,
                {Shard, Gen},
                undefined,
                #{message => Message, message_size => byte_size(MessageBin)}
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

-spec delete_expired_data([emqx_mq_types:mq()]) -> ok.
delete_expired_data(MQs) ->
    Shards = emqx_ds:list_shards(?MQ_MESSAGE_LASTVALUE_DB),
    Refs = lists:filtermap(
        fun(Shard) ->
            TxOpts = #{
                db => ?MQ_MESSAGE_LASTVALUE_DB,
                shard => Shard,
                generation => insert_generation(?MQ_MESSAGE_LASTVALUE_DB),
                sync => false
            },
            Indices = [dirty_index(MQ, Shard) || MQ <- MQs],
            Res = emqx_ds:trans(TxOpts, fun() ->
                lists:foreach(
                    fun({MQ, Index}) -> tx_mq_delete_expired_data(MQ, Index) end,
                    lists:zip(MQs, Indices)
                )
            end),
            case Res of
                {async, Ref, _} ->
                    {true, Ref};
                {nop, ok} ->
                    false;
                {error, IsRecoverable, Reason} ->
                    ?tp(error, mq_message_db_delete_expired_error, #{
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
                    case emqx_ds:tx_commit_outcome(?MQ_MESSAGE_LASTVALUE_DB, Ref, Reply) of
                        {ok, _} ->
                            ok;
                        {error, IsRecoverable, Reason} ->
                            ?tp(error, mq_message_db_delete_expired_error, #{
                                mqs => MQs,
                                is_recoverable => IsRecoverable,
                                reason => Reason
                            })
                    end
            end
        end,
        Refs
    ).

tx_mq_delete_expired_data(#{data_retention_period := DataRetentionPeriod} = MQ, Index) ->
    TimeRetentionDeadline = max(now_ms() - DataRetentionPeriod, 0),
    LimitsDeadline =
        case Index of
            undefined ->
                0;
            _ ->
                emqx_mq_quota_index:deadline(Index)
        end,
    DeleteTill = max(TimeRetentionDeadline, LimitsDeadline),
    Topic = mq_message_topic(MQ, '#'),
    emqx_ds:tx_del_topic(Topic, 0, DeleteTill).

-spec regular_db_slab_info() -> #{emqx_ds:slab() => emqx_ds:slab_info()}.
regular_db_slab_info() ->
    emqx_ds:list_slabs(?MQ_MESSAGE_REGULAR_DB).

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
old_message_quota_info([{_, _, OldMessageBin}]) ->
    OldMessage = decode_message(OldMessageBin),
    #{message => OldMessage, message_size => byte_size(OldMessageBin)}.

update_quota(MQHandle, Slab, OldMessageInfo, NewMessageInfo) ->
    emqx_mq_quota_buffer:add(
        ?MQ_QUOTA_BUFFER,
        {db(MQHandle), Slab},
        mq_index_topic(MQHandle),
        emqx_mq_prop:quota_index_opts(MQHandle),
        OldMessageInfo,
        NewMessageInfo
    ).

mq_index_topic(#{topic_filter := TopicFilter, id := Id} = _MQ) ->
    ?MQ_INDEX_TOPIC(TopicFilter, Id).

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
    {_Time, Errors} = timer:tc(fun() ->
        %% NOTE
        %% This is a temporary workaround for the behavior of the ds tx manager.
        %% Simultaneous asycnhronous txs to the same shard having a new generation
        %% may result to a conflict.
        %% So we delete in groups with all different shards.
        lists:flatmap(
            fun(Slabs) ->
                do_delete(DB, Topics, ?MQ_MESSAGE_DB_DELETE_RETRY + 1, Slabs, [])
            end,
            slabs_by_generation(DB)
        )
    end),
    ?tp_debug(mq_message_db_delete, #{
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
                            ?tp(warning, mq_message_db_delete_error, #{
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
            timer:sleep(?MQ_MESSAGE_DB_DELETE_RETRY_DELAY),
            do_delete(DB, Topics, Retries - 1, Slabs, Errors)
    end.

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

retry_interval(?MQ_MESSAGE_LASTVALUE_DB) ->
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
    [mq_index_topic(MQHandle), mq_message_topic(MQHandle, '#')].

now_ms() ->
    erlang:monotonic_time(millisecond).
