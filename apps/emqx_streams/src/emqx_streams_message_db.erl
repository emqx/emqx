%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_streams_message_db).

-moduledoc """
Facade for all operations with the message database.
""".

-include_lib("emqx_durable_storage/include/emqx_ds.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
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
    %% TODO: Implement quota index flush
    % flush_quota_index/3
]).

%% For testing/maintenance
-export([
    delete_all/0,
    dirty_read_all/1,
    dirty_index/2
]).

-define(STREAMS_MESSAGE_DB_APPEND_RETRY, 3).
% -define(STREAMS_MESSAGE_DB_INDEX_APPEND_RETRY, 3).
-define(STREAMS_MESSAGE_DB_DELETE_RETRY, 1).
-define(STREAMS_MESSAGE_DB_DELETE_RETRY_DELAY, 1000).
-define(STREAMS_MESSAGE_DB_LTS_SETTINGS, #{
    %% "topic/MQ_TOPIC/MQ_ID/key/СOMPACTION_KEY"
    lts_threshold_spec => {simple, {100, 0, 0, 100, 0}}
}).
-define(STREAMS_MESSAGE_DB_TOPIC(MQ_TOPIC, MQ_ID, KEY), [
    <<"topic">>, MQ_TOPIC, MQ_ID, <<"key">>, KEY
]).
-define(STREAMS_INDEX_TOPIC(MQ_TOPIC, MQ_ID), [
    <<"topic">>, MQ_TOPIC, MQ_ID, <<"index">>
]).
-define(STREAMS_DIRTY_APPEND_TIMEOUT, 5000).

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
        _ -> error(failed_to_open_mq_databases)
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

-spec insert(emqx_streams_types:stream_handle(), emqx_types:message()) ->
    ok | {error, term()}.
insert(StreamHandle, Message) ->
    insert(StreamHandle, emqx_streams_prop:is_limited(StreamHandle), Message).

insert(#{is_lastvalue := true} = StreamHandle, IsLimited, Message) ->
    DB = db(StreamHandle),
    case key(StreamHandle, Message) of
        {error, _} = Error ->
            Error;
        {ok, Key} ->
            Shard = emqx_ds:shard_of(DB, Key),
            Topic = stream_message_topic(StreamHandle, Key),
            MessageBin = encode_message(Message),
            Gen = insert_generation(DB),
            TxOpts = #{
                db => DB,
                shard => Shard,
                generation => Gen,
                sync => true,
                retries => ?STREAMS_MESSAGE_DB_APPEND_RETRY,
                retry_interval => retry_interval(DB)
            },
            ?tp(debug, streams_message_db_insert, #{
                stream => StreamHandle,
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
            {_Time, Result} = timer:tc(fun() -> emqx_ds:trans(TxOpts, TxFun) end),
            case Result of
                {atomic, _Serial, _MaybeOldMessage} ->
                    %% TODO: Implement metrics
                    % emqx_mq_metrics:observe_hist_mq(MQHandle, insert_latency_ms, us_to_ms(Time)),
                    %% TODO: Implement quota index updates
                    % IsLimited andalso
                    %     begin
                    %         QuotaIndexUpdates = quota_index_updates(
                    %             MaybeOldMessage, Message, byte_size(MessageBin)
                    %         ),
                    %         ok = emqx_mq_message_quota_buffer:add(
                    %             Gen, Shard, MQHandle, QuotaIndexUpdates
                    %         )
                    %     end,
                    ok;
                {error, IsRecoverable, Reason} ->
                    %% TODO: Implement metrics
                    % emqx_mq_metrics:inc_mq(MQHandle, insert_errors),
                    {error, {IsRecoverable, Reason}}
            end
    end;
insert(#{is_lastvalue := false} = StreamHandle, true = _IsLimited, Message) ->
    DB = db(StreamHandle),
    ClientId = emqx_message:from(Message),
    Shard = emqx_ds:shard_of(DB, ClientId),
    Topic = stream_message_topic(StreamHandle, ClientId),
    MessageBin = encode_message(Message),
    Gen = insert_generation(DB),
    TxOpts = #{
        db => DB,
        shard => Shard,
        generation => Gen,
        sync => true,
        retries => ?STREAMS_MESSAGE_DB_APPEND_RETRY,
        retry_interval => retry_interval(DB)
    },
    ?tp(debug, streams_message_db_insert, #{
        stream => StreamHandle,
        topic => Topic,
        tx_opts => TxOpts
    }),
    TxFun = fun() ->
        emqx_ds:tx_write({Topic, ?ds_tx_ts_monotonic, MessageBin})
    end,
    {_Time, Result} = timer:tc(fun() -> emqx_ds:trans(TxOpts, TxFun) end),
    case Result of
        {atomic, _Serial, ok} ->
            % emqx_mq_metrics:observe_hist_mq(MQHandle, insert_latency_ms, us_to_ms(Time)),
            %% TODO: Implement quota index updates
            % QuotaIndexUpdates = quota_index_updates([], Message, byte_size(MessageBin)),
            % ok = emqx_mq_message_quota_buffer:add(Gen, Shard, MQHandle, QuotaIndexUpdates),
            ok;
        {error, IsRecoverable, Reason} ->
            % emqx_mq_metrics:inc_mq(MQHandle, insert_errors),
            {error, {IsRecoverable, Reason}}
    end;
insert(#{is_lastvalue := false} = StreamHandle, false = _IsLimited, Message) ->
    DB = db(StreamHandle),
    ClientId = emqx_message:from(Message),
    Shard = emqx_ds:shard_of(DB, ClientId),
    Topic = stream_message_topic(StreamHandle, ClientId),
    MessageBin = encode_message(Message),
    NeedReply = need_reply(Message),
    DirtyOpts = #{
        db => DB,
        shard => Shard,
        reply => NeedReply
    },
    ?tp(debug, streams_message_db_insert, #{
        stream => StreamHandle,
        topic => Topic,
        dirty_opts => DirtyOpts
    }),
    % TimeStartMs = erlang:monotonic_time(millisecond),
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
                            % ElapsedMs = erlang:monotonic_time(millisecond) - TimeStartMs,
                            % emqx_mq_metrics:observe_hist_mq(MQHandle, insert_latency_ms, ElapsedMs),
                            ok;
                        {error, IsRecoverable, Reason} ->
                            % emqx_mq_metrics:inc_mq(MQHandle, insert_errors),
                            {error, {IsRecoverable, Reason}}
                    end
            after ?STREAMS_DIRTY_APPEND_TIMEOUT ->
                {error, dirty_append_timeout}
            end
    end.

-spec drop(emqx_streams_types:stream_handle()) -> ok | {error, term()}.
drop(StreamHandle) ->
    DB = db(StreamHandle),
    delete(DB, delete_topics(DB, StreamHandle)).

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
    {ok, DSClient, State} = emqx_ds_client:subscribe(DSClient0, SubOpts, State0),
    {ok, DSClient, State}.

-spec suback(
    emqx_streams_types:stream() | emqx_ds:db(), emqx_ds:subscription_handle(), emqx_ds:sub_seqno()
) -> ok.
suback(Stream, SubHandle, SeqNo) when is_map(Stream) ->
    emqx_ds:suback(db(Stream), SubHandle, SeqNo);
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
    ok = emqx_ds:add_generation(?STREAMS_MESSAGE_REGULAR_DB).

-spec delete_expired_data([emqx_streams_types:stream()]) -> ok.
delete_expired_data(Streams) ->
    Shards = emqx_ds:list_shards(?STREAMS_MESSAGE_LASTVALUE_DB),
    Refs = lists:filtermap(
        fun(Shard) ->
            TxOpts = #{
                db => ?MQ_MESSAGE_LASTVALUE_DB,
                shard => Shard,
                generation => insert_generation(?STREAMS_MESSAGE_LASTVALUE_DB),
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
                %% TODO: Implement limits deadline
                % emqx_mq_message_quota_index:deadline(Index)
                0
        end,
    DeleteTill = max(TimeRetentionDeadline, LimitsDeadline),
    Topic = stream_message_topic(Stream, '#'),
    emqx_ds:tx_del_topic(Topic, 0, DeleteTill).

-spec regular_db_slab_info() -> #{emqx_ds:slab() => emqx_ds:slab_info()}.
regular_db_slab_info() ->
    emqx_ds:list_slabs(?STREAMS_MESSAGE_REGULAR_DB).

-spec initial_generations(emqx_streams_types:stream() | emqx_streams_types:stream_handle()) ->
    #{emqx_ds:slab() => emqx_ds:generation()}.
initial_generations(Stream) ->
    SlabInfo = emqx_ds:list_slabs(db(Stream)),
    maps:from_list(lists:reverse(lists:sort(maps:keys(SlabInfo)))).

-spec drop_regular_db_slab(emqx_ds:slab()) -> ok.
drop_regular_db_slab(Slab) ->
    ok = emqx_ds:drop_slab(?STREAMS_MESSAGE_REGULAR_DB, Slab).

-spec dirty_read_all(emqx_streams_types:stream() | emqx_streams_types:stream_handle()) ->
    [emqx_ds:ttv()].
dirty_read_all(Stream) ->
    emqx_ds:dirty_read(db(Stream), stream_message_topic(Stream, '#')).

-spec dirty_index(
    emqx_streams_types:stream() | emqx_streams_types:stream_handle(), emqx_ds:shard()
) ->
    emqx_mq_message_quota_index:t() | {error, term()} | not_found.
dirty_index(Stream, Shard) ->
    dirty_index(Stream, Shard, emqx_streams_prop:is_limited(Stream)).

dirty_index(_StreanHandle, _Shard, false = _IsLimited) ->
    undefined;
dirty_index(_StreamHandle, _Shard, true = _IsLimited) ->
    %% TODO: Implement quota
    undefined.
% DB = db(MQHandle),
% case
%     emqx_ds:dirty_read(
%         #{db => DB, shard => Shard, generation => insert_generation(DB)},
%         mq_index_topic(MQHandle)
%     )
% of
%     [] ->
%         undefined;
%     [{_, ?INDEX_TS, IndexBin}] ->
%         Opts = emqx_mq_prop:quota_index_opts(MQHandle),
%         emqx_mq_message_quota_index:decode(Opts, IndexBin)
% end.

%%--------------------------------------------------------------------
%% Internal API
%%--------------------------------------------------------------------

%% TODO: Implement flush quota index
% -spec flush_quota_index(
%     emqx_ds:shard(),
%     emqx_ds:generation(),
%     list(#{
%         mq_handle => emqx_mq_types:mq_handle(),
%         updates => list({
%             emqx_mq_message_quota_index:timestamp_us(), emqx_mq_message_quota_index:update()
%         })
%     })
% ) -> ok.
% flush_quota_index(Shard, Generation, UpdatesByMQ) ->
%     TxOpts = #{
%         db => ?MQ_MESSAGE_LASTVALUE_DB,
%         shard => Shard,
%         generation => Generation,
%         sync => true,
%         retries => ?MQ_MESSAGE_DB_INDEX_APPEND_RETRY,
%         retry_interval => retry_interval(?MQ_MESSAGE_LASTVALUE_DB)
%     },
%     TxFun = fun() ->
%         lists:foreach(
%             fun({MQHandle, Updates}) ->
%                 tx_update_index(MQHandle, Updates)
%             end,
%             UpdatesByMQ
%         )
%     end,
%     {Time, Result} = timer:tc(fun() -> emqx_ds:trans(TxOpts, TxFun) end),
%     case Result of
%         {atomic, _Serial, ok} ->
%             % emqx_mq_metrics:observe_hist(flush_quota_index, flush_latency_ms, us_to_ms(Time)),
%             ok;
%         {error, IsRecoverable, Reason} ->
%             % emqx_mq_metrics:inc(flush_quota_index, flush_errors),
%             {error, {IsRecoverable, Reason}}
%     end.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

key(#{is_lastvalue := true, key_expression := KeyExpression} = StreamHandle, Message) ->
    Bindings = #{message => message_to_map(Message)},
    case emqx_variform:render(KeyExpression, Bindings, #{eval_as_string => true}) of
        {error, Reason} ->
            ?tp(warning, streams_message_db_key_expression_error, #{
                stream => StreamHandle,
                reason => Reason,
                key_expression => emqx_variform:decompile(KeyExpression),
                bindings => Bindings
            }),
            {error, Reason};
        {ok, Key} ->
            {ok, Key}
    end.

need_reply(Message) ->
    case emqx_message:qos(Message) of
        ?QOS_0 -> false;
        _ -> true
    end.

% tx_update_index(_MQHandle, []) ->
%     ok;
% tx_update_index(MQHandle, Updates) ->
%     Index0 =
%         case tx_read_index(MQHandle) of
%             undefined ->
%                 IndexStartTsUs = lists:min(
%                     lists:map(fun(?QUOTA_INDEX_UPDATE(TsUs, _, _)) -> TsUs end, Updates)
%                 ),
%                 emqx_mq_message_quota_index:new(
%                     emqx_mq_prop:quota_index_opts(MQHandle), IndexStartTsUs
%                 );
%             Index ->
%                 Index
%         end,
%     Index1 = emqx_mq_message_quota_index:apply_updates(Index0, Updates),
%     tx_write_index(MQHandle, Index1).

% tx_read_index(MQHandle) ->
%     Opts = emqx_mq_prop:quota_index_opts(MQHandle),
%     case emqx_ds:tx_read(mq_index_topic(MQHandle)) of
%         [] ->
%             undefined;
%         [{_, ?INDEX_TS, IndexBin}] ->
%             emqx_mq_message_quota_index:decode(Opts, IndexBin)
%     end.

% tx_write_index(MQHandle, Index) ->
%     emqx_ds:tx_write({
%         mq_index_topic(MQHandle), ?INDEX_TS, emqx_mq_message_quota_index:encode(Index)
%     }).

% quota_index_updates(MaybeOldMessage, Message, MessageSize) ->
%     %% Old record to delete from the index
%     OldUpdates =
%         case MaybeOldMessage of
%             [] ->
%                 [];
%             [{_, _, OldMessageBin}] ->
%                 OldMessage = decode_message(OldMessageBin),
%                 [
%                     ?QUOTA_INDEX_UPDATE(
%                         message_timestamp_us(OldMessage),
%                         -byte_size(OldMessageBin),
%                         -1
%                     )
%                 ]
%         end,
%     %% Add new record to the index
%     NewUpdate = ?QUOTA_INDEX_UPDATE(
%         message_timestamp_us(Message),
%         MessageSize,
%         1
%     ),
%     OldUpdates ++ [NewUpdate].

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

stream_index_topic(#{topic_filter := TopicFilter, id := Id} = _MQ) ->
    ?STREAMS_INDEX_TOPIC(TopicFilter, Id).

db(StreamHandle) ->
    case emqx_streams_prop:is_append_only(StreamHandle) of
        true ->
            ?STREAMS_MESSAGE_REGULAR_DB;
        false ->
            ?STREAMS_MESSAGE_LASTVALUE_DB
    end.

insert_generation(?STREAMS_MESSAGE_LASTVALUE_DB) ->
    1.

retry_interval(?STREAMS_MESSAGE_LASTVALUE_DB) ->
    emqx:get_config([mq, message_db, transaction, flush_interval], 100) * 2.

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

% message_timestamp_us(Message) ->
%     Ms = emqx_message:timestamp(Message),
%     erlang:convert_time_unit(Ms, millisecond, microsecond).

us_to_ms(Us) ->
    erlang:convert_time_unit(Us, microsecond, millisecond).

delete_topics(?STREAMS_MESSAGE_REGULAR_DB, StreamHandle) ->
    [stream_message_topic(StreamHandle, '#')];
delete_topics(?STREAMS_MESSAGE_LASTVALUE_DB, StreamHandle) ->
    [stream_index_topic(StreamHandle), stream_message_topic(StreamHandle, '#')].

now_ms() ->
    erlang:monotonic_time(millisecond).
