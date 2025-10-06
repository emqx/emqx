%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mq_message_db).

-moduledoc """
Facade for all operations with the message database.
""".

-include_lib("emqx_durable_storage/include/emqx_ds.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
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
    delete_lastvalue_data/2,
    regular_db_slab_info/0,
    drop_regular_db_slab/1,
    initial_generations/1
]).

%% For testing/maintenance
-export([
    delete_all/0,
    dirty_read_all/1
]).

-define(MQ_MESSAGE_DB_APPEND_RETRY, 1).
-define(MQ_MESSAGE_DB_DELETE_RETRY, 1).
-define(MQ_MESSAGE_DB_DELETE_RETRY_DELAY, 1000).
-define(MQ_MESSAGE_DB_LTS_SETTINGS, #{
    %% "topic/MQ_TOPIC/MQ_ID/key/СOMPACTION_KEY"
    lts_threshold_spec => {simple, {100, 0, 0, 100, 0}}
}).
-define(MQ_MESSAGE_DB_TOPIC(MQ_TOPIC, MQ_ID, KEY), [
    <<"topic">>, MQ_TOPIC, MQ_ID, <<"key">>, KEY
]).
-define(MQ_MESSAGE_REGULAR_DB_DIRTY_APPEND_TIMEOUT, 5000).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec open() -> ok.
open() ->
    Config = maps:merge(emqx_ds_schema:db_config_mq_messages(), #{
        storage => {emqx_ds_storage_skipstream_lts_v2, ?MQ_MESSAGE_DB_LTS_SETTINGS},
        store_ttv => true,
        atomic_batches => true
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
insert(#{is_lastvalue := true} = MQHandle, Message) ->
    DB = ?MQ_MESSAGE_LASTVALUE_DB,
    case key(MQHandle, Message) of
        {error, _} = Error ->
            Error;
        {ok, Key} ->
            Shard = emqx_ds:shard_of(DB, Key),
            Topic = mq_message_topic(MQHandle, Key),
            Value = encode_message(Message),
            TxOpts = #{
                db => DB,
                shard => Shard,
                generation => insert_generation(DB),
                sync => true,
                retries => ?MQ_MESSAGE_DB_APPEND_RETRY
            },
            ?tp(debug, mq_message_db_insert, #{
                mq => MQHandle,
                topic => Topic,
                key => Key,
                tx_opts => TxOpts
            }),
            TxFun = fun() ->
                emqx_ds:tx_del_topic(Topic),
                emqx_ds:tx_write({Topic, ?ds_tx_ts_monotonic, Value})
            end,
            case emqx_ds:trans(TxOpts, TxFun) of
                {atomic, _Serial, ok} ->
                    ok;
                {error, IsRecoverable, Reason} ->
                    {error, {IsRecoverable, Reason}}
            end
    end;
insert(#{is_lastvalue := false} = MQHandle, Message) ->
    DB = ?MQ_MESSAGE_REGULAR_DB,
    ClientId = emqx_message:from(Message),
    Shard = emqx_ds:shard_of(DB, ClientId),
    Topic = mq_message_topic(MQHandle, ClientId),
    Value = encode_message(Message),
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
    case emqx_ds:dirty_append(DirtyOpts, [{Topic, ?ds_tx_ts_monotonic, Value}]) of
        noreply ->
            ok;
        {error, IsRecoverable, Reason} ->
            {error, {IsRecoverable, Reason}};
        Ref when is_reference(Ref) ->
            receive
                ?ds_tx_commit_reply(Ref, Reply) ->
                    case emqx_ds:dirty_append_outcome(Ref, Reply) of
                        {ok, _Serial} ->
                            ok;
                        {error, IsRecoverable, Reason} ->
                            {error, {IsRecoverable, Reason}}
                    end
            after ?MQ_MESSAGE_REGULAR_DB_DIRTY_APPEND_TIMEOUT ->
                {error, dirty_append_timeout}
            end
    end.

-spec drop(emqx_mq_types:mq_handle()) -> ok | {error, term()}.
drop(MQHandle) ->
    delete(db(MQHandle), mq_message_topic(MQHandle, '#')).

-spec delete_all() -> ok.
delete_all() ->
    ok = delete(?MQ_MESSAGE_LASTVALUE_DB, ['#']),
    ok = delete(?MQ_MESSAGE_REGULAR_DB, ['#']).

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

-spec delete_lastvalue_data([emqx_mq_types:mq()], non_neg_integer()) -> ok.
delete_lastvalue_data(MQs, NowMS) ->
    Shards = emqx_ds:list_shards(?MQ_MESSAGE_LASTVALUE_DB),
    Refs = lists:filtermap(
        fun(Shard) ->
            TxOpts = #{
                db => ?MQ_MESSAGE_LASTVALUE_DB,
                shard => Shard,
                generation => insert_generation(?MQ_MESSAGE_LASTVALUE_DB),
                sync => false,
                retries => ?MQ_MESSAGE_DB_APPEND_RETRY
            },
            Res = emqx_ds:trans(TxOpts, fun() ->
                lists:foreach(
                    fun(#{is_lastvalue := true, data_retention_period := DataRetentionPeriod} = MQ) ->
                        Topic = mq_message_topic(MQ, '#'),
                        DeleteTill = max(NowMS - DataRetentionPeriod, 0),
                        emqx_ds:tx_del_topic(Topic, 0, DeleteTill)
                    end,
                    MQs
                )
            end),
            case Res of
                {async, Ref, _} -> {true, Ref};
                {nop, ok} -> false
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
%% Internal functions
%%--------------------------------------------------------------------

key(#{is_lastvalue := true, key_expression := KeyExpression} = MQ, Message) ->
    Bindings = #{message => message_to_map(Message)},
    case emqx_variform:render(KeyExpression, Bindings, #{eval_as_string => true}) of
        {error, Reason} ->
            ?tp(warning, mq_message_db_key_expression_error, #{
                mq => MQ,
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

delete(DB, Topic) ->
    {_Time, Errors} = timer:tc(fun() ->
        %% NOTE
        %% This is a temporary workaround for the behavior of the ds tx manager.
        %% Simultaneous asycnhronous txs to the same shard having a new generation
        %% may result to a conflict.
        %% So we delete in groups with all different shards.
        lists:flatmap(
            fun(Slabs) ->
                do_delete(DB, Topic, ?MQ_MESSAGE_DB_DELETE_RETRY + 1, Slabs, [])
            end,
            slabs_by_generation(DB)
        )
    end),
    ?tp_debug(mq_message_db_delete, #{
        topic => Topic,
        time => erlang:convert_time_unit(_Time, microsecond, millisecond),
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

do_delete(_DB, _Topic, 0, _Slabs, Errors) ->
    Errors;
do_delete(DB, Topic, Retries, Slabs0, _Errors0) ->
    Refs = lists:map(
        fun({Shard, Generation} = Slab) ->
            TxOpts = #{
                db => DB,
                shard => Shard,
                generation => Generation,
                sync => false
            },
            {async, Ref, _} = emqx_ds:trans(TxOpts, fun() ->
                emqx_ds:tx_del_topic(Topic)
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
                                topic => Topic,
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
            do_delete(DB, Topic, Retries - 1, Slabs, Errors)
    end.

mq_message_topic(#{topic_filter := TopicFilter, id := Id} = _MQ, Key) ->
    ?MQ_MESSAGE_DB_TOPIC(TopicFilter, Id, Key).

db(#{is_lastvalue := true} = _MQ) ->
    ?MQ_MESSAGE_LASTVALUE_DB;
db(#{is_lastvalue := false} = _MQ) ->
    ?MQ_MESSAGE_REGULAR_DB.

insert_generation(?MQ_MESSAGE_LASTVALUE_DB) ->
    1.

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
