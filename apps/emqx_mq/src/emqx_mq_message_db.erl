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

-define(MQ_MESSAGE_DB_APPEND_RETRY, 5).
-define(MQ_MESSAGE_DB_LTS_SETTINGS, #{
    %% "topic/TOPIC/key/СOMPACTION_KEY"
    lts_threshold_spec => {simple, {100, 0, 0, 100, 0, 100}}
}).
-define(MQ_MESSAGE_DB_TOPIC(MQ_TOPIC, MQ_ID, KEY), [
    <<"topic">>, MQ_TOPIC, MQ_ID, <<"key">>, KEY
]).

-define(SHARDS_PER_SITE, 3).

-define(REPLICATION_FACTOR, 3).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec open() -> ok.
open() ->
    maybe
        ok ?= emqx_ds:open_db(?MQ_MESSAGE_LASTVALUE_DB, settings()),
        ok ?= emqx_ds:open_db(?MQ_MESSAGE_REGULAR_DB, settings()),
        ok ?= emqx_ds:wait_db(?MQ_MESSAGE_LASTVALUE_DB, all, infinity),
        ok ?= emqx_ds:wait_db(?MQ_MESSAGE_REGULAR_DB, all, infinity)
    else
        _ -> error(failed_to_open_mq_databases)
    end.

-spec close() -> ok.
close() ->
    ok = emqx_ds:close_db(?MQ_MESSAGE_LASTVALUE_DB),
    ok = emqx_ds:close_db(?MQ_MESSAGE_REGULAR_DB).

-spec insert(emqx_mq_types:mq(), list(emqx_types:message())) ->
    ok | {error, list(emqx_ds:error(_Reason))}.
insert(#{is_lastvalue := true} = MQ, Messages) ->
    % ?tp_debug(mq_message_db_insert, #{topic => Topic, generation => 1, value => Value}),
    insert(MQ, Messages, fun(_MQ, Topic, Value) ->
        emqx_ds:tx_del_topic(Topic),
        emqx_ds:tx_write({Topic, ?ds_tx_ts_monotonic, Value})
    end);
insert(#{is_lastvalue := false} = MQ, Messages) ->
    insert(MQ, Messages, fun(_MQ, Topic, Value) ->
        emqx_ds:tx_write({Topic, ?ds_tx_ts_monotonic, Value})
    end).

insert(MQ, Messages, TxInsertFun) ->
    TopicValueByShard = group_by_shard(MQ, Messages),
    DB = db(MQ),
    Results = lists:map(
        fun({Shard, TVs}) ->
            TxOpts = #{
                db => DB,
                shard => Shard,
                generation => insert_generation(DB),
                sync => true,
                retries => ?MQ_MESSAGE_DB_APPEND_RETRY
            },
            Res = emqx_ds:trans(TxOpts, fun() ->
                ok = lists:foreach(
                    fun({Topic, Value}) ->
                        TxInsertFun(MQ, Topic, Value)
                    end,
                    TVs
                ),
                ok
            end),
            ?tp_debug(mq_message_db_insert, #{
                shard => Shard,
                result => Res
            }),
            Res
        end,
        TopicValueByShard
    ),
    format_insert_tx_results(Results).

-spec drop(emqx_mq_types:mq_handle()) -> ok.
drop(MQHandle) ->
    delete(db(MQHandle), mq_message_topic(MQHandle, '#')).

-spec delete_all() -> ok.
delete_all() ->
    delete(?MQ_MESSAGE_LASTVALUE_DB, ['#']),
    delete(?MQ_MESSAGE_REGULAR_DB, ['#']).

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
delete_lastvalue_data(MQs, TimeThreshold) ->
    Shards = emqx_ds:list_shards(?MQ_MESSAGE_LASTVALUE_DB),
    lists:foreach(
        fun(Shard) ->
            TxOpts = #{
                db => ?MQ_MESSAGE_LASTVALUE_DB,
                shard => Shard,
                generation => insert_generation(?MQ_MESSAGE_LASTVALUE_DB),
                sync => true,
                retries => ?MQ_MESSAGE_DB_APPEND_RETRY
            },
            emqx_ds:trans(TxOpts, fun() ->
                lists:foreach(
                    fun(#{is_lastvalue := true, data_retention_period := DataRetentionPeriod} = MQ) ->
                        Topic = mq_message_topic(MQ, '#'),
                        DeleteTill = max(TimeThreshold - DataRetentionPeriod, 0),
                        emqx_ds:tx_del_topic(Topic, 0, DeleteTill)
                    end,
                    MQs
                )
            end)
        end,
        Shards
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

group_by_shard(#{is_lastvalue := false} = MQ, Messages) ->
    ByShard = lists:foldl(
        fun(Message, Acc) ->
            ClientId = emqx_message:from(Message),
            Shard = emqx_ds:shard_of(?MQ_MESSAGE_REGULAR_DB, ClientId),
            Topic = mq_message_topic(MQ, ClientId),
            Value = encode_message(Message),
            maps:update_with(Shard, fun(Msgs) -> [{Topic, Value} | Msgs] end, [{Topic, Value}], Acc)
        end,
        #{},
        Messages
    ),
    groupped_by_shard_to_list(ByShard);
group_by_shard(#{is_lastvalue := true} = MQ, Messages) ->
    ByShard = lists:foldl(
        fun(Message, Acc) ->
            Props = emqx_message:get_header(properties, Message, #{}),
            UserProperties = maps:get('User-Property', Props, []),
            Key = proplists:get_value(
                ?MQ_KEY_USER_PROPERTY, UserProperties, undefined
            ),
            case Key of
                undefined ->
                    ?tp_debug(mq_message_db_insert_error, #{
                        mq => MQ, message => Message, reason => undefined_key
                    }),
                    Acc;
                _ ->
                    Shard = emqx_ds:shard_of(?MQ_MESSAGE_LASTVALUE_DB, Key),
                    Topic = mq_message_topic(MQ, Key),
                    Value = encode_message(Message),
                    maps:update_with(
                        Shard, fun(Msgs) -> [{Topic, Value} | Msgs] end, [{Topic, Value}], Acc
                    )
            end
        end,
        #{},
        Messages
    ),
    groupped_by_shard_to_list(ByShard).

groupped_by_shard_to_list(ByShard) ->
    lists:map(
        fun({Shard, TVs}) ->
            {Shard, lists:reverse(TVs)}
        end,
        maps:to_list(ByShard)
    ).

format_insert_tx_results(Results) ->
    format_insert_tx_results(Results, []).

format_insert_tx_results([] = _Results, [] = _ErrorAcc) ->
    ok;
format_insert_tx_results([] = _Results, ErrorAcc) ->
    {error, lists:reverse(ErrorAcc)};
format_insert_tx_results([{atomic, _Serial, ok} | Results], ErrorAcc) ->
    format_insert_tx_results(Results, ErrorAcc);
format_insert_tx_results([{error, _, _} = Error | Results], ErrorAcc) ->
    format_insert_tx_results(Results, [Error | ErrorAcc]).

delete(DB, Topic) ->
    lists:foreach(
        fun({Shard, Generation}) ->
            TxOpts = #{
                db => DB,
                shard => Shard,
                generation => Generation,
                sync => true,
                retries => ?MQ_MESSAGE_DB_APPEND_RETRY
            },
            emqx_ds:trans(TxOpts, fun() ->
                emqx_ds:tx_del_topic(Topic)
            end)
        end,
        maps:keys(emqx_ds:list_slabs(DB))
    ).

mq_message_topic(#{topic_filter := TopicFilter, id := Id} = _MQ, Key) ->
    ?MQ_MESSAGE_DB_TOPIC(TopicFilter, Id, Key).

db(#{is_lastvalue := true} = _MQ) ->
    ?MQ_MESSAGE_LASTVALUE_DB;
db(#{is_lastvalue := false} = _MQ) ->
    ?MQ_MESSAGE_REGULAR_DB.

insert_generation(?MQ_MESSAGE_LASTVALUE_DB) ->
    1;
insert_generation(?MQ_MESSAGE_REGULAR_DB) ->
    latest.

settings() ->
    NSites = length(emqx:running_nodes()),
    #{
        transaction =>
            #{
                flush_interval => 100,
                idle_flush_interval => 20,
                conflict_window => 5000
            },
        storage =>
            {emqx_ds_storage_skipstream_lts_v2, ?MQ_MESSAGE_DB_LTS_SETTINGS},
        store_ttv => true,
        atomic_batches => true,
        backend => builtin_raft,
        n_shards => NSites * ?SHARDS_PER_SITE,
        replication_options => #{},
        n_sites => NSites,
        replication_factor => ?REPLICATION_FACTOR
    }.
