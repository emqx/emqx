%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mq_payload_db).

-include_lib("emqx_durable_storage/include/emqx_ds.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include("emqx_mq_internal.hrl").

-export([
    open/0,
    insert/3,
    suback/2,
    create_client/1,
    subscribe/4
]).

-export([
    encode_message/1,
    decode_message/1
]).

%% For testing/maintenance
-export([
    delete_all/0
]).

-define(MQ_PAYLOAD_DB, mq_payload).
-define(MQ_PAYLOAD_DB_APPEND_RETRY, 5).
-define(MQ_PAYLOAD_DB_LTS_SETTINGS, #{
    %% "topic/TOPIC/key/СOMPACTION_KEY"
    lts_threshold_spec => {simple, {100, 0, 100, 0, 100}}
}).
-define(MQ_PAYLOAD_DB_TOPIC(MQ_TOPIC, COMPACTION_KEY), [
    <<"topic">>, MQ_TOPIC, <<"key">>, COMPACTION_KEY
]).
-define(SHARDS_PER_SITE, 4).
-define(REPLICATION_FACTOR, 3).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

open() ->
    Result = emqx_ds:open_db(?MQ_PAYLOAD_DB, settings()),
    ?tp(warning, mq_db_open, #{db => ?MQ_PAYLOAD_DB, result => Result}),
    Result.

insert(#{is_compacted := true} = MQ, _Message, undefined) ->
    ?tp(warning, mq_db_insert_error, #{mq => MQ, reason => undefined_compaction_key}),
    ok;
insert(#{is_compacted := false} = MQ, _Message, CompactionKey) when CompactionKey =/= undefined ->
    ?tp(warning, mq_db_insert_error, #{
        mq => MQ, compaction_key => CompactionKey, reason => compaction_key_set_for_non_compacted_mq
    }),
    ok;
insert(#{is_compacted := true, topic_filter := TopicFilter} = _MQ, Message, CompactionKey) ->
    Topic = ?MQ_PAYLOAD_DB_TOPIC(TopicFilter, CompactionKey),
    TxOpts = #{
        db => ?MQ_PAYLOAD_DB,
        shard => {auto, CompactionKey},
        generation => 1,
        sync => true,
        retries => ?MQ_PAYLOAD_DB_APPEND_RETRY
    },
    Value = encode_message(Message),
    % ?tp(warning, mq_payload_db_insert, #{topic => Topic, generation => 1, value => Value}),
    emqx_ds:trans(TxOpts, fun() ->
        emqx_ds:tx_del_topic(Topic),
        emqx_ds:tx_write({Topic, ?ds_tx_ts_monotonic, Value})
    end);
insert(#{is_compacted := false, topic_filter := TopicFilter} = _MQ, Message, undefined) ->
    Value = encode_message(Message),
    ClientId = emqx_message:from(Message),
    Topic = ?MQ_PAYLOAD_DB_TOPIC(TopicFilter, ClientId),
    TxOpts = #{
        db => ?MQ_PAYLOAD_DB,
        shard => {auto, ClientId},
        generation => 1,
        sync => true,
        retries => ?MQ_PAYLOAD_DB_APPEND_RETRY
    },
    % ?tp(warning, mq_payload_db_insert, #{topic => Topic, generation => 1, value => Value}),
    emqx_ds:trans(TxOpts, fun() ->
        emqx_ds:tx_write({Topic, ?ds_tx_ts_monotonic, Value})
    end).

delete_all() ->
    Shards = emqx_ds:list_shards(?MQ_PAYLOAD_DB),
    lists:foreach(
        fun(Shard) ->
            Topic = ['#'],
            TxOpts = #{
                db => ?MQ_PAYLOAD_DB,
                shard => Shard,
                generation => 1,
                sync => true,
                retries => ?MQ_PAYLOAD_DB_APPEND_RETRY
            },
            emqx_ds:trans(TxOpts, fun() ->
                emqx_ds:tx_del_topic(Topic)
            end)
        end,
        Shards
    ).

create_client(Module) ->
    emqx_ds_client:new(Module, #{}).

subscribe(DSClient0, SubId, MQTopic, State0) ->
    SubOpts = #{
        db => ?MQ_PAYLOAD_DB,
        id => SubId,
        topic => ?MQ_PAYLOAD_DB_TOPIC(MQTopic, '#'),
        ds_sub_opts => #{
            max_unacked => ?MQ_CONSUMER_MAX_UNACKED
        }
    },
    {ok, DSClient, State} = emqx_ds_client:subscribe(DSClient0, SubOpts, State0),
    {ok, DSClient, State}.

suback(SubRef, SeqNo) ->
    emqx_ds:suback(?MQ_PAYLOAD_DB, SubRef, SeqNo).

-spec encode_message(emqx_types:message()) -> binary().
encode_message(Message) ->
    emqx_ds_msg_serializer:serialize(asn1, Message).

-spec decode_message(binary()) -> emqx_types:message().
decode_message(Bin) ->
    emqx_ds_msg_serializer:deserialize(asn1, Bin).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

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
            {emqx_ds_storage_skipstream_lts_v2, ?MQ_PAYLOAD_DB_LTS_SETTINGS},
        store_ttv => true,
        backend => builtin_raft,
        n_shards => NSites * ?SHARDS_PER_SITE,
        replication_options => #{},
        n_sites => NSites,
        replication_factor => ?REPLICATION_FACTOR
    }.
