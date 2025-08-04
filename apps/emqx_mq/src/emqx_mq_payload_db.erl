%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mq_payload_db).

-include_lib("emqx_durable_storage/include/emqx_ds.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include("emqx_mq_internal.hrl").

-elvis([{elvis_style, atom_naming_convention, disable}]).
-include("../gen_src/MQMessage.hrl").

-export([
    open/0,
    insert/3,
    suback/2,
    create_client/1,
    subscribe/4
]).

-export([
    to_mq_message/1,
    from_mq_message/1,
    encode_mq_message/1,
    decode_mq_message/1
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
    Value = encode_mq_message(to_mq_message(Message)),
    ?tp(warning, mq_payload_db_insert, #{topic => Topic, generation => 1, value => Value}),
    emqx_ds:trans(TxOpts, fun() ->
        emqx_ds:tx_del_topic(Topic),
        emqx_ds:tx_write({Topic, ?ds_tx_ts_monotonic, Value})
    end);
insert(#{is_compacted := false, topic_filter := TopicFilter} = _MQ, Message, undefined) ->
    Value = encode_mq_message(to_mq_message(Message)),
    ClientId = emqx_message:from(Message),
    Topic = ?MQ_PAYLOAD_DB_TOPIC(TopicFilter, ClientId),
    TxOpts = #{
        db => ?MQ_PAYLOAD_DB,
        shard => {auto, ClientId},
        generation => 1,
        sync => true,
        retries => ?MQ_PAYLOAD_DB_APPEND_RETRY
    },
    ?tp(warning, mq_payload_db_insert, #{topic => Topic, generation => 1, value => Value}),
    emqx_ds:trans(TxOpts, fun() ->
        emqx_ds:tx_write({Topic, ?ds_tx_ts_monotonic, Value})
    end).

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

-spec to_mq_message(emqx_types:message()) -> emqx_mq_types:mq_message().
to_mq_message(#message{from = From, topic = Topic, payload = Payload, timestamp = Timestamp}) ->
    #'MQMessage'{
        from = From,
        topic = Topic,
        payload = Payload,
        timestamp = Timestamp
    }.

-spec from_mq_message(emqx_mq_types:mq_message()) -> emqx_types:message().
from_mq_message(#'MQMessage'{from = From, topic = Topic, payload = Payload, timestamp = Timestamp}) ->
    Message0 = emqx_message:make(From, ?QOS_1, Topic, Payload),
    Message0#message{timestamp = Timestamp}.

-spec encode_mq_message(emqx_mq_types:mq_message()) -> binary().
encode_mq_message(#'MQMessage'{} = MQMessage) ->
    {ok, Bin} = 'MQMessage':encode('MQMessage', MQMessage),
    Bin.

-spec decode_mq_message(binary()) -> emqx_mq_types:mq_message().
decode_mq_message(Bin) ->
    {ok, MQMessage} = 'MQMessage':decode('MQMessage', Bin),
    MQMessage.

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
