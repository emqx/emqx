%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mq_db).

-include("emqx_mq_internal.hrl").
-include_lib("emqx_durable_storage/include/emqx_ds.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-export([
    open/0,
    insert/3
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
    Payload = emqx_message:payload(Message),
    emqx_ds:trans(TxOpts, fun() ->
        emqx_ds:tx_del_topic(Topic),
        emqx_ds:tx_write({Topic, ?ds_tx_ts_monotonic, Payload})
    end);
insert(#{is_compacted := false, topic_filter := TopicFilter} = _MQ, Message, undefined) ->
    Payload = emqx_message:payload(Message),
    ClientId = emqx_message:from(Message),
    Topic = ?MQ_PAYLOAD_DB_TOPIC(TopicFilter, ClientId),
    TxOpts = #{
        db => ?MQ_PAYLOAD_DB,
        shard => {auto, ClientId},
        generation => 1,
        sync => true,
        retries => ?MQ_PAYLOAD_DB_APPEND_RETRY
    },
    emqx_ds:trans(TxOpts, fun() ->
        emqx_ds:tx_write({Topic, ?ds_tx_ts_monotonic, Payload})
    end).

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
