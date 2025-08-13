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
    insert/3,
    suback/3,
    create_client/1,
    subscribe/4,
    drop/1
]).

-export([
    encode_message/1,
    decode_message/1
]).

%% For testing/maintenance
-export([
    add_regular_db_generation/0,
    last_regular_db_generations/0
]).

%% For testing/maintenance
-export([
    delete_all/0
]).

-define(MQ_MESSAGE_DB_APPEND_RETRY, 5).
-define(MQ_MESSAGE_DB_LTS_SETTINGS, #{
    %% "topic/TOPIC/key/СOMPACTION_KEY"
    lts_threshold_spec => {simple, {100, 0, 0, 100, 0, 100}}
}).
-define(MQ_MESSAGE_DB_TOPIC(MQ_TOPIC, MQ_ID, COMPACTION_KEY), [
    <<"topic">>, MQ_TOPIC, MQ_ID, <<"key">>, COMPACTION_KEY
]).

%% TODO: increase
-define(SHARDS_PER_SITE, 1).
-define(REPLICATION_FACTOR, 1).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec open() -> ok.
open() ->
    maybe
        ok ?= emqx_ds:open_db(?MQ_MESSAGE_COMPACTED_DB, settings()),
        ok ?= emqx_ds:open_db(?MQ_MESSAGE_REGULAR_DB, settings()),
        ok ?= emqx_ds:wait_db(?MQ_MESSAGE_COMPACTED_DB, all, infinity),
        ok ?= emqx_ds:wait_db(?MQ_MESSAGE_REGULAR_DB, all, infinity)
    else
        _ -> error(failed_to_open_mq_databases)
    end.

-spec insert(emqx_mq_types:mq(), emqx_types:message(), _CompactionKey :: undefined | binary()) ->
    ok.
insert(#{is_compacted := true} = MQ, _Message, undefined) ->
    ?tp(warning, mq_db_insert_error, #{mq => MQ, reason => undefined_compaction_key}),
    ok;
insert(#{is_compacted := false} = MQ, _Message, CompactionKey) when CompactionKey =/= undefined ->
    ?tp(warning, mq_db_insert_error, #{
        mq => MQ, compaction_key => CompactionKey, reason => compaction_key_set_for_non_compacted_mq
    }),
    ok;
insert(#{is_compacted := true} = MQ, Message, CompactionKey) ->
    Topic = mq_message_topic(MQ, CompactionKey),
    TxOpts = #{
        db => ?MQ_MESSAGE_COMPACTED_DB,
        shard => {auto, CompactionKey},
        generation => insert_generation_compacted(),
        sync => true,
        retries => ?MQ_MESSAGE_DB_APPEND_RETRY
    },
    Value = encode_message(Message),
    % ?tp(warning, mq_message_db_insert, #{topic => Topic, generation => 1, value => Value}),
    emqx_ds:trans(TxOpts, fun() ->
        emqx_ds:tx_del_topic(Topic),
        emqx_ds:tx_write({Topic, ?ds_tx_ts_monotonic, Value})
    end);
insert(#{is_compacted := false} = MQ, Message, undefined) ->
    Value = encode_message(Message),
    ClientId = emqx_message:from(Message),
    Topic = mq_message_topic(MQ, ClientId),
    Shard = emqx_ds:shard_of(?MQ_MESSAGE_REGULAR_DB, ClientId),
    Generation = insert_generation_regular(),
    TxOpts = #{
        db => ?MQ_MESSAGE_REGULAR_DB,
        shard => Shard,
        generation => Generation,
        sync => true,
        retries => ?MQ_MESSAGE_DB_APPEND_RETRY
    },
    Res = emqx_ds:trans(TxOpts, fun() ->
        emqx_ds:tx_write({Topic, ?ds_tx_ts_monotonic, Value})
    end),
    ?tp(warning, mq_message_db_insert, #{
        topic => Topic,
        shard => Shard,
        generation => Generation,
        message_topic => emqx_message:topic(Message),
        result => Res
    }),
    Res.

-spec drop(emqx_mq_types:mq()) -> ok.
drop(MQ) ->
    delete(db(MQ), mq_message_topic(MQ, '#')).

-spec delete_all() -> ok.
delete_all() ->
    delete(?MQ_MESSAGE_COMPACTED_DB, ['#']),
    delete(?MQ_MESSAGE_REGULAR_DB, ['#']).

-spec create_client(module()) -> emqx_ds_client:t().
create_client(Module) ->
    emqx_ds_client:new(Module, #{}).

-spec subscribe(
    emqx_mq_types:mq(),
    emqx_ds_client:t(),
    emqx_ds_client:sub_id(),
    emqx_mq_consumer_stream_buffer:state()
) ->
    {ok, emqx_ds_client:t(), emqx_mq_consumer_stream_buffer:state()}.
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
    emqx_mq_types:mq() | emqx_ds:db(), emqx_ds_client:sub_handle(), emqx_ds_client:sub_seqno()
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

%%--------------------------------------------------------------------
%% Maintenance API
%%--------------------------------------------------------------------

add_regular_db_generation() ->
    ok = emqx_ds:add_generation(?MQ_MESSAGE_REGULAR_DB).

last_regular_db_generations() ->
    maps:values(maps:from_list(lists:sort(maps:keys(emqx_ds:list_slabs(?MQ_MESSAGE_REGULAR_DB))))).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

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
        maps:keys(emqx_ds:list_slabs(?MQ_MESSAGE_REGULAR_DB))
    ).

mq_message_topic(#{topic_filter := TopicFilter, id := Id} = _MQ, CompactionKey) ->
    ?MQ_MESSAGE_DB_TOPIC(TopicFilter, Id, CompactionKey).

db(#{is_compacted := true} = _MQ) ->
    ?MQ_MESSAGE_COMPACTED_DB;
db(#{is_compacted := false} = _MQ) ->
    ?MQ_MESSAGE_REGULAR_DB.

insert_generation_compacted() ->
    1.

insert_generation_regular() ->
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
        backend => builtin_raft,
        n_shards => NSites * ?SHARDS_PER_SITE,
        replication_options => #{},
        n_sites => NSites,
        replication_factor => ?REPLICATION_FACTOR
    }.
