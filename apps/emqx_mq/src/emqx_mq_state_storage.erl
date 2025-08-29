%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mq_state_storage).

-include_lib("emqx_durable_storage/include/emqx_ds_pmap.hrl").
-include_lib("emqx_durable_storage/include/emqx_ds.hrl").
-include_lib("../gen_src/MessageQueue.hrl").

-moduledoc """
Persistence of Message queue state:
* MQ properties
* MQ consumption state
""".

-behaviour(emqx_ds_pmap).

-export([
    open_db/0,
    close_db/0
]).

-export([
    open_consumer_state/1,
    commit_consumer_state/2,
    destroy_consumer_state/1,
    put_shard_progress/3,
    get_shards_progress/1
]).

-export([
    create_mq_state/1,
    update_mq_state/1,
    destroy_mq_state/1,
    find_mq/1
]).

%% For tests/maintenance.
-export([
    delete_all/0
]).

-export([
    pmap_encode_key/3,
    pmap_decode_key/2,
    pmap_encode_val/3,
    pmap_decode_val/3
]).

-define(DB, mq_state).

-type id() :: binary().

%% col_* are collection field names
%% pn_* are pmap names
-define(pn_shard_progress, <<"shp">>).
-define(col_shard_progress, col_shard_progress).
-define(pn_mq, <<"mq">>).
-define(col_mq, col_mq).

-define(mq_key, mq_key).
-define(mq_key_bin, <<"mq_key">>).

-type consumer_state() :: #{
    id := id(),
    mq := emqx_mq_types:mq(),
    ?collection_dirty := boolean(),
    ?collection_guard := emqx_ds_pmap:guard() | undefined,
    ?col_shard_progress := emqx_ds_pmap:pmap(emqx_ds:shard(), emqx_mq_types:progress())
}.

-type mq_state() :: #{
    id := id(),
    ?collection_dirty := boolean(),
    ?collection_guard := emqx_ds_pmap:guard() | undefined,
    %% NOTE
    %% we store MQ as a singleton key-value pair
    %% since we are not going to update MQ properties individually.
    %%
    %% This also allows us to dirty read the MQ consistently.
    ?col_mq := emqx_ds_pmap:pmap(?mq_key, emqx_mq_types:mq())
}.

-export_type([
    consumer_state/0,
    mq_state/0
]).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

open_db() ->
    Config = emqx_mq_schema:db_mq_state(),
    emqx_ds:open_db(?DB, Config#{
        atomic_batches => true,
        append_only => false,
        store_ttv => true,
        storage => emqx_ds_pmap:storage_opts(#{})
    }),
    emqx_ds:wait_db(?DB, all, infinity).

close_db() ->
    emqx_ds:close_db(?DB).

%%------------------------------------------------------------------------------
%% Consumer State API
%%------------------------------------------------------------------------------

open_consumer_state(MQ) ->
    Id = consumer_state_id(MQ),
    TxRes = emqx_ds:trans(
        trans_opts(Id),
        fun() ->
            Rec =
                case open_consumer_state_tx(Id) of
                    undefined ->
                        new_consumer_state(MQ);
                    Rec0 ->
                        Rec0#{?collection_dirty => true}
                end,
            persist_consumer_state_tx(_NeedClaimOwnership = true, Rec)
        end
    ),
    case TxRes of
        {atomic, TXSerial, Rec} ->
            {ok, Rec#{?collection_guard := TXSerial}};
        {nop, Rec} ->
            {ok, Rec};
        Err ->
            {error, {failed_to_open_consumer_state, Err}}
    end.

-spec put_shard_progress(
    emqx_ds:shard(), emqx_mq_consumer_stream_buffer:progress(), consumer_state()
) -> consumer_state().
put_shard_progress(Shard, Progress, CSRec) ->
    emqx_ds_pmap:collection_put(?col_shard_progress, Shard, Progress, CSRec).

-spec get_shards_progress(consumer_state()) -> emqx_mq_consumer_streams:progress().
get_shards_progress(#{?col_shard_progress := ShardProgress}) ->
    emqx_ds_pmap:fold(
        fun(Shard, Progress, Acc) ->
            Acc#{Shard => Progress}
        end,
        #{},
        ShardProgress
    ).

commit_consumer_state(_NeedClaimOwnership = false, #{?collection_dirty := false} = CSRec) ->
    {ok, CSRec};
commit_consumer_state(
    NeedClaimOwnership,
    CSRec0 = #{id := Id}
) ->
    TxRes = emqx_ds:trans(
        trans_opts(Id),
        fun() -> persist_consumer_state_tx(NeedClaimOwnership, CSRec0) end
    ),
    case TxRes of
        {atomic, TXSerial, CSRec} when NeedClaimOwnership ->
            {ok, CSRec#{?collection_guard := TXSerial}};
        {atomic, _, CSRec} ->
            {ok, CSRec};
        {nop, CSRec} ->
            {ok, CSRec};
        Err ->
            {error, {failed_to_commit_consumer_state, Err}}
    end.

-spec destroy_consumer_state(emqx_mq_types:mq_handle()) -> ok.
destroy_consumer_state(MQHandle) ->
    Id = consumer_state_id(MQHandle),
    TxRes = emqx_ds:trans(
        trans_opts(Id),
        fun() ->
            emqx_ds_pmap:tx_delete_guard(Id),
            emqx_ds_pmap:tx_destroy(Id, ?pn_shard_progress)
        end
    ),
    case TxRes of
        {atomic, _TXSerial, _} ->
            ok;
        {nop, _} ->
            ok;
        Err ->
            {error, {failed_to_destroy_consumer_state, Err}}
    end.

-spec create_mq_state(emqx_mq_types:mq()) -> ok.
create_mq_state(MQ) ->
    TxRes = emqx_ds:trans(
        trans_opts(mq_state_id(MQ)),
        fun() ->
            MQStateRec =
                case open_mq_state_tx(mq_state_id(MQ)) of
                    undefined ->
                        put_mq(MQ, new_mq_state(MQ));
                    FoundMQStateRec ->
                        FoundMQStateRec
                end,
            persist_mq_state_tx(MQStateRec)
        end
    ),
    case TxRes of
        {atomic, TXSerial, MQStateRec} ->
            {ok, MQStateRec#{?collection_guard := TXSerial}};
        {nop, MQStateRec} ->
            {ok, MQStateRec};
        Err ->
            {error, {failed_to_create_mq_state, Err}}
    end.

update_mq_state(MQ) ->
    TxRes = emqx_ds:trans(
        trans_opts(mq_state_id(MQ)),
        fun() ->
            case open_mq_state_tx(mq_state_id(MQ)) of
                undefined ->
                    not_found;
                MQStateRec ->
                    put_mq(MQ, MQStateRec),
                    ok
            end
        end
    ),
    case TxRes of
        {atomic, _TXSerial, Res} ->
            Res;
        {nop, Res} ->
            Res;
        Err ->
            error({failed_to_update_mq_state, Err})
    end.

find_mq(MQId) ->
    Id = mq_state_id(MQId),
    FoldOptions = maps:merge(trans_opts(Id), #{errors => ignore}),
    case emqx_ds_pmap:dirty_read(?MODULE, ?pn_mq, Id, FoldOptions) of
        #{?mq_key := MQ} ->
            {ok, MQ};
        #{} ->
            not_found
    end.

destroy_mq_state(MQ) ->
    Id = mq_state_id(MQ),
    TxRes = emqx_ds:trans(
        trans_opts(Id),
        fun() ->
            emqx_ds_pmap:tx_delete_guard(Id),
            emqx_ds_pmap:tx_destroy(Id, ?pn_mq)
        end
    ),
    case TxRes of
        {atomic, _TXSerial, _} ->
            ok;
        {nop, _} ->
            ok;
        Err ->
            {error, {failed_to_destroy_mq_state, Err}}
    end.

delete_all() ->
    Shards = emqx_ds:list_shards(?DB),
    lists:foreach(
        fun(Shard) ->
            emqx_ds:trans(
                #{db => ?DB, shard => Shard, generation => 1},
                fun() ->
                    emqx_ds:tx_del_topic(['#'])
                end
            )
        end,
        Shards
    ).

%%------------------------------------------------------------------------------
%% emqx_ds_pmap behaviour
%%------------------------------------------------------------------------------

pmap_encode_key(?pn_shard_progress, Shard = _Key, _Val) when is_binary(Shard) ->
    Shard;
pmap_encode_key(?pn_mq, ?mq_key, _Val) ->
    ?mq_key_bin.

pmap_decode_key(?pn_shard_progress, Shard = _KeyBin) ->
    Shard;
pmap_decode_key(?pn_mq, ?mq_key_bin) ->
    ?mq_key.

pmap_encode_val(?pn_shard_progress, _Key, #{
    status := active,
    generation := Generation,
    buffer_progress := #{
        it := It,
        last_message_id := LastMessageId,
        unacked := Unacked
    },
    stream := Stream
}) ->
    {ok, StreamBin} = emqx_ds:stream_to_binary(?DB, Stream),
    {ok, ItBin} = emqx_ds:iterator_to_binary(?DB, It),
    Rec =
        #'ShardProgress'{
            progress =
                {active, #'ActiveShardProgress'{
                    generation = Generation,
                    stream = StreamBin,
                    bufferProgress = #'StreamBufferProgress'{
                        it = ItBin,
                        lastMessageId = to_optional_integer(LastMessageId),
                        unacked = maps:keys(emqx_mq_utils:merge_maps(Unacked))
                    }
                }}
        },
    {ok, Bin} = 'MessageQueue':encode('ShardProgress', Rec),
    Bin;
pmap_encode_val(?pn_shard_progress, _Key, #{
    status := finished,
    generation := Generation
}) ->
    Rec = {v1, {finished, #'FinishedShardProgress'{generation = Generation}}},
    {ok, Bin} = 'MessageQueue':encode('ShardProgress', Rec),
    Bin;
pmap_encode_val(
    ?pn_mq,
    ?mq_key,
    #{
        id := Id,
        topic_filter := TopicFilter,
        is_lastvalue := IsLastvalue,
        consumer_max_inactive := ConsumerMaxInactive,
        ping_interval := PingInterval,
        redispatch_interval := RedispatchInterval,
        dispatch_strategy := DispatchStrategy,
        local_max_inflight := LocalMaxInflight,
        busy_session_retry_interval := BusySessionRetryInterval,
        stream_max_buffer_size := StreamMaxBufferSize,
        stream_max_unacked := StreamMaxUnacked,
        consumer_persistence_interval := ConsumerPersistenceInterval,
        data_retention_period := DataRetentionPeriod
    } = _MQ
) ->
    {ok, Bin} = 'MessageQueue':encode('MQ', #'MQ'{
        id = Id,
        topicFilter = TopicFilter,
        isLastvalue = IsLastvalue,
        consumerMaxInactive = ConsumerMaxInactive,
        pingInterval = PingInterval,
        redispatchInterval = RedispatchInterval,
        dispatchStrategy = DispatchStrategy,
        localMaxInflight = LocalMaxInflight,
        busySessionRetryInterval = BusySessionRetryInterval,
        streamMaxBufferSize = StreamMaxBufferSize,
        streamMaxUnacked = StreamMaxUnacked,
        consumerPersistenceInterval = ConsumerPersistenceInterval,
        dataRetentionPeriod = DataRetentionPeriod
    }),
    Bin.

pmap_decode_val(?pn_shard_progress, _Key, ValBin) ->
    case 'MessageQueue':decode('ShardProgress', ValBin) of
        {ok, #'ShardProgress'{
            progress =
                {active, #'ActiveShardProgress'{
                    generation = Generation,
                    stream = StreamBin,
                    bufferProgress = #'StreamBufferProgress'{
                        it = ItBin,
                        lastMessageId = LastMessageIdASN1,
                        unacked = Unacked
                    }
                }}
        }} ->
            {ok, Stream} = emqx_ds:binary_to_stream(?DB, StreamBin),
            {ok, It} = emqx_ds:binary_to_iterator(?DB, ItBin),
            #{
                status => active,
                generation => Generation,
                stream => Stream,
                buffer_progress => #{
                    it => It,
                    last_message_id => from_optional_integer(LastMessageIdASN1),
                    unacked => [maps:from_keys(Unacked, true)]
                }
            };
        {ok, #'ShardProgress'{
            progress = {finished, #'FinishedShardProgress'{generation = Generation}}
        }} ->
            #{status => finished, generation => Generation}
    end;
pmap_decode_val(?pn_mq, ?mq_key, ValBin) ->
    {ok, #'MQ'{
        id = Id,
        topicFilter = TopicFilter,
        isLastvalue = IsLastvalue,
        consumerMaxInactive = ConsumerMaxInactive,
        pingInterval = PingInterval,
        redispatchInterval = RedispatchInterval,
        dispatchStrategy = DispatchStrategy,
        localMaxInflight = LocalMaxInflight,
        busySessionRetryInterval = BusySessionRetryInterval,
        streamMaxBufferSize = StreamMaxBufferSize,
        streamMaxUnacked = StreamMaxUnacked,
        consumerPersistenceInterval = ConsumerPersistenceInterval,
        dataRetentionPeriod = DataRetentionPeriod
    }} = 'MessageQueue':decode('MQ', ValBin),
    #{
        id => Id,
        topic_filter => TopicFilter,
        is_lastvalue => IsLastvalue,
        consumer_max_inactive => ConsumerMaxInactive,
        ping_interval => PingInterval,
        redispatch_interval => RedispatchInterval,
        dispatch_strategy => DispatchStrategy,
        local_max_inflight => LocalMaxInflight,
        busy_session_retry_interval => BusySessionRetryInterval,
        stream_max_buffer_size => StreamMaxBufferSize,
        stream_max_unacked => StreamMaxUnacked,
        consumer_persistence_interval => ConsumerPersistenceInterval,
        data_retention_period => DataRetentionPeriod
    }.

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------

-spec new_consumer_state(emqx_mq_types:mq()) -> consumer_state().
new_consumer_state(MQ) ->
    #{
        id => consumer_state_id(MQ),
        mq => MQ,
        ?collection_dirty => true,
        ?collection_guard => undefined,
        ?col_shard_progress => emqx_ds_pmap:new_pmap(?MODULE, ?pn_shard_progress)
    }.

-spec open_consumer_state_tx(id()) -> consumer_state().
open_consumer_state_tx(Id) ->
    case emqx_ds_pmap:tx_guard(Id) of
        undefined ->
            undefined;
        Guard ->
            Rec = #{
                id => Id,
                ?collection_guard => Guard,
                ?collection_dirty => false,
                ?col_shard_progress => emqx_ds_pmap:tx_restore(?MODULE, ?pn_shard_progress, Id)
            },
            Rec
    end.

persist_consumer_state_tx(
    NeedClaimOwnership,
    #{
        id := Id,
        ?collection_guard := OldGuard,
        ?col_shard_progress := ShardProgress
    } = CSRec
) ->
    emqx_ds_pmap:tx_assert_guard(Id, OldGuard),
    NeedClaimOwnership andalso emqx_ds_pmap:tx_write_guard(Id, ?ds_tx_serial),
    CSRec#{
        ?col_shard_progress := emqx_ds_pmap:tx_commit(Id, ShardProgress),
        ?collection_dirty := false
    }.

-spec new_mq_state(emqx_mq_types:mq()) -> mq_state().
new_mq_state(MQ) ->
    #{
        id => mq_state_id(MQ),
        mq => MQ,
        ?collection_dirty => true,
        ?collection_guard => undefined,
        ?col_mq => emqx_ds_pmap:new_pmap(?MODULE, ?pn_mq)
    }.

-spec put_mq(emqx_mq_types:mq(), mq_state()) -> mq_state().
put_mq(MQ, MQStateRec) ->
    maps:fold(
        fun(Key, Val, AccMQStateRec) ->
            emqx_ds_pmap:collection_put(?col_mq, Key, Val, AccMQStateRec)
        end,
        MQStateRec,
        MQ
    ).

-spec open_mq_state_tx(id()) -> {ok, mq_state()} | undefined.
open_mq_state_tx(Id) ->
    case emqx_ds_pmap:tx_guard(Id) of
        undefined ->
            undefined;
        Guard ->
            #{
                id => Id,
                ?collection_guard => Guard,
                ?collection_dirty => false,
                ?col_mq => emqx_ds_pmap:tx_restore(?MODULE, ?pn_mq, Id)
            }
    end.

persist_mq_state_tx(#{id := Id, ?col_mq := MQPmap} = MQStateRec) ->
    MQStateRec#{
        ?col_mq := emqx_ds_pmap:tx_commit(Id, MQPmap),
        ?collection_dirty := false
    }.

trans_opts(Id) ->
    #{
        db => ?DB,
        shard => emqx_ds:shard_of(?DB, Id),
        generation => 1
    }.

consumer_state_id(#{id := Id}) ->
    <<"c-", Id/binary>>.

mq_state_id(#{id := Id}) ->
    <<"m-", Id/binary>>;
mq_state_id(Id) when is_binary(Id) ->
    <<"m-", Id/binary>>.

to_optional_integer(undefined) ->
    asn1_NOVALUE;
to_optional_integer(Integer) ->
    Integer.

from_optional_integer(asn1_NOVALUE) ->
    undefined;
from_optional_integer(Integer) ->
    Integer.
