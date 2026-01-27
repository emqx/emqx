%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mq_state_storage).

-include_lib("emqx_durable_storage/include/emqx_ds_pmap.hrl").
-include_lib("emqx_durable_storage/include/emqx_ds.hrl").
-include("../gen_src/MessageQueue.hrl").
-include_lib("emqx_utils/include/emqx_ds_dbs.hrl").

-moduledoc """
Persistence of Message queue state:
* MQ properties
* MQ consumption state
""".

%% ASN1-generated struct field names are in camelCase.
-elvis([{elvis_style, atom_naming_convention, #{regex => "^[a-z]\\w*$"}}]).

-behaviour(emqx_ds_pmap).

-export([
    open_db/0,
    close_db/0,
    wait_readiness/1
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
    update_mq_state/2,
    destroy_mq_state/1,
    find_mq/1
]).

-export([
    set_name_index/2,
    find_id_by_name/1,
    destroy_name_index/2
]).

%% For tests/maintenance.
-export([
    delete_all/0,
    mq_ids/0
]).

-export([
    pmap_encode_key/3,
    pmap_decode_key/2,
    pmap_encode_val/3,
    pmap_decode_val/3
]).

-define(DB, ?MQ_STATE_DB).

-type id() :: binary().

%% col_* are collection field names
%% pn_* are pmap names
-define(pn_shard_progress, <<"shp">>).
-define(col_shard_progress, col_shard_progress).
-define(pn_mq, <<"mq">>).
-define(pn_id, <<"id">>).

%% pmap keys
-define(mq_key, mq_key).
-define(mq_key_bin, <<"mq_key">>).

-define(id_key, id_key).
-define(id_key_bin, <<"id_key">>).

-define(mq_id_prefix, "m-").
-define(consumer_id_prefix, "c-").
-define(name_id_prefix, "n-").

-type consumer_state() :: #{
    id := id(),
    ?collection_dirty := boolean(),
    ?collection_guard := emqx_ds_pmap:guard() | undefined,
    ?col_shard_progress := emqx_ds_pmap:pmap(
        emqx_ds:shard(), emqx_mq_consumer_streams:shard_progress()
    )
}.

-export_type([
    consumer_state/0
]).

-define(STATE_DELETE_RETRY, 1).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

open_db() ->
    Config = emqx_ds_schema:db_config_mq_states(),
    ok = emqx_ds:open_db(?DB, Config#{
        storage => emqx_ds_pmap:storage_opts(#{})
    }).

close_db() ->
    emqx_ds:close_db(?DB).

-spec wait_readiness(timeout()) -> ok | timeout.
wait_readiness(Timeout) ->
    emqx_ds:wait_db(?DB, all, Timeout).

%%------------------------------------------------------------------------------
%% Consumer State API
%%------------------------------------------------------------------------------

-spec open_consumer_state(emqx_mq_types:mq()) -> {ok, consumer_state()} | {error, term()}.
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
    format_tx_result(TxRes, failed_to_open_consumer_state).

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

-spec commit_consumer_state(boolean(), consumer_state()) ->
    {ok, consumer_state()} | {error, term()}.
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

-spec destroy_consumer_state(emqx_mq_types:mq_handle()) -> ok | {error, term()}.
destroy_consumer_state(MQHandle) ->
    Id = consumer_state_id(MQHandle),
    TxRes = emqx_ds:trans(
        trans_opts(Id, #{retries => ?STATE_DELETE_RETRY}),
        fun() ->
            emqx_ds_pmap:tx_delete_guard(Id),
            emqx_ds_pmap:tx_destroy(Id, ?pn_shard_progress)
        end
    ),
    format_tx_result(TxRes, failed_to_destroy_consumer_state).

%%------------------------------------------------------------------------------
%% MQ State API
%%------------------------------------------------------------------------------

-spec create_mq_state(emqx_mq_types:mq()) -> ok | {error, term()}.
create_mq_state(MQ) ->
    Id = mq_state_id(MQ),
    TransOpts = trans_opts(Id),
    TxRes = emqx_ds:trans(
        TransOpts,
        fun() ->
            case emqx_ds_pmap:tx_guard(Id) of
                undefined ->
                    Pmap0 = emqx_ds_pmap:new_pmap(?MODULE, ?pn_mq),
                    Pmap = emqx_ds_pmap:put(?mq_key, MQ, Pmap0),
                    emqx_ds_pmap:tx_write_guard(Id, ?ds_tx_serial),
                    emqx_ds_pmap:tx_commit(Id, Pmap),
                    ok;
                _Guard ->
                    {error, {mq_state_already_exists, Id}}
            end,
            ok
        end
    ),
    format_tx_result(TxRes, failed_to_create_mq_state).

-spec update_mq_state(emqx_mq_types:mqid(), map()) ->
    {ok, emqx_mq_types:mq()} | not_found | {error, term()}.
update_mq_state(MQId, MQFields) ->
    Id = mq_state_id(MQId),
    TransOpts = trans_opts(Id),
    TxRes = emqx_ds:trans(
        TransOpts,
        fun() ->
            case emqx_ds_pmap:tx_guard(Id) of
                undefined ->
                    not_found;
                _Guard ->
                    Pmap0 = emqx_ds_pmap:tx_restore(?MODULE, ?pn_mq, Id),
                    MQ0 = emqx_ds_pmap:get(?mq_key, Pmap0),
                    MQ = maps:merge(MQ0, MQFields),
                    Pmap = emqx_ds_pmap:put(?mq_key, MQ, Pmap0),
                    emqx_ds_pmap:tx_write_guard(Id, ?ds_tx_serial),
                    emqx_ds_pmap:tx_commit(Id, Pmap),
                    {ok, MQ}
            end
        end
    ),
    format_tx_result(TxRes, failed_to_update_mq_state).

-spec find_mq(emqx_mq_types:mqid()) -> {ok, emqx_mq_types:mq()} | not_found.
find_mq(MQId) ->
    Id = mq_state_id(MQId),
    FoldOptions = maps:merge(trans_opts(Id), #{errors => ignore}),
    case emqx_ds_pmap:dirty_read(?MODULE, ?pn_mq, Id, FoldOptions) of
        #{?mq_key := MQ} ->
            {ok, MQ};
        #{} ->
            not_found
    end.

-spec destroy_mq_state(emqx_mq_types:mq_handle()) -> ok | {error, term()}.
destroy_mq_state(MQ) ->
    Id = mq_state_id(MQ),
    TxRes = emqx_ds:trans(
        trans_opts(Id, #{retries => ?STATE_DELETE_RETRY}),
        fun() ->
            emqx_ds_pmap:tx_delete_guard(Id),
            emqx_ds_pmap:tx_destroy(Id, ?pn_mq)
        end
    ),
    format_tx_result(TxRes, failed_to_destroy_mq_state).

%%------------------------------------------------------------------------------
%% Name Index API
%%------------------------------------------------------------------------------

-spec set_name_index(emqx_mq_types:mq_name(), emqx_mq_types:mqid()) ->
    ok | {error, term()}.
set_name_index(Name, MQId) ->
    Id = name_id(Name),
    TransOpts = trans_opts(Id),
    TxRes = emqx_ds:trans(
        TransOpts,
        fun() ->
            case emqx_ds_pmap:tx_guard(Id) of
                undefined ->
                    Pmap0 = emqx_ds_pmap:new_pmap(?MODULE, ?pn_id),
                    Pmap = emqx_ds_pmap:put(?id_key, MQId, Pmap0),
                    emqx_ds_pmap:tx_write_guard(Id, ?ds_tx_serial),
                    emqx_ds_pmap:tx_commit(Id, Pmap),
                    ok;
                _Guard ->
                    Pmap0 = emqx_ds_pmap:tx_restore(?MODULE, ?pn_id, Id),
                    case emqx_ds_pmap:get(?id_key, Pmap0) of
                        MQId ->
                            ok;
                        _ ->
                            {error, {name_index_already_exists, Name}}
                    end
            end
        end
    ),
    format_tx_result(TxRes, failed_to_update_name_index_state).

-spec find_id_by_name(binary()) -> {ok, emqx_mq_types:mqid()} | not_found.
find_id_by_name(Name) ->
    FoldOptions = maps:merge(trans_opts(Name), #{errors => ignore}),
    case emqx_ds_pmap:dirty_read(?MODULE, ?pn_id, Name, FoldOptions) of
        #{?id_key := Id} ->
            {ok, Id};
        #{} ->
            not_found
    end.

-spec destroy_name_index(emqx_mq_types:mq_name(), emqx_mq_types:mqid()) -> ok | {error, term()}.
destroy_name_index(Name, MQId) ->
    Id = name_id(Name),
    TxRes = emqx_ds:trans(
        trans_opts(Id),
        fun() ->
            case emqx_ds_pmap:tx_guard(Id) of
                undefined ->
                    {error, {name_index_not_found, Name}};
                _Guard ->
                    Pmap = emqx_ds_pmap:tx_restore(?MODULE, ?pn_id, Id),
                    case emqx_ds_pmap:get(?id_key, Pmap) of
                        MQId ->
                            emqx_ds_pmap:tx_delete_guard(Id),
                            emqx_ds_pmap:tx_destroy(Id, ?pn_id);
                        _ ->
                            ok
                    end
            end
        end
    ),
    format_tx_result(TxRes, failed_to_destroy_name_index_state).

%%------------------------------------------------------------------------------
%% Utility functions
%%------------------------------------------------------------------------------

-spec delete_all() -> ok.
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

-spec mq_ids() -> [emqx_mq_types:mqid()].
mq_ids() ->
    emqx_ds:fold_topic(
        fun({Topic, _, _}, Acc) ->
            case Topic of
                ?guard_topic(<<?mq_id_prefix, MQId/binary>>) ->
                    [MQId | Acc];
                _ ->
                    Acc
            end
        end,
        [],
        ?guard_topic('+'),
        #{
            db => ?DB
        }
    ).

%%------------------------------------------------------------------------------
%% emqx_ds_pmap behaviour
%%------------------------------------------------------------------------------

pmap_encode_key(?pn_shard_progress, Shard = _Key, _Val) when is_binary(Shard) ->
    Shard;
pmap_encode_key(?pn_mq, ?mq_key, _Val) ->
    ?mq_key_bin;
pmap_encode_key(?pn_id, ?id_key, _Val) ->
    ?id_key_bin.

pmap_decode_key(?pn_shard_progress, Shard = _KeyBin) ->
    Shard;
pmap_decode_key(?pn_mq, ?mq_key_bin) ->
    ?mq_key;
pmap_decode_key(?pn_id, ?id_key_bin) ->
    ?id_key.

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
                        lastMessageId = to_asn1_optional_integer(LastMessageId),
                        unacked = maps:keys(emqx_utils_maps:merge(Unacked))
                    }
                }}
        },
    'MessageQueue':encode('ShardProgress', Rec);
pmap_encode_val(?pn_shard_progress, _Key, #{
    status := finished,
    generation := Generation
}) ->
    Rec = #'ShardProgress'{
        progress = {finished, #'FinishedShardProgress'{generation = Generation}}
    },
    'MessageQueue':encode('ShardProgress', Rec);
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
        data_retention_period := DataRetentionPeriod,
        limits := Limits
    } = MQ
) ->
    KeyExpression = maps:get(key_expression, MQ, undefined),
    Name = maps:get(name, MQ, undefined),
    'MessageQueue':encode('MQ', #'MQ'{
        id = Id,
        topicFilter = TopicFilter,
        isLastvalue = IsLastvalue,
        consumerMaxInactive = ConsumerMaxInactive,
        pingInterval = PingInterval,
        redispatchInterval = RedispatchInterval,
        dispatchStrategy = atom_to_binary(DispatchStrategy),
        localMaxInflight = LocalMaxInflight,
        busySessionRetryInterval = BusySessionRetryInterval,
        streamMaxBufferSize = StreamMaxBufferSize,
        streamMaxUnacked = StreamMaxUnacked,
        consumerPersistenceInterval = ConsumerPersistenceInterval,
        dataRetentionPeriod = DataRetentionPeriod,
        keyExpression = key_expression_to_asn1(KeyExpression),
        limits = limits_to_asn1(Limits),
        name = name_to_asn1(Name)
    });
pmap_encode_val(?pn_id, ?id_key, MQId) ->
    MQId.

pmap_decode_val(?pn_shard_progress, _Key, ValBin) ->
    case 'MessageQueue':decode('ShardProgress', ValBin) of
        #'ShardProgress'{
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
        } ->
            {ok, Stream} = emqx_ds:binary_to_stream(?DB, StreamBin),
            {ok, It} = emqx_ds:binary_to_iterator(?DB, ItBin),
            #{
                status => active,
                generation => Generation,
                stream => Stream,
                buffer_progress => #{
                    it => It,
                    last_message_id => from_asn1_optional_integer(LastMessageIdASN1),
                    unacked => [maps:from_keys(Unacked, true)]
                }
            };
        #'ShardProgress'{
            progress = {finished, #'FinishedShardProgress'{generation = Generation}}
        } ->
            #{status => finished, generation => Generation}
    end;
pmap_decode_val(?pn_mq, ?mq_key, ValBin) ->
    #'MQ'{
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
        dataRetentionPeriod = DataRetentionPeriod,
        keyExpression = KeyExpression,
        limits = Limits,
        name = Name
    } = 'MessageQueue':decode('MQ', ValBin),
    MQ0 = #{
        id => Id,
        topic_filter => TopicFilter,
        is_lastvalue => IsLastvalue,
        consumer_max_inactive => ConsumerMaxInactive,
        ping_interval => PingInterval,
        redispatch_interval => RedispatchInterval,
        dispatch_strategy => binary_to_existing_atom(DispatchStrategy),
        local_max_inflight => LocalMaxInflight,
        busy_session_retry_interval => BusySessionRetryInterval,
        stream_max_buffer_size => StreamMaxBufferSize,
        stream_max_unacked => StreamMaxUnacked,
        consumer_persistence_interval => ConsumerPersistenceInterval,
        data_retention_period => DataRetentionPeriod,
        limits => limits_from_asn1(Limits)
    },
    MQ = key_expression_from_asn1(MQ0, KeyExpression),
    name_from_asn1(MQ, Name);
pmap_decode_val(?pn_id, ?id_key, IdKeyBin) ->
    IdKeyBin.

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------

-spec new_consumer_state(emqx_mq_types:mq()) -> consumer_state().
new_consumer_state(MQ) ->
    #{
        id => consumer_state_id(MQ),
        ?collection_dirty => true,
        ?collection_guard => undefined,
        ?col_shard_progress => emqx_ds_pmap:new_pmap(?MODULE, ?pn_shard_progress)
    }.

-spec open_consumer_state_tx(id()) -> consumer_state() | undefined.
open_consumer_state_tx(Id) ->
    case emqx_ds_pmap:tx_guard(Id) of
        undefined ->
            undefined;
        Guard ->
            #{
                id => Id,
                ?collection_guard => Guard,
                ?collection_dirty => false,
                ?col_shard_progress => emqx_ds_pmap:tx_restore(?MODULE, ?pn_shard_progress, Id)
            }
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

trans_opts(Id) ->
    trans_opts(Id, #{}).

trans_opts(Id, Opts) ->
    maps:merge(
        #{
            db => ?DB,
            shard => emqx_ds:shard_of(?DB, Id),
            generation => 1
        },
        Opts
    ).

consumer_state_id(#{id := Id}) ->
    <<?consumer_id_prefix, Id/binary>>.

mq_state_id(#{id := Id}) ->
    mq_state_id(Id);
mq_state_id(Id) when is_binary(Id) ->
    <<?mq_id_prefix, Id/binary>>.

name_id(#{name := Name}) ->
    name_id(Name);
name_id(Name) when is_binary(Name) ->
    <<?name_id_prefix, Name/binary>>.

to_asn1_optional_integer(undefined) ->
    asn1_NOVALUE;
to_asn1_optional_integer(Integer) when is_integer(Integer) ->
    Integer.

from_asn1_optional_integer(asn1_NOVALUE) ->
    undefined;
from_asn1_optional_integer(Integer) when is_integer(Integer) ->
    Integer.

key_expression_from_asn1(MQ, asn1_NOVALUE) ->
    MQ;
key_expression_from_asn1(MQ, KeyExpression) ->
    {ok, KeyExpressionCompiled} = emqx_variform:compile(KeyExpression),
    MQ#{key_expression => KeyExpressionCompiled}.

key_expression_to_asn1(undefined) ->
    asn1_NOVALUE;
key_expression_to_asn1(KeyExpression) ->
    emqx_variform:decompile(KeyExpression).

name_to_asn1(undefined) ->
    asn1_NOVALUE;
name_to_asn1(Name) ->
    Name.

name_from_asn1(MQ, asn1_NOVALUE) ->
    MQ;
name_from_asn1(MQ, Name) ->
    MQ#{name => Name}.

limit_from_asn1_optional_integer(asn1_NOVALUE) ->
    infinity;
limit_from_asn1_optional_integer(Integer) when is_integer(Integer) ->
    Integer.

limit_to_asn1_optional_integer(infinity) ->
    asn1_NOVALUE;
limit_to_asn1_optional_integer(Integer) when is_integer(Integer) ->
    Integer.

limits_from_asn1(asn1_NOVALUE) ->
    #{
        max_shard_message_count => infinity,
        max_shard_message_bytes => infinity
    };
limits_from_asn1(#'Limits'{
    maxShardMessageCount = MaxShardMessageCount,
    maxShardMessageBytes = MaxShardMessageBytes
}) ->
    #{
        max_shard_message_count => limit_from_asn1_optional_integer(MaxShardMessageCount),
        max_shard_message_bytes => limit_from_asn1_optional_integer(MaxShardMessageBytes)
    }.

limits_to_asn1(#{
    max_shard_message_count := MaxShardMessageCount,
    max_shard_message_bytes := MaxShardMessageBytes
}) ->
    #'Limits'{
        maxShardMessageCount = limit_to_asn1_optional_integer(MaxShardMessageCount),
        maxShardMessageBytes = limit_to_asn1_optional_integer(MaxShardMessageBytes)
    }.

format_tx_result({atomic, _TXSerial, Res}, _ErrorLabel) ->
    Res;
format_tx_result({nop, Res}, _ErrorLabel) ->
    Res;
format_tx_result(Err, ErrorLabel) ->
    {error, {ErrorLabel, Err}}.
