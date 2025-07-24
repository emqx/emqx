%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mq_consumer_db).

-include("emqx_mq_internal.hrl").
-include_lib("emqx_durable_storage/include/emqx_ds.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-export([
    open/0
]).

-export([
    claim_leadership/3,
    find_consumer/2,
    update_consumer_data/4,
    drop_leadership/1
]).

-type mq_topic() :: emqx_mq_types:mq_topic().
-type consumer_ref() :: emqx_mq_types:consumer_ref().
-type timestamp() :: emqx_message:timestamp().
-type consumer_data() :: emqx_mq_types:consumer_data().

-define(MQ_CONSUMER_DB, mq_consumer).
-define(MQ_CONSUMER_DB_LTS_SETTINGS, #{
    %% "topic/TOPIC/INFO_KEY"
    lts_threshold_spec => {simple, {100, 0, 1000}}
}).
-define(TOPIC_LEADERSHIP(MQ_TOPIC), [
    <<"topic">>, MQ_TOPIC, <<"leader">>
]).
-define(TOPIC_CONSUMER_DATA(MQ_TOPIC), [
    <<"topic">>, MQ_TOPIC, <<"data">>
]).
-define(SHARDS_PER_SITE, 4).
-define(REPLICATION_FACTOR, 3).
-define(LEADER_TTL, 30_000).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

open() ->
    Result = emqx_ds:open_db(?MQ_CONSUMER_DB, settings()),
    ?tp(warning, mq_db_open, #{db => ?MQ_CONSUMER_DB, result => Result}),
    Result.

-spec claim_leadership(mq_topic(), consumer_ref(), timestamp()) ->
    {ok, consumer_data()} | emqx_ds:error(term()).
claim_leadership(MQTopic, ConsumerRef, TS) ->
    TxOpts = tx_opts(MQTopic),
    LeadershipTopic = ?TOPIC_LEADERSHIP(MQTopic),
    DataTopic = ?TOPIC_CONSUMER_DATA(MQTopic),
    Claim = {ConsumerRef, TS},
    TxRes = emqx_ds:trans(TxOpts, fun() ->
        case read_claim(LeadershipTopic) of
            not_found ->
                ok = write_claim(LeadershipTopic, Claim),
                {ok, read_or_init_consumer_data(DataTopic)};
            {ok, {ConsumerRef, _TS}} ->
                %% This should never happen, a process does not call `claim_leadership` twice
                {error, unrecoverable, claim_self};
            {ok, {_ConsumerRef, OldTS}} when OldTS < TS - ?LEADER_TTL ->
                %% Outdated claim, we can replace it
                ok = write_claim(LeadershipTopic, Claim),
                {ok, read_or_init_consumer_data(DataTopic)};
            {ok, {ExistingConsumerRef, _OldTS}} ->
                {error, unrecoverable, {active_consumer, ExistingConsumerRef}}
        end
    end),
    get_result(TxRes).

-spec find_consumer(mq_topic(), timestamp()) ->
    {ok, consumer_ref()} | not_found.
find_consumer(MQTopic, TS) ->
    LeadershipTopic = ?TOPIC_LEADERSHIP(MQTopic),
    case emqx_ds:dirty_read(LeadershipTopic, LeadershipTopic) of
        [] ->
            not_found;
        [{_Topic, 0, ClaimBin}] ->
            case decode_claim(ClaimBin) of
                {ConsumerRef, OldTS} when OldTS > TS - ?LEADER_TTL ->
                    {ok, ConsumerRef};
                _ ->
                    not_found
            end
    end.

-spec update_consumer_data(mq_topic(), consumer_ref(), consumer_data(), timestamp()) ->
    ok | emqx_ds:error(term()).
update_consumer_data(MQTopic, ConsumerRef, ConsumerData, TS) ->
    TxOpts = tx_opts(MQTopic),
    LeadershipTopic = ?TOPIC_LEADERSHIP(MQTopic),
    DataTopic = ?TOPIC_CONSUMER_DATA(MQTopic),
    TxRes = emqx_ds:trans(TxOpts, fun() ->
        case read_claim(LeadershipTopic) of
            {ok, {ConsumerRef, _TS}} ->
                ok = write_claim(LeadershipTopic, {ConsumerRef, TS}),
                ok = write_consumer_data(DataTopic, ConsumerData);
            {ok, {OtherConsumer, _OldTS}} ->
                {error, unrecoverable, {claim_lost, OtherConsumer}};
            not_found ->
                {error, unrecoverable, claim_disappeared};
            Error ->
                Error
        end
    end),
    get_result(TxRes).

-spec drop_leadership(mq_topic()) ->
    ok | emqx_ds:error(term()).
drop_leadership(MQTopic) ->
    TxOpts = tx_opts(MQTopic),
    LeadershipTopic = ?TOPIC_LEADERSHIP(MQTopic),
    TxRes = emqx_ds:trans(TxOpts, fun() ->
        ok = delete_claim(LeadershipTopic)
    end),
    get_result(TxRes).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

init_consumer_data() ->
    #{}.

read_claim(LeadershipTopic) ->
    case emqx_ds:tx_read(LeadershipTopic) of
        [] ->
            not_found;
        [{_Topic, 0, ClaimBin}] ->
            {ok, decode_claim(ClaimBin)}
    end.

write_claim(LeadershipTopic, Claim) ->
    emqx_ds:tx_write({LeadershipTopic, 0, encode_claim(Claim)}).

delete_claim(LeadershipTopic) ->
    emqx_ds:tx_del_topic(LeadershipTopic).

read_or_init_consumer_data(DataTopic) ->
    case emqx_ds:tx_read(DataTopic) of
        [] ->
            InitData = init_consumer_data(),
            ok = write_consumer_data(DataTopic, InitData),
            InitData;
        [{_Topic, 0, DataBin}] ->
            decode_consumer_data(DataBin)
    end.

write_consumer_data(DataTopic, Data) ->
    emqx_ds:tx_write({DataTopic, 0, encode_consumer_data(Data)}).

%% TODO
%% better serialize/deserialize
decode_claim(ClaimBin) ->
    binary_to_term(ClaimBin).

encode_claim(Claim) ->
    term_to_binary(Claim).

decode_consumer_data(DataBin) ->
    binary_to_term(DataBin).

encode_consumer_data(Data) ->
    term_to_binary(Data).

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
            {emqx_ds_storage_skipstream_lts_v2, ?MQ_CONSUMER_DB_LTS_SETTINGS},
        store_ttv => true,
        backend => builtin_raft,
        n_shards => NSites * ?SHARDS_PER_SITE,
        replication_options => #{},
        n_sites => NSites,
        replication_factor => ?REPLICATION_FACTOR
    }.

tx_opts(MQTopic) ->
    #{
        db => ?MQ_CONSUMER_DB,
        shard => {auto, MQTopic},
        generation => 1,
        sync => true,
        retries => 5
    }.

get_result(TxRes) ->
    case TxRes of
        {atomic, _Serial, Res} -> Res;
        {nop, Res} -> Res;
        Error -> Error
    end.
