%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mq_db).

-include("emqx_mq_internal.hrl").
-include_lib("emqx_durable_storage/include/emqx_ds.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-export([
    open/0
]).

%% Debuggging functions, till we fully implement the MQ
-export([
    start_generator/2,
    stop_generator/1,
    read_all/1,
    get_streams/1,
    get_iterators/1,
    run_consumer/2,
    start_consumer/2,
    stop_consumer/1
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

start_generator(MQTopic, NKeys) ->
    Pid = spawn_link(fun() ->
        generate_messages(MQTopic, NKeys, 1)
    end),
    Pid.

stop_generator(Pid) ->
    Pid ! stop.

start_consumer(MQTopic, Options) ->
    Pid = spawn_link(fun() ->
        run_consumer(MQTopic, Options)
    end),
    Pid.

stop_consumer(Pid) ->
    Pid ! stop.

read_all(MQTopic) ->
    emqx_ds:dirty_read(?MQ_PAYLOAD_DB, ?MQ_PAYLOAD_DB_TOPIC(MQTopic, '#')).

get_streams(MQTopic) ->
    emqx_ds:get_streams(?MQ_PAYLOAD_DB, ?MQ_PAYLOAD_DB_TOPIC(MQTopic, '#'), 0).

get_iterators(MQTopic) ->
    lists:map(
        fun({Slab, Stream}) ->
            {ok, It} = emqx_ds:make_iterator(
                ?MQ_PAYLOAD_DB, Stream, ?MQ_PAYLOAD_DB_TOPIC(MQTopic, '#'), 0
            ),
            {Slab, It}
        end,
        get_streams(MQTopic)
    ).

run_consumer(MQTopic, Options) ->
    CSs = emqx_mq_consumer_streams:new(MQTopic, Options),
    loop_consumer(CSs).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

loop_consumer(CSs) ->
    receive
        #ds_sub_reply{} = DSReply ->
            {ok, Messages, CSs1} = emqx_mq_consumer_streams:handle_ds_reply(CSs, DSReply),
            ?tp(warning, mq_consumer_stream_handle_ds_reply, #{
                messages => Messages,
                cs => emqx_mq_consumer_streams:info(CSs1)
            }),
            CSs2 = ack_messages(CSs1, Messages),
            loop_consumer(CSs2);
        stop ->
            ?tp(warning, mq_consumer_stream_stop, #{}),
            ok
    end.

ack_messages(CSs0, [{Slab, {_Topic, MessageId, _Payload}} | Rest]) ->
    CSs1 = emqx_mq_consumer_streams:handle_ack(CSs0, Slab, MessageId),
    ack_messages(CSs1, Rest);
ack_messages(CSs, []) ->
    CSs.

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

generate_messages(MQTopic, NKeys, I) ->
    receive
        stop ->
            ok
    after 500 ->
        CompactionKey = key(rand:uniform(NKeys)),
        Payload = <<"dummy message ", (integer_to_binary(I))/binary>>,
        TxOpts = #{
            db => ?MQ_PAYLOAD_DB,
            shard => {auto, CompactionKey},
            generation => 1,
            sync => true,
            retries => ?MQ_PAYLOAD_DB_APPEND_RETRY
        },
        Topic = ?MQ_PAYLOAD_DB_TOPIC(MQTopic, CompactionKey),
        TxResult = emqx_ds:trans(TxOpts, fun() ->
            emqx_ds:tx_del_topic(Topic),
            emqx_ds:tx_write({Topic, ?ds_tx_ts_monotonic, Payload})
        end),
        case TxResult of
            {atomic, _Serial, _} ->
                % ?tp(warning, mq_db_append_ok, #{
                %     topic => Topic, payload => Payload, serial => Serial
                % }),
                ok;
            {error, IsRecoverable, Reason} ->
                ?tp(warning, mq_db_append_error, #{
                    is_recoverable => IsRecoverable, reason => Reason
                })
        end,
        generate_messages(MQTopic, NKeys, I + 1)
    end.

key(N) ->
    <<"key", (integer_to_binary(N))/binary>>.
