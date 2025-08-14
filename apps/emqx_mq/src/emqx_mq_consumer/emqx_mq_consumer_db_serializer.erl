%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mq_consumer_db_serializer).

-moduledoc """
This module provides serialization/deserialization into/from binary of
    - emqx_mq_consumer_db:claim()
    - emqx_mq_types:consumer_data()
""".

-include("../emqx_mq_internal.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("../../gen_src/ConsumerClaim.hrl").
-include_lib("../../gen_src/ConsumerState.hrl").

-export([
    encode_claim/1,
    decode_claim/1,
    encode_consumer_data/1,
    decode_consumer_data/1
]).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec encode_claim(emqx_mq_consumer_db:claim()) -> binary().
encode_claim(
    #claim{
        consumer_ref = ConsumerRef, last_seen_timestamp = LastSeenTimestamp
    } = _Claim
) when
    is_pid(ConsumerRef) andalso is_integer(LastSeenTimestamp)
->
    {ok, Bin} = 'ConsumerClaim':encode(
        'ConsumerClaim',
        {v1,
            {live, #'ConsumerClaimLive'{
                consumerRef = pack_consumer_ref(ConsumerRef),
                lastSeenTimestamp = LastSeenTimestamp
            }}}
    ),
    Bin;
encode_claim(#tombstone{last_seen_timestamp = LastSeenTimestamp}) ->
    {ok, Bin} = 'ConsumerClaim':encode(
        'ConsumerClaim',
        {v1, {tombstone, #'ConsumerClaimTombstone'{lastSeenTimestamp = LastSeenTimestamp}}}
    ),
    Bin.

-spec decode_claim(binary()) -> emqx_mq_consumer_db:claim().
decode_claim(Bin) ->
    {ok, {v1, ClaimPacked}} = 'ConsumerClaim':decode('ConsumerClaim', Bin),
    case ClaimPacked of
        {live, #'ConsumerClaimLive'{
            consumerRef = ConsumerRefBin, lastSeenTimestamp = LastSeenTimestamp
        }} ->
            #claim{
                consumer_ref = unpack_consumer_ref(ConsumerRefBin),
                last_seen_timestamp = LastSeenTimestamp
            };
        {tombstone, #'ConsumerClaimTombstone'{lastSeenTimestamp = LastSeenTimestamp}} ->
            #tombstone{
                last_seen_timestamp = LastSeenTimestamp
            }
    end.

-spec encode_consumer_data(emqx_mq_types:consumer_data()) -> binary().
encode_consumer_data(#{
    progress := ShardProgress
}) ->
    ConsumerState =
        {v1, #'ConsumerStateV1'{
            progress = lists:map(fun pack_shard_progress/1, maps:to_list(ShardProgress))
        }},
    ?tp(warning, mq_consumer_db_encode_consumer_data, #{consumer_state => ConsumerState}),
    {ok, Bin} = 'ConsumerState':encode('ConsumerState', ConsumerState),
    Bin.

-spec decode_consumer_data(binary()) -> emqx_mq_types:consumer_data().
decode_consumer_data(Bin) ->
    {ok,
        {v1, #'ConsumerStateV1'{
            progress = ProgressPacked
        }}} = 'ConsumerState':decode('ConsumerState', Bin),
    #{
        progress => maps:from_list(
            lists:map(fun unpack_shard_progress/1, ProgressPacked)
        )
    }.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

pack_shard_progress(
    {Shard, #{
        status := active,
        generation := Generation,
        stream := Stream,
        buffer_progress := BufferProgress
    }}
) ->
    #'ShardProgress'{
        shard = Shard,
        progress =
            {active, #'ActiveShardProgress'{
                generation = Generation,
                stream = pack_stream(Stream),
                bufferProgress = pack_buffer_progress(BufferProgress)
            }}
    };
pack_shard_progress({Shard, #{status := finished, generation := Generation}}) ->
    #'ShardProgress'{
        shard = Shard,
        progress = {finished, #'FinishedShardProgress'{generation = Generation}}
    }.

pack_last_message_id(undefined) ->
    asn1_NOVALUE;
pack_last_message_id(MessageId) when is_integer(MessageId) ->
    MessageId.

pack_buffer_progress(#{it := It, last_message_id := LastMessageId, unacked := Unacked}) ->
    #'StreamBufferProgress'{
        it = pack_it(It),
        lastMessageId = pack_last_message_id(LastMessageId),
        unacked = Unacked
    }.

pack_consumer_ref(ConsumerRef) ->
    term_to_binary(ConsumerRef).

pack_it(It) ->
    {ok, ItBin} = emqx_ds:iterator_to_binary(?MQ_CONSUMER_DB, It),
    ItBin.

pack_stream(Stream) ->
    {ok, StreamBin} = emqx_ds:stream_to_binary(?MQ_CONSUMER_DB, Stream),
    StreamBin.

unpack_shard_progress(#'ShardProgress'{
    shard = Shard,
    progress =
        {active, #'ActiveShardProgress'{
            generation = Generation, stream = StreamPacked, bufferProgress = PackedBufferProgress
        }}
}) ->
    {Shard, #{
        status => active,
        generation => Generation,
        stream => unpack_stream(StreamPacked),
        buffer_progress => unpack_buffer_progress(PackedBufferProgress)
    }};
unpack_shard_progress(#'ShardProgress'{
    shard = Shard, progress = {finished, #'FinishedShardProgress'{generation = Generation}}
}) ->
    {Shard, #{
        status => finished,
        generation => Generation
    }}.

unpack_buffer_progress(
    #'StreamBufferProgress'{
        it = ItPacked,
        lastMessageId = LastMessageIdPacked,
        unacked = Unacked
    }
) ->
    #{
        it => unpack_it(ItPacked),
        last_message_id => unpack_last_message_id(LastMessageIdPacked),
        unacked => Unacked
    }.

unpack_consumer_ref(ConsumerRefBin) ->
    binary_to_term(ConsumerRefBin).

unpack_last_message_id(asn1_NOVALUE) ->
    undefined;
unpack_last_message_id(MessageId) ->
    MessageId.

unpack_it(ItBin) ->
    {ok, It} = emqx_ds:binary_to_iterator(?MQ_CONSUMER_DB, ItBin),
    It.

unpack_stream(StreamBin) ->
    {ok, Stream} = emqx_ds:binary_to_stream(?MQ_CONSUMER_DB, StreamBin),
    Stream.
