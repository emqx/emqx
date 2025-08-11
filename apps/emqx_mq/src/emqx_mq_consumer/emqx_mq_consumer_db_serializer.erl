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
        consumer_ref = ConsumerRef, last_seen_timestamp = LastSeenTimestamp, tombstone = Tombstone
    } = _Claim
) when
    is_pid(ConsumerRef) andalso is_integer(LastSeenTimestamp)
->
    {ok, Bin} = 'ConsumerClaim':encode(
        'ConsumerClaim',
        {v1, #'ConsumerClaimV1'{
            consumerRef = pack_consumer_ref(ConsumerRef),
            lastSeenTimestamp = LastSeenTimestamp,
            tombstone = Tombstone
        }}
    ),
    Bin.

-spec decode_claim(binary()) -> emqx_mq_consumer_db:claim().
decode_claim(Bin) ->
    {ok,
        {v1, #'ConsumerClaimV1'{
            consumerRef = ConsumerRefBin,
            lastSeenTimestamp = LastSeenTimestamp,
            tombstone = Tombstone
        }}} = 'ConsumerClaim':decode('ConsumerClaim', Bin),
    #claim{
        consumer_ref = unpack_consumer_ref(ConsumerRefBin),
        last_seen_timestamp = LastSeenTimestamp,
        tombstone = Tombstone
    }.

-spec encode_consumer_data(emqx_mq_types:consumer_data()) -> binary().
encode_consumer_data(#{
    progress := #{
        generation_progress := GenerationProgress,
        streams_progress := StreamProgress
    }
}) ->
    {ok, Bin} = 'ConsumerState':encode(
        'ConsumerState',
        {v1, #'ConsumerStateV1'{
            progress = #'Progress'{
                generationProgress = pack_generation_progress(GenerationProgress),
                streamProgress = pack_streams_progress(StreamProgress)
            }
        }}
    ),
    Bin.

-spec decode_consumer_data(binary()) -> emqx_mq_types:consumer_data().
decode_consumer_data(Bin) ->
    {ok,
        {v1, #'ConsumerStateV1'{
            progress = #'Progress'{
                generationProgress = GenerationProgress,
                streamProgress = StreamProgress
            }
        }}} = 'ConsumerState':decode('ConsumerState', Bin),
    #{
        progress => #{
            generation_progress => unpack_generation_progress(GenerationProgress),
            streams_progress => unpack_streams_progress(StreamProgress)
        }
    }.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

pack_generation_progress(GenerationProgress) ->
    lists:map(
        fun({Shard, Generation}) ->
            #'GenerationProgress'{
                shard = Shard,
                generation = Generation
            }
        end,
        maps:to_list(GenerationProgress)
    ).

pack_streams_progress(StreamProgress) ->
    lists:map(
        fun(
            {Stream, #{
                slab := {Shard, Generation},
                progress := BufferProgress
            }}
        ) ->
            #'StreamProgress'{
                shard = Shard,
                generation = Generation,
                stream = pack_stream(Stream),
                bufferProgress = pack_buffer_progress(BufferProgress)
            }
        end,
        maps:to_list(StreamProgress)
    ).

pack_last_message_id(undefined) ->
    asn1_NOVALUE;
pack_last_message_id(MessageId) when is_integer(MessageId) ->
    MessageId.

pack_buffer_progress(finished) ->
    {finished, asn1_NOVALUE};
pack_buffer_progress(#{it := It, last_message_id := LastMessageId, unacked := Unacked}) ->
    {active, #'ActiveStreamBufferProgress'{
        it = pack_it(It),
        lastMessageId = pack_last_message_id(LastMessageId),
        unacked = Unacked
    }}.

pack_consumer_ref(ConsumerRef) ->
    term_to_binary(ConsumerRef).

pack_it(It) ->
    {ok, ItBin} = emqx_ds:iterator_to_binary(?MQ_CONSUMER_DB, It),
    ItBin.

pack_stream(Stream) ->
    {ok, StreamBin} = emqx_ds:stream_to_binary(?MQ_CONSUMER_DB, Stream),
    StreamBin.

unpack_streams_progress(StreamProgress) ->
    lists:foldl(
        fun(
            #'StreamProgress'{
                shard = Shard,
                generation = Generation,
                stream = StreamPacked,
                bufferProgress = PackedBufferProgress
            },
            Acc
        ) ->
            Acc#{
                unpack_stream(StreamPacked) => #{
                    slab => {Shard, Generation},
                    progress => unpack_buffer_progress(PackedBufferProgress)
                }
            }
        end,
        #{},
        StreamProgress
    ).

unpack_buffer_progress({finished, _}) ->
    finished;
unpack_buffer_progress(
    {active, #'ActiveStreamBufferProgress'{
        it = ItPacked,
        lastMessageId = LastMessageIdPacked,
        unacked = Unacked
    }}
) ->
    #{
        it => unpack_it(ItPacked),
        last_message_id => unpack_last_message_id(LastMessageIdPacked),
        unacked => Unacked
    }.

unpack_generation_progress(GenerationProgress) ->
    lists:foldl(
        fun(
            #'GenerationProgress'{
                shard = Shard,
                generation = Generation
            },
            Acc
        ) ->
            Acc#{Shard => Generation}
        end,
        #{},
        GenerationProgress
    ).

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
