%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mq_consumer_db_serialization).

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
encode_claim({ConsumerRef, LastSeenTimestamp} = _Claim) when
    is_pid(ConsumerRef) andalso is_integer(LastSeenTimestamp)
->
    {ok, Bin} = 'ConsumerClaim':encode('ConsumerClaim', #'ConsumerClaim'{
        consumerRef = pack_consumer_ref(ConsumerRef),
        lastSeenTimestamp = LastSeenTimestamp
    }),
    Bin.

-spec decode_claim(binary()) -> emqx_mq_consumer_db:claim().
decode_claim(Bin) ->
    {ok, #'ConsumerClaim'{
        consumerRef = ConsumerRefBin,
        lastSeenTimestamp = LastSeenTimestamp
    }} = 'ConsumerClaim':decode('ConsumerClaim', Bin),
    {unpack_consumer_ref(ConsumerRefBin), LastSeenTimestamp}.

-spec decode_consumer_data(binary()) -> emqx_mq_types:consumer_data().
decode_consumer_data(Bin) ->
    {ok, #'ConsumerState'{
        progress = #'Progress'{
            generationProgress = GenerationProgress,
            streamProgress = StreamProgress
        }
    }} = 'ConsumerState':decode('ConsumerState', Bin),
    #{
        progress => #{
            generation_progress => unpack_generation_progress(GenerationProgress),
            streams_progress => unpack_streams_progress(StreamProgress)
        }
    }.

-spec encode_consumer_data(emqx_mq_types:consumer_data()) -> binary().
encode_consumer_data(#{
    progress := #{
        generation_progress := GenerationProgress,
        streams_progress := StreamProgress
    }
}) ->
    {ok, Bin} = 'ConsumerState':encode('ConsumerState', #'ConsumerState'{
        progress = #'Progress'{
            generationProgress = pack_generation_progress(GenerationProgress),
            streamProgress = pack_streams_progress(StreamProgress)
        }
    }),
    Bin.

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
                progress := #{
                    it := It,
                    last_message_id := LastMessageId,
                    unacked := Unacked
                }
            }}
        ) ->
            #'StreamProgress'{
                shard = Shard,
                generation = Generation,
                stream = pack_stream(Stream),
                bufferProgress = #'StreamBufferProgress'{
                    it = pack_it(It),
                    lastMessageId = pack_last_message_id(LastMessageId),
                    unacked = Unacked
                }
            }
        end,
        maps:to_list(StreamProgress)
    ).

pack_last_message_id(undefined) ->
    asn1_NOVALUE;
pack_last_message_id(MessageId) when is_integer(MessageId) ->
    MessageId.

pack_consumer_ref(ConsumerRef) ->
    term_to_binary(ConsumerRef).

%% TODO
%% how to encode streams and iterators having in mind that they are
%% opaque terms?

pack_it(It) ->
    term_to_binary(It).

pack_stream(Stream) ->
    term_to_binary(Stream).

unpack_streams_progress(StreamProgress) ->
    lists:foldl(
        fun(
            #'StreamProgress'{
                shard = Shard,
                generation = Generation,
                stream = StreamPacked,
                bufferProgress = #'StreamBufferProgress'{
                    it = ItPacked,
                    lastMessageId = LastMessageIdPacked,
                    unacked = Unacked
                }
            },
            Acc
        ) ->
            Acc#{
                unpack_stream(StreamPacked) => #{
                    slab => {Shard, Generation},
                    progress => #{
                        it => unpack_it(ItPacked),
                        last_message_id => unpack_last_message_id(LastMessageIdPacked),
                        unacked => Unacked
                    }
                }
            }
        end,
        #{},
        StreamProgress
    ).

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
    binary_to_term(ItBin).

unpack_stream(StreamBin) ->
    binary_to_term(StreamBin).
