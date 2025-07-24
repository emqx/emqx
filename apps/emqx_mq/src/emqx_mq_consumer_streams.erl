%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mq_consumer_streams).

-moduledoc """
The module represents a consumer of all streams of a single Message Queue.
""".

%% NOTE
%% This is mostly a stub working with a single generation.
%% Most of the logic of stream advancement is and discovery
%% should be done by future `emqx_ds_client`

-include("emqx_mq_internal.hrl").
-include_lib("emqx_durable_storage/include/emqx_ds.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-export([
    new/2,
    new/3,
    get_data/1,
    renew_streams/1,
    handle_ds_reply/2,
    handle_ack/2,
    info/1
]).

-type t() :: #{
    mq_topic := binary(),
    options := map(),
    streams := #{
        emqx_ds:slab() => #{
            sub_ref := undefined | reference(),
            stream := emqx_ds:stream(),
            consumer := undefined | emqx_mq_consumer_stream:t()
        }
    }
}.

-export_type([t/0]).

-spec new(emqx_mq_types:mq_topic(), emqx_mq_types:consumer_data()) -> t().
new(MQTopic, ConsumerData) ->
    new(MQTopic, ConsumerData, #{}).

-spec new(emqx_mq_types:mq_topic(), emqx_mq_types:consumer_data(), map()) -> t().
new(MQTopic, ConsumerData, Options) ->
    State = #{
        options => Options,
        mq_topic => MQTopic,
        streams => #{},
        %% TODO
        %% use ConsumerData to initialize the streams
        %% While this module is a stub, we do not use it.
        consumer_data => ConsumerData
    },
    renew_streams(State).

-spec get_data(t()) -> emqx_mq_types:consumer_data().
get_data(#{consumer_data := ConsumerData}) ->
    ConsumerData.

-spec renew_streams(t()) -> t().
renew_streams(#{mq_topic := MQTopic} = State) ->
    Streams = emqx_mq_payload_db:get_streams(MQTopic),
    add_streams(State, Streams).

-spec handle_ds_reply(t(), #ds_sub_reply{}) ->
    {ok, [{emqx_ds:slab(), emqx_mq_types:message()}], t()}.
handle_ds_reply(State, #ds_sub_reply{ref = SubRef} = DsReply) ->
    case find_stream(State, SubRef) of
        {ok, Slab, StreamData} ->
            do_handle_ds_reply(State, Slab, StreamData, DsReply);
        not_found ->
            {ok, [], State}
    end.

-spec handle_ack(t(), emqx_mq_types:message_id()) -> t().
handle_ack(#{streams := Streams} = State, {Slab, StreamMessageId}) ->
    case Streams of
        #{Slab := #{consumer := SC} = StreamData} ->
            case emqx_mq_consumer_stream:handle_ack(SC, StreamMessageId) of
                {ok, SC1} ->
                    State#{streams => Streams#{Slab => StreamData#{consumer => SC1}}};
                finished ->
                    State#{
                        streams => Streams#{
                            Slab => StreamData#{consumer => undefined, sub_ref => undefined}
                        }
                    }
            end;
        _ ->
            State
    end.

-spec info(t()) -> map().
info(#{streams := Streams}) ->
    lists:foldl(
        fun({Slab, #{consumer := SC}}, Acc) ->
            Acc#{Slab => emqx_mq_consumer_stream:info(SC)}
        end,
        #{},
        maps:to_list(Streams)
    ).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

do_handle_ds_reply(#{streams := Streams} = State, Slab, #{consumer := SC} = StreamData, DSReply) ->
    case emqx_mq_consumer_stream:handle_ds_reply(SC, DSReply) of
        {ok, Messages0, SC1} ->
            Messages = [
                {{Slab, StreamMessageId}, Payload}
             || {_Topic, StreamMessageId, Payload} <- Messages0
            ],
            {ok, Messages, State#{streams => Streams#{Slab => StreamData#{consumer => SC1}}}};
        finished ->
            {ok, [], State#{
                streams => Streams#{
                    Slab => StreamData#{consumer => undefined, sub_ref => undefined}
                }
            }}
    end.

add_streams(State, Streams) ->
    lists:foldl(
        fun({Slab, Stream}, StateAcc) ->
            add_stream(StateAcc, Slab, Stream)
        end,
        State,
        Streams
    ).

add_stream(#{streams := Streams} = State, Slab, Stream) ->
    case Streams of
        #{Slab := _} ->
            State;
        _ ->
            do_add_stream(State, Slab, Stream)
    end.

do_add_stream(#{mq_topic := MQTopic, options := Options, streams := Streams} = State, Slab, Stream) ->
    ?tp(warning, emqx_mq_consumer_streams_add_stream, #{mq_topic => MQTopic, slab => Slab}),
    {ok, It} = emqx_mq_payload_db:make_iterator(Stream, MQTopic),
    {ok, SubRef, SC} = emqx_mq_consumer_stream:new(It, Options),
    State#{
        streams => Streams#{
            Slab => #{
                sub_ref => SubRef,
                stream => Stream,
                consumer => SC
            }
        }
    }.

find_stream(#{streams := Streams}, SubRef) ->
    do_find_stream(maps:to_list(Streams), SubRef).

do_find_stream([{Slab, StreamData} | Rest], SubRef) ->
    case StreamData of
        #{sub_ref := SubRef} ->
            {ok, Slab, StreamData};
        _ ->
            do_find_stream(Rest, SubRef)
    end;
do_find_stream([], _) ->
    not_found.
