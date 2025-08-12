%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mq_consumer_streams).

-behaviour(emqx_ds_client).

-moduledoc """
The module holds a stream_buffers for all streams of a single Message Queue.
""".

-include("../emqx_mq_internal.hrl").
-include_lib("emqx_durable_storage/include/emqx_ds.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-export([
    new/2,
    progress/1,
    handle_ds_info/2,
    handle_ack/2,
    info/1
]).

-export([
    get_current_generation/3,
    on_advance_generation/4,
    get_iterator/4,
    on_new_iterator/5,
    on_unrecoverable_error/5,
    on_subscription_down/4
]).

-type generation_progress() :: #{
    emqx_ds:shard() => emqx_ds:generation()
}.
-type streams_progress() :: #{
    emqx_ds:stream() => #{
        slab := emqx_ds:slab(),
        progress := emqx_mq_consumer_stream_buffer:progress()
    }
}.

-type progress() :: #{
    generation_progress := generation_progress(),
    streams_progress := streams_progress()
}.

-type state() :: #{
    mq := emqx_mq_types:mq(),
    streams := #{
        emqx_ds:stream() => #{
            stream_buffer := undefined | emqx_mq_consumer_stream_buffer:t(),
            slab := emqx_ds:slab()
        }
    },
    streams_by_slab := #{
        emqx_ds:slab() => emqx_ds:stream()
    },
    progress := generation_progress(),
    ds_client := emqx_ds_client:t()
}.

-record(cs, {
    state :: state(),
    ds_client :: emqx_ds_client:t()
}).

-type t() :: #cs{}.

-export_type([t/0]).

%% We create only one subscription in the `emqx_ds_client`.
-define(SUB_ID, []).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec new(emqx_mq_types:mq(), progress()) -> t().
new(MQ, Progress) ->
    StreamsProgress = maps:get(streams_progress, Progress, #{}),
    GenerationProgress = maps:get(generation_progress, Progress, #{}),
    State0 = #{
        mq => MQ,
        streams => #{},
        streams_by_slab => #{},
        progress => GenerationProgress
    },
    State1 = restore_streams(State0, StreamsProgress),
    DSClient0 = emqx_mq_message_db:create_client(?MODULE),
    {ok, DSClient, State} = emqx_mq_message_db:subscribe(MQ, DSClient0, ?SUB_ID, State1),
    #cs{state = State, ds_client = DSClient}.

-spec progress(t()) -> progress().
progress(#cs{state = #{progress := GenerationProgress, streams := Streams}}) ->
    StreamProgress = maps:fold(
        fun(Stream, #{stream_buffer := SB, slab := {Shard, StreamGeneration} = Slab}, ProgressAcc) ->
            case GenerationProgress of
                #{Shard := Generation} when StreamGeneration < Generation ->
                    ProgressAcc;
                _ ->
                    case SB of
                        undefined ->
                            ProgressAcc#{
                                Stream => #{
                                    slab => Slab,
                                    progress => finished
                                }
                            };
                        _ ->
                            ?tp(warning, emqx_mq_consumer_streams_progress_stream, #{
                                slab => Slab, sb => buffer_info(SB)
                            }),
                            ProgressAcc#{
                                Stream => #{
                                    slab => Slab,
                                    progress => emqx_mq_consumer_stream_buffer:progress(SB)
                                }
                            }
                    end
            end
        end,
        #{},
        Streams
    ),
    #{
        generation_progress => GenerationProgress,
        streams_progress => StreamProgress
    }.

-spec handle_ds_info(t(), term()) ->
    {ok, [{emqx_mq_types:message_id(), emqx_types:message()}], t()}.
handle_ds_info(#cs{ds_client = DSC0, state = State0} = CS0, GenericMessage) ->
    Res = emqx_ds_client:dispatch_message(GenericMessage, DSC0, State0),
    ?tp(warning, emqx_mq_consumer_streams_handle_ds_info, #{res => Res, info => GenericMessage}),
    case Res of
        ignore ->
            ignore;
        {data, ?SUB_ID, Stream, Handle, DSReply} ->
            {ok, Messages, CS} = handle_ds_reply(CS0, Stream, Handle, DSReply),
            {ok, Messages, CS};
        {DSC, State} ->
            {ok, [], CS0#cs{ds_client = DSC, state = State}}
    end.

-spec handle_ack(t(), emqx_mq_types:message_id()) -> t().
handle_ack(
    #cs{state = #{streams_by_slab := StreamsBySlab, streams := Streams} = State} = CS,
    {Slab, StreamMessageId} = MessagesId
) ->
    ?tp(warning, emqx_mq_consumer_streams_handle_ack, #{
        message_id => MessagesId
    }),
    maybe
        #{Slab := Stream} ?= StreamsBySlab,
        #{Stream := #{stream_buffer := SB} = StreamData} ?= Streams,
        case emqx_mq_consumer_stream_buffer:handle_ack(SB, StreamMessageId) of
            {ok, SB1} ->
                CS#cs{
                    state = State#{streams => Streams#{Stream => StreamData#{stream_buffer => SB1}}}
                };
            finished ->
                CS#cs{
                    state = State#{
                        streams => Streams#{Stream => StreamData#{stream_buffer => undefined}}
                    }
                }
        end
    else
        _ -> CS
    end.

-spec info(t()) -> map().
info(#cs{state = #{streams := Streams}}) ->
    maps:fold(
        fun(_Stream, #{stream_buffer := SB, slab := Slab}, Acc) ->
            Acc#{Slab => buffer_info(SB)}
        end,
        #{},
        Streams
    ).

%%--------------------------------------------------------------------
%% emqx_ds_client callbacks
%%--------------------------------------------------------------------

get_current_generation(?SUB_ID, Shard, #{progress := GenerationProgress}) ->
    Result =
        case GenerationProgress of
            #{Shard := Generation} ->
                Generation;
            _ ->
                0
        end,
    ?tp(warning, emqx_mq_consumer_streams_get_current_generation, #{
        shard => Shard, result => Result
    }),
    Result.

on_advance_generation(
    ?SUB_ID, Shard, Generation, #{progress := GenerationProgress} = State
) ->
    ?tp(warning, emqx_mq_consumer_streams_on_advance_generation, #{
        shard => Shard, generation => Generation
    }),
    State#{progress => GenerationProgress#{Shard => Generation}}.

get_iterator(?SUB_ID, Slab, Stream, #{streams := Streams}) ->
    Result =
        case Streams of
            #{Stream := #{stream_buffer := undefined}} ->
                {ok, end_of_stream};
            #{Stream := #{stream_buffer := SB}} ->
                case emqx_mq_consumer_stream_buffer:iterator(SB) of
                    end_of_stream ->
                        {ok, end_of_stream};
                    It ->
                        {subscribe, It}
                end;
            _ ->
                undefined
        end,
    ?tp(warning, emqx_mq_consumer_streams_get_iterator, #{
        slab => Slab, stream => Stream, result => Result
    }),
    Result.

on_new_iterator(
    ?SUB_ID,
    Slab,
    Stream,
    It,
    #{mq := MQ, streams := Streams, streams_by_slab := StreamsBySlab} =
        State
) ->
    ?tp(warning, emqx_mq_consumer_streams_on_new_iterator, #{slab => Slab, stream => Stream}),
    StreamData = #{
        stream_buffer => emqx_mq_consumer_stream_buffer:new(It, MQ), slab => Slab
    },
    {subscribe, State#{
        streams => Streams#{Stream => StreamData},
        streams_by_slab => StreamsBySlab#{Slab => Stream}
    }}.

on_unrecoverable_error(
    ?SUB_ID, Slab, Stream, Error, #{mq := #{topic_filter := MQTopic}, streams := Streams} = State
) ->
    ?tp(error, emqx_mq_consumer_streams_unrecoverable_error, #{
        mq_topic => MQTopic, slab => Slab, stream => Stream, error => Error
    }),
    maybe
        #{Stream := StreamData} ?= Streams,
        %% We consider this stream as finished.
        State#{
            streams => Streams#{Stream => StreamData#{stream_buffer => undefined}}
        }
    else
        _ -> State
    end.

on_subscription_down(?SUB_ID, Slab, Stream, State) ->
    ?tp(warning, emqx_mq_consumer_streams_subscription_down, #{slab => Slab, stream => Stream}),
    %% TODO
    %% Handle gracefully
    State.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

restore_streams(State, StreamsProgress) ->
    maps:fold(
        fun(Stream, #{slab := Slab, progress := StreamProgress}, StateAcc) ->
            restore_stream(StateAcc, Slab, Stream, StreamProgress)
        end,
        State,
        StreamsProgress
    ).

restore_stream(
    #{streams := Streams, streams_by_slab := StreamsBySlab, mq := MQ} =
        State,
    Slab,
    Stream,
    StreamProgress
) ->
    StreamData =
        case StreamProgress of
            finished ->
                #{stream_buffer => undefined, slab => Slab};
            _ ->
                #{
                    stream_buffer => emqx_mq_consumer_stream_buffer:restore(
                        StreamProgress, MQ
                    ),
                    slab => Slab
                }
        end,
    State#{
        streams => Streams#{Stream => StreamData}, streams_by_slab => StreamsBySlab#{Slab => Stream}
    }.

handle_ds_reply(#cs{state = #{streams := Streams}} = CS, Stream, Handle, DSReply) ->
    case Streams of
        #{Stream := StreamData} ->
            ?tp(warning, emqx_mq_consumer_streams_handle_ds_reply_stream_exists, #{stream => Stream}),
            do_handle_ds_reply(CS, Stream, StreamData, Handle, DSReply);
        _ ->
            ?tp(warning, emqx_mq_consumer_streams_handle_ds_reply_stream_not_exists, #{
                stream => Stream
            }),
            {ok, [], CS}
    end.

do_handle_ds_reply(
    #cs{state = #{streams := Streams0} = State0, ds_client = DSC0} = CS,
    Stream,
    #{stream_buffer := SB, slab := Slab} = StreamData,
    Handle,
    #ds_sub_reply{ref = SRef} = DSReply
) ->
    ?tp(warning, emqx_mq_consumer_streams_do_handle_ds_reply, #{
        slab => Slab, stream => Stream, handle => Handle, ds_reply => DSReply
    }),
    case emqx_mq_consumer_stream_buffer:handle_ds_reply(SB, Handle, DSReply) of
        {ok, Messages0, SB1} ->
            ?tp(warning, emqx_mq_consumer_streams_do_handle_ds_reply_ok, #{
                slab => Slab, sb => buffer_info(SB1)
            }),
            Messages = [
                {{Slab, StreamMessageId}, emqx_mq_message_db:decode_message(Payload)}
             || {_Topic, StreamMessageId, Payload} <- Messages0
            ],
            Streams = Streams0#{Stream => StreamData#{stream_buffer => SB1}},
            State = State0#{streams => Streams},
            {ok, Messages, CS#cs{state = State}};
        finished ->
            ?tp(warning, emqx_mq_consumer_streams_do_handle_ds_reply_finished, #{
                slab => Slab
            }),
            ?tp(
                warning,
                emqx_mq_consumer_streams_do_handle_ds_reply_finished_complete_stream_started,
                #{
                    slab => Slab
                }
            ),
            {DSC, #{streams := Streams1} = State1} = emqx_ds_client:complete_stream(
                DSC0, SRef, State0
            ),
            State = State1#{
                streams => Streams1#{
                    Stream => StreamData#{stream_buffer => undefined, sub_ref => undefined}
                }
            },
            {ok, [], CS#cs{ds_client = DSC, state = State}}
    end.

buffer_info(undefined) ->
    undefined;
buffer_info(SB) ->
    emqx_mq_consumer_stream_buffer:info(SB).
