%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mq_consumer_streams).

-behaviour(emqx_ds_client).

-moduledoc """
The module holds a stream_buffers for all streams of a single Message Queue.
""".

-include("../emqx_mq_internal.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx_durable_storage/include/emqx_ds.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-export([
    new/2,
    fetch_progress_updates/1,
    handle_ds_info/2,
    handle_ack/2,
    destroy/1,
    attach_cleaner/2,
    flush_progress_reporters/1,
    inspect/1
]).

-export([
    get_current_generation/3,
    on_advance_generation/4,
    get_iterator/4,
    on_new_iterator/5,
    on_unrecoverable_error/5,
    on_subscription_down/4,
    list_known_streams/2
]).

%%--------------------------------------------------------------------
%% Types for persistence of the state
%%--------------------------------------------------------------------

-type shard_progress() ::
    #{
        status := active,
        generation := emqx_ds:generation(),
        buffer_progress := emqx_mq_consumer_stream_buffer:progress(),
        stream := emqx_ds:stream()
    }
    | #{
        status := finished,
        generation := emqx_ds:generation()
    }.

-type progress() :: #{
    emqx_ds:shard() => shard_progress()
}.

-export_type([shard_progress/0, progress/0]).

%%--------------------------------------------------------------------
%% Runtime state
%%--------------------------------------------------------------------

-type shard_state() ::
    #{
        status := active,
        generation := emqx_ds:generation(),
        stream_buffer := emqx_mq_consumer_stream_buffer:t(),
        %% Reports acknowledgment progress of the stream buffer to the cleaner
        progress_reporter := emqx_mq_consumer_progress_reporter:t(),
        stream := emqx_ds:stream(),
        sub_ref := undefined | emqx_ds:sub_ref()
    }
    | #{
        status := finished,
        generation := emqx_ds:generation()
    }.

-type state() :: #{
    mq := emqx_mq_types:mq(),
    cleaner := pid() | undefined,
    %% To tie `emqx_ds_client`'s data responses to the stream buffers
    streams := #{
        emqx_ds:stream() => emqx_ds:shard()
    },
    shards := #{emqx_ds:shard() => shard_state()},
    %% Accumulated progress updates to be persisted
    progress_updates := progress()
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
    State0 = #{
        mq => MQ,
        cleaner => undefined,
        streams => #{},
        shards => #{},
        progress_updates => #{}
    },
    State1 = restore_streams(State0, Progress),
    DSClient0 = emqx_mq_message_db:create_client(?MODULE),
    {ok, DSClient, State} = emqx_mq_message_db:subscribe(MQ, DSClient0, ?SUB_ID, State1),
    #cs{state = State, ds_client = DSClient}.

-spec handle_ds_info(t(), term()) ->
    {ok, [{emqx_mq_types:message_id(), emqx_types:message()}], t()} | ignore.
handle_ds_info(#cs{ds_client = DSC0, state = State0} = CS0, GenericMessage) ->
    Res = emqx_ds_client:dispatch_message(GenericMessage, DSC0, State0),
    ?tp_debug(mq_consumer_streams_handle_ds_info, #{res => Res, info => GenericMessage}),
    case Res of
        ignore ->
            ignore;
        {data, ?SUB_ID, Stream, Handle, DSReply} ->
            {ok, Messages0, CS1} = handle_ds_reply(CS0, Stream, Handle, DSReply),
            {Messages, CS} = ack_skipped_messages(CS1, Messages0),
            {ok, Messages, CS};
        {DSC, State} ->
            {ok, [], CS0#cs{ds_client = DSC, state = State}}
    end.

attach_cleaner(#cs{state = State0} = CS, Cleaner) ->
    State = map_shard_states(State0, fun
        (#{status := active, progress_reporter := Reporter} = ShardState) ->
            ShardState#{
                progress_reporter := emqx_mq_consumer_progress_reporter:attach(Reporter, Cleaner)
            };
        (ShardState) ->
            ShardState
    end),
    CS#cs{state = State#{cleaner := Cleaner}}.

-doc "Send unreported consumption progress of all shards to the cleaner.".
-spec flush_progress_reporters(t()) -> t().
flush_progress_reporters(#cs{state = State0} = CS) ->
    State = map_shard_states(State0, fun
        (#{status := active, progress_reporter := Reporter} = ShardState) ->
            ShardState#{progress_reporter := emqx_mq_consumer_progress_reporter:flush(Reporter)};
        (ShardState) ->
            ShardState
    end),
    CS#cs{state = State}.

-spec handle_ack(t(), emqx_mq_types:message_id()) -> t().
handle_ack(
    #cs{
        state = #{shards := Shards0, streams := Streams0} = State0, ds_client = DSC0
    } = CS,
    {{Shard, Generation}, StreamMessageId} = MessagesId
) ->
    ?tp_debug(mq_consumer_streams_handle_ack, #{
        message_id => MessagesId, shards => maps:keys(Shards0)
    }),
    maybe
        #{
            Shard := #{
                status := active,
                stream_buffer := SB,
                progress_reporter := Reporter0,
                sub_ref := SubRef,
                generation := Generation,
                stream := Stream
            } = ShardState0
        } ?= Shards0,
        case emqx_mq_consumer_stream_buffer:handle_ack(SB, StreamMessageId) of
            {ok, Progress, SB1} ->
                Reporter = emqx_mq_consumer_progress_reporter:handle_progress(Reporter0, Progress),
                State1 = State0#{
                    shards => Shards0#{
                        Shard => ShardState0#{stream_buffer => SB1, progress_reporter => Reporter}
                    }
                },
                State = save_progress_update(State1, Shard),
                CS#cs{state = State};
            {finished, Progress} ->
                ok = finish_progress_reporter(Reporter0, Progress),
                Streams = maps:remove(Stream, Streams0),
                ShardState = #{status => finished, generation => Generation},
                State1 = State0#{
                    shards => Shards0#{Shard => ShardState},
                    streams => Streams
                },
                {DSC, State2} = emqx_ds_client:complete_stream(DSC0, SubRef, State1),
                State = save_progress_update(State2, Shard),
                CS#cs{state = State, ds_client = DSC}
        end
    else
        _ ->
            ?tp(error, mq_consumer_streams_handle_ack_shard_not_found, #{
                messages_id => MessagesId,
                cs => inspect(CS)
            }),
            CS
    end.

-spec fetch_progress_updates(t()) -> {progress(), t()}.
fetch_progress_updates(#cs{state = #{progress_updates := ProgressUpdates} = State} = CS) ->
    {ProgressUpdates, CS#cs{state = State#{progress_updates => #{}}}}.

-spec inspect(t()) -> map().
inspect(#cs{state = #{shards := Shards, streams := Streams}}) ->
    #{
        shards => shards_inspect(Shards),
        streams => Streams
    }.

-spec destroy(t()) -> t().
destroy(#cs{state = State, ds_client = DSC} = CS) ->
    CS#cs{state = emqx_ds_client:destroy(DSC, State)}.

%%--------------------------------------------------------------------
%% emqx_ds_client callbacks
%%--------------------------------------------------------------------

get_current_generation(?SUB_ID, Shard, #{
    shards := Shards
}) ->
    Result =
        case Shards of
            #{Shard := #{status := finished, generation := Generation}} ->
                Generation + 1;
            #{Shard := #{generation := Generation}} ->
                Generation;
            _ ->
                0
        end,
    ?tp_debug(mq_consumer_streams_get_current_generation, #{
        shard => Shard, result => Result
    }),
    Result.

on_advance_generation(
    ?SUB_ID, Shard, Generation, #{shards := Shards} = State
) ->
    ?tp_debug(mq_consumer_streams_on_advance_generation, #{
        shard => Shard, generation => Generation
    }),
    case Shards of
        #{Shard := #{status := active, generation := OldGeneration}} ->
            ?tp(error, mq_consumer_streams_on_advance_generation_error, #{
                reason => from_wrong_state,
                shard => Shard,
                shard_status => active,
                old_generation => OldGeneration,
                new_generation => Generation
            }),
            State#{
                shards => Shards#{Shard => #{status => finished, generation => Generation - 1}}
            };
        _ ->
            State#{
                shards => Shards#{Shard => #{status => finished, generation => Generation - 1}}
            }
    end.

get_iterator(?SUB_ID, {Shard, Generation}, _Stream, #{shards := Shards, mq := MQ}) ->
    StartTime = emqx_mq_prop:delivery_start_time(MQ),
    Result =
        case Shards of
            #{Shard := #{status := active, generation := Generation, stream_buffer := SB}} ->
                case emqx_mq_consumer_stream_buffer:iterator(SB) of
                    end_of_stream ->
                        %% Actually cannot happen. Active status implies that the buffer
                        %% has unacked messages, so its (starting) iterator cannot be end_of_stream.
                        {ok, end_of_stream};
                    It ->
                        case replay_start_time(emqx_mq_consumer_stream_buffer:progress(SB)) of
                            ReplayStart when ReplayStart < StartTime ->
                                {undefined, #{start_time => StartTime}};
                            _ ->
                                {subscribe, It}
                        end
                end;
            _ ->
                {undefined, #{start_time => StartTime}}
        end,
    ?tp_debug(mq_consumer_streams_get_iterator, #{
        slab => {Shard, Generation}, stream => _Stream, result => Result
    }),
    Result.

on_new_iterator(
    ?SUB_ID,
    {Shard, Generation} = Slab,
    Stream,
    It,
    #{shards := Shards, mq := MQ} = State
) ->
    ?tp_debug(mq_consumer_streams_on_new_iterator, #{
        slab => {Shard, Generation}, stream => Stream
    }),
    case Shards of
        #{
            Shard := #{
                status := active, generation := Generation, stream := Stream, stream_buffer := SB0
            } = ShardState
        } ->
            Progress = emqx_mq_consumer_stream_buffer:progress(SB0),
            SB = emqx_mq_consumer_stream_buffer:restore(Progress#{it := It}, MQ),
            {subscribe, State#{shards := Shards#{Shard := ShardState#{stream_buffer := SB}}}};
        #{Shard := #{status := finished, generation := OldGeneration}} when
            OldGeneration < Generation
        ->
            {subscribe, init_shard_state(State, Slab, Stream, It)};
        #{Shard := #{status := Status, generation := OldGeneration}} ->
            ?tp(error, mq_consumer_streams_on_new_iterator_wrong_shard_state, #{
                slab => {Shard, Generation}, old_generation => OldGeneration, status => Status
            }),
            {ignore, State};
        _ ->
            {subscribe, init_shard_state(State, Slab, Stream, It)}
    end.

on_unrecoverable_error(
    ?SUB_ID,
    {Shard, Generation},
    Stream,
    Error,
    #{mq := #{topic_filter := MQTopic}, shards := Shards, streams := Streams} = State
) ->
    ?tp(error, mq_consumer_streams_unrecoverable_error, #{
        mq_topic => MQTopic, slab => {Shard, Generation}, stream => Stream, error => Error
    }),
    case Shards of
        #{Shard := #{status := active, generation := Generation, progress_reporter := Reporter}} ->
            ok = finish_progress_reporter(Reporter, undefined),
            State#{
                shards => Shards#{Shard => #{status => finished, generation => Generation}},
                streams => maps:remove(Stream, Streams)
            };
        _ ->
            State
    end.

on_subscription_down(
    ?SUB_ID, {Shard, Generation}, Stream, #{mq := MQ, shards := Shards, streams := Streams} = State
) ->
    ?tp(error, mq_consumer_streams_subscription_down, #{
        shard => Shard, generation => Generation, stream => Stream
    }),
    case {Shards, Streams} of
        {
            #{
                Shard := #{status := active, generation := Generation, stream_buffer := SB0} =
                    ShardState
            },
            #{Stream := Shard}
        } ->
            SB = emqx_mq_consumer_stream_buffer:restore(
                emqx_mq_consumer_stream_buffer:progress(SB0), MQ
            ),
            State#{
                shards => Shards#{Shard => ShardState#{stream_buffer => SB}}
            };
        _ ->
            State
    end.

list_known_streams(?SUB_ID, #{shards := ShardStates}) ->
    [
        {{Shard, Generation}, Stream}
     || {Shard, #{status := active, generation := Generation, stream := Stream}} <-
            maps:to_list(ShardStates)
    ].

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

save_progress_update(State, Shard) ->
    #{shards := #{Shard := ShardState}, progress_updates := ProgressUpdates} = State,
    Progress = shard_progress(ShardState),
    State#{progress_updates => ProgressUpdates#{Shard => Progress}}.

replay_start_time(#{last_message_id := undefined}) ->
    0;
replay_start_time(#{last_message_id := LastMessageId, unacked := Unacked}) ->
    lists:foldl(
        fun(IDs, Earliest) ->
            maps:fold(fun(ID, _, Acc) -> min(ID, Acc) end, Earliest, IDs)
        end,
        LastMessageId + 1,
        Unacked
    ).

shard_progress(#{
    status := active, generation := Generation, stream := Stream, stream_buffer := StreamBuffer
}) ->
    #{
        status => active,
        stream => Stream,
        generation => Generation,
        buffer_progress => emqx_mq_consumer_stream_buffer:progress(StreamBuffer)
    };
shard_progress(#{status := finished, generation := Generation}) ->
    #{
        status => finished,
        generation => Generation
    }.

restore_streams(State, StreamsProgress) ->
    maps:fold(
        fun(Shard, ShardState, StateAcc) ->
            restore_stream(StateAcc, Shard, ShardState)
        end,
        State,
        StreamsProgress
    ).

restore_stream(
    #{shards := Shards, streams := Streams, mq := MQ} = State,
    Shard,
    ShardState
) ->
    %% Convert
    case ShardState of
        #{status := finished, generation := Generation} ->
            State#{
                shards => Shards#{Shard => #{status => finished, generation => Generation}}
            };
        #{
            status := active,
            generation := Generation,
            buffer_progress := BufferProgress,
            stream := Stream
        } ->
            State#{
                shards => Shards#{
                    Shard => #{
                        status => active,
                        generation => Generation,
                        stream_buffer => emqx_mq_consumer_stream_buffer:restore(
                            BufferProgress, MQ
                        ),
                        progress_reporter => new_progress_reporter(State, Shard),
                        stream => Stream,
                        sub_ref => undefined
                    }
                },
                streams => Streams#{Stream => Shard}
            }
    end.

handle_ds_reply(
    #cs{state = #{streams := Streams, shards := Shards}} = CS0,
    Stream,
    Handle,
    #ds_sub_reply{} = DSReply
) ->
    ok = inc_received_message_stat(DSReply),
    maybe
        #{Stream := Shard} ?= Streams,
        #{Shard := #{status := active} = ShardState} ?= Shards,
        ?tp_debug(mq_consumer_streams_handle_ds_reply_stream_exists, #{
            stream => Stream
        }),
        {Messages, #cs{state = State} = CS} = do_handle_ds_reply(
            CS0, Stream, Shard, ShardState, Handle, DSReply
        ),
        {ok, Messages, CS#cs{state = save_progress_update(State, Shard)}}
    else
        _ ->
            ?tp_debug(mq_consumer_streams_handle_ds_reply_stream_not_exists, #{
                stream => Stream
            }),
            {ok, [], CS0}
    end.

do_handle_ds_reply(
    #cs{state = #{shards := Shards0} = State0, ds_client = DSC0} = CS,
    _Stream,
    Shard,
    #{stream_buffer := SB, progress_reporter := Reporter0, generation := Generation} = ShardState,
    Handle,
    #ds_sub_reply{ref = SRef} = DSReply
) ->
    Slab = {Shard, Generation},
    ?tp_debug(mq_consumer_streams_do_handle_ds_reply, #{
        slab => Slab, stream => _Stream, handle => Handle, ds_reply => DSReply
    }),
    case emqx_mq_consumer_stream_buffer:handle_ds_reply(SB, Handle, DSReply) of
        {ok, Messages0, Progress, SB1} ->
            ?tp_debug(mq_consumer_streams_do_handle_ds_reply_ok, #{
                slab => Slab, sb => emqx_mq_consumer_stream_buffer:inspect(SB1)
            }),
            Messages = [
                {
                    {Slab, StreamMessageId},
                    restore_timestamp(StreamMessageId, emqx_mq_message_db:decode_message(Payload))
                }
             || {_Topic, StreamMessageId, Payload} <- Messages0
            ],
            Reporter = emqx_mq_consumer_progress_reporter:handle_progress(Reporter0, Progress),
            Shards = Shards0#{
                Shard => ShardState#{
                    stream_buffer => SB1, progress_reporter => Reporter, sub_ref => SRef
                }
            },
            State = State0#{shards => Shards},
            {Messages, CS#cs{state = State}};
        {finished, Progress} ->
            ok = finish_progress_reporter(Reporter0, Progress),
            State1 = State0#{
                shards => Shards0#{
                    Shard => #{status => finished, generation => Generation}
                }
            },
            {DSC, #{shards := _Shards1} = State} = emqx_ds_client:complete_stream(
                DSC0, SRef, State1
            ),
            {[], CS#cs{ds_client = DSC, state = State}}
    end.

restore_timestamp(TsUs, #message{} = Message) ->
    Message#message{timestamp = erlang:convert_time_unit(TsUs, microsecond, millisecond)}.

ack_skipped_messages(#cs{state = #{mq := MQ}} = CS0, Messages0) ->
    {CS, Messages} = lists:foldl(
        fun({Id, Message} = MesssageWithId, {CSAcc, MessagesAcc}) ->
            case is_expired(MQ, Message) orelse is_before_deliver_cutoff(MQ, Message) of
                true ->
                    {handle_ack(CSAcc, Id), MessagesAcc};
                false ->
                    {CSAcc, [MesssageWithId | MessagesAcc]}
            end
        end,
        {CS0, []},
        Messages0
    ),
    {lists:reverse(Messages), CS}.

init_shard_state(
    #{shards := Shards, streams := Streams, mq := MQ} = State,
    {Shard, Generation} = _Slab,
    Stream,
    It
) ->
    ShardState = #{
        status => active,
        generation => Generation,
        stream_buffer => emqx_mq_consumer_stream_buffer:new(It, MQ),
        progress_reporter => new_progress_reporter(State, Shard),
        sub_ref => undefined,
        stream => Stream
    },
    State#{
        shards => Shards#{Shard => ShardState},
        streams => Streams#{Stream => Shard}
    }.

new_progress_reporter(#{mq := MQ, cleaner := Cleaner}, Shard) ->
    emqx_mq_consumer_progress_reporter:new(MQ, Shard, Cleaner).

map_shard_states(#{shards := Shards} = State, Fun) ->
    State#{shards := maps:map(fun(_Shard, ShardState) -> Fun(ShardState) end, Shards)}.

%% The progress reporter is dropped with a finished stream, so send its unreported progress.
finish_progress_reporter(Reporter, Progress) ->
    _ = emqx_mq_consumer_progress_reporter:flush(
        emqx_mq_consumer_progress_reporter:handle_progress(Reporter, Progress)
    ),
    ok.

shards_inspect(Shards) ->
    maps:map(fun(_Shard, ShardState) -> shard_inspect(ShardState) end, Shards).

shard_inspect(#{status := active, stream_buffer := SB} = ShardState) ->
    ShardState#{
        stream_buffer => emqx_mq_consumer_stream_buffer:inspect(SB)
    };
shard_inspect(ShardState) ->
    ShardState.

inc_received_message_stat(#ds_sub_reply{payload = {ok, _It, TTVs}}) ->
    emqx_mq_metrics:inc(ds, received_messages, length(TTVs));
inc_received_message_stat(_DsSubReply) ->
    ok.

is_expired(#{data_retention_period := DataRetentionPeriod} = _MQ, Message) when
    is_integer(DataRetentionPeriod)
->
    emqx_message:timestamp(Message) < now_ms() - DataRetentionPeriod.

is_before_deliver_cutoff(#{deliver_cutoff_date := CutoffDate}, Message) ->
    emqx_message:timestamp(Message) < CutoffDate;
is_before_deliver_cutoff(_MQ, _Message) ->
    false.

now_ms() ->
    erlang:system_time(millisecond).
