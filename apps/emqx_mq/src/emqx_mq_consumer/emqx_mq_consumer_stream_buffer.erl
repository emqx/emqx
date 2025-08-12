%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mq_consumer_stream_buffer).

-moduledoc """
The module represents a consumer of a single stream of the Message Queue data.
""".

-include("../emqx_mq_internal.hrl").
-include_lib("emqx_durable_storage/include/emqx_ds.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-export([
    new/1,
    new/2,
    restore/2,
    handle_ds_reply/3,
    handle_ack/2,
    progress/1,
    iterator/1,
    info/1
]).

%%--------------------------------------------------------------------
%% Types
%%--------------------------------------------------------------------

-type options() :: #{
    max_buffer_size := non_neg_integer()
}.

-type message_id() :: non_neg_integer().

-type buffer() :: #{
    it_begin := emqx_ds:iterator() | end_of_stream,
    it_end := emqx_ds:iterator() | end_of_stream,
    n := non_neg_integer(),
    unacked := #{message_id() => true},
    actual_unacked := #{message_id() => true}
}.

%% State of the active stream buffer, receiving messages from DS and dispatching them to the consumer
-type st_active() :: #{
    status := active,
    options := options(),
    lower_buffer := buffer(),
    upper_buffer := undefined | buffer(),
    upper_seqno := undefined | {emqx_ds:sub_ref(), emqx_ds:sub_seqno()},
    last_message_id := undefined | message_id()
}.

%% State of the stream buffer when restoring from previously saved state
%% The buffer is receiving and accumulating messages from the DS,
%% but not dispatching them to the consumer.
%% After successfully restoring the buffer, the state is transitioned to st_active.
-type st_restoring() :: #{
    status := restoring,
    options := options(),
    it_begin := emqx_ds:iterator(),
    unacked := #{message_id() => true},
    last_message_id := message_id(),
    messages := [emqx_ds:ttv()]
}.

-type t() :: st_active() | st_restoring().

-type progress() ::
    end_of_stream
    | #{
        it := emqx_ds:iterator(),
        last_message_id := message_id(),
        unacked := [message_id()]
    }.

-export_type([t/0, progress/0, options/0]).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec new(emqx_ds:iterator()) -> t().
new(StartIterator) ->
    new(StartIterator, #{}).

-spec new(emqx_ds:iterator(), options()) -> t().
new(StartIterator, Options) ->
    #{
        status => active,
        options => handle_options(Options),
        lower_buffer => #{
            it_begin => StartIterator,
            it_end => StartIterator,
            n => 0,
            unacked => #{}
        },
        upper_buffer => undefined,
        upper_seqno => undefined,
        last_message_id => undefined
    }.

-spec restore(progress(), options()) -> t().
%% The previous buffer did not see any messages, so we can just start a new buffer
restore(#{last_message_id := undefined, it_begin := It}, Options) ->
    new(It, Options);
%% The previous buffer saw some messages, so we need to restore the buffer from the saved state
restore(#{it := It, last_message_id := LastMessageId, unacked := Unacked}, Options) ->
    #{
        status => restoring,
        options => handle_options(Options),
        it_begin => It,
        unacked => maps:from_keys(Unacked, true),
        actual_unacked => #{},
        last_message_id => LastMessageId,
        messages => []
    }.

-spec handle_ds_reply(t(), emqx_ds:subscription_handle(), #ds_sub_reply{}) ->
    {ok, [emqx_ds:ttv()], t()} | finished.
handle_ds_reply(
    #{status := active, lower_buffer := LowerBuffer0, upper_buffer := UpperBuffer0} = SC0,
    Handle,
    #ds_sub_reply{payload = {ok, end_of_stream}, seqno = SeqNo}
) ->
    SC1 =
        case UpperBuffer0 of
            undefined ->
                LowerBuffer1 = LowerBuffer0#{
                    it_end => end_of_stream
                },
                SC0#{
                    lower_buffer => LowerBuffer1
                };
            _ ->
                UpperBuffer1 = UpperBuffer0#{
                    it_end => end_of_stream
                },
                SC0#{
                    upper_buffer => UpperBuffer1
                }
        end,
    SC2 = suback(SC1, Handle, SeqNo),
    case compact(SC2) of
        finished ->
            finished;
        {ok, SC} ->
            {ok, [], SC}
    end;
handle_ds_reply(
    #{status := active, lower_buffer := LowerBuffer0, upper_buffer := UpperBuffer0} = SC0,
    Handle,
    #ds_sub_reply{payload = {ok, It, TTVs}, seqno = SeqNo, size = Size}
) ->
    SC =
        case can_advance_lower_buffer(SC0) of
            true ->
                {LastMessageId, LowerBuffer} = push_to_buffer(LowerBuffer0, It, TTVs, Size),
                SC1 = SC0#{lower_buffer => LowerBuffer},
                SC2 = update_last_seen_message_id(SC1, LastMessageId),
                suback(SC2, Handle, SeqNo);
            false ->
                {LastMessageId, UpperBuffer} = push_to_buffer(
                    maybe_init_upper_buffer(LowerBuffer0, UpperBuffer0), It, TTVs, Size
                ),
                SC1 = SC0#{upper_buffer => UpperBuffer},
                SC2 = update_last_seen_message_id(SC1, LastMessageId),
                %% TODO check if is paused also
                case is_buffer_full(SC2, UpperBuffer) of
                    true ->
                        pause(SC2, Handle, SeqNo);
                    false ->
                        suback(SC2, Handle, SeqNo)
                end
        end,
    {ok, TTVs, SC};
%% End of stream occured while restoring the buffer
handle_ds_reply(
    #{
        status := restoring,
        it_begin := ItBegin,
        actual_unacked := ActualUnacked,
        messages := Messages,
        last_message_id := LastMessageId,
        options := Options
    } = _SC0,
    Handle,
    #ds_sub_reply{payload = {ok, end_of_stream}, seqno = SeqNo}
) ->
    SC0 = #{
        status => active,
        options => Options,
        lower_buffer => #{
            it_begin => ItBegin,
            it_end => end_of_stream,
            n => length(Messages),
            unacked => ActualUnacked
        },
        upper_buffer => undefined,
        upper_seqno => undefined,
        last_message_id => LastMessageId
    },
    SC = suback(SC0, Handle, SeqNo),
    {ok, lists:reverse(Messages), SC};
handle_ds_reply(#{status := restoring} = SC0, Handle, #ds_sub_reply{
    payload = {ok, It, NewTTVs}, seqno = SeqNo, size = _Size
}) ->
    case handle_restore(SC0, NewTTVs, 0, It) of
        {ok, TTVs, #{status := active} = SC1} ->
            SC2 = pause(SC1, Handle, SeqNo),
            ?tp(warning, emqx_mq_consumer_stream_buffer_handle_ds_reply_buffer_restored, #{
                sb => info(SC2)
            }),
            {ok, TTVs, SC2};
        {ok, TTVs, #{status := restoring} = SC} ->
            {ok, TTVs, suback(SC, Handle, SeqNo)}
    end.

-spec handle_ack(t(), message_id()) -> {ok, t()} | finished.
%% Must not receive any acks in restoring state
handle_ack(
    #{status := active, lower_buffer := LowerBuffer0, upper_buffer := UpperBuffer0} = SC0, MessageId
) ->
    SC =
        case ack_from_buffer(LowerBuffer0, MessageId) of
            {true, LowerBuffer} ->
                SC0#{lower_buffer => LowerBuffer};
            false ->
                case ack_from_buffer(UpperBuffer0, MessageId) of
                    {true, UpperBuffer} ->
                        SC0#{upper_buffer => UpperBuffer};
                    false ->
                        error({message_not_found, MessageId})
                end
        end,
    compact(SC).

-spec progress(t()) -> progress().
progress(
    #{
        status := active,
        lower_buffer := #{it_begin := ItBegin, unacked := LowerUnacked} = _LowerBuffer,
        upper_buffer := UpperBuffer,
        last_message_id := LastMessageId
    } = SC
) ->
    case is_finished(SC) of
        true ->
            end_of_stream;
        false ->
            UpperUnacked =
                case UpperBuffer of
                    undefined ->
                        #{};
                    #{unacked := Unacked} ->
                        Unacked
                end,
            AllUnacked = maps:keys(maps:merge(LowerUnacked, UpperUnacked)),
            #{it => ItBegin, last_message_id => LastMessageId, unacked => AllUnacked}
    end;
progress(
    #{
        status := restoring,
        it_begin := ItBegin,
        last_message_id := LastMessageId,
        unacked := Unacked
    } = _SC
) ->
    #{it => ItBegin, last_message_id => LastMessageId, unacked => maps:keys(Unacked)}.

-spec iterator(t()) -> emqx_ds:iterator() | end_of_stream.
iterator(#{status := active, lower_buffer := #{it_begin := ItBegin}} = _SC) ->
    ItBegin;
iterator(#{status := restoring, it_begin := ItBegin} = _SC) ->
    ItBegin.

-spec info(t()) ->
    #{
        status := active,
        lower_buffer := undefined | #{n := non_neg_integer(), unacked := non_neg_integer()},
        upper_buffer := undefined | #{n := non_neg_integer(), unacked := non_neg_integer()},
        paused := boolean(),
        finished := boolean(),
        last_message_id := undefined | message_id()
    }
    | #{
        status := restoring,
        last_message_id := message_id(),
        unacked := non_neg_integer(),
        actual_unacked := non_neg_integer(),
        messages := non_neg_integer()
    }.
info(
    #{
        status := active,
        lower_buffer := LowerBuffer,
        upper_buffer := UpperBuffer,
        last_message_id := LastMessageId
    } = SC
) ->
    #{
        status => active,
        lower_buffer => info_buffer(LowerBuffer),
        upper_buffer => info_buffer(UpperBuffer),
        paused => info_paused(SC),
        finished => is_finished(SC),
        last_message_id => LastMessageId
    };
info(
    #{
        status := restoring,
        last_message_id := LastMessageId,
        unacked := Unacked,
        actual_unacked := ActualUnacked,
        messages := Messages
    } = _SC
) ->
    #{
        status => restoring,
        last_message_id => LastMessageId,
        unacked => map_size(Unacked),
        actual_unacked => map_size(ActualUnacked),
        messages => length(Messages)
    }.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

handle_restore(
    #{
        status := restoring,
        it_begin := ItBegin,
        actual_unacked := ActualUnacked,
        messages := Messages,
        last_message_id := LastMessageId,
        options := Options
    } = SC0,
    [],
    LastTTVMessageId,
    It
) ->
    case LastMessageId =< LastTTVMessageId of
        %% Restoration is complete, we can transition to active state in paused state
        true ->
            SC = SC0#{
                status => active,
                options => Options,
                lower_buffer => #{
                    it_begin => ItBegin,
                    it_end => It,
                    n => length(Messages),
                    unacked => ActualUnacked
                },
                upper_buffer => undefined,
                upper_seqno => undefined,
                last_message_id => LastTTVMessageId
            },
            {ok, lists:reverse(Messages), SC};
        false ->
            {ok, [], SC0}
    end;
handle_restore(
    #{
        status := restoring,
        unacked := Unacked,
        actual_unacked := ActualUnacked,
        messages := Messages,
        last_message_id := LastMessageId
    } = SC0,
    [{_Topic, MessageId, _TTV} = TTV | Rest],
    _LastTTVMessageId,
    It
) ->
    SC =
        case MessageId =< LastMessageId of
            true ->
                %% Message from the previous buffer
                case Unacked of
                    %% Unacked message from the previous buffer
                    #{MessageId := true} ->
                        SC0#{
                            actual_unacked => ActualUnacked#{MessageId => true},
                            messages => [TTV | Messages]
                        };
                    %% Acknowledged message from the previous buffer
                    _ ->
                        SC0
                end;
            false ->
                %% New message, was not seen in the previous buffer
                SC0#{
                    actual_unacked => ActualUnacked#{MessageId => true},
                    messages => [TTV | Messages]
                }
        end,
    handle_restore(SC, Rest, MessageId, It).

maybe_init_upper_buffer(#{it_end := ItEnd} = _LowerBuffer, undefined) ->
    #{it_begin => ItEnd, it_end => ItEnd, n => 0, unacked => #{}};
maybe_init_upper_buffer(_LowerBuffer, #{} = UpperBuffer) ->
    UpperBuffer.

compact(SC0) ->
    SC1 = try_rotate_upper_buffer(SC0),
    SC2 = try_compact_lower_buffer(SC1),
    case is_finished(SC2) of
        true ->
            finished;
        false ->
            {ok, SC2}
    end.

is_finished(
    #{
        lower_buffer := #{it_begin := end_of_stream, it_end := end_of_stream},
        upper_buffer := undefined
    } = _SC
) ->
    true;
is_finished(_SC) ->
    false.

try_compact_lower_buffer(#{lower_buffer := LowerBuffer0} = SC) ->
    case LowerBuffer0 of
        #{it_end := ItEnd, unacked := Unacked} when map_size(Unacked) =:= 0 ->
            LowerBuffer = LowerBuffer0#{
                it_begin => ItEnd,
                it_end => ItEnd,
                n => 0
            },
            SC#{lower_buffer => LowerBuffer};
        _ ->
            SC
    end.

try_rotate_upper_buffer(#{upper_buffer := undefined} = SC) ->
    SC;
try_rotate_upper_buffer(#{lower_buffer := LowerBuffer, upper_buffer := UpperBuffer} = SC0) ->
    case LowerBuffer of
        #{unacked := Unacked} when map_size(Unacked) =:= 0 ->
            SC = SC0#{lower_buffer => UpperBuffer, upper_buffer => undefined},
            resume(SC);
        _ ->
            SC0
    end.

ack_from_buffer(#{unacked := Unacked} = Buffer, MessageId) ->
    case Unacked of
        #{MessageId := true} ->
            {true, Buffer#{unacked => maps:remove(MessageId, Unacked)}};
        _ ->
            false
    end.

push_to_buffer(#{n := N, unacked := Unacked0} = Buffer0, It, TTVs, Size) ->
    {LastMessageId, Unacked} = lists:foldl(
        fun({_Topic, MessageId, _TTV}, {_LastTTVMessageId, UnackedAcc}) ->
            {MessageId, UnackedAcc#{MessageId => true}}
        end,
        {undefined, Unacked0},
        TTVs
    ),
    {LastMessageId, Buffer0#{
        it_end => It,
        n => N + Size,
        unacked => Unacked
    }}.

update_last_seen_message_id(SC, undefined) ->
    SC;
update_last_seen_message_id(SC, LastMessageId) ->
    SC#{last_message_id => LastMessageId}.

can_advance_lower_buffer(#{lower_buffer := LowerBuffer, upper_buffer := undefined} = SC) ->
    not is_buffer_full(SC, LowerBuffer);
can_advance_lower_buffer(#{}) ->
    false.

is_buffer_full(#{options := #{max_buffer_size := MaxBufferSize}} = _SC, #{n := N} = _Buffer) ->
    N >= MaxBufferSize.

suback(SC, Handle, SeqNo) ->
    ok = emqx_mq_message_db:suback(Handle, SeqNo),
    SC#{upper_seqno => undefined}.

pause(SC, Handle, SeqNo) ->
    SC#{upper_seqno => {Handle, SeqNo}}.

resume(#{upper_seqno := undefined} = SC) ->
    SC;
resume(#{upper_seqno := {Handle, SeqNo}} = SC) ->
    suback(SC, Handle, SeqNo).

info_buffer(undefined) -> undefined;
info_buffer(#{n := N, unacked := Unacked} = _Buffer) -> #{n => N, unacked => map_size(Unacked)}.

info_paused(#{upper_seqno := undefined} = _SC) ->
    false;
info_paused(#{upper_seqno := _UpperSeqNo} = _SC) ->
    true.

handle_options(Options) ->
    MaxBufferSizeTotal = maps:get(max_buffer_size, Options, ?MQ_CONSUMER_MAX_BUFFER_SIZE),
    MaxBufferSize = max(1, MaxBufferSizeTotal div 2),
    Options#{max_buffer_size => MaxBufferSize}.
