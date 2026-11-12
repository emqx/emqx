%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mq_consumer_stream_buffer).

-moduledoc """
The module represents a consumer of a single stream of the Message Queue data.
""".

-include("../emqx_mq_internal.hrl").
-include_lib("emqx_durable_storage/include/emqx_ds.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-export([
    new/2,
    restore/2,
    handle_ds_reply/3,
    handle_ack/2,
    progress/1,
    iterator/1,
    inspect/1
]).

-define(EMPTY_TOTALS, #{count => 0, bytes => 0}).

%%--------------------------------------------------------------------
%% Types
%%--------------------------------------------------------------------

-type options() :: #{
    max_buffer_size := non_neg_integer()
}.

-type message_id() :: non_neg_integer().

%% Number and serialized size of the records read into a buffer.
-type totals() :: #{count := non_neg_integer(), bytes := non_neg_integer()}.

%% Acknowledgment progress of the lower buffers retired by one call,
%% or `undefined` when no buffer retired.
-type acked_progress() :: #progress{} | undefined.

-type buffer() :: #{
    last_record := undefined | message_id(),
    it_begin := emqx_ds:iterator() | end_of_stream,
    it_end := emqx_ds:iterator() | end_of_stream,
    n := non_neg_integer(),
    unacked := #{message_id() => true},
    totals := totals() | undefined
}.

%% State of the active stream buffer, receiving messages from DS and dispatching them to the consumer
-type st_active() :: #{
    status := active,
    mq := emqx_mq_types:mq(),
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
    mq := emqx_mq_types:mq(),
    options := options(),
    it_begin := emqx_ds:iterator(),
    %% NOTE
    %% We need to keep both unacked and actual_unacked because in the moment of restoration
    %% some unacked messages may have been deleted from the DS.
    %% So in the end of restoration the actual set of messages requiring acknowledgement
    %% may be smaller than the set of messages in the preserved unacked set.
    unacked := #{message_id() => true},
    actual_unacked := #{message_id() => emqx_ds:ttv()},
    last_message_id := message_id()
}.

-type t() :: st_active() | st_restoring().

-type progress() ::
    #{
        it := emqx_ds:iterator(),
        last_message_id := undefined | message_id(),
        unacked := [#{message_id() => true}]
    }.

-export_type([t/0, progress/0, options/0, totals/0, acked_progress/0]).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec new(emqx_ds:iterator(), emqx_mq_types:mq()) -> t().
new(StartIterator, MQ) ->
    #{
        status => active,
        options => handle_options(MQ),
        mq => MQ,
        lower_buffer => #{
            it_begin => StartIterator,
            it_end => StartIterator,
            n => 0,
            last_record => undefined,
            unacked => #{},
            totals => ?EMPTY_TOTALS
        },
        upper_buffer => undefined,
        upper_seqno => undefined,
        last_message_id => undefined
    }.

-spec restore(progress(), emqx_mq_types:mq()) -> t().
%% The previous buffer did not see any messages, so we can just start a new buffer
restore(#{last_message_id := undefined, it := It}, MQ) ->
    new(It, MQ);
%% The previous buffer saw some messages, so we need to restore the buffer from the saved state
restore(#{it := It, last_message_id := LastMessageId, unacked := UnackedMaps}, MQ) ->
    #{
        status => restoring,
        mq => MQ,
        options => handle_options(MQ),
        it_begin => It,
        unacked => emqx_utils_maps:merge(UnackedMaps),
        actual_unacked => #{},
        last_message_id => LastMessageId
    }.

-doc """
Handle a DS subscription reply.
Also return the acknowledgment progress of the lower buffers that the reply retired.
Return `{finished, Progress}` when the stream is complete.
""".
-spec handle_ds_reply(t(), emqx_ds:subscription_handle(), #ds_sub_reply{}) ->
    {ok, [emqx_ds:ttv()], acked_progress(), t()} | {finished, acked_progress()}.
handle_ds_reply(
    #{status := active, lower_buffer := LowerBuffer0, upper_buffer := UpperBuffer0} = SB0,
    Handle,
    #ds_sub_reply{payload = {ok, end_of_stream}, seqno = SeqNo}
) ->
    SB1 =
        case UpperBuffer0 of
            undefined ->
                LowerBuffer1 = LowerBuffer0#{
                    it_end => end_of_stream
                },
                SB0#{
                    lower_buffer => LowerBuffer1
                };
            _ ->
                UpperBuffer1 = UpperBuffer0#{
                    it_end => end_of_stream
                },
                SB0#{
                    upper_buffer => UpperBuffer1
                }
        end,
    SB2 = suback(SB1, Handle, SeqNo),
    case compact(SB2) of
        {finished, _} = Finished ->
            Finished;
        {ok, Progress, SB} ->
            {ok, [], Progress, SB}
    end;
handle_ds_reply(
    #{status := active, lower_buffer := LowerBuffer0, upper_buffer := UpperBuffer0} = SB0,
    Handle,
    #ds_sub_reply{payload = {ok, It, TTVs}, seqno = SeqNo, size = Size}
) ->
    SB =
        case can_advance_lower_buffer(SB0) of
            true ->
                {LastMessageId, LowerBuffer} = push_to_buffer(LowerBuffer0, It, TTVs, Size),
                SB1 = SB0#{lower_buffer => LowerBuffer},
                SB2 = update_last_seen_message_id(SB1, LastMessageId),
                suback(SB2, Handle, SeqNo);
            false ->
                {LastMessageId, UpperBuffer} = push_to_buffer(
                    maybe_init_upper_buffer(LowerBuffer0, UpperBuffer0), It, TTVs, Size
                ),
                SB1 = SB0#{upper_buffer => UpperBuffer},
                SB2 = update_last_seen_message_id(SB1, LastMessageId),
                case is_buffer_full(SB2, UpperBuffer) orelse is_paused(SB2) of
                    true ->
                        pause(SB2, Handle, SeqNo);
                    false ->
                        suback(SB2, Handle, SeqNo)
                end
        end,
    {ok, TTVs, undefined, SB};
%% End of stream occured while restoring the buffer
handle_ds_reply(
    #{
        mq := MQ,
        status := restoring,
        it_begin := ItBegin,
        actual_unacked := ActualUnacked,
        last_message_id := LastMessageId,
        options := Options
    } = _SB0,
    Handle,
    #ds_sub_reply{payload = {ok, end_of_stream}, seqno = SeqNo}
) ->
    {Unacked, TTVs} = restore_unacked(ActualUnacked),
    SB0 = #{
        mq => MQ,
        status => active,
        options => Options,
        lower_buffer => #{
            it_begin => ItBegin,
            it_end => end_of_stream,
            n => length(TTVs),
            %% Each record up to the checkpoint is acknowledged, unacknowledged in this buffer,
            %% or deleted
            last_record => LastMessageId,
            unacked => Unacked,
            %% Records read during restoration are not counted
            totals => undefined
        },
        upper_buffer => undefined,
        upper_seqno => undefined,
        last_message_id => LastMessageId
    },
    SB1 = suback(SB0, Handle, SeqNo),
    case compact(SB1) of
        {finished, _} = Finished ->
            Finished;
        {ok, Progress, SB} ->
            {ok, TTVs, Progress, SB}
    end;
handle_ds_reply(#{status := restoring} = SB0, Handle, #ds_sub_reply{
    payload = {ok, It, NewTTVs}, seqno = SeqNo, size = _Size
}) ->
    case handle_restore(SB0, NewTTVs, undefined, It) of
        {ok, TTVs, #{status := active} = SB} ->
            ?tp_debug(mq_consumer_stream_buffer_handle_ds_reply_buffer_restored, #{
                sb => inspect(SB)
            }),
            {ok, Progress, SB1} = compact(suback(SB, Handle, SeqNo)),
            {ok, TTVs, Progress, SB1};
        {ok, TTVs, #{status := restoring} = SB} ->
            {ok, TTVs, undefined, suback(SB, Handle, SeqNo)}
    end.

-doc """
Acknowledge a message.
Also return the acknowledgment progress of the lower buffers that the acknowledgment retired.
Return `{finished, Progress}` when the stream is complete.
""".
-spec handle_ack(t(), message_id()) -> {ok, acked_progress(), t()} | {finished, acked_progress()}.
handle_ack(
    #{status := active, lower_buffer := LowerBuffer0, upper_buffer := UpperBuffer0} = SB0, MessageId
) ->
    SB =
        case ack_from_buffer(LowerBuffer0, MessageId) of
            {true, LowerBuffer} ->
                SB0#{lower_buffer => LowerBuffer};
            false ->
                case ack_from_buffer(UpperBuffer0, MessageId) of
                    {true, UpperBuffer} ->
                        SB0#{upper_buffer => UpperBuffer};
                    false ->
                        SB0
                end
        end,
    compact(SB);
%% This may happen if we reset the buffer due to subscription down
handle_ack(
    #{status := restoring, unacked := Unacked0, actual_unacked := ActualUnacked0} = SB, MessageId
) ->
    {ok, undefined, SB#{
        unacked => maps:remove(MessageId, Unacked0),
        actual_unacked => maps:remove(MessageId, ActualUnacked0)
    }}.

-spec progress(t()) -> progress().
progress(
    #{
        status := active,
        lower_buffer := #{it_begin := ItBegin, unacked := LowerUnacked} = _LowerBuffer,
        upper_buffer := UpperBuffer,
        last_message_id := LastMessageId
    }
) ->
    UpperUnacked =
        case UpperBuffer of
            undefined ->
                #{};
            #{unacked := Unacked} ->
                Unacked
        end,
    AllUnacked = [LowerUnacked, UpperUnacked],
    #{it => ItBegin, last_message_id => LastMessageId, unacked => AllUnacked};
progress(
    #{
        status := restoring,
        it_begin := ItBegin,
        last_message_id := LastMessageId,
        unacked := Unacked
    } = _SB
) ->
    #{it => ItBegin, last_message_id => LastMessageId, unacked => [Unacked]}.

-spec iterator(t()) -> emqx_ds:iterator() | end_of_stream.
iterator(#{status := active, lower_buffer := #{it_begin := ItBegin}} = _SB) ->
    ItBegin;
iterator(#{status := restoring, it_begin := ItBegin} = _SB) ->
    ItBegin.

-spec inspect(t()) ->
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
        actual_unacked := non_neg_integer()
    }.
inspect(
    #{
        status := active,
        lower_buffer := LowerBuffer,
        upper_buffer := UpperBuffer,
        last_message_id := LastMessageId
    } = SB
) ->
    #{
        status => active,
        lower_buffer => info_buffer(LowerBuffer),
        upper_buffer => info_buffer(UpperBuffer),
        paused => info_paused(SB),
        finished => is_finished(SB),
        last_message_id => LastMessageId
    };
inspect(
    #{
        status := restoring,
        last_message_id := LastMessageId,
        unacked := Unacked,
        actual_unacked := ActualUnacked
    } = _SB
) ->
    #{
        status => restoring,
        last_message_id => LastMessageId,
        unacked => map_size(Unacked),
        actual_unacked => map_size(ActualUnacked)
    }.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

handle_restore(
    #{
        status := restoring,
        mq := MQ,
        it_begin := ItBegin,
        actual_unacked := ActualUnacked,
        last_message_id := LastMessageId,
        options := Options
    } = SB0,
    [],
    LastTTVMessageId,
    It
) ->
    case is_integer(LastTTVMessageId) andalso LastMessageId =< LastTTVMessageId of
        %% Restoration is complete, we can transition to active state
        true ->
            {Unacked, TTVs} = restore_unacked(ActualUnacked),
            SB = SB0#{
                status => active,
                mq => MQ,
                options => Options,
                lower_buffer => #{
                    it_begin => ItBegin,
                    it_end => It,
                    n => map_size(Unacked),
                    last_record => LastTTVMessageId,
                    unacked => Unacked,
                    %% Records read during restoration are not counted
                    totals => undefined
                },
                upper_buffer => undefined,
                upper_seqno => undefined,
                last_message_id => LastTTVMessageId
            },
            {ok, TTVs, SB};
        false ->
            {ok, [], SB0}
    end;
handle_restore(
    #{
        status := restoring,
        unacked := Unacked,
        actual_unacked := ActualUnacked,
        last_message_id := LastMessageId
    } = SB0,
    [{_Topic, MessageId, _TTV} = TTV | Rest],
    _LastTTVMessageId,
    It
) ->
    SB =
        case MessageId =< LastMessageId of
            true ->
                %% Message from the previous buffer
                case Unacked of
                    %% Unacked message from the previous buffer
                    #{MessageId := true} ->
                        SB0#{
                            actual_unacked => ActualUnacked#{MessageId => TTV}
                        };
                    %% Acknowledged message from the previous buffer
                    _ ->
                        SB0
                end;
            false ->
                %% New message, was not seen in the previous buffer
                SB0#{
                    actual_unacked => ActualUnacked#{MessageId => TTV}
                }
        end,
    handle_restore(SB, Rest, MessageId, It).

maybe_init_upper_buffer(#{it_end := ItEnd} = _LowerBuffer, undefined) ->
    #{
        it_begin => ItEnd,
        it_end => ItEnd,
        n => 0,
        unacked => #{},
        last_record => undefined,
        totals => ?EMPTY_TOTALS
    };
maybe_init_upper_buffer(_LowerBuffer, #{} = UpperBuffer) ->
    UpperBuffer.

%% Rotation and compaction can each retire a lower buffer, so merge their progress.
compact(SB0) ->
    {Progress1, SB1} = try_rotate_upper_buffer(SB0),
    {Progress2, SB2} = try_compact_lower_buffer(SB1),
    Progress = merge_progress(Progress1, Progress2),
    case is_finished(SB2) of
        true ->
            {finished, Progress};
        false ->
            {ok, Progress, SB2}
    end.

is_finished(
    #{
        lower_buffer := #{it_begin := end_of_stream, it_end := end_of_stream},
        upper_buffer := undefined
    } = _SB
) ->
    true;
is_finished(_SB) ->
    false.

try_compact_lower_buffer(#{lower_buffer := LowerBuffer0} = SB0) ->
    case LowerBuffer0 of
        #{it_end := ItEnd, unacked := Unacked} when map_size(Unacked) =:= 0 ->
            LowerBuffer = LowerBuffer0#{
                it_begin => ItEnd,
                it_end => ItEnd,
                n => 0,
                last_record => undefined,
                totals => ?EMPTY_TOTALS
            },
            {buffer_progress(LowerBuffer0), SB0#{lower_buffer => LowerBuffer}};
        _ ->
            {undefined, SB0}
    end.

try_rotate_upper_buffer(#{upper_buffer := undefined} = SB) ->
    {undefined, SB};
try_rotate_upper_buffer(#{lower_buffer := LowerBuffer, upper_buffer := UpperBuffer} = SB0) ->
    case LowerBuffer of
        #{unacked := Unacked} when map_size(Unacked) =:= 0 ->
            SB = SB0#{lower_buffer => UpperBuffer, upper_buffer => undefined},
            {buffer_progress(LowerBuffer), resume(SB)};
        _ ->
            {undefined, SB0}
    end.

%% Progress of a completely acknowledged buffer. A buffer without records reports no progress.
buffer_progress(#{last_record := Last, totals := Totals}) when is_integer(Last) ->
    #progress{boundary = Last + 1, totals = Totals};
buffer_progress(_Buffer) ->
    undefined.

%% Totals are unknown if either part is uncounted.
merge_progress(Progress, undefined) ->
    Progress;
merge_progress(undefined, Progress) ->
    Progress;
merge_progress(
    #progress{boundary = Boundary1, totals = Totals1},
    #progress{boundary = Boundary2, totals = Totals2}
) ->
    #progress{boundary = max(Boundary1, Boundary2), totals = sum_totals(Totals1, Totals2)}.

sum_totals(undefined, _Totals) ->
    undefined;
sum_totals(_Totals, undefined) ->
    undefined;
sum_totals(#{count := Count1, bytes := Bytes1}, #{count := Count2, bytes := Bytes2}) ->
    #{count => Count1 + Count2, bytes => Bytes1 + Bytes2}.

ack_from_buffer(undefined, _MessageId) ->
    false;
ack_from_buffer(#{unacked := Unacked} = Buffer, MessageId) ->
    case Unacked of
        #{MessageId := true} ->
            {true, Buffer#{unacked => maps:remove(MessageId, Unacked)}};
        _ ->
            false
    end.

push_to_buffer(
    #{n := N, unacked := Unacked0, last_record := LastRecord, totals := Totals} = Buffer0,
    It,
    TTVs,
    Size
) ->
    {LastMessageId, Unacked} = lists:foldl(
        fun({_Topic, MessageId, _TTV}, {_LastTTVMessageId, UnackedAcc}) ->
            {MessageId, UnackedAcc#{MessageId => true}}
        end,
        {LastRecord, Unacked0},
        TTVs
    ),
    {LastMessageId, Buffer0#{
        it_end => It,
        n => N + Size,
        last_record => LastMessageId,
        unacked => Unacked,
        totals => add_totals(Totals, TTVs)
    }}.

%% A restored buffer stays uncounted when it receives more records.
add_totals(undefined, _TTVs) ->
    undefined;
add_totals(#{count := Count, bytes := Bytes}, TTVs) ->
    #{
        count => Count + length(TTVs),
        bytes => Bytes + lists:sum([byte_size(Payload) || {_Topic, _MessageId, Payload} <- TTVs])
    }.

update_last_seen_message_id(SB, undefined) ->
    SB;
update_last_seen_message_id(SB, LastMessageId) ->
    SB#{last_message_id => LastMessageId}.

can_advance_lower_buffer(#{lower_buffer := LowerBuffer, upper_buffer := undefined} = SB) ->
    not is_buffer_full(SB, LowerBuffer);
can_advance_lower_buffer(#{}) ->
    false.

is_buffer_full(#{options := #{max_buffer_size := MaxBufferSize}} = _SB, #{n := N} = _Buffer) ->
    N >= MaxBufferSize.

suback(#{mq := MQ} = SB, Handle, SeqNo) ->
    ok = emqx_mq_message_db:suback(MQ, Handle, SeqNo),
    SB#{upper_seqno => undefined}.

pause(SB, Handle, SeqNo) ->
    SB#{upper_seqno => {Handle, SeqNo}}.

resume(#{upper_seqno := undefined} = SB) ->
    SB;
resume(#{upper_seqno := {Handle, SeqNo}} = SB) ->
    suback(SB, Handle, SeqNo).

is_paused(#{upper_seqno := undefined} = _SB) ->
    false;
is_paused(#{upper_seqno := {_Handle, _SeqNo}} = _SB) ->
    true.

info_buffer(undefined) -> undefined;
info_buffer(#{n := N, unacked := Unacked} = _Buffer) -> #{n => N, unacked => map_size(Unacked)}.

info_paused(#{upper_seqno := undefined} = _SB) ->
    false;
info_paused(#{upper_seqno := _UpperSeqNo} = _SB) ->
    true.

handle_options(#{stream_max_buffer_size := MaxStreamBufferSize} = _MQ) ->
    MaxBufferSize = max(1, MaxStreamBufferSize div 2),
    #{max_buffer_size => MaxBufferSize}.

restore_unacked(ActualUnacked) ->
    {MessageIds, TTVs} = lists:unzip(lists:sort(maps:to_list(ActualUnacked))),
    {maps:from_keys(MessageIds, true), TTVs}.
