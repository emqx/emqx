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
    handle_ds_reply/3,
    handle_ack/2,
    progress/1,
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
    unacked := #{message_id() => true}
}.

-type t() :: #{
    mq_topic := emqx_mq_types:mq_topic(),
    sub_handle := emqx_ds:subscription_handle(),
    sub_ref := emqx_ds:sub_ref(),
    lower_buffer := buffer(),
    upper_buffer := undefined | buffer(),
    upper_seqno := undefined | {emqx_ds:sub_ref(), emqx_ds:sub_seqno()}
}.

-type progress() :: end_of_stream | emqx_ds:iterator().

-export_type([t/0, progress/0, options/0]).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec new(emqx_ds:iterator()) -> {ok, emqx_ds:sub_ref(), t()}.
new(StartIterator) ->
    new(StartIterator, #{}).

-spec new(emqx_ds:iterator(), options()) -> {ok, emqx_ds:sub_ref(), t()}.
new(StartIterator, Options) ->
    MaxBufferSizeTotal = maps:get(max_buffer_size, Options, ?MQ_CONSUMER_MAX_BUFFER_SIZE),
    MaxBufferSize = max(1, MaxBufferSizeTotal div 2),
    #{
        max_buffer_size => MaxBufferSize,
        lower_buffer => #{
            it_begin => StartIterator,
            it_end => StartIterator,
            n => 0,
            unacked => #{}
        },
        upper_buffer => undefined,
        upper_seqno => undefined
    }.

-spec handle_ds_reply(t(), emqx_ds:subscription_handle(), #ds_sub_reply{}) ->
    {ok, [emqx_ds:ttv()], t()} | finished.
handle_ds_reply(
    #{lower_buffer := LowerBuffer0, upper_buffer := UpperBuffer0} = SC0,
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
            {ok, [], SC#{upper_seqno => undefined}}
    end;
handle_ds_reply(
    #{lower_buffer := LowerBuffer0, upper_buffer := UpperBuffer0} = SC0,
    Handle,
    #ds_sub_reply{payload = {ok, It, Payloads}, seqno = SeqNo, size = Size}
) ->
    SC =
        case can_advance_lower_buffer(SC0) of
            true ->
                LowerBuffer = push_to_buffer(LowerBuffer0, It, Payloads, Size),
                SC1 = SC0#{lower_buffer => LowerBuffer},
                suback(SC1, Handle, SeqNo);
            false ->
                UpperBuffer = push_to_buffer(
                    maybe_init_upper_buffer(LowerBuffer0, UpperBuffer0), It, Payloads, Size
                ),
                SC1 = SC0#{upper_buffer => UpperBuffer},
                case is_buffer_full(SC1, UpperBuffer) of
                    true ->
                        SC1#{upper_seqno => {Handle, SeqNo}};
                    false ->
                        suback(SC1, Handle, SeqNo)
                end
        end,
    {ok, Payloads, SC}.

-spec handle_ack(t(), message_id()) -> {ok, t()} | finished.
handle_ack(#{lower_buffer := LowerBuffer0, upper_buffer := UpperBuffer0} = SC0, MessageId) ->
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

%% TODO
%% Implement unacked message restoration
-spec progress(t()) -> progress().
progress(#{lower_buffer := #{it_begin := ItBegin} = _LowerBuffer} = SC) ->
    case is_finished(SC) of
        true ->
            end_of_stream;
        false ->
            ItBegin
    end.

-spec info(t()) ->
    #{
        lower_buffer := undefined | #{n := non_neg_integer(), unacked := non_neg_integer()},
        upper_buffer := undefined | #{n := non_neg_integer(), unacked := non_neg_integer()},
        paused := boolean(),
        finished := boolean()
    }.
info(#{lower_buffer := LowerBuffer, upper_buffer := UpperBuffer} = SC) ->
    #{
        lower_buffer => info_buffer(LowerBuffer),
        upper_buffer => info_buffer(UpperBuffer),
        paused => info_paused(SC),
        finished => is_finished(SC)
    }.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

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

push_to_buffer(#{n := N, unacked := Unacked0} = Buffer0, It, Payloads, Size) ->
    Unacked = lists:foldl(
        fun({_Topic, MessageId, _Payload}, UnackedAcc) ->
            UnackedAcc#{MessageId => true}
        end,
        Unacked0,
        Payloads
    ),
    Buffer0#{
        it_end => It,
        n => N + Size,
        unacked => Unacked
    }.

can_advance_lower_buffer(#{lower_buffer := LowerBuffer, upper_buffer := undefined} = SC) ->
    not is_buffer_full(SC, LowerBuffer);
can_advance_lower_buffer(#{}) ->
    false.

is_buffer_full(#{max_buffer_size := MaxBufferSize} = _SC, #{n := N} = _Buffer) ->
    N >= MaxBufferSize.

suback(SC, Handle, SeqNo) ->
    ok = emqx_mq_payload_db:suback(Handle, SeqNo),
    SC#{upper_seqno => undefined}.

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
