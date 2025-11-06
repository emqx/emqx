%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_extsub_buffer).

-moduledoc """
Buffer of message_buffer received from the ExtSub handlers
""".

%% TODO
%% Use internal message ids

-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("emqx_utils/include/emqx_message.hrl").
-include("emqx_extsub_internal.hrl").

-export([
    new/0,
    take/2,
    add_new/3,
    add_back/4,
    size/1,
    delivering_count/2,
    set_delivered/3
]).

-type seq_id() :: non_neg_integer().

-record(buffer, {
    seq = 0 :: seq_id(),
    message_buffer = gb_trees:empty() :: gb_trees:tree(
        seq_id(),
        {emqx_extsub_types:handler_ref(), emqx_types:message()}
    ),
    delivering = #{} :: #{emqx_extsub_types:handler_ref() => non_neg_integer()}
}).

-type t() :: #buffer{}.

-export_type([t/0, seq_id/0]).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec new() -> t().
new() ->
    #buffer{}.

-spec add_new(t(), emqx_extsub_types:handler_ref(), [emqx_types:message()]) ->
    t().
add_new(
    #buffer{seq = SeqId0, message_buffer = MessageBuffer0, delivering = Delivering0} = Buffer,
    HandlerRef,
    Messages
) ->
    {SeqId, MessageBuffer} = lists:foldl(
        fun(Msg, {SeqIdAcc, MessageBufferAcc}) ->
            {SeqIdAcc + 1, gb_trees:insert(SeqIdAcc, {HandlerRef, Msg}, MessageBufferAcc)}
        end,
        {SeqId0, MessageBuffer0},
        Messages
    ),
    SubRefDelivering0 = maps:get(HandlerRef, Delivering0, 0),
    SubRefDelivering1 = SubRefDelivering0 + (SeqId - SeqId0),
    Delivering = Delivering0#{HandlerRef => SubRefDelivering1},
    Buffer#buffer{message_buffer = MessageBuffer, delivering = Delivering, seq = SeqId}.

-spec add_back(
    t(), emqx_extsub_types:handler_ref(), seq_id(), emqx_types:message()
) -> t().
add_back(#buffer{message_buffer = MessageBuffer0} = Buffer, HandlerRef, SeqId, Msg) ->
    MessageBuffer = gb_trees:insert(SeqId, {HandlerRef, Msg}, MessageBuffer0),
    Buffer#buffer{message_buffer = MessageBuffer}.

-spec take(t(), non_neg_integer()) ->
    {
        [
            {
                emqx_extsub_types:handler_ref(),
                seq_id(),
                emqx_types:message()
            }
        ],
        t()
    }.
take(#buffer{message_buffer = MessageBuffer0} = Buffer, N) when
    (is_integer(N) andalso N >= 0) orelse N == infinity
->
    {Messages, MessageBuffer} = do_take(MessageBuffer0, 0, N, []),
    {Messages, Buffer#buffer{message_buffer = MessageBuffer}}.

-spec set_delivered(t(), emqx_extsub_types:handler_ref(), seq_id()) -> t().
set_delivered(#buffer{delivering = Delivering0} = Buffer, HandlerRef, _SeqId) ->
    SubRefDelivering0 = maps:get(HandlerRef, Delivering0),
    SubRefDelivering = SubRefDelivering0 - 1,
    Buffer#buffer{delivering = Delivering0#{HandlerRef => SubRefDelivering}}.

-spec delivering_count(t(), emqx_extsub_types:handler_ref()) -> non_neg_integer().
delivering_count(#buffer{delivering = Delivering}, HandlerRef) ->
    maps:get(HandlerRef, Delivering, 0).

-spec size(t()) -> non_neg_integer().
size(#buffer{message_buffer = MessageBuffer}) ->
    gb_trees:size(MessageBuffer).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

%% Fetch at mostTotal messages from the buffer
do_take(MessageBuffer, Taken, Total, Acc) when Taken >= Total ->
    {lists:reverse(Acc), MessageBuffer};
do_take(MessageBuffer0, Taken, Total, Acc) ->
    case gb_trees:is_empty(MessageBuffer0) of
        true ->
            {lists:reverse(Acc), MessageBuffer0};
        false ->
            {SeqId, {HandlerRef, Msg}, MessageBuffer} = gb_trees:take_smallest(
                MessageBuffer0
            ),
            do_take(
                MessageBuffer,
                Taken + 1,
                Total,
                [{HandlerRef, SeqId, Msg} | Acc]
            )
    end.
