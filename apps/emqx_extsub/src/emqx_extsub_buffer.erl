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
        {emqx_extsub_types:subscriber_ref(), emqx_types:message()}
    ),
    delivering = #{} :: #{emqx_extsub_types:subscriber_ref() => non_neg_integer()}
}).

-type t() :: #buffer{}.

-export_type([t/0, seq_id/0]).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec new() -> t().
new() ->
    #buffer{}.

-spec add_new(t(), emqx_extsub_types:subscriber_ref(), [emqx_types:message()]) ->
    t().
add_new(
    #buffer{seq = SeqId0, message_buffer = MessageBuffer0, delivering = Delivering0} = Buffer,
    SubscriberRef,
    Messages
) ->
    {SeqId, MessageBuffer} = lists:foldl(
        fun(Msg, {SeqIdAcc, MessageBufferAcc}) ->
            {SeqIdAcc + 1, gb_trees:insert(SeqIdAcc, {SubscriberRef, Msg}, MessageBufferAcc)}
        end,
        {SeqId0, MessageBuffer0},
        Messages
    ),
    SubRefDelivering0 = maps:get(SubscriberRef, Delivering0, 0),
    SubRefDelivering1 = SubRefDelivering0 + length(Messages),
    Delivering = Delivering0#{SubscriberRef => SubRefDelivering1},
    Buffer#buffer{message_buffer = MessageBuffer, delivering = Delivering, seq = SeqId}.

-spec add_back(
    t(), emqx_extsub_types:subscriber_ref(), seq_id(), emqx_types:message()
) -> t().
add_back(#buffer{message_buffer = MessageBuffer0} = Buffer, SubscriberRef, SeqId, Msg) ->
    MessageBuffer = gb_trees:insert(SeqId, {SubscriberRef, Msg}, MessageBuffer0),
    Buffer#buffer{message_buffer = MessageBuffer}.

-spec take(t(), non_neg_integer()) ->
    {
        [
            {
                emqx_extsub_types:subscriber_ref(),
                seq_id(),
                emqx_types:message()
            }
        ],
        t()
    }.
take(Buffer, N) when (is_integer(N) andalso N >= 0) orelse N == infinity ->
    do_take(Buffer, 0, N, []).

-spec set_delivered(t(), emqx_extsub_types:subscriber_ref(), seq_id()) -> t().
set_delivered(#buffer{delivering = Delivering0} = Buffer, SubscriberRef, _SeqId) ->
    SubRefDelivering0 = maps:get(SubscriberRef, Delivering0),
    SubRefDelivering = SubRefDelivering0 - 1,
    Buffer#buffer{delivering = Delivering0#{SubscriberRef => SubRefDelivering}}.

-spec delivering_count(t(), emqx_extsub_types:subscriber_ref()) -> non_neg_integer().
delivering_count(#buffer{delivering = Delivering}, SubscriberRef) ->
    maps:get(SubscriberRef, Delivering, 0).

-spec size(t()) -> non_neg_integer().
size(#buffer{message_buffer = MessageBuffer}) ->
    gb_trees:size(MessageBuffer).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

%% Fetch at least Total messages from the buffer
do_take(Buffer, Taken, Total, Acc) when Taken >= Total ->
    {lists:reverse(Acc), Buffer};
do_take(#buffer{message_buffer = MessageBuffer0} = Buffer, Taken, Total, Acc) ->
    case gb_trees:is_empty(MessageBuffer0) of
        true ->
            {lists:reverse(Acc), Buffer};
        false ->
            {SeqId, {SubscriberRef, Msg}, MessageBuffer} = gb_trees:take_smallest(
                MessageBuffer0
            ),
            do_take(Buffer#buffer{message_buffer = MessageBuffer}, Taken + 1, Total, [
                {SubscriberRef, SeqId, Msg} | Acc
            ])
    end.
