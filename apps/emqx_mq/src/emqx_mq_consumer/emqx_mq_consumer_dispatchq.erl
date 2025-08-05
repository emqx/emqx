%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mq_consumer_dispatchq).

-moduledoc """
This module implements the dispatch queue (time-based priority) for the MQ consumer.
""".

%% NOTE
%% May improve introducing a separate simple queue for messages that are
%% added for the first time.
%% Thus in the case of normal functioning and the absense of re-dispatches
%% we will avoid time-based calculations.

-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-export([
    new/0,
    add/2,
    add_redispatch/3,
    fetch/1,
    to_list/1
]).

%%--------------------------------------------------------------------
%% Types
%%--------------------------------------------------------------------

-type ts_monotonic() :: integer().
-type entry() :: {ts_monotonic(), emqx_mq_types:message_id()}.

-record(dispatch_queue, {
    queue = gb_sets:empty() :: gb_sets:set(entry())
}).

-type t() :: #dispatch_queue{}.

-export_type([t/0]).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec new() -> t().
new() ->
    #dispatch_queue{
        queue = gb_trees:empty()
    }.

-spec add(t(), [emqx_mq_types:message_id()]) -> t().
add(DispatchQueue, MessageIds) when is_list(MessageIds) ->
    NowMs = now_ms_monotonic(),
    lists:foldl(
        fun(MessageId, DispatchQueueAcc) ->
            add(DispatchQueueAcc, MessageId, NowMs)
        end,
        DispatchQueue,
        MessageIds
    ).

-spec add_redispatch(t(), emqx_mq_types:message_id(), ts_monotonic()) -> t().
add_redispatch(DispatchQueue, MessageId, DelayMs) when is_tuple(MessageId) ->
    add(DispatchQueue, MessageId, now_ms_monotonic() + DelayMs).

-spec fetch(t()) ->
    {ok, [emqx_mq_types:message_id()], t()} | {delay, ts_monotonic()} | empty.
fetch(#dispatch_queue{queue = Queue0} = DispatchQueue) ->
    NowMs = now_ms_monotonic(),
    It = gb_sets:iterator(Queue0),
    case fetch(Queue0, It, NowMs, []) of
        {[], _Queue} ->
            empty;
        {[], Delay, _Queue} ->
            {delay, Delay};
        {Fetched, Queue} ->
            {ok, message_ids(Fetched), DispatchQueue#dispatch_queue{queue = Queue}};
        {Fetched, _Delay, Queue} ->
            {ok, message_ids(Fetched), DispatchQueue#dispatch_queue{queue = Queue}}
    end.

-spec to_list(t()) -> [entry()].
to_list(#dispatch_queue{queue = Queue}) ->
    gb_sets:to_list(Queue).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

add(#dispatch_queue{queue = Queue} = DispatchQueue, {Slab, SlabMessageId}, TimestampMs) ->
    DispatchQueue#dispatch_queue{
        %% let the message be ordered by raft's ts within the same timestamp
        queue = gb_sets:add({TimestampMs, {SlabMessageId, Slab}}, Queue)
    }.

fetch(Queue, It, NowMs, Acc) ->
    case gb_sets:next(It) of
        {{TimestampMs, _} = Entry, NextIt} when TimestampMs =< NowMs ->
            fetch(Queue, NextIt, NowMs, [Entry | Acc]);
        {{TimestampMs, _}, _NextIt} ->
            Delay = TimestampMs - NowMs + 1,
            {lists:reverse(Acc), Delay, delete_fetched(Queue, Acc)};
        none ->
            {lists:reverse(Acc), gb_sets:empty()}
    end.

delete_fetched(Queue, Entries) ->
    lists:foldl(
        fun(Entry, Q) ->
            gb_sets:delete(Entry, Q)
        end,
        Queue,
        Entries
    ).

message_ids(Entries) ->
    [{Slab, SlabMessageId} || {_TimestampMs, {SlabMessageId, Slab}} <- Entries].

now_ms_monotonic() ->
    erlang:monotonic_time(millisecond).
