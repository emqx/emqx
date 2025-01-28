%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

%% @doc Session uses this module for buffering replies from the DS
%% while the stream is blocked or inflight is full. It groups small
%% batches together, increasing the efficiency of replay.
-module(emqx_persistent_session_ds_buffer).

%% API:
-export([
    new/0, len/2, push_batch/3, pop_batch/2, iterator/1, next/1, clean_by_subid/2, drop_stream/2
]).

-export_type([t/0, item/0]).

-include_lib("emqx_durable_storage/include/emqx_ds.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%%================================================================================
%% Type declarations
%%================================================================================

%% Buffered poll reply:
-type item() :: #poll_reply{}.

-type q() :: queue:queue(item()).

%% Collection of per-stream buffers:
-type mqs() :: #{emqx_persistent_session_ds_stream_scheduler:stream_key() => q()}.

-record(buffer, {
    messages = #{} :: mqs()
}).

-opaque t() :: #buffer{}.

%%================================================================================
%% API functions
%%================================================================================

-spec new() -> t().
new() ->
    #buffer{}.

iterator(#buffer{messages = Msgs}) ->
    maps:iterator(Msgs).

next(It) ->
    case maps:next(It) of
        {K, _V, I} ->
            {K, I};
        none ->
            none
    end.

%% @doc Enqueue a batch of messages. Return new queue and the number
%% of batches currently buffered in the stream
-spec push_batch(emqx_persistent_session_ds_stream_scheduler:stream_key(), item(), t()) -> t().
push_batch(StreamId, Item, Buf = #buffer{messages = MsgQs}) ->
    case MsgQs of
        #{StreamId := Q0} ->
            ok;
        #{} ->
            Q0 = queue:new()
    end,
    Q = queue:in(Item, Q0),
    Buf#buffer{
        messages = MsgQs#{StreamId => Q}
    }.

%% @doc Delete all buffered data from the streams that belong to the
%% given subscription
-spec clean_by_subid(emqx_persistent_session_ds:subscription_id(), t()) -> t().
clean_by_subid(SubId, Buf = #buffer{messages = MsgQs0}) ->
    MsgQs = maps:filter(
        fun(Key, _Val) ->
            {SID, _} = Key,
            SID =/= SubId
        end,
        MsgQs0
    ),
    Buf#buffer{messages = MsgQs}.

%% @doc Delete buffered data for a particular stream.
-spec drop_stream(emqx_persistent_session_ds_stream_scheduler:stream_key(), t()) -> t().
drop_stream(StreamKey, Buf = #buffer{messages = Msgs}) ->
    Buf#buffer{messages = maps:remove(StreamKey, Msgs)}.

%% @doc Dequeue a batch of messages from a specified stream.
-spec pop_batch(emqx_persistent_session_ds_stream_scheduler:stream_key(), t()) ->
    {[item()], t()}.
pop_batch(StreamId, Buf = #buffer{messages = MsgQs0}) ->
    case MsgQs0 of
        #{StreamId := Q0} ->
            {{value, Item}, Q} = queue:out(Q0),
            MsgQs =
                case queue:is_empty(Q) of
                    true ->
                        maps:remove(StreamId, MsgQs0);
                    false ->
                        MsgQs0#{StreamId := Q}
                end,
            {[Item], Buf#buffer{messages = MsgQs}};
        #{} ->
            {[], Buf}
    end.

%% @doc Get number of buffered messages in a given stream:
-spec len(emqx_persistent_session_ds_stream_scheduler:stream_key(), t()) -> non_neg_integer().
len(StreamId, #buffer{messages = MsgQs}) ->
    case MsgQs of
        #{StreamId := Q} ->
            queue:len(Q);
        #{} ->
            0
    end.

%%================================================================================
%% Tests
%%================================================================================

-ifdef(TEST).

push_pop_test() ->
    StreamKey = 1,
    ?assertMatch({[], _}, pop_batch(StreamKey, new())),
    Q1 = push_batch(StreamKey, 1, new()),
    Q2 = push_batch(StreamKey, 2, Q1),
    Q3 = push_batch(StreamKey, 3, Q2),
    %%
    {[1], Q3_} = pop_batch(StreamKey, Q3),
    {[2], Q4_} = pop_batch(StreamKey, Q3_),
    {[3], Q5_} = pop_batch(StreamKey, Q4_),
    {[], _} = pop_batch(StreamKey, Q5_).

multiple_streams_test() ->
    Q1 = push_batch(a, a1, new()),
    Q2 = push_batch(b, b1, Q1),
    Q3 = push_batch(a, a2, Q2),
    Q4 = push_batch(b, b2, Q3),
    %%
    {[b1], Q5} = pop_batch(b, Q4),
    {[a1], Q6} = pop_batch(a, Q5),
    {[b2], Q7} = pop_batch(b, Q6),
    {[a2], Q8} = pop_batch(a, Q7),
    {[], Q8} = pop_batch(a, Q8),
    {[], Q8} = pop_batch(b, Q8).

-endif.
