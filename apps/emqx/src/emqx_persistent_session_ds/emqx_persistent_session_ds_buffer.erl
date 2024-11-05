%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-export([new/1, len/1, len/2, push_batch/3]).

-export_type([t/0, item/0]).

-include_lib("emqx_durable_storage/include/emqx_ds.hrl").

%%================================================================================
%% Type declarations
%%================================================================================

%% Buffered poll reply:
-type item() :: #poll_reply{}.

-type q() :: queue:queue(item()).

%% Collection of per-stream buffers:
-type mqs() :: #{emqx_persistent_session_ds_stream_scheduler:stream_key() => q()}.

-record(buffer, {
    n_max :: pos_integer(),
    n_buffered = 0 :: non_neg_integer(),
    %% Per-stream buffers:
    messages = #{} :: mqs()
}).

-opaque t() :: #buffer{}.

%%================================================================================
%% API functions
%%================================================================================

-spec new(pos_integer()) -> t().
new(NMax) when is_integer(NMax), NMax > 0 ->
    #buffer{
        n_max = NMax
    }.

%% @doc Enqueue a batch of messages:
-spec push_batch(emqx_persistent_session_ds_stream_scheduler:stream_key(), item(), t()) ->
    {ok, t()} | {error, overflow}.
push_batch(_StreamId, _Item, #buffer{n_buffered = N, n_max = NMax}) when N >= NMax ->
    {error, overflow};
push_batch(StreamId, Item, Buf = #buffer{n_buffered = N, messages = MsgQs}) ->
    case MsgQs of
        #{StreamId := Q0} ->
            ok;
        #{} ->
            Q0 = queue:new()
    end,
    Q = queue:in(Item, Q0),
    % FIXME
    NNew = 1,
    Buf#buffer{
        n_buffered = N + NNew,
        messages = MsgQs#{StreamId => Q}
    }.

%% @doc Get number of buffered messages in a given stream:
-spec len(emqx_persistent_sessionb_ds_stream_scheduler:stream_key(), t()) -> non_neg_integer().
len(StreamId, #buffer{messages = MsgQs}) ->
    case MsgQs of
        #{StreamId := Q} ->
            queue:len(Q);
        #{} ->
            0
    end.

%% @doc Get total number of stored messages (excl. PUBRELs):
len(#buffer{n_buffered = N}) ->
    N.

%%================================================================================
%% Internal functions
%%================================================================================
