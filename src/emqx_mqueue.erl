%% Copyright (c) 2018 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.

%% @doc A Simple in-memory message queue.
%%
%% Notice that MQTT is not an enterprise messaging queue. MQTT assume that client
%% should be online in most of the time.
%%
%% This module implements a simple in-memory queue for MQTT persistent session.
%%
%% If the broker restarts or crashes, all queued messages will be lost.
%%
%% Concept of Message Queue and Inflight Window:
%%
%%       |<----------------- Max Len ----------------->|
%%       -----------------------------------------------
%% IN -> |      Messages Queue   |  Inflight Window    | -> Out
%%       -----------------------------------------------
%%                               |<---   Win Size  --->|
%%
%%
%% 1. Inflight Window is to store the messages
%%    that are delivered but still awaiting for puback.
%%
%% 2. Messages are enqueued to tail when the inflight window is full.
%%
%% 3. QoS=0 messages are only enqueued when `store_qos0' is given `true`
%%    in init options
%%
%% 4. If the queue is full drop the oldest one unless `max_len' is set to `0'.
%%
%% @end

-module(emqx_mqueue).

-include("emqx.hrl").
-include("emqx_mqtt.hrl").

-export([init/1, type/1]).
-export([is_empty/1]).
-export([len/1, max_len/1]).
-export([in/2, out/1]).
-export([stats/1, dropped/1]).

-define(PQUEUE, emqx_pqueue).

-type(priority() :: {iolist(), pos_integer()}).

-type(options() :: #{type       := simple | priority,
                     max_len    := non_neg_integer(),
                     priorities => list(priority()),
                     store_qos0 => boolean()}).

-type(stat() :: {len, non_neg_integer()}
              | {max_len, non_neg_integer()}
              | {dropped, non_neg_integer()}).

-record(mqueue, {
          type :: simple | priority,
          q :: queue:queue() | ?PQUEUE:q(),
          %% priority table
          priorities = [],
          pseq = 0,
          len = 0,
          max_len = 0,
          qos0 = false,
          dropped = 0
         }).

-type(mqueue() :: #mqueue{}).

-export_type([mqueue/0, priority/0, options/0]).

-spec(init(options()) -> mqueue()).
init(Opts = #{type := Type, max_len := MaxLen, store_qos0 := QoS0}) ->
    init_q(#mqueue{type = Type, len = 0, max_len = MaxLen, qos0 = QoS0}, Opts).

init_q(MQ = #mqueue{type = simple}, _Opts) ->
    MQ#mqueue{q = queue:new()};
init_q(MQ = #mqueue{type = priority}, #{priorities := Priorities}) ->
    init_pq(Priorities, MQ#mqueue{q = ?PQUEUE:new()}).

init_pq([], MQ) ->
    MQ;
init_pq([{Topic, P} | L], MQ) ->
    {_, MQ1} = insert_p(iolist_to_binary(Topic), P, MQ),
    init_pq(L, MQ1).

insert_p(Topic, P, MQ = #mqueue{priorities = L, pseq = Seq}) ->
    <<PInt:48>> = <<P:8, (erlang:phash2(Topic)):32, Seq:8>>,
    {PInt, MQ#mqueue{priorities = [{Topic, PInt} | L], pseq = Seq + 1}}.

-spec(type(mqueue()) -> simple | priority).
type(#mqueue{type = Type}) -> Type.

is_empty(#mqueue{type = simple, len = Len}) -> Len =:= 0;
is_empty(#mqueue{type = priority, q = Q})   -> ?PQUEUE:is_empty(Q).

len(#mqueue{type = simple, len = Len}) -> Len;
len(#mqueue{type = priority, q = Q})   -> ?PQUEUE:len(Q).

max_len(#mqueue{max_len = MaxLen}) -> MaxLen.

%% @doc Dropped of the mqueue
-spec(dropped(mqueue()) -> non_neg_integer()).
dropped(#mqueue{dropped = Dropped}) -> Dropped.

%% @doc Stats of the mqueue
-spec(stats(mqueue()) -> [stat()]).
stats(#mqueue{type = Type, q = Q, max_len = MaxLen, len = Len, dropped = Dropped}) ->
    [{len, case Type of
                simple   -> Len;
                priority -> ?PQUEUE:len(Q)
            end} | [{max_len, MaxLen}, {dropped, Dropped}]].

%% @doc Enqueue a message.
-spec(in(emqx_types:message(), mqueue()) -> mqueue()).
in(#message{qos = ?QOS_0}, MQ = #mqueue{qos0 = false}) ->
    MQ;
in(Msg, MQ = #mqueue{type = simple, q = Q, len = Len, max_len = 0}) ->
    MQ#mqueue{q = queue:in(Msg, Q), len = Len + 1};
in(Msg, MQ = #mqueue{type = simple, q = Q, len = Len, max_len = MaxLen, dropped = Dropped})
    when Len >= MaxLen ->
    {{value, _Old}, Q2} = queue:out(Q),
    MQ#mqueue{q = queue:in(Msg, Q2), dropped = Dropped +1};
in(Msg, MQ = #mqueue{type = simple, q = Q, len = Len}) ->
    MQ#mqueue{q = queue:in(Msg, Q), len = Len + 1};

in(Msg = #message{topic = Topic}, MQ = #mqueue{type = priority, q = Q,
                                               priorities = Priorities,
                                               max_len = 0}) ->
    case lists:keysearch(Topic, 1, Priorities) of
        {value, {_, Pri}} ->
            MQ#mqueue{q = ?PQUEUE:in(Msg, Pri, Q)};
        false ->
            {Pri, MQ1} = insert_p(Topic, priority_in_user_props(Msg), MQ),
            MQ1#mqueue{q = ?PQUEUE:in(Msg, Pri, Q)}
    end;
in(Msg = #message{topic = Topic}, MQ = #mqueue{type = priority, q = Q,
                                               priorities = Priorities,
                                               max_len = MaxLen}) ->
    case lists:keysearch(Topic, 1, Priorities) of
        {value, {_, Pri}} ->
            case ?PQUEUE:plen(Pri, Q) >= MaxLen of
                true ->
                    {_, Q1} = ?PQUEUE:out(Pri, Q),
                    MQ#mqueue{q = ?PQUEUE:in(Msg, Pri, Q1)};
                false ->
                    MQ#mqueue{q = ?PQUEUE:in(Msg, Pri, Q)}
            end;
        false ->
            {Pri, MQ1} = insert_p(Topic, priority_in_user_props(Msg), MQ),
            MQ1#mqueue{q = ?PQUEUE:in(Msg, Pri, Q)}
    end.

out(MQ = #mqueue{type = simple, len = 0}) ->
    {empty, MQ};
out(MQ = #mqueue{type = simple, q = Q, len = Len, max_len = 0}) ->
    {R, Q2} = queue:out(Q),
    {R, MQ#mqueue{q = Q2, len = Len - 1}};
out(MQ = #mqueue{type = simple, q = Q, len = Len}) ->
    {R, Q2} = queue:out(Q),
    {R, MQ#mqueue{q = Q2, len = Len - 1}};
out(MQ = #mqueue{type = priority, q = Q}) ->
    {R, Q2} = ?PQUEUE:out(Q),
    {R, MQ#mqueue{q = Q2}}.

priority_in_user_props(#message{headers = #{'User-Property' := UserProperty}}) ->
    case lists:keyfind(<<"priority">>, 1, UserProperty) of 
        false ->
            0;
        {<<"priority">>, Priority} when is_binary(Priority) ->
            list_to_integer(binary_to_list(Priority));
        _ ->
            0
    end;
priority_in_user_props(_) ->
    0.