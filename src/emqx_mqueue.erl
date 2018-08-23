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

%% TODO: should be a bound queue.
%% @doc A Simple in-memory message queue.
%%
%% Notice that MQTT is not an enterprise messaging queue. MQTT assume that client
%% should be online in most of the time.
%%
%% This module implements a simple in-memory queue for MQTT persistent session.
%%
%% If the broker restarted or crashed, all the messages queued will be gone.
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
%% 1. Inflight Window to store the messages delivered and awaiting for puback.
%%
%% 2. Enqueue messages when the inflight window is full.
%%
%% 3. If the queue is full, dropped qos0 messages if store_qos0 is true,
%%    otherwise dropped the oldest one.
%%
%% @end

%% TODO: ...
-module(emqx_mqueue).

-include("emqx.hrl").
-include("emqx_mqtt.hrl").

-import(proplists, [get_value/3]).

-export([new/2, type/1, name/1, is_empty/1, len/1, max_len/1, in/2, out/1]).
-export([dropped/1, stats/1]).

-define(PQUEUE, emqx_pqueue).

-type(priority() :: {iolist(), pos_integer()}).

-type(options() :: #{type       => simple | priority,
                     max_len    => non_neg_integer(),
                     priority   => list(priority()),
                     store_qos0 => boolean()}).

-type(stat() :: {max_len, non_neg_integer()}
              | {len, non_neg_integer()}
              | {dropped, non_neg_integer()}).

-record(mqueue, {type :: simple | priority,
                 name, q :: queue:queue() | ?PQUEUE:q(),
                 %% priority table
                 pseq = 0, priorities = [],
                 %% len of simple queue
                 len = 0, max_len = 0,
                 qos0 = false, dropped = 0}).

-type(mqueue() :: #mqueue{}).

-export_type([mqueue/0, priority/0, options/0]).

-spec(new(iolist(), options()) -> mqueue()).
new(Name, #{type := Type, max_len := MaxLen, store_qos0 := StoreQos0}) ->
    init_q(#mqueue{type = Type, name = iolist_to_binary(Name),
                   len = 0, max_len = MaxLen, qos0 = StoreQos0}).

init_q(MQ = #mqueue{type = simple}) ->
    MQ#mqueue{q = queue:new()};
init_q(MQ = #mqueue{type = priority}) ->
    %%Priorities = get_value(priority, Opts, []),
    init_p([], MQ#mqueue{q = ?PQUEUE:new()}).

init_p([], MQ) ->
    MQ;
init_p([{Topic, P} | L], MQ) ->
    {_, MQ1} = insert_p(iolist_to_binary(Topic), P, MQ),
    init_p(L, MQ1).

insert_p(Topic, P, MQ = #mqueue{priorities = Tab, pseq = Seq}) ->
    <<PInt:48>> = <<P:8, (erlang:phash2(Topic)):32, Seq:8>>,
    {PInt, MQ#mqueue{priorities = [{Topic, PInt} | Tab], pseq = Seq + 1}}.

-spec(name(mqueue()) -> iolist()).
name(#mqueue{name = Name}) ->
    Name.

-spec(type(mqueue()) -> atom()).
type(#mqueue{type = Type}) ->
    Type.

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
-spec(in(message(), mqueue()) -> mqueue()).
in(#message{flags = #{qos := ?QOS_0}}, MQ = #mqueue{qos0 = false}) ->
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
            {Pri, MQ1} = insert_p(Topic, 0, MQ),
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
            {Pri, MQ1} = insert_p(Topic, 0, MQ),
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

