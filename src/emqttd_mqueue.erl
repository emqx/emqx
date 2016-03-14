%%--------------------------------------------------------------------
%% Copyright (c) 2012-2016 Feng Lee <feng@emqtt.io>.
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

-module(emqttd_mqueue).

-include("emqttd.hrl").

-include("emqttd_protocol.hrl").

-import(proplists, [get_value/3]).

-export([new/3, type/1, name/1, is_empty/1, len/1, max_len/1, in/2, out/1, stats/1]).

-define(LOW_WM, 0.2).

-define(HIGH_WM, 0.6).

-type priority() :: {iolist(), pos_integer()}.

-type option() :: {type, simple | priority}
                | {max_length, pos_integer() | infinity}
                | {priority, list(priority())}
                | {low_watermark, float()}     %% Low watermark
                | {high_watermark, float()}    %% High watermark
                | {queue_qos0, boolean()}.     %% Queue Qos0?

-type mqueue_option() :: {max_length, pos_integer()} %% Max queue length
                       | {low_watermark, float()}    %% Low watermark
                       | {high_watermark, float()}   %% High watermark
                       | {queue_qos0, boolean()}.    %% Queue Qos0

-type stat() :: {max_len, infinity | pos_integer()}
              | {len, non_neg_integer()}
              | {dropped, non_neg_integer()}.

-record(mqueue, {type :: simple | priority,
                 name, q :: queue:queue() | priority_queue:q(),
                 %% priority table
                 pseq = 0, priorities = [],
                 %% len of simple queue
                 len = 0, max_len = ?MAX_LEN,
                 low_wm = ?LOW_WM, high_wm = ?HIGH_WM,
                 qos0 = false, dropped = 0,
                 alarm_fun}).

-type mqueue() :: #mqueue{}.

-export_type([mqueue/0, priority/0, option/0]).

%% @doc New Queue.
-spec(new(iolist(), list(mqueue_option()), fun()) -> mqueue()).
new(Name, Opts, AlarmFun) ->
    Type = get_value(type, Opts, simple),
    MaxLen = get_value(max_length, Opts, infinity),
    init_q(#mqueue{type = Type, name = iolist_to_binary(Name),
                   len = 0, max_len = MaxLen,
                   low_wm = low_wm(MaxLen, Opts),
                   high_wm = high_wm(MaxLen, Opts),
                   qos0 = get_value(queue_qos0, Opts, false),
                   alarm_fun = AlarmFun}, Opts).

init_q(MQ = #mqueue{type = simple}, _Opts) ->
    MQ#mqueue{q = queue:new()};
init_q(MQ = #mqueue{type = priority}, Opts) ->
    Priorities = get_value(priority, Opts, []),
    init_p(Priorities, MQ#mqueue{q = priority_queue:new()}).

init_p([], MQ) ->
    MQ;
init_p([{Topic, P} | L], MQ) ->
    {_, MQ1} = insert_p(iolist_to_binary(Topic), P, MQ),
    init_p(L, MQ1).

insert_p(Topic, P, MQ = #mqueue{priorities = Tab, pseq = Seq}) ->
    <<PInt:48>> = <<P:8, (erlang:phash2(Topic)):32, Seq:8>>,
    {PInt, MQ#mqueue{priorities = [{Topic, PInt} | Tab], pseq = Seq + 1}}.

low_wm(infinity, _Opts) ->
    infinity;
low_wm(MaxLen, Opts) ->
    round(MaxLen * get_value(low_watermark, Opts, ?LOW_WM)).

high_wm(infinity, _Opts) ->
    infinity;
high_wm(MaxLen, Opts) ->
    round(MaxLen * get_value(high_watermark, Opts, ?HIGH_WM)).

-spec(name(mqueue()) -> iolist()).
name(#mqueue{name = Name}) ->
    Name.

-spec(type(mqueue()) -> atom()).
type(#mqueue{type = Type}) ->
    Type.

is_empty(#mqueue{type = simple, len = Len}) -> Len =:= 0;
is_empty(#mqueue{type = priority, q = Q})   -> priority_queue:is_empty(Q).

len(#mqueue{type = simple, len = Len}) -> Len;
len(#mqueue{type = priority, q = Q})   -> priority_queue:len(Q).

max_len(#mqueue{max_len= MaxLen}) -> MaxLen.

%% @doc Stats of the mqueue
-spec(stats(mqueue()) -> [stat()]).
stats(#mqueue{type = Type, q = Q, max_len = MaxLen, len = Len, dropped = Dropped}) ->
    [{len, case Type of
                simple   -> Len;
                priority -> priority_queue:len(Q)
            end} | [{max_len, MaxLen}, {dropped, Dropped}]].

%% @doc Enqueue a message.
-spec(in(mqtt_message(), mqueue()) -> mqueue()).
in(#mqtt_message{qos = ?QOS_0}, MQ = #mqueue{qos0 = false}) ->
    MQ;
in(Msg, MQ = #mqueue{type = simple, q = Q, len = Len, max_len = infinity}) ->
    MQ#mqueue{q = queue:in(Msg, Q), len = Len + 1};
in(Msg, MQ = #mqueue{type = simple, q = Q, len = Len, max_len = MaxLen, dropped = Dropped})
    when Len >= MaxLen ->
    {{value, _Old}, Q2} = queue:out(Q),
    MQ#mqueue{q = queue:in(Msg, Q2), dropped = Dropped +1};
in(Msg, MQ = #mqueue{type = simple, q = Q, len = Len}) ->
    maybe_set_alarm(MQ#mqueue{q = queue:in(Msg, Q), len = Len + 1});

in(Msg = #mqtt_message{topic = Topic}, MQ = #mqueue{type = priority, q = Q,
                                                    priorities = Priorities,
                                                    max_len = infinity}) ->
    case lists:keysearch(Topic, 1, Priorities) of
        {value, {_, Pri}} ->
            MQ#mqueue{q = priority_queue:in(Msg, Pri, Q)};
        false ->
            {Pri, MQ1} = insert_p(Topic, 0, MQ),
            MQ1#mqueue{q = priority_queue:in(Msg, Pri, Q)}
    end;
in(Msg = #mqtt_message{topic = Topic}, MQ = #mqueue{type = priority, q = Q,
                                                    priorities = Priorities,
                                                    max_len = MaxLen}) ->
    case lists:keysearch(Topic, 1, Priorities) of
        {value, {_, Pri}} ->
            case priority_queue:plen(Pri, Q) >= MaxLen of
                true ->
                    {_, Q1} = priority_queue:out(Pri, Q),
                    MQ#mqueue{q = priority_queue:in(Msg, Pri, Q1)};
                false ->
                    MQ#mqueue{q = priority_queue:in(Msg, Pri, Q)}
            end;
        false ->
            {Pri, MQ1} = insert_p(Topic, 0, MQ),
            MQ1#mqueue{q = priority_queue:in(Msg, Pri, Q)}
    end.

out(MQ = #mqueue{type = simple, len = 0}) ->
    {empty, MQ};
out(MQ = #mqueue{type = simple, q = Q, len = Len, max_len = infinity}) ->
    {R, Q2} = queue:out(Q),
    {R, MQ#mqueue{q = Q2, len = Len - 1}};
out(MQ = #mqueue{type = simple, q = Q, len = Len}) ->
    {R, Q2} = queue:out(Q),
    {R, maybe_clear_alarm(MQ#mqueue{q = Q2, len = Len - 1})};
out(MQ = #mqueue{type = priority, q = Q}) ->
    {R, Q2} = priority_queue:out(Q),
    {R, MQ#mqueue{q = Q2}}.

maybe_set_alarm(MQ = #mqueue{name = Name, len = Len, high_wm = HighWM, alarm_fun = AlarmFun})
    when Len > HighWM ->
    Alarm = #mqtt_alarm{id = iolist_to_binary(["queue_high_watermark.", Name]),
                        severity = warning,
                        title = io_lib:format("Queue ~s high-water mark", [Name]),
                        summary = io_lib:format("queue len ~p > high_watermark ~p", [Len, HighWM])},
    MQ#mqueue{alarm_fun = AlarmFun(alert, Alarm)};

maybe_set_alarm(MQ) ->
    MQ.

maybe_clear_alarm(MQ = #mqueue{name = Name, len = Len, low_wm = LowWM, alarm_fun = AlarmFun})
    when Len < LowWM ->
    MQ#mqueue{alarm_fun = AlarmFun(clear, list_to_binary(["queue_high_watermark.", Name]))};

maybe_clear_alarm(MQ) ->
    MQ.

