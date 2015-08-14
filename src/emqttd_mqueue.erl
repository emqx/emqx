%%%-----------------------------------------------------------------------------
%%% Copyright (c) 2012-2015 eMQTT.IO, All Rights Reserved.
%%%
%%% Permission is hereby granted, free of charge, to any person obtaining a copy
%%% of this software and associated documentation files (the "Software"), to deal
%%% in the Software without restriction, including without limitation the rights
%%% to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
%%% copies of the Software, and to permit persons to whom the Software is
%%% furnished to do so, subject to the following conditions:
%%%
%%% The above copyright notice and this permission notice shall be included in all
%%% copies or substantial portions of the Software.
%%%
%%% THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
%%% IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
%%% FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
%%% AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
%%% LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
%%% OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
%%% SOFTWARE.
%%%-----------------------------------------------------------------------------
%%% @doc
%%%
%%% A Simple in-memory message queue.
%%%
%%% Notice that MQTT is not an enterprise messaging queue. MQTT assume that client
%%% should be online in most of the time.
%%%
%%% This module implements a simple in-memory queue for MQTT persistent session.
%%%
%%% If the broker restarted or crashed, all the messages queued will be gone.
%%% 
%%% Desgin of The Queue:
%%%       |<----------------- Max Len ----------------->|
%%%       -----------------------------------------------
%%% IN -> |       Pending Messages   | Inflight Window  | -> Out
%%%       -----------------------------------------------
%%%                                  |<--- Win Size --->|
%%%
%%%
%%% 1. Inflight Window to store the messages awaiting for ack.
%%%
%%% 2. Suspend IN messages when the queue is deactive, or inflight windows is full.
%%%
%%% 3. If the queue is full, dropped qos0 messages if store_qos0 is true,
%%%    otherwise dropped the oldest pending one.
%%%
%%% @end
%%%-----------------------------------------------------------------------------

-module(emqttd_mqueue).

-author("Feng Lee <feng@emqtt.io>").

-include("emqttd.hrl").

-include("emqttd_protocol.hrl").

-export([new/3, name/1,
         is_empty/1, is_full/1,
         len/1, max_len/1,
         in/2, out/1,
         stats/1]).

-define(LOW_WM, 0.2).

-define(HIGH_WM, 0.6).

-record(mqueue, {name,
                 q          = queue:new(), %% pending queue
                 len        = 0,           %% current queue len
                 low_wm     = ?LOW_WM,
                 high_wm    = ?HIGH_WM,
                 max_len    = ?MAX_LEN,
                 qos0       = false,
                 dropped    = 0,
                 alarm_fun}).

-type mqueue() :: #mqueue{}.

-type mqueue_option() :: {max_length, pos_integer()}      %% Max queue length
                       | {low_watermark, float()}         %% Low watermark
                       | {high_watermark, float()}        %% High watermark
                       | {queue_qos0, boolean()}.         %% Queue Qos0

-export_type([mqueue/0]).

%%------------------------------------------------------------------------------
%% @doc New Queue.
%% @end
%%------------------------------------------------------------------------------
-spec new(binary(), list(mqueue_option()), fun()) -> mqueue().
new(Name, Opts, AlarmFun) ->
    MaxLen = emqttd_opts:g(max_length, Opts, 1000),
    #mqueue{name     = Name,
            max_len  = MaxLen,
            low_wm   = round(MaxLen * emqttd_opts:g(low_watermark, Opts, ?LOW_WM)),
            high_wm  = round(MaxLen * emqttd_opts:g(high_watermark, Opts, ?HIGH_WM)),
            qos0     = emqttd_opts:g(queue_qos0, Opts, true),
            alarm_fun = AlarmFun}.

name(#mqueue{name = Name}) ->
    Name.

is_empty(#mqueue{len = 0}) -> true;
is_empty(_MQ)              -> false.

is_full(#mqueue{len = Len, max_len = MaxLen})
    when Len =:= MaxLen -> true;
is_full(_MQ) -> false.

len(#mqueue{len = Len}) -> Len.

max_len(#mqueue{max_len= MaxLen}) -> MaxLen.

stats(#mqueue{max_len = MaxLen, len = Len, dropped = Dropped}) ->
    [{max_len, MaxLen}, {len, Len}, {dropped, Dropped}].

%%------------------------------------------------------------------------------
%% @doc Queue one message.
%% @end
%%------------------------------------------------------------------------------

-spec in(mqtt_message(), mqueue()) -> mqueue().
%% drop qos0
in(#mqtt_message{qos = ?QOS_0}, MQ = #mqueue{qos0 = false}) ->
    MQ;

%% simply drop the oldest one if queue is full, improve later
in(Msg, MQ = #mqueue{q = Q, len = Len, max_len = MaxLen, dropped = Dropped})
    when Len =:= MaxLen ->
    {{value, _OldMsg}, Q2} = queue:out(Q),
    %lager:error("MQueue(~s) drop ~s", [Name, emqttd_message:format(OldMsg)]),
    MQ#mqueue{q = queue:in(Msg, Q2), dropped = Dropped +1};

in(Msg, MQ = #mqueue{q = Q, len = Len}) ->
    maybe_set_alarm(MQ#mqueue{q = queue:in(Msg, Q), len = Len + 1}).

out(MQ = #mqueue{len = 0}) ->
    {empty, MQ};

out(MQ = #mqueue{q = Q, len = Len}) ->
    {Result, Q2} = queue:out(Q),
    {Result, maybe_clear_alarm(MQ#mqueue{q = Q2, len = Len - 1})}.

maybe_set_alarm(MQ = #mqueue{name = Name, len = Len, high_wm = HighWM, alarm_fun = AlarmFun})
    when Len > HighWM ->
    Alarm = #mqtt_alarm{id = list_to_binary(["queue_high_watermark.", Name]),
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

