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
%%% simple message queue.
%%%
%%% Notice that MQTT is not an enterprise messaging queue. MQTT assume that client
%%% should be online in most of the time.
%%%
%%% This module wraps an erlang queue to store offline messages temporarily for MQTT
%%% persistent session.
%%%
%%% If the broker restarted or crashed, all the messages stored will be gone.
%%%
%%% @end
%%%-----------------------------------------------------------------------------

-module(emqttd_mqueue).

-author("Feng Lee <feng@emqtt.io>").

-include_lib("emqtt/include/emqtt.hrl").

-export([new/2, name/1,
         is_empty/1, len/1,
         in/2, out/1,
         peek/1,
         to_list/1]).

%% in_r/2, out_r/1,

-define(MAX_LEN, 600).

-define(HIGH_WM, 0.6).

-define(LOW_WM, 0.2).

-record(mqueue, {name,
                 len = 0,
                 max_len = ?MAX_LEN,
                 queue = queue:new(),
                 store_qos0 = false,
                 high_watermark = ?HIGH_WM,
                 low_watermark = ?LOW_WM,
                 alert = false}).

-type mqueue() :: #mqueue{}.

-type queue_option() :: {max_queued_messages, pos_integer()} %% Max messages queued
                      | {high_queue_watermark, float()}      %% High watermark
                      | {low_queue_watermark, float()}       %% Low watermark
                      | {queue_qos0_messages, boolean()}.    %% Queue Qos0 messages?

%%------------------------------------------------------------------------------
%% @doc New Queue.
%% @end
%%------------------------------------------------------------------------------
-spec new(binary() | string(), list(queue_option())) -> mqueue().
new(Name, Opts) ->
    MaxLen = emqttd_opts:g(max_queued_messages, Opts, ?MAX_LEN),
    HighWM = round(MaxLen * emqttd_opts:g(high_queue_watermark, Opts, ?HIGH_WM)),
    LowWM  = round(MaxLen * emqttd_opts:g(low_queue_watermark, Opts, ?LOW_WM)),
    #mqueue{name = Name, max_len = MaxLen,
            store_qos0 = emqttd_opts:g(queue_qos0_messages, Opts, false),
            high_watermark = HighWM, low_watermark = LowWM}.

name(#mqueue{name = Name}) ->
    Name.

len(#mqueue{len = Len}) ->
    Len.

is_empty(#mqueue{len = 0}) -> true;
is_empty(_Q)               -> false.

%%------------------------------------------------------------------------------
%% @doc
%% Queue one message.
%%
%% @end
%%------------------------------------------------------------------------------
-spec in(mqtt_message(), mqueue()) -> mqueue().
in(#mqtt_message{qos = ?QOS_0}, MQ = #mqueue{store_qos0 = false}) ->
    MQ;
%% queue is full, drop the oldest
in(Msg, MQ = #mqueue{name = Name, len = Len, max_len = MaxLen, queue = Q}) when Len =:= MaxLen ->
    Q2 = case queue:out(Q) of
        {{value, OldMsg}, Q1} ->
            %%TODO: publish the dropped message to $SYS?
            lager:error("Queue(~s) drop message: ~p", [Name, OldMsg]),
            Q1;
        {empty, Q1} -> %% maybe max_len is 1
            Q1
    end,
    MQ#mqueue{queue = queue:in(Msg, Q2)};
in(Msg, MQ = #mqueue{len = Len, queue = Q}) ->
    maybe_set_alarm(MQ#mqueue{len = Len+1, queue = queue:in(Msg, Q)}).

out(MQ = #mqueue{len = 0, queue = _Q}) ->
    {empty, MQ};
out(MQ = #mqueue{len = Len, queue = Q}) ->
    {Result, Q1} = queue:out(Q),
    {Result, maybe_clear_alarm(MQ#mqueue{len = Len - 1, queue = Q1})}.

peek(#mqueue{queue = Q}) ->
    queue:peek(Q).

to_list(#mqueue{queue = Q}) ->
    queue:to_list(Q).

maybe_set_alarm(MQ = #mqueue{name = Name, len = Len, high_watermark = HighWM, alert = false})
    when Len >= HighWM ->
    AlarmDescr = io_lib:format("len ~p > high_watermark ~p", [Len, HighWM]),
    emqttd_alarm:set_alarm({{queue_high_watermark, Name}, AlarmDescr}),
    MQ#mqueue{alert = true};
maybe_set_alarm(MQ) ->
    MQ.

maybe_clear_alarm(MQ = #mqueue{name = Name, len = Len, low_watermark = LowWM, alert = true})
    when Len =< LowWM ->
    emqttd_alarm:clear_alarm({queue_high_watermark, Name}), MQ#mqueue{alert = false};
maybe_clear_alarm(MQ) ->
    MQ.

