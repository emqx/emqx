%%%-----------------------------------------------------------------------------
%%% @Copyright (C) 2012-2015, Feng Lee <feng@emqtt.io>
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
%%% MQTT Common Header.
%%%
%%% @end
%%%-----------------------------------------------------------------------------

%%------------------------------------------------------------------------------
%% MQTT Protocol Version and Levels
%%------------------------------------------------------------------------------
-define(MQTT_PROTO_V31,  3).
-define(MQTT_PROTO_V311, 4).

-define(PROTOCOL_NAMES, [
    {?MQTT_PROTO_V31, <<"MQIsdp">>},
    {?MQTT_PROTO_V311, <<"MQTT">>}]).

-type mqtt_vsn() :: ?MQTT_PROTO_V31 | ?MQTT_PROTO_V311.

%%------------------------------------------------------------------------------
%% QoS Levels
%%------------------------------------------------------------------------------

-define(QOS_0, 0).
-define(QOS_1, 1).
-define(QOS_2, 2).

-define(IS_QOS(I), (I >= ?QOS_0 andalso I =< ?QOS_2)).

-type mqtt_qos() :: ?QOS_0 | ?QOS_1 | ?QOS_2.

%%------------------------------------------------------------------------------
%% MQTT Message
%%------------------------------------------------------------------------------

-type mqtt_msgid() :: undefined | 1..16#ffff.

-record(mqtt_message, {
    %% topic is first for message may be retained
    topic           :: binary(),
    qos    = ?QOS_0 :: mqtt_qos(),
    retain = false  :: boolean(),
    dup    = false  :: boolean(),
    msgid           :: mqtt_msgid(),
    payload         :: binary()
}).

-type mqtt_message() :: #mqtt_message{}.

