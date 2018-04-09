%%--------------------------------------------------------------------
%% Copyright (c) 2013-2018 EMQ Inc. All rights reserved.
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

-module(emqx_mqtt_metrics).

-include("emqx_mqtt.hrl").

-import(emqx_metrics, [inc/1]).

-export([init/0]).

%% Received/Sent Metrics
-export([received/1, sent/1]).

%% Bytes sent and received of Broker
-define(SYSTOP_BYTES, [
    {counter, 'bytes/received'},   % Total bytes received
    {counter, 'bytes/sent'}        % Total bytes sent
]).

%% Packets sent and received of Broker
-define(SYSTOP_PACKETS, [
    {counter, 'packets/received'},         % All Packets received
    {counter, 'packets/sent'},             % All Packets sent
    {counter, 'packets/connect'},          % CONNECT Packets received
    {counter, 'packets/connack'},          % CONNACK Packets sent
    {counter, 'packets/publish/received'}, % PUBLISH packets received
    {counter, 'packets/publish/sent'},     % PUBLISH packets sent
    {counter, 'packets/puback/received'},  % PUBACK packets received
    {counter, 'packets/puback/sent'},      % PUBACK packets sent
    {counter, 'packets/puback/missed'},    % PUBACK packets missed
    {counter, 'packets/pubrec/received'},  % PUBREC packets received
    {counter, 'packets/pubrec/sent'},      % PUBREC packets sent
    {counter, 'packets/pubrec/missed'},    % PUBREC packets missed
    {counter, 'packets/pubrel/received'},  % PUBREL packets received
    {counter, 'packets/pubrel/sent'},      % PUBREL packets sent
    {counter, 'packets/pubrel/missed'},    % PUBREL packets missed
    {counter, 'packets/pubcomp/received'}, % PUBCOMP packets received
    {counter, 'packets/pubcomp/sent'},     % PUBCOMP packets sent
    {counter, 'packets/pubcomp/missed'},   % PUBCOMP packets missed
    {counter, 'packets/subscribe'},        % SUBSCRIBE Packets received 
    {counter, 'packets/suback'},           % SUBACK packets sent 
    {counter, 'packets/unsubscribe'},      % UNSUBSCRIBE Packets received
    {counter, 'packets/unsuback'},         % UNSUBACK Packets sent
    {counter, 'packets/pingreq'},          % PINGREQ packets received
    {counter, 'packets/pingresp'},         % PINGRESP Packets sent
    {counter, 'packets/disconnect'}        % DISCONNECT Packets received 
]).

%% Messages sent and received of broker
-define(SYSTOP_MESSAGES, [
    {counter, 'messages/received'},      % All Messages received
    {counter, 'messages/sent'},          % All Messages sent
    {counter, 'messages/qos0/received'}, % QoS0 Messages received
    {counter, 'messages/qos0/sent'},     % QoS0 Messages sent
    {counter, 'messages/qos1/received'}, % QoS1 Messages received
    {counter, 'messages/qos1/sent'},     % QoS1 Messages sent
    {counter, 'messages/qos2/received'}, % QoS2 Messages received
    {counter, 'messages/qos2/sent'},     % QoS2 Messages sent
    {counter, 'messages/qos2/dropped'},  % QoS2 Messages dropped
    {gauge,   'messages/retained'},      % Messagea retained
    {counter, 'messages/dropped'},       % Messages dropped
    {counter, 'messages/forward'}        % Messages forward
]).

% Init metrics
init() ->
    lists:foreach(fun emqx_metrics:create/1,
                  ?SYSTOP_BYTES ++ ?SYSTOP_PACKETS ++ ?SYSTOP_MESSAGES).

%% @doc Count packets received.
-spec(received(mqtt_packet()) -> ok).
received(Packet) ->
    inc('packets/received'),
    received1(Packet).
received1(?PUBLISH_PACKET(Qos, _PktId)) ->
    inc('packets/publish/received'),
    inc('messages/received'),
    qos_received(Qos);
received1(?PACKET(Type)) ->
    received2(Type).
received2(?CONNECT) ->
    inc('packets/connect');
received2(?PUBACK) ->
    inc('packets/puback/received');
received2(?PUBREC) ->
    inc('packets/pubrec/received');
received2(?PUBREL) ->
    inc('packets/pubrel/received');
received2(?PUBCOMP) ->
    inc('packets/pubcomp/received');
received2(?SUBSCRIBE) ->
    inc('packets/subscribe');
received2(?UNSUBSCRIBE) ->
    inc('packets/unsubscribe');
received2(?PINGREQ) ->
    inc('packets/pingreq');
received2(?DISCONNECT) ->
    inc('packets/disconnect');
received2(_) ->
    ignore.
qos_received(?QOS_0) ->
    inc('messages/qos0/received');
qos_received(?QOS_1) ->
    inc('messages/qos1/received');
qos_received(?QOS_2) ->
    inc('messages/qos2/received').

%% @doc Count packets received. Will not count $SYS PUBLISH.
-spec(sent(mqtt_packet()) -> ignore | non_neg_integer()).
sent(?PUBLISH_PACKET(_Qos, <<"$SYS/", _/binary>>, _, _)) ->
    ignore;
sent(Packet) ->
    inc('packets/sent'),
    sent1(Packet).
sent1(?PUBLISH_PACKET(Qos, _PktId)) ->
    inc('packets/publish/sent'),
    inc('messages/sent'),
    qos_sent(Qos);
sent1(?PACKET(Type)) ->
    sent2(Type).
sent2(?CONNACK) ->
    inc('packets/connack');
sent2(?PUBACK) ->
    inc('packets/puback/sent');
sent2(?PUBREC) ->
    inc('packets/pubrec/sent');
sent2(?PUBREL) ->
    inc('packets/pubrel/sent');
sent2(?PUBCOMP) ->
    inc('packets/pubcomp/sent');
sent2(?SUBACK) ->
    inc('packets/suback');
sent2(?UNSUBACK) ->
    inc('packets/unsuback');
sent2(?PINGRESP) ->
    inc('packets/pingresp');
sent2(_Type) ->
    ignore.
qos_sent(?QOS_0) ->
    inc('messages/qos0/sent');
qos_sent(?QOS_1) ->
    inc('messages/qos1/sent');
qos_sent(?QOS_2) ->
    inc('messages/qos2/sent').

