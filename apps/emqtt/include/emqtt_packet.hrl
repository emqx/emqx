%%------------------------------------------------------------------------------
%% Copyright (c) 2015, Feng Lee <feng@emqtt.io>
%% 
%% Permission is hereby granted, free of charge, to any person obtaining a copy
%% of this software and associated documentation files (the "Software"), to deal
%% in the Software without restriction, including without limitation the rights
%% to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
%% copies of the Software, and to permit persons to whom the Software is
%% furnished to do so, subject to the following conditions:
%% 
%% The above copyright notice and this permission notice shall be included in all
%% copies or substantial portions of the Software.
%% 
%% THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
%% IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
%% FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
%% AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
%% LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
%% OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
%% SOFTWARE.
%%------------------------------------------------------------------------------
%%
%% The Original Code is from RabbitMQ.
%%

%%------------------------------------------------------------------------------
%% MQTT Protocol Version and Levels
%%------------------------------------------------------------------------------
-define(MQTT_PROTO_V31, 3).
-define(MQTT_PROTO_V311, 4).

-define(PROTOCOL_NAMES, [{?MQTT_PROTO_V31, <<"MQIsdp">>}, {?MQTT_PROTO_V311, <<"MQTT">>}]).

-define(MAX_CLIENTID_LEN, 1024).

%%------------------------------------------------------------------------------
%% MQTT Control Packet Types
%%------------------------------------------------------------------------------
-define(RESERVED,     0).   %% Reserved
-define(CONNECT,      1).   %% Client request to connect to Server
-define(CONNACK,      2).   %% Server to Client: Connect acknowledgment
-define(PUBLISH,      3).   %% Publish message
-define(PUBACK,       4).   %% Publish acknowledgment
-define(PUBREC,       5).   %% Publish received (assured delivery part 1)
-define(PUBREL,       6).   %% Publish release (assured delivery part 2)
-define(PUBCOMP,      7).   %% Publish complete (assured delivery part 3)
-define(SUBSCRIBE,    8).   %% Client subscribe request
-define(SUBACK,       9).   %% Server Subscribe acknowledgment
-define(UNSUBSCRIBE, 10).   %% Unsubscribe request
-define(UNSUBACK,    11).   %% Unsubscribe acknowledgment
-define(PINGREQ,     12).   %% PING request
-define(PINGRESP,    13).   %% PING response
-define(DISCONNECT,  14).   %% Client is disconnecting

%%------------------------------------------------------------------------------
%% MQTT Connect Return Codes
%%------------------------------------------------------------------------------
-define(CONNACK_ACCEPT,      0).    %% Connection accepted
-define(CONNACK_PROTO_VER,   1).    %% Unacceptable protocol version
-define(CONNACK_INVALID_ID,  2).    %% Client Identifier is correct UTF-8 but not allowed by the Server
-define(CONNACK_SERVER,      3).    %% Server unavailable
-define(CONNACK_CREDENTIALS, 4).    %% Username or password is malformed
-define(CONNACK_AUTH,        5).    %% Client is not authorized to connect

%%------------------------------------------------------------------------------
%% MQTT Erlang Types
%%------------------------------------------------------------------------------
-type mqtt_packet_type()  :: ?RESERVED..?DISCONNECT.

-type mqtt_packet_id() :: 1..16#ffff | undefined.


%%------------------------------------------------------------------------------
%% MQTT Packet Fixed Header
%%------------------------------------------------------------------------------
-record(mqtt_packet_header, {
    type   = ?RESERVED  :: mqtt_packet_type(),
    dup    = false      :: boolean(),
    qos    = 0          :: 0 | 1 | 2,
    retain = false      :: boolean() }).

%%------------------------------------------------------------------------------
%% MQTT Packets
%%------------------------------------------------------------------------------
-record(mqtt_packet_connect,  {
    proto_ver,
    proto_name,
    will_retain,
    will_qos,
    will_flag,
    clean_sess,
    keep_alive,
    client_id,
    will_topic,
    will_msg,
    username,
    password }).

-record(mqtt_packet_connack, {
    ack_flags = ?RESERVED,
	return_code }).

-record(mqtt_packet_publish,  {
    topic_name  :: binary(),
    packet_id   :: mqtt_packet_id() }).

-record(mqtt_packet_puback,  { 
    packet_id   :: mqtt_packet_id() }).

-record(mqtt_topic, {
    name    :: binary(),
    qos     :: 0 | 1 | 2 }).

-record(mqtt_packet_subscribe, {
    packet_id   :: mqtt_packet_id(),
    topic_table :: list(#mqtt_topic{}) }).

-record(mqtt_packet_suback, {
    packet_id   :: mqtt_packet_id(),
    qos_table = [] }).

%%------------------------------------------------------------------------------
%% MQTT Control Packet
%%------------------------------------------------------------------------------
-record(mqtt_packet, {
    header    :: #mqtt_packet_header{},
    variable  :: #mqtt_packet_connect{} | #mqtt_packet_connack{} 
               | #mqtt_packet_publish{} | #mqtt_packet_puback{} 
               | #mqtt_packet_subscribe{} | #mqtt_packet_suback{}
               | mqtt_packet_id(), 
    payload   :: binary() }).

-type mqtt_packet() :: #mqtt_packet{}.


