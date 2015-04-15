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
%%% MQTT Packet Header.
%%%
%%% @end
%%%-----------------------------------------------------------------------------

%%------------------------------------------------------------------------------
%% Max ClientId Length. Why 1024? NiDongDe!
%%------------------------------------------------------------------------------
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

-define(TYPE_NAMES, [
    'CONNECT',
    'CONNACK',
    'PUBLISH',
    'PUBACK',
    'PUBREC',
    'PUBREL',
    'PUBCOMP',
    'SUBSCRIBE',
    'SUBACK',
    'UNSUBSCRIBE',
    'UNSUBACK',
    'PINGREQ',
    'PINGRESP',
    'DISCONNECT']).

-type mqtt_packet_type() :: ?RESERVED..?DISCONNECT.

%%------------------------------------------------------------------------------
%% MQTT Connect Return Codes
%%------------------------------------------------------------------------------
-define(CONNACK_ACCEPT,      0).    %% Connection accepted
-define(CONNACK_PROTO_VER,   1).    %% Unacceptable protocol version
-define(CONNACK_INVALID_ID,  2).    %% Client Identifier is correct UTF-8 but not allowed by the Server
-define(CONNACK_SERVER,      3).    %% Server unavailable
-define(CONNACK_CREDENTIALS, 4).    %% Username or password is malformed
-define(CONNACK_AUTH,        5).    %% Client is not authorized to connect

-type mqtt_connack() :: ?CONNACK_ACCEPT..?CONNACK_AUTH.

%%------------------------------------------------------------------------------
%% MQTT Parser and Serialiser
%%------------------------------------------------------------------------------
-define(MAX_LEN, 16#fffffff).
-define(HIGHBIT, 2#10000000).
-define(LOWBITS, 2#01111111).

%%------------------------------------------------------------------------------
%% MQTT Packet Fixed Header
%%------------------------------------------------------------------------------
-record(mqtt_packet_header, {
    type   = ?RESERVED  :: mqtt_packet_type(),
    dup    = false      :: boolean(),
    qos    = ?QOS_0     :: mqtt_qos(),
    retain = false      :: boolean()}).

%%------------------------------------------------------------------------------
%% MQTT Packets
%%------------------------------------------------------------------------------
-type mqtt_clientid()  :: binary().
-type mqtt_packet_id() :: 1..16#ffff | undefined.

-record(mqtt_packet_connect,  {
    clientid    = <<>>              :: mqtt_clientid(),
    proto_ver   = ?MQTT_PROTO_V311  :: mqtt_vsn(),
    proto_name  = <<"MQTT">>        :: binary(),
    will_retain = false             :: boolean(),
    will_qos    = ?QOS_0            :: mqtt_qos(),
    will_flag   = false             :: boolean(),
    clean_sess  = false             :: boolean(),
    keep_alive  = 60                :: non_neg_integer(),
    will_topic  = undefined         :: undefined | binary(),
    will_msg    = undefined         :: undefined | binary(),
    username    = undefined         :: undefined | binary(),
    password    = undefined         :: undefined | binary()}).

-record(mqtt_packet_connack, {
    ack_flags = ?RESERVED   :: 0 | 1,
    return_code             :: mqtt_connack() }).

-record(mqtt_packet_publish, {
    topic_name  :: binary(),
    packet_id   :: mqtt_packet_id() }).

-record(mqtt_packet_puback, {
    packet_id   :: mqtt_packet_id() }).

-record(mqtt_packet_subscribe, {
    packet_id   :: mqtt_packet_id(),
    topic_table :: list({binary(), mqtt_qos()}) }).

-record(mqtt_packet_unsubscribe, {
    packet_id   :: mqtt_packet_id(),
    topics      :: list(binary()) }).

-record(mqtt_packet_suback, {
    packet_id   :: mqtt_packet_id(),
    qos_table   :: list(mqtt_qos() | 128) }).

-record(mqtt_packet_unsuback, {
    packet_id   :: mqtt_packet_id() }).

%%------------------------------------------------------------------------------
%% MQTT Control Packet
%%------------------------------------------------------------------------------
-record(mqtt_packet, {
    header    :: #mqtt_packet_header{},
    variable  :: #mqtt_packet_connect{} | #mqtt_packet_connack{}
                | #mqtt_packet_publish{} | #mqtt_packet_puback{}
                | #mqtt_packet_subscribe{} | #mqtt_packet_suback{}
                | #mqtt_packet_unsubscribe{} | #mqtt_packet_unsuback{}
                | mqtt_packet_id() | undefined,
    payload   :: binary() | undefined }).

-type mqtt_packet() :: #mqtt_packet{}.

%%------------------------------------------------------------------------------
%% MQTT Packet Match
%%------------------------------------------------------------------------------
-define(CONNECT_PACKET(Var),
    #mqtt_packet{header = #mqtt_packet_header{type = ?CONNECT}, variable = Var}).

-define(CONNACK_PACKET(ReturnCode),
    #mqtt_packet{header = #mqtt_packet_header{type = ?CONNACK},
                 variable = #mqtt_packet_connack{return_code = ReturnCode}}).

-define(PUBLISH_PACKET(Qos, Topic, PacketId, Payload),
    #mqtt_packet{header = #mqtt_packet_header{type = ?PUBLISH,
                                              qos = Qos},
                 variable = #mqtt_packet_publish{topic_name = Topic,
                                                 packet_id  = PacketId},
                 payload = Payload}).

-define(PUBACK_PACKET(Type, PacketId),
    #mqtt_packet{header = #mqtt_packet_header{type = Type},
                 variable = #mqtt_packet_puback{packet_id = PacketId}}).

-define(PUBREL_PACKET(PacketId),
    #mqtt_packet{header   = #mqtt_packet_header{type = ?PUBREL, qos = ?QOS_1},
                 variable = #mqtt_packet_puback{packet_id = PacketId}}).

-define(SUBSCRIBE_PACKET(PacketId, TopicTable), 
    #mqtt_packet{header = #mqtt_packet_header{type = ?SUBSCRIBE, qos = ?QOS_1},
                 variable = #mqtt_packet_subscribe{packet_id   = PacketId,
                                                   topic_table = TopicTable}}).
-define(SUBACK_PACKET(PacketId, QosTable),
    #mqtt_packet{header = #mqtt_packet_header{type = ?SUBACK},
                 variable = #mqtt_packet_suback{packet_id = PacketId,
                                                qos_table = QosTable}}).
-define(UNSUBSCRIBE_PACKET(PacketId, Topics), 
    #mqtt_packet{header = #mqtt_packet_header{type = ?UNSUBSCRIBE, qos = ?QOS_1},
                 variable = #mqtt_packet_unsubscribe{packet_id = PacketId,
                                                     topics    = Topics}}).
-define(UNSUBACK_PACKET(PacketId),
    #mqtt_packet{header = #mqtt_packet_header{type = ?UNSUBACK},
                 variable = #mqtt_packet_unsuback{packet_id = PacketId}}).

-define(PACKET(Type),
    #mqtt_packet{header = #mqtt_packet_header{type = Type}}).


