%%--------------------------------------------------------------------
%% Copyright (c) 2013-2018 EMQ Enterprise, Inc. All Rights Reserved.
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

%%--------------------------------------------------------------------
%% MQTT SockOpts
%%--------------------------------------------------------------------

-define(MQTT_SOCKOPTS, [binary, {packet, raw}, {reuseaddr, true},
                        {backlog, 512}, {nodelay, true}]).

%%--------------------------------------------------------------------
%% MQTT Protocol Version and Levels
%%--------------------------------------------------------------------

-define(MQTT_PROTO_V3, 3).
-define(MQTT_PROTO_V4, 4).
-define(MQTT_PROTO_V5, 5).

-define(PROTOCOL_NAMES, [
    {?MQTT_PROTO_V3, <<"MQIsdp">>},
    {?MQTT_PROTO_V4, <<"MQTT">>},
    {?MQTT_PROTO_V5, <<"MQTT">>}]).

-type(mqtt_vsn() :: ?MQTT_PROTO_V3 | ?MQTT_PROTO_V4 | ?MQTT_PROTO_V5).

%%--------------------------------------------------------------------
%% MQTT QoS Level
%%--------------------------------------------------------------------

-define(QOS_0, 0). %% At most once
-define(QOS_1, 1). %% At least once
-define(QOS_2, 2). %% Exactly once

-define(QOS0, 0). %% At most once
-define(QOS1, 1). %% At least once
-define(QOS2, 2). %% Exactly once

-define(IS_QOS(I), (I >= ?QOS0 andalso I =< ?QOS2)).

-type(mqtt_qos() :: ?QOS0 | ?QOS1 | ?QOS2).

-type(mqtt_qos_name() :: qos0 | at_most_once  |
                         qos1 | at_least_once |
                         qos2 | exactly_once).

-define(QOS_I(Name),
    begin
        (case Name of
            ?QOS_0        -> ?QOS_0;
            qos0          -> ?QOS_0;
            at_most_once  -> ?QOS_0;
            ?QOS_1        -> ?QOS_1;
            qos1          -> ?QOS_1;
            at_least_once -> ?QOS_1;
            ?QOS_2        -> ?QOS_2;
            qos2          -> ?QOS_2;
            exactly_once  -> ?QOS_2
        end)
    end).

%%--------------------------------------------------------------------
%% Max ClientId Length. Why 1024?
%%--------------------------------------------------------------------

-define(MAX_CLIENTID_LEN, 1024).

%%--------------------------------------------------------------------
%% MQTT Control Packet Types
%%--------------------------------------------------------------------

-define(RESERVED,     0). %% Reserved
-define(CONNECT,      1). %% Client request to connect to Server
-define(CONNACK,      2). %% Server to Client: Connect acknowledgment
-define(PUBLISH,      3). %% Publish message
-define(PUBACK,       4). %% Publish acknowledgment
-define(PUBREC,       5). %% Publish received (assured delivery part 1)
-define(PUBREL,       6). %% Publish release (assured delivery part 2)
-define(PUBCOMP,      7). %% Publish complete (assured delivery part 3)
-define(SUBSCRIBE,    8). %% Client subscribe request
-define(SUBACK,       9). %% Server Subscribe acknowledgment
-define(UNSUBSCRIBE, 10). %% Unsubscribe request
-define(UNSUBACK,    11). %% Unsubscribe acknowledgment
-define(PINGREQ,     12). %% PING request
-define(PINGRESP,    13). %% PING response
-define(DISCONNECT,  14). %% Client or Server is disconnecting
-define(AUTH,        15). %% Authentication exchange

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
    'DISCONNECT',
    'AUTH']).

-type(mqtt_packet_type() :: ?RESERVED..?AUTH).

%%--------------------------------------------------------------------
%% MQTT Connect Return Codes
%%--------------------------------------------------------------------

-define(CONNACK_ACCEPT,      0). %% Connection accepted
-define(CONNACK_PROTO_VER,   1). %% Unacceptable protocol version
-define(CONNACK_INVALID_ID,  2). %% Client Identifier is correct UTF-8 but not allowed by the Server
-define(CONNACK_SERVER,      3). %% Server unavailable
-define(CONNACK_CREDENTIALS, 4). %% Username or password is malformed
-define(CONNACK_AUTH,        5). %% Client is not authorized to connect

-type(mqtt_connack() :: ?CONNACK_ACCEPT..?CONNACK_AUTH).

%%--------------------------------------------------------------------
%% Max MQTT Packet Length
%%--------------------------------------------------------------------

-define(MAX_PACKET_SIZE, 16#fffffff).

%%--------------------------------------------------------------------
%% MQTT Parser and Serializer
%%--------------------------------------------------------------------

-define(HIGHBIT, 2#10000000).
-define(LOWBITS, 2#01111111).

%%--------------------------------------------------------------------
%% MQTT Packet Fixed Header
%%--------------------------------------------------------------------

-record(mqtt_packet_header,
        { type   = ?RESERVED :: mqtt_packet_type(),
          dup    = false     :: boolean(),
          qos    = ?QOS_0    :: mqtt_qos(),
          retain = false     :: boolean()
        }).

%%--------------------------------------------------------------------
%% MQTT Packets
%%--------------------------------------------------------------------

-type(mqtt_topic() :: binary()).

-type(mqtt_client_id() :: binary()).

-type(mqtt_username()  :: binary() | undefined).

-type(mqtt_packet_id() :: 1..16#ffff | undefined).

-type(mqtt_reason_code() :: 1..16#ff | undefined).

-type(mqtt_properties() :: undefined | map()).

-type(mqtt_subopt() :: list({qos, mqtt_qos()}
                          | {retain_handling, boolean()}
                          | {keep_retain, boolean()}
                          | {no_local, boolean()})).

-record(mqtt_packet_connect,
        { client_id   = <<>>           :: mqtt_client_id(),
          proto_ver   = ?MQTT_PROTO_V4 :: mqtt_vsn(),
          proto_name  = <<"MQTT">>     :: binary(),
          will_retain = false          :: boolean(),
          will_qos    = ?QOS_1         :: mqtt_qos(),
          will_flag   = false          :: boolean(),
          clean_sess  = false          :: boolean(),
          clean_start = true           :: boolean(),
          keep_alive  = 60             :: non_neg_integer(),
          will_props  = undefined      :: undefined | map(),
          will_topic  = undefined      :: undefined | binary(),
          will_msg    = undefined      :: undefined | binary(),
          username    = undefined      :: undefined | binary(),
          password    = undefined      :: undefined | binary(),
          is_bridge   = false          :: boolean(),
          properties  = undefined      :: mqtt_properties() %% MQTT Version 5.0
        }).

-record(mqtt_packet_connack,
        { ack_flags = ?RESERVED :: 0 | 1,
          reason_code           :: mqtt_connack(),
          properties            :: map()
        }).

-record(mqtt_packet_publish,
        { topic_name :: binary(),
          packet_id  :: mqtt_packet_id(),
          properties :: mqtt_properties()
        }).

-record(mqtt_packet_puback,
        { packet_id   :: mqtt_packet_id(),
          reason_code :: mqtt_reason_code(),
          properties  :: mqtt_properties()
        }).

-record(mqtt_packet_subscribe,
        { packet_id     :: mqtt_packet_id(),
          properties    :: mqtt_properties(),
          topic_filters :: list({binary(), mqtt_subopt()})
        }).

-record(mqtt_packet_unsubscribe,
        { packet_id  :: mqtt_packet_id(),
          properties :: mqtt_properties(),
          topics     :: list(binary())
        }).

-record(mqtt_packet_suback,
        { packet_id    :: mqtt_packet_id(),
          properties   :: mqtt_properties(),
          reason_codes :: list(mqtt_reason_code())
        }).

-record(mqtt_packet_unsuback,
        { packet_id  :: mqtt_packet_id(),
          properties :: mqtt_properties(),
          reason_codes :: list(mqtt_reason_code())
        }).

-record(mqtt_packet_disconnect,
        { reason_code :: mqtt_reason_code(),
          properties  :: mqtt_properties()
        }).

-record(mqtt_packet_auth,
        { reason_code :: mqtt_reason_code(),
          properties  :: mqtt_properties()
        }).

%%--------------------------------------------------------------------
%% MQTT Control Packet
%%--------------------------------------------------------------------

-record(mqtt_packet,
        { header   :: #mqtt_packet_header{},
          variable :: #mqtt_packet_connect{}
                    | #mqtt_packet_connack{}
                    | #mqtt_packet_publish{}
                    | #mqtt_packet_puback{}
                    | #mqtt_packet_subscribe{}
                    | #mqtt_packet_suback{}
                    | #mqtt_packet_unsubscribe{}
                    | #mqtt_packet_unsuback{}
                    | #mqtt_packet_disconnect{}
                    | #mqtt_packet_auth{}
                    | mqtt_packet_id()
                    | undefined,
          payload  :: binary() | undefined
        }).

-type(mqtt_packet() :: #mqtt_packet{}).

%%--------------------------------------------------------------------
%% MQTT Packet Match
%%--------------------------------------------------------------------

-define(CONNECT_PACKET(Var),
    #mqtt_packet{header = #mqtt_packet_header{type = ?CONNECT}, variable = Var}).

-define(CONNACK_PACKET(ReasonCode),
    #mqtt_packet{header   = #mqtt_packet_header{type = ?CONNACK},
                 variable = #mqtt_packet_connack{reason_code = ReturnCode}}).

-define(CONNACK_PACKET(ReasonCode, SessPresent),
    #mqtt_packet{header   = #mqtt_packet_header{type = ?CONNACK},
                 variable = #mqtt_packet_connack{ack_flags = SessPresent,
                                                 reason_code = ReturnCode}}).

-define(CONNACK_PACKET(ReasonCode, SessPresent, Properties),
    #mqtt_packet{header   = #mqtt_packet_header{type = ?CONNACK},
                 variable = #mqtt_packet_connack{ack_flags = SessPresent,
                                                 reason_code = ReasonCode,
                                                 properties = Properties}}).

-define(PUBLISH_PACKET(Qos, PacketId),
    #mqtt_packet{header   = #mqtt_packet_header{type = ?PUBLISH,
                                                qos = Qos},
                 variable = #mqtt_packet_publish{packet_id = PacketId}}).

-define(PUBLISH_PACKET(Qos, Topic, PacketId, Payload),
    #mqtt_packet{header   = #mqtt_packet_header{type = ?PUBLISH,
                                                qos = Qos},
                 variable = #mqtt_packet_publish{topic_name = Topic,
                                                 packet_id  = PacketId},
                 payload  = Payload}).

-define(PUBACK_PACKET(Type, PacketId),
    #mqtt_packet{header   = #mqtt_packet_header{type = Type},
                 variable = #mqtt_packet_puback{packet_id = PacketId}}).

-define(PUBREL_PACKET(PacketId),
    #mqtt_packet{header   = #mqtt_packet_header{type = ?PUBREL, qos = ?QOS_1},
                 variable = #mqtt_packet_puback{packet_id = PacketId}}).

-define(SUBSCRIBE_PACKET(PacketId, TopicFilters), 
    #mqtt_packet{header = #mqtt_packet_header{type = ?SUBSCRIBE, qos = ?QOS_1},
                 variable = #mqtt_packet_subscribe{packet_id     = PacketId,
                                                   topic_filters = TopicFilters}}).

-define(SUBACK_PACKET(PacketId, ReasonCodes),
    #mqtt_packet{header = #mqtt_packet_header{type = ?SUBACK},
                 variable = #mqtt_packet_suback{packet_id    = PacketId,
                                                reason_codes = ReasonCodes}}).

-define(SUBACK_PACKET(PacketId, Properties, ReasonCodes),
    #mqtt_packet{header = #mqtt_packet_header{type = ?SUBACK},
                 variable = #mqtt_packet_suback{packet_id    = PacketId,
                                                properties   = Properties,
                                                reason_codes = ReasonCodes}}).
-define(UNSUBSCRIBE_PACKET(PacketId, Topics),
    #mqtt_packet{header = #mqtt_packet_header{type = ?UNSUBSCRIBE, qos = ?QOS_1},
                 variable = #mqtt_packet_unsubscribe{packet_id = PacketId,
                                                     topics    = Topics}}).
-define(UNSUBACK_PACKET(PacketId),
    #mqtt_packet{header = #mqtt_packet_header{type = ?UNSUBACK},
                 variable = #mqtt_packet_unsuback{packet_id = PacketId}}).

-define(PACKET(Type),
    #mqtt_packet{header = #mqtt_packet_header{type = Type}}).

%%--------------------------------------------------------------------
%% MQTT Message
%%--------------------------------------------------------------------

-type(mqtt_msg_id() :: binary() | undefined).

-type(mqtt_msg_from() :: atom() | {binary(), undefined | binary()}).

-record(mqtt_message,
        { %% Global unique message ID
          id              :: mqtt_msg_id(),
          %% PacketId
          packet_id       :: mqtt_packet_id(),
          %% ClientId and Username
          from            :: mqtt_msg_from(),
          %% Topic that the message is published to
          topic           :: binary(),
          %% Message QoS
          qos     = 0     :: mqtt_qos(),
          %% Message Flags
          flags   = []    :: [retain | dup | sys],
          %% Retain flag
          retain  = false :: boolean(),
          %% Dup flag
          dup     = false :: boolean(),
          %% $SYS flag
          sys     = false :: boolean(),
          %% Headers
          headers = []    :: list(),
          %% Payload
          payload         :: binary(),
          %% Timestamp
          timestamp       :: erlang:timestamp()
        }).

-type(mqtt_message() :: #mqtt_message{}).

%%--------------------------------------------------------------------
%% MQTT Delivery
%%--------------------------------------------------------------------

-record(mqtt_delivery,
        { sender  :: pid(),
          message :: mqtt_message(),
          flows   :: list()
        }).

-type(mqtt_delivery() :: #mqtt_delivery{}).

