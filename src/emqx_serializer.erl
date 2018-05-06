%%%===================================================================
%%% Copyright (c) 2013-2018 EMQ Inc. All rights reserved.
%%%
%%% Licensed under the Apache License, Version 2.0 (the "License");
%%% you may not use this file except in compliance with the License.
%%% You may obtain a copy of the License at
%%%
%%%     http://www.apache.org/licenses/LICENSE-2.0
%%%
%%% Unless required by applicable law or agreed to in writing, software
%%% distributed under the License is distributed on an "AS IS" BASIS,
%%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%%% See the License for the specific language governing permissions and
%%% limitations under the License.
%%%===================================================================

-module(emqx_serializer).

-include("emqx.hrl").

-include("emqx_mqtt.hrl").

-type(option() :: {version, mqtt_version()}).

-export_type([option/0]).

-export([serialize/1, serialize/2]).

-spec(serialize(mqtt_packet()) -> iodata()).
serialize(Packet) -> serialize(Packet, []).

-spec(serialize(mqtt_packet(), [option()]) -> iodata()).
serialize(#mqtt_packet{header   = Header,
                       variable = Variable,
                       payload  = Payload}, Opts) when is_list(Opts) ->
    Opts1 = parse_opt(Opts, #{version => ?MQTT_PROTO_V4}),
    serialize(Header, serialize_variable(Variable, Opts1), serialize_payload(Payload)).

parse_opt([], Map) ->
    Map;
parse_opt([{version, Ver}|Opts], Map) ->
    parse_opt(Opts, Map#{version := Ver});
parse_opt([_|Opts], Map) ->
    parse_opt(Opts, Map).

serialize(#mqtt_packet_header{type   = Type,
                              dup    = Dup,
                              qos    = Qos,
                              retain = Retain}, VariableData, PayloadData)
    when ?CONNECT =< Type andalso Type =< ?AUTH ->
    Len = iolist_size(VariableData) + iolist_size(PayloadData),
    true = (Len =< ?MAX_PACKET_SIZE),
    [<<Type:4, (opt(Dup)):1, (opt(Qos)):2, (opt(Retain)):1>>,
     serialize_remaining_len(Len), VariableData, PayloadData].

serialize_variable(#mqtt_packet_connect{proto_name   = ProtoName,
                                        proto_ver    = ProtoVer,
                                        is_bridge    = IsBridge,
                                        clean_start  = CleanStart,
                                        will_flag    = WillFlag,
                                        will_qos     = WillQos,
                                        will_retain  = WillRetain,
                                        keepalive    = KeepAlive,
                                        properties   = Properties,
                                        client_id    = ClientId,
                                        will_props   = WillProps,
                                        will_topic   = WillTopic,
                                        will_payload = WillPayload,
                                        username     = Username,
                                        password     = Password}, _Opts) ->
    [serialize_binary_data(ProtoName),
     <<(case IsBridge of
           true  -> 16#80 + ProtoVer;
           false -> ProtoVer
       end):8,
       (opt(Username)):1,
       (opt(Password)):1,
       (opt(WillRetain)):1,
       WillQos:2,
       (opt(WillFlag)):1,
       (opt(CleanStart)):1,
       0:1,
       KeepAlive:16/big-unsigned-integer>>,
     serialize_properties(Properties, ProtoVer),
     serialize_utf8_string(ClientId),
     case WillFlag of
         true -> [serialize_properties(WillProps, ProtoVer),
                  serialize_utf8_string(WillTopic),
                  serialize_binary_data(WillPayload)];
         false -> <<>>
     end,
     serialize_utf8_string(Username, true),
     serialize_utf8_string(Password, true)];

serialize_variable(#mqtt_packet_connack{ack_flags   = AckFlags,
                                        reason_code = ReasonCode,
                                        properties  = Properties}, #{version := Ver}) ->
    [AckFlags, ReasonCode, serialize_properties(Properties, Ver)];

serialize_variable(#mqtt_packet_publish{topic_name = TopicName,
                                        packet_id  = PacketId,
                                        properties = Properties}, #{version := Ver}) ->
    [serialize_utf8_string(TopicName),
     if
         PacketId =:= undefined -> <<>>;
         true -> <<PacketId:16/big-unsigned-integer>>
     end,
     serialize_properties(Properties, Ver)];

serialize_variable(#mqtt_packet_puback{packet_id = PacketId}, #{version := Ver})
    when Ver == ?MQTT_PROTO_V3; Ver == ?MQTT_PROTO_V4 ->
    <<PacketId:16/big-unsigned-integer>>;
serialize_variable(#mqtt_packet_puback{packet_id   = PacketId,
                                       reason_code = ReasonCode,
                                       properties  = Properties},
                   #{version := ?MQTT_PROTO_V5}) ->
    [<<PacketId:16/big-unsigned-integer>>, ReasonCode,
     serialize_properties(Properties, ?MQTT_PROTO_V5)];

serialize_variable(#mqtt_packet_subscribe{packet_id     = PacketId,
                                          properties    = Properties,
                                          topic_filters = TopicFilters},
                   #{version := Ver}) ->
    [<<PacketId:16/big-unsigned-integer>>, serialize_properties(Properties, Ver),
     serialize_topic_filters(subscribe, TopicFilters, Ver)];

serialize_variable(#mqtt_packet_suback{packet_id    = PacketId,
                                       properties   = Properties,
                                       reason_codes = ReasonCodes},
                   #{version := Ver}) ->
    [<<PacketId:16/big-unsigned-integer>>, serialize_properties(Properties, Ver),
     << <<Code>> || Code <- ReasonCodes >>];

serialize_variable(#mqtt_packet_unsubscribe{packet_id     = PacketId,
                                            properties    = Properties,
                                            topic_filters = TopicFilters},
                   #{version := Ver}) ->
    [<<PacketId:16/big-unsigned-integer>>, serialize_properties(Properties, Ver),
     serialize_topic_filters(unsubscribe, TopicFilters, Ver)];

serialize_variable(#mqtt_packet_unsuback{packet_id    = PacketId,
                                         properties   = Properties,
                                         reason_codes = ReasonCodes},
                   #{version := Ver}) ->
    [<<PacketId:16/big-unsigned-integer>>, serialize_properties(Properties, Ver),
     << <<Code>> || Code <- ReasonCodes >>];

serialize_variable(#mqtt_packet_disconnect{}, #{version := Ver})
    when Ver == ?MQTT_PROTO_V3; Ver == ?MQTT_PROTO_V4 ->
    <<>>;

serialize_variable(#mqtt_packet_disconnect{reason_code = ReasonCode,
                                           properties  = Properties},
                   #{version := Ver = ?MQTT_PROTO_V5}) ->
    [ReasonCode, serialize_properties(Properties, Ver)];
serialize_variable(#mqtt_packet_disconnect{}, _Ver) ->
    <<>>;

serialize_variable(#mqtt_packet_auth{reason_code = ReasonCode,
                                     properties  = Properties},
                   #{version := Ver = ?MQTT_PROTO_V5}) ->
    [ReasonCode, serialize_properties(Properties, Ver)];

serialize_variable(PacketId, ?MQTT_PROTO_V3) when is_integer(PacketId) ->
    <<PacketId:16/big-unsigned-integer>>;
serialize_variable(PacketId, ?MQTT_PROTO_V4) when is_integer(PacketId) ->
    <<PacketId:16/big-unsigned-integer>>;
serialize_variable(undefined, _Ver) ->
    <<>>.

serialize_payload(undefined) ->
    <<>>;
serialize_payload(Bin) when is_binary(Bin); is_list(Bin) ->
    Bin.

serialize_properties(_Props, Ver) when Ver =/= ?MQTT_PROTO_V5 ->
    <<>>;
serialize_properties(Props, ?MQTT_PROTO_V5) ->
    serialize_properties(Props).

serialize_properties(undefined) ->
    <<0>>;
serialize_properties(Props) when map_size(Props) == 0 ->
    <<0>>;
serialize_properties(Props) when is_map(Props) ->
    Bin = << <<(serialize_property(Prop, Val))/binary>> || {Prop, Val} <- maps:to_list(Props) >>,
    [serialize_variable_byte_integer(byte_size(Bin)), Bin].

%% Ignore undefined
serialize_property(_, undefined) ->
    <<>>;
serialize_property('Payload-Format-Indicator', Val) ->
    <<16#01, Val>>;
serialize_property('Message-Expiry-Interval', Val) ->
    <<16#02, Val:32/big>>;
serialize_property('Content-Type', Val) ->
    <<16#03, (serialize_utf8_string(Val))/binary>>;
serialize_property('Response-Topic', Val) ->
    <<16#08, (serialize_utf8_string(Val))/binary>>;
serialize_property('Correlation-Data', Val) ->
    <<16#09, (byte_size(Val)):16, Val/binary>>;
serialize_property('Subscription-Identifier', Val) ->
    <<16#0B, (serialize_variable_byte_integer(Val))/binary>>;
serialize_property('Session-Expiry-Interval', Val) ->
    <<16#11, Val:32/big>>;
serialize_property('Assigned-Client-Identifier', Val) ->
    <<16#12, (serialize_utf8_string(Val))/binary>>;
serialize_property('Server-Keep-Alive', Val) ->
    <<16#13, Val:16/big>>;
serialize_property('Authentication-Method', Val) ->
    <<16#15, (serialize_utf8_string(Val))/binary>>;
serialize_property('Authentication-Data', Val) ->
    <<16#16, (iolist_size(Val)):16, Val/binary>>;
serialize_property('Request-Problem-Information', Val) ->
    <<16#17, Val>>;
serialize_property('Will-Delay-Interval', Val) ->
    <<16#18, Val:32/big>>;
serialize_property('Request-Response-Information', Val) ->
    <<16#19, Val>>;
serialize_property('Response-Information', Val) ->
    <<16#1A, (serialize_utf8_string(Val))/binary>>;
serialize_property('Server-Reference', Val) ->
    <<16#1C, (serialize_utf8_string(Val))/binary>>;
serialize_property('Reason-String', Val) ->
    <<16#1F, (serialize_utf8_string(Val))/binary>>;
serialize_property('Receive-Maximum', Val) ->
    <<16#21, Val:16/big>>;
serialize_property('Topic-Alias-Maximum', Val) ->
    <<16#22, Val:16/big>>;
serialize_property('Topic-Alias', Val) ->
    <<16#23, Val:16/big>>;
serialize_property('Maximum-QoS', Val) ->
    <<16#24, Val>>;
serialize_property('Retain-Available', Val) ->
    <<16#25, Val>>;
serialize_property('User-Property', {Key, Val}) ->
    <<16#26, (serialize_utf8_pair({Key, Val}))/binary>>;
serialize_property('User-Property', Props) when is_list(Props) ->
    << <<(serialize_property('User-Property', {Key, Val}))/binary>>
       || {Key, Val} <- Props >>;
serialize_property('Maximum-Packet-Size', Val) ->
    <<16#27, Val:32/big>>;
serialize_property('Wildcard-Subscription-Available', Val) ->
    <<16#28, Val>>;
serialize_property('Subscription-Identifier-Available', Val) ->
    <<16#29, Val>>;
serialize_property('Shared-Subscription-Available', Val) ->
    <<16#2A, Val>>.

serialize_topic_filters(subscribe, TopicFilters, ?MQTT_PROTO_V5) ->
    << <<(serialize_utf8_string(Topic))/binary, ?RESERVED:2, Rh:2, (opt(Rap)):1, (opt(Nl)):1, Qos:2>>
       || {Topic, #mqtt_subopts{rh = Rh, rap = Rap, nl = Nl, qos = Qos}} <- TopicFilters >>;

serialize_topic_filters(subscribe, TopicFilters, _Ver) ->
    << <<(serialize_utf8_string(Topic))/binary, ?RESERVED:6, Qos:2>>
       || {Topic, #mqtt_subopts{qos = Qos}} <- TopicFilters >>;

serialize_topic_filters(unsubscribe, TopicFilters, _Ver) ->
    << <<(serialize_utf8_string(Topic))/binary>> || Topic <- TopicFilters >>.

serialize_utf8_pair({Name, Value}) ->
    << <<(serialize_utf8_string(S))/binary,
         (serialize_utf8_string(S))/binary>> || S <- [Name, Value] >>.

serialize_binary_data(Bin) ->
    [<<(byte_size(Bin)):16/big-unsigned-integer>>, Bin].

serialize_utf8_string(undefined, false) ->
    error(utf8_string_undefined);
serialize_utf8_string(undefined, true) ->
    <<>>;
serialize_utf8_string(String, _AllowNull) ->
    serialize_utf8_string(String).

serialize_utf8_string(String) ->
    StringBin = unicode:characters_to_binary(String),
    Len = byte_size(StringBin),
    true = (Len =< 16#ffff),
    <<Len:16/big, StringBin/binary>>.

serialize_remaining_len(I) ->
    serialize_variable_byte_integer(I).

serialize_variable_byte_integer(N) when N =< ?LOWBITS ->
    <<0:1, N:7>>;
serialize_variable_byte_integer(N) ->
    <<1:1, (N rem ?HIGHBIT):7, (serialize_variable_byte_integer(N div ?HIGHBIT))/binary>>.

opt(undefined)            -> ?RESERVED;
opt(false)                -> 0;
opt(true)                 -> 1;
opt(X) when is_integer(X) -> X;
opt(B) when is_binary(B)  -> 1.

