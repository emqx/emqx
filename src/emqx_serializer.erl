%%--------------------------------------------------------------------
%% Copyright (c) 2013-2018 EMQ Inc. All Rights Reserved.
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

-module(emqx_serializer).

-include("emqx.hrl").

-include("emqx_mqtt.hrl").

%% API
-export([serialize/1]).

%% @doc Serialise MQTT Packet
-spec(serialize(mqtt_packet()) -> iolist()).
serialize(#mqtt_packet{header   = Header = #mqtt_packet_header{type = Type},
                       variable = Variable,
                       payload  = Payload}) ->
    serialize_header(Header,
        serialize_variable(Type, Variable,
            serialize_payload(Payload))).

serialize_header(#mqtt_packet_header{type   = Type,
                                     dup    = Dup,
                                     qos    = Qos,
                                     retain = Retain},
                 {VariableBin, PayloadBin})
    when ?CONNECT =< Type andalso Type =< ?DISCONNECT ->
    Len = byte_size(VariableBin) + byte_size(PayloadBin),
    true = (Len =< ?MAX_PACKET_SIZE),
    [<<Type:4, (opt(Dup)):1, (opt(Qos)):2, (opt(Retain)):1>>,
     serialize_len(Len), VariableBin, PayloadBin].

serialize_variable(?CONNECT, #mqtt_packet_connect{client_id   = ClientId,
                                                  proto_ver   = ProtoVer,
                                                  proto_name  = ProtoName,
                                                  will_retain = WillRetain,
                                                  will_qos    = WillQos,
                                                  will_flag   = WillFlag,
                                                  clean_sess  = CleanSess,
                                                  keep_alive  = KeepAlive,
                                                  will_topic  = WillTopic,
                                                  will_msg    = WillMsg,
                                                  username    = Username,
                                                  password    = Password}, undefined) ->
    VariableBin = <<(byte_size(ProtoName)):16/big-unsigned-integer,
                    ProtoName/binary,
                    ProtoVer:8,
                    (opt(Username)):1,
                    (opt(Password)):1,
                    (opt(WillRetain)):1,
                    WillQos:2,
                    (opt(WillFlag)):1,
                    (opt(CleanSess)):1,
                    0:1,
                    KeepAlive:16/big-unsigned-integer>>,
    PayloadBin = serialize_utf(ClientId),
    PayloadBin1 = case WillFlag of
                      true -> <<PayloadBin/binary,
                                (serialize_utf(WillTopic))/binary,
                                (byte_size(WillMsg)):16/big-unsigned-integer,
                                WillMsg/binary>>;
                      false -> PayloadBin
                  end,
    UserPasswd = << <<(serialize_utf(B))/binary>> || B <- [Username, Password], B =/= undefined >>,
    {VariableBin, <<PayloadBin1/binary, UserPasswd/binary>>};

serialize_variable(?CONNACK, #mqtt_packet_connack{ack_flags   = AckFlags,
                                                  reason_code = ReasonCode,
                                                  properties  = Properties}, undefined) ->
    PropsBin = serialize_properties(Properties),
    {<<AckFlags:8, ReasonCode:8, PropsBin/binary>>, <<>>};

serialize_variable(?SUBSCRIBE, #mqtt_packet_subscribe{packet_id = PacketId,
                                                      topic_filters = TopicFilters}, undefined) ->
    {<<PacketId:16/big>>, serialize_topics(TopicFilters)};

serialize_variable(?SUBACK, #mqtt_packet_suback{packet_id = PacketId,
                                                properties = Properties,
                                                reason_codes = ReasonCodes}, undefined) ->
    io:format("SubAck ReasonCodes: ~p~n", [ReasonCodes]),
    {<<PacketId:16/big, (serialize_properties(Properties))/binary>>, << <<Code>> || Code <- ReasonCodes >>};

serialize_variable(?UNSUBSCRIBE, #mqtt_packet_unsubscribe{packet_id  = PacketId,
                                                          topics     = Topics }, undefined) ->
    {<<PacketId:16/big>>, serialize_topics(Topics)};

serialize_variable(?UNSUBACK, #mqtt_packet_unsuback{packet_id = PacketId,
                                                    properties = Properties,
                                                    reason_codes = ReasonCodes}, undefined) ->
    {<<PacketId:16/big, (serialize_properties(Properties))/binary>>, << <<Code>> || Code <- ReasonCodes >>};

serialize_variable(?PUBLISH, #mqtt_packet_publish{topic_name = TopicName,
                                                  packet_id  = PacketId }, PayloadBin) ->
    TopicBin = serialize_utf(TopicName),
    PacketIdBin = if
                      PacketId =:= undefined -> <<>>;
                      true -> <<PacketId:16/big>>
                  end,
    {<<TopicBin/binary, PacketIdBin/binary>>, PayloadBin};

serialize_variable(PubAck, #mqtt_packet_puback{packet_id = PacketId}, _Payload)
    when PubAck =:= ?PUBACK; PubAck =:= ?PUBREC; PubAck =:= ?PUBREL; PubAck =:= ?PUBCOMP ->
    {<<PacketId:16/big>>, <<>>};

serialize_variable(?PINGREQ, undefined, undefined) ->
    {<<>>, <<>>};

serialize_variable(?PINGRESP, undefined, undefined) ->
    {<<>>, <<>>};

serialize_variable(?DISCONNECT, #mqtt_packet_disconnect{reason_code = ReasonCode,
                                                        properties  = Properties}, undefined) ->
    {<<ReasonCode, (serialize_properties(Properties))/binary>>, <<>>};

serialize_variable(?AUTH, #mqtt_packet_auth{reason_code = ReasonCode,
                                            properties  = Properties}, undefined) ->
    {<<ReasonCode, (serialize_properties(Properties))/binary>>, <<>>}.

serialize_payload(undefined) ->
    undefined;
serialize_payload(Bin) when is_binary(Bin) ->
    Bin.

serialize_properties(undefined) ->
    <<>>;
serialize_properties(Props) ->
    << <<(serialize_property(Prop, Val))/binary>> || {Prop, Val} <- maps:to_list(Props) >>.

%% 01: Byte;
serialize_property('Payload-Format-Indicator', Val) ->
    <<16#01, Val>>;
%% 02: Four Byte Integer;
serialize_property('Message-Expiry-Interval', Val) ->
    <<16#02, Val:32/big>>;
%% 03: UTF-8 Encoded String;
serialize_property('Content-Type', Val) ->
    <<16#03, (serialize_utf(Val))/binary>>;
%% 08: UTF-8 Encoded String;
serialize_property('Response-Topic', Val) ->
    <<16#08, (serialize_utf(Val))/binary>>;
%% 09: Binary Data;
serialize_property('Correlation-Data', Val) ->
    <<16#09, (iolist_size(Val)):16, Val/binary>>;
%% 11: Variable Byte Integer;
serialize_property('Subscription-Identifier', Val) ->
    <<16#0B, (serialize_variable_byte_integer(Val))/binary>>;
%% 17: Four Byte Integer;
serialize_property('Session-Expiry-Interval', Val) ->
    <<16#11, Val:32/big>>;
%% 18: UTF-8 Encoded String;
serialize_property('Assigned-Client-Identifier', Val) ->
    <<16#12, (serialize_utf(Val))/binary>>;
%% 19: Two Byte Integer;
serialize_property('Server-Keep-Alive', Val) ->
    <<16#13, Val:16/big>>;
%% 21: UTF-8 Encoded String;
serialize_property('Authentication-Method', Val) ->
    <<16#15, (serialize_utf(Val))/binary>>;
%% 22: Binary Data;
serialize_property('Authentication-Data', Val) ->
    <<16#16, (iolist_size(Val)):16, Val/binary>>;
%% 23: Byte;
serialize_property('Request-Problem-Information', Val) ->
    <<16#17, Val>>;
%% 24: Four Byte Integer;
serialize_property('Will-Delay-Interval', Val) ->
    <<16#18, Val:32/big>>;
%% 25: Byte;
serialize_property('Request-Response-Information', Val) ->
    <<16#19, Val>>;
%% 26: UTF-8 Encoded String;
serialize_property('Response-Information', Val) ->
    <<16#1A, (serialize_utf(Val))/binary>>;
%% 28: UTF-8 Encoded String;
serialize_property('Server-Reference', Val) ->
    <<16#1C, (serialize_utf(Val))/binary>>;
%% 31: UTF-8 Encoded String;
serialize_property('Reason-String', Val) ->
    <<16#1F, (serialize_utf(Val))/binary>>;
%% 33: Two Byte Integer;
serialize_property('Receive-Maximum', Val) ->
    <<16#21, Val:16/big>>;
%% 34: Two Byte Integer;
serialize_property('Topic-Alias-Maximum', Val) ->
    <<16#22, Val:16/big>>;
%% 35: Two Byte Integer;
serialize_property('Topic-Alias', Val) ->
    <<16#23, Val:16/big>>;
%% 36: Byte;
serialize_property('Maximum-QoS', Val) ->
    <<16#24, Val>>;
%% 37: Byte;
serialize_property('Retain-Available', Val) ->
    <<16#25, Val>>;
%% 38: UTF-8 String Pair;
serialize_property('User-Property', Val) ->
    <<16#26, (serialize_utf_pair(Val))/binary>>;
%% 39: Four Byte Integer;
serialize_property('Maximum-Packet-Size', Val) ->
    <<16#27, Val:32/big>>;
%% 40: Byte;
serialize_property('Wildcard-Subscription-Available', Val) ->
    <<16#28, Val>>;
%% 41: Byte;
serialize_property('Subscription-Identifier-Available', Val) ->
    <<16#29, Val>>;
%% 42: Byte;
serialize_property('Shared-Subscription-Available', Val) ->
    <<16#2A, Val>>.

serialize_topics([{_Topic, _Qos}|_] = Topics) ->
    << <<(serialize_utf(Topic))/binary, ?RESERVED:6, Qos:2>> || {Topic, Qos} <- Topics >>;

serialize_topics([H|_] = Topics) when is_binary(H) ->
    << <<(serialize_utf(Topic))/binary>> || Topic <- Topics >>.

serialize_utf_pair({Name, Value}) ->
    << <<(serialize_utf(S))/binary, (serialize_utf(S))/binary>> || S <- [Name, Value] >>.

serialize_utf(String) ->
    StringBin = unicode:characters_to_binary(String),
    Len = byte_size(StringBin),
    true = (Len =< 16#ffff),
    <<Len:16/big, StringBin/binary>>.

serialize_len(I) ->
    serialize_variable_byte_integer(I). %%TODO: refactor later.

serialize_variable_byte_integer(N) when N =< ?LOWBITS ->
    <<0:1, N:7>>;
serialize_variable_byte_integer(N) ->
    <<1:1, (N rem ?HIGHBIT):7, (serialize_variable_byte_integer(N div ?HIGHBIT))/binary>>.

opt(undefined)            -> ?RESERVED;
opt(false)                -> 0;
opt(true)                 -> 1;
opt(X) when is_integer(X) -> X;
opt(B) when is_binary(B)  -> 1.

