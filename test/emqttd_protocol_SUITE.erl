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

-module(emqttd_protocol_SUITE).

-compile(export_all).

-import(emqttd_serializer, [serialize/1]).

-include("emqttd.hrl").

-include("emqttd_protocol.hrl").

all() ->
    [{group, parser},
     {group, serializer},
     {group, packet},
     {group, message}].

groups() ->
    [{parser, [],
      [parse_connect,
       parse_bridge,
       parse_publish,
       parse_puback,
       parse_pubrec,
       parse_pubrel,
       parse_pubcomp,
       parse_subscribe,
       parse_unsubscribe,
       parse_pingreq,
       parse_disconnect]},
     {serializer, [],
      [serialize_connect,
       serialize_connack,
       serialize_publish,
       serialize_puback,
       serialize_pubrel,
       serialize_subscribe,
       serialize_suback,
       serialize_unsubscribe,
       serialize_unsuback,
       serialize_pingreq,
       serialize_pingresp,
       serialize_disconnect]},
     {packet, [],
      [packet_proto_name,
       packet_type_name,
       packet_connack_name,
       packet_format]},
     {message, [],
      [message_make,
       message_from_packet,
       message_flag]}].

%%--------------------------------------------------------------------
%% Parse Cases
%%--------------------------------------------------------------------

parse_connect(_) ->
    Parser = emqttd_parser:new([]),
    %% CONNECT(Q0, R0, D0, ClientId=mosqpub/10451-iMac.loca, ProtoName=MQIsdp, ProtoVsn=3, CleanSess=true, KeepAlive=60, Username=undefined, Password=undefined)
    V31ConnBin = <<16,37,0,6,77,81,73,115,100,112,3,2,0,60,0,23,109,111,115,113,112,117,98,47,49,48,52,53,49,45,105,77,97,99,46,108,111,99,97>>,
    {ok, #mqtt_packet{header = #mqtt_packet_header{type = ?CONNECT,
                                                   dup  = false,
                                                   qos  = 0,
                                                   retain = false},
                      variable = #mqtt_packet_connect{proto_ver  = 3,
                                                      proto_name = <<"MQIsdp">>,
                                                      client_id  = <<"mosqpub/10451-iMac.loca">>,
                                                      clean_sess = true,
                                                      keep_alive = 60}}, <<>>} = Parser(V31ConnBin),
    %% CONNECT(Q0, R0, D0, ClientId=mosqpub/10451-iMac.loca, ProtoName=MQTT, ProtoVsn=4, CleanSess=true, KeepAlive=60, Username=undefined, Password=undefined)
    V311ConnBin = <<16,35,0,4,77,81,84,84,4,2,0,60,0,23,109,111,115,113,112,117,98,47,49,48,52,53,49,45,105,77,97,99,46,108,111,99,97>>,
    {ok, #mqtt_packet{header = #mqtt_packet_header{type = ?CONNECT, 
                                                   dup = false,
                                                   qos = 0,
                                                   retain = false},
                      variable = #mqtt_packet_connect{proto_ver  = 4,
                                                      proto_name = <<"MQTT">>,
                                                      client_id  = <<"mosqpub/10451-iMac.loca">>,
                                                      clean_sess = true,
                                                      keep_alive = 60 } }, <<>>} = Parser(V311ConnBin),

    %% CONNECT(Qos=0, Retain=false, Dup=false, ClientId="", ProtoName=MQTT, ProtoVsn=4, CleanSess=true, KeepAlive=60)
    V311ConnWithoutClientId = <<16,12,0,4,77,81,84,84,4,2,0,60,0,0>>,
    {ok, #mqtt_packet{header = #mqtt_packet_header{type = ?CONNECT,
                                                   dup = false,
                                                   qos = 0,
                                                   retain = false},
                      variable = #mqtt_packet_connect{proto_ver = 4,
                                                      proto_name = <<"MQTT">>,
                                                      client_id  = <<>>,
                                                      clean_sess = true,
                                                      keep_alive = 60 } }, <<>>} = Parser(V311ConnWithoutClientId),
    %%CONNECT(Q0, R0, D0, ClientId=mosqpub/10452-iMac.loca, ProtoName=MQIsdp, ProtoVsn=3, CleanSess=true, KeepAlive=60,
    %% Username=test, Password=******, Will(Qos=1, Retain=false, Topic=/will, Msg=willmsg))
    ConnBinWithWill = <<16,67,0,6,77,81,73,115,100,112,3,206,0,60,0,23,109,111,115,113,112,117,98,47,49,48,52,53,50,45,105,77,97,99,46,108,111,99,97,0,5,47,119,105,108,108,0,7,119,105,108,108,109,115,103,0,4,116,101,115,116,0,6,112,117,98,108,105,99>>,
    {ok, #mqtt_packet{header = #mqtt_packet_header{type = ?CONNECT,
                                                   dup = false,
                                                   qos = 0,
                                                   retain = false},
                      variable = #mqtt_packet_connect{proto_ver = 3,
                                                      proto_name = <<"MQIsdp">>,
                                                      client_id  = <<"mosqpub/10452-iMac.loca">>,
                                                      clean_sess = true,
                                                      keep_alive = 60,
                                                      will_retain = false,
                                                      will_qos = 1,
                                                      will_flag = true,
                                                      will_topic = <<"/will">>,
                                                      will_msg = <<"willmsg">>,
                                                      username = <<"test">>,
                                                      password = <<"public">>}}, <<>>} = Parser(ConnBinWithWill),
    ok.

parse_bridge(_) ->
    Parser = emqttd_parser:new([]),
    Data = <<16,86,0,6,77,81,73,115,100,112,131,44,0,60,0,19,67,95,48,48,58,48,67,58,50,57,58,50,66,58,55,55,58,53,50,
             0,48,36,83,89,83,47,98,114,111,107,101,114,47,99,111,110,110,101,99,116,105,111,110,47,67,95,48,48,58,48,
             67,58,50,57,58,50,66,58,55,55,58,53,50,47,115,116,97,116,101,0,1,48>>,

    %% CONNECT(Q0, R0, D0, ClientId=C_00:0C:29:2B:77:52, ProtoName=MQIsdp, ProtoVsn=131, CleanSess=false, KeepAlive=60,
    %% Username=undefined, Password=undefined, Will(Q1, R1, Topic=$SYS/broker/connection/C_00:0C:29:2B:77:52/state, Msg=0))
    {ok, #mqtt_packet{variable = Variable}, <<>>} = Parser(Data),
    #mqtt_packet_connect{client_id  = <<"C_00:0C:29:2B:77:52">>,
                         proto_ver  = 16#03,
                         proto_name = <<"MQIsdp">>,
                         will_retain = true,
                         will_qos   = 1,
                         will_flag  = true,
                         clean_sess = false,
                         keep_alive = 60,
                         will_topic = <<"$SYS/broker/connection/C_00:0C:29:2B:77:52/state">>,
                         will_msg   = <<"0">>} = Variable.

parse_publish(_) ->
    Parser = emqttd_parser:new([]),
    %%PUBLISH(Qos=1, Retain=false, Dup=false, TopicName=a/b/c, PacketId=1, Payload=<<"hahah">>)
    PubBin = <<50,14,0,5,97,47,98,47,99,0,1,104,97,104,97,104>>,
    {ok, #mqtt_packet{header = #mqtt_packet_header{type = ?PUBLISH,
                                                   dup = false,
                                                   qos = 1,
                                                   retain = false},
                      variable = #mqtt_packet_publish{topic_name = <<"a/b/c">>,
                                                      packet_id = 1},
                      payload = <<"hahah">> }, <<>>} = Parser(PubBin),
    
    %PUBLISH(Qos=0, Retain=false, Dup=false, TopicName=xxx/yyy, PacketId=undefined, Payload=<<"hello">>)
    %DISCONNECT(Qos=0, Retain=false, Dup=false)
    PubBin1 = <<48,14,0,7,120,120,120,47,121,121,121,104,101,108,108,111,224,0>>,
    {ok, #mqtt_packet{header = #mqtt_packet_header{type = ?PUBLISH,
                                                   dup = false,
                                                   qos = 0,
                                                   retain = false},
                      variable = #mqtt_packet_publish{topic_name = <<"xxx/yyy">>,
                                                      packet_id = undefined},
                      payload = <<"hello">> }, <<224,0>>} = Parser(PubBin1),
    {ok, #mqtt_packet{header = #mqtt_packet_header{type = ?DISCONNECT,
                                                   dup = false,
                                                   qos = 0,
                                                   retain = false}}, <<>>} = Parser(<<224, 0>>).

parse_puback(_) ->
    Parser = emqttd_parser:new([]),
    %%PUBACK(Qos=0, Retain=false, Dup=false, PacketId=1)
    {ok, #mqtt_packet{header = #mqtt_packet_header{type = ?PUBACK,
                                                   dup = false,
                                                   qos = 0,
                                                   retain = false}}, <<>>} = Parser(<<64,2,0,1>>).
parse_pubrec(_) ->
    Parser = emqttd_parser:new([]),
    %%PUBREC(Qos=0, Retain=false, Dup=false, PacketId=1)
    {ok, #mqtt_packet{header = #mqtt_packet_header{type = ?PUBREC,
                                                   dup = false,
                                                   qos = 0,
                                                   retain = false}}, <<>>} = Parser(<<5:4,0:4,2,0,1>>).

parse_pubrel(_) ->
    Parser = emqttd_parser:new([]),
    {ok, #mqtt_packet{header = #mqtt_packet_header{type = ?PUBREL,
                                                   dup = false,
                                                   qos = 1,
                                                   retain = false}}, <<>>} = Parser(<<6:4,2:4,2,0,1>>).

parse_pubcomp(_) ->
    Parser = emqttd_parser:new([]),
    {ok, #mqtt_packet{header = #mqtt_packet_header{type = ?PUBCOMP,
                                                   dup = false,
                                                   qos = 0,
                                                   retain = false}}, <<>>} = Parser(<<7:4,0:4,2,0,1>>).

parse_subscribe(_) ->
    Parser = emqttd_parser:new([]),
    %% SUBSCRIBE(Q1, R0, D0, PacketId=2, TopicTable=[{<<"TopicA">>,2}])
    {ok, #mqtt_packet{header = #mqtt_packet_header{type = ?SUBSCRIBE,
                                                   dup  = false,
                                                   qos  = 1,
                                                   retain = false},
                      variable = #mqtt_packet_subscribe{packet_id = 2,
                                                        topic_table = [{<<"TopicA">>,2}]} }, <<>>}
        = Parser(<<130,11,0,2,0,6,84,111,112,105,99,65,2>>).

parse_unsubscribe(_) ->
    Parser = emqttd_parser:new([]),
    %% UNSUBSCRIBE(Q1, R0, D0, PacketId=2, TopicTable=[<<"TopicA">>])
    {ok, #mqtt_packet{header = #mqtt_packet_header{type = ?UNSUBSCRIBE,
                                                   dup  = false,
                                                   qos  = 1,
                                                   retain = false},
                      variable = #mqtt_packet_unsubscribe{packet_id = 2,
                                                          topics = [<<"TopicA">>]}}, <<>>}
        = Parser(<<162,10,0,2,0,6,84,111,112,105,99,65>>).

parse_pingreq(_) ->
    Parser = emqttd_parser:new([]),
    {ok, #mqtt_packet{header = #mqtt_packet_header{type = ?PINGREQ,
                                                   dup = false,
                                                   qos = 0,
                                                   retain = false}}, <<>>}
        = Parser(<<?PINGREQ:4, 0:4, 0:8>>).

parse_disconnect(_) ->
    Parser = emqttd_parser:new([]),
    %DISCONNECT(Qos=0, Retain=false, Dup=false)
    Bin = <<224, 0>>,
    {ok, #mqtt_packet{header = #mqtt_packet_header{type = ?DISCONNECT,
                                                   dup  = false,
                                                   qos  = 0,
                                                   retain = false}}, <<>>} = Parser(Bin).

%%--------------------------------------------------------------------
%% Serialize Cases
%%--------------------------------------------------------------------

serialize_connect(_) ->
    serialize(?CONNECT_PACKET(#mqtt_packet_connect{})),
    serialize(?CONNECT_PACKET(#mqtt_packet_connect{
                client_id = <<"clientId">>,
                will_qos = ?QOS1,
                will_flag = true,
                will_retain = true,
                will_topic = <<"will">>,
                will_msg = <<"haha">>,
                clean_sess = true})).

serialize_connack(_) ->
    ConnAck = #mqtt_packet{header = #mqtt_packet_header{type = ?CONNACK}, 
                           variable = #mqtt_packet_connack{ack_flags = 0, return_code = 0}},
    <<32,2,0,0>> = serialize(ConnAck).

serialize_publish(_) ->
    serialize(?PUBLISH_PACKET(?QOS_0, <<"Topic">>, undefined, <<"Payload">>)),
    serialize(?PUBLISH_PACKET(?QOS_1, <<"Topic">>, 938, <<"Payload">>)),
    serialize(?PUBLISH_PACKET(?QOS_2, <<"Topic">>, 99, long_payload())).

serialize_puback(_) ->
    serialize(?PUBACK_PACKET(?PUBACK, 10384)).

serialize_pubrel(_) ->
    serialize(?PUBREL_PACKET(10384)).

serialize_subscribe(_) ->
    TopicTable = [{<<"TopicQos0">>, ?QOS_0}, {<<"TopicQos1">>, ?QOS_1}, {<<"TopicQos2">>, ?QOS_2}],
    serialize(?SUBSCRIBE_PACKET(10, TopicTable)).

serialize_suback(_) ->
    serialize(?SUBACK_PACKET(10, [?QOS_0, ?QOS_1, 128])).

serialize_unsubscribe(_) ->
    serialize(?UNSUBSCRIBE_PACKET(10, [<<"Topic1">>, <<"Topic2">>])).

serialize_unsuback(_) ->
    serialize(?UNSUBACK_PACKET(10)).

serialize_pingreq(_) ->
    serialize(?PACKET(?PINGREQ)).

serialize_pingresp(_) ->
    serialize(?PACKET(?PINGRESP)).

serialize_disconnect(_) ->
    serialize(?PACKET(?DISCONNECT)).

long_payload() ->
    iolist_to_binary(["payload." || _I <- lists:seq(1, 100)]).

%%--------------------------------------------------------------------
%% Packet Cases
%%--------------------------------------------------------------------

packet_proto_name(_) ->
    <<"MQIsdp">> = emqttd_packet:protocol_name(3),
    <<"MQTT">> = emqttd_packet:protocol_name(4).

packet_type_name(_) ->
    'CONNECT' = emqttd_packet:type_name(?CONNECT),
    'UNSUBSCRIBE' = emqttd_packet:type_name(?UNSUBSCRIBE).

packet_connack_name(_) ->
    'CONNACK_ACCEPT'      = emqttd_packet:connack_name(?CONNACK_ACCEPT),
    'CONNACK_PROTO_VER'   = emqttd_packet:connack_name(?CONNACK_PROTO_VER),
    'CONNACK_INVALID_ID'  = emqttd_packet:connack_name(?CONNACK_INVALID_ID),
    'CONNACK_SERVER'      = emqttd_packet:connack_name(?CONNACK_SERVER),
    'CONNACK_CREDENTIALS' = emqttd_packet:connack_name(?CONNACK_CREDENTIALS),
    'CONNACK_AUTH'        = emqttd_packet:connack_name(?CONNACK_AUTH).

packet_format(_) ->
    io:format("~s", [emqttd_packet:format(?CONNECT_PACKET(#mqtt_packet_connect{}))]),
    io:format("~s", [emqttd_packet:format(?CONNACK_PACKET(?CONNACK_SERVER))]),
    io:format("~s", [emqttd_packet:format(?PUBLISH_PACKET(?QOS_1, 1))]),
    io:format("~s", [emqttd_packet:format(?PUBLISH_PACKET(?QOS_2, <<"topic">>, 10, <<"payload">>))]),
    io:format("~s", [emqttd_packet:format(?PUBACK_PACKET(?PUBACK, 98))]),
    io:format("~s", [emqttd_packet:format(?PUBREL_PACKET(99))]),
    io:format("~s", [emqttd_packet:format(?SUBSCRIBE_PACKET(15, [{<<"topic">>, ?QOS0}, {<<"topic1">>, ?QOS1}]))]),
    io:format("~s", [emqttd_packet:format(?SUBACK_PACKET(40, [?QOS0, ?QOS1]))]),
    io:format("~s", [emqttd_packet:format(?UNSUBSCRIBE_PACKET(89, [<<"t">>, <<"t2">>]))]),
    io:format("~s", [emqttd_packet:format(?UNSUBACK_PACKET(90))]).

%%--------------------------------------------------------------------
%% Message Cases
%%--------------------------------------------------------------------

message_make(_) ->
    Msg = emqttd_message:make(<<"clientid">>, <<"topic">>, <<"payload">>),
    0 = Msg#mqtt_message.qos,
    undefined = Msg#mqtt_message.msgid,
    Msg1 = emqttd_message:make(<<"clientid">>, qos2, <<"topic">>, <<"payload">>),
    true = is_binary(Msg1#mqtt_message.msgid),
    2 = Msg1#mqtt_message.qos.

message_from_packet(_) ->
    Msg = emqttd_message:from_packet(?PUBLISH_PACKET(1, <<"topic">>, 10, <<"payload">>)),
    1 = Msg#mqtt_message.qos,
    10 = Msg#mqtt_message.pktid,
    <<"topic">> = Msg#mqtt_message.topic,

    WillMsg = emqttd_message:from_packet(#mqtt_packet_connect{will_flag  = true,
                                                              will_topic = <<"WillTopic">>,
                                                              will_msg   = <<"WillMsg">>}),
    <<"WillTopic">> = WillMsg#mqtt_message.topic,
    <<"WillMsg">> = WillMsg#mqtt_message.payload,

    Msg2 = emqttd_message:from_packet(<<"username">>, <<"clientid">>,
                                      ?PUBLISH_PACKET(1, <<"topic">>, 20, <<"payload">>)),
    <<"clientid">> = Msg2#mqtt_message.from,
    <<"username">> = Msg2#mqtt_message.sender,
    io:format("~s", [emqttd_message:format(Msg2)]).

message_flag(_) ->
    Pkt = ?PUBLISH_PACKET(1, <<"t">>, 2, <<"payload">>),
    Msg2 = emqttd_message:from_packet(<<"clientid">>, Pkt),
    Msg3 = emqttd_message:set_flag(retain, Msg2),
    Msg4 = emqttd_message:set_flag(dup, Msg3),
    true = Msg4#mqtt_message.dup,
    true = Msg4#mqtt_message.retain,
    Msg5 = emqttd_message:set_flag(Msg4),
    Msg6 = emqttd_message:unset_flag(dup, Msg5),
    Msg7 = emqttd_message:unset_flag(retain, Msg6),
    false = Msg7#mqtt_message.dup,
    false = Msg7#mqtt_message.retain,
    emqttd_message:unset_flag(Msg7),
    emqttd_message:to_packet(Msg7).

