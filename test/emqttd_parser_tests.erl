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

-module(emqttd_parser_tests).

-ifdef(TEST).

-include("emqttd_protocol.hrl").

-include_lib("eunit/include/eunit.hrl").

parse_connect_test() ->
    Parser = emqttd_parser:new([]),
    %% CONNECT(Qos=0, Retain=false, Dup=false, ClientId=mosqpub/10451-iMac.loca, ProtoName=MQIsdp, ProtoVsn=3, CleanSess=true, KeepAlive=60, Username=undefined, Password=undefined)
    V31ConnBin = <<16,37,0,6,77,81,73,115,100,112,3,2,0,60,0,23,109,111,115,113,112,117,98,47,49,48,52,53,49,45,105,77,97,99,46,108,111,99,97>>,
    ?assertMatch({ok, #mqtt_packet{
                         header = #mqtt_packet_header{type = ?CONNECT,
                                                      dup  = false,
                                                      qos  = 0,
                                                      retain = false},
                         variable = #mqtt_packet_connect{proto_ver  = 3,
                                                         proto_name = <<"MQIsdp">>,
                                                         client_id  = <<"mosqpub/10451-iMac.loca">>,
                                                         clean_sess = true,
                                                         keep_alive = 60}}, <<>>}, Parser(V31ConnBin)),
    %% CONNECT(Qos=0, Retain=false, Dup=false, ClientId=mosqpub/10451-iMac.loca, ProtoName=MQTT, ProtoVsn=4, CleanSess=true, KeepAlive=60, Username=undefined, Password=undefined)
    V311ConnBin = <<16,35,0,4,77,81,84,84,4,2,0,60,0,23,109,111,115,113,112,117,98,47,49,48,52,53,49,45,105,77,97,99,46,108,111,99,97>>,
    ?assertMatch({ok, #mqtt_packet{ 
                         header = #mqtt_packet_header{type = ?CONNECT, 
                                                      dup = false, 
                                                      qos = 0, 
                                                      retain = false}, 
                         variable = #mqtt_packet_connect{proto_ver  = 4, 
                                                         proto_name = <<"MQTT">>, 
                                                         client_id  = <<"mosqpub/10451-iMac.loca">>,
                                                         clean_sess = true, 
                                                         keep_alive = 60 } }, <<>>}, Parser(V311ConnBin)),

    %% CONNECT(Qos=0, Retain=false, Dup=false, ClientId="", ProtoName=MQTT, ProtoVsn=4, CleanSess=true, KeepAlive=60)
    V311ConnWithoutClientId = <<16,12,0,4,77,81,84,84,4,2,0,60,0,0>>,
    ?assertMatch({ok, #mqtt_packet{ 
                         header = #mqtt_packet_header{type = ?CONNECT, 
                                                      dup = false, 
                                                      qos = 0, 
                                                      retain = false}, 
                         variable = #mqtt_packet_connect{proto_ver = 4, 
                                                         proto_name = <<"MQTT">>, 
                                                         client_id  = <<>>,
                                                         clean_sess = true, 
                                                         keep_alive = 60 } }, <<>>}, Parser(V311ConnWithoutClientId)),
    %%CONNECT(Qos=0, Retain=false, Dup=false, ClientId=mosqpub/10452-iMac.loca, ProtoName=MQIsdp, ProtoVsn=3, CleanSess=true, KeepAlive=60, Username=test, Password=******, Will(Qos=1, Retain=false, Topic=/will, Msg=willmsg))
    ConnBinWithWill = <<16,67,0,6,77,81,73,115,100,112,3,206,0,60,0,23,109,111,115,113,112,117,98,47,49,48,52,53,50,45,105,77,97,99,46,108,111,99,97,0,5,47,119,105,108,108,0,7,119,105,108,108,109,115,103,0,4,116,101,115,116,0,6,112,117,98,108,105,99>>,
    ?assertMatch({ok, #mqtt_packet{ 
                         header = #mqtt_packet_header{type = ?CONNECT,
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
                                                         will_msg = <<"willmsg">> ,
                                                         username = <<"test">>,
                                                         password = <<"public">>}},
                  <<>> }, Parser(ConnBinWithWill)),
    ok.

parse_publish_test() ->
    Parser = emqttd_parser:new([]),
    %%PUBLISH(Qos=1, Retain=false, Dup=false, TopicName=a/b/c, PacketId=1, Payload=<<"hahah">>)
    PubBin = <<50,14,0,5,97,47,98,47,99,0,1,104,97,104,97,104>>,
    ?assertMatch({ok, #mqtt_packet{ 
                         header = #mqtt_packet_header{type = ?PUBLISH, 
                                                      dup = false, 
                                                      qos = 1, 
                                                      retain = false},  
                         variable = #mqtt_packet_publish{topic_name = <<"a/b/c">>, 
                                                         packet_id = 1},
                         payload = <<"hahah">> }, <<>>}, Parser(PubBin)),
    
    %PUBLISH(Qos=0, Retain=false, Dup=false, TopicName=xxx/yyy, PacketId=undefined, Payload=<<"hello">>)
    %DISCONNECT(Qos=0, Retain=false, Dup=false)
    PubBin1 = <<48,14,0,7,120,120,120,47,121,121,121,104,101,108,108,111,224,0>>,
    ?assertMatch({ok, #mqtt_packet { 
                         header = #mqtt_packet_header{type = ?PUBLISH,
                                                      dup = false,
                                                      qos = 0,
                                                      retain = false},
                         variable = #mqtt_packet_publish{topic_name = <<"xxx/yyy">>, 
                                                         packet_id = undefined},
                         payload = <<"hello">> }, <<224,0>>}, Parser(PubBin1)),
    ?assertMatch({ok, #mqtt_packet{ 
                         header = #mqtt_packet_header{type = ?DISCONNECT,
                                                      dup = false,
                                                      qos = 0,
                                                      retain = false}
                        }, <<>>}, Parser(<<224, 0>>)).

parse_puback_test() ->
    Parser = emqttd_parser:new([]),
    %%PUBACK(Qos=0, Retain=false, Dup=false, PacketId=1)
    PubAckBin = <<64,2,0,1>>,
    ?assertMatch({ok, #mqtt_packet { 
                         header = #mqtt_packet_header{type = ?PUBACK, 
                                                      dup = false, 
                                                      qos = 0, 
                                                      retain = false } 
                }, <<>>}, Parser(PubAckBin)),
    ok.

parse_subscribe_test() ->
    ok.

parse_pingreq_test() ->
    ok.

parse_disconnect_test() ->
    Parser = emqttd_parser:new([]),
    %DISCONNECT(Qos=0, Retain=false, Dup=false)
    Bin = <<224, 0>>,
    ?assertMatch({ok, #mqtt_packet{ 
                         header = #mqtt_packet_header{type = ?DISCONNECT,
                                                      dup = false,
                                                      qos = 0,
                                                      retain = false}
                }, <<>>}, Parser(Bin)).

-endif.

