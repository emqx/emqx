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
%%% emqtt_parser tests.
%%%
%%% @end
%%%-----------------------------------------------------------------------------
-module(emqtt_parser_tests).

-include("emqtt.hrl").
-include("emqtt_packet.hrl").

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

parse_connect_test() ->
    State = emqtt_parser:init([]),
    %% CONNECT(Qos=0, Retain=false, Dup=false, ClientId=mosqpub/10451-iMac.loca, ProtoName=MQIsdp, ProtoVsn=3, CleanSess=true, KeepAlive=60, Username=undefined, Password=undefined)
    V31ConnBin = <<16,37,0,6,77,81,73,115,100,112,3,2,0,60,0,23,109,111,115,113,112,117,98,47,49,48,52,53,49,45,105,77,97,99,46,108,111,99,97>>,
    ?assertMatch({ok, #mqtt_packet{
                         header = #mqtt_packet_header{type = ?CONNECT,
                                                      dup  = false,
                                                      qos  = 0,
                                                      retain = false},
                         variable = #mqtt_packet_connect{proto_ver  = 3,
                                                         proto_name = <<"MQIsdp">>,
                                                         clientid   = <<"mosqpub/10451-iMac.loca">>,
                                                         clean_sess = true,
                                                         keep_alive = 60}}, <<>>}, emqtt_parser:parse(V31ConnBin, State)),
    %% CONNECT(Qos=0, Retain=false, Dup=false, ClientId=mosqpub/10451-iMac.loca, ProtoName=MQTT, ProtoVsn=4, CleanSess=true, KeepAlive=60, Username=undefined, Password=undefined)
    V311ConnBin = <<16,35,0,4,77,81,84,84,4,2,0,60,0,23,109,111,115,113,112,117,98,47,49,48,52,53,49,45,105,77,97,99,46,108,111,99,97>>,
    ?assertMatch({ok, #mqtt_packet{ 
                         header = #mqtt_packet_header{type = ?CONNECT, 
                                                      dup = false, 
                                                      qos = 0, 
                                                      retain = false}, 
                         variable = #mqtt_packet_connect{proto_ver  = 4, 
                                                         proto_name = <<"MQTT">>, 
                                                         clientid   = <<"mosqpub/10451-iMac.loca">>,
                                                         clean_sess = true, 
                                                         keep_alive = 60 } }, <<>>}, emqtt_parser:parse(V311ConnBin, State)),

    %% CONNECT(Qos=0, Retain=false, Dup=false, ClientId="", ProtoName=MQTT, ProtoVsn=4, CleanSess=true, KeepAlive=60)
    V311ConnWithoutClientId = <<16,12,0,4,77,81,84,84,4,2,0,60,0,0>>,
    ?assertMatch({ok, #mqtt_packet{ 
                         header = #mqtt_packet_header{type = ?CONNECT, 
                                                      dup = false, 
                                                      qos = 0, 
                                                      retain = false}, 
                         variable = #mqtt_packet_connect{proto_ver = 4, 
                                                         proto_name = <<"MQTT">>, 
                                                         clientid = <<>>,
                                                         clean_sess = true, 
                                                         keep_alive = 60 } }, <<>>}, emqtt_parser:parse(V311ConnWithoutClientId, State)),
    %%CONNECT(Qos=0, Retain=false, Dup=false, ClientId=mosqpub/10452-iMac.loca, ProtoName=MQIsdp, ProtoVsn=3, CleanSess=true, KeepAlive=60, Username=test, Password=******, Will(Qos=1, Retain=false, Topic=/will, Msg=willmsg))
    ConnBinWithWill = <<16,67,0,6,77,81,73,115,100,112,3,206,0,60,0,23,109,111,115,113,112,117,98,47,49,48,52,53,50,45,105,77,97,99,46,108,111,99,97,0,5,47,119,105,108,108,0,7,119,105,108,108,109,115,103,0,4,116,101,115,116,0,6,112,117,98,108,105,99>>,
    ?assertMatch({ok, #mqtt_packet{ 
                         header = #mqtt_packet_header{type = ?CONNECT,
                                                      dup = false,
                                                      qos = 0,
                                                      retain = false},
                         variable = #mqtt_packet_connect{proto_ver = 3,
                                                         proto_name = <<"MQIsdp">>, 
                                                         clientid = <<"mosqpub/10452-iMac.loca">>,
                                                         clean_sess = true, 
                                                         keep_alive = 60,
                                                         will_retain = false,
                                                         will_qos = 1,
                                                         will_flag = true,
                                                         will_topic = <<"/will">>,
                                                         will_msg = <<"willmsg">> ,
                                                         username = <<"test">>,
                                                         password = <<"public">>}},
                                                   <<>> }, emqtt_parser:parse(ConnBinWithWill, State)),
    ok.

parse_publish_test() ->
    State = emqtt_parser:init([]),
    %%PUBLISH(Qos=1, Retain=false, Dup=false, TopicName=a/b/c, PacketId=1, Payload=<<"hahah">>)
    PubBin = <<50,14,0,5,97,47,98,47,99,0,1,104,97,104,97,104>>,
    ?assertMatch({ok, #mqtt_packet{ 
                         header = #mqtt_packet_header{type = ?PUBLISH, 
                                                      dup = false, 
                                                      qos = 1, 
                                                      retain = false},  
                         variable = #mqtt_packet_publish{topic_name = <<"a/b/c">>, 
                                                         packet_id = 1},
                         payload = <<"hahah">> }, <<>>}, emqtt_parser:parse(PubBin, State)),
    
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
                         payload = <<"hello">> }, <<224,0>>}, emqtt_parser:parse(PubBin1, State)),
    ?assertMatch({ok, #mqtt_packet{ 
                         header = #mqtt_packet_header{type = ?DISCONNECT,
                                                      dup = false,
                                                      qos = 0,
                                                      retain = false}
                        }, <<>>}, emqtt_parser:parse(<<224, 0>>, State)).

parse_puback_test() ->
    %%PUBACK(Qos=0, Retain=false, Dup=false, PacketId=1)
    PubAckBin = <<64,2,0,1>>,
    ?assertMatch({ok, #mqtt_packet { 
                         header = #mqtt_packet_header{type = ?PUBACK, 
                                                      dup = false, 
                                                      qos = 0, 
                                                      retain = false } 
                }, <<>>}, emqtt_parser:parse(PubAckBin, emqtt_parser:init([]))),
    ok.

parse_subscribe_test() ->
    ok.

parse_pingreq_test() ->
    ok.

parse_disconnect_test() ->
    %DISCONNECT(Qos=0, Retain=false, Dup=false)
    Bin = <<224, 0>>,
    ?assertMatch({ok, #mqtt_packet{ 
                         header = #mqtt_packet_header{type = ?DISCONNECT,
                                                      dup = false,
                                                      qos = 0,
                                                      retain = false}
                }, <<>>}, emqtt_parser:parse(Bin, emqtt_parser:init([]))).

-endif.

