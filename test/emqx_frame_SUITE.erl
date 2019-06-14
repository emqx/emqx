%%--------------------------------------------------------------------
%% Copyright (c) 2019 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_frame_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include("emqx_mqtt.hrl").
-include_lib("eunit/include/eunit.hrl").

all() ->
    [{group, connect},
     {group, connack},
     {group, publish},
     {group, puback},
     {group, subscribe},
     {group, suback},
     {group, unsubscribe},
     {group, unsuback},
     {group, ping},
     {group, disconnect},
     {group, auth}].

groups() ->
    [{connect, [parallel],
      [serialize_parse_connect,
       serialize_parse_v3_connect,
       serialize_parse_v4_connect,
       serialize_parse_v5_connect,
       serialize_parse_connect_without_clientid,
       serialize_parse_connect_with_will,
       serialize_parse_bridge_connect
      ]},
     {connack, [parallel],
      [serialize_parse_connack,
       serialize_parse_connack_v5
      ]},
     {publish, [parallel],
      [serialize_parse_qos0_publish,
       serialize_parse_qos1_publish,
       serialize_parse_qos2_publish,
       serialize_parse_publish_v5
      ]},
     {puback, [parallel],
      [serialize_parse_puback,
       serialize_parse_puback_v5,
       serialize_parse_pubrec,
       serialize_parse_pubrec_v5,
       serialize_parse_pubrel,
       serialize_parse_pubrel_v5,
       serialize_parse_pubcomp,
       serialize_parse_pubcomp_v5
      ]},
     {subscribe, [parallel],
      [serialize_parse_subscribe,
       serialize_parse_subscribe_v5
      ]},
     {suback, [parallel],
      [serialize_parse_suback,
       serialize_parse_suback_v5
      ]},
     {unsubscribe, [parallel],
      [serialize_parse_unsubscribe,
       serialize_parse_unsubscribe_v5
      ]},
     {unsuback, [parallel],
      [serialize_parse_unsuback,
       serialize_parse_unsuback_v5
      ]},
     {ping, [parallel],
      [serialize_parse_pingreq,
       serialize_parse_pingresp
      ]},
     {disconnect, [parallel],
      [serialize_parse_disconnect,
       serialize_parse_disconnect_v5
      ]},
     {auth, [parallel],
      [serialize_parse_auth_v5]
     }].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(_Group, Config) ->
    Config.

end_per_group(_Group, _Config) ->
	ok.

serialize_parse_connect(_) ->
    Packet1 = ?CONNECT_PACKET(#mqtt_packet_connect{}),
    ?assertEqual(Packet1, parse_serialize(Packet1)),
    Packet2 = ?CONNECT_PACKET(#mqtt_packet_connect{
                                 client_id    = <<"clientId">>,
                                 will_qos     = ?QOS_1,
                                 will_flag    = true,
                                 will_retain  = true,
                                 will_topic   = <<"will">>,
                                 will_payload = <<"bye">>,
                                 clean_start  = true
                                }),
    ?assertEqual(Packet2, parse_serialize(Packet2)).

serialize_parse_v3_connect(_) ->
    Bin = <<16,37,0,6,77,81,73,115,100,112,3,2,0,60,0,23,109,111,115,
            113,112,117, 98,47,49,48,52,53,49,45,105,77,97,99,46,108,
            111,99,97>>,
    Packet = ?CONNECT_PACKET(
                #mqtt_packet_connect{proto_ver   = ?MQTT_PROTO_V3,
                                     proto_name  = <<"MQIsdp">>,
                                     client_id   = <<"mosqpub/10451-iMac.loca">>,
                                     clean_start = true,
                                     keepalive   = 60
                                    }),
    ?assertMatch({ok, Packet, <<>>, _}, emqx_frame:parse(Bin)).

serialize_parse_v4_connect(_) ->
    Bin = <<16,35,0,4,77,81,84,84,4,2,0,60,0,23,109,111,115,113,112,117,
            98,47,49,48,52,53,49,45,105,77,97,99,46,108,111,99,97>>,
    Packet = ?CONNECT_PACKET(#mqtt_packet_connect{proto_ver   = 4,
                                                  proto_name  = <<"MQTT">>,
                                                  client_id   = <<"mosqpub/10451-iMac.loca">>,
                                                  clean_start = true,
                                                  keepalive   = 60}),
    ?assertEqual(Bin, serialize_to_binary(Packet)),
    ?assertMatch({ok, Packet, <<>>, _}, emqx_frame:parse(Bin)).

serialize_parse_v5_connect(_) ->
    Props = #{'Session-Expiry-Interval'      => 60,
              'Receive-Maximum'              => 100,
              'Maximum-QoS'                  => ?QOS_2,
              'Retain-Available'             => 1,
              'Maximum-Packet-Size'          => 1024,
              'Topic-Alias-Maximum'          => 10,
              'Request-Response-Information' => 1,
              'Request-Problem-Information'  => 1,
              'Authentication-Method'        => <<"oauth2">>,
              'Authentication-Data'          => <<"33kx93k">>
             },

    WillProps = #{'Will-Delay-Interval'      => 60,
                  'Payload-Format-Indicator' => 1,
                  'Message-Expiry-Interval'  => 60,
                  'Content-Type'             => <<"text/json">>,
                  'Response-Topic'           => <<"topic">>,
                  'Correlation-Data'         => <<"correlateid">>,
                  'User-Property'            => [{<<"k">>, <<"v">>}]
                 },
    Packet = ?CONNECT_PACKET(
                #mqtt_packet_connect{proto_name   = <<"MQTT">>,
                                     proto_ver    = ?MQTT_PROTO_V5,
                                     is_bridge    = false,
                                     clean_start  = true,
                                     client_id    = <<>>,
                                     will_flag    = true,
                                     will_qos     = ?QOS_1,
                                     will_retain  = false,
                                     keepalive    = 60,
                                     properties   = Props,
                                     will_props   = WillProps,
                                     will_topic   = <<"topic">>,
                                     will_payload = <<>>,
                                     username     = <<"device:1">>,
                                     password     = <<"passwd">>
                                    }),
    ?assertEqual(Packet, parse_serialize(Packet)).

serialize_parse_connect_without_clientid(_) ->
    Bin = <<16,12,0,4,77,81,84,84,4,2,0,60,0,0>>,
    Packet = ?CONNECT_PACKET(
                #mqtt_packet_connect{proto_ver   = 4,
                                     proto_name  = <<"MQTT">>,
                                     client_id   = <<>>,
                                     clean_start = true,
                                     keepalive   = 60
                                    }),
    ?assertEqual(Bin, serialize_to_binary(Packet)),
    ?assertMatch({ok, Packet, <<>>, _}, emqx_frame:parse(Bin)).

serialize_parse_connect_with_will(_) ->
    Bin = <<16,67,0,6,77,81,73,115,100,112,3,206,0,60,0,23,109,111,115,113,112,
            117,98,47,49,48,52,53,50,45,105,77,97,99,46,108,111,99,97,0,5,47,119,
            105,108,108,0,7,119,105,108,108,109,115,103,0,4,116,101,115,116,0,6,
            112,117,98,108,105,99>>,
    Packet = #mqtt_packet{header   = #mqtt_packet_header{type = ?CONNECT},
                          variable = #mqtt_packet_connect{proto_ver    = ?MQTT_PROTO_V3,
                                                          proto_name   = <<"MQIsdp">>,
                                                          client_id    = <<"mosqpub/10452-iMac.loca">>,
                                                          clean_start  = true,
                                                          keepalive    = 60,
                                                          will_retain  = false,
                                                          will_qos     = ?QOS_1,
                                                          will_flag    = true,
                                                          will_topic   = <<"/will">>,
                                                          will_payload = <<"willmsg">>,
                                                          username     = <<"test">>,
                                                          password     = <<"public">>
                                                         }},
    ?assertEqual(Bin, serialize_to_binary(Packet)),
    ?assertMatch({ok, Packet, <<>>, _}, emqx_frame:parse(Bin)).

serialize_parse_bridge_connect(_) ->
    Bin = <<16,86,0,6,77,81,73,115,100,112,131,44,0,60,0,19,67,95,48,48,58,48,67,
            58,50,57,58,50,66,58,55,55,58,53,50,0,48,36,83,89,83,47,98,114,111,107,
            101,114,47,99,111,110,110,101,99,116,105,111,110,47,67,95,48,48,58,48,
            67,58,50,57,58,50,66,58,55,55,58,53,50,47,115,116,97,116,101,0,1,48>>,
    Topic = <<"$SYS/broker/connection/C_00:0C:29:2B:77:52/state">>,
    Packet = #mqtt_packet{header   = #mqtt_packet_header{type = ?CONNECT},
                          variable = #mqtt_packet_connect{client_id    = <<"C_00:0C:29:2B:77:52">>,
                                                          proto_ver    = 16#03,
                                                          proto_name   = <<"MQIsdp">>,
                                                          is_bridge    = true,
                                                          will_retain  = true,
                                                          will_qos     = ?QOS_1,
                                                          will_flag    = true,
                                                          clean_start  = false,
                                                          keepalive    = 60,
                                                          will_topic   = Topic,
                                                          will_payload = <<"0">>
                                                         }},
    ?assertEqual(Bin, serialize_to_binary(Packet)),
    ?assertMatch({ok, Packet, <<>>, _}, emqx_frame:parse(Bin)).

serialize_parse_connack(_) ->
    Packet = ?CONNACK_PACKET(?RC_SUCCESS),
    ?assertEqual(<<32,2,0,0>>, serialize_to_binary(Packet)),
    ?assertEqual(Packet, parse_serialize(Packet)).

serialize_parse_connack_v5(_) ->
    Props = #{'Session-Expiry-Interval'            => 60,
              'Receive-Maximum'                    => 100,
              'Maximum-QoS'                        => ?QOS_2,
              'Retain-Available'                   => 1,
              'Maximum-Packet-Size'                => 1024,
              'Assigned-Client-Identifier'         => <<"id">>,
              'Topic-Alias-Maximum'                => 10,
              'Reason-String'                      => <<>>,
              'Wildcard-Subscription-Available'    => 1,
              'Subscription-Identifier-Available'  => 1,
              'Shared-Subscription-Available'      => 1,
              'Server-Keep-Alive'                  => 60,
              'Response-Information'               => <<"response">>,
              'Server-Reference'                   => <<"192.168.1.10">>,
              'Authentication-Method'              => <<"oauth2">>,
              'Authentication-Data'                => <<"33kx93k">>
             },
    Packet = ?CONNACK_PACKET(?RC_SUCCESS, 0, Props),
    ?assertEqual(Packet, parse_serialize(Packet, #{version => ?MQTT_PROTO_V5})).

serialize_parse_qos0_publish(_) ->
    Bin = <<48,14,0,7,120,120,120,47,121,121,121,104,101,108,108,111>>,
    Packet = #mqtt_packet{header   = #mqtt_packet_header{type   = ?PUBLISH,
                                                         dup    = false,
                                                         qos    = ?QOS_0,
                                                         retain = false},
                          variable = #mqtt_packet_publish{topic_name = <<"xxx/yyy">>,
                                                          packet_id  = undefined},
                          payload  = <<"hello">>},
    ?assertEqual(Bin, serialize_to_binary(Packet)),
    ?assertMatch({ok, Packet, <<>>, _}, emqx_frame:parse(Bin)).

serialize_parse_qos1_publish(_) ->
    Bin = <<50,13,0,5,97,47,98,47,99,0,1,104,97,104,97>>,
    Packet = #mqtt_packet{header   = #mqtt_packet_header{type   = ?PUBLISH,
                                                         dup    = false,
                                                         qos    = ?QOS_1,
                                                         retain = false},
                          variable = #mqtt_packet_publish{topic_name = <<"a/b/c">>,
                                                          packet_id  = 1},
                          payload  = <<"haha">>},
    ?assertEqual(Bin, serialize_to_binary(Packet)),
    ?assertMatch({ok, Packet, <<>>, _}, emqx_frame:parse(Bin)).

serialize_parse_qos2_publish(_) ->
    Packet = ?PUBLISH_PACKET(?QOS_2, <<"Topic">>, 1, payload()),
    ?assertEqual(Packet, parse_serialize(Packet)).

serialize_parse_publish_v5(_) ->
    Props = #{'Payload-Format-Indicator' => 1,
              'Message-Expiry-Interval'  => 60,
              'Topic-Alias'              => 16#AB,
              'Response-Topic'           => <<"reply">>,
              'Correlation-Data'         => <<"correlation-id">>,
              'Subscription-Identifier'  => 1,
              'Content-Type'             => <<"text/json">>},
    Packet = ?PUBLISH_PACKET(?QOS_1, <<"$share/group/topic">>, 1, Props, <<"payload">>),
    ?assertEqual(Packet, parse_serialize(Packet, #{version => ?MQTT_PROTO_V5})).

serialize_parse_puback(_) ->
    Packet = ?PUBACK_PACKET(1),
    ?assertEqual(<<64,2,0,1>>, serialize_to_binary(Packet)),
    ?assertEqual(Packet, parse_serialize(Packet)).

serialize_parse_puback_v5(_) ->
    Packet = ?PUBACK_PACKET(16, ?RC_SUCCESS, #{'Reason-String' => <<"success">>}),
    ?assertEqual(Packet, parse_serialize(Packet, #{version => ?MQTT_PROTO_V5})).

serialize_parse_pubrec(_) ->
    Packet = ?PUBREC_PACKET(1),
    ?assertEqual(<<5:4,0:4,2,0,1>>, serialize_to_binary(Packet)),
    ?assertEqual(Packet, parse_serialize(Packet)).

serialize_parse_pubrec_v5(_) ->
    Packet = ?PUBREC_PACKET(16, ?RC_SUCCESS, #{'Reason-String' => <<"success">>}),
    ?assertEqual(Packet, parse_serialize(Packet, #{version => ?MQTT_PROTO_V5})).

serialize_parse_pubrel(_) ->
    Packet = ?PUBREL_PACKET(1),
    Bin = serialize_to_binary(Packet),
    ?assertEqual(<<6:4,2:4,2,0,1>>, Bin),
    ?assertEqual(Packet, parse_serialize(Packet)).

serialize_parse_pubrel_v5(_) ->
    Packet = ?PUBREL_PACKET(16, ?RC_SUCCESS, #{'Reason-String' => <<"success">>}),
    ?assertEqual(Packet, parse_serialize(Packet, #{version => ?MQTT_PROTO_V5})).

serialize_parse_pubcomp(_) ->
    Packet = ?PUBCOMP_PACKET(1),
    Bin = serialize_to_binary(Packet),
    ?assertEqual(<<7:4,0:4,2,0,1>>, Bin),
    ?assertEqual(Packet, parse_serialize(Packet)).

serialize_parse_pubcomp_v5(_) ->
    Packet = ?PUBCOMP_PACKET(16, ?RC_SUCCESS, #{'Reason-String' => <<"success">>}),
    ?assertEqual(Packet, parse_serialize(Packet, #{version => ?MQTT_PROTO_V5})).

serialize_parse_subscribe(_) ->
    %% SUBSCRIBE(Q1, R0, D0, PacketId=2, TopicTable=[{<<"TopicA">>,2}])
    Bin = <<130,11,0,2,0,6,84,111,112,105,99,65,2>>,
    TopicOpts = #{nl => 0 , rap => 0, rc => 0, rh => 0, qos => 2},
    TopicFilters = [{<<"TopicA">>, TopicOpts}],
    Packet = ?SUBSCRIBE_PACKET(2, TopicFilters),
    ?assertEqual(Bin, serialize_to_binary(Packet)),
    %%ct:log("Bin: ~p, Packet: ~p ~n", [Packet, parse(Bin)]),
    ?assertMatch({ok, Packet, <<>>, _}, emqx_frame:parse(Bin)).

serialize_parse_subscribe_v5(_) ->
    TopicFilters = [{<<"TopicQos0">>, #{rh => 1, qos => ?QOS_2, rap => 0, nl => 0, rc => 0}},
                    {<<"TopicQos1">>, #{rh => 1, qos => ?QOS_2, rap => 0, nl => 0, rc => 0}}],
    Packet = ?SUBSCRIBE_PACKET(3, #{'Subscription-Identifier' => 16#FFFFFFF}, TopicFilters),
    ?assertEqual(Packet, parse_serialize(Packet, #{version => ?MQTT_PROTO_V5})).

serialize_parse_suback(_) ->
    Packet = ?SUBACK_PACKET(10, [?QOS_0, ?QOS_1, 128]),
    ?assertEqual(Packet, parse_serialize(Packet)).

serialize_parse_suback_v5(_) ->
    Packet = ?SUBACK_PACKET(1, #{'Reason-String' => <<"success">>,
                                 'User-Property' => [{<<"key">>, <<"value">>}]},
                            [?QOS_0, ?QOS_1, 128]),
    ?assertEqual(Packet, parse_serialize(Packet, #{version => ?MQTT_PROTO_V5})).

serialize_parse_unsubscribe(_) ->
    %% UNSUBSCRIBE(Q1, R0, D0, PacketId=2, TopicTable=[<<"TopicA">>])
    Packet = ?UNSUBSCRIBE_PACKET(2, [<<"TopicA">>]),
    Bin = <<162,10,0,2,0,6,84,111,112,105,99,65>>,
    ?assertEqual(Bin, serialize_to_binary(Packet)),
    ?assertMatch({ok, Packet, <<>>, _}, emqx_frame:parse(Bin)).

serialize_parse_unsubscribe_v5(_) ->
    Props = #{'User-Property' => [{<<"key">>, <<"val">>}]},
    Packet = ?UNSUBSCRIBE_PACKET(10, Props, [<<"Topic1">>, <<"Topic2">>]),
    ?assertEqual(Packet, parse_serialize(Packet, #{version => ?MQTT_PROTO_V5})).

serialize_parse_unsuback(_) ->
    Packet = ?UNSUBACK_PACKET(10),
    ?assertEqual(Packet, parse_serialize(Packet)).

serialize_parse_unsuback_v5(_) ->
    Packet = ?UNSUBACK_PACKET(10, #{'Reason-String' => <<"Not authorized">>,
                                    'User-Property' => [{<<"key">>, <<"val">>}]},
                              [16#87, 16#87, 16#87]),
    ?assertEqual(Packet, parse_serialize(Packet, #{version => ?MQTT_PROTO_V5})).

serialize_parse_pingreq(_) ->
    PingReq = ?PACKET(?PINGREQ),
    ?assertEqual(PingReq, parse_serialize(PingReq)).

serialize_parse_pingresp(_) ->
    PingResp = ?PACKET(?PINGRESP),
    ?assertEqual(PingResp, parse_serialize(PingResp)).

parse_disconnect(_) ->
    Packet = ?DISCONNECT_PACKET(?RC_SUCCESS),
    ?assertMatch({ok, Packet, <<>>, _}, emqx_frame:parse(<<224, 0>>)).

serialize_parse_disconnect(_) ->
    Packet = ?DISCONNECT_PACKET(?RC_SUCCESS),
    ?assertEqual(Packet, parse_serialize(Packet)).

serialize_parse_disconnect_v5(_) ->
    Packet = ?DISCONNECT_PACKET(?RC_SUCCESS,
                                #{'Session-Expiry-Interval' => 60,
                                  'Reason-String' => <<"server_moved">>,
                                  'Server-Reference' => <<"192.168.1.10">>
                                 }),
    ?assertEqual(Packet, parse_serialize(Packet, #{version => ?MQTT_PROTO_V5})).

serialize_parse_auth_v5(_) ->
    Packet = ?AUTH_PACKET(?RC_SUCCESS,
                          #{'Authentication-Method' => <<"oauth2">>,
                            'Authentication-Data' => <<"3zekkd">>,
                            'Reason-String' => <<"success">>,
                            'User-Property' => [{<<"key">>, <<"val">>}]
                           }),
    ?assertEqual(Packet, parse_serialize(Packet, #{version => ?MQTT_PROTO_V5})).

parse_serialize(Packet) ->
    parse_serialize(Packet, #{}).

parse_serialize(Packet, Opts) when is_map(Opts) ->
    Bin = iolist_to_binary(emqx_frame:serialize(Packet, Opts)),
    ParseState = emqx_frame:initial_parse_state(Opts),
    {ok, NPacket, <<>>, _} = emqx_frame:parse(Bin, ParseState),
    NPacket.

serialize_to_binary(Packet) ->
    iolist_to_binary(emqx_frame:serialize(Packet)).

payload() ->
    iolist_to_binary(["payload." || _I <- lists:seq(1, 1000)]).

