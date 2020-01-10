%%--------------------------------------------------------------------
%% Copyright (c) 2020 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("emqx_ct_helpers/include/emqx_ct.hrl").

%%-define(PROPTEST(F), ?assert(proper:quickcheck(F()))).
-define(PROPTEST(F), ?assert(proper:quickcheck(F(), [{to_file, user}]))).

all() ->
    [{group, parse},
     {group, connect},
     {group, connack},
     {group, publish},
     {group, puback},
     {group, subscribe},
     {group, suback},
     {group, unsubscribe},
     {group, unsuback},
     {group, ping},
     {group, disconnect},
     {group, auth}
    ].

groups() ->
    [{parse, [parallel],
      [t_parse_cont,
       t_parse_frame_too_large
      ]},
     {connect, [parallel],
      [t_serialize_parse_connect,
       t_serialize_parse_v3_connect,
       t_serialize_parse_v4_connect,
       t_serialize_parse_v5_connect,
       t_serialize_parse_connect_without_clientid,
       t_serialize_parse_connect_with_will,
       t_serialize_parse_bridge_connect
      ]},
     {connack, [parallel],
      [t_serialize_parse_connack,
       t_serialize_parse_connack_v5
      ]},
     {publish, [parallel],
      [t_serialize_parse_qos0_publish,
       t_serialize_parse_qos1_publish,
       t_serialize_parse_qos2_publish,
       t_serialize_parse_publish_v5
      ]},
     {puback, [parallel],
      [t_serialize_parse_puback,
       t_serialize_parse_puback_v3_4,
       t_serialize_parse_puback_v5,
       t_serialize_parse_pubrec,
       t_serialize_parse_pubrec_v5,
       t_serialize_parse_pubrel,
       t_serialize_parse_pubrel_v5,
       t_serialize_parse_pubcomp,
       t_serialize_parse_pubcomp_v5
      ]},
     {subscribe, [parallel],
      [t_serialize_parse_subscribe,
       t_serialize_parse_subscribe_v5
      ]},
     {suback, [parallel],
      [t_serialize_parse_suback,
       t_serialize_parse_suback_v5
      ]},
     {unsubscribe, [parallel],
      [t_serialize_parse_unsubscribe,
       t_serialize_parse_unsubscribe_v5
      ]},
     {unsuback, [parallel],
      [t_serialize_parse_unsuback,
       t_serialize_parse_unsuback_v5
      ]},
     {ping, [parallel],
      [t_serialize_parse_pingreq,
       t_serialize_parse_pingresp
      ]},
     {disconnect, [parallel],
      [t_serialize_parse_disconnect,
       t_serialize_parse_disconnect_v5
      ]},
     {auth, [parallel],
      [t_serialize_parse_auth_v5]
     }].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(_Group, Config) ->
    Config.

end_per_group(_Group, _Config) ->
	ok.

t_parse_cont(_) ->
    Packet = ?CONNECT_PACKET(#mqtt_packet_connect{}),
    ParseState = emqx_frame:initial_parse_state(),
    <<HdrBin:1/binary, LenBin:1/binary, RestBin/binary>> = serialize_to_binary(Packet),
    {more, ContParse} = emqx_frame:parse(<<>>, ParseState),
    {more, ContParse1} = emqx_frame:parse(HdrBin, ContParse),
    {more, ContParse2} = emqx_frame:parse(LenBin, ContParse1),
    {more, ContParse3} = emqx_frame:parse(<<>>, ContParse2),
    {ok, Packet, <<>>, _} = emqx_frame:parse(RestBin, ContParse3).

t_parse_frame_too_large(_) ->
    Packet = ?PUBLISH_PACKET(?QOS_1, <<"t">>, 1, payload(1000)),
    ?catch_error(frame_too_large, parse_serialize(Packet, #{max_size => 256})),
    ?catch_error(frame_too_large, parse_serialize(Packet, #{max_size => 512})),
    ?assertEqual(Packet, parse_serialize(Packet, #{max_size => 2048, version => ?MQTT_PROTO_V4})).

t_serialize_parse_connect(_) ->
    ?PROPTEST(prop_serialize_parse_connect).

prop_serialize_parse_connect() ->
    ?FORALL(Opts = #{version := ProtoVer}, parse_opts(),
            begin
                ProtoName = proplists:get_value(ProtoVer, ?PROTOCOL_NAMES),
                DefaultProps = if ProtoVer == ?MQTT_PROTO_V5 ->
                                      #{};
                                  true -> undefined
                               end,
                Packet = ?CONNECT_PACKET(#mqtt_packet_connect{
                                            proto_name   = ProtoName,
                                            proto_ver    = ProtoVer,
                                            clientid     = <<"clientId">>,
                                            will_qos     = ?QOS_1,
                                            will_flag    = true,
                                            will_retain  = true,
                                            will_topic   = <<"will">>,
                                            will_props   = DefaultProps,
                                            will_payload = <<"bye">>,
                                            clean_start  = true,
                                            properties = DefaultProps
                                           }),
                ok == ?assertEqual(Packet, parse_serialize(Packet, Opts))
            end).

t_serialize_parse_v3_connect(_) ->
    Bin = <<16,37,0,6,77,81,73,115,100,112,3,2,0,60,0,23,109,111,115,
            113,112,117, 98,47,49,48,52,53,49,45,105,77,97,99,46,108,
            111,99,97>>,
    Packet = ?CONNECT_PACKET(
                #mqtt_packet_connect{proto_ver   = ?MQTT_PROTO_V3,
                                     proto_name  = <<"MQIsdp">>,
                                     clientid    = <<"mosqpub/10451-iMac.loca">>,
                                     clean_start = true,
                                     keepalive   = 60
                                    }),
    {ok, Packet, <<>>, PState} = emqx_frame:parse(Bin),
    ?assertMatch({none, #{version := ?MQTT_PROTO_V3}}, PState).

t_serialize_parse_v4_connect(_) ->
    Bin = <<16,35,0,4,77,81,84,84,4,2,0,60,0,23,109,111,115,113,112,117,
            98,47,49,48,52,53,49,45,105,77,97,99,46,108,111,99,97>>,
    Packet = ?CONNECT_PACKET(
                #mqtt_packet_connect{proto_ver   = ?MQTT_PROTO_V4,
                                     proto_name  = <<"MQTT">>,
                                     clientid    = <<"mosqpub/10451-iMac.loca">>,
                                     clean_start = true,
                                     keepalive   = 60
                                    }),
    ?assertEqual(Bin, serialize_to_binary(Packet)),
    ?assertMatch({ok, Packet, <<>>, _}, emqx_frame:parse(Bin)).

t_serialize_parse_v5_connect(_) ->
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
                                     clientid     = <<>>,
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

t_serialize_parse_connect_without_clientid(_) ->
    Bin = <<16,12,0,4,77,81,84,84,4,2,0,60,0,0>>,
    Packet = ?CONNECT_PACKET(#mqtt_packet_connect{proto_ver   = ?MQTT_PROTO_V4,
                                                  proto_name  = <<"MQTT">>,
                                                  clientid    = <<>>,
                                                  clean_start = true,
                                                  keepalive   = 60
                                                 }),
    ?assertEqual(Bin, serialize_to_binary(Packet)),
    ?assertMatch({ok, Packet, <<>>, _}, emqx_frame:parse(Bin)).

t_serialize_parse_connect_with_will(_) ->
    Bin = <<16,67,0,6,77,81,73,115,100,112,3,206,0,60,0,23,109,111,115,113,112,
            117,98,47,49,48,52,53,50,45,105,77,97,99,46,108,111,99,97,0,5,47,119,
            105,108,108,0,7,119,105,108,108,109,115,103,0,4,116,101,115,116,0,6,
            112,117,98,108,105,99>>,
    Packet = #mqtt_packet{header   = #mqtt_packet_header{type = ?CONNECT},
                          variable = #mqtt_packet_connect{proto_ver    = ?MQTT_PROTO_V3,
                                                          proto_name   = <<"MQIsdp">>,
                                                          clientid     = <<"mosqpub/10452-iMac.loca">>,
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

t_serialize_parse_bridge_connect(_) ->
    Bin = <<16,86,0,6,77,81,73,115,100,112,131,44,0,60,0,19,67,95,48,48,58,48,67,
            58,50,57,58,50,66,58,55,55,58,53,50,0,48,36,83,89,83,47,98,114,111,107,
            101,114,47,99,111,110,110,101,99,116,105,111,110,47,67,95,48,48,58,48,
            67,58,50,57,58,50,66,58,55,55,58,53,50,47,115,116,97,116,101,0,1,48>>,
    Topic = <<"$SYS/broker/connection/C_00:0C:29:2B:77:52/state">>,
    Packet = #mqtt_packet{header   = #mqtt_packet_header{type = ?CONNECT},
                          variable = #mqtt_packet_connect{clientid     = <<"C_00:0C:29:2B:77:52">>,
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
    ?assertMatch({ok, Packet, <<>>, _}, emqx_frame:parse(Bin)),
    Packet1 = ?CONNECT_PACKET(#mqtt_packet_connect{is_bridge = true}),
    ?assertEqual(Packet1, parse_serialize(Packet1)).

t_serialize_parse_connack(_) ->
    Packet = ?CONNACK_PACKET(?RC_SUCCESS),
    ?assertEqual(<<32,2,0,0>>, serialize_to_binary(Packet)),
    ?assertEqual(Packet, parse_serialize(Packet)).

t_serialize_parse_connack_v5(_) ->
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

t_serialize_parse_qos0_publish(_) ->
    Bin = <<48,14,0,7,120,120,120,47,121,121,121,104,101,108,108,111>>,
    Packet = #mqtt_packet{header   = #mqtt_packet_header{type   = ?PUBLISH,
                                                         dup    = false,
                                                         qos    = ?QOS_0,
                                                         retain = false},
                          variable = #mqtt_packet_publish{topic_name = <<"xxx/yyy">>,
                                                          packet_id  = undefined},
                          payload  = <<"hello">>},
    ?assertEqual(Bin, serialize_to_binary(Packet)),
    ?assertMatch(Packet, parse_to_packet(Bin, #{strict_mode => true})).

t_serialize_parse_qos1_publish(_) ->
    Bin = <<50,13,0,5,97,47,98,47,99,0,1,104,97,104,97>>,
    Packet = #mqtt_packet{header   = #mqtt_packet_header{type   = ?PUBLISH,
                                                         dup    = false,
                                                         qos    = ?QOS_1,
                                                         retain = false},
                          variable = #mqtt_packet_publish{topic_name = <<"a/b/c">>,
                                                          packet_id  = 1},
                          payload  = <<"haha">>},
    ?assertEqual(Bin, serialize_to_binary(Packet)),
    ?assertMatch(Packet, parse_to_packet(Bin, #{strict_mode => true})),
    %% strict_mode = true
    ?catch_error(bad_packet_id, parse_serialize(?PUBLISH_PACKET(?QOS_1, <<"Topic">>, 0, <<>>))),
    %% strict_mode = false
    _ = parse_serialize(?PUBLISH_PACKET(?QOS_1, <<"Topic">>, 0, <<>>), #{strict_mode => false}).

t_serialize_parse_qos2_publish(_) ->
    Packet = ?PUBLISH_PACKET(?QOS_2, <<"Topic">>, 1, <<>>),
    Bin = <<52,9,0,5,84,111,112,105,99,0,1>>,
    ?assertEqual(Packet, parse_serialize(Packet)),
    ?assertEqual(Bin, serialize_to_binary(Packet)),
    ?assertMatch(Packet, parse_to_packet(Bin, #{strict_mode => true})),
    %% strict_mode = true
    ?catch_error(bad_packet_id, parse_serialize(?PUBLISH_PACKET(?QOS_2, <<"Topic">>, 0, <<>>))),
    %% strict_mode = false
    _ = parse_serialize(?PUBLISH_PACKET(?QOS_2, <<"Topic">>, 0, <<>>), #{strict_mode => false}).

t_serialize_parse_publish_v5(_) ->
    Props = #{'Payload-Format-Indicator' => 1,
              'Message-Expiry-Interval'  => 60,
              'Topic-Alias'              => 16#AB,
              'Response-Topic'           => <<"reply">>,
              'Correlation-Data'         => <<"correlation-id">>,
              'Subscription-Identifier'  => 1,
              'Content-Type'             => <<"text/json">>},
    Packet = ?PUBLISH_PACKET(?QOS_1, <<"$share/group/topic">>, 1, Props, <<"payload">>),
    ?assertEqual(Packet, parse_serialize(Packet, #{version => ?MQTT_PROTO_V5})).

t_serialize_parse_puback(_) ->
    Packet = ?PUBACK_PACKET(1),
    ?assertEqual(<<64,2,0,1>>, serialize_to_binary(Packet)),
    ?assertEqual(Packet, parse_serialize(Packet)),
    %% strict_mode = true
    ?catch_error(bad_packet_id, parse_serialize(?PUBACK_PACKET(0))),
    %% strict_mode = false
    ?PUBACK_PACKET(0) = parse_serialize(?PUBACK_PACKET(0), #{strict_mode => false}).

t_serialize_parse_puback_v3_4(_) ->
    Bin = <<64,2,0,1>>,
    Packet = #mqtt_packet{header = #mqtt_packet_header{type = ?PUBACK}, variable = 1},
    ?assertEqual(Bin, serialize_to_binary(Packet, ?MQTT_PROTO_V3)),
    ?assertEqual(Bin, serialize_to_binary(Packet, ?MQTT_PROTO_V4)),
    ?assertEqual(?PUBACK_PACKET(1), parse_to_packet(Bin, #{version => ?MQTT_PROTO_V3})),
    ?assertEqual(?PUBACK_PACKET(1), parse_to_packet(Bin, #{version => ?MQTT_PROTO_V4})).

t_serialize_parse_puback_v5(_) ->
    Packet = ?PUBACK_PACKET(16, ?RC_SUCCESS, #{'Reason-String' => <<"success">>}),
    ?assertEqual(Packet, parse_serialize(Packet, #{version => ?MQTT_PROTO_V5})).

t_serialize_parse_pubrec(_) ->
    Packet = ?PUBREC_PACKET(1),
    ?assertEqual(<<5:4,0:4,2,0,1>>, serialize_to_binary(Packet)),
    ?assertEqual(Packet, parse_serialize(Packet)),
    %% strict_mode = true
    ?catch_error(bad_packet_id, parse_serialize(?PUBREC_PACKET(0))),
    %% strict_mode = false
    ?PUBREC_PACKET(0) = parse_serialize(?PUBREC_PACKET(0), #{strict_mode => false}).

t_serialize_parse_pubrec_v5(_) ->
    Packet = ?PUBREC_PACKET(16, ?RC_SUCCESS, #{'Reason-String' => <<"success">>}),
    ?assertEqual(Packet, parse_serialize(Packet, #{version => ?MQTT_PROTO_V5})).

t_serialize_parse_pubrel(_) ->
    Packet = ?PUBREL_PACKET(1),
    Bin = serialize_to_binary(Packet),
    ?assertEqual(<<6:4,2:4,2,0,1>>, Bin),
    ?assertEqual(Packet, parse_serialize(Packet)),
    %% PUBREL with bad qos 0
    Bin0 = <<6:4,0:4,2,0,1>>,
    ?assertMatch(Packet, parse_to_packet(Bin0, #{strict_mode => false})),
    ?catch_error(bad_frame_header, parse_to_packet(Bin0, #{strict_mode => true})),
    %% strict_mode = false
    ?PUBREL_PACKET(0) = parse_serialize(?PUBREL_PACKET(0), #{strict_mode => false}),
    %% strict_mode = true
    ?catch_error(bad_packet_id, parse_serialize(?PUBREL_PACKET(0))).

t_serialize_parse_pubrel_v5(_) ->
    Packet = ?PUBREL_PACKET(16, ?RC_SUCCESS, #{'Reason-String' => <<"success">>}),
    ?assertEqual(Packet, parse_serialize(Packet, #{version => ?MQTT_PROTO_V5})).

t_serialize_parse_pubcomp(_) ->
    Packet = ?PUBCOMP_PACKET(1),
    Bin = serialize_to_binary(Packet),
    ?assertEqual(<<7:4,0:4,2,0,1>>, Bin),
    ?assertEqual(Packet, parse_serialize(Packet)),
    %% strict_mode = false
    ?PUBCOMP_PACKET(0) = parse_serialize(?PUBCOMP_PACKET(0), #{strict_mode => false}),
    %% strict_mode = true
    ?catch_error(bad_packet_id, parse_serialize(?PUBCOMP_PACKET(0))).

t_serialize_parse_pubcomp_v5(_) ->
    Packet = ?PUBCOMP_PACKET(16, ?RC_SUCCESS, #{'Reason-String' => <<"success">>}),
    ?assertEqual(Packet, parse_serialize(Packet, #{version => ?MQTT_PROTO_V5})).

t_serialize_parse_subscribe(_) ->
    %% SUBSCRIBE(Q1, R0, D0, PacketId=2, TopicTable=[{<<"TopicA">>,2}])
    Bin = <<?SUBSCRIBE:4,2:4,11,0,2,0,6,84,111,112,105,99,65,2>>,
    TopicOpts = #{nl => 0 , rap => 0, rh => 0, qos => 2},
    TopicFilters = [{<<"TopicA">>, TopicOpts}],
    Packet = ?SUBSCRIBE_PACKET(2, TopicFilters),
    ?assertEqual(Bin, serialize_to_binary(Packet)),
    ?assertMatch(Packet, parse_to_packet(Bin, #{strict_mode => true})),
    %% SUBSCRIBE with bad qos 0
    Bin0 = <<?SUBSCRIBE:4,0:4,11,0,2,0,6,84,111,112,105,99,65,2>>,
    ?assertMatch(Packet, parse_to_packet(Bin0, #{strict_mode => false})),
    %% strict_mode = false
    _ = parse_to_packet(Bin0, #{strict_mode => false}),
    ?catch_error(bad_frame_header, parse_to_packet(Bin0, #{strict_mode => true})),
    %% strict_mode = false
    _ = parse_serialize(?SUBSCRIBE_PACKET(0, TopicFilters), #{strict_mode => false}),
    %% strict_mode = true
    ?catch_error(bad_packet_id, parse_serialize(?SUBSCRIBE_PACKET(0, TopicFilters))),
    ?catch_error(bad_subqos, parse_serialize(?SUBSCRIBE_PACKET(1, [{<<"t">>, #{qos => 3}}]))).

t_serialize_parse_subscribe_v5(_) ->
    TopicFilters = [{<<"TopicQos0">>, #{rh => 1, qos => ?QOS_2, rap => 0, nl => 0}},
                    {<<"TopicQos1">>, #{rh => 1, qos => ?QOS_2, rap => 0, nl => 0}}],
    Packet = ?SUBSCRIBE_PACKET(3, #{'Subscription-Identifier' => 16#FFFFFFF}, TopicFilters),
    ?assertEqual(Packet, parse_serialize(Packet, #{version => ?MQTT_PROTO_V5})).

t_serialize_parse_suback(_) ->
    Packet = ?SUBACK_PACKET(10, [?QOS_0, ?QOS_1, 128]),
    ?assertEqual(Packet, parse_serialize(Packet)),
    %% strict_mode = false
    _ = parse_serialize(?SUBACK_PACKET(0, [?QOS_0]), #{strict_mode => false}),
    %% strict_mode = true
    ?catch_error(bad_packet_id, parse_serialize(?SUBACK_PACKET(0, [?QOS_0]))).

t_serialize_parse_suback_v5(_) ->
    Packet = ?SUBACK_PACKET(1, #{'Reason-String' => <<"success">>,
                                 'User-Property' => [{<<"key">>, <<"value">>}]},
                            [?QOS_0, ?QOS_1, 128]),
    ?assertEqual(Packet, parse_serialize(Packet, #{version => ?MQTT_PROTO_V5})).

t_serialize_parse_unsubscribe(_) ->
    %% UNSUBSCRIBE(Q1, R1, D0, PacketId=2, TopicTable=[<<"TopicA">>])
    Bin = <<?UNSUBSCRIBE:4,2:4,10,0,2,0,6,84,111,112,105,99,65>>,
    Packet = ?UNSUBSCRIBE_PACKET(2, [<<"TopicA">>]),
    ?assertEqual(Bin, serialize_to_binary(Packet)),
    ?assertMatch(Packet, parse_to_packet(Bin, #{strict_mode => true})),
    %% UNSUBSCRIBE with bad qos
    %% UNSUBSCRIBE(Q1, R0, D0, PacketId=2, TopicTable=[<<"TopicA">>])
    Bin0 = <<?UNSUBSCRIBE:4,0:4,10,0,2,0,6,84,111,112,105,99,65>>,
    ?assertMatch(Packet, parse_to_packet(Bin0, #{strict_mode => false})),
    ?catch_error(bad_frame_header, parse_to_packet(Bin0, #{strict_mode => true})),
    %% strict_mode = false
    _ = parse_serialize(?UNSUBSCRIBE_PACKET(0, [<<"TopicA">>]), #{strict_mode => false}),
    %% strict_mode = true
    ?catch_error(bad_packet_id, parse_serialize(?UNSUBSCRIBE_PACKET(0, [<<"TopicA">>]))).

t_serialize_parse_unsubscribe_v5(_) ->
    Props = #{'User-Property' => [{<<"key">>, <<"val">>}]},
    Packet = ?UNSUBSCRIBE_PACKET(10, Props, [<<"Topic1">>, <<"Topic2">>]),
    ?assertEqual(Packet, parse_serialize(Packet, #{version => ?MQTT_PROTO_V5})).

t_serialize_parse_unsuback(_) ->
    Packet = ?UNSUBACK_PACKET(10),
    ?assertEqual(Packet, parse_serialize(Packet)).

t_serialize_parse_unsuback_v5(_) ->
    Packet = ?UNSUBACK_PACKET(10, #{'Reason-String' => <<"Not authorized">>,
                                    'User-Property' => [{<<"key">>, <<"val">>}]},
                              [16#87, 16#87, 16#87]),
    ?assertEqual(Packet, parse_serialize(Packet, #{version => ?MQTT_PROTO_V5})).

t_serialize_parse_pingreq(_) ->
    PingReq = ?PACKET(?PINGREQ),
    ?assertEqual(PingReq, parse_serialize(PingReq)).

t_serialize_parse_pingresp(_) ->
    PingResp = ?PACKET(?PINGRESP),
    ?assertEqual(PingResp, parse_serialize(PingResp)).

t_parse_disconnect(_) ->
    Packet = ?DISCONNECT_PACKET(?RC_SUCCESS),
    ?assertMatch({ok, Packet, <<>>, _}, emqx_frame:parse(<<224, 0>>)).

t_serialize_parse_disconnect(_) ->
    Packet = ?DISCONNECT_PACKET(?RC_SUCCESS),
    ?assertEqual(Packet, parse_serialize(Packet)).

t_serialize_parse_disconnect_v5(_) ->
    Packet = ?DISCONNECT_PACKET(?RC_SUCCESS,
                                #{'Session-Expiry-Interval' => 60,
                                  'Reason-String' => <<"server_moved">>,
                                  'Server-Reference' => <<"192.168.1.10">>
                                 }),
    ?assertEqual(Packet, parse_serialize(Packet, #{version => ?MQTT_PROTO_V5})).

t_serialize_parse_auth_v5(_) ->
    Packet = ?AUTH_PACKET(?RC_SUCCESS,
                          #{'Authentication-Method' => <<"oauth2">>,
                            'Authentication-Data' => <<"3zekkd">>,
                            'Reason-String' => <<"success">>,
                            'User-Property' => [{<<"key1">>, <<"val1">>},
                                                {<<"key2">>, <<"val2">>}]
                           }),
    ?assertEqual(Packet, parse_serialize(Packet, #{version => ?MQTT_PROTO_V5})),
    ?assertEqual(Packet, parse_serialize(Packet, #{version => ?MQTT_PROTO_V5,
                                                   strict_mode => true})).

parse_opts() ->
    ?LET(PropList, [{strict_mode, boolean()}, {version, range(4,5)}], maps:from_list(PropList)).

parse_serialize(Packet) ->
    parse_serialize(Packet, #{strict_mode => true}).

parse_serialize(Packet, Opts) when is_map(Opts) ->
    Ver = maps:get(version, Opts, ?MQTT_PROTO_V4),
    Bin = iolist_to_binary(emqx_frame:serialize(Packet, Ver)),
    ParseState = emqx_frame:initial_parse_state(Opts),
    {ok, NPacket, <<>>, _} = emqx_frame:parse(Bin, ParseState),
    NPacket.

serialize_to_binary(Packet) ->
    iolist_to_binary(emqx_frame:serialize(Packet)).

serialize_to_binary(Packet, Ver) ->
    iolist_to_binary(emqx_frame:serialize(Packet, Ver)).

parse_to_packet(Bin, Opts) ->
    PState = emqx_frame:initial_parse_state(Opts),
    {ok, Packet, <<>>, _} = emqx_frame:parse(Bin, PState),
    Packet.

payload(Len) -> iolist_to_binary(lists:duplicate(Len, 1)).

