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

-module(emqx_packet_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include("emqx.hrl").
-include("emqx_mqtt.hrl").

-include_lib("eunit/include/eunit.hrl").

all() -> emqx_ct:all(?MODULE).

t_type(_) ->
    ?assertEqual(?CONNECT, emqx_packet:type(?CONNECT_PACKET(#mqtt_packet_connect{}))),
    ?assertEqual(?CONNACK, emqx_packet:type(?CONNACK_PACKET(?RC_SUCCESS))),
    ?assertEqual(?PUBLISH, emqx_packet:type(?PUBLISH_PACKET(?QOS_1))),
    ?assertEqual(?PUBACK, emqx_packet:type(?PUBACK_PACKET(1))),
    ?assertEqual(?PUBREC, emqx_packet:type(?PUBREC_PACKET(1))),
    ?assertEqual(?PUBREL, emqx_packet:type(?PUBREL_PACKET(1))),
    ?assertEqual(?PUBCOMP, emqx_packet:type(?PUBCOMP_PACKET(1))),
    ?assertEqual(?SUBSCRIBE, emqx_packet:type(?SUBSCRIBE_PACKET(1, []))),
    ?assertEqual(?SUBACK, emqx_packet:type(?SUBACK_PACKET(1, [0]))),
    ?assertEqual(?UNSUBSCRIBE, emqx_packet:type(?UNSUBSCRIBE_PACKET(1, []))),
    ?assertEqual(?UNSUBACK, emqx_packet:type(?UNSUBACK_PACKET(1))),
    ?assertEqual(?DISCONNECT, emqx_packet:type(?DISCONNECT_PACKET(?RC_SUCCESS))),
    ?assertEqual(?AUTH, emqx_packet:type(?AUTH_PACKET())).

t_type_name(_) ->
    ?assertEqual('CONNECT', emqx_packet:type_name(?CONNECT_PACKET(#mqtt_packet_connect{}))),
    ?assertEqual('CONNACK', emqx_packet:type_name(?CONNACK_PACKET(?RC_SUCCESS))),
    ?assertEqual('PUBLISH', emqx_packet:type_name(?PUBLISH_PACKET(?QOS_1))),
    ?assertEqual('PUBACK', emqx_packet:type_name(?PUBACK_PACKET(1))),
    ?assertEqual('PUBREC', emqx_packet:type_name(?PUBREC_PACKET(1))),
    ?assertEqual('PUBREL', emqx_packet:type_name(?PUBREL_PACKET(1))),
    ?assertEqual('PUBCOMP', emqx_packet:type_name(?PUBCOMP_PACKET(1))),
    ?assertEqual('SUBSCRIBE', emqx_packet:type_name(?SUBSCRIBE_PACKET(1, []))),
    ?assertEqual('SUBACK', emqx_packet:type_name(?SUBACK_PACKET(1, [0]))),
    ?assertEqual('UNSUBSCRIBE', emqx_packet:type_name(?UNSUBSCRIBE_PACKET(1, []))),
    ?assertEqual('UNSUBACK', emqx_packet:type_name(?UNSUBACK_PACKET(1))),
    ?assertEqual('DISCONNECT', emqx_packet:type_name(?DISCONNECT_PACKET(?RC_SUCCESS))),
    ?assertEqual('AUTH', emqx_packet:type_name(?AUTH_PACKET())).

t_dup(_) ->
    ?assertEqual(false, emqx_packet:dup(?PUBLISH_PACKET(?QOS_1))).

t_qos(_) ->
    ?assertEqual(?QOS_0, emqx_packet:qos(?PUBLISH_PACKET(?QOS_0))),
    ?assertEqual(?QOS_1, emqx_packet:qos(?PUBLISH_PACKET(?QOS_1))),
    ?assertEqual(?QOS_2, emqx_packet:qos(?PUBLISH_PACKET(?QOS_2))).

t_retain(_) ->
    ?assertEqual(false, emqx_packet:retain(?PUBLISH_PACKET(?QOS_1))).

t_proto_name(_) ->
    lists:foreach(
      fun({Ver, Name}) ->
              ConnPkt = ?CONNECT_PACKET(#mqtt_packet_connect{proto_ver  = Ver,
                                                             proto_name = Name}),
              ?assertEqual(Name, emqx_packet:proto_name(ConnPkt))
      end, ?PROTOCOL_NAMES).

t_proto_ver(_) ->
    lists:foreach(
      fun(Ver) ->
              ?assertEqual(Ver, emqx_packet:proto_ver(#mqtt_packet_connect{proto_ver = Ver}))
      end, [?MQTT_PROTO_V3, ?MQTT_PROTO_V4, ?MQTT_PROTO_V5]).

t_check_publish(_) ->
    Props = #{'Response-Topic' => <<"responsetopic">>, 'Topic-Alias' => 1},
    ok = emqx_packet:check(?PUBLISH_PACKET(?QOS_1, <<"topic">>, 1, Props, <<"payload">>)),
    ok = emqx_packet:check(#mqtt_packet_publish{packet_id = 1, topic_name = <<"t">>}),
    {error, ?RC_PROTOCOL_ERROR} = emqx_packet:check(?PUBLISH_PACKET(1,<<>>,1,#{},<<"payload">>)),
    {error, ?RC_TOPIC_NAME_INVALID} = emqx_packet:check(?PUBLISH_PACKET(1, <<"+/+">>, 1, #{}, <<"payload">>)),
    {error, ?RC_TOPIC_ALIAS_INVALID} = emqx_packet:check(?PUBLISH_PACKET(1, <<"topic">>, 1, #{'Topic-Alias' => 0}, <<"payload">>)),
    %% TODO::
    %% {error, ?RC_PROTOCOL_ERROR} = emqx_packet:check(?PUBLISH_PACKET(1, <<"topic">>, 1, #{'Subscription-Identifier' => 10}, <<"payload">>)),
    ok = emqx_packet:check(?PUBLISH_PACKET(1, <<"topic">>, 1, #{'Subscription-Identifier' => 10}, <<"payload">>)),
    {error, ?RC_PROTOCOL_ERROR} = emqx_packet:check(?PUBLISH_PACKET(1, <<"topic">>, 1, #{'Response-Topic' => <<"+/+">>}, <<"payload">>)).

t_check_subscribe(_) ->
    ok = emqx_packet:check(?SUBSCRIBE_PACKET(1, #{'Subscription-Identifier' => 1},
                                             [{<<"topic">>, #{qos => ?QOS_0}}])),
    {error, ?RC_SUBSCRIPTION_IDENTIFIERS_NOT_SUPPORTED} =
    emqx_packet:check(?SUBSCRIBE_PACKET(1, #{'Subscription-Identifier' => -1},
                                        [{<<"topic">>, #{qos => ?QOS_0, rp => 0}}])).

t_check_unsubscribe(_) ->
    ok = emqx_packet:check(?UNSUBSCRIBE_PACKET(1, [<<"topic">>])),
    {error, ?RC_TOPIC_FILTER_INVALID} = emqx_packet:check(?UNSUBSCRIBE_PACKET(1,[])).

t_check_connect(_) ->
    Opts = #{max_clientid_len => 5, mqtt_retain_available => false},
    ok = emqx_packet:check(#mqtt_packet_connect{}, Opts),
    ok = emqx_packet:check(?CONNECT_PACKET(#mqtt_packet_connect{properties = #{'Receive-Maximum' => 1}}), Opts),
    ConnPkt1 = #mqtt_packet_connect{proto_name = <<"MQIsdp">>,
                                    proto_ver  = ?MQTT_PROTO_V5
                                   },
    {error, ?RC_UNSUPPORTED_PROTOCOL_VERSION} = emqx_packet:check(ConnPkt1, Opts),

    ConnPkt2 = #mqtt_packet_connect{proto_ver  = ?MQTT_PROTO_V3,
                                    proto_name = <<"MQIsdp">>,
                                    client_id  = <<>>
                                   },
    {error, ?RC_CLIENT_IDENTIFIER_NOT_VALID} = emqx_packet:check(ConnPkt2, Opts),

    ConnPkt3 = #mqtt_packet_connect{client_id = <<"123456">>},
    {error, ?RC_CLIENT_IDENTIFIER_NOT_VALID} = emqx_packet:check(ConnPkt3, Opts),

    ConnPkt4 = #mqtt_packet_connect{will_flag   = true,
                                    will_retain = true
                                   },
    {error, ?RC_RETAIN_NOT_SUPPORTED} = emqx_packet:check(ConnPkt4, Opts),

    ConnPkt5 = #mqtt_packet_connect{will_flag  = true,
                                    will_topic = <<"#">>
                                   },
    {error, ?RC_TOPIC_NAME_INVALID} = emqx_packet:check(ConnPkt5, Opts),

    ConnPkt6 = ?CONNECT_PACKET(#mqtt_packet_connect{properties = #{'Request-Response-Information' => -1}}),
    {error, ?RC_PROTOCOL_ERROR} = emqx_packet:check(ConnPkt6, Opts),

    {error, ?RC_PROTOCOL_ERROR} = emqx_packet:check(
                                    ?CONNECT_PACKET(#mqtt_packet_connect{
                                                       properties = #{'Request-Problem-Information' => 2}}), Opts),
    {error, ?RC_PROTOCOL_ERROR} = emqx_packet:check(
                                    ?CONNECT_PACKET(#mqtt_packet_connect{
                                                       properties = #{'Receive-Maximum' => 0}}), Opts).

t_from_to_message(_) ->
    ExpectedMsg = emqx_message:set_headers(
                    #{peername => {{127,0,0,1}, 9527}, username => <<"test">>},
                    emqx_message:make(<<"clientid">>, ?QOS_0, <<"topic">>, <<"payload">>)),
    ExpectedMsg1 = emqx_message:set_flag(retain, false, ExpectedMsg),
    Pkt = #mqtt_packet{header = #mqtt_packet_header{type   = ?PUBLISH,
                                                    qos    = ?QOS_0,
                                                    retain = false,
                                                    dup    = false},
                       variable = #mqtt_packet_publish{topic_name = <<"topic">>,
                                                       packet_id  = 10,
                                                       properties = #{}},
                       payload = <<"payload">>},
    MsgFromPkt = emqx_packet:to_message(#{client_id => <<"clientid">>,
                                          username => <<"test">>,
                                          peername => {{127,0,0,1}, 9527}}, Pkt),
    ?assertEqual(ExpectedMsg1, MsgFromPkt#message{id = emqx_message:id(ExpectedMsg),
                                                  timestamp = emqx_message:timestamp(ExpectedMsg)
                                                 }).

t_will_msg(_) ->
    Pkt = #mqtt_packet_connect{will_flag = true,
                               client_id = <<"clientid">>,
                               username = "test",
                               will_retain = true,
                               will_qos = ?QOS_2,
                               will_topic = <<"topic">>,
                               will_props = #{},
                               will_payload = <<"payload">>
                              },
    Msg = emqx_packet:will_msg(Pkt),
    ?assertEqual(<<"clientid">>, Msg#message.from),
    ?assertEqual(<<"topic">>, Msg#message.topic).

t_format(_) ->
    io:format("~s", [emqx_packet:format(?CONNECT_PACKET(#mqtt_packet_connect{}))]),
    io:format("~s", [emqx_packet:format(?CONNACK_PACKET(?CONNACK_SERVER))]),
    io:format("~s", [emqx_packet:format(?PUBLISH_PACKET(?QOS_1, 1))]),
    io:format("~s", [emqx_packet:format(?PUBLISH_PACKET(?QOS_2, <<"topic">>, 10, <<"payload">>))]),
    io:format("~s", [emqx_packet:format(?PUBACK_PACKET(?PUBACK, 98))]),
    io:format("~s", [emqx_packet:format(?PUBREL_PACKET(99))]),
    io:format("~s", [emqx_packet:format(?SUBSCRIBE_PACKET(15, [{<<"topic">>, ?QOS_0}, {<<"topic1">>, ?QOS_1}]))]),
    io:format("~s", [emqx_packet:format(?SUBACK_PACKET(40, [?QOS_0, ?QOS_1]))]),
    io:format("~s", [emqx_packet:format(?UNSUBSCRIBE_PACKET(89, [<<"t">>, <<"t2">>]))]),
    io:format("~s", [emqx_packet:format(?UNSUBACK_PACKET(90))]).

