%% Copyright (c) 2013-2019 EMQ Technologies Co., Ltd. All Rights Reserved.
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


-module(emqx_packet_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include("emqx.hrl").

-include("emqx_mqtt.hrl").

-include_lib("eunit/include/eunit.hrl").

all() ->
    [
        packet_proto_name,
        packet_type_name,
        packet_validate,
        packet_message,
        packet_format,
        packet_will_msg
    ].

packet_proto_name(_) ->
    ?assertEqual(<<"MQIsdp">>, emqx_packet:protocol_name(3)),
    ?assertEqual(<<"MQTT">>, emqx_packet:protocol_name(4)),
    ?assertEqual(<<"MQTT">>, emqx_packet:protocol_name(5)).

packet_type_name(_) ->
    ?assertEqual('CONNECT',     emqx_packet:type_name(?CONNECT)),
    ?assertEqual('UNSUBSCRIBE', emqx_packet:type_name(?UNSUBSCRIBE)).

packet_validate(_) ->
    ?assert(emqx_packet:validate(?SUBSCRIBE_PACKET(15, #{'Subscription-Identifier' => 1}, [{<<"topic">>, #{qos => ?QOS_0}}]))),
    ?assert(emqx_packet:validate(?UNSUBSCRIBE_PACKET(89, [<<"topic">>]))),
    ?assert(emqx_packet:validate(?CONNECT_PACKET(#mqtt_packet_connect{}))),
    ?assert(emqx_packet:validate(?PUBLISH_PACKET(1, <<"topic">>, 1, #{'Response-Topic' => <<"responsetopic">>, 'Topic-Alias' => 1}, <<"payload">>))),
    ?assert(emqx_packet:validate(?CONNECT_PACKET(#mqtt_packet_connect{properties = #{'Receive-Maximum' => 1}}))),
    ?assertError(subscription_identifier_invalid,
                 emqx_packet:validate(
                   ?SUBSCRIBE_PACKET(15, #{'Subscription-Identifier' => -1},
                                     [{<<"topic">>, #{qos => ?QOS_0}}]))),
    ?assertError(topic_filters_invalid,
                 emqx_packet:validate(?UNSUBSCRIBE_PACKET(1,[]))),
    ?assertError(topic_name_invalid,
                 emqx_packet:validate(?PUBLISH_PACKET(1,<<>>,1,#{},<<"payload">>))),
    ?assertError(topic_name_invalid,
                 emqx_packet:validate(?PUBLISH_PACKET
                                             (1, <<"+/+">>, 1, #{}, <<"payload">>))),
    ?assertError(topic_alias_invalid,
                 emqx_packet:validate(
                       ?PUBLISH_PACKET
                          (1, <<"topic">>, 1, #{'Topic-Alias' => 0}, <<"payload">>))),
    ?assertError(protocol_error,
                 emqx_packet:validate(
                   ?PUBLISH_PACKET(1, <<"topic">>, 1,
                                   #{'Subscription-Identifier' => 10}, <<"payload">>))),
    ?assertError(protocol_error,
                 emqx_packet:validate(
                   ?PUBLISH_PACKET(1, <<"topic">>, 1,
                                   #{'Response-Topic' => <<"+/+">>}, <<"payload">>))),
    ?assertError(protocol_error,
                 emqx_packet:validate(
                   ?CONNECT_PACKET(#mqtt_packet_connect{
                                      properties =
                                          #{'Request-Response-Information' => -1}}))),
    ?assertError(protocol_error,
                 emqx_packet:validate(
                   ?CONNECT_PACKET(#mqtt_packet_connect{
                                      properties =
                                          #{'Request-Problem-Information' => 2}}))),
    ?assertError(protocol_error,
                emqx_packet:validate(
                  ?CONNECT_PACKET(#mqtt_packet_connect{
                                      properties =
                                          #{'Receive-Maximum' => 0}}))).

packet_message(_) ->
    Pkt = #mqtt_packet{header = #mqtt_packet_header{type   = ?PUBLISH,
                                                    qos    = ?QOS_0,
                                                    retain = false,
                                                    dup    = false},
                       variable = #mqtt_packet_publish{topic_name = <<"topic">>,
                                                       packet_id  = 10,
                                                       properties = #{}},
                       payload = <<"payload">>},
    Msg = emqx_message:make(<<"clientid">>, ?QOS_0, <<"topic">>, <<"payload">>),
    Msg2 = emqx_message:set_flag(retain, false, Msg),
    Pkt = emqx_packet:from_message(10, Msg2),
    Msg3 = emqx_message:set_header(
             peername, {{127,0,0,1}, 9527},
             emqx_message:set_header(username, "test", Msg2)
            ),
    Msg4 = emqx_packet:to_message(#{client_id => <<"clientid">>,
                                    username => "test",
                                    peername => {{127,0,0,1}, 9527}}, Pkt),
    Msg5 = Msg4#message{timestamp = Msg3#message.timestamp, id = Msg3#message.id},
    Msg5 = Msg3.

packet_format(_) ->
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

packet_will_msg(_) ->
    Pkt = #mqtt_packet_connect{ will_flag = true,
                                client_id = <<"clientid">>,
                                username = "test",
                                will_retain = true,
                                will_qos = ?QOS_2,
                                will_topic = <<"topic">>,
                                will_props = #{},
                                will_payload = <<"payload">>},
    Msg = emqx_packet:will_msg(Pkt),
    ?assertEqual(<<"clientid">>, Msg#message.from),
    ?assertEqual(<<"topic">>, Msg#message.topic).
