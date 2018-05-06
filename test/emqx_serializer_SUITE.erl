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

-module(emqx_serializer_SUITE).

-compile(export_all).

-include("emqx_mqtt.hrl").

-include_lib("eunit/include/eunit.hrl").

-import(emqx_serializer, [serialize/1]).

all() ->
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
     serialize_disconnect].

serialize_connect(_) ->
    serialize(?CONNECT_PACKET(#mqtt_packet_connect{})),
    serialize(?CONNECT_PACKET(#mqtt_packet_connect{
                client_id = <<"clientId">>,
                will_qos = ?QOS1,
                will_flag = true,
                will_retain = true,
                will_topic = <<"will">>,
                will_payload = <<"haha">>,
                clean_sess = true})).

serialize_connack(_) ->
    ConnAck = #mqtt_packet{header = #mqtt_packet_header{type = ?CONNACK},
                           variable = #mqtt_packet_connack{ack_flags = 0, return_code = 0}},
    ?assertEqual(<<32,2,0,0>>, iolist_to_binary(serialize(ConnAck))).

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
