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

-module(emqttd_serializer_tests).

-ifdef(TEST).

-include("emqttd_protocol.hrl").

-include_lib("eunit/include/eunit.hrl").

-import(emqttd_serializer, [serialize/1]).

serialize_connect_test() ->
    serialize(?CONNECT_PACKET(#mqtt_packet_connect{})).

serialize_connack_test() ->
    ConnAck = #mqtt_packet{header = #mqtt_packet_header{type = ?CONNACK}, 
                           variable = #mqtt_packet_connack{ack_flags = 0, return_code = 0}},
    ?assertEqual(<<32,2,0,0>>, serialize(ConnAck)).

serialize_publish_test() ->
    serialize(?PUBLISH_PACKET(?QOS_0, <<"Topic">>, undefined, <<"Payload">>)),
    serialize(?PUBLISH_PACKET(?QOS_1, <<"Topic">>, 938, <<"Payload">>)).

serialize_puback_test() ->
    serialize(?PUBACK_PACKET(?PUBACK, 10384)).

serialize_pubrel_test() ->
    serialize(?PUBREL_PACKET(10384)).

serialize_subscribe_test() ->
    TopicTable = [{<<"TopicQos0">>, ?QOS_0}, {<<"TopicQos1">>, ?QOS_1}, {<<"TopicQos2">>, ?QOS_2}],
    serialize(?SUBSCRIBE_PACKET(10, TopicTable)).

serialize_suback_test() ->
    serialize(?SUBACK_PACKET(10, [?QOS_0, ?QOS_1, 128])).

serialize_unsubscribe_test() ->
    serialize(?UNSUBSCRIBE_PACKET(10, [<<"Topic1">>, <<"Topic2">>])).

serialize_unsuback_test() ->
    serialize(?UNSUBACK_PACKET(10)).

serialize_pingreq_test() ->
    serialize(?PACKET(?PINGREQ)).

serialize_pingresp_test() ->
    serialize(?PACKET(?PINGRESP)).

serialize_disconnect_test() ->
    serialize(?PACKET(?DISCONNECT)).

-endif.

