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
%%% emqttd_serializer tests.
%%%
%%% @end
%%%-----------------------------------------------------------------------------
-module(emqttd_serializer_tests).

-ifdef(TEST).

-include("emqttd_protocol.hrl").

-include_lib("eunit/include/eunit.hrl").

-import(emqttd_serializer, [serialize/1]).

serilize_connect_test() ->
    serilize(?CONNECT_PACKET(#mqtt_packet_connect{})).

serilize_connack_test() ->
    ConnAck = #mqtt_packet{header = #mqtt_packet_header{type = ?CONNACK}, 
                           variable = #mqtt_packet_connack{ack_flags = 0, return_code = 0}},
    ?assertEqual(<<32,2,0,0>>, serilize(ConnAck)).

serilize_publish_test() ->
    serilize(?PUBLISH_PACKET(?QOS_0, <<"Topic">>, undefined, <<"Payload">>)),
    serilize(?PUBLISH_PACKET(?QOS_1, <<"Topic">>, 938, <<"Payload">>)).

serilize_puback_test() ->
    serilize(?PUBACK_PACKET(?PUBACK, 10384)).

serilize_pubrel_test() ->
    serilize(?PUBREL_PACKET(10384)).

serilize_subscribe_test() ->
    TopicTable = [{<<"TopicQos0">>, ?QOS_0}, {<<"TopicQos1">>, ?QOS_1}, {<<"TopicQos2">>, ?QOS_2}],
    serilize(?SUBSCRIBE_PACKET(10, TopicTable)).

serilize_suback_test() ->
    serilize(?SUBACK_PACKET(10, [?QOS_0, ?QOS_1, 128])).

serilize_unsubscribe_test() ->
    serilize(?UNSUBSCRIBE_PACKET(10, [<<"Topic1">>, <<"Topic2">>])).

serilize_unsuback_test() ->
    serilize(?UNSUBACK_PACKET(10)).

serilize_pingreq_test() ->
    serilize(?PACKET(?PINGREQ)).

serilize_pingresp_test() ->
    serilize(?PACKET(?PINGRESP)).

serilize_disconnect_test() ->
    serilize(?PACKET(?DISCONNECT)).

-endif.

