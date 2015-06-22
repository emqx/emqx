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
%%% emqttd_serialiser tests.
%%%
%%% @end
%%%-----------------------------------------------------------------------------
-module(emqttd_serialiser_tests).

-ifdef(TEST).

-include("emqttd_protocol.hrl").

-include_lib("eunit/include/eunit.hrl").

-import(emqttd_serialiser, [serialise/1]).

serialise_connect_test() ->
    serialise(?CONNECT_PACKET(#mqtt_packet_connect{})).

serialise_connack_test() ->
    ConnAck = #mqtt_packet{header = #mqtt_packet_header{type = ?CONNACK}, 
                           variable = #mqtt_packet_connack{ack_flags = 0, return_code = 0}},
    ?assertEqual(<<32,2,0,0>>, serialise(ConnAck)).

serialise_publish_test() ->
    serialise(?PUBLISH_PACKET(?QOS_0, <<"Topic">>, undefined, <<"Payload">>)),
    serialise(?PUBLISH_PACKET(?QOS_1, <<"Topic">>, 938, <<"Payload">>)).

serialise_puback_test() ->
    serialise(?PUBACK_PACKET(?PUBACK, 10384)).

serialise_pubrel_test() ->
    serialise(?PUBREL_PACKET(10384)).

serialise_subscribe_test() ->
    TopicTable = [{<<"TopicQos0">>, ?QOS_0}, {<<"TopicQos1">>, ?QOS_1}, {<<"TopicQos2">>, ?QOS_2}],
    serialise(?SUBSCRIBE_PACKET(10, TopicTable)).

serialise_suback_test() ->
    serialise(?SUBACK_PACKET(10, [?QOS_0, ?QOS_1, 128])).

serialise_unsubscribe_test() ->
    serialise(?UNSUBSCRIBE_PACKET(10, [<<"Topic1">>, <<"Topic2">>])).

serialise_unsuback_test() ->
    serialise(?UNSUBACK_PACKET(10)).

serialise_pingreq_test() ->
    serialise(?PACKET(?PINGREQ)).

serialise_pingresp_test() ->
    serialise(?PACKET(?PINGRESP)).

serialise_disconnect_test() ->
    serialise(?PACKET(?DISCONNECT)).

-endif.

