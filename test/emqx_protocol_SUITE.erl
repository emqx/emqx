
%%--------------------------------------------------------------------
%% Copyright (c) 2013-2018 EMQ Enterprise, Inc. (http://emqtt.io)
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

-module(emqx_protocol_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-import(lists, [nth/2]).

-import(emqx_frame_SUITE, [parse/1,parse/2]).

-include_lib("eunit/include/eunit.hrl").

-include_lib1("common_test/include/ct.hrl").

-include("emqx_mqtt.hrl").

-define(TOPICS, [<<"TopicA">>, <<"TopicA/B">>, <<"Topic/C">>, <<"TopicA/C">>,
                 <<"/TopicA">>]).

-define(CLIENT2, ?CONNECT_PACKET(#mqtt_packet_connect{
                                    username  = <<"admin">>,
                                    clean_start = false,
                                    password  = <<"public">>})).

receive_messages(Count) ->
    receive_messages(Count, []).

receive_messages(0, Msgs) ->
    Msgs;
receive_messages(Count, Msgs) ->
    receive
        Other ->
            ct:log("~p~n", [Other]),
            receive_messages(Count-1, Msgs)
    after 10 ->
        Msgs
    end.

all() ->
    [{group, mqttv4},
     {group, mqttv5}].

groups() ->
    [{mqttv4,
      [sequence],
      [connect_v4]},
     {mqttv5,
      [sequence],
      [connect_v5]
     }].

init_per_suite(Config) ->
    emqx_ct_broker_helpers:run_setup_steps(),
    Config.

end_per_suite(_Config) ->
    emqx_ct_broker_helpers:run_teardown_steps().

connect_v4(_) ->
    ct:print("MQTT v4 connect test starting"),
    {ok, Sock} = emqx_client_sock:connect({127,0,0,1}, 1883, [binary, {packet, raw}, {active, false}], 3000),
    ConnectPacket = raw_send_serialize(?PACKET(?PUBLISH)),
    emqx_client_sock:send(Sock, ConnectPacket),
    {error, closed} = gen_tcp:recv(Sock, 0),
    emqx_client_sock:close(Sock),




    {ok, Sock3} = emqx_client_sock:connect({127,0,0,1}, 1883, [binary, {packet, raw}, {active, false}], 3000),
    ConnectPacket3 = raw_send_serialize(?CONNECT_PACKET(#mqtt_packet_connect{
                                                           client_id  = <<"mqttv4_client">>,
                                                           username = <<"admin">>,
                                                           password = <<"public">>,
                                                           proto_ver  = ?MQTT_PROTO_V4})),
    emqx_client_sock:send(Sock3, ConnectPacket3),
    {ok, Data} = gen_tcp:recv(Sock3, 0),
    {ok, ?CONNACK_PACKET(?CONNACK_ACCEPT), _} = raw_recv_parse(Data, ?MQTT_PROTO_V4),

    emqx_client_sock:send(Sock3, ConnectPacket3),
    {error, closed} = gen_tcp:recv(Sock3, 0),
    emqx_client_sock:close(Sock3),
    ok.


connect_v5(_) ->
    ct:print("MQTT v5 connect test starting"),
    {ok, Sock1} = emqx_client_sock:connect({127,0,0,1}, 1883, [binary, {packet, raw}, {active, false}], 3000),
    ConnectPacket1 = raw_send_serialize(?CONNECT_PACKET(#mqtt_packet_connect{
                                                           proto_ver  = ?MQTT_PROTO_V5,
                                                           properties =
                                                               #{'Request-Response-Information' => -1}})),
    emqx_client_sock:send(Sock1, ConnectPacket1),
    {error, closed} = gen_tcp:recv(Sock1, 0),
    emqx_client_sock:close(Sock1),

    {ok, Sock2} = emqx_client_sock:connect({127,0,0,1}, 1883, [binary, {packet, raw}, {active, false}], 3000),
    ConnectPacket2 = raw_send_serialize(?CONNECT_PACKET(#mqtt_packet_connect{
                                                           proto_ver  = ?MQTT_PROTO_V5,
                                                           properties =
                                                               #{'Request-Problem-Information' => 2}})),
    emqx_client_sock:send(Sock2, ConnectPacket2),
    {error, closed} = gen_tcp:recv(Sock2, 0),
    emqx_client_sock:close(Sock2),

    {ok, Sock3} = emqx_client_sock:connect({127,0,0,1}, 1883, [binary, {packet, raw}, {active, false}], 3000),
    ConnectPacket3 = raw_send_serialize(?CONNECT_PACKET(#mqtt_packet_connect{
                                                           proto_ver  = ?MQTT_PROTO_V5,
                                                           properties =
                                                               #{'Request-Response-Information' => 1}})),
    emqx_client_sock:send(Sock3, ConnectPacket3),
    {ok, Data} = gen_tcp:recv(Sock2, 0),

    ct:log("~p", [raw_recv_parse(Data, ?MQTT_PROTO_V4)]),
    emqx_client_sock:close(Sock3),

    ok.

publish_received(_) ->
    ok.

puback_received(_) ->
    ok.

pubrec_received(_) ->
    ok.

pubrel_received(_) ->
    ok.

pubcomp_received(_) ->
    ok.

subscribe(_) ->
    ok.

unsubscribe(_) ->
    ok.

pingreq(_) ->
    ok.

disconnect_received(_) ->
    ok.

connack(_) ->
    ok.

publish_send(_) ->
    ok.

puback_send(_) ->
    ok.

pubrec_send(_) ->
    ok.

pubrel_send(_) ->
    ok.

pubcomp_send(_) ->
    ok.

suback(_) ->
    ok.

unsuback(_) ->
    ok.

pingresp(_) ->
    ok.

disconnect_send(_) ->
    ok.

raw_send_serialize(Packet) ->
    emqx_frame:serialize(Packet).

raw_send_serialize(Packet, Opts) ->
    emqx_frame:serialize(Packet, Opts).

raw_recv_parse(P, ProtoVersion) ->
    emqx_frame:parse(P, {none, #{max_packet_size => ?MAX_PACKET_SIZE,
                                 version         => ProtoVersion}}).
