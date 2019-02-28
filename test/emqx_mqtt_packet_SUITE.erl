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

-module(emqx_mqtt_packet_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-import(emqx_frame, [serialize/1]).

-include("emqx_mqtt.hrl").

-include_lib("eunit/include/eunit.hrl").

-define(INVALID_RESERVED, 1).

-define(CONNECT_INVALID_PACKET(Var),
    #mqtt_packet{header   = #mqtt_packet_header{type = ?INVALID_RESERVED},
                 variable = Var}).

-define(CASE1_PROTOCOL_NAME, ?CONNECT_PACKET(#mqtt_packet_connect{
                                proto_name = <<"MQTC">>,
                                client_id = <<"mqtt_protocol_name">>,
                                username  = <<"admin">>,
                                password  = <<"public">>})).

-define(CASE2_PROTOCAL_VER, ?CONNECT_PACKET(#mqtt_packet_connect{
                                client_id = <<"mqtt_client">>,
                                proto_ver = 6,
                                username  = <<"admin">>,
                                password  = <<"public">>})).

-define(CASE3_PROTOCAL_INVALID_RESERVED, ?CONNECT_INVALID_PACKET(#mqtt_packet_connect{
                                client_id = <<"mqtt_client">>,
                                proto_ver = 5,
                                username  = <<"admin">>,
                                password  = <<"public">>})).

-define(PROTOCOL5, ?CONNECT_PACKET(#mqtt_packet_connect{
                                proto_ver = 5,
                                keepalive = 60,
                                properties = #{'Message-Expiry-Interval' => 3600},
                                client_id = <<"mqtt_client">>,
                                will_topic = <<"will_tipic">>,
                                will_payload = <<"will message">>,
                                username  = <<"admin">>,
                                password  = <<"public">>})).



all() -> [{group, connect}].

groups() -> [{connect, [sequence],
              [case1_protocol_name,
               case2_protocol_ver%,
             %TOTO case3_invalid_reserved
              ]}].

init_per_suite(Config) ->
    emqx_ct_broker_helpers:run_setup_steps(),
    Config.

end_per_suite(_Config) ->
    emqx_ct_broker_helpers:run_teardown_steps().

init_per_group(_Group, Config) ->
    Config.

end_per_group(_Group, _Config) ->
	ok.

case1_protocol_name(_) ->
    {ok, Sock} = emqx_client_sock:connect({127,0,0,1}, 1883, [binary, {packet, raw}, {active, false}], 3000),
    MqttPacket = serialize(?CASE1_PROTOCOL_NAME),
    emqx_client_sock:send(Sock, MqttPacket),
    {ok, Data} = gen_tcp:recv(Sock, 0),
    {ok, ?CONNACK_PACKET(?CONNACK_PROTO_VER), _} = raw_recv_pase(Data),
    Disconnect  = gen_tcp:recv(Sock, 0),
    ?assertEqual({error, closed}, Disconnect).

case2_protocol_ver(_) ->
    {ok, Sock} = emqx_client_sock:connect({127,0,0,1}, 1883, [binary, {packet, raw}, {active, false}], 3000),
    Packet = serialize(?CASE2_PROTOCAL_VER),
    emqx_client_sock:send(Sock, Packet),
    {ok, Data} = gen_tcp:recv(Sock, 0),
    %% case1 Unacceptable protocol version
    {ok, ?CONNACK_PACKET(?CONNACK_PROTO_VER), _} = raw_recv_pase(Data),
    Disconnect  = gen_tcp:recv(Sock, 0),
    ?assertEqual({error, closed}, Disconnect).

case3_invalid_reserved(_) ->
    {ok, Sock} = emqx_client_sock:connect({127,0,0,1}, 1883, [binary, {packet, raw}, {active, false}], 3000),
    Packet = serialize(?CASE3_PROTOCAL_INVALID_RESERVED),
    emqx_client_sock:send(Sock, Packet),
    {ok, Data} = gen_tcp:recv(Sock, 0),
    %% case1 Unacceptable protocol version
    ct:log("Data:~p~n", [raw_recv_pase(Data)]),
    {ok, ?CONNACK_PACKET(?CONNACK_PROTO_VER), _} = raw_recv_pase(Data),
    Disconnect  = gen_tcp:recv(Sock, 0),
    ?assertEqual({error, closed}, Disconnect).

raw_recv_pase(P) ->
    emqx_frame:parse(P, {none, #{max_packet_size => ?MAX_PACKET_SIZE,
                                 version         => ?MQTT_PROTO_V4} }).
