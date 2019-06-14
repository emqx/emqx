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

-module(emqx_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-define(APP, emqx).

-include_lib("eunit/include/eunit.hrl").

-include_lib("common_test/include/ct.hrl").

-include("emqx_mqtt.hrl").

-record(ssl_socket, {tcp, ssl}).


-define(CLIENT, ?CONNECT_PACKET(#mqtt_packet_connect{
                                client_id = <<"mqtt_client">>,
                                username  = <<"admin">>,
                                password  = <<"public">>})).

-define(CLIENT2, ?CONNECT_PACKET(#mqtt_packet_connect{
                                username  = <<"admin">>,
                                clean_start = false,
                                password  = <<"public">>})).

-define(CLIENT3, ?CONNECT_PACKET(#mqtt_packet_connect{
                                username  = <<"admin">>,
                                proto_ver = ?MQTT_PROTO_V5,
                                clean_start = false,
                                password  = <<"public">>,
                                will_props = #{'Will-Delay-Interval' => 2}})).

-define(SUBCODE, [0]).

-define(PACKETID, 1).

-define(PUBQOS, 1).

-define(SUBPACKET, ?SUBSCRIBE_PACKET(?PACKETID, [{<<"sub/topic">>, ?DEFAULT_SUBOPTS}])).

-define(PUBPACKET, ?PUBLISH_PACKET(?PUBQOS, <<"sub/topic">>, ?PACKETID, <<"publish">>)).

-define(PAYLOAD, [{type,"dsmSimulationData"},
                  {id, 9999},
                  {status, "running"},
                  {soc, 1536702170},
                  {fracsec, 451000},
                  {data, lists:seq(1, 20480)}]).

-define(BIG_PUBPACKET, ?PUBLISH_PACKET(?PUBQOS, <<"sub/topic">>, ?PACKETID, emqx_json:encode(?PAYLOAD))).

all() ->
    [{group, connect},
     {group, publish}].

groups() ->
    [{connect, [non_parallel_tests],
      [mqtt_connect,
       mqtt_connect_with_tcp,
       mqtt_connect_with_will_props,
       mqtt_connect_with_ssl_oneway,
       mqtt_connect_with_ssl_twoway,
       mqtt_connect_with_ws]},
     {publish, [non_parallel_tests],
      [packet_size]}].

init_per_suite(Config) ->
    emqx_ct_helpers:start_apps([]),
    Config.

end_per_suite(_Config) ->
    emqx_ct_helpers:stop_apps([]).

%%--------------------------------------------------------------------
%% Protocol Test
%%--------------------------------------------------------------------
mqtt_connect(_) ->
    %% Issue #599
    %% Empty clientId and clean_session = false
    ?assertEqual(<<32,2,0,2>>, connect_broker_(<<16,12,0,4,77,81,84,84,4,0,0,90,0,0>>, 4)),
    %% Empty clientId and clean_session = true
    ?assertEqual(<<32,2,0,0>>, connect_broker_(<<16,12,0,4,77,81,84,84,4,2,0,90,0,0>>, 4)).

connect_broker_(Packet, RecvSize) ->
    {ok, Sock} = emqx_client_sock:connect({127,0,0,1}, 1883, [binary, {packet, raw}, {active, false}], 3000),
    emqx_client_sock:send(Sock, Packet),
    {ok, Data} = gen_tcp:recv(Sock, RecvSize, 3000),
    emqx_client_sock:close(Sock),
    Data.

mqtt_connect_with_tcp(_) ->
    %% Issue #599
    %% Empty clientId and clean_session = false
    {ok, Sock} = emqx_client_sock:connect({127,0,0,1}, 1883, [binary, {packet, raw}, {active, false}], 3000),
    Packet = raw_send_serialize(?CLIENT2),
    emqx_client_sock:send(Sock, Packet),
    {ok, Data} = gen_tcp:recv(Sock, 0),
    {ok, ?CONNACK_PACKET(?CONNACK_INVALID_ID), <<>>, _} = raw_recv_pase(Data),
    emqx_client_sock:close(Sock).

mqtt_connect_with_will_props(_) ->
    %% Issue #599
    %% Empty clientId and clean_session = false
    {ok, Sock} = emqx_client_sock:connect({127,0,0,1}, 1883, [binary, {packet, raw}, {active, false}], 3000),
    Packet = raw_send_serialize(?CLIENT3),
    emqx_client_sock:send(Sock, Packet),
    emqx_client_sock:close(Sock).

mqtt_connect_with_ssl_oneway(_) ->
    emqx:shutdown(),
    emqx_ct_helpers:change_emqx_opts(ssl_oneway),
    emqx:start(),
    ClientSsl = emqx_ct_helpers:client_ssl(),
    {ok, #ssl_socket{tcp = _Sock1, ssl = SslSock} = Sock}
    = emqx_client_sock:connect("127.0.0.1", 8883, [{ssl_opts, ClientSsl}], 3000),
    Packet = raw_send_serialize(?CLIENT),
    emqx_client_sock:setopts(Sock, [{active, once}]),
    emqx_client_sock:send(Sock, Packet),
    ?assert(
    receive {ssl, _, ConAck}->
        {ok, ?CONNACK_PACKET(?CONNACK_ACCEPT), <<>>, _} = raw_recv_pase(ConAck), true
    after 1000 ->
        false
    end),
    ssl:close(SslSock).

mqtt_connect_with_ssl_twoway(_Config) ->
    emqx:shutdown(),
    emqx_ct_helpers:change_emqx_opts(ssl_twoway),
    emqx:start(),
    ClientSsl = emqx_ct_helpers:client_ssl_twoway(),
    {ok, #ssl_socket{tcp = _Sock1, ssl = SslSock} = Sock}
        = emqx_client_sock:connect("127.0.0.1", 8883, [{ssl_opts, ClientSsl}], 3000),
    Packet = raw_send_serialize(?CLIENT),
    emqx_client_sock:setopts(Sock, [{active, once}]),
    emqx_client_sock:send(Sock, Packet),
    timer:sleep(500),
    ?assert(
    receive {ssl, _, Data}->
        {ok, ?CONNACK_PACKET(?CONNACK_ACCEPT), <<>>, _} = raw_recv_pase(Data), true
    after 1000 ->
        false
    end),
    ssl:close(SslSock),
    emqx_client_sock:close(Sock).

mqtt_connect_with_ws(_Config) ->
    WS = rfc6455_client:new("ws://127.0.0.1:8083" ++ "/mqtt", self()),
    {ok, _} = rfc6455_client:open(WS),

    %% Connect Packet
    Packet = raw_send_serialize(?CLIENT),
    ok = rfc6455_client:send_binary(WS, Packet),
    {binary, CONACK} = rfc6455_client:recv(WS),
    {ok, ?CONNACK_PACKET(?CONNACK_ACCEPT), <<>>, _} = raw_recv_pase(CONACK),

    %% Sub Packet
    SubPacket = raw_send_serialize(?SUBPACKET),
    rfc6455_client:send_binary(WS, SubPacket),
    {binary, SubAck} = rfc6455_client:recv(WS),
    {ok, ?SUBACK_PACKET(?PACKETID, ?SUBCODE), <<>>, _} = raw_recv_pase(SubAck),

    %% Pub Packet QoS 1
    PubPacket = raw_send_serialize(?PUBPACKET),
    rfc6455_client:send_binary(WS, PubPacket),
    {binary, PubAck} = rfc6455_client:recv(WS),
    {ok, ?PUBACK_PACKET(?PACKETID), <<>>, _} = raw_recv_pase(PubAck),
    {close, _} = rfc6455_client:close(WS),
    ok.

%%issue 1811
packet_size(_Config) ->
    {ok, Sock} = emqx_client_sock:connect({127,0,0,1}, 1883, [binary, {packet, raw}, {active, false}], 3000),
    Packet = raw_send_serialize(?CLIENT),
    emqx_client_sock:send(Sock, Packet),
    {ok, Data} = gen_tcp:recv(Sock, 0),
    {ok, ?CONNACK_PACKET(?CONNACK_ACCEPT), <<>>, _} = raw_recv_pase(Data),

    %% Pub Packet QoS 1
    PubPacket = raw_send_serialize(?BIG_PUBPACKET),
    emqx_client_sock:send(Sock, PubPacket),
    {ok, Data1} = gen_tcp:recv(Sock, 0),
    {ok, ?PUBACK_PACKET(?PACKETID), <<>>, _} = raw_recv_pase(Data1),
    emqx_client_sock:close(Sock).

raw_send_serialize(Packet) ->
    emqx_frame:serialize(Packet).

raw_recv_pase(Bin) ->
    emqx_frame:parse(Bin).

