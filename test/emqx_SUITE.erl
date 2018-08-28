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

-module(emqx_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-define(APP, emqx).

-include_lib("eunit/include/eunit.hrl").

-include_lib("common_test/include/ct.hrl").

-include("emqx_mqtt.hrl").

-record(ssl_socket, {tcp, ssl}).

-type(socket() :: inet:socket() | #ssl_socket{}).

-define(CLIENT, ?CONNECT_PACKET(#mqtt_packet_connect{
                                client_id = <<"mqtt_client">>,
                                username  = <<"admin">>,
                                password  = <<"public">>})).

-define(CLIENT2, ?CONNECT_PACKET(#mqtt_packet_connect{
                                username  = <<"admin">>,
                                clean_start = false,
                                password  = <<"public">>})).
all() ->
    [{group, connect}%,
    % {group, cleanSession}
    ].

groups() ->
    [{connect, [non_parallel_tests],
      [
      % mqtt_connect,
      % mqtt_connect_with_tcp,
      % mqtt_connect_with_ssl_oneway,
      % mqtt_connect_with_ssl_twoway%,
       mqtt_connect_with_ws%,
      ]},
     {cleanSession, [sequence],
      [cleanSession_validate]
     }
    ].

init_per_suite(Config) ->
    emqx_ct_broker_helpers:run_setup_steps(),
    Config.

end_per_suite(_Config) ->
    emqx_ct_broker_helpers:run_teardown_steps().

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
    Packet = raw_send_serialise(?CLIENT2),
    emqx_client_sock:send(Sock, Packet),
    {ok, Data} = gen_tcp:recv(Sock, 0),
    {ok, ?CONNACK_PACKET(?CONNACK_INVALID_ID), _} = raw_recv_pase(Data),
    emqx_client_sock:close(Sock).

mqtt_connect_with_ssl_oneway(_) ->
    emqx:shutdown(),
    emqx_ct_broker_helpers:change_opts(ssl_oneway),
    emqx:start(),
    ClientSsl = emqx_ct_broker_helpers:client_ssl(),
    {ok, #ssl_socket{tcp = Sock, ssl = SslSock}}
    = emqx_client_sock:connect("127.0.0.1", 8883, [{ssl_opts, ClientSsl}], 3000),
%%     Packet = raw_send_serialise(?CLIENT),
%%     ssl:send(SslSock, Packet),
%%     receive Data  ->
%%         ct:log("Data:~p~n", [Data])
%%     after 30000 ->
%%               ok
%%     end,
    ssl:close(SslSock).

mqtt_connect_with_ssl_twoway(_Config) ->
    emqx:shutdown(),
    emqx_ct_broker_helpers:change_opts(ssl_twoway),
    emqx:start(),
    ClientSsl = emqx_ct_broker_helpers:client_ssl_twoway(),
    {ok, #ssl_socket{tcp = _Sock1, ssl = SslSock} = Sock}
    = emqx_client_sock:connect("127.0.0.1", 8883, [{ssl_opts, ClientSsl}], 3000),
    Packet = raw_send_serialise(?CLIENT),
    emqx_client_sock:setopts(Sock, [{active, once}]),
    emqx_client_sock:send(Sock, Packet),
    timer:sleep(500),
    receive {ssl, _, Data}->
        {ok, ?CONNACK_PACKET(?CONNACK_ACCEPT), _} = raw_recv_pase(Data)
    after 1000 ->
        ok
    end,
    emqx_client_sock:close(Sock).

mqtt_connect_with_ws(_Config) ->
    WS = rfc6455_client:new("ws://127.0.0.1:8083" ++ "/mqtt", self()),
    {ok, _} = rfc6455_client:open(WS),
    Packet = raw_send_serialise(?CLIENT),
    ok = rfc6455_client:send_binary(WS, Packet),
    {binary, P} = rfc6455_client:recv(WS),
    ct:log(":~p", [P]),
    {close, _} = rfc6455_client:close(WS),
    ok.

cleanSession_validate(_) ->
    {ok, C1} = emqttc:start_link([{host, "localhost"},
                                         {port, 1883},
                                         {client_id, <<"c1">>},
                                         {clean_sess, false}]),
    timer:sleep(10),
    emqttc:subscribe(C1, <<"topic">>, qos0),
    emqttc:disconnect(C1),
    {ok, Pub} = emqttc:start_link([{host, "localhost"},
                                         {port, 1883},
                                         {client_id, <<"pub">>}]),

    emqttc:publish(Pub, <<"topic">>, <<"m1">>, [{qos, 0}]),
    timer:sleep(10),
    {ok, C11} = emqttc:start_link([{host, "localhost"},
                                   {port, 1883},
                                   {client_id, <<"c1">>},
                                   {clean_sess, false}]),
    timer:sleep(100),
    receive {publish, _Topic, M1} ->
        ?assertEqual(<<"m1">>, M1)
    after 1000 -> false
    end,
    emqttc:disconnect(Pub),
    emqttc:disconnect(C11).

raw_send_serialise(Packet) ->
    emqx_frame:serialize(Packet).

raw_recv_pase(P) ->
    emqx_frame:parse(P, {none, #{max_packet_size => ?MAX_PACKET_SIZE,
                                 version         => ?MQTT_PROTO_V4} }).

