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

-define(CLIENT, ?CONNECT_PACKET(#mqtt_packet_connect{
                                client_id = <<"mqtt_client">>,
                                username  = <<"admin">>,
                                password  = <<"public">>})).
all() ->
    [{group, connect},
     {group, cleanSession}].

groups() ->
    [{connect, [non_parallel_tests],
      [mqtt_connect,
%       mqtt_connect_with_tcp,
       mqtt_connect_with_ssl_oneway,
       mqtt_connect_with_ssl_twoway%,
     %  mqtt_connect_with_ws
      ]},
     {cleanSession, [sequence],
      [cleanSession_validate]
     }
    ].

init_per_suite(Config) ->
    emqx_ct_broker_helpers:run_setup_steps(),
   % ct:log("Apps:~p", [Apps]),
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
    {ok, Sock} = gen_tcp:connect({127,0,0,1}, 1883, [binary, {packet, raw}, {active, false}]),
    gen_tcp:send(Sock, Packet),
    {ok, Data} = gen_tcp:recv(Sock, RecvSize, 3000),
    gen_tcp:close(Sock),
    Data.

%% mqtt_connect_with_tcp(_) ->
%%     %% Issue #599
%%     %% Empty clientId and clean_session = false
%%     {ok, Sock} = gen_tcp:connect({127,0,0,1}, 1883, [binary, {packet, raw}, {active, false}]),
%%     Packet = raw_send_serialise(?CLIENT),
%%     gen_tcp:send(Sock, Packet),
%%     {ok, Data} = gen_tcp:recv(Sock, 0),
%% %    {ok, ?CONNACK_PACKET(?CONNACK_ACCEPT), _} = raw_recv_pase(Data),
%%     gen_tcp:close(Sock).

mqtt_connect_with_ssl_oneway(_) ->
    emqx:stop(),
    emqx_ct_broker_helpers:change_opts(ssl_oneway),
    emqx:start(),
    timer:sleep(5000),
    {ok, SslOneWay} = emqttc:start_link([{host, "localhost"},
                                         {port, 8883},
                                         {logger, debug},
                                         {client_id, <<"ssloneway">>}, ssl]),
    timer:sleep(100),
    emqttc:subscribe(SslOneWay, <<"topic">>, qos1),
    {ok, Pub} = emqttc:start_link([{host, "localhost"},
                                   {client_id, <<"pub">>}]),
    emqttc:publish(Pub, <<"topic">>, <<"SSL oneWay test">>, [{qos, 1}]),
    timer:sleep(100),
    receive {publish, _Topic, RM} ->
        ?assertEqual(<<"SSL oneWay test">>, RM)
    after 1000 -> false
    end,
    timer:sleep(100),
    emqttc:disconnect(SslOneWay),
    emqttc:disconnect(Pub).

mqtt_connect_with_ssl_twoway(_Config) ->
    emqx:stop(),
    emqx_ct_broker_helpers:change_opts(ssl_twoway),
    emqx:start(),
    timer:sleep(3000),
    ClientSSl = emqx_ct_broker_helpers:client_ssl(),
    {ok, SslTwoWay} = emqttc:start_link([{host, "localhost"},
                                         {port, 8883},
                                         {client_id, <<"ssltwoway">>},
                                         {ssl, ClientSSl}]),
    {ok, Sub} = emqttc:start_link([{host, "localhost"},
                                   {client_id, <<"sub">>}]),
    emqttc:subscribe(Sub, <<"topic">>, qos1),
    emqttc:publish(SslTwoWay, <<"topic">>, <<"ssl client pub message">>, [{qos, 1}]),
    timer:sleep(10),
    receive {publish, _Topic, RM} ->
        ?assertEqual(<<"ssl client pub message">>, RM)
    after 1000 -> false
    end,
    emqttc:disconnect(SslTwoWay),
    emqttc:disconnect(Sub).

%% mqtt_connect_with_ws(_Config) ->
%%     WS = rfc6455_client:new("ws://127.0.0.1:8083" ++ "/mqtt", self()),
%%     {ok, _} = rfc6455_client:open(WS),
%%     Packet = raw_send_serialise(?CLIENT),
%%     ok = rfc6455_client:send_binary(WS, Packet),
%%     {binary, P} = rfc6455_client:recv(WS),
%% %    {ok, ?CONNACK_PACKET(?CONNACK_ACCEPT), _} = raw_recv_pase(P),
%%     {close, _} = rfc6455_client:close(WS),
%%     ok.

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
    emqttc_serialiser:serialise(Packet).

raw_recv_pase(P) ->
    emqttc_parser:parse(P, emqttc_parser:new()).

