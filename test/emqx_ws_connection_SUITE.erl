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

-module(emqx_ws_connection_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

-include_lib("common_test/include/ct.hrl").

-include("emqx_mqtt.hrl").


-define(CLIENT, ?CONNECT_PACKET(#mqtt_packet_connect{
                                client_id = <<"mqtt_client">>,
                                username  = <<"admin">>,
                                password  = <<"public">>})).

-define(SUBCODE, [0]).

-define(PACKETID, 1).

-define(PUBQOS, 1).

all() ->
    [t_ws_connect_api].

init_per_suite(Config) ->
    emqx_ct_broker_helpers:run_setup_steps(),
    Config.

end_per_suite(_Config) ->
    emqx_ct_broker_helpers:run_teardown_steps().

t_ws_connect_api(_Config) ->
    WS = rfc6455_client:new("ws://127.0.0.1:8083" ++ "/mqtt", self()),
    {ok, _} = rfc6455_client:open(WS),
    Packet = raw_send_serialize(?CLIENT),
    ok = rfc6455_client:send_binary(WS, Packet),
    {binary, CONACK} = rfc6455_client:recv(WS),
    {ok, ?CONNACK_PACKET(?CONNACK_ACCEPT), _} = raw_recv_pase(CONACK),
    Pid = emqx_cm:lookup_conn_pid(<<"mqtt_client">>),
    ConnInfo = emqx_ws_connection:info(Pid),
    ok = t_info(ConnInfo),
    ConnAttrs = emqx_ws_connection:attrs(Pid),
    ok = t_attrs(ConnAttrs),
    ConnStats = emqx_ws_connection:stats(Pid),
    ok = t_stats(ConnStats),
    SessionPid = emqx_ws_connection:session(Pid),
    true = is_pid(SessionPid),
    ok = emqx_ws_connection:kick(Pid),
    {close, _} = rfc6455_client:close(WS),
    ok.

raw_send_serialize(Packet) ->
    emqx_frame:serialize(Packet).

raw_recv_pase(P) ->
    emqx_frame:parse(P, {none, #{max_packet_size => ?MAX_PACKET_SIZE,
                                 version         => ?MQTT_PROTO_V4} }).

t_info(InfoData) ->
    ?assertEqual(websocket, proplists:get_value(socktype, InfoData)),
    ?assertEqual(running, proplists:get_value(conn_state, InfoData)),
    ?assertEqual(<<"mqtt_client">>, proplists:get_value(client_id, InfoData)),
    ?assertEqual(<<"admin">>, proplists:get_value(username, InfoData)),
    ?assertEqual(<<"MQTT">>, proplists:get_value(proto_name, InfoData)).

t_attrs(AttrsData) ->
    ?assertEqual(<<"mqtt_client">>, proplists:get_value(client_id, AttrsData)),
    ?assertEqual(emqx_ws_connection, proplists:get_value(conn_mod, AttrsData)),
    ?assertEqual(<<"admin">>, proplists:get_value(username, AttrsData)).

t_stats(StatsData) ->
    ?assertEqual(true, proplists:get_value(recv_oct, StatsData) >= 0),
    ?assertEqual(true, proplists:get_value(mailbox_len, StatsData) >= 0),
    ?assertEqual(true, proplists:get_value(heap_size, StatsData) >= 0),
    ?assertEqual(true, proplists:get_value(reductions, StatsData) >=0),
    ?assertEqual(true, proplists:get_value(recv_pkt, StatsData) =:=1),
    ?assertEqual(true, proplists:get_value(recv_msg, StatsData) >=0),
    ?assertEqual(true, proplists:get_value(send_pkt, StatsData) =:=1).