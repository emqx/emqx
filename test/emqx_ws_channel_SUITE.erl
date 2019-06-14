%%--------------------------------------------------------------------
%% Copyright (c) 2019 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_ws_channel_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include("emqx_mqtt.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-define(CLIENT, ?CONNECT_PACKET(#mqtt_packet_connect{
                                client_id = <<"mqtt_client">>,
                                username  = <<"admin">>,
                                password  = <<"public">>})).

all() ->
    [ t_ws_connect_api
    , t_ws_auth_failure
    ].

init_per_suite(Config) ->
    emqx_ct_helpers:start_apps([]),
    Config.

end_per_suite(_Config) ->
    emqx_ct_helpers:stop_apps([]).

t_ws_auth_failure(_Config) ->
    application:set_env(emqx, allow_anonymous, false),
    WS = rfc6455_client:new("ws://127.0.0.1:8083" ++ "/mqtt", self()),
    {ok, _} = rfc6455_client:open(WS),
    Packet = raw_send_serialize(?CLIENT),
    ok = rfc6455_client:send_binary(WS, Packet),
    {binary, CONNACK} = rfc6455_client:recv(WS),
    {ok, ?CONNACK_PACKET(?CONNACK_AUTH), <<>>, _} = raw_recv_pase(CONNACK),
    application:set_env(emqx, allow_anonymous, true),
    ok.

t_ws_connect_api(_Config) ->
    WS = rfc6455_client:new("ws://127.0.0.1:8083" ++ "/mqtt", self()),
    {ok, _} = rfc6455_client:open(WS),
    ok = rfc6455_client:send_binary(WS, raw_send_serialize(?CLIENT)),
    {binary, Bin} = rfc6455_client:recv(WS),
    Connack = ?CONNACK_PACKET(?CONNACK_ACCEPT),
    {ok, Connack, <<>>, _} = raw_recv_pase(Bin),
    Pid = emqx_cm:lookup_conn_pid(<<"mqtt_client">>),
    ConnInfo = emqx_ws_channel:info(Pid),
    ok = t_info(ConnInfo),
    ConnAttrs = emqx_ws_channel:attrs(Pid),
    ok = t_attrs(ConnAttrs),
    ConnStats = emqx_ws_channel:stats(Pid),
    ok = t_stats(ConnStats),
    SessionPid = emqx_ws_channel:session(Pid),
    true = is_pid(SessionPid),
    ok = emqx_ws_channel:kick(Pid),
    {close, _} = rfc6455_client:close(WS),
    ok.

raw_send_serialize(Packet) ->
    emqx_frame:serialize(Packet).

raw_recv_pase(Packet) ->
    emqx_frame:parse(Packet).

t_info(InfoData) ->
    ?assertEqual(websocket, maps:get(socktype, InfoData)),
    ?assertEqual(running, maps:get(conn_state, InfoData)),
    ?assertEqual(<<"mqtt_client">>, maps:get(client_id, InfoData)),
    ?assertEqual(<<"admin">>, maps:get(username, InfoData)),
    ?assertEqual(<<"MQTT">>, maps:get(proto_name, InfoData)).

t_attrs(AttrsData) ->
    ?assertEqual(<<"mqtt_client">>, maps:get(client_id, AttrsData)),
    ?assertEqual(emqx_ws_channel, maps:get(conn_mod, AttrsData)),
    ?assertEqual(<<"admin">>, maps:get(username, AttrsData)).

t_stats(StatsData) ->
    ?assertEqual(true, proplists:get_value(recv_oct, StatsData) >= 0),
    ?assertEqual(true, proplists:get_value(mailbox_len, StatsData) >= 0),
    ?assertEqual(true, proplists:get_value(heap_size, StatsData) >= 0),
    ?assertEqual(true, proplists:get_value(reductions, StatsData) >=0),
    ?assertEqual(true, proplists:get_value(recv_pkt, StatsData) =:=1),
    ?assertEqual(true, proplists:get_value(recv_msg, StatsData) >=0),
    ?assertEqual(true, proplists:get_value(send_pkt, StatsData) =:=1).
