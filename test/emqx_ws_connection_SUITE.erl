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

-define(INFO, [{socktype, _},
               {conn_state, _},
               {peername, _},
               {sockname, _},
               {zone, _},
               {client_id, <<"mqtt_client">>},
               {username, <<"admin">>},
               {peername, _},
               {peercert, _},
               {proto_ver, _},
               {proto_name, _},
               {clean_start, _},
               {keepalive, _},
               {mountpoint, _},
               {is_super, _},
               {is_bridge, _},
               {connected_at, _},
               {conn_props, _},
               {ack_props, _},
               {session, _},
               {topic_aliases, _},
               {enable_acl, _}]).

-define(ATTRS, [{clean_start,true},
                {client_id, <<"mqtt_client">>},
                {connected_at, _},
                {is_bridge, _},
                {is_super, _},
                {keepalive, _},
                {mountpoint, _},
                {peercert, _},
                {peername, _},
                {proto_name, _},
                {proto_ver, _},
                {sockname, _},
                {username, <<"admin">>},
                {zone, _}]).

-define(STATS, [{recv_oct, _},
                {recv_cnt, _},
                {send_oct, _},
                {send_cnt, _},
                {mailbox_len, _},
                {heap_size, _},
                {reductions, _},
                {recv_pkt, _},
                {recv_msg, _},
                {send_pkt, _},
                {send_msg, _}]).

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
    ?INFO = emqx_ws_connection:info(Pid),
    ?ATTRS = emqx_ws_connection:attrs(Pid),
    ?STATS = emqx_ws_connection:stats(Pid),
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
