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

-module(emqx_ws_connection_SUITE).

-include("emqx_mqtt.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(export_all).
-compile(nowarn_export_all).

-import(emqx_ws_connection,
        [ websocket_handle/2
        , websocket_info/2
        ]).

-define(STATS_KEYS, [recv_oct, recv_cnt, send_oct, send_cnt,
                     recv_pkt, recv_msg, send_pkt, send_msg
                    ]).

all() -> emqx_ct:all(?MODULE).

%%--------------------------------------------------------------------
%% CT callbacks
%%--------------------------------------------------------------------

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(_TestCase, Config) ->
    %% Meck CowboyReq
    ok = meck:new(cowboy_req, [passthrough, no_history]),
    ok = meck:expect(cowboy_req, peer, fun(_) -> {{127,0,0,1}, 3456} end),
    ok = meck:expect(cowboy_req, sock, fun(_) -> {{127,0,0,1}, 8883} end),
    ok = meck:expect(cowboy_req, cert, fun(_) -> undefined end),
    ok = meck:expect(cowboy_req, parse_cookies, fun(_) -> undefined end),
    %% Meck Channel
    ok = meck:new(emqx_channel, [passthrough, no_history]),
    ok = meck:expect(emqx_channel, recvd,
                     fun(_Oct, Channel) ->
                             {ok, Channel}
                     end),
    %% Meck Metrics
    ok = meck:new(emqx_metrics, [passthrough, no_history]),
    ok = meck:expect(emqx_metrics, inc, fun(_, _) -> ok end),
    ok = meck:expect(emqx_metrics, inc_recv, fun(_) -> ok end),
    ok = meck:expect(emqx_metrics, inc_sent, fun(_) -> ok end),
    Config.

end_per_testcase(_TestCase, Config) ->
    ok = meck:unload(cowboy_req),
    ok = meck:unload(emqx_channel),
    ok = meck:unload(emqx_metrics),
    Config.

%%--------------------------------------------------------------------
%% Test Cases
%%--------------------------------------------------------------------

%%TODO:...
t_ws_conn_init(_) ->
    with_ws_conn(fun(_WsConn) -> ok end).

t_ws_conn_info(_) ->
    with_ws_conn(fun(WsConn) ->
                         #{sockinfo := SockInfo} = emqx_ws_connection:info(WsConn),
                         #{socktype  := ws,
                           peername  := {{127,0,0,1}, 3456},
                           sockname  := {{127,0,0,1}, 8883},
                           sockstate := idle} = SockInfo
                 end).

t_ws_conn_stats(_) ->
    with_ws_conn(fun(WsConn) ->
                         Stats = emqx_ws_connection:stats(WsConn),
                         lists:foreach(fun(Key) ->
                                               0 = proplists:get_value(Key, Stats)
                                       end, ?STATS_KEYS)
                 end).

t_websocket_init(_) ->
    with_ws_conn(fun(WsConn) ->
                         #{sockinfo := SockInfo} = emqx_ws_connection:info(WsConn),
                         #{socktype  := ws,
                           peername  := {{127,0,0,1}, 3456},
                           sockname  := {{127,0,0,1}, 8883},
                           sockstate := idle
                          } = SockInfo
                 end).

t_websocket_handle_binary(_) ->
    with_ws_conn(fun(WsConn) ->
                         ok = meck:expect(emqx_channel, recvd, fun(_Oct, Channel) -> Channel end),
                         {ok, WsConn} = websocket_handle({binary, [<<>>]}, WsConn)
                 end).

t_websocket_handle_ping_pong(_) ->
    with_ws_conn(fun(WsConn) ->
                         {ok, WsConn} = websocket_handle(ping, WsConn),
                         {ok, WsConn} = websocket_handle(pong, WsConn),
                         {ok, WsConn} = websocket_handle({ping, <<>>}, WsConn),
                         {ok, WsConn} = websocket_handle({pong, <<>>}, WsConn)
                 end).

t_websocket_handle_bad_frame(_) ->
    with_ws_conn(fun(WsConn) ->
                         {stop, WsConn1} = websocket_handle({badframe, <<>>}, WsConn),
                         ?assertEqual({shutdown, unexpected_ws_frame}, stop_reason(WsConn1))
                 end).

t_websocket_info_call(_) ->
    with_ws_conn(fun(WsConn) ->
                         From = {make_ref(), self()},
                         Call = {call, From, badreq},
                         websocket_info(Call, WsConn)
                 end).

t_websocket_info_cast(_) ->
    ok = meck:expect(emqx_channel, handle_info, fun(_Msg, Channel) -> {ok, Channel} end),
    with_ws_conn(fun(WsConn) -> websocket_info({cast, msg}, WsConn) end).

t_websocket_info_incoming(_) ->
    ok = meck:expect(emqx_channel, handle_in, fun(_Packet, Channel) -> {ok, Channel} end),
    with_ws_conn(fun(WsConn) ->
                         Connect = ?CONNECT_PACKET(
                                      #mqtt_packet_connect{proto_ver   = ?MQTT_PROTO_V5,
                                                           proto_name  = <<"MQTT">>,
                                                           clientid    = <<>>,
                                                           clean_start = true,
                                                           keepalive   = 60}),
                         {ok, WsConn1} = websocket_info({incoming, Connect}, WsConn),
                         Publish = ?PUBLISH_PACKET(?QOS_1, <<"t">>, 1, <<"payload">>),
                         {ok, _WsConn2} = websocket_info({incoming, Publish}, WsConn1)
                 end).

t_websocket_info_deliver(_) ->
    with_ws_conn(fun(WsConn) ->
                         ok = meck:expect(emqx_channel, handle_out,
                                          fun(Delivers, Channel) ->
                                                  Packets = [emqx_message:to_packet(1, Msg) || {deliver, _, Msg} <- Delivers],
                                                  {ok, {outgoing, Packets}, Channel}
                                          end),
                         Deliver = {deliver, <<"#">>, emqx_message:make(<<"topic">>, <<"payload">>)},
                         {reply, {binary, _Data}, _WsConn1} = websocket_info(Deliver, WsConn)
                 end).

t_websocket_info_timeout(_) ->
    with_ws_conn(fun(WsConn) ->
                         websocket_info({timeout, make_ref(), keepalive}, WsConn),
                         websocket_info({timeout, make_ref(), emit_stats}, WsConn),
                         websocket_info({timeout, make_ref(), retry_delivery}, WsConn)
                 end).

t_websocket_info_close(_) ->
    with_ws_conn(fun(WsConn) ->
                         {stop, WsConn1} = websocket_info({close, sock_error}, WsConn),
                         ?assertEqual({shutdown, sock_error}, stop_reason(WsConn1))
                 end).

t_websocket_info_shutdown(_) ->
    with_ws_conn(fun(WsConn) ->
                         {stop, WsConn1} = websocket_info({shutdown, reason}, WsConn),
                         ?assertEqual({shutdown, reason}, stop_reason(WsConn1))
                 end).


t_websocket_info_stop(_) ->
    with_ws_conn(fun(WsConn) ->
                         {stop, WsConn1} = websocket_info({stop, normal}, WsConn),
                         ?assertEqual(normal, stop_reason(WsConn1))
                 end).

t_websocket_close(_) ->
    ok = meck:expect(emqx_channel, handle_info,
                     fun({sock_closed, badframe}, Channel) ->
                             {shutdown, sock_closed, Channel}
                     end),
    with_ws_conn(fun(WsConn) ->
                         {stop, WsConn1} = emqx_ws_connection:websocket_close(badframe, WsConn),
                         ?assertEqual(sock_closed, stop_reason(WsConn1))
                 end).

t_handle_call(_) ->
    with_ws_conn(fun(WsConn) -> ok end).

t_handle_info(_) ->
    with_ws_conn(fun(WsConn) -> ok end).

t_handle_timeout(_) ->
    with_ws_conn(fun(WsConn) -> ok end).

t_parse_incoming(_) ->
    with_ws_conn(fun(WsConn) -> ok end).

t_handle_incoming(_) ->
    with_ws_conn(fun(WsConn) -> ok end).

t_handle_return(_) ->
    with_ws_conn(fun(WsConn) -> ok end).

t_handle_outgoing(_) ->
    with_ws_conn(fun(WsConn) -> ok end).

%%--------------------------------------------------------------------
%% Helper functions
%%--------------------------------------------------------------------

with_ws_conn(TestFun) ->
    with_ws_conn(TestFun, []).

with_ws_conn(TestFun, Opts) ->
    {ok, WsConn} = emqx_ws_connection:websocket_init(
                     [req, emqx_misc:merge_opts([{zone, external}], Opts)]),
    TestFun(WsConn).

stop_reason(WsConn) ->
    emqx_ws_connection:info(stop_reason, WsConn).

