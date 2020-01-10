%%--------------------------------------------------------------------
%% Copyright (c) 2020 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-include("emqx.hrl").
-include("emqx_mqtt.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(export_all).
-compile(nowarn_export_all).

-import(emqx_ws_connection,
        [ websocket_handle/2
        , websocket_info/2
        , websocket_close/2
        ]).

-define(STATS_KEYS, [recv_oct, recv_cnt, send_oct, send_cnt,
                     recv_pkt, recv_msg, send_pkt, send_msg
                    ]).

-define(ws_conn, emqx_ws_connection).

all() -> emqx_ct:all(?MODULE).

%%--------------------------------------------------------------------
%% CT callbacks
%%--------------------------------------------------------------------

init_per_suite(Config) ->
    %% Mock cowboy_req
    ok = meck:new(cowboy_req, [passthrough, no_history, no_link]),
    ok = meck:expect(cowboy_req, peer, fun(_) -> {{127,0,0,1}, 3456} end),
    ok = meck:expect(cowboy_req, sock, fun(_) -> {{127,0,0,1}, 18083} end),
    ok = meck:expect(cowboy_req, cert, fun(_) -> undefined end),
    ok = meck:expect(cowboy_req, parse_cookies, fun(_) -> error(badarg) end),
    %% Mock emqx_zone
    ok = meck:new(emqx_zone, [passthrough, no_history, no_link]),
    ok = meck:expect(emqx_zone, oom_policy,
                     fun(_) -> #{max_heap_size => 838860800,
                                 message_queue_len => 8000
                                }
                     end),
    %% Mock emqx_access_control
    ok = meck:new(emqx_access_control, [passthrough, no_history, no_link]),
    ok = meck:expect(emqx_access_control, check_acl, fun(_, _, _) -> allow end),
    %% Mock emqx_hooks
    ok = meck:new(emqx_hooks, [passthrough, no_history, no_link]),
    ok = meck:expect(emqx_hooks, run, fun(_Hook, _Args) -> ok end),
    ok = meck:expect(emqx_hooks, run_fold, fun(_Hook, _Args, Acc) -> Acc end),
    %% Mock emqx_broker
    ok = meck:new(emqx_broker, [passthrough, no_history, no_link]),
    ok = meck:expect(emqx_broker, subscribe, fun(_, _, _) -> ok end),
    ok = meck:expect(emqx_broker, publish, fun(#message{topic = Topic}) ->
                                                   [{node(), Topic, 1}]
                                           end),
    ok = meck:expect(emqx_broker, unsubscribe, fun(_) -> ok end),
    %% Mock emqx_metrics
    ok = meck:new(emqx_metrics, [passthrough, no_history, no_link]),
    ok = meck:expect(emqx_metrics, inc, fun(_) -> ok end),
    ok = meck:expect(emqx_metrics, inc, fun(_, _) -> ok end),
    ok = meck:expect(emqx_metrics, inc_recv, fun(_) -> ok end),
    ok = meck:expect(emqx_metrics, inc_sent, fun(_) -> ok end),
    Config.

end_per_suite(_Config) ->
    lists:foreach(fun meck:unload/1,
                  [cowboy_req,
                   emqx_zone,
                   emqx_access_control,
                   emqx_broker,
                   emqx_hooks,
                   emqx_metrics
                  ]).

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, Config) ->
    Config.

%%--------------------------------------------------------------------
%% Test Cases
%%--------------------------------------------------------------------

t_info(_) ->
    WsPid = spawn(fun() ->
                      receive {call, From, info} ->
                                  gen_server:reply(From, ?ws_conn:info(st()))
                      end
                  end),
    #{sockinfo := SockInfo} = ?ws_conn:call(WsPid, info),
    #{socktype  := ws,
      active_n  := 100,
      peername  := {{127,0,0,1}, 3456},
      sockname  := {{127,0,0,1}, 18083},
      sockstate := running
     } = SockInfo.

t_info_limiter(_) ->
    St = st(#{limiter => emqx_limiter:init([])}),
    ?assertEqual(undefined, ?ws_conn:info(limiter, St)).

t_info_channel(_) ->
    #{conn_state := connected} = ?ws_conn:info(channel, st()).

t_info_gc_state(_) ->
    GcSt = emqx_gc:init(#{count => 10, bytes => 1000}),
    GcInfo = ?ws_conn:info(gc_state, st(#{gc_state => GcSt})),
    ?assertEqual(#{cnt => {10,10}, oct => {1000,1000}}, GcInfo).

t_info_postponed(_) ->
    ?assertEqual([], ?ws_conn:info(postponed, st())),
    St = ?ws_conn:postpone({active, false}, st()),
    ?assertEqual([{active, false}], ?ws_conn:info(postponed, St)).

t_stats(_) ->
    WsPid = spawn(fun() ->
                      receive {call, From, stats} ->
                                  gen_server:reply(From, ?ws_conn:stats(st()))
                      end
                  end),
    Stats = ?ws_conn:call(WsPid, stats),
    [{recv_oct, 0}, {recv_cnt, 0}, {send_oct, 0}, {send_cnt, 0},
     {recv_pkt, 0}, {recv_msg, 0}, {send_pkt, 0}, {send_msg, 0}|_] = Stats.

t_call(_) ->
    Info = ?ws_conn:info(st()),
    WsPid = spawn(fun() ->
                      receive {call, From, info} -> gen_server:reply(From, Info) end
                  end),
    ?assertEqual(Info, ?ws_conn:call(WsPid, info)).

t_init(_) ->
    Opts = [{idle_timeout, 300000}],
    WsOpts = #{compress       => false,
               deflate_opts   => #{},
               max_frame_size => infinity,
               idle_timeout   => 300000
              },
    ok = meck:expect(cowboy_req, parse_header, fun(_, req) -> undefined end),
    {cowboy_websocket, req, [req, Opts], WsOpts} = ?ws_conn:init(req, Opts),
    ok = meck:expect(cowboy_req, parse_header, fun(_, req) -> [<<"mqtt">>] end),
    ok = meck:expect(cowboy_req, set_resp_header, fun(_, <<"mqtt">>, req) -> resp end),
    {cowboy_websocket, resp, [req, Opts], WsOpts} = ?ws_conn:init(req, Opts).

t_websocket_handle_binary(_) ->
    {ok, _} = websocket_handle({binary, <<>>}, st()),
    {ok, _} = websocket_handle({binary, [<<>>]}, st()),
    {ok, _} = websocket_handle({binary, <<192,0>>}, st()),
    receive {incoming, ?PACKET(?PINGREQ)} -> ok
    after 0 -> error(expect_incoming_pingreq)
    end.

t_websocket_handle_ping(_) ->
    {ok, St} = websocket_handle(ping, St = st()),
    {ok, St} = websocket_handle({ping, <<>>}, St).

t_websocket_handle_pong(_) ->
    {ok, St} = websocket_handle(pong, St = st()),
    {ok, St} = websocket_handle({pong, <<>>}, St).

t_websocket_handle_bad_frame(_) ->
    {[{shutdown, unexpected_ws_frame}], _St} = websocket_handle({badframe, <<>>}, st()).

t_websocket_info_call(_) ->
    From = {make_ref(), self()},
    Call = {call, From, badreq},
    {ok, _St} = websocket_info(Call, st()).

t_websocket_info_rate_limit(_) ->
    {ok, _} = websocket_info({cast, rate_limit}, st()),
    ok = timer:sleep(1),
    receive
        {check_gc, Stats} ->
            ?assertEqual(#{cnt => 0, oct => 0}, Stats)
    after 0 -> error(expect_check_gc)
    end.

t_websocket_info_cast(_) ->
    {ok, _St} = websocket_info({cast, msg}, st()).

t_websocket_info_incoming(_) ->
    ConnPkt = #mqtt_packet_connect{
                 proto_name  = <<"MQTT">>,
                 proto_ver   = ?MQTT_PROTO_V5,
                 is_bridge   = false,
                 clean_start = true,
                 keepalive   = 60,
                 properties  = undefined,
                 clientid    = <<"clientid">>,
                 username    = <<"username">>,
                 password    = <<"passwd">>
                },
    {[{close,protocol_error}], St1} = websocket_info({incoming, ?CONNECT_PACKET(ConnPkt)}, st()),
    % ?assertEqual(<<224,2,130,0>>, iolist_to_binary(IoData1)),
    %% PINGREQ
    {[{binary, IoData2}], St2} =
        websocket_info({incoming, ?PACKET(?PINGREQ)}, St1),
    ?assertEqual(<<208,0>>, iolist_to_binary(IoData2)),
    %% PUBLISH
    Publish = ?PUBLISH_PACKET(?QOS_1, <<"t">>, 1, <<"payload">>),
    {[{binary, IoData3}], _St3} = websocket_info({incoming, Publish}, St2),
    ?assertEqual(<<64,4,0,1,0,0>>, iolist_to_binary(IoData3)).

t_websocket_info_check_gc(_) ->
    Stats = #{cnt => 10, oct => 1000},
    {ok, _St} = websocket_info({check_gc, Stats}, st()).

t_websocket_info_deliver(_) ->
    Msg0 = emqx_message:make(clientid, ?QOS_0, <<"t">>, <<"">>),
    Msg1 = emqx_message:make(clientid, ?QOS_1, <<"t">>, <<"">>),
    self() ! {deliver, <<"#">>, Msg1},
    {ok, _St} = websocket_info({deliver, <<"#">>, Msg0}, st()).
    % ?assertEqual(<<48,3,0,1,116,50,5,0,1,116,0,1>>, iolist_to_binary(IoData)).

t_websocket_info_timeout_limiter(_) ->
    Ref = make_ref(),
    Event = {timeout, Ref, limit_timeout},
    {[{active, true}], St} = websocket_info(Event, st(#{limit_timer => Ref})),
    ?assertEqual([], ?ws_conn:info(postponed, St)).

t_websocket_info_timeout_keepalive(_) ->
    {ok, _St} = websocket_info({timeout, make_ref(), keepalive}, st()).

t_websocket_info_timeout_emit_stats(_) ->
    Ref = make_ref(),
    St = st(#{stats_timer => Ref}),
    {ok, St1} = websocket_info({timeout, Ref, emit_stats}, St),
    ?assertEqual(undefined, ?ws_conn:info(stats_timer, St1)).

t_websocket_info_timeout_retry(_) ->
    {ok, _St} = websocket_info({timeout, make_ref(), retry_delivery}, st()).

t_websocket_info_close(_) ->
    {[{close, _}], _St} = websocket_info({close, sock_error}, st()).

t_websocket_info_shutdown(_) ->
    {[{shutdown, reason}], _St} = websocket_info({shutdown, reason}, st()).

t_websocket_info_stop(_) ->
    {[{shutdown, normal}], _St} = websocket_info({stop, normal}, st()).

t_websocket_close(_) ->
    {[{shutdown, badframe}], _St} = websocket_close(badframe, st()).

t_handle_info_connack(_) ->
    ConnAck = ?CONNACK_PACKET(?RC_SUCCESS),
    {[{binary, IoData}], _St} =
        ?ws_conn:handle_info({connack, ConnAck}, st()),
    ?assertEqual(<<32,2,0,0>>, iolist_to_binary(IoData)).

t_handle_info_close(_) ->
    {[{close, _}], _St} = ?ws_conn:handle_info({close, protocol_error}, st()).

t_handle_info_event(_) ->
    ok = meck:new(emqx_cm, [passthrough, no_history]),
    ok = meck:expect(emqx_cm, register_channel, fun(_,_,_) -> ok end),
    ok = meck:expect(emqx_cm, connection_closed, fun(_) -> true end),
    {ok, _} = ?ws_conn:handle_info({event, connected}, st()),
    {ok, _} = ?ws_conn:handle_info({event, disconnected}, st()),
    {ok, _} = ?ws_conn:handle_info({event, updated}, st()),
    ok = meck:unload(emqx_cm).

t_handle_timeout_idle_timeout(_) ->
    TRef = make_ref(),
    St = st(#{idle_timer => TRef}),
    {[{shutdown, idle_timeout}], _St} = ?ws_conn:handle_timeout(TRef, idle_timeout, St).

t_handle_timeout_keepalive(_) ->
    {ok, _St} = ?ws_conn:handle_timeout(make_ref(), keepalive, st()).

t_handle_timeout_emit_stats(_) ->
    TRef = make_ref(),
    {ok, St} = ?ws_conn:handle_timeout(
                  TRef, emit_stats, st(#{stats_timer => TRef})),
    ?assertEqual(undefined, ?ws_conn:info(stats_timer, St)).

t_ensure_rate_limit(_) ->
    Limiter = emqx_limiter:init([{pub_limit, {1, 10}},
                                 {rate_limit, {100, 1000}}
                                ]),
    St = st(#{limiter => Limiter}),
    St1 = ?ws_conn:ensure_rate_limit(#{cnt => 0, oct => 0}, St),
    St2 = ?ws_conn:ensure_rate_limit(#{cnt => 11, oct => 1200}, St1),
    ?assertEqual(blocked, ?ws_conn:info(sockstate, St2)),
    ?assertEqual([{active, false}], ?ws_conn:info(postponed, St2)).

t_parse_incoming(_) ->
    St = ?ws_conn:parse_incoming(<<48,3>>, st()),
    St1 = ?ws_conn:parse_incoming(<<0,1,116>>, St),
    Packet = ?PUBLISH_PACKET(?QOS_0, <<"t">>, undefined, <<>>),
    [{incoming, Packet}] = ?ws_conn:info(postponed, St1).

t_parse_incoming_frame_error(_) ->
    St = ?ws_conn:parse_incoming(<<3,2,1,0>>, st()),
    FrameError = {frame_error, function_clause},
    [{incoming, FrameError}] = ?ws_conn:info(postponed, St).

t_handle_incomming_frame_error(_) ->
    FrameError = {frame_error, bad_qos},
    Serialize = emqx_frame:serialize_fun(#{version => 5, max_size => 16#FFFF}),
    {[{close, bad_qos}], _St} = ?ws_conn:handle_incoming(FrameError, st(#{serialize => Serialize})).
    % ?assertEqual(<<224,2,129,0>>, iolist_to_binary(IoData)).

t_handle_outgoing(_) ->
    Packets = [?PUBLISH_PACKET(?QOS_1, <<"t1">>, 1, <<"payload">>),
               ?PUBLISH_PACKET(?QOS_2, <<"t2">>, 2, <<"payload">>)
              ],
    {{binary, IoData}, _St} = ?ws_conn:handle_outgoing(Packets, st()),
    ?assert(is_binary(iolist_to_binary(IoData))).

t_run_gc(_) ->
    GcSt = emqx_gc:init(#{count => 10, bytes => 100}),
    WsSt = st(#{gc_state => GcSt}),
    ?ws_conn:run_gc(#{cnt => 100, oct => 10000}, WsSt).

t_check_oom(_) ->
    %%Policy = #{max_heap_size => 10, message_queue_len => 10},
    %%meck:expect(emqx_zone, oom_policy, fun(_) -> Policy end),
    _St = ?ws_conn:check_oom(st()),
    ok = timer:sleep(10).
    %%receive {shutdown, proc_heap_too_large} -> ok
    %%after 0 -> error(expect_shutdown)
    %%end.

t_enqueue(_) ->
    Packet = ?PUBLISH_PACKET(?QOS_0),
    St = ?ws_conn:enqueue(Packet, st()),
    [Packet] = ?ws_conn:info(postponed, St).

t_shutdown(_) ->
    {[{shutdown, closed}], _St} = ?ws_conn:shutdown(closed, st()).

%%--------------------------------------------------------------------
%% Helper functions
%%--------------------------------------------------------------------

st() -> st(#{}).
st(InitFields) when is_map(InitFields) ->
    {ok, St, _} = ?ws_conn:websocket_init([req, [{zone, external}]]),
    maps:fold(fun(N, V, S) -> ?ws_conn:set_field(N, V, S) end,
              ?ws_conn:set_field(channel, channel(), St),
              InitFields
             ).

channel() -> channel(#{}).
channel(InitFields) ->
    ConnInfo = #{peername => {{127,0,0,1}, 3456},
                 sockname => {{127,0,0,1}, 18083},
                 conn_mod => emqx_ws_connection,
                 proto_name => <<"MQTT">>,
                 proto_ver => ?MQTT_PROTO_V5,
                 clean_start => true,
                 keepalive => 30,
                 clientid => <<"clientid">>,
                 username => <<"username">>,
                 receive_maximum => 100,
                 expiry_interval => 0
                },
    ClientInfo = #{zone       => zone,
                   protocol   => mqtt,
                   peerhost   => {127,0,0,1},
                   clientid   => <<"clientid">>,
                   username   => <<"username">>,
                   is_superuser => false,
                   peercert   => undefined,
                   mountpoint => undefined
                  },
    Session = emqx_session:init(#{zone => external},
                                #{receive_maximum => 0}
                               ),
    maps:fold(fun(Field, Value, Channel) ->
                      emqx_channel:set_field(Field, Value, Channel)
              end,
              emqx_channel:init(ConnInfo, [{zone, zone}]),
              maps:merge(#{clientinfo => ClientInfo,
                           session    => Session,
                           conn_state => connected
                          }, InitFields)).

