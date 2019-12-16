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

-module(emqx_connection_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include("emqx_mqtt.hrl").
-include_lib("eunit/include/eunit.hrl").

all() -> emqx_ct:all(?MODULE).

%%--------------------------------------------------------------------
%% CT callbacks
%%--------------------------------------------------------------------

init_per_suite(Config) ->
    %% Meck Transport
    ok = meck:new(emqx_transport, [non_strict, passthrough, no_history, no_link]),
    %% Meck Channel
    ok = meck:new(emqx_channel, [passthrough, no_history, no_link]),
    %% Meck Cm
    ok = meck:new(emqx_cm, [passthrough, no_history, no_link]),
    %% Meck Metrics
    ok = meck:new(emqx_metrics, [passthrough, no_history, no_link]),
    ok = meck:expect(emqx_metrics, inc, fun(_, _) -> ok end),
    ok = meck:expect(emqx_metrics, inc_recv, fun(_) -> ok end),
    ok = meck:expect(emqx_metrics, inc_sent, fun(_) -> ok end),
    Config.

end_per_suite(_Config) ->
    ok = meck:unload(emqx_transport),
    ok = meck:unload(emqx_channel),
    ok = meck:unload(emqx_cm),
    ok = meck:unload(emqx_metrics),
    ok.

init_per_testcase(_TestCase, Config) ->
    ok = meck:expect(emqx_transport, wait, fun(Sock) -> {ok, Sock} end),
    ok = meck:expect(emqx_transport, type, fun(_Sock) -> tcp end),
    ok = meck:expect(emqx_transport, ensure_ok_or_exit,
                     fun(peername, [sock]) -> {ok, {{127,0,0,1}, 3456}};
                        (sockname, [sock]) -> {ok, {{127,0,0,1}, 1883}};
                        (peercert, [sock]) -> undefined
                     end),
    ok = meck:expect(emqx_transport, setopts, fun(_Sock, _Opts) -> ok end),
    ok = meck:expect(emqx_transport, getstat, fun(_Sock, Options) ->
                                                      {ok, [{K, 0} || K <- Options]}
                                              end),
    ok = meck:expect(emqx_transport, async_send, fun(_Sock, _Data) -> ok end),
    ok = meck:expect(emqx_transport, fast_close, fun(_Sock) -> ok end),
    Config.

end_per_testcase(_TestCase, Config) ->
    Config.

%%--------------------------------------------------------------------
%% Test cases
%%--------------------------------------------------------------------

t_start_link_ok(_) ->
    with_conn(fun(CPid) -> state = element(1, sys:get_state(CPid)) end).

t_start_link_exit_on_wait(_) ->
    ok = exit_on_wait_error(enotconn, normal),
    ok = exit_on_wait_error(einval, normal),
    ok = exit_on_wait_error(closed, normal),
    ok = exit_on_wait_error(timeout, {shutdown, ssl_upgrade_timeout}),
    ok = exit_on_wait_error(enetdown, {shutdown, enetdown}).

t_start_link_exit_on_activate(_) ->
    ok = exit_on_activate_error(enotconn, normal),
    ok = exit_on_activate_error(einval, normal),
    ok = exit_on_activate_error(closed, normal),
    ok = exit_on_activate_error(econnreset, {shutdown, econnreset}).

t_get_conn_info(_) ->
    with_conn(fun(CPid) ->
                      #{sockinfo := SockInfo} = emqx_connection:info(CPid),
                      ?assertEqual(#{active_n => 100,
                                     peername => {{127,0,0,1},3456},
                                     sockname => {{127,0,0,1},1883},
                                     sockstate => running,
                                     socktype => tcp
                                    }, SockInfo)
              end).

t_handle_call_discard(_) ->
    with_conn(fun(CPid) ->
                      ok = meck:expect(emqx_channel, handle_call,
                                       fun(discard, Channel) ->
                                               {shutdown, discarded, ok, Channel}
                                       end),
                      ok = emqx_connection:call(CPid, discard),
                      timer:sleep(100),
                      ok = trap_exit(CPid, {shutdown, discarded})
              end, #{trap_exit => true}),
    with_conn(fun(CPid) ->
                      ok = meck:expect(emqx_channel, handle_call,
                                       fun(discard, Channel) ->
                                               {shutdown, discarded, ok, ?DISCONNECT_PACKET(?RC_SESSION_TAKEN_OVER), Channel}
                                       end),
                      ok = emqx_connection:call(CPid, discard),
                      timer:sleep(100),
                      ok = trap_exit(CPid, {shutdown, discarded})
              end, #{trap_exit => true}).

t_handle_call_takeover(_) ->
    with_conn(fun(CPid) ->
                      ok = meck:expect(emqx_channel, handle_call,
                                       fun({takeover, 'begin'}, Channel) ->
                                               {reply, session, Channel};
                                          ({takeover, 'end'}, Channel) ->
                                               {shutdown, takeovered, [], Channel}
                                       end),
                      session = emqx_connection:call(CPid, {takeover, 'begin'}),
                      [] = emqx_connection:call(CPid, {takeover, 'end'}),
                      timer:sleep(100),
                      ok = trap_exit(CPid, {shutdown, takeovered})
              end, #{trap_exit => true}).

t_handle_call_any(_) ->
    with_conn(fun(CPid) ->
                      ok = meck:expect(emqx_channel, handle_call,
                                       fun(_Req, Channel) -> {reply, ok, Channel} end),
                      ok = emqx_connection:call(CPid, req)
              end).

t_handle_incoming_connect(_) ->
    with_conn(fun(CPid) ->
                      ok = meck:expect(emqx_channel, handle_in, fun(_Packet, Channel) -> {ok, Channel} end),
                      ConnPkt = #mqtt_packet_connect{proto_ver   = ?MQTT_PROTO_V5,
                                                     proto_name  = <<"MQTT">>,
                                                     clientid    = <<>>,
                                                     clean_start = true,
                                                     keepalive   = 60
                                                    },
                      Frame = make_frame(?CONNECT_PACKET(ConnPkt)),
                      CPid ! {tcp, sock, Frame}
              end).

t_handle_incoming_publish(_) ->
    with_conn(fun(CPid) ->
                      ok = meck:expect(emqx_channel, handle_in, fun(_Packet, Channel) -> {ok, Channel} end),
                      Frame = make_frame(?PUBLISH_PACKET(?QOS_1, <<"t">>, 1, <<"payload">>)),
                      CPid ! {tcp, sock, Frame}
              end).

t_handle_incoming_subscribe(_) ->
    with_conn(fun(CPid) ->
                      ok = meck:expect(emqx_channel, handle_in, fun(_Packet, Channel) -> {ok, Channel} end),
                      Frame = <<?SUBSCRIBE:4,2:4,11,0,2,0,6,84,111,112,105,99,65,2>>,
                      CPid ! {tcp, sock, Frame}
              end).

t_handle_incoming_unsubscribe(_) ->
    with_conn(fun(CPid) ->
                      ok = meck:expect(emqx_channel, handle_in, fun(_Packet, Channel) -> {ok, Channel} end),
                      Frame = <<?UNSUBSCRIBE:4,2:4,10,0,2,0,6,84,111,112,105,99,65>>,
                      CPid ! {tcp, sock, Frame}
              end).

t_handle_incoming_undefined(_) ->
    with_conn(fun(CPid) ->
                      ok = meck:expect(emqx_channel, handle_in, fun(_Packet, Channel) -> {ok, Channel} end),
                      CPid ! {incoming, undefined}
              end).

t_handle_sock_error(_) ->
    with_conn(fun(CPid) ->
                      ok = meck:expect(emqx_channel, handle_info,
                                       fun({_, Reason}, Channel) ->
                                               {shutdown, Reason, Channel}
                                       end),
                            %% TODO: fixme later
                            CPid ! {tcp_error, sock, econnreset},
                            timer:sleep(100),
                            trap_exit(CPid, {shutdown, econnreset})
              end, #{trap_exit => true}).

t_handle_sock_activate(_) ->
    with_conn(fun(CPid) -> CPid ! activate_socket end).

t_handle_sock_closed(_) ->
    with_conn(fun(CPid) ->
                            ok = meck:expect(emqx_channel, handle_info,
                                             fun({sock_closed, Reason}, Channel) ->
                                                     {shutdown, Reason, Channel}
                                             end),
                            CPid ! {tcp_closed, sock},
                            timer:sleep(100),
                            trap_exit(CPid, {shutdown, tcp_closed})
                    end, #{trap_exit => true}),
    with_conn(fun(CPid) ->
                      ok = meck:expect(emqx_channel, handle_info,
                                       fun({sock_closed, Reason}, Channel) ->
                                               {shutdown, Reason, ?DISCONNECT_PACKET(), Channel}
                                       end),
                      CPid ! {tcp_closed, sock},
                      timer:sleep(100),
                      trap_exit(CPid, {shutdown, tcp_closed})
              end, #{trap_exit => true}).

t_handle_outgoing(_) ->
    with_conn(fun(CPid) ->
                      Publish = ?PUBLISH_PACKET(?QOS_2, <<"Topic">>, 1, <<>>),
                      CPid ! {outgoing, Publish},
                      CPid ! {outgoing, ?PUBREL_PACKET(1)},
                      CPid ! {outgoing, [?PUBCOMP_PACKET(1)]}
              end).

t_conn_rate_limit(_) ->
    with_conn(fun(CPid) ->
                      ok = meck:expect(emqx_channel, handle_in, fun(_, Channel) -> {ok, Channel} end),
                      lists:foreach(fun(I) ->
                                            Publish = ?PUBLISH_PACKET(?QOS_0, <<"Topic">>, I, payload(2000)),
                                            CPid ! {tcp, sock, make_frame(Publish)}
                                    end, [1, 2])
              end, #{active_n => 1, rate_limit => {1, 1024}}).

t_conn_pub_limit(_) ->
    with_conn(fun(CPid) ->
                      ok = meck:expect(emqx_channel, handle_in, fun(_, Channel) -> {ok, Channel} end),
                      ok = lists:foreach(fun(I) ->
                                                 CPid ! {incoming, ?PUBLISH_PACKET(?QOS_0, <<"Topic">>, I, <<>>)}
                                         end, lists:seq(1, 3))
                      %%#{sockinfo := #{sockstate := blocked}} = emqx_connection:info(CPid)
              end, #{active_n => 1, publish_limit => {1, 2}}).

t_conn_pingreq(_) ->
    with_conn(fun(CPid) -> CPid ! {incoming, ?PACKET(?PINGREQ)} end).

t_inet_reply(_) ->
    ok = meck:new(emqx_pd, [passthrough, no_history]),
    with_conn(fun(CPid) ->
                      ok = meck:expect(emqx_pd, get_counter, fun(_) -> 10 end),
                      CPid ! {inet_reply, for_testing, ok},
                      timer:sleep(100)
              end, #{active_n => 1, trap_exit => true}),
    ok = meck:unload(emqx_pd),
    with_conn(fun(CPid) ->
                      CPid ! {inet_reply, for_testing, {error, for_testing}},
                      timer:sleep(100),
                      trap_exit(CPid, {shutdown, for_testing})
              end, #{trap_exit => true}).

t_deliver(_) ->
    with_conn(fun(CPid) ->
                      ok = meck:expect(emqx_channel, handle_deliver,
                                       fun(_, Channel) -> {ok, Channel} end),
                      CPid ! {deliver, topic, msg}
              end).

t_event_disconnected(_) ->
    with_conn(fun(CPid) ->
                      ok = meck:expect(emqx_cm, set_chan_info, fun(_, _) -> ok end),
                      ok = meck:expect(emqx_cm, connection_closed, fun(_) -> ok end),
                      CPid ! {event, disconnected}
              end).

t_event_undefined(_) ->
    with_conn(fun(CPid) ->
                      ok = meck:expect(emqx_channel, stats, fun(_Channel) -> [] end),
                      ok = meck:expect(emqx_cm, set_chan_info, fun(_, _) -> ok end),
                      ok = meck:expect(emqx_cm, set_chan_stats, fun(_, _) -> true end),
                      CPid ! {event, undefined}
              end).

t_cloes(_) ->
    with_conn(fun(CPid) ->
                      CPid ! {close, normal},
                      timer:sleep(100),
                      trap_exit(CPid, {shutdown, normal})
              end, #{trap_exit => true}).

t_oom_shutdown(_) ->
    with_conn(fun(CPid) ->
                      CPid ! {shutdown, message_queue_too_long},
                      timer:sleep(100),
                      trap_exit(CPid, {shutdown, message_queue_too_long})
              end, #{trap_exit => true}).

t_handle_idle_timeout(_) ->
    ok = emqx_zone:set_env(external, idle_timeout, 10),
    with_conn(fun(CPid) ->
                      timer:sleep(100),
                      trap_exit(CPid, {shutdown, idle_timeout})
              end, #{zone => external, trap_exit => true}).

t_handle_emit_stats(_) ->
    ok = emqx_zone:set_env(external, idle_timeout, 1000),
    with_conn(fun(CPid) ->
                      ok = meck:expect(emqx_channel, stats, fun(_Channel) -> [] end),
                      ok = meck:expect(emqx_channel, handle_in, fun(_Packet, Channel) -> {ok, Channel} end),
                      ok = meck:expect(emqx_cm, set_chan_stats, fun(_, _) -> true end),
                      CPid ! {incoming, ?CONNECT_PACKET(#{strict_mode => false,
                                                          max_size    => ?MAX_PACKET_SIZE,
                                                          version     => ?MQTT_PROTO_V4
                                                         })},
                      timer:sleep(1000)
              end,#{zone => external, trap_exit => true}).

t_handle_limit_timeout(_) ->
    with_conn(fun(CPid) ->
                      CPid ! {timeout, undefined, limit_timeout},
                      timer:sleep(100),
                      true = erlang:is_process_alive(CPid)
              end).

t_handle_keepalive_timeout(_) ->
    with_conn(fun(CPid) ->
                      ok = meck:expect(emqx_channel, handle_timeout,
                                       fun(_TRef, _TMsg, Channel) ->
                                               {shutdown, keepalive_timeout, Channel}
                                       end),
                      CPid ! {timeout, make_ref(), keepalive},
                      timer:sleep(100),
                      trap_exit(CPid, {shutdown, keepalive_timeout})
              end, #{trap_exit => true}),
    with_conn(fun(CPid) ->
                      ok = meck:expect(emqx_transport, getstat, fun(_Sock, _Options) -> {error, for_testing} end),
                      ok = meck:expect(emqx_channel, handle_timeout,
                                       fun(_TRef, _TMsg, Channel) ->
                                               {shutdown, keepalive_timeout, Channel}
                                       end),
                      CPid ! {timeout, make_ref(), keepalive},
                      timer:sleep(100),
                      false = erlang:is_process_alive(CPid)
              end, #{trap_exit => true}).

t_handle_shutdown(_) ->
    with_conn(fun(CPid) ->
                      CPid ! Shutdown = {shutdown, reason},
                      timer:sleep(100),
                      trap_exit(CPid, Shutdown)
              end, #{trap_exit => true}).

t_exit_message(_) ->
    with_conn(fun(CPid) ->
                      CPid ! {'EXIT', CPid, for_testing},
                      timer:sleep(1000)
              end, #{trap_exit => true}).

%%--------------------------------------------------------------------
%% Helper functions
%%--------------------------------------------------------------------

exit_on_wait_error(SockErr, Reason) ->
    ok = meck:expect(emqx_transport, wait,
                     fun(_Sock) ->
                             {error, SockErr}
                     end),
    with_conn(fun(CPid) ->
                      timer:sleep(100),
                      trap_exit(CPid, Reason)
              end, #{trap_exit => true}).

exit_on_activate_error(SockErr, Reason) ->
    ok = meck:expect(emqx_transport, setopts,
                     fun(_Sock, _Opts) ->
                             {error, SockErr}
                     end),
    with_conn(fun(CPid) ->
                      timer:sleep(100),
                      trap_exit(CPid, Reason)
              end, #{trap_exit => true}).

with_conn(TestFun) ->
    with_conn(TestFun, #{trap_exit => false}).

with_conn(TestFun, Options) when is_map(Options) ->
    with_conn(TestFun, maps:to_list(Options));

with_conn(TestFun, Options) ->
    TrapExit = proplists:get_value(trap_exit, Options, false),
    process_flag(trap_exit, TrapExit),
    {ok, CPid} = emqx_connection:start_link(emqx_transport, sock, Options),
    TestFun(CPid),
    TrapExit orelse emqx_connection:stop(CPid),
    ok.

trap_exit(Pid, Reason) ->
    receive
        {'EXIT', Pid, Reason} -> ok;
        {'EXIT', Pid, Other}  -> error({unexpect_exit, Other})
    after
        0 -> error({expect_exit, Reason})
    end.

make_frame(Packet) ->
    iolist_to_binary(emqx_frame:serialize(Packet)).

payload(Len) -> iolist_to_binary(lists:duplicate(Len, 1)).

