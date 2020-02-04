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
    %% Meck Limiter
    ok = meck:new(emqx_limiter, [passthrough, no_history, no_link]),
    %% Meck Pd
    ok = meck:new(emqx_pd, [passthrough, no_history, no_link]),
    %% Meck Metrics
    ok = meck:new(emqx_metrics, [passthrough, no_history, no_link]),
    ok = meck:expect(emqx_metrics, inc, fun(_) -> ok end),
    ok = meck:expect(emqx_metrics, inc, fun(_, _) -> ok end),
    ok = meck:expect(emqx_metrics, inc_recv, fun(_) -> ok end),
    ok = meck:expect(emqx_metrics, inc_sent, fun(_) -> ok end),
    %% Meck Hooks
    ok = meck:new(emqx_hooks, [passthrough, no_history, no_link]),
    ok = meck:expect(emqx_hooks, run, fun(_Hook, _Args) -> ok end),
    ok = meck:expect(emqx_hooks, run_fold, fun(_Hook, _Args, Acc) -> {ok, Acc} end),

    ok = meck:expect(emqx_channel, ensure_disconnected, fun(_, Channel) -> Channel end),

    Config.

end_per_suite(_Config) ->
    ok = meck:unload(emqx_transport),
    ok = meck:unload(emqx_channel),
    ok = meck:unload(emqx_cm),
    ok = meck:unload(emqx_limiter),
    ok = meck:unload(emqx_pd),
    ok = meck:unload(emqx_metrics),
    ok = meck:unload(emqx_hooks),
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

t_info(_) ->
    CPid = spawn(fun() ->
                    receive 
                        {'$gen_call', From, info} ->
                            gen_server:reply(From, emqx_connection:info(st()))
                    after
                        100 -> error("error")
                    end
                end),
    #{sockinfo := SockInfo} = emqx_connection:info(CPid),
    ?assertMatch(#{active_n := 100,
                    peername := {{127,0,0,1},3456},
                    sockname := {{127,0,0,1},1883},
                    sockstate := idle,
                    socktype := tcp}, SockInfo).

t_info_limiter(_) ->
    St = st(#{limiter => emqx_limiter:init([])}),
    ?assertEqual(undefined, emqx_connection:info(limiter, St)).

t_stats(_) ->
    CPid = spawn(fun() ->
                        receive 
                            {'$gen_call', From, stats} ->
                                gen_server:reply(From, emqx_connection:stats(st()))
                        after
                            100 -> error("error")
                        end
                    end),
    Stats = emqx_connection:stats(CPid),
    ?assertMatch([{recv_oct,0},
                  {recv_cnt,0},
                  {send_oct,0},
                  {send_cnt,0},
                  {send_pend,0}| _] , Stats).

t_process_msg(_) ->
    with_conn(fun(CPid) -> 
                        ok = meck:expect(emqx_channel, handle_in, 
                                        fun(_Packet, Channel) -> 
                                                {ok, Channel} 
                                        end),
                        CPid ! {incoming, ?PACKET(?PINGREQ)},
                        CPid ! {incoming, undefined},
                        CPid ! {tcp_passive, sock},
                        CPid ! {tcp_closed, sock},
                        timer:sleep(100),
                        ok = trap_exit(CPid, {shutdown, tcp_closed})
                end, #{trap_exit => true}).

t_ensure_stats_timer(_) ->
    NStats = emqx_connection:ensure_stats_timer(100, st()),
    Stats_timer = emqx_connection:info(stats_timer, NStats),
    ?assert(is_reference(Stats_timer)),
    ?assertEqual(NStats, emqx_connection:ensure_stats_timer(100, NStats)).

t_cancel_stats_timer(_) ->
    NStats = emqx_connection:cancel_stats_timer(st(#{stats_timer => make_ref()})),
    Stats_timer = emqx_connection:info(stats_timer, NStats),
    ?assertEqual(undefined, Stats_timer),
    ?assertEqual(NStats, emqx_connection:cancel_stats_timer(NStats)).

t_append_msg(_) ->
    ?assertEqual([msg], emqx_connection:append_msg([], [msg])),
    ?assertEqual([msg], emqx_connection:append_msg([], msg)),
    ?assertEqual([msg1,msg], emqx_connection:append_msg([msg1], [msg])),
    ?assertEqual([msg1,msg], emqx_connection:append_msg([msg1], msg)).

t_handle_msg(_) ->
    From = {make_ref(), self()},
    ?assertMatch({ok, _St}, emqx_connection:handle_msg({'$gen_call', From, for_testing}, st())),
    ?assertMatch({stop, {shutdown,discarded}, _St}, emqx_connection:handle_msg({'$gen_call', From, discard}, st())),
    ?assertMatch({stop, {shutdown,discarded}, _St}, emqx_connection:handle_msg({'$gen_call', From, discard}, st())),
    ?assertMatch({ok, [], _St}, emqx_connection:handle_msg({tcp, From, <<"for_testing">>}, st())),
    ?assertMatch({ok, _St}, emqx_connection:handle_msg(for_testing, st())).

t_handle_msg_incoming(_) ->
    ?assertMatch({ok, _Out, _St}, emqx_connection:handle_msg({incoming, ?CONNECT_PACKET(#mqtt_packet_connect{})}, st())),
    ?assertEqual(ok, emqx_connection:handle_msg({incoming, ?PACKET(?PINGREQ)}, st())),
    ok = meck:expect(emqx_channel, handle_in, fun(_Packet, Channel) -> {ok, Channel} end),
    ?assertMatch({ok, _St}, emqx_connection:handle_msg({incoming, ?PUBLISH_PACKET(?QOS_1, <<"t">>, 1, <<"payload">>)}, st())),
    ?assertMatch({ok, _St}, emqx_connection:handle_msg({incoming, <<?SUBSCRIBE:4,2:4,11,0,2,0,6,84,111,112,105,99,65,2>>}, st())),
    ?assertMatch({ok, _St}, emqx_connection:handle_msg({incoming, <<?UNSUBSCRIBE:4,2:4,10,0,2,0,6,84,111,112,105,99,65>>}, st())),
    ?assertMatch({ok, _St}, emqx_connection:handle_msg({incoming, undefined}, st())).

t_handle_msg_outgoing(_) ->
    ?assertEqual(ok, emqx_connection:handle_msg({outgoing, ?PUBLISH_PACKET(?QOS_2, <<"Topic">>, 1, <<>>)}, st())),
    ?assertEqual(ok, emqx_connection:handle_msg({outgoing, ?PUBREL_PACKET(1)}, st())),
    ?assertEqual(ok, emqx_connection:handle_msg({outgoing, ?PUBCOMP_PACKET(1)}, st())).

t_handle_msg_tcp_error(_) ->
    ?assertMatch({stop, {shutdown, econnreset}, _St}, emqx_connection:handle_msg({tcp_error, sock, econnreset}, st())).

t_handle_msg_tcp_closed(_) ->
    ?assertMatch({stop, {shutdown, tcp_closed}, _St}, emqx_connection:handle_msg({tcp_closed, sock}, st())).

t_handle_msg_passive(_) ->
    ?assertMatch({ok, _Event, _St}, emqx_connection:handle_msg({tcp_passive, sock}, st())).
    
t_handle_msg_deliver(_) ->
    ok = meck:expect(emqx_channel, handle_deliver, fun(_, Channel) -> {ok, Channel} end),
    ?assertMatch({ok, _St}, emqx_connection:handle_msg({deliver, topic, msg}, st())).
    
t_handle_msg_inet_reply(_) ->
    ok = meck:expect(emqx_pd, get_counter, fun(_) -> 10 end),
    ?assertMatch({ok, _St}, emqx_connection:handle_msg({inet_reply, for_testing, ok}, st(#{active_n => 0}))),
    ?assertEqual(ok, emqx_connection:handle_msg({inet_reply, for_testing, ok}, st(#{active_n => 100}))),
    ?assertMatch({stop, {shutdown, for_testing}, _St}, emqx_connection:handle_msg({inet_reply, for_testing, {error, for_testing}}, st())).

t_handle_msg_connack(_) ->
    ?assertEqual(ok, emqx_connection:handle_msg({connack, ?CONNACK_PACKET(?CONNACK_ACCEPT)}, st())).

t_handle_msg_close(_) ->
    ?assertMatch({stop, {shutdown, normal}, _St}, emqx_connection:handle_msg({close, normal}, st())).
    
t_handle_msg_event(_) ->
    ok = meck:expect(emqx_cm, register_channel, fun(_, _, _) -> ok end),
    ok = meck:expect(emqx_cm, set_chan_info, fun(_, _) -> ok end),
    ok = meck:expect(emqx_cm, connection_closed, fun(_) -> ok end),
    ?assertEqual(ok, emqx_connection:handle_msg({event, connected}, st())),
    ?assertMatch({ok, _St}, emqx_connection:handle_msg({event, disconnected}, st())),
    ?assertMatch({ok, _St}, emqx_connection:handle_msg({event, undefined}, st())).
    
t_handle_msg_timeout(_) ->
    ?assertMatch({ok, _St}, emqx_connection:handle_msg({timeout, make_ref(), for_testing}, st())).

t_handle_msg_shutdown(_) ->
    ?assertMatch({stop, {shutdown, for_testing}, _St}, emqx_connection:handle_msg({shutdown, for_testing}, st())).

t_handle_call(_) ->
    St = st(),
    ?assertMatch({ok, _St}, emqx_connection:handle_msg({event, undefined}, St)),
    ?assertMatch({reply, _Info, _NSt}, emqx_connection:handle_call(self(), info, St)),
    ?assertMatch({reply, _Stats, _NSt }, emqx_connection:handle_call(self(), stats, St)),
    ?assertEqual({reply, ignored, St}, emqx_connection:handle_call(self(), for_testing, St)),
    ?assertMatch({stop, {shutdown,kicked}, ok, _NSt}, emqx_connection:handle_call(self(), kick, St)).

t_handle_timeout(_) ->
    TRef = make_ref(),
    State = st(#{idle_timer => TRef, limit_timer => TRef, stats_timer => TRef}),
    ?assertMatch({stop, {shutdown,idle_timeout}, _NState}, emqx_connection:handle_timeout(TRef, idle_timeout, State)),
    ?assertMatch({ok, {event,running}, _NState}, emqx_connection:handle_timeout(TRef, limit_timeout, State)),
    ?assertMatch({ok, _NState}, emqx_connection:handle_timeout(TRef, emit_stats, State)),
    ?assertMatch({ok, _NState}, emqx_connection:handle_timeout(TRef, keepalive, State)),

    ok = meck:expect(emqx_transport, getstat, fun(_Sock, _Options) -> {error, for_testing} end),
    ?assertMatch({stop, {shutdown,for_testing}, _NState}, emqx_connection:handle_timeout(TRef, keepalive, State)),
    ?assertMatch({ok, _NState}, emqx_connection:handle_timeout(TRef, undefined, State)).

t_parse_incoming(_) ->
    ?assertMatch({ok, [], _NState}, emqx_connection:parse_incoming(<<>>, st())),
    ?assertMatch({[], _NState}, emqx_connection:parse_incoming(<<"for_testing">>, [], st())).

t_next_incoming_msgs(_) ->
    ?assertEqual({incoming, packet}, emqx_connection:next_incoming_msgs([packet])),
    ?assertEqual([{incoming, packet2}, {incoming, packet1}], emqx_connection:next_incoming_msgs([packet1, packet2])).

t_handle_incoming(_) ->
    ?assertMatch({ok, _Out, _NState}, emqx_connection:handle_incoming(?CONNECT_PACKET(#mqtt_packet_connect{}), st())),
    ?assertMatch({ok, _Out, _NState}, emqx_connection:handle_incoming(frame_error, st())).

t_with_channel(_) ->
    State = st(),
    
    ok = meck:expect(emqx_channel, handle_in, fun(_, _) -> ok end),
    ?assertEqual({ok, State}, emqx_connection:with_channel(handle_in, [for_testing], State)),

    ok = meck:expect(emqx_channel, handle_in, fun(_, _) -> Channel = channel(), {ok, Channel} end),
    ?assertMatch({ok, _NState}, emqx_connection:with_channel(handle_in, [for_testing], State)),

    ok = meck:expect(emqx_channel, handle_in, fun(_, _) -> Channel = channel(), {ok, ?DISCONNECT_PACKET(),Channel} end),
    ?assertMatch({ok, _Out, _NChannel}, emqx_connection:with_channel(handle_in, [for_testing], State)),

    ok = meck:expect(emqx_channel, handle_in, fun(_, _) -> Channel = channel(), {shutdown, [for_testing], Channel} end),
    ?assertMatch({stop, {shutdown,[for_testing]}, _NState}, emqx_connection:with_channel(handle_in, [for_testing], State)),

    ok = meck:expect(emqx_channel, handle_in, fun(_, _) -> Channel = channel(), {shutdown, [for_testing], ?DISCONNECT_PACKET(), Channel} end),
    ?assertMatch({stop, {shutdown,[for_testing]}, _NState}, emqx_connection:with_channel(handle_in, [for_testing], State)).

t_handle_outgoing(_) ->
    ?assertEqual(ok, emqx_connection:handle_outgoing(?PACKET(?PINGRESP), st())),
    ?assertEqual(ok, emqx_connection:handle_outgoing([?PACKET(?PINGRESP)], st())).
    
t_handle_info(_) ->
    ?assertMatch({ok, {event,running}, _NState}, emqx_connection:handle_info(activate_socket, st())),
    ?assertMatch({stop, {shutdown, for_testing}, _NStats}, emqx_connection:handle_info({sock_error, for_testing}, st())),
    ?assertMatch({ok, _NState}, emqx_connection:handle_info(for_testing, st())).

t_ensure_rate_limit(_) ->
    State = emqx_connection:ensure_rate_limit(#{}, st(#{limiter => undefined})),
    ?assertEqual(undefined, emqx_connection:info(limiter, State)),

    ok = meck:expect(emqx_limiter, check, fun(_, _) -> {ok, emqx_limiter:init([])} end),
    State1 = emqx_connection:ensure_rate_limit(#{}, st(#{limiter => #{}})),
    ?assertEqual(undefined, emqx_connection:info(limiter, State1)),

    ok = meck:expect(emqx_limiter, check, fun(_, _) -> {pause, 3000, emqx_limiter:init([])} end),
    State2 = emqx_connection:ensure_rate_limit(#{}, st(#{limiter => #{}})),
    ?assertEqual(undefined, emqx_connection:info(limiter, State2)),
    ?assertEqual(blocked, emqx_connection:info(sockstate, State2)).

t_activate_socket(_) ->
    State = st(),
    {ok, NStats} = emqx_connection:activate_socket(State),
    ?assertEqual(running, emqx_connection:info(sockstate, NStats)),
 
    State1 = st(#{sockstate => blocked}),
    ?assertEqual({ok, State1}, emqx_connection:activate_socket(State1)),

    State2 = st(#{sockstate => closed}),
    ?assertEqual({ok, State2}, emqx_connection:activate_socket(State2)).

t_close_socket(_) ->
    State = emqx_connection:close_socket(st(#{sockstate => closed})),
    ?assertEqual(closed, emqx_connection:info(sockstate, State)),
    State1 = emqx_connection:close_socket(st()),
    ?assertEqual(closed, emqx_connection:info(sockstate, State1)).

t_system_code_change(_) ->
    State = st(),
    ?assertEqual({ok, State}, emqx_connection:system_code_change(State, [], [], [])).

t_next_msgs(_) ->
    ?assertEqual({outgoing, ?CONNECT_PACKET()}, emqx_connection:next_msgs(?CONNECT_PACKET())),
    ?assertEqual({}, emqx_connection:next_msgs({})),
    ?assertEqual([], emqx_connection:next_msgs([])).

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
        100 -> error({expect_exit, Reason})
    end.

make_frame(Packet) ->
    iolist_to_binary(emqx_frame:serialize(Packet)).

payload(Len) -> iolist_to_binary(lists:duplicate(Len, 1)).

st() -> st(#{}).
st(InitFields) when is_map(InitFields) ->
    St = emqx_connection:init_state(emqx_transport, sock, [#{zone => external}]),
    maps:fold(fun(N, V, S) -> emqx_connection:set_field(N, V, S) end,
              emqx_connection:set_field(channel, channel(), St),
              InitFields
             ).

channel() -> channel(#{}).
channel(InitFields) ->
    ConnInfo = #{peername => {{127,0,0,1}, 3456},
                 sockname => {{127,0,0,1}, 18083},
                 conn_mod => emqx_connection,
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
