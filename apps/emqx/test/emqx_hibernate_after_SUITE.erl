%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_hibernate_after_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-define(HIBERNATE_AFTER_MS, 300).
-define(HIBERNATED, {current_function, {erlang, hibernate, 3}}).
%% A `gen_server' hibernates with `erlang:hibernate/0' from this function.
-define(GEN_SERVER_HIBERNATED, {current_function, {gen_server, loop_hibernate, 4}}).
-define(QUIC_PORT, 24567).
%% With a keepalive of 4 s: `max_idle_millisecond` is 6 s, and the check
%% interval is the configured 1 s.
-define(KA, 4).
-define(MAX_IDLE_MS, 6000).
-define(CHECK_INTERVAL_MS, 1000).
%% The time the test process takes to see a disconnect it has waited for.
-define(MARGIN_MS, 500).
%% The sampling timer and the deadline timer of the keepalive.
-define(IS_KEEPALIVE_TIMER(Name), (Name =:= keepalive orelse Name =:= keepalive_deadline)).

all() ->
    [
        {group, gen_tcp},
        {group, socket},
        {group, ws}
    ] ++ quic_groups().

groups() ->
    All = emqx_common_test_helpers:all(?MODULE),
    {QuicTCs, TCs0} = lists:partition(fun is_quic_case/1, All),
    %% A QUIC connection with streams keeps sampling, see `emqx_channel:hibernate/1`.
    {KeepaliveTCs, TCs} = lists:partition(fun is_keepalive_case/1, TCs0),
    [
        {gen_tcp, [], TCs ++ KeepaliveTCs},
        {socket, [], TCs ++ KeepaliveTCs},
        {ws, [], TCs ++ KeepaliveTCs},
        {quic, [], TCs ++ QuicTCs}
    ].

-ifndef(BUILD_WITHOUT_QUIC).
quic_groups() -> [{group, quic}].
-else.
quic_groups() -> [].
-endif.

is_quic_case(TC) ->
    lists:prefix("t_quic_", atom_to_list(TC)).

is_keepalive_case(TC) ->
    lists:prefix("t_keepalive_", atom_to_list(TC)).

init_per_group(Group, Config) ->
    Apps = emqx_cth_suite:start(
        [{emqx, conf(Group)}],
        #{work_dir => emqx_cth_suite:work_dir(Group, Config)}
    ),
    [{apps, Apps}, {group, Group} | Config].

end_per_group(_Group, Config) ->
    emqx_cth_suite:stop(?config(apps, Config)).

init_per_testcase(TC, Config) when
    TC =:= t_never_hibernates_when_infinity;
    TC =:= t_keepalive_samples_when_never_hibernates
->
    ok = emqx_config:put([zones, default, mqtt, hibernate_after], infinity),
    Config;
init_per_testcase(t_keepalive_samples_when_olp_backoff, Config) ->
    case ?config(group, Config) of
        ws ->
            {skip, no_olp_backoff_hibernation_for_ws};
        _ ->
            ok = meck:new(emqx_olp, [passthrough]),
            ok = meck:expect(emqx_olp, backoff_hibernation, fun(_Zone) -> true end),
            Config
    end;
init_per_testcase(t_keepalive_update_while_hibernated, Config) ->
    case ?config(group, Config) of
        ws -> {skip, no_keepalive_cast_for_ws};
        _ -> Config
    end;
init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(TC, _Config) when
    TC =:= t_never_hibernates_when_infinity;
    TC =:= t_keepalive_samples_when_never_hibernates
->
    emqx_config:put([zones, default, mqtt, hibernate_after], ?HIBERNATE_AFTER_MS);
end_per_testcase(t_keepalive_samples_when_olp_backoff, _Config) ->
    meck:unload(emqx_olp);
end_per_testcase(t_quic_new_conn_follows_zone_change, _Config) ->
    {ok, _} = emqx:update_config([mqtt, hibernate_after], <<"300ms">>),
    ok;
end_per_testcase(_TestCase, _Config) ->
    ok.

%% `idle_timeout' is also the period of the stats timer, which wakes a
%% hibernated connection when it fires. Keep it short so
%% `t_hibernates_after_timer_wakeup' does not have to wait for the default.
%% The QUIC listener sets its deprecated `ssl_options.hibernate_after' far
%% above the zone value. A connection that used it would not hibernate within
%% the waits of this suite.
conf(quic) ->
    emqx_utils:format(
        """
        mqtt.hibernate_after = ~pms
        mqtt.idle_timeout = 1s
        listeners.quic.default {
          enable = true
          bind = "127.0.0.1:~p"
          ssl_options.hibernate_after = 1h
        }
        """,
        [?HIBERNATE_AFTER_MS, ?QUIC_PORT]
    );
conf(ws) ->
    emqx_utils:format(
        """
        mqtt.hibernate_after = ~pms
        mqtt.idle_timeout = 1s
        mqtt.keepalive_check_interval = ~pms
        """,
        [?HIBERNATE_AFTER_MS, ?CHECK_INTERVAL_MS]
    );
conf(TcpBackend) ->
    emqx_utils:format(
        """
        mqtt.hibernate_after = ~pms
        mqtt.idle_timeout = 1s
        mqtt.keepalive_check_interval = ~pms
        listeners.tcp.default.tcp_backend = ~p
        """,
        [?HIBERNATE_AFTER_MS, ?CHECK_INTERVAL_MS, TcpBackend]
    ).

-doc "An idle connection hibernates after `mqtt.hibernate_after`.".
t_hibernates_when_idle(Config) ->
    ClientId = atom_to_binary(?FUNCTION_NAME),
    C = connect(ClientId, Config),
    [Pid] = emqx_cm:lookup_channels(ClientId),
    ?assertEqual(?HIBERNATED, await_hibernated(Pid)),
    ok = emqtt:disconnect(C).

-doc "A connection hibernates again after it has handled a packet.".
t_hibernates_again_after_activity(Config) ->
    ClientId = atom_to_binary(?FUNCTION_NAME),
    C = connect(ClientId, Config),
    [Pid] = emqx_cm:lookup_channels(ClientId),
    ?assertEqual(?HIBERNATED, await_hibernated(Pid)),
    {ok, _, [0]} = emqtt:subscribe(C, <<"t/hibernate">>, 0),
    ?assertEqual(?HIBERNATED, await_hibernated(Pid)),
    ok = emqtt:disconnect(C).

-doc "A connection hibernates again after one of its own timers wakes it up.".
t_hibernates_after_timer_wakeup(Config) ->
    ClientId = atom_to_binary(?FUNCTION_NAME),
    C = connect(ClientId, Config),
    [Pid] = emqx_cm:lookup_channels(ClientId),
    {ok, _, [0]} = emqtt:subscribe(C, <<"t/hibernate">>, 0),
    %% Sleep past the stats timer, which runs for `mqtt.idle_timeout'.
    timer:sleep(1500),
    ?assertEqual(?HIBERNATED, await_hibernated(Pid)),
    ok = emqtt:disconnect(C).

-doc "A connection never hibernates when `mqtt.hibernate_after` is `infinity`.".
t_never_hibernates_when_infinity(Config) ->
    ClientId = atom_to_binary(?FUNCTION_NAME),
    C = connect(ClientId, Config),
    [Pid] = emqx_cm:lookup_channels(ClientId),
    timer:sleep(10 * ?HIBERNATE_AFTER_MS),
    ?assertNotEqual(?HIBERNATED, process_info(Pid, current_function)),
    ok = emqtt:disconnect(C).

-doc """
A QUIC data stream hibernates after `mqtt.hibernate_after` of the zone, not
after the listener's `ssl_options.hibernate_after`.
""".
t_quic_data_stream_hibernates_when_idle(Config) ->
    ClientId = atom_to_binary(?FUNCTION_NAME),
    C = connect(ClientId, Config),
    DataStream = open_data_stream(C),
    ?assertEqual(?GEN_SERVER_HIBERNATED, await_hibernated(DataStream, ?GEN_SERVER_HIBERNATED)),
    ok = emqtt:disconnect(C).

-doc """
The QUIC listener config sets the deprecated `ssl_options.hibernate_after`.
The node starts with it, the checked config drops it, and the control stream
hibernates after the zone value.
""".
t_quic_listener_hibernate_after_ignored(Config) ->
    ?assertNot(
        maps:is_key(hibernate_after, emqx_config:get_listener_conf(quic, default, [ssl_options]))
    ),
    ClientId = atom_to_binary(?FUNCTION_NAME),
    C = connect(ClientId, Config),
    [Pid] = emqx_cm:lookup_channels(ClientId),
    ?assertEqual(?HIBERNATED, await_hibernated(Pid)),
    ok = emqtt:disconnect(C).

-doc """
A change of `mqtt.hibernate_after` applies to the control stream and the data
streams of a QUIC connection that starts after the change.
""".
t_quic_new_conn_follows_zone_change(Config) ->
    {ok, _} = emqx:update_config([mqtt, hibernate_after], <<"infinity">>),
    ClientId = atom_to_binary(?FUNCTION_NAME),
    C = connect(ClientId, Config),
    [Pid] = emqx_cm:lookup_channels(ClientId),
    DataStream = open_data_stream(C),
    timer:sleep(10 * ?HIBERNATE_AFTER_MS),
    ?assertNotEqual(?HIBERNATED, process_info(Pid, current_function)),
    ?assertNotEqual(?GEN_SERVER_HIBERNATED, process_info(DataStream, current_function)),
    ok = emqtt:disconnect(C).

-doc """
An idle connection with a keepalive hibernates, and its keepalive timer fires
once, at the deadline, instead of once per check interval.
""".
t_keepalive_deadline_fires_once(Config) ->
    {_C, Pid, LastPkt} = connect_idle(atom_to_binary(?FUNCTION_NAME), ?KA, Config),
    {DownAt, Fires} = await_keepalive_timeout(Pid),
    ?assertEqual(1, Fires),
    assert_deadline(?MAX_IDLE_MS, LastPkt, DownAt).

-doc """
The deadline is not early. A PINGREQ just before it keeps the connection, and
the keepalive then runs from that PINGREQ.
""".
t_keepalive_deadline_not_early(Config) ->
    {C, Pid, {T0, _}} = connect_idle(atom_to_binary(?FUNCTION_NAME), ?KA, Config),
    sleep_until(T0 + ?MAX_IDLE_MS - ?CHECK_INTERVAL_MS),
    LastPkt = timed(fun() -> pong = emqtt:ping(C) end),
    {DownAt, Fires} = await_keepalive_timeout(Pid),
    ?assertEqual(1, Fires),
    assert_deadline(?MAX_IDLE_MS, LastPkt, DownAt).

-doc """
A delivery wakes the connection but is not a received packet. The deadline
counts from the last received packet, not from the hibernation that follows
the delivery.
""".
t_keepalive_deadline_from_last_packet(Config) ->
    ClientId = atom_to_binary(?FUNCTION_NAME),
    Topic = <<"t/", ClientId/binary>>,
    {_C, Pid, {T0, _} = LastPkt} = connect_idle(ClientId, ?KA, Topic, Config),
    ?assertEqual(?HIBERNATED, await_hibernated(Pid)),
    sleep_until(T0 + ?MAX_IDLE_MS div 2),
    _ = emqx:publish(emqx_message:make(Topic, <<"wakeup">>)),
    {DownAt, Fires} = await_keepalive_timeout(Pid),
    ?assertEqual(1, Fires),
    assert_deadline(?MAX_IDLE_MS, LastPkt, DownAt).

-doc """
Deliveries that wake the connection between two hibernations do not delay
the deadline: the time the connection is awake counts as idle time. The
WebSocket connection hibernates only after a whole `hibernate_after` period
without activity, so the deliveries are two periods apart.
""".
t_keepalive_deadline_frequent_deliveries(Config) ->
    ClientId = atom_to_binary(?FUNCTION_NAME),
    Topic = <<"t/", ClientId/binary>>,
    {_C, Pid, LastPkt} = connect_idle(ClientId, ?KA, Topic, Config),
    Publisher = spawn_link(fun() -> publish_loop(Topic, 2 * ?HIBERNATE_AFTER_MS + 100) end),
    {DownAt, Fires} = await_keepalive_timeout(Pid),
    unlink(Publisher),
    exit(Publisher, kill),
    ?assertEqual(1, Fires),
    assert_deadline(?MAX_IDLE_MS, LastPkt, DownAt).

-doc """
A keepalive update while the connection is hibernated replaces the pending
deadline. The connection times out at the new `max_idle_millisecond`.
""".
t_keepalive_update_while_hibernated(Config) ->
    {_C, Pid, LastPkt} = connect_idle(atom_to_binary(?FUNCTION_NAME), ?KA, Config),
    ?assertEqual(?HIBERNATED, await_hibernated(Pid)),
    ok = (conn_mod(Config)):cast(Pid, {keepalive, ?KA div 2}),
    {DownAt, Fires} = await_keepalive_timeout(Pid),
    ?assertEqual(1, Fires),
    assert_deadline(?MAX_IDLE_MS div 2, LastPkt, DownAt).

-doc "A connection that never hibernates samples the receive counter as before.".
t_keepalive_samples_when_never_hibernates(Config) ->
    {_C, Pid, LastPkt} = connect_idle(atom_to_binary(?FUNCTION_NAME), ?KA, Config),
    {DownAt, Fires} = await_keepalive_timeout(Pid),
    ?assert(Fires >= ?MAX_IDLE_MS div ?CHECK_INTERVAL_MS - 1, Fires),
    assert_deadline(?MAX_IDLE_MS, LastPkt, DownAt).

-doc """
A connection that stays awake because of the overload protection
`backoff_hibernation` samples the receive counter as before.
""".
t_keepalive_samples_when_olp_backoff(Config) ->
    {_C, Pid, LastPkt} = connect_idle(atom_to_binary(?FUNCTION_NAME), ?KA, Config),
    {DownAt, Fires} = await_keepalive_timeout(Pid),
    ?assert(meck:num_calls(emqx_olp, backoff_hibernation, '_') > 0),
    ?assert(Fires >= ?MAX_IDLE_MS div ?CHECK_INTERVAL_MS - 1, Fires),
    assert_deadline(?MAX_IDLE_MS, LastPkt, DownAt).

-doc "A connection with keepalive 0 hibernates without a keepalive timer.".
t_keepalive_zero_no_timer(Config) ->
    {_C, Pid, {T0, _}} = connect_idle(atom_to_binary(?FUNCTION_NAME), 0, Config),
    ?assertEqual(?HIBERNATED, await_hibernated(Pid)),
    ?assertEqual({alive, 0}, await_down(Pid, T0 + ?MAX_IDLE_MS + ?CHECK_INTERVAL_MS)),
    ?assertEqual(?HIBERNATED, process_info(Pid, current_function)).

%% Connect with a keepalive, stop the client keepalive, and trace the messages
%% that the server process receives. Return the time interval in which the
%% server received the last packet from the client.
connect_idle(ClientId, KA, Config) ->
    connect_idle(ClientId, KA, undefined, Config).

connect_idle(ClientId, KA, Topic, Config) ->
    {ok, C} = emqtt:start_link([
        {clientid, ClientId}, {keepalive, KA} | conn_opts(?config(group, Config))
    ]),
    unlink(C),
    LastPkt0 = timed(fun() -> {ok, _} = do_connect(C, Config) end),
    ok = emqtt:pause(C),
    LastPkt =
        case Topic of
            undefined -> LastPkt0;
            _ -> timed(fun() -> {ok, _, [0]} = emqtt:subscribe(C, Topic, 0) end)
        end,
    [Pid] = emqx_cm:lookup_channels(ClientId),
    1 = erlang:trace(Pid, true, ['receive']),
    {C, Pid, LastPkt}.

timed(Fun) ->
    T0 = now_ms(),
    _ = Fun(),
    {T0, now_ms()}.

conn_mod(Config) ->
    case ?config(group, Config) of
        gen_tcp -> emqx_connection;
        socket -> emqx_socket_connection
    end.

await_keepalive_timeout(Pid) ->
    case await_down(Pid, now_ms() + 3 * ?MAX_IDLE_MS) of
        %% The WebSocket connection gives the reason as a binary.
        {DownAt, {shutdown, Reason}, Fires} when
            Reason =:= keepalive_timeout; Reason =:= <<"keepalive_timeout">>
        ->
            {DownAt, Fires};
        Other ->
            error({unexpected, Other})
    end.

%% Wait until the process stops, at most until `Until'. Count the keepalive
%% timer messages it received since the trace started.
await_down(Pid, Until) ->
    MRef = erlang:monitor(process, Pid),
    await_down(Pid, MRef, Until, 0).

await_down(Pid, MRef, Until, Fires) ->
    receive
        {trace, Pid, 'receive', {timeout, _TRef, Name}} when ?IS_KEEPALIVE_TIMER(Name) ->
            await_down(Pid, MRef, Until, Fires + 1);
        {'DOWN', MRef, process, Pid, Reason} ->
            DownAt = now_ms(),
            {DownAt, Reason, Fires + count_delivered_fires(Pid)}
    after max(Until - now_ms(), 0) ->
        erlang:demonitor(MRef, [flush]),
        Delivered = count_delivered_fires(Pid),
        _ = erlang:trace(Pid, false, ['receive']),
        {alive, Fires + Delivered}
    end.

count_delivered_fires(Pid) ->
    Ref = erlang:trace_delivered(Pid),
    receive
        {trace_delivered, Pid, Ref} -> count_fires(Pid, 0)
    end.

count_fires(Pid, N) ->
    receive
        {trace, Pid, 'receive', {timeout, _TRef, Name}} when ?IS_KEEPALIVE_TIMER(Name) ->
            count_fires(Pid, N + 1)
    after 0 -> N
    end.

%% The server received the last packet in `{From, To}'. It must disconnect
%% `MaxIdle' after that packet, and at most one check interval late.
assert_deadline(MaxIdle, {From, To}, DownAt) ->
    Info = #{since_last_packet => {DownAt - To, DownAt - From}, max_idle => MaxIdle},
    ?assert(DownAt - From >= MaxIdle, Info),
    ?assert(DownAt - To =< MaxIdle + ?CHECK_INTERVAL_MS + ?MARGIN_MS, Info).

publish_loop(Topic, Interval) ->
    _ = emqx:publish(emqx_message:make(Topic, <<"wakeup">>)),
    timer:sleep(Interval),
    publish_loop(Topic, Interval).

sleep_until(T) ->
    timer:sleep(max(T - now_ms(), 0)).

now_ms() ->
    erlang:monotonic_time(millisecond).

connect(ClientId, Config) ->
    Group = ?config(group, Config),
    {ok, C} = emqtt:start_link([{clientid, ClientId} | conn_opts(Group)]),
    {ok, _} = do_connect(C, Config),
    C.

do_connect(C, Config) ->
    case ?config(group, Config) of
        ws -> emqtt:ws_connect(C);
        quic -> emqtt:quic_connect(C);
        _ -> emqtt:connect(C)
    end.

conn_opts(ws) -> [{host, "localhost"}, {port, 8083}];
conn_opts(quic) -> [{host, "127.0.0.1"}, {port, ?QUIC_PORT}];
conn_opts(_TcpBackend) -> [{port, 1883}].

%% Open a data stream with a subscription on it, and return the server process
%% that owns the stream.
open_data_stream(C) ->
    Before = data_stream_pids(),
    {ok, _, [0]} = emqtt:subscribe_via(C, {new_data_stream, []}, #{}, [{<<"t/ds">>, [{qos, 0}]}]),
    [Pid] = data_stream_pids() -- Before,
    Pid.

data_stream_pids() ->
    [P || P <- erlang:processes(), is_data_stream(P)].

is_data_stream(Pid) ->
    case proc_lib:initial_call(Pid) of
        {quicer_stream, init, _} ->
            #{callback := Callback} = sys:get_state(Pid),
            Callback =:= emqx_quic_data_stream;
        _ ->
            false
    end.

await_hibernated(Pid) ->
    await_hibernated(Pid, ?HIBERNATED).

await_hibernated(Pid, Hibernated) ->
    await_hibernated(Pid, Hibernated, 50).

await_hibernated(Pid, _Hibernated, 0) ->
    process_info(Pid, current_function);
await_hibernated(Pid, Hibernated, Retries) ->
    case process_info(Pid, current_function) of
        Hibernated ->
            Hibernated;
        _Other ->
            timer:sleep(?HIBERNATE_AFTER_MS div 3),
            await_hibernated(Pid, Hibernated, Retries - 1)
    end.
