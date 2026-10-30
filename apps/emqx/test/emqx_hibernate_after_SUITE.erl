%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_hibernate_after_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

%% `mqtt.idle_timeout`, from which the hibernation deadline is derived as half,
%% clamped to [3 s, 15 s]. Half of 6 s is the lower bound, so the deadline is
%% 3 s and is below both `idle_timeout` and the keepalive check interval. It
%% has to be below both: each is a timer whose message re-enters `recvloop/2`
%% and restarts the deadline.
-define(IDLE_TIMEOUT_MS, 6000).
-define(HIBERNATE_AFTER_MS, 3000).
-define(HIBERNATED, {current_function, {erlang, hibernate, 3}}).
%% With a keepalive of 8 s: `max_idle_millisecond` is 12 s, and the check
%% interval is the configured 4 s, which `emqx_keepalive:init/3` caps at half
%% the keepalive.
-define(KA, 8).
-define(MAX_IDLE_MS, 12000).
-define(CHECK_INTERVAL_MS, 4000).
%% The time the test process takes to see a disconnect it has waited for.
-define(MARGIN_MS, 500).
%% The sampling timer and the deadline timer of the keepalive.
-define(IS_KEEPALIVE_TIMER(Name), (Name =:= keepalive orelse Name =:= keepalive_deadline)).

%% `emqx_ws_connection` is out of scope: it is a `cowboy_websocket` handler
%% that returns `hibernate` from every callback, so it does not have the
%% deadline that this suite exercises.
all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [{emqx, conf()}],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    emqx_cth_suite:stop(?config(apps, Config)).

init_per_testcase(TC, Config) when
    TC =:= t_never_hibernates_when_infinity;
    TC =:= t_keepalive_samples_when_never_hibernates
->
    ok = emqx_config:put([zones, default, mqtt, idle_timeout], infinity),
    Config;
init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(TC, _Config) when
    TC =:= t_never_hibernates_when_infinity;
    TC =:= t_keepalive_samples_when_never_hibernates
->
    emqx_config:put([zones, default, mqtt, idle_timeout], ?IDLE_TIMEOUT_MS);
end_per_testcase(_TestCase, _Config) ->
    ok.

conf() ->
    emqx_utils:format(
        """
        mqtt.idle_timeout = ~pms
        mqtt.keepalive_check_interval = ~pms
        """,
        [?IDLE_TIMEOUT_MS, ?CHECK_INTERVAL_MS]
    ).

-doc "An idle connection hibernates after the deadline derived from `mqtt.idle_timeout`.".
t_hibernates_when_idle(_Config) ->
    ClientId = atom_to_binary(?FUNCTION_NAME),
    C = connect(ClientId),
    [Pid] = emqx_cm:lookup_channels(ClientId),
    ?assertEqual(?HIBERNATED, await_hibernated(Pid)),
    ok = emqtt:disconnect(C).

-doc "A connection hibernates again after it has handled a packet.".
t_hibernates_again_after_activity(_Config) ->
    ClientId = atom_to_binary(?FUNCTION_NAME),
    C = connect(ClientId),
    [Pid] = emqx_cm:lookup_channels(ClientId),
    ?assertEqual(?HIBERNATED, await_hibernated(Pid)),
    {ok, _, [0]} = emqtt:subscribe(C, <<"t/hibernate">>, 0),
    ?assertEqual(?HIBERNATED, await_hibernated(Pid)),
    ok = emqtt:disconnect(C).

-doc "A connection never hibernates when `mqtt.idle_timeout` is `infinity`.".
t_never_hibernates_when_infinity(_Config) ->
    ClientId = atom_to_binary(?FUNCTION_NAME),
    C = connect(ClientId),
    [Pid] = emqx_cm:lookup_channels(ClientId),
    timer:sleep(3 * ?HIBERNATE_AFTER_MS),
    ?assertNotEqual(?HIBERNATED, process_info(Pid, current_function)),
    ok = emqtt:disconnect(C).

-doc """
An idle connection with a keepalive hibernates, and its keepalive timer fires
once, at the deadline, instead of once per check interval.
""".
t_keepalive_deadline_fires_once(_Config) ->
    {_C, Pid, LastPkt} = connect_idle(atom_to_binary(?FUNCTION_NAME), ?KA),
    {DownAt, Fires} = await_keepalive_timeout(Pid),
    ?assertEqual(1, Fires),
    assert_deadline(?MAX_IDLE_MS, LastPkt, DownAt).

-doc """
The deadline is not early. A PINGREQ just before it keeps the connection, and
the keepalive then runs from that PINGREQ.
""".
t_keepalive_deadline_not_early(_Config) ->
    {C, Pid, {T0, _}} = connect_idle(atom_to_binary(?FUNCTION_NAME), ?KA),
    sleep_until(T0 + ?MAX_IDLE_MS - ?CHECK_INTERVAL_MS),
    LastPkt = timed(fun() -> pong = emqtt:ping(C) end),
    {DownAt, Fires} = await_keepalive_timeout(Pid),
    ?assertEqual(1, Fires),
    assert_deadline(?MAX_IDLE_MS, LastPkt, DownAt).

-doc "A connection that never hibernates samples the receive counter as before.".
t_keepalive_samples_when_never_hibernates(_Config) ->
    {_C, Pid, LastPkt} = connect_idle(atom_to_binary(?FUNCTION_NAME), ?KA),
    {DownAt, Fires} = await_keepalive_timeout(Pid),
    ?assert(Fires >= ?MAX_IDLE_MS div ?CHECK_INTERVAL_MS - 1, Fires),
    assert_deadline(?MAX_IDLE_MS, LastPkt, DownAt).

-doc "A connection with keepalive 0 hibernates without a keepalive timer.".
t_keepalive_zero_no_timer(_Config) ->
    {_C, Pid, {T0, _}} = connect_idle(atom_to_binary(?FUNCTION_NAME), 0),
    ?assertEqual(?HIBERNATED, await_hibernated(Pid)),
    ?assertEqual({alive, 0}, await_down(Pid, T0 + ?MAX_IDLE_MS + ?CHECK_INTERVAL_MS)),
    ?assertEqual(?HIBERNATED, process_info(Pid, current_function)).

%% Connect with a keepalive, stop the client keepalive, and trace the messages
%% that the server process receives. Return the time interval in which the
%% server received the last packet from the client.
connect_idle(ClientId, KA) ->
    {ok, C} = emqtt:start_link([{clientid, ClientId}, {keepalive, KA} | conn_opts()]),
    unlink(C),
    LastPkt = timed(fun() -> {ok, _} = emqtt:connect(C) end),
    ok = emqtt:pause(C),
    [Pid] = emqx_cm:lookup_channels(ClientId),
    1 = erlang:trace(Pid, true, ['receive']),
    {C, Pid, LastPkt}.

timed(Fun) ->
    T0 = now_ms(),
    _ = Fun(),
    {T0, now_ms()}.

await_keepalive_timeout(Pid) ->
    case await_down(Pid, now_ms() + 3 * ?MAX_IDLE_MS) of
        {DownAt, {shutdown, keepalive_timeout}, Fires} ->
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

sleep_until(T) ->
    timer:sleep(max(T - now_ms(), 0)).

now_ms() ->
    erlang:monotonic_time(millisecond).

connect(ClientId) ->
    {ok, C} = emqtt:start_link([{clientid, ClientId} | conn_opts()]),
    {ok, _} = emqtt:connect(C),
    C.

conn_opts() -> [{port, 1883}].

await_hibernated(Pid) ->
    await_hibernated(Pid, 50).

await_hibernated(Pid, 0) ->
    process_info(Pid, current_function);
await_hibernated(Pid, Retries) ->
    case process_info(Pid, current_function) of
        ?HIBERNATED ->
            ?HIBERNATED;
        _Other ->
            timer:sleep(?HIBERNATE_AFTER_MS div 3),
            await_hibernated(Pid, Retries - 1)
    end.
