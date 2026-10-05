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

all() ->
    [
        {group, gen_tcp},
        {group, socket},
        {group, ws}
    ] ++ quic_groups().

groups() ->
    All = emqx_common_test_helpers:all(?MODULE),
    {QuicTCs, TCs} = lists:partition(fun is_quic_case/1, All),
    [
        {gen_tcp, [], TCs},
        {socket, [], TCs},
        {ws, [], TCs},
        {quic, [], TCs ++ QuicTCs}
    ].

-ifndef(BUILD_WITHOUT_QUIC).
quic_groups() -> [{group, quic}].
-else.
quic_groups() -> [].
-endif.

is_quic_case(TC) ->
    lists:prefix("t_quic_", atom_to_list(TC)).

init_per_group(Group, Config) ->
    Apps = emqx_cth_suite:start(
        [{emqx, conf(Group)}],
        #{work_dir => emqx_cth_suite:work_dir(Group, Config)}
    ),
    [{apps, Apps}, {group, Group} | Config].

end_per_group(_Group, Config) ->
    emqx_cth_suite:stop(?config(apps, Config)).

init_per_testcase(t_never_hibernates_when_infinity, Config) ->
    ok = emqx_config:put([zones, default, mqtt, hibernate_after], infinity),
    Config;
init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(t_never_hibernates_when_infinity, _Config) ->
    emqx_config:put([zones, default, mqtt, hibernate_after], ?HIBERNATE_AFTER_MS);
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
        """,
        [?HIBERNATE_AFTER_MS]
    );
conf(TcpBackend) ->
    emqx_utils:format(
        """
        mqtt.hibernate_after = ~pms
        mqtt.idle_timeout = 1s
        listeners.tcp.default.tcp_backend = ~p
        """,
        [?HIBERNATE_AFTER_MS, TcpBackend]
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

connect(ClientId, Config) ->
    Group = ?config(group, Config),
    {ok, C} = emqtt:start_link([{clientid, ClientId} | conn_opts(Group)]),
    {ok, _} =
        case Group of
            ws -> emqtt:ws_connect(C);
            quic -> emqtt:quic_connect(C);
            _ -> emqtt:connect(C)
        end,
    C.

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
