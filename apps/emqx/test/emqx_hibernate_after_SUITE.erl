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

all() ->
    [
        {group, gen_tcp},
        {group, socket},
        {group, ws}
    ].

groups() ->
    TCs = emqx_common_test_helpers:all(?MODULE),
    [
        {gen_tcp, [], TCs},
        {socket, [], TCs},
        {ws, [], TCs}
    ].

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
end_per_testcase(_TestCase, _Config) ->
    ok.

%% `idle_timeout' is also the period of the stats timer, which wakes a
%% hibernated connection when it fires. Keep it short so
%% `t_hibernates_after_timer_wakeup' does not have to wait for the default.
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

connect(ClientId, Config) ->
    Group = ?config(group, Config),
    {ok, C} = emqtt:start_link([{clientid, ClientId} | conn_opts(Group)]),
    {ok, _} =
        case Group of
            ws -> emqtt:ws_connect(C);
            _ -> emqtt:connect(C)
        end,
    C.

conn_opts(ws) -> [{host, "localhost"}, {port, 8083}];
conn_opts(_TcpBackend) -> [{port, 1883}].

await_hibernated(Pid) ->
    await_hibernated(Pid, 50).

await_hibernated(Pid, 0) ->
    process_info(Pid, current_function);
await_hibernated(Pid, Retries) ->
    case process_info(Pid, current_function) of
        ?HIBERNATED = Info ->
            Info;
        _Other ->
            timer:sleep(?HIBERNATE_AFTER_MS div 3),
            await_hibernated(Pid, Retries - 1)
    end.
