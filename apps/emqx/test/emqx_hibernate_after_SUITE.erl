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
-define(CHECK_INTERVAL_MS, 4000).
-define(HIBERNATED, {current_function, {erlang, hibernate, 3}}).

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [{emqx, conf()}],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    emqx_cth_suite:stop(?config(apps, Config)).

init_per_testcase(t_never_hibernates_when_infinity, Config) ->
    ok = emqx_config:put([zones, default, mqtt, idle_timeout], infinity),
    Config;
init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(t_never_hibernates_when_infinity, _Config) ->
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

-doc """
The deadline is half of `idle_timeout`, bounded to [3 s, 15 s]. The lower bound
keeps a busy connection from hibernating between two messages; the upper bound
keeps a connection hibernating promptly however high `idle_timeout` is set.
""".
t_hibernate_timeout_bounds(_Config) ->
    %% The default: half of 15 s, inside both bounds.
    ?assertEqual(7500, emqx_connection:hibernate_timeout(15000)),
    %% Half of 6 s is exactly the lower bound.
    ?assertEqual(3000, emqx_connection:hibernate_timeout(6000)),
    %% Below that the lower bound applies, so the deadline is no longer under
    %% `idle_timeout` and such a connection does not hibernate. That is how it
    %% behaved before this change.
    ?assertEqual(3000, emqx_connection:hibernate_timeout(1000)),
    ?assertEqual(3000, emqx_connection:hibernate_timeout(1)),
    %% A high `idle_timeout` is capped rather than tracked.
    ?assertEqual(15000, emqx_connection:hibernate_timeout(60000)),
    ?assertEqual(15000, emqx_connection:hibernate_timeout(7200000)),
    %% `infinity` is preserved, so hibernation stays disabled when asked for.
    ?assertEqual(infinity, emqx_connection:hibernate_timeout(infinity)).

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

connect(ClientId) ->
    {ok, C} = emqtt:start_link([{clientid, ClientId}, {port, 1883}]),
    {ok, _} = emqtt:connect(C),
    C.

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
