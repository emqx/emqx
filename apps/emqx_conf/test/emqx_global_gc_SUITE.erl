%%--------------------------------------------------------------------
%% Copyright (c) 2020-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_global_gc_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    emqx_config:erase_all(),
    ok.

-doc "A configured interval arms the timer, and `run/0` sweeps without changing it.".
t_run_gc(_) ->
    ok = load_interval(<<"1s">>),
    ?assertEqual({ok, 1000}, application:get_env(emqx_machine, global_gc_interval)),
    {ok, Pid} = emqx_global_gc:start_link(),
    try
        #{timer := Timer} = sys:get_state(Pid),
        ?assert(is_reference(Timer)),
        ok = timer:sleep(1500),
        ?assertEqual(ok, emqx_global_gc:run()),
        ?assertMatch(#{worker := {_Pid, _MRef}}, sys:get_state(Pid))
    after
        ok = emqx_global_gc:stop()
    end.

-doc "A `disabled` interval leaves the timer unarmed.".
t_disabled(_) ->
    ok = load_interval(disabled),
    ?assertEqual({ok, disabled}, application:get_env(emqx_machine, global_gc_interval)),
    {ok, Pid} = emqx_global_gc:start_link(),
    try
        ?assertMatch(#{timer := undefined}, sys:get_state(Pid))
    after
        ok = emqx_global_gc:stop()
    end.

-doc """
The pause between chunks scales with the interval and the process count, so a
sweep finishes within half the interval whatever the node size.
""".
t_chunk_pause(_) ->
    %% A million processes over 30 minutes: 10000 chunks, half the interval is
    %% 15 minutes, so 90ms between chunks.
    ?assertEqual(90, emqx_global_gc:chunk_pause(timer:minutes(30), 1000000)),
    ?assertEqual(900, emqx_global_gc:chunk_pause(timer:minutes(30), 100000)),
    %% A small node reaches the one-second ceiling instead of idling for
    %% minutes between chunks, which would buy no further smoothing.
    ?assertEqual(1000, emqx_global_gc:chunk_pause(timer:minutes(30), 10000)),
    ?assertEqual(1000, emqx_global_gc:chunk_pause(timer:minutes(30), 200)),
    %% A node large enough that the interval, not the smoothing, is the
    %% constraint: the pause shrinks so the sweep still finishes.
    ?assertEqual(3, emqx_global_gc:chunk_pause(timer:minutes(10), 10000000)),
    %% Never zero, whatever the arithmetic comes to.
    ?assertEqual(1, emqx_global_gc:chunk_pause(1, 1000000)).

-doc "`run/1` rejects an interval shorter than 10 minutes without reaching the server.".
t_run_rejects_short_interval(_) ->
    ?assertEqual(
        {error, {interval_too_short, timer:minutes(10)}},
        emqx_global_gc:run(timer:minutes(10) - 1)
    ).

-doc "`run/1` replaces the pending timer and keeps its own interval for later runs.".
t_run_with_interval(_) ->
    ok = load_interval(<<"1s">>),
    {ok, Pid} = emqx_global_gc:start_link(),
    try
        #{timer := Timer0, interval := undefined} = sys:get_state(Pid),
        ?assert(is_reference(Timer0)),
        Interval = timer:minutes(10),
        ?assertEqual(ok, emqx_global_gc:run(Interval)),
        #{timer := Timer1, interval := Interval1} = sys:get_state(Pid),
        ?assertEqual(Interval, Interval1),
        ?assert(is_reference(Timer1)),
        ?assertNotEqual(Timer0, Timer1)
    after
        ok = emqx_global_gc:stop()
    end.

-doc "Starting a sweep kills the sweep still in progress, so two never overlap.".
t_no_overlapping_sweeps(_) ->
    ok = load_interval(<<"1h">>),
    {ok, Pid} = emqx_global_gc:start_link(),
    %% Enough waiting processes that one sweep takes several chunks, so the
    %% first worker is still running when the second sweep starts.
    Idle = [spawn(fun idle/0) || _ <- lists:seq(1, 5000)],
    try
        ?assertEqual(ok, emqx_global_gc:run()),
        #{worker := {Worker1, _}} = sys:get_state(Pid),
        ?assert(is_process_alive(Worker1)),
        ?assertEqual(ok, emqx_global_gc:run()),
        #{worker := {Worker2, _}} = sys:get_state(Pid),
        ?assertNotEqual(Worker1, Worker2),
        ?retry(20, 100, ?assertNot(is_process_alive(Worker1)))
    after
        lists:foreach(fun(P) -> exit(P, kill) end, Idle),
        ok = emqx_global_gc:stop()
    end.

-doc """
Loading this module into a running node leaves the server holding the state
the previous version built, which has only `timer`. A code load does not call
`code_change/3`, so every callback has to tolerate that state.

`sys:replace_state/2` puts the server into it, which is what a code load
leaves behind, and then each callback is driven.
""".
t_upgrade_from_previous_state(_) ->
    ok = load_interval(<<"1h">>),
    {ok, Pid} = emqx_global_gc:start_link(),
    try
        %% The state the version before pacing built.
        Previous = fun(#{timer := TRef}) -> #{timer => TRef} end,
        _ = sys:replace_state(Pid, Previous),
        ?assertEqual([timer], maps:keys(sys:get_state(Pid))),
        %% run/0 reads `worker' and `interval', and writes `worker'.
        ?assertEqual(ok, emqx_global_gc:run()),
        ?assertMatch(#{worker := {_, _}}, sys:get_state(Pid)),
        %% run/1 writes `interval' into a state that has no such key.
        _ = sys:replace_state(Pid, Previous),
        ?assertEqual(ok, emqx_global_gc:run(timer:minutes(10))),
        ?assertMatch(#{interval := 600000, worker := {_, _}}, sys:get_state(Pid)),
        %% The periodic tick reads and writes both.
        _ = sys:replace_state(Pid, Previous),
        #{timer := TRef} = sys:get_state(Pid),
        Pid ! {timeout, TRef, run},
        ?retry(20, 100, ?assertMatch(#{worker := {_, _}}, sys:get_state(Pid))),
        %% terminate/2 reads `worker'.
        ?assertEqual(ok, emqx_global_gc:stop()),
        ?assertNot(is_process_alive(Pid))
    after
        catch emqx_global_gc:stop()
    end.

idle() ->
    receive
        stop -> ok
    end.

load_interval(Interval) ->
    Conf = #{
        node => #{
            cookie => <<"cookie">>,
            data_dir => <<"data">>,
            global_gc_interval => Interval
        }
    },
    _ = emqx_common_test_helpers:load_config(emqx_conf_schema, Conf),
    ok.
