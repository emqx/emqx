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
    Apps = emqx_cth_suite:start([], #{work_dir => emqx_cth_suite:work_dir(Config)}),
    _ = emqx_config:create_tables(),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    ok = emqx_cth_suite:stop(?config(apps, Config)),
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

-doc """
A collection is requested for every process in a chunk before any reply is
waited for, and the wait has no timeout, so the sweep depends on every request
being answered even when the process is already gone. `garbage_collect/2`
guarantees that: the reply still arrives, carrying `false` because the process
"terminated before the request could be satisfied".

Without this, a chunk that caught a process mid-exit would block the sweep
forever.
""".
t_async_collect_answers_for_a_dead_process(_) ->
    Dead = spawn(fun() -> ok end),
    ?retry(20, 20, ?assertNot(is_process_alive(Dead))),
    Ref = make_ref(),
    ?assertEqual(async, erlang:garbage_collect(Dead, [{async, Ref}])),
    receive
        {garbage_collect, Ref, Result} ->
            %% `false' means the process terminated before the request could be
            %% satisfied, which is exactly this case.
            ?assertEqual(false, Result)
    after 5000 ->
        ct:fail("no reply for a dead process: a sweep would block forever")
    end,
    %% The synchronous form answers the same way, so neither can stall a chunk.
    ?assertEqual(false, erlang:garbage_collect(Dead)),
    %% A live process still answers `true', so the counter stays meaningful.
    Live = spawn(fun() ->
        receive
            stop -> ok
        end
    end),
    LiveRef = make_ref(),
    ?assertEqual(async, erlang:garbage_collect(Live, [{async, LiveRef}])),
    receive
        {garbage_collect, LiveRef, LiveResult} ->
            ?assertEqual(true, LiveResult)
    after 5000 ->
        ct:fail("no reply for a live process")
    end,
    exit(Live, kill).

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
