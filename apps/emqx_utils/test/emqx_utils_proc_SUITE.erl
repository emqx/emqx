%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_utils_proc_SUITE).

-compile([nowarn_export_all, export_all]).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

suite() -> [{timetrap, {seconds, 60}}].

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_testcase(TCName, TCConfig) ->
    Counter = list_to_atom("emqx_utils_proc_counter_" ++ atom_to_list(TCName)),
    _ = ets:new(Counter, [named_table, public, set]),
    ets:insert(Counter, {runs, 0}),
    [{counter, Counter}, {lock, lock_name(TCName)} | TCConfig].

end_per_testcase(_TCName, TCConfig) ->
    catch ets:delete(?config(counter, TCConfig)),
    ok.

lock_name(TCName) ->
    list_to_atom("emqx_utils_proc_lock_" ++ atom_to_list(TCName)).

%%------------------------------------------------------------------------------
%% Functions run under the lock. External so they survive being carried into
%% another process.
%%------------------------------------------------------------------------------

count(Counter, SleepMs) ->
    timer:sleep(SleepMs),
    ets:update_counter(Counter, runs, 1),
    ok.

runs(Counter) ->
    [{runs, N}] = ets:lookup(Counter, runs),
    N.

boom() ->
    error(deliberate).

sleep_then_boom(SleepMs) ->
    timer:sleep(SleepMs),
    error(deliberate).

forever() ->
    receive
        never -> ok
    end.

%%------------------------------------------------------------------------------
%% Test cases
%%------------------------------------------------------------------------------

-doc "The function runs, and the lock is released afterwards.".
t_runs_and_releases(TCConfig) ->
    Lock = ?config(lock, TCConfig),
    Counter = ?config(counter, TCConfig),
    ?assertEqual(
        ok, emqx_utils_proc:singleton(Lock, fun ?MODULE:count/2, [Counter, 0], #{timeout => 5_000})
    ),
    ?assertEqual(1, runs(Counter)),
    ?assertEqual(undefined, whereis(Lock)).

-doc """
Concurrent callers run the function once between them. The ones that lose the
race wait for the winner and return without running it themselves.
""".
t_runs_once_under_concurrency(TCConfig) ->
    Lock = ?config(lock, TCConfig),
    Counter = ?config(counter, TCConfig),
    Parent = self(),
    Callers = [
        spawn_link(fun() ->
            Parent !
                {
                    self(),
                    emqx_utils_proc:singleton(Lock, fun ?MODULE:count/2, [Counter, 50], #{
                        timeout => 30_000
                    })
                }
        end)
     || _ <- lists:seq(1, 16)
    ],
    lists:foreach(
        fun(Caller) ->
            receive
                {Caller, Result} -> ?assertEqual(ok, Result)
            after 30_000 -> ct:fail("caller ~p timed out", [Caller])
            end
        end,
        Callers
    ),
    ?assertEqual(1, runs(Counter)),
    ?assertEqual(undefined, whereis(Lock)).

-doc "Sequential calls each run the function: this serializes, it does not remember.".
t_sequential_calls_each_run(TCConfig) ->
    Lock = ?config(lock, TCConfig),
    Counter = ?config(counter, TCConfig),
    ?assertEqual(
        ok, emqx_utils_proc:singleton(Lock, fun ?MODULE:count/2, [Counter, 0], #{timeout => 5_000})
    ),
    ?assertEqual(
        ok, emqx_utils_proc:singleton(Lock, fun ?MODULE:count/2, [Counter, 0], #{timeout => 5_000})
    ),
    ?assertEqual(2, runs(Counter)).

-doc "A function that raises is reported to the caller rather than silently swallowed.".
t_crash_is_reported(TCConfig) ->
    Lock = ?config(lock, TCConfig),
    ?assertMatch(
        {error, {crashed, _}},
        emqx_utils_proc:singleton(Lock, fun ?MODULE:boom/0, [], #{timeout => 5_000})
    ),
    ?assertEqual(undefined, whereis(Lock)).

-doc """
A caller gives up when the function outlasts its timeout, and the worker is
killed so the lock does not stay held for the life of the node.
""".
t_timeout_kills_the_worker(TCConfig) ->
    Lock = ?config(lock, TCConfig),
    ?assertEqual(
        {error, timeout},
        emqx_utils_proc:singleton(Lock, fun ?MODULE:forever/0, [], #{timeout => 200})
    ),
    %% The name is free again, so the next caller can make progress.
    ok = wait_until_free(Lock, 50),
    Counter = ?config(counter, TCConfig),
    ?assertEqual(
        ok, emqx_utils_proc:singleton(Lock, fun ?MODULE:count/2, [Counter, 0], #{timeout => 5_000})
    ),
    ?assertEqual(1, runs(Counter)).

-doc """
A caller waiting on a holder whose function crashes is told so, and does not
quietly run the function itself instead.
""".
t_holder_crash_reaches_waiters(TCConfig) ->
    Lock = ?config(lock, TCConfig),
    Counter = ?config(counter, TCConfig),
    Parent = self(),
    Holder = spawn_link(fun() ->
        Parent !
            {
                self(),
                emqx_utils_proc:singleton(Lock, fun ?MODULE:sleep_then_boom/1, [300], #{
                    timeout => 30_000
                })
            }
    end),
    ok = wait_until_held(Lock, 50),
    %% Queued behind the holder.
    ?assertMatch(
        {error, {holder_crashed, _}},
        emqx_utils_proc:singleton(Lock, fun ?MODULE:count/2, [Counter, 0], #{timeout => 30_000})
    ),
    %% The waiter reported the failure rather than running the function.
    ?assertEqual(0, runs(Counter)),
    receive
        {Holder, HolderResult} -> ?assertMatch({error, {crashed, _}}, HolderResult)
    after 30_000 -> ct:fail("holder did not finish")
    end.

-doc """
A waiter stops when its caller dies. Nothing else would end it: it is not the
holder, so no later caller kills it, and the timeout that would have belonged
to its caller died with the caller.
""".
t_waiter_stops_when_caller_dies(TCConfig) ->
    Lock = ?config(lock, TCConfig),
    Parent = self(),
    %% Holds the name and never finishes, so the caller below waits.
    Wedged = spawn(fun() ->
        true = register(Lock, self()),
        Parent ! {self(), holding},
        receive
            never -> ok
        end
    end),
    receive
        {Wedged, holding} -> ok
    after 5_000 -> ct:fail("wedged holder did not start")
    end,
    Caller = spawn(fun() ->
        _ = emqx_utils_proc:singleton(Lock, fun ?MODULE:forever/0, [], #{timeout => 60_000})
    end),
    ok = wait_until(fun() -> waiters() =/= [] end, 50),
    exit(Caller, kill),
    %% The waiter it spawned goes too, rather than sitting in its receive for
    %% the life of the node.
    ok = wait_until(fun() -> waiters() =:= [] end, 50),
    exit(Wedged, kill).

-doc """
The retry options are accepted and leave the guarantees intact: with them set
explicitly, concurrent callers still run the function exactly once between
them.

The `retries_exhausted' result they bound is not covered. Reaching it needs the
lock to be taken when a caller tries to register and gone by the time it looks
up the holder, a window too narrow to open deliberately from outside.
""".
t_accepts_retry_options(TCConfig) ->
    Lock = ?config(lock, TCConfig),
    Counter = ?config(counter, TCConfig),
    Opts = #{timeout => 30_000, max_attempts => 3, retry_interval => 1},
    Parent = self(),
    Callers = [
        spawn_link(fun() ->
            Parent !
                {self(), emqx_utils_proc:singleton(Lock, fun ?MODULE:count/2, [Counter, 50], Opts)}
        end)
     || _ <- lists:seq(1, 8)
    ],
    lists:foreach(
        fun(Caller) ->
            receive
                {Caller, Result} -> ?assertEqual(ok, Result)
            after 30_000 -> ct:fail("caller ~p timed out", [Caller])
            end
        end,
        Callers
    ),
    ?assertEqual(1, runs(Counter)).

%% Processes parked in the wait: identified by where they are executing, since
%% `singleton/4' does not hand back the process it spawned.
waiters() ->
    [
        P
     || P <- erlang:processes(),
        {current_function, {emqx_utils_proc, wait_for, 2}} =:=
            erlang:process_info(P, current_function)
    ].

wait_until(_Fun, 0) ->
    ct:fail("condition never held");
wait_until(Fun, Attempts) ->
    case Fun() of
        true ->
            ok;
        false ->
            timer:sleep(20),
            wait_until(Fun, Attempts - 1)
    end.

wait_until_held(_Lock, 0) ->
    ct:fail("lock was never taken");
wait_until_held(Lock, Attempts) ->
    case whereis(Lock) of
        undefined ->
            timer:sleep(20),
            wait_until_held(Lock, Attempts - 1);
        _Pid ->
            ok
    end.

wait_until_free(_Lock, 0) ->
    ct:fail("lock was never released");
wait_until_free(Lock, Attempts) ->
    case whereis(Lock) of
        undefined ->
            ok;
        _Pid ->
            timer:sleep(20),
            wait_until_free(Lock, Attempts - 1)
    end.
