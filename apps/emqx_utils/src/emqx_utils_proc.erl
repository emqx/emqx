%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_utils_proc).

-moduledoc """
Process helpers.

`singleton/4' runs a function under a node-local lock, so that concurrent
callers do not all run it: one does, the rest wait for it to finish.
""".

-export([singleton/4]).

%% Internal export: spawned by `singleton/4'.
-export([run/1]).

-export_type([opts/0]).

-type opts() :: #{
    timeout := timeout(),
    max_attempts => pos_integer(),
    retry_interval => timeout()
}.

%% Defaults for `max_attempts' and `retry_interval': how many times a caller
%% retries when it loses the registration race but then finds nobody holding
%% the name, and how long it waits between tries.
-define(MAX_ATTEMPTS, 10).
-define(RETRY_INTERVAL, 5).

-doc """
Runs `apply(Func, Args)' at most once across concurrent callers, under a lock
named `Name' on this node.

One caller runs the function; the others wait for it and return once it has
finished, **without running it themselves**. The function's own return value is
not reported back — callers check whatever state it was meant to establish.
Sequential calls each run the function again: this serializes concurrent work,
it does not remember that it ever ran.

`Opts' must carry a `timeout'. `max_attempts' and `retry_interval' govern the
retries a caller makes when it loses the registration race but then finds
nobody holding the name; both have defaults and are rarely worth setting.

Returns `ok' when the function completed, whoever ran it. Returns
`{error, timeout}' if that did not happen within the `timeout',
`{error, {holder_crashed, Reason}}' if the caller that ran it died, and
`{error, {crashed, Reason}}' if this caller's own worker died unexpectedly.

The lock is a registered name held by a process that exists only to run the
function, so it is released when that process exits — whether the function
returned, raised, or was killed. Nothing has to unregister it.

On timeout the worker is killed, to stop a wedged one from holding the name for
the life of the node. A worker that is *waiting* also stops if its caller dies,
since nothing would kill it then. A worker that is already *running* the
function does not: it is executing rather than waiting, so it cannot notice,
and it keeps the name until the function finishes. **A killed worker does not run cleanup**: an
untrappable exit skips `after' blocks inside `Func', so anything it must not
leak behind has to survive being killed at any point.

Pass an external fun (`fun some_module:some_function/0'). A local fun would pin
the version of the module that created it, which matters because the fun is
carried into another process.
""".
-spec singleton(atom(), function(), list(), opts()) -> ok | {error, term()}.
singleton(Name, Func, Args, #{timeout := Timeout} = Opts) when
    is_atom(Name), is_function(Func), is_list(Args)
->
    Ctx = #{
        name => Name,
        func => Func,
        args => Args,
        parent => self(),
        max_attempts => maps:get(max_attempts, Opts, ?MAX_ATTEMPTS),
        retry_interval => maps:get(retry_interval, Opts, ?RETRY_INTERVAL)
    },
    {Pid, MRef} = erlang:spawn_monitor(?MODULE, run, [Ctx]),
    await(Pid, MRef, Timeout).

-doc """
Acquires the lock and runs the function, or waits for whoever holds it.

Exported only so `singleton/4' can spawn it; not part of this module's
interface.
""".
-spec run(map()) -> ok.
run(#{max_attempts := MaxAttempts} = Ctx) ->
    do_run(Ctx, MaxAttempts).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

await(Pid, MRef, Timeout) ->
    receive
        {'DOWN', MRef, process, _Pid, normal} ->
            ok;
        {'DOWN', MRef, process, _Pid, {?MODULE, Reason}} ->
            {error, Reason};
        {'DOWN', MRef, process, _Pid, Reason} ->
            {error, {crashed, Reason}}
    after Timeout ->
        %% Killed rather than left behind: a worker wedged while holding the
        %% name would keep every later caller waiting too.
        exit(Pid, kill),
        _ = erlang:demonitor(MRef, [flush]),
        {error, timeout}
    end.

do_run(#{name := Name}, 0) ->
    exit({?MODULE, {retries_exhausted, Name}});
do_run(#{name := Name, func := Func, args := Args} = Ctx, Attempts) ->
    case acquire(Name) of
        ok ->
            %% Nothing to release afterwards: the name is held until this
            %% process exits, which is as soon as the function returns. A
            %% function that raises releases it just the same, and the reason
            %% reaches the caller.
            _ = erlang:apply(Func, Args),
            ok;
        busy ->
            wait_for_holder(Ctx, Attempts)
    end.

acquire(Name) ->
    try erlang:register(Name, self()) of
        true -> ok
    catch
        %% Someone else holds it, or took it just now.
        error:badarg -> busy
    end.

wait_for_holder(#{name := Name, parent := Parent, retry_interval := RetryInterval} = Ctx, Attempts) ->
    case erlang:whereis(Name) of
        undefined ->
            %% The holder finished between the failed registration and this
            %% lookup, so nobody is running it; try to take it over.
            timer:sleep(RetryInterval),
            do_run(Ctx, Attempts - 1);
        Pid ->
            wait_for(Pid, Parent)
    end.

%% No timeout on this receive, deliberately: `singleton/4' bounds it from the
%% outside. When its own timeout expires it kills this process, which is the one
%% parked here. A timeout here as well would be a second deadline to keep in
%% step with the caller's, and the two could only disagree.
wait_for(Pid, Parent) ->
    HolderRef = erlang:monitor(process, Pid),
    ParentRef = erlang:monitor(process, Parent),
    receive
        {'DOWN', HolderRef, process, _Pid, Reason} when Reason =:= normal; Reason =:= noproc ->
            %% `noproc' means it had already finished when the monitor was set
            %% up, which is as good as a clean exit.
            ok;
        {'DOWN', HolderRef, process, _Pid, Reason} ->
            exit({?MODULE, {holder_crashed, Reason}});
        {'DOWN', ParentRef, process, _Pid, _Reason} ->
            %% Nobody is waiting for this result any more, and nobody is left
            %% to kill this process on timeout either, so stop waiting.
            ok
    end.
