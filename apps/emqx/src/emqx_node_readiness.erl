%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_node_readiness).

-moduledoc """
Node readiness flag.

Connection processes (MQTT and gateway) check this flag at init and
refuse to serve until the node has finished booting, so no client can
connect before authentication, authorization and plugin hooks are
installed.  The `GET /status` REST API and cluster join checks read it
too.

The flag defaults to `true`: only the managed boot sequence
(`emqx_machine_boot:ensure_apps_started/0`) clears it and sets it back
once all applications (including plugins) are started.  Contexts that
do not boot through `emqx_machine` (test suites) are therefore always
ready.

An application can keep the flag cleared past that point by
registering a check from its `start/2` callback with
`register_check/2`.  `mark_ready/0` runs the registered checks in
registration order and sets the flag once they all return `true`,
running them again every second until they do.  Setting the flag drops
the checks, and so does `mark_not_ready/0`, so applications register
them again when they restart.  `is_ready/0` never runs a check.

A check that raises, or returns anything other than a boolean, counts
as not passing and is reported through a throttled
`readiness_check_failed` log.
""".

-include("logger.hrl").

-export([is_ready/0, mark_ready/0, mark_not_ready/0]).
-export([register_check/2]).

-define(KEY, {?MODULE, ready}).
-define(CHECKS, {?MODULE, checks}).
%% Registered name of the process that runs the checks until they pass.
-define(POLLER, ?MODULE).
-define(POLL_INTERVAL, 1_000).

-type check_name() :: atom() | binary() | {atom(), term()}.
-type check() :: fun(() -> boolean()).

-export_type([check_name/0, check/0]).

-doc "Return `true` once this node has finished booting.".
-spec is_ready() -> boolean().
is_ready() ->
    persistent_term:get(?KEY, true).

-doc """
Mark the node as fully booted.  The flag is set at once if every
registered check returns `true`, otherwise by a process that runs the
checks every second until they do.
""".
-spec mark_ready() -> ok.
mark_ready() ->
    case checks_pass() of
        true -> set_ready();
        false -> ensure_poller()
    end.

-doc """
Mark the node as booting and drop all registered checks.  Connection
processes refuse to serve.
""".
-spec mark_not_ready() -> ok.
mark_not_ready() ->
    ok = stop_poller(),
    persistent_term:put(?KEY, false),
    persistent_term:put(?CHECKS, []).

-doc """
Register a readiness check under `Name`, replacing any check already
registered under it.  `mark_ready/0` does not set the flag while the
check returns anything but `true`.  Registration is a read-modify-write,
so calls must not run concurrently; the boot sequence starts
applications one at a time.
""".
-spec register_check(check_name(), check()) -> ok.
register_check(Name, Check) when is_function(Check, 0) ->
    persistent_term:put(?CHECKS, lists:keystore(Name, 1, checks(), {Name, Check})).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

checks() ->
    persistent_term:get(?CHECKS, []).

set_ready() ->
    persistent_term:put(?KEY, true),
    persistent_term:put(?CHECKS, []).

%% The poller sleeps before its first run, so it is still alive here.
%% The name lets `mark_not_ready/0' kill it before clearing the flag.
ensure_poller() ->
    case whereis(?POLLER) of
        undefined ->
            true = register(?POLLER, proc_lib:spawn(fun poll/0)),
            ok;
        _Pid ->
            ok
    end.

stop_poller() ->
    case whereis(?POLLER) of
        undefined ->
            ok;
        Pid ->
            MRef = erlang:monitor(process, Pid),
            exit(Pid, kill),
            receive
                {'DOWN', MRef, process, Pid, _} -> ok
            end
    end.

poll() ->
    timer:sleep(?POLL_INTERVAL),
    case checks_pass() of
        true -> set_ready();
        false -> poll()
    end.

checks_pass() ->
    lists:all(fun run_check/1, checks()).

run_check({Name, Check}) ->
    try Check() of
        true ->
            true;
        false ->
            false;
        Other ->
            log_failed(Name, #{reason => unexpected_return, returned => Other}),
            false
    catch
        Class:Reason:Stacktrace ->
            log_failed(Name, #{
                reason => Class, details => Reason, stacktrace => Stacktrace
            }),
            false
    end.

log_failed(Name, Details) ->
    ?SLOG_THROTTLE(
        error,
        Details#{msg => readiness_check_failed, check => Name},
        #{}
    ).
