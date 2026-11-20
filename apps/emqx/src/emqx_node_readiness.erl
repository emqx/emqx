%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_node_readiness).

-moduledoc """
Node readiness flag and admission check for new client connections.

Connection processes (MQTT and gateway) call `check_new_connection/0` at
init.  They refuse to serve in two cases:

- The node has not finished booting, so no client can connect before
  authentication, authorization and plugin hooks are installed.  The
  `GET /status` REST API and cluster join checks read the same flag.
- The VM process count has reached 90% of the process limit.  The
  remaining processes stay available to the system processes (Erlang
  distribution, database replication, supervisors), so that a high rate
  of new connections cannot stop the node.

The readiness flag defaults to `true`: only the managed boot sequence
(`emqx_machine_boot:ensure_apps_started/0`) clears it and sets it back
once all applications (including plugins) are started.  Contexts that
do not boot through `emqx_machine` (test suites) are therefore always
ready.

An application can keep the flag cleared past that point by
registering a check from its `start/2` callback with
`register_check/2`.  `mark_ready/0` starts a process that runs the
registered checks in registration order and sets the flag once they
all return `true`, running them again every second until they do.
Setting the flag drops the checks, and so does `mark_not_ready/0`, so
applications register them again when they restart.  `is_ready/0`
never runs a check.

A check that raises, or returns anything other than a boolean, counts
as not passing and is reported through a throttled
`readiness_check_failed` log.
""".

-include("logger.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-export([is_ready/0, mark_ready/0, mark_not_ready/0]).
-export([register_check/2]).
-export([check_new_connection/0, log_process_limit_refusal/1]).

%% Exported for mocking in tests.
-export([process_usage/0]).

%% Internal exports
-export([start_poller/0, poll/0]).

-define(KEY, {?MODULE, ready}).
-define(CHECKS, {?MODULE, checks}).
%% Registered name of the process that runs the checks until they pass.
-define(POLLER, ?MODULE).
-define(POLL_INTERVAL, 1_000).

-type check_name() :: atom() | binary() | {atom(), term()}.
-type check() :: fun(() -> boolean()).

-export_type([check_name/0, check/0]).

%% New connections are refused when the process count reaches this
%% percentage of the process limit.
-define(PROCESS_LIMIT_PERCENT, 90).

-type refuse_reason() :: node_not_ready | too_many_processes.

-export_type([refuse_reason/0]).

-doc "Return `true` once this node has finished booting.".
-spec is_ready() -> boolean().
is_ready() ->
    persistent_term:get(?KEY, true).

-doc """
Mark the node as fully booted.  Starts a process that runs the
registered checks and sets the flag once they all return `true`,
running them again every second until they do.  Returns without
waiting for the checks, so `is_ready/0` can return `false` right after.
""".
-spec mark_ready() -> ok.
mark_ready() ->
    ensure_poller().

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
registered under it.  The flag is not set while the check returns
anything but `true`.  Registration is a read-modify-write, so calls
must not run concurrently; the boot sequence starts applications one
at a time.
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

%% The name is registered before the first run of the checks, so
%% `mark_not_ready/0' can find and kill the poller before clearing the
%% flag.
ensure_poller() ->
    case whereis(?POLLER) of
        undefined ->
            Pid = proc_lib:spawn(fun ?MODULE:start_poller/0),
            true = register(?POLLER, Pid),
            Pid ! start,
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

%% Waits for the name to be registered: a first run that passes would
%% otherwise exit before `register/2'.
start_poller() ->
    receive
        start -> ?MODULE:poll()
    end.

poll() ->
    case run_checks(checks()) of
        ok ->
            ok = set_ready(),
            ?tp(debug, readiness_checks_passed, #{});
        {pending, Name} ->
            ?tp(debug, readiness_check_pending, #{check => Name}),
            timer:sleep(?POLL_INTERVAL),
            ?MODULE:poll()
    end.

run_checks([]) ->
    ok;
run_checks([{Name, _} = Check | Rest]) ->
    case run_check(Check) of
        true -> run_checks(Rest);
        false -> {pending, Name}
    end.

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

-doc """
Check whether a new client connection can start.

Return `{error, node_not_ready}` while the node boots, and
`{error, too_many_processes}` when the process count is at or above 90%
of the process limit.  Return `ok` otherwise.
""".
-spec check_new_connection() -> ok | {error, refuse_reason()}.
check_new_connection() ->
    case is_ready() of
        true ->
            check_process_usage();
        false ->
            {error, node_not_ready}
    end.

-doc """
Log a connection that was refused for `too_many_processes`.

When the node is close to its process limit, each new connection is
refused, so the log is a throttled warning.  The listener shutdown
counter records every refusal.
""".
-spec log_process_limit_refusal(map()) -> ok.
log_process_limit_refusal(Meta) ->
    {Count, Limit} = ?MODULE:process_usage(),
    _ = ?SLOG_THROTTLE(warning, Meta#{
        msg => connection_refused_due_to_process_limit,
        process_count => Count,
        process_limit => Limit
    }),
    ok.

-doc "Return the current process count and the process limit of the VM.".
-spec process_usage() -> {non_neg_integer(), pos_integer()}.
process_usage() ->
    {erlang:system_info(process_count), erlang:system_info(process_limit)}.

check_process_usage() ->
    {Count, Limit} = ?MODULE:process_usage(),
    case Count * 100 >= Limit * ?PROCESS_LIMIT_PERCENT of
        true ->
            {error, too_many_processes};
        false ->
            ok
    end.
