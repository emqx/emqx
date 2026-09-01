%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_node_readiness).

-moduledoc """
Node readiness gate.

Connection processes (MQTT and gateway) check this gate at init and
refuse to serve until the node is ready, so no client can connect
before authentication, authorization and plugin hooks are installed.
The `GET /status` REST API and cluster join checks read it too.

The gate is closed while the node is booting and re-opened by the
managed boot sequence (`emqx_machine_boot:ensure_apps_started/0`) once
all applications (including plugins) are started.  Contexts that do
not boot through `emqx_machine` (test suites) are therefore always
ready.

"All applications started" is not always the same as "this node can
serve traffic", so applications can narrow the gate further by
registering a predicate with `register_check/2`.  `is_ready/0` returns
`true` only once boot has completed *and* every registered check
returns `true`.

Registered checks are evaluated on every `is_ready/0` call, and that
happens once per incoming connection.  A check must therefore be
cheap: poll in your own process and let the predicate read the cached
answer.  A check that raises, or returns anything other than a
boolean, holds the gate closed and is reported through a throttled
`readiness_check_failed` log.

The registry is kept in a `persistent_term` and is updated with a
read-modify-write, so registration is only safe from a serialized
context — an application's `start/2` and `stop/1` callbacks, or a
single owning process.  Deregister a check before purging the module
that defines its fun.
""".

-include("logger.hrl").

-export([is_ready/0, mark_ready/0, mark_not_ready/0]).
-export([register_check/2, deregister_check/1]).

-define(KEY, {?MODULE, ready}).
-define(CHECKS, {?MODULE, checks}).

-type check_name() :: atom() | binary() | {atom(), term()}.
-type check() :: fun(() -> boolean()).

-export_type([check_name/0, check/0]).

-doc """
Return `true` once this node has finished booting and every registered
readiness check returns `true`.
""".
-spec is_ready() -> boolean().
is_ready() ->
    persistent_term:get(?KEY, true) andalso checks_pass().

-doc "Mark the node as fully booted.".
-spec mark_ready() -> ok.
mark_ready() ->
    persistent_term:put(?KEY, true).

-doc "Mark the node as booting. Connection processes refuse to serve.".
-spec mark_not_ready() -> ok.
mark_not_ready() ->
    persistent_term:put(?KEY, false).

-doc """
Register a readiness check under `Name`, replacing any check already
registered under it.  While the check returns anything but `true` the
node reports itself as not ready.
""".
-spec register_check(check_name(), check()) -> ok.
register_check(Name, Check) when is_function(Check, 0) ->
    persistent_term:put(?CHECKS, maps:put(Name, Check, checks())).

-doc "Remove the readiness check registered under `Name`, if any.".
-spec deregister_check(check_name()) -> ok.
deregister_check(Name) ->
    case checks() of
        #{Name := _} = Checks ->
            persistent_term:put(?CHECKS, maps:remove(Name, Checks));
        _ ->
            ok
    end.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

checks() ->
    persistent_term:get(?CHECKS, #{}).

checks_pass() ->
    case checks() of
        Checks when map_size(Checks) =:= 0 ->
            %% Common case: nothing registered, no iteration.
            true;
        Checks ->
            checks_pass(maps:next(maps:iterator(Checks)))
    end.

checks_pass(none) ->
    true;
checks_pass({Name, Check, Iter}) ->
    run_check(Name, Check) andalso checks_pass(maps:next(Iter)).

run_check(Name, Check) ->
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
