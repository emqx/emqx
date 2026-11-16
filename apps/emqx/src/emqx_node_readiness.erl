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
""".

-include("logger.hrl").

-export([is_ready/0, mark_ready/0, mark_not_ready/0]).
-export([check_new_connection/0, log_process_limit_refusal/1]).

%% Exported for mocking in tests.
-export([process_usage/0]).

-define(KEY, {?MODULE, ready}).

%% New connections are refused when the process count reaches this
%% percentage of the process limit.
-define(PROCESS_LIMIT_PERCENT, 90).

-type refuse_reason() :: node_not_ready | too_many_processes.

-export_type([refuse_reason/0]).

-doc "Return `true` once this node has finished booting.".
-spec is_ready() -> boolean().
is_ready() ->
    persistent_term:get(?KEY, true).

-doc "Mark the node as fully booted.".
-spec mark_ready() -> ok.
mark_ready() ->
    persistent_term:put(?KEY, true).

-doc "Mark the node as booting. Connection processes refuse to serve.".
-spec mark_not_ready() -> ok.
mark_not_ready() ->
    persistent_term:put(?KEY, false).

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
