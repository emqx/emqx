%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_node_readiness).

-moduledoc """
Node readiness flag.

MQTT connection processes check this flag at init and refuse the
connection while the node is not ready.

The flag defaults to `true`. Only the managed boot sequence
(`emqx_machine_boot:ensure_apps_started/0`) clears it, then sets it
back once all applications and plugins are started. Contexts that do
not boot through `emqx_machine` (for example test suites) never clear
the flag, so they are not affected.
""".

-export([is_ready/0, mark_ready/0, mark_not_ready/0]).

-define(KEY, {?MODULE, ready}).

-doc "Return `true` if the node is ready to serve MQTT connections.".
-spec is_ready() -> boolean().
is_ready() ->
    persistent_term:get(?KEY, true).

-doc "Mark the node ready to serve MQTT connections.".
-spec mark_ready() -> ok.
mark_ready() ->
    persistent_term:put(?KEY, true).

-doc "Mark the node not ready. New MQTT connections are refused.".
-spec mark_not_ready() -> ok.
mark_not_ready() ->
    persistent_term:put(?KEY, false).
