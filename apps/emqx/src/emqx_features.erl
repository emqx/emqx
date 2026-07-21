%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_features).

-moduledoc """
Boot-time feature capabilities resolved from `EMQX_FEATURES` (see
`emqx_machine_features`) and pushed down into `persistent_term` so that the
base `emqx` application can cheaply gate optional bookkeeping on the hot path
without an upward dependency on `emqx_machine`.

Accessors default to `true` (capability enabled) when unset, so a node that has
not resolved its feature set yet, or that runs the `FULL` preset, behaves
exactly as before.
""".

-export([
    set_capability/2,
    observability_enabled/0,
    client_info_enabled/0
]).

-define(PT(Key), {?MODULE, Key}).

-doc "Set a boot-time capability flag. Called once by `emqx_machine` at boot.".
-spec set_capability(observability | client_info, boolean()) -> ok.
set_capability(Key, Enabled) when is_boolean(Enabled) ->
    persistent_term:put(?PT(Key), Enabled).

-doc """
Whether observability sinks (Prometheus/metrics, dashboard monitor, telemetry,
OpenTelemetry, `$SYS`) are available. When disabled, purely-observability
counters on the message hot path can be skipped.
""".
-spec observability_enabled() -> boolean().
observability_enabled() ->
    persistent_term:get(?PT(observability), true).

-doc """
Whether the client-info REST API (dashboard/management `GET /clients`,
`GET /subscriptions`) is available. When disabled, per-connection info/stats
book-keeping that only feeds those endpoints can be skipped.
""".
-spec client_info_enabled() -> boolean().
client_info_enabled() ->
    persistent_term:get(?PT(client_info), true).
