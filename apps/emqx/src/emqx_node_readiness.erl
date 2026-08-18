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
""".

-export([is_ready/0, mark_ready/0, mark_not_ready/0]).

-define(KEY, {?MODULE, ready}).

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
