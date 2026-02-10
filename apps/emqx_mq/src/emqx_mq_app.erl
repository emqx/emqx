%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mq_app).
-moduledoc """
Application may start in disabled state, and be enabled later.
When enabled, actual startup is triggered.

Actual startup runs asynchronously in relation to the application startup, and
does not block node boot sequence. Use  `emqx_mq_controller:status/0` and/or
`emqx_mq_controller:wait_status/1` to find out if full application functionality
is available.

This is a temporary solution that is meant to work around a problem related to
the EMQX startup sequence: before cluster discovery begins, EMQX waits for the
full startup of the applications. It cannot occur if the DS DBs are configured
to wait for a certain number of replicas.
""".

-behaviour(application).

-export([start/2, stop/1]).

%% Behaviour callbacks

start(_StartType, _StartArgs) ->
    emqx_conf:add_handler([mq], emqx_mq_config),
    ok = mria:wait_for_tables(emqx_mq_registry:create_tables()),
    %% The rest of the startup is orchestrated by `emqx_mq_controller` running under the main supervisor.
    {ok, Sup} = emqx_mq_sup:start_link(),
    {ok, Sup}.

stop(_State) ->
    ok.
