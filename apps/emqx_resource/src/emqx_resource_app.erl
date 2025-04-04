%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_resource_app).

-behaviour(application).

-include("emqx_resource.hrl").

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    %% since the handler is generic and executed in the process
    %% emitting the event, we need to install only a single handler
    %% for the whole app.
    TelemetryHandlerID = telemetry_handler_id(),
    ok = emqx_resource_metrics:install_telemetry_handler(TelemetryHandlerID),
    emqx_resource_sup:start_link().

stop(_State) ->
    TelemetryHandlerID = telemetry_handler_id(),
    ok = emqx_resource_metrics:uninstall_telemetry_handler(TelemetryHandlerID),
    ok.

%% internal functions
telemetry_handler_id() ->
    <<"emqx-resource-app-telemetry-handler">>.
