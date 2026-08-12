%%--------------------------------------------------------------------
%% Copyright (c) 2021-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_plugins_app).

-behaviour(application).

-include("emqx_plugins.hrl").
-include_lib("snabbkaffe/include/trace.hrl").

-export([
    start/2,
    stop/1
]).

start(_Type, _Args) ->
    %% Load all pre-configured plugins.
    %% Plugin applications are started by `emqx_machine_boot:ensure_apps_started/0'
    %% after all EMQX applications are up.
    {ok, Sup} = emqx_plugins_sup:start_link(),
    ok = emqx_plugins:ensure_installed(),
    emqx_plugins:log_unconfigured_plugins(),
    ok = emqx_config_handler:add_handler([?CONF_ROOT], emqx_plugins),
    ?tp("emqx_plugins_app_started", #{}),
    {ok, Sup}.

stop(_State) ->
    ok = emqx_config_handler:remove_handler([?CONF_ROOT]),
    ok.
