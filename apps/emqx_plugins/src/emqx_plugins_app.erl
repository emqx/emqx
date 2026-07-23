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
    %% load all pre-configured
    {ok, Sup} = emqx_plugins_sup:start_link(),
    ok = emqx_plugins:ensure_installed(),
    ok = emqx_plugins:ensure_started(),
    emqx_plugins:log_unconfigured_plugins(),
    ok = emqx_config_handler:add_handler([?CONF_ROOT], emqx_plugins),
    emqx_plugins_cli_utils:load(),
    ?tp("emqx_plugins_app_started", #{}),
    {ok, Sup}.

stop(_State) ->
    emqx_plugins_cli_utils:unload(),
    ok = emqx_config_handler:remove_handler([?CONF_ROOT]),
    ok.
