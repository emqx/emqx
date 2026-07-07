%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_sync_request_app).

-behaviour(application).

-emqx_plugin(?MODULE).

%% Application callbacks
-export([
    start/2,
    stop/1
]).

%% EMQX Plugin callbacks
-export([
    on_config_changed/2,
    on_health_check/1
]).

start(_StartType, _StartArgs) ->
    {ok, Sup} = emqx_sync_request_sup:start_link(),
    ok = emqx_sync_request:install_api_dispatch(),
    {ok, Sup}.

stop(_State) ->
    ok = emqx_sync_request:uninstall_api_dispatch().

on_config_changed(_OldConf, NewConf) ->
    emqx_sync_request:on_config_changed(NewConf).

on_health_check(_Options) ->
    emqx_sync_request:on_health_check().
