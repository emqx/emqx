%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_backup_sync_app).

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
    {ok, Sup} = emqx_backup_sync_sup:start_link(),
    ok = emqx_backup_sync_cli:load(),
    {ok, Sup}.

stop(_State) ->
    emqx_backup_sync_cli:unload().

on_config_changed(OldConf, NewConf) ->
    emqx_backup_sync:on_config_changed(OldConf, NewConf).

on_health_check(_Options) ->
    emqx_backup_sync:on_health_check().
