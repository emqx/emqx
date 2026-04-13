%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_offline_messages_app).

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
    {ok, Sup} = emqx_offline_messages_sup:start_link(),
    {ok, Sup}.

stop(_State) ->
    ok.

on_config_changed(OldConf, NewConf) ->
    emqx_offline_messages:on_config_changed(OldConf, NewConf).

on_health_check(_Options) ->
    emqx_offline_messages:on_health_check().
