%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_maptabs_app).

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
    {ok, Sup} = emqx_maptabs_sup:start_link(),
    ok = emqx_rule_engine:register_external_functions(emqx_maptabs_rule_funcs),
    ok = emqx_maptabs_cli:load(),
    {ok, Sup}.

stop(_State) ->
    ok = emqx_maptabs_cli:unload(),
    ok = emqx_rule_engine:unregister_external_functions(emqx_maptabs_rule_funcs),
    ok.

on_config_changed(_OldConf, _NewConf) ->
    ok.

on_health_check(_Options) ->
    emqx_maptabs:health_check().
