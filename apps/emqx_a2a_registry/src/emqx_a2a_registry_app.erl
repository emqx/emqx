%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_a2a_registry_app).

-behaviour(application).

-export([
    start/2,
    stop/1
]).

start(_Type, _Args) ->
    {ok, Sup} = emqx_a2a_registry_sup:start_link(),
    emqx_a2a_registry:ensure_card_schema_registered(),
    ok = emqx_conf:add_handler([a2a_registry, enable], emqx_a2a_registry_config),
    ok = emqx_a2a_registry_config:maybe_register_hooks(),
    emqx_a2a_registry_cli:load(),
    {ok, Sup}.

stop(_State) ->
    ok = emqx_conf:remove_handler([a2a_registry, enable]),
    emqx_a2a_registry_cli:unload(),
    emqx_a2a_registry_hookcb:unregister_hooks(),
    ok.
