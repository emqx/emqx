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
    emqx_a2a_registry_hookcb:register_hooks(),
    {ok, Sup}.

stop(_State) ->
    emqx_a2a_registry_hookcb:unregister_hooks(),
    ok.
