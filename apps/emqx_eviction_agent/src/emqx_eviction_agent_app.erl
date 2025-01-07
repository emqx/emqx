%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_eviction_agent_app).

-behaviour(application).

-export([
    start/2,
    stop/1
]).

start(_Type, _Args) ->
    ok = emqx_eviction_agent:hook(),
    {ok, Sup} = emqx_eviction_agent_sup:start_link(),
    ok = emqx_eviction_agent_cli:load(),
    {ok, Sup}.

stop(_State) ->
    ok = emqx_eviction_agent:unhook(),
    ok = emqx_eviction_agent_cli:unload().
