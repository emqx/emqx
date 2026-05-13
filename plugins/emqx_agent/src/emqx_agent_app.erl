%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_agent_app).

-behaviour(application).

-emqx_plugin(?MODULE).

-export([start/2, stop/1]).
-export([on_config_changed/2, on_handle_api_call/4]).

start(_StartType, _StartArgs) ->
    {ok, Sup} = emqx_agent_sup:start_link(),
    ok = emqx_agent_config:init_config(),
    ok = emqx_agent_skill_registry:reconcile(),
    ok = emqx_agent_skill_connections:init(),
    ok = emqx_agent_skill:init(),
    ok = emqx_agent_session:init_hook(),
    ok = emqx_agent_pipeline_mgr:init_hook(),
    {ok, Sup}.

stop(_State) ->
    ok = emqx_agent_skill:deinit(),
    ok = emqx_agent_skill_connections:deinit(),
    ok = emqx_agent_session:deinit_hook(),
    ok = emqx_agent_pipeline_mgr:deinit_hook(),
    ok = emqx_agent_config:clear_config_schema().

on_config_changed(OldConfig, NewConfig) ->
    ok = emqx_agent_config:update_config(OldConfig, NewConfig),
    ok = emqx_agent_skill_registry:reconcile(),
    emqx_agent_skill_connections:reconcile().

on_handle_api_call(Method, PathRemainder, Request, _Context) ->
    emqx_agent_api:handle(Method, PathRemainder, Request).
