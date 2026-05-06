%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_agent_app).

-behaviour(application).

-emqx_plugin(?MODULE).

-export([start/2, stop/1]).
-export([on_handle_api_call/4]).

start(_StartType, _StartArgs) ->
    {ok, Sup} = emqx_agent_sup:start_link(),
    ok = emqx_agent_skill_postgresql:init(),
    ok = emqx_agent_skill_http:init(),
    ok = emqx_agent_skill_publish:init(),
    ok = emqx_agent_skill_mqtt_request:init(),
    ok = emqx_agent_skill_create_skill:init(),
    ok = emqx_agent_skill_create_pipeline:init(),
    ok = emqx_agent_skill_query_skills:init(),
    ok = emqx_agent_skill_query_providers:init(),
    ok = emqx_agent_skill_query_pipelines:init(),
    ok = emqx_agent_skill_delete_skill:init(),
    ok = emqx_agent_skill_delete_pipeline:init(),
    ok = emqx_agent_skill:init_hook(),
    ok = emqx_agent_session:init_hook(),
    ok = emqx_agent_pipeline_mgr:init_hook(),
    {ok, Sup}.

stop(_State) ->
    ok = emqx_agent_skill:deinit_hook(),
    ok = emqx_agent_skill_postgresql:deinit(),
    ok = emqx_agent_skill_http:deinit(),
    ok = emqx_agent_skill_publish:deinit(),
    ok = emqx_agent_skill_mqtt_request:deinit(),
    ok = emqx_agent_skill_create_skill:deinit(),
    ok = emqx_agent_skill_create_pipeline:deinit(),
    ok = emqx_agent_skill_query_skills:deinit(),
    ok = emqx_agent_skill_query_providers:deinit(),
    ok = emqx_agent_skill_query_pipelines:deinit(),
    ok = emqx_agent_skill_delete_skill:deinit(),
    ok = emqx_agent_skill_delete_pipeline:deinit(),
    ok = emqx_agent_session:deinit_hook(),
    ok = emqx_agent_pipeline_mgr:deinit_hook().

on_handle_api_call(Method, PathRemainder, Request, _Context) ->
    emqx_agent_api:handle(Method, PathRemainder, Request).
