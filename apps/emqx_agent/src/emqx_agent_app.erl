%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_agent_app).

-behaviour(application).

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    {ok, Sup} = emqx_agent_sup:start_link(),
    ok = emqx_agent_skill_postgresql:init(),
    ok = emqx_agent_skill_kv:init(),
    ok = emqx_agent_skill_http:init(),
    ok = emqx_agent_skill_publish:init(),
    ok = emqx_agent_session:init_hook(),
    ok = emqx_agent_pipeline_mgr:init_hook(),
    {ok, Sup}.

stop(_State) ->
    ok = emqx_agent_skill_postgresql:deinit(),
    ok = emqx_agent_skill_kv:deinit(),
    ok = emqx_agent_skill_http:deinit(),
    ok = emqx_agent_skill_publish:deinit(),
    ok = emqx_agent_session:deinit_hook(),
    ok = emqx_agent_pipeline_mgr:deinit_hook().
