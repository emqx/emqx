%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_a2a_api).

-moduledoc """
REST API handlers for A2A agent orchestration.

Agent discovery cards are managed by emqx_a2a_registry.
This API manages agent execution configs, workflows, and sessions.

Endpoints:
  GET    /status              - Plugin status
  GET    /agent_configs       - List agent execution configs
  GET    /agent_configs/:id   - Get agent execution config
  POST   /agent_configs       - Create/update agent execution config
  DELETE /agent_configs/:id   - Delete agent execution config
  GET    /workflows           - List workflow definitions
  GET    /workflows/:id       - Get workflow definition
  POST   /workflows           - Create/update workflow
  DELETE /workflows/:id       - Delete workflow
  GET    /sessions            - List active sessions
  GET    /sessions/:id        - Get session details
  DELETE /sessions/:id        - Cancel/delete session
""".

-export([handle/3]).

-include("emqx_a2a.hrl").

%%--------------------------------------------------------------------
%% Status
%%--------------------------------------------------------------------

handle(get, [<<"status">>], _Request) ->
    {ok, Cfgs} = emqx_a2a_store:list_agent_cfgs(),
    {ok, Workflows} = emqx_a2a_store:list_workflows(),
    {ok, Sessions} = emqx_a2a_store:list_sessions(),
    ActiveSessions = [
        S
     || S <- Sessions,
        maps:get(status, S) =/= completed,
        maps:get(status, S) =/= failed
    ],
    {ok, 200, #{}, #{
        plugin => <<"emqx_a2a">>,
        agent_configs => length(Cfgs),
        workflows => length(Workflows),
        active_sessions => length(ActiveSessions),
        provider => emqx_a2a_config:provider(),
        default_model => emqx_a2a_config:default_model()
    }};
%%--------------------------------------------------------------------
%% Agent execution configs
%%--------------------------------------------------------------------

handle(get, [<<"agent_configs">>], _Request) ->
    {ok, Cfgs} = emqx_a2a_store:list_agent_cfgs(),
    {ok, 200, #{}, #{data => Cfgs}};
handle(get, [<<"agent_configs">>, Id], _Request) ->
    case emqx_a2a_store:get_agent_cfg(Id) of
        {ok, Cfg} ->
            {ok, 200, #{}, Cfg};
        {error, not_found} ->
            {error, 404, #{}, #{
                code => <<"NOT_FOUND">>,
                message => <<"Agent config not found">>
            }}
    end;
handle(post, [<<"agent_configs">>], Request) ->
    Body = maps:get(body, Request, #{}),
    case maps:get(<<"id">>, Body, undefined) of
        undefined ->
            {error, 400, #{}, #{
                code => <<"BAD_REQUEST">>,
                message => <<"'id' is required">>
            }};
        _Id ->
            ok = emqx_a2a_store:put_agent_cfg(Body),
            {ok, 201, #{}, Body}
    end;
handle(delete, [<<"agent_configs">>, Id], _Request) ->
    ok = emqx_a2a_store:delete_agent_cfg(Id),
    {ok, 200, #{}, #{}};
%%--------------------------------------------------------------------
%% Workflows CRUD
%%--------------------------------------------------------------------

handle(get, [<<"workflows">>], _Request) ->
    {ok, Workflows} = emqx_a2a_store:list_workflows(),
    {ok, 200, #{}, #{data => Workflows}};
handle(get, [<<"workflows">>, Id], _Request) ->
    case emqx_a2a_store:get_workflow(Id) of
        {ok, Workflow} ->
            {ok, 200, #{}, Workflow};
        {error, not_found} ->
            {error, 404, #{}, #{
                code => <<"NOT_FOUND">>,
                message => <<"Workflow not found">>
            }}
    end;
handle(post, [<<"workflows">>], Request) ->
    Body = maps:get(body, Request, #{}),
    case maps:get(<<"id">>, Body, undefined) of
        undefined ->
            {error, 400, #{}, #{
                code => <<"BAD_REQUEST">>,
                message => <<"'id' is required">>
            }};
        _Id ->
            ok = emqx_a2a_store:put_workflow(Body),
            {ok, 201, #{}, Body}
    end;
handle(delete, [<<"workflows">>, Id], _Request) ->
    ok = emqx_a2a_store:delete_workflow(Id),
    {ok, 200, #{}, #{}};
%%--------------------------------------------------------------------
%% Sessions
%%--------------------------------------------------------------------

handle(get, [<<"sessions">>], _Request) ->
    {ok, Sessions} = emqx_a2a_store:list_sessions(),
    {ok, 200, #{}, #{data => Sessions}};
handle(get, [<<"sessions">>, Id], _Request) ->
    case emqx_a2a_store:get_session(Id) of
        {ok, Session} ->
            {ok, 200, #{}, Session};
        {error, not_found} ->
            {error, 404, #{}, #{
                code => <<"NOT_FOUND">>,
                message => <<"Session not found">>
            }}
    end;
handle(delete, [<<"sessions">>, Id], _Request) ->
    ok = emqx_a2a_store:delete_session(Id),
    {ok, 200, #{}, #{}};
%%--------------------------------------------------------------------
%% Fallback
%%--------------------------------------------------------------------

handle(_Method, _Path, _Request) ->
    {error, 404, #{}, #{
        code => <<"NOT_FOUND">>,
        message => <<"Endpoint not found">>
    }}.
