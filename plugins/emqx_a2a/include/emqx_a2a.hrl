%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-ifndef(EMQX_A2A_HRL).
-define(EMQX_A2A_HRL, true).

%% ETS table names
-define(AGENT_CFG_TAB, emqx_a2a_agent_cfg).
-define(WORKFLOW_TAB, emqx_a2a_workflow).
-define(SESSION_TAB, emqx_a2a_session).

%% Defaults
-define(DEFAULT_REQUEST_TOPIC_PREFIX, <<"$a2a/request/">>).
-define(DEFAULT_PROVIDER, openai).
-define(DEFAULT_MODEL, <<"gpt-5-mini">>).
-define(DEFAULT_MAX_TOKENS, 4096).

%% Logging
-include_lib("emqx/include/logger.hrl").
-define(LOG(Level, Data),
    ?SLOG(Level, maps:merge(#{tag => "A2A", domain => [a2a]}, (Data)))
).

%% Agent execution config record.
%% Discovery/card metadata lives in emqx_a2a_registry.
%% This stores the runtime config needed to actually run an agent:
%% system prompt, model, max_tokens, tools.
%%
%% `id` is the agent_id matching the registry card's agent_id.
%% `registry_id` optionally links to {org, unit, agent_id} in the registry.
-record(?AGENT_CFG_TAB, {
    id :: binary(),
    system_prompt :: binary(),
    model :: binary(),
    max_tokens :: pos_integer(),
    tools :: list(),
    %% Topic filters to read retained messages from before calling the LLM.
    %% Gives the agent device state context (e.g. ["devices/+/state"]).
    context_topics :: [binary()],
    registry_id :: {binary(), binary(), binary()} | undefined,
    extra :: map()
}).

%% Workflow definition record
-record(?WORKFLOW_TAB, {
    id :: binary(),
    name :: binary(),
    description :: binary(),
    variables :: [binary()],
    %% list of task_def maps
    tasks :: list(),
    extra :: map()
}).

%% Session record (running instance)
-record(?SESSION_TAB, {
    id :: binary(),
    type :: agent | workflow,
    %% agent_id or workflow_id
    ref_id :: binary(),
    reply_topic :: binary(),
    correlation :: binary(),
    %% task_id => task_state map
    tasks :: map(),
    status :: submitted | working | completed | failed,
    created_at :: integer(),
    extra :: map()
}).

-endif.
