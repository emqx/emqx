%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_a2a_store).

-moduledoc """
ETS-backed storage for agent execution configs, workflow definitions,
and session state.

Agent discovery cards are managed by emqx_a2a_registry.
This module stores the runtime execution config (system prompt, model,
tools) needed to actually run agents as LLM-backed Erlang processes.
""".

-behaviour(gen_server).

-export([
    start_link/0,
    create_tables/0,
    %% Agent execution configs
    put_agent_cfg/1,
    get_agent_cfg/1,
    delete_agent_cfg/1,
    list_agent_cfgs/0,
    %% Workflows
    put_workflow/1,
    get_workflow/1,
    delete_workflow/1,
    list_workflows/0,
    %% Sessions
    put_session/1,
    get_session/1,
    update_session/1,
    delete_session/1,
    list_sessions/0
]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-include("emqx_a2a.hrl").

%%--------------------------------------------------------------------
%% Table creation (ETS)
%%--------------------------------------------------------------------

create_tables() ->
    ensure_table(?AGENT_CFG_TAB, [
        named_table,
        ordered_set,
        public,
        {keypos, #?AGENT_CFG_TAB.id}
    ]),
    ensure_table(?WORKFLOW_TAB, [
        named_table,
        ordered_set,
        public,
        {keypos, #?WORKFLOW_TAB.id}
    ]),
    ensure_table(?SESSION_TAB, [
        named_table,
        ordered_set,
        public,
        {keypos, #?SESSION_TAB.id}
    ]),
    ok.

ensure_table(Name, Opts) ->
    case ets:whereis(Name) of
        undefined -> ets:new(Name, Opts);
        _Ref -> Name
    end.

%%--------------------------------------------------------------------
%% API: Agent execution configs
%%--------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

put_agent_cfg(CfgMap) when is_map(CfgMap) ->
    Record = map_to_agent_cfg(CfgMap),
    ets:insert(?AGENT_CFG_TAB, Record),
    ok.

get_agent_cfg(Id) ->
    case ets:lookup(?AGENT_CFG_TAB, Id) of
        [Record] -> {ok, agent_cfg_to_map(Record)};
        [] -> {error, not_found}
    end.

delete_agent_cfg(Id) ->
    ets:delete(?AGENT_CFG_TAB, Id),
    ok.

list_agent_cfgs() ->
    Records = ets:tab2list(?AGENT_CFG_TAB),
    {ok, [agent_cfg_to_map(R) || R <- Records]}.

%%--------------------------------------------------------------------
%% API: Workflows
%%--------------------------------------------------------------------

put_workflow(WorkflowMap) when is_map(WorkflowMap) ->
    Record = map_to_workflow(WorkflowMap),
    ets:insert(?WORKFLOW_TAB, Record),
    ok.

get_workflow(Id) ->
    case ets:lookup(?WORKFLOW_TAB, Id) of
        [Record] -> {ok, workflow_to_map(Record)};
        [] -> {error, not_found}
    end.

delete_workflow(Id) ->
    ets:delete(?WORKFLOW_TAB, Id),
    ok.

list_workflows() ->
    Records = ets:tab2list(?WORKFLOW_TAB),
    {ok, [workflow_to_map(R) || R <- Records]}.

%%--------------------------------------------------------------------
%% API: Sessions
%%--------------------------------------------------------------------

put_session(SessionMap) when is_map(SessionMap) ->
    Record = map_to_session(SessionMap),
    ets:insert(?SESSION_TAB, Record),
    ok.

get_session(Id) ->
    case ets:lookup(?SESSION_TAB, Id) of
        [Record] -> {ok, session_to_map(Record)};
        [] -> {error, not_found}
    end.

update_session(SessionMap) ->
    put_session(SessionMap).

delete_session(Id) ->
    ets:delete(?SESSION_TAB, Id),
    ok.

list_sessions() ->
    Records = ets:tab2list(?SESSION_TAB),
    {ok, [session_to_map(R) || R <- Records]}.

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([]) ->
    {ok, #{}}.

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% Internal: agent_cfg record <-> map
%%--------------------------------------------------------------------

map_to_agent_cfg(#{<<"id">> := Id} = M) ->
    RegistryId =
        case M of
            #{<<"org_id">> := Org, <<"unit_id">> := Unit} ->
                {Org, Unit, Id};
            _ ->
                undefined
        end,
    #?AGENT_CFG_TAB{
        id = Id,
        system_prompt = maps:get(<<"system_prompt">>, M, <<>>),
        model = maps:get(<<"model">>, M, <<>>),
        max_tokens = maps:get(<<"max_tokens">>, M, 0),
        tools = maps:get(<<"tools">>, M, []),
        context_topics = maps:get(<<"context_topics">>, M, []),
        registry_id = RegistryId,
        extra = maps:get(<<"extra">>, M, #{})
    }.

agent_cfg_to_map(#?AGENT_CFG_TAB{} = R) ->
    Base = #{
        id => R#?AGENT_CFG_TAB.id,
        system_prompt => R#?AGENT_CFG_TAB.system_prompt,
        model => R#?AGENT_CFG_TAB.model,
        max_tokens => R#?AGENT_CFG_TAB.max_tokens,
        tools => R#?AGENT_CFG_TAB.tools,
        context_topics => R#?AGENT_CFG_TAB.context_topics,
        extra => R#?AGENT_CFG_TAB.extra
    },
    case R#?AGENT_CFG_TAB.registry_id of
        {Org, Unit, _} ->
            Base#{org_id => Org, unit_id => Unit};
        undefined ->
            Base
    end.

%%--------------------------------------------------------------------
%% Internal: workflow record <-> map
%%--------------------------------------------------------------------

map_to_workflow(#{<<"id">> := Id} = M) ->
    #?WORKFLOW_TAB{
        id = Id,
        name = maps:get(<<"name">>, M, Id),
        description = maps:get(<<"description">>, M, <<>>),
        variables = maps:get(<<"variables">>, M, []),
        tasks = maps:get(<<"tasks">>, M, []),
        extra = maps:get(<<"extra">>, M, #{})
    }.

workflow_to_map(#?WORKFLOW_TAB{} = R) ->
    #{
        id => R#?WORKFLOW_TAB.id,
        name => R#?WORKFLOW_TAB.name,
        description => R#?WORKFLOW_TAB.description,
        variables => R#?WORKFLOW_TAB.variables,
        tasks => R#?WORKFLOW_TAB.tasks,
        extra => R#?WORKFLOW_TAB.extra
    }.

%%--------------------------------------------------------------------
%% Internal: session record <-> map
%%--------------------------------------------------------------------

map_to_session(#{id := Id} = M) ->
    #?SESSION_TAB{
        id = Id,
        type = maps:get(type, M, agent),
        ref_id = maps:get(ref_id, M, <<>>),
        reply_topic = maps:get(reply_topic, M, <<>>),
        correlation = maps:get(correlation, M, <<>>),
        tasks = maps:get(tasks, M, #{}),
        status = maps:get(status, M, submitted),
        created_at = maps:get(created_at, M, erlang:system_time(millisecond)),
        extra = maps:get(extra, M, #{})
    }.

session_to_map(#?SESSION_TAB{} = R) ->
    #{
        id => R#?SESSION_TAB.id,
        type => R#?SESSION_TAB.type,
        ref_id => R#?SESSION_TAB.ref_id,
        reply_topic => R#?SESSION_TAB.reply_topic,
        correlation => R#?SESSION_TAB.correlation,
        tasks => R#?SESSION_TAB.tasks,
        status => R#?SESSION_TAB.status,
        created_at => R#?SESSION_TAB.created_at,
        extra => R#?SESSION_TAB.extra
    }.
