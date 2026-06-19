%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_agent_tool_update_pipeline_step).

-moduledoc """
Management tool: update a single step in a pipeline by step id.

Args:
  pipeline_id — pipeline id (required)
  step_id     — existing step id to update (required)
  step        — partial or full step definition (required)

The provided step is merged into the existing step, replacing only the
provided fields. The step id cannot be changed.

Invoke topic:  cap/agent__update_pipeline_step/<tool_id>/request/<req_id>
Reply  topic:  cap/agent__update_pipeline_step/<tool_id>/response/<req_id>
""".

-behaviour(emqx_agent_tool).

-define(TOOL_TYPE, <<"agent__update_pipeline_step">>).

-define(INPUT_SCHEMA, #{
    <<"type">> => <<"object">>,
    <<"properties">> => #{
        <<"pipeline_id">> => #{<<"type">> => <<"string">>, <<"description">> => <<"Pipeline id">>},
        <<"step_id">> => #{
            <<"type">> => <<"string">>, <<"description">> => <<"Step id to update">>
        },
        <<"step">> => #{
            <<"type">> => <<"object">>,
            <<"description">> => <<"Step fields to merge. Cannot change the step id.">>
        }
    },
    <<"required">> => [<<"pipeline_id">>, <<"step_id">>, <<"step">>]
}).

-export([init/0, deinit/0, create/1, destroy/1, to_map/1, handle_invoke/2]).

%%--------------------------------------------------------------------
%% Public API
%%--------------------------------------------------------------------

-spec init() -> ok.
init() ->
    emqx_agent_tool_registry:register_type(?TOOL_TYPE, ?MODULE).

-spec deinit() -> ok.
deinit() ->
    emqx_agent_tool_registry:unregister_type(?TOOL_TYPE).

-spec create(map()) -> {ok, map()} | {error, term()}.
create(#{<<"id">> := ToolId}) ->
    {ok, #{
        tool_id => ToolId,
        type => ?TOOL_TYPE,
        module => ?MODULE,
        display_name => <<"Update Pipeline Step">>,
        description => <<"Update a single step in a pipeline by step id">>,
        context => #{<<"id">> => ToolId},
        input_schema => ?INPUT_SCHEMA
    }}.

-spec destroy(map()) -> ok.
destroy(_Tool) ->
    ok.

-spec to_map(map()) -> map().
to_map(#{tool_id := Id, description := Desc, input_schema := In}) ->
    #{
        <<"tool_id">> => Id,
        <<"type">> => ?TOOL_TYPE,
        <<"description">> => Desc,
        <<"input_schema">> => In
    }.

handle_invoke(_Context, Request) ->
    emqx_agent_builder_tool_server:call(fun() -> do_handle_invoke(Request) end).

do_handle_invoke(Request) ->
    Args = maps:get(<<"args">>, Request, #{}),
    do_update(Args).

%%--------------------------------------------------------------------
%% Internal
%%--------------------------------------------------------------------

do_update(#{<<"pipeline_id">> := PipelineId, <<"step_id">> := StepId, <<"step">> := Step}) when
    is_map(Step)
->
    case emqx_agent_service:pipeline_step_update(PipelineId, StepId, Step) of
        {ok, Pipeline} -> {ok, #{<<"item">> => Pipeline}};
        {error, not_found} -> {error, <<"pipeline not found">>};
        {error, step_not_found} -> {error, <<"step not found">>};
        {error, Reason} -> {error, emqx_agent_tool_helpers:format_error(Reason)}
    end;
do_update(_) ->
    {error, <<"missing required fields: pipeline_id, step_id, step">>}.
