%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_agent_tool_delete_pipeline_step).

-moduledoc """
Management tool: delete a step from a pipeline by step id.

Args:
  pipeline_id — pipeline id (required)
  step_id     — step id to delete (required)

Invoke topic:  cap/agent__delete_pipeline_step/<tool_id>/request/<req_id>
Reply  topic:  cap/agent__delete_pipeline_step/<tool_id>/response/<req_id>
""".

-behaviour(emqx_agent_tool).

-define(TOOL_TYPE, <<"agent__delete_pipeline_step">>).

-define(INPUT_SCHEMA, #{
    <<"type">> => <<"object">>,
    <<"properties">> => #{
        <<"pipeline_id">> => #{<<"type">> => <<"string">>, <<"description">> => <<"Pipeline id">>},
        <<"step_id">> => #{<<"type">> => <<"string">>, <<"description">> => <<"Step id to delete">>}
    },
    <<"required">> => [<<"pipeline_id">>, <<"step_id">>]
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
        display_name => <<"Delete Pipeline Step">>,
        description => <<"Delete a step from a pipeline by step id">>,
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
    do_delete(Args).

%%--------------------------------------------------------------------
%% Internal
%%--------------------------------------------------------------------

do_delete(#{<<"pipeline_id">> := PipelineId, <<"step_id">> := StepId}) ->
    case emqx_agent_service:pipeline_step_delete(PipelineId, StepId) of
        {ok, Pipeline} -> {ok, #{<<"item">> => Pipeline}};
        {error, not_found} -> {error, <<"pipeline not found">>};
        {error, step_not_found} -> {error, <<"step not found">>};
        {error, Reason} -> {error, emqx_agent_tool_helpers:format_error(Reason)}
    end;
do_delete(_) ->
    {error, <<"missing required fields: pipeline_id, step_id">>}.
