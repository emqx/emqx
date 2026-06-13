%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_agent_tool_insert_pipeline_step).

-moduledoc """
Management tool: insert a step into a pipeline.

Args:
  pipeline_id — pipeline id (required)
  step        — step definition with a unique id (required)
  position    — "first", "last", or {"after", "<existing_step_id>"}  (required)

Invoke topic:  cap/agent__insert_pipeline_step/<tool_id>/request/<req_id>
Reply  topic:  cap/agent__insert_pipeline_step/<tool_id>/response/<req_id>
""".

-behaviour(emqx_agent_tool).

-define(TOOL_TYPE, <<"agent__insert_pipeline_step">>).

-define(INPUT_SCHEMA, #{
    <<"type">> => <<"object">>,
    <<"properties">> => #{
        <<"pipeline_id">> => #{<<"type">> => <<"string">>, <<"description">> => <<"Pipeline id">>},
        <<"step">> => #{
            <<"type">> => <<"object">>,
            <<"description">> => <<"Step definition with a unique id">>
        },
        <<"position">> => #{
            <<"type">> => <<"object">>,
            <<"description">> =>
                <<"One of: {\"first\": true}, {\"last\": true}, or {\"after\": \"<step_id>\"}">>
        }
    },
    <<"required">> => [<<"pipeline_id">>, <<"step">>, <<"position">>]
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
        display_name => <<"Insert Pipeline Step">>,
        description => <<"Insert a step into a pipeline at first, last, or after a given step id">>,
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
    do_insert(Args).

%%--------------------------------------------------------------------
%% Internal
%%--------------------------------------------------------------------

do_insert(#{<<"pipeline_id">> := PipelineId, <<"step">> := Step, <<"position">> := Pos}) when
    is_map(Step), is_map(Pos)
->
    case parse_position(Pos) of
        {ok, Position} ->
            case emqx_agent_service:pipeline_step_insert(PipelineId, Step, Position, #{}) of
                {ok, Pipeline} -> {ok, #{<<"item">> => Pipeline}};
                {error, not_found} -> {error, <<"pipeline not found">>};
                {error, step_not_found} -> {error, <<"position step not found">>};
                {error, duplicate_step_id} -> {error, <<"step id already exists">>};
                {error, Reason} -> {error, emqx_agent_tool_helpers:format_error(Reason)}
            end;
        {error, Reason} ->
            {error, Reason}
    end;
do_insert(_) ->
    {error, <<"missing required fields: pipeline_id, step, position">>}.

parse_position(#{<<"first">> := true}) ->
    {ok, first};
parse_position(#{<<"last">> := true}) ->
    {ok, last};
parse_position(#{<<"after">> := AfterId}) when is_binary(AfterId) ->
    {ok, {'after', AfterId}};
parse_position(_) ->
    {error, <<"invalid position: expected {first: true}, {last: true}, or {after: \"<id>\"}">>}.
