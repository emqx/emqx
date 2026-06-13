%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_agent_tool_update_pipeline).

-moduledoc """
Management tool: update a pipeline definition while retaining steps.

Args:
  id         — pipeline id (required)
  definition — map of fields to update (optional)

Allowed update fields: trigger, active, and any other pipeline fields
except pipeline_id and steps. Existing steps are always retained.

Invoke topic:  cap/agent__update_pipeline/<tool_id>/request/<req_id>
Reply  topic:  cap/agent__update_pipeline/<tool_id>/response/<req_id>
""".

-behaviour(emqx_agent_tool).

-define(TOOL_TYPE, <<"agent__update_pipeline">>).

-define(INPUT_SCHEMA, #{
    <<"type">> => <<"object">>,
    <<"properties">> => #{
        <<"id">> => #{
            <<"type">> => <<"string">>,
            <<"description">> => <<"Pipeline id">>
        },
        <<"definition">> => #{
            <<"type">> => <<"object">>,
            <<"description">> =>
                <<"Pipeline fields to update. Cannot change pipeline_id or steps.">>
        }
    },
    <<"required">> => [<<"id">>]
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
        display_name => <<"Update Pipeline">>,
        description => <<"Update a pipeline definition while retaining its steps">>,
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

do_update(#{<<"id">> := Id} = Args) ->
    do_update(Id, maps:get(<<"definition">>, Args, #{}));
do_update(_) ->
    {error, <<"missing required field: id">>}.

do_update(Id, Def) when is_map(Def) ->
    case emqx_agent_service:pipeline_update_retain_steps(Id, Def) of
        {ok, Pipeline} -> {ok, #{<<"item">> => Pipeline}};
        {error, not_found} -> {error, <<"pipeline not found">>};
        {error, Reason} -> {error, emqx_agent_tool_helpers:format_error(Reason)}
    end.
