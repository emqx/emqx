%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_agent_tool_delete_pipeline).

-moduledoc """
Management tool: delete a pipeline definition.

Refuses deletion if the pipeline is currently active.
Deactivate via the API or agent.update_pipeline before deleting.

Args:
  id — pipeline id  (required)

Invoke topic:  $cap/agent__delete_pipeline/<tool_id>/request/<req_id>
Reply  topic:  $cap/agent__delete_pipeline/<tool_id>/response/<req_id>
""".

-behaviour(emqx_agent_tool).

-define(TOOL_TYPE, <<"agent__delete_pipeline">>).

-define(INPUT_SCHEMA, #{
    <<"type">> => <<"object">>,
    <<"properties">> => #{
        <<"id">> => #{<<"type">> => <<"string">>, <<"description">> => <<"Pipeline id">>}
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
create(#{tool_id := ToolId}) ->
    {ok, #{
        tool_id => ToolId,
        type => ?TOOL_TYPE,
        module => ?MODULE,
        display_name => <<"Delete Pipeline">>,
        description => <<"Delete a pipeline definition. Refused if the pipeline is active.">>,
        context => #{tool_id => ToolId},
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

do_delete(#{<<"id">> := Id}) ->
    case emqx_agent_service:pipeline_delete(Id) of
        ok ->
            ok;
        {error, not_found} ->
            {error, <<"pipeline not found">>};
        {error, pipeline_is_active} ->
            {error, <<"pipeline is active; set active=false before deleting">>}
    end;
do_delete(_) ->
    {error, <<"missing required field: id">>}.
