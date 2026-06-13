%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_agent_tool_query_pipelines).

-moduledoc """
Management tool: list or introspect pipeline definitions.

Args (all optional):
  id — pipeline id. Omit to list all pipelines.

Invoke topic:  cap/agent__query_pipelines/<tool_id>/request/<req_id>
Reply  topic:  cap/agent__query_pipelines/<tool_id>/response/<req_id>
""".

-behaviour(emqx_agent_tool).

-define(TOOL_TYPE, <<"agent__query_pipelines">>).

-define(INPUT_SCHEMA, #{
    <<"type">> => <<"object">>,
    <<"properties">> => #{
        <<"id">> => #{
            <<"type">> => <<"string">>,
            <<"description">> => <<"Pipeline id. Omit to list all pipelines.">>
        }
    }
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
        display_name => <<"Query Pipelines">>,
        description => <<"List all pipeline definitions or look up a specific one by id">>,
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
    query(Args).

%%--------------------------------------------------------------------
%% Internal
%%--------------------------------------------------------------------

query(#{<<"id">> := Id}) ->
    case emqx_agent_service:pipeline_get(Id) of
        {ok, Pipeline} ->
            {ok, #{<<"item">> => Pipeline}};
        {error, not_found} ->
            {error, <<"not found">>}
    end;
query(_) ->
    Items = emqx_agent_service:pipeline_list(),
    {ok, #{<<"items">> => Items}}.
