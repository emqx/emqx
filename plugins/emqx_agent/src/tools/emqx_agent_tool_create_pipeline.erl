%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_agent_tool_create_pipeline).

-moduledoc """
Management tool: create a pipeline definition at runtime.

The tool instance carries no configuration — only an id.
When invoked, the LLM supplies the full pipeline payload as args.
The tool enforces active=false unconditionally — this flag is not
exposed in the input schema so the LLM cannot accidentally activate
an untested pipeline.

Invoke topic:  $cap/agent__create_pipeline/<tool_id>/request/<req_id>
Reply  topic:  $cap/agent__create_pipeline/<tool_id>/response/<req_id>
""".

-behaviour(emqx_agent_tool).

-define(TOOL_TYPE, <<"agent__create_pipeline">>).

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
        display_name => <<"Create Pipeline">>,
        description =>
            <<"Create or overwrite a pipeline definition (upsert). Registered as inactive draft; activate via the API or admin UI.">>,
        context => #{<<"id">> => ToolId},
        input_schema => input_schema()
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
    %% Enforce active=false unconditionally — not exposed to the LLM.
    Body = Args#{<<"active">> => false},
    case emqx_agent_service:pipeline_create(Body) of
        ok ->
            {ok, #{
                <<"pipeline_id">> => maps:get(<<"pipeline_id">>, Args, <<>>),
                <<"active">> => false
            }};
        {error, Reason} ->
            {error, emqx_agent_tool_helpers:format_error(Reason)}
    end.

%%--------------------------------------------------------------------
%% Internal
%%--------------------------------------------------------------------

input_schema() ->
    emqx_agent_schema_oai_tool_converter:to_json_schema([pipelines, items]).
