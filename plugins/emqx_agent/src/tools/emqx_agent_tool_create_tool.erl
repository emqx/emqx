%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_agent_tool_create_tool).

-moduledoc """
Management tool: create any tool type at runtime.

The tool instance itself carries no configuration — only an id.
When invoked, the LLM supplies the full tool creation payload as args.
The tool delegates to emqx_agent_service:tool_create/1 and returns
a structured result so the LLM can detect and retry failures.

Invoke topic:  cap/agent__create_tool/<tool_id>/request/<req_id>
Reply  topic:  cap/agent__create_tool/<tool_id>/response/<req_id>
""".

-behaviour(emqx_agent_tool).

-define(TOOL_TYPE, <<"agent__create_tool">>).

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
        display_name => <<"Create Tool">>,
        description =>
            <<"Create or overwrite a tool (upsert). Types: message__publish, message__request, http, postgresql__query.">>,
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
    Args = maps:get(<<"definition">>, maps:get(<<"args">>, Request, #{}), #{}),
    case emqx_agent_service:tool_create(Args) of
        ok ->
            {ok, #{
                <<"tool_id">> => maps:get(<<"id">>, Args, <<>>),
                <<"type">> => maps:get(<<"type">>, Args, <<>>)
            }};
        {error, unknown_type} ->
            {error, <<"unknown tool type">>};
        {error, Reason} ->
            {error, emqx_agent_tool_helpers:format_error(Reason)}
    end.

%%--------------------------------------------------------------------
%% Internal
%%--------------------------------------------------------------------

input_schema() ->
    #{
        <<"type">> => <<"object">>,
        <<"properties">> => #{
            <<"definition">> => emqx_agent_schema_oai_tool_converter:to_json_schema([tools, items])
        },
        <<"required">> => [<<"definition">>],
        <<"additionalProperties">> => false
    }.
