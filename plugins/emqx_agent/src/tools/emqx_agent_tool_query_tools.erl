%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_agent_tool_query_tools).

-moduledoc """
Management tool: list or introspect registered tools.

Args (all optional):
  type — tool type filter, e.g. "message__publish"
  id   — tool instance id (requires type)

No args → list all tools
type only → list tools of that type
type + id → get single tool

Invoke topic:  cap/agent__query_tools/<tool_id>/request/<req_id>
Reply  topic:  cap/agent__query_tools/<tool_id>/response/<req_id>
""".

-behaviour(emqx_agent_tool).

-define(TOOL_TYPE, <<"agent__query_tools">>).

-define(INPUT_SCHEMA, #{
    <<"type">> => <<"object">>,
    <<"properties">> => #{
        <<"type">> => #{
            <<"type">> => <<"string">>,
            <<"description">> => <<"Tool type filter, e.g. message__publish. Omit to list all.">>
        },
        <<"id">> => #{
            <<"type">> => <<"string">>,
            <<"description">> => <<"Tool instance id. Requires type. Returns a single tool.">>
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
create(#{<<"tool_id">> := ToolId}) ->
    {ok, #{
        tool_id => ToolId,
        type => ?TOOL_TYPE,
        module => ?MODULE,
        display_name => <<"Query Tools">>,
        description => <<"List all registered tools or look up a specific one by type and id">>,
        context => #{<<"tool_id">> => ToolId},
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

query(#{<<"type">> := Type, <<"id">> := Id}) ->
    case emqx_agent_service:tool_get(Type, Id) of
        {ok, Tool} ->
            {ok, #{<<"item">> => Tool}};
        {error, not_found} ->
            {error, <<"not found">>}
    end;
query(#{<<"type">> := Type}) ->
    All = emqx_agent_service:tool_list(),
    Items = [S || S <- All, maps:get(<<"type">>, S, undefined) =:= Type],
    {ok, #{<<"items">> => Items}};
query(_) ->
    Items = emqx_agent_service:tool_list(),
    {ok, #{<<"items">> => Items}}.
