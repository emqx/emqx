%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_agent_tool_query_connections).

-moduledoc """
Management tool: list or introspect configured connections.

Args (all optional):
  id — connection id. Omit to list all connections.

Invoke topic:  cap/agent__query_connections/<tool_id>/request/<req_id>
Reply  topic:  cap/agent__query_connections/<tool_id>/response/<req_id>
""".

-behaviour(emqx_agent_tool).

-define(TOOL_TYPE, <<"agent__query_connections">>).

-define(INPUT_SCHEMA, #{
    <<"type">> => <<"object">>,
    <<"properties">> => #{
        <<"id">> => #{
            <<"type">> => <<"string">>,
            <<"description">> => <<"Connection id. Omit to list all connections.">>
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
        display_name => <<"Query Connections">>,
        description => <<"List all configured connections or look up a specific one by id">>,
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
    case emqx_agent_service:connection_get(Id) of
        {ok, Conn} ->
            {ok, #{<<"item">> => emqx_utils:redact(Conn)}};
        {error, not_found} ->
            {error, <<"not found">>}
    end;
query(_) ->
    Items = [emqx_utils:redact(Conn) || Conn <- emqx_agent_service:connection_list()],
    {ok, #{<<"items">> => Items}}.
