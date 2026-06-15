%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_agent_tool_update_tool).

-moduledoc """
Management tool: update an existing tool at runtime.

Args:
  type       — tool type, e.g. "message__publish"  (required)
  id         — tool instance id                   (required)
  definition — partial tool definition             (required)

The definition is merged into the existing tool, replacing only the
provided fields. The type and id cannot be changed.

Invoke topic:  cap/agent__update_tool/<tool_id>/request/<req_id>
Reply  topic:  cap/agent__update_tool/<tool_id>/response/<req_id>
""".

-behaviour(emqx_agent_tool).

-define(TOOL_TYPE, <<"agent__update_tool">>).

-define(INPUT_SCHEMA, #{
    <<"type">> => <<"object">>,
    <<"properties">> => #{
        <<"type">> => #{
            <<"type">> => <<"string">>,
            <<"description">> => <<"Tool type, e.g. message__publish">>
        },
        <<"id">> => #{<<"type">> => <<"string">>, <<"description">> => <<"Tool instance id">>},
        <<"definition">> => #{
            <<"type">> => <<"object">>,
            <<"description">> => <<"Partial tool definition; only provided fields are replaced">>
        }
    },
    <<"required">> => [<<"type">>, <<"id">>, <<"definition">>]
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
        display_name => <<"Update Tool">>,
        description => <<"Update an existing tool by merging a partial definition">>,
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

do_update(#{<<"type">> := Type, <<"id">> := Id, <<"definition">> := Def}) when
    is_map(Def)
->
    case emqx_agent_service:tool_get(Type, Id) of
        {ok, Existing} ->
            Merged = maps:merge(Existing, maps:without([<<"type">>, <<"id">>], Def)),
            case emqx_agent_service:tool_update(Type, Id, Merged) of
                {ok, Tool} -> {ok, #{<<"item">> => Tool}};
                {error, Reason} -> {error, emqx_agent_tool_helpers:format_error(Reason)}
            end;
        {error, not_found} ->
            {error, <<"tool not found">>}
    end;
do_update(_) ->
    {error, <<"missing required fields: type, id, definition">>}.
