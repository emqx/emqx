%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_agent_tool_delete_tool).

-moduledoc """
Management tool: delete a registered tool.

Refuses deletion if the tool is referenced in any pipeline step
(call_tool.tool or llm_loop.tools).

Args:
  type — tool type, e.g. "message__publish"  (required)
  id   — tool instance id                   (required)

Invoke topic:  cap/agent__delete_tool/<tool_id>/request/<req_id>
Reply  topic:  cap/agent__delete_tool/<tool_id>/response/<req_id>
""".

-behaviour(emqx_agent_tool).

-define(TOOL_TYPE, <<"agent__delete_tool">>).

-define(INPUT_SCHEMA, #{
    <<"type">> => <<"object">>,
    <<"properties">> => #{
        <<"type">> => #{
            <<"type">> => <<"string">>, <<"description">> => <<"Tool type, e.g. message__publish">>
        },
        <<"id">> => #{<<"type">> => <<"string">>, <<"description">> => <<"Tool instance id">>}
    },
    <<"required">> => [<<"type">>, <<"id">>]
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
        display_name => <<"Delete Tool">>,
        description =>
            <<"Delete a registered tool. Refused if the tool is used in any pipeline.">>,
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

do_delete(#{<<"type">> := Type, <<"id">> := Id}) ->
    case emqx_agent_service:tool_delete(Type, Id) of
        ok ->
            ok;
        {error, not_found} ->
            {error, <<"tool not found">>};
        {error, {in_use, PipelineIds}} ->
            Joined = join_ids(PipelineIds),
            {error, <<"tool is used in pipeline(s): ", Joined/binary>>}
    end;
do_delete(_) ->
    {error, <<"missing required fields: type, id">>}.

join_ids(Ids) ->
    iolist_to_binary(lists:join(<<", ">>, Ids)).
