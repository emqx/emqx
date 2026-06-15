%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_agent_tool_query_providers).

-moduledoc """
Management tool: list or introspect AI providers.
""".

-behaviour(emqx_agent_tool).

-define(TOOL_TYPE, <<"agent__query_providers">>).

-define(INPUT_SCHEMA, #{
    <<"type">> => <<"object">>,
    <<"properties">> => #{
        <<"name">> => #{
            <<"type">> => <<"string">>,
            <<"description">> => <<"Provider name. Omit to list all providers.">>
        }
    }
}).

-export([init/0, deinit/0, create/1, destroy/1, to_map/1, handle_invoke/2]).

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
        display_name => <<"Query AI Providers">>,
        description => <<"List all AI providers or look up a specific one by name">>,
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

query(#{<<"name">> := Name}) ->
    case find_provider(Name) of
        {ok, Provider} ->
            {ok, #{<<"item">> => format_provider(Provider)}};
        not_found ->
            {error, <<"not found">>}
    end;
query(_) ->
    Items = [format_provider(P) || P <- emqx_ai_completion_config:get_providers_raw()],
    {ok, #{<<"items">> => Items}}.

find_provider(Name) ->
    case
        [P || #{<<"name">> := N} = P <- emqx_ai_completion_config:get_providers_raw(), N =:= Name]
    of
        [Provider] -> {ok, Provider};
        [] -> not_found
    end.

format_provider(Provider) ->
    emqx_utils:redact(Provider).
