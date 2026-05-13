%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% Management skill: list or introspect AI providers.

-module(emqx_agent_skill_query_providers).

-behaviour(emqx_agent_skill).

-define(SKILL_TYPE, <<"agent__query_providers">>).

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
    emqx_agent_skill_registry:register_type(?SKILL_TYPE, ?MODULE).

-spec deinit() -> ok.
deinit() ->
    emqx_agent_skill_registry:unregister_type(?SKILL_TYPE).

-spec create(map()) -> {ok, map()} | {error, term()}.
create(#{skill_id := SkillId}) ->
    {ok, #{
        skill_id => SkillId,
        type => ?SKILL_TYPE,
        module => ?MODULE,
        display_name => <<"Query AI Providers">>,
        description => <<"List all AI providers or look up a specific one by name">>,
        context => #{skill_id => SkillId},
        input_schema => ?INPUT_SCHEMA
    }}.

-spec destroy(map()) -> ok.
destroy(_Skill) ->
    ok.

-spec to_map(map()) -> map().
to_map(#{skill_id := Id, description := Desc, input_schema := In}) ->
    #{
        <<"skill_id">> => Id,
        <<"type">> => ?SKILL_TYPE,
        <<"description">> => Desc,
        <<"input_schema">> => In
    }.

handle_invoke(_Context, Request) ->
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
