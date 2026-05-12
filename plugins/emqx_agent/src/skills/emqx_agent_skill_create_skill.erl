%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% Management skill: create any skill type at runtime.
%%
%% The skill instance itself carries no configuration — only an id.
%% When invoked, the LLM supplies the full skill creation payload as args.
%% The skill delegates to emqx_agent_service:skill_create/1 and returns
%% a structured result so the LLM can detect and retry failures.
%%
%% Invoke topic:  cap/agent.create_skill/<skill_id>/request/<req_id>
%% Reply  topic:  cap/agent.create_skill/<skill_id>/response/<req_id>

-module(emqx_agent_skill_create_skill).

-define(SKILL_TYPE, <<"agent.create_skill">>).

-export([init/0, deinit/0, create/1, destroy/1, to_map/1, handle_invoke/2]).

%%--------------------------------------------------------------------
%% Public API
%%--------------------------------------------------------------------

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
        display_name => <<"Create Skill">>,
        description =>
            <<"Create or overwrite a skill (upsert). Types: message.publish, message.request, http, postgresql.query.">>,
        context => #{skill_id => SkillId},
        input_schema => input_schema()
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

%%--------------------------------------------------------------------
%% Internal
%%--------------------------------------------------------------------

handle_invoke(_Context, Request) ->
    Args = maps:get(<<"definition">>, maps:get(<<"args">>, Request, #{}), #{}),
    case emqx_agent_service:skill_create(Args) of
        ok ->
            {ok, #{
                <<"skill_id">> => maps:get(<<"id">>, Args, <<>>),
                <<"type">> => maps:get(<<"type">>, Args, <<>>)
            }};
        {error, unknown_type} ->
            {error, <<"unknown skill type">>};
        {error, Reason} ->
            {error, emqx_agent_skill_helpers:format_error(Reason)}
    end.

input_schema() ->
    emqx_agent_schema_oai_tool_converter:to_json_schema(
        {object, [{definition, emqx_agent_schema:skill_create_type()}]}
    ).
