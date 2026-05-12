%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% Management skill: list or introspect registered skills.
%%
%% Args (all optional):
%%   type — skill type filter, e.g. "message.publish"
%%   id   — skill instance id (requires type)
%%
%% No args → list all skills
%% type only → list skills of that type
%% type + id → get single skill
%%
%% Invoke topic:  cap/agent.query_skills/<skill_id>/request/<req_id>
%% Reply  topic:  cap/agent.query_skills/<skill_id>/response/<req_id>

-module(emqx_agent_skill_query_skills).

-define(SKILL_TYPE, <<"agent.query_skills">>).

-define(INPUT_SCHEMA, #{
    <<"type">> => <<"object">>,
    <<"properties">> => #{
        <<"type">> => #{
            <<"type">> => <<"string">>,
            <<"description">> => <<"Skill type filter, e.g. message.publish. Omit to list all.">>
        },
        <<"id">> => #{
            <<"type">> => <<"string">>,
            <<"description">> => <<"Skill instance id. Requires type. Returns a single skill.">>
        }
    }
}).

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
        display_name => <<"Query Skills">>,
        description => <<"List all registered skills or look up a specific one by type and id">>,
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

%%--------------------------------------------------------------------
%% Internal
%%--------------------------------------------------------------------

handle_invoke(_Context, Request) ->
    Args = maps:get(<<"args">>, Request, #{}),
    query(Args).

query(#{<<"type">> := Type, <<"id">> := Id}) ->
    case emqx_agent_service:skill_get(Type, Id) of
        {ok, Skill} ->
            {ok, #{<<"item">> => Skill}};
        {error, not_found} ->
            {error, <<"not found">>}
    end;
query(#{<<"type">> := Type}) ->
    All = emqx_agent_service:skill_list(),
    Items = [S || S <- All, maps:get(<<"type">>, S, undefined) =:= Type],
    {ok, #{<<"items">> => Items}};
query(_) ->
    Items = emqx_agent_service:skill_list(),
    {ok, #{<<"items">> => Items}}.
