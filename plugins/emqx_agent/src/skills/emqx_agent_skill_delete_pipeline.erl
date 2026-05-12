%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% Management skill: delete a pipeline definition.
%%
%% Refuses deletion if the pipeline is currently active.
%% Deactivate via the API or agent.update_pipeline before deleting.
%%
%% Args:
%%   id — pipeline id  (required)
%%
%% Invoke topic:  cap/agent__delete_pipeline/<skill_id>/request/<req_id>
%% Reply  topic:  cap/agent__delete_pipeline/<skill_id>/response/<req_id>

-module(emqx_agent_skill_delete_pipeline).

-define(SKILL_TYPE, <<"agent__delete_pipeline">>).

-define(INPUT_SCHEMA, #{
    <<"type">> => <<"object">>,
    <<"properties">> => #{
        <<"id">> => #{<<"type">> => <<"string">>, <<"description">> => <<"Pipeline id">>}
    },
    <<"required">> => [<<"id">>]
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
        display_name => <<"Delete Pipeline">>,
        description => <<"Delete a pipeline definition. Refused if the pipeline is active.">>,
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
    do_delete(Args).

do_delete(#{<<"id">> := Id}) ->
    case emqx_agent_service:pipeline_delete(Id) of
        ok ->
            ok;
        {error, not_found} ->
            {error, <<"pipeline not found">>};
        {error, pipeline_is_active} ->
            {error, <<"pipeline is active; set active=false before deleting">>}
    end;
do_delete(_) ->
    {error, <<"missing required field: id">>}.
