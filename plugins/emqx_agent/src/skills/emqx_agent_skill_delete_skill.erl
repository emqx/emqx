%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% Management skill: delete a registered skill.
%%
%% Refuses deletion if the skill is referenced in any pipeline step
%% (call_skill.skill or llm_loop.tools).
%%
%% Args:
%%   type — skill type, e.g. "message.publish"  (required)
%%   id   — skill instance id                   (required)
%%
%% Invoke topic:  cap/agent.delete_skill/<skill_id>/request/<req_id>
%% Reply  topic:  cap/agent.delete_skill/<skill_id>/response/<req_id>

-module(emqx_agent_skill_delete_skill).

-define(SKILL_TYPE, <<"agent.delete_skill">>).

-define(INPUT_SCHEMA, #{
    <<"type">> => <<"object">>,
    <<"properties">> => #{
        <<"type">> => #{
            <<"type">> => <<"string">>, <<"description">> => <<"Skill type, e.g. message.publish">>
        },
        <<"id">> => #{<<"type">> => <<"string">>, <<"description">> => <<"Skill instance id">>}
    },
    <<"required">> => [<<"type">>, <<"id">>]
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
        display_name => <<"Delete Skill">>,
        description =>
            <<"Delete a registered skill. Refused if the skill is used in any pipeline.">>,
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

do_delete(#{<<"type">> := Type, <<"id">> := Id}) ->
    case emqx_agent_service:skill_delete(Type, Id) of
        ok ->
            ok;
        {error, not_found} ->
            {error, <<"skill not found">>};
        {error, {in_use, PipelineIds}} ->
            Joined = join_ids(PipelineIds),
            {error, <<"skill is used in pipeline(s): ", Joined/binary>>}
    end;
do_delete(_) ->
    {error, <<"missing required fields: type, id">>}.

join_ids(Ids) ->
    iolist_to_binary(lists:join(<<", ">>, Ids)).
