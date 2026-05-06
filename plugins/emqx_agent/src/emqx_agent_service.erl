%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% Service layer for the agent subsystem.
%%
%% Accepts raw maps and binary IDs, performs validation and schema
%% coercion, then delegates to the registries and skill modules.
%%
%% Returns:
%%   ok | {ok, term()} | {error, Reason}
%%
%% Reasons:
%%   not_found
%%   unknown_type
%%   {missing_field, binary()}
%%   term()  (hocon validation errors)

-module(emqx_agent_service).

-export([
    %% Skills
    skill_list/0,
    skill_create/1,
    skill_get/2,
    skill_update/3,
    skill_delete/2,
    %% Pipelines
    pipeline_list/0,
    pipeline_create/1,
    pipeline_get/1,
    pipeline_update/2,
    pipeline_delete/1
]).

%%--------------------------------------------------------------------
%% Skills
%%--------------------------------------------------------------------

-spec skill_list() -> [map()].
skill_list() ->
    [skill_to_map(S) || S <- emqx_agent_skill_registry:list()].

-spec skill_create(map()) ->
    ok | {error, unknown_type | {missing_field, binary()} | term()}.
skill_create(Body) ->
    do_create_skill(Body).

-spec skill_get(binary(), binary()) -> {ok, map()} | {error, not_found}.
skill_get(Type, Id) ->
    case emqx_agent_skill_registry:lookup(Type, Id) of
        {ok, Skill} -> {ok, skill_to_map(Skill)};
        {error, not_found} -> {error, not_found}
    end.

-spec skill_update(binary(), binary(), map()) ->
    {ok, map()} | {error, unknown_type | {missing_field, binary()} | not_found | term()}.
skill_update(Type, Id, Body) ->
    Body2 = Body#{<<"id">> => Id},
    case do_create_skill(Body2) of
        ok ->
            case emqx_agent_skill_registry:lookup(Type, Id) of
                {ok, Skill} -> {ok, skill_to_map(Skill)};
                {error, not_found} -> {error, not_found}
            end;
        {error, _} = Err ->
            Err
    end.

-spec skill_delete(binary(), binary()) ->
    ok | {error, not_found | {in_use, [binary()]}}.
skill_delete(Type, Id) ->
    case emqx_agent_skill_registry:lookup(Type, Id) of
        {error, not_found} ->
            {error, not_found};
        {ok, Skill} ->
            Ref = <<Type/binary, "@", Id/binary>>,
            case pipelines_using_skill(Ref) of
                [] -> do_destroy_skill(Skill, Id);
                Ids -> {error, {in_use, Ids}}
            end
    end.

%%--------------------------------------------------------------------
%% Pipelines
%%--------------------------------------------------------------------

-spec pipeline_list() -> [map()].
pipeline_list() ->
    emqx_agent_pipeline_registry:list().

-spec pipeline_create(map()) -> ok | {error, {missing_field, binary()} | term()}.
pipeline_create(#{<<"pipeline_id">> := _} = Body) ->
    emqx_agent_pipeline_registry:register(Body);
pipeline_create(_) ->
    {error, {missing_field, <<"pipeline_id">>}}.

-spec pipeline_get(binary()) -> {ok, map()} | {error, not_found}.
pipeline_get(Id) ->
    emqx_agent_pipeline_registry:lookup(Id).

-spec pipeline_update(binary(), map()) -> {ok, map()} | {error, term()}.
pipeline_update(Id, Body) ->
    Body2 = Body#{<<"pipeline_id">> => Id},
    case emqx_agent_pipeline_registry:register(Body2) of
        ok -> emqx_agent_pipeline_registry:lookup(Id);
        {error, _} = Err -> Err
    end.

-spec pipeline_delete(binary()) ->
    ok | {error, not_found | pipeline_is_active}.
pipeline_delete(Id) ->
    case emqx_agent_pipeline_registry:lookup(Id) of
        {error, not_found} ->
            {error, not_found};
        {ok, #{<<"active">> := true}} ->
            {error, pipeline_is_active};
        {ok, _} ->
            emqx_agent_pipeline_registry:unregister(Id)
    end.

%%--------------------------------------------------------------------
%% Internal — skill validation and dispatch
%%--------------------------------------------------------------------

do_create_skill(#{<<"type">> := Type} = Body) ->
    Schema = #{roots => [{skill, emqx_agent_schema:skill_create_type()}]},
    try hocon_tconf:check_plain(Schema, #{<<"skill">> => Body}, #{atom_key => true}) of
        #{skill := Ctx} ->
            Ctx2 = maps:put(skill_id, maps:get(id, Ctx), maps:remove(id, Ctx)),
            Mod = emqx_agent_skill_registry:resolve_type(Type),
            Mod:create(Ctx2)
    catch
        throw:unknown_type ->
            {error, unknown_type};
        throw:#{field_name := <<"type">>} ->
            {error, unknown_type};
        throw:Error ->
            {error, Error}
    end;
do_create_skill(_) ->
    {error, {missing_field, <<"type">>}}.

do_destroy_skill(#{module := Mod}, Id) ->
    Mod:destroy(Id).

skill_to_map(#{module := Mod} = Skill) ->
    Mod:to_map(Skill).

pipelines_using_skill(Ref) ->
    [
        maps:get(<<"pipeline_id">>, P)
     || P <- emqx_agent_pipeline_registry:list(),
        skill_ref_in_pipeline(Ref, P)
    ].

skill_ref_in_pipeline(Ref, Pipeline) ->
    Steps = maps:get(<<"steps">>, Pipeline, []),
    lists:any(fun(Step) -> skill_ref_in_step(Ref, Step) end, Steps).

skill_ref_in_step(Ref, #{<<"type">> := <<"call_skill">>} = Step) ->
    maps:get(<<"skill">>, Step, undefined) =:= Ref;
skill_ref_in_step(Ref, #{<<"type">> := <<"llm_loop">>} = Step) ->
    lists:member(Ref, maps:get(<<"tools">>, Step, []));
skill_ref_in_step(_Ref, _Step) ->
    false.
