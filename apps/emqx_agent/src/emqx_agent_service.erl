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
    %% Session profiles
    profile_list/0,
    profile_create/1,
    profile_get/1,
    profile_update/2,
    profile_delete/1,
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

-spec skill_delete(binary(), binary()) -> ok | {error, not_found}.
skill_delete(Type, Id) ->
    case emqx_agent_skill_registry:lookup(Type, Id) of
        {error, not_found} ->
            {error, not_found};
        {ok, _} ->
            do_destroy_skill(Type, Id)
    end.

%%--------------------------------------------------------------------
%% Session profiles
%%--------------------------------------------------------------------

-spec profile_list() -> [map()].
profile_list() ->
    emqx_agent_pipeline_registry:list_profiles().

-spec profile_create(map()) -> ok | {error, {missing_field, binary()}}.
profile_create(#{<<"name">> := Name} = Body) ->
    emqx_agent_pipeline_registry:register_profile(Name, Body);
profile_create(_) ->
    {error, {missing_field, <<"name">>}}.

-spec profile_get(binary()) -> {ok, map()} | {error, not_found}.
profile_get(Name) ->
    emqx_agent_pipeline_registry:lookup_profile(Name).

-spec profile_update(binary(), map()) -> {ok, map()}.
profile_update(Name, Body) ->
    Body2 = Body#{<<"name">> => Name},
    ok = emqx_agent_pipeline_registry:register_profile(Name, Body2),
    {ok, Body2}.

-spec profile_delete(binary()) -> ok | {error, not_found}.
profile_delete(Name) ->
    case emqx_agent_pipeline_registry:lookup_profile(Name) of
        {error, not_found} -> {error, not_found};
        {ok, _} -> emqx_agent_pipeline_registry:unregister_profile(Name)
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
        ok -> {ok, Body2};
        {error, _} = Err -> Err
    end.

-spec pipeline_delete(binary()) -> ok | {error, not_found}.
pipeline_delete(Id) ->
    case emqx_agent_pipeline_registry:lookup(Id) of
        {error, not_found} -> {error, not_found};
        {ok, _} -> emqx_agent_pipeline_registry:unregister(Id)
    end.

%%--------------------------------------------------------------------
%% Internal — skill validation and dispatch
%%--------------------------------------------------------------------

do_create_skill(#{<<"type">> := Type} = Body) ->
    Schema = #{roots => [{skill, emqx_agent_schema:skill_create_type()}]},
    try hocon_tconf:check_plain(Schema, #{<<"skill">> => Body}, #{atom_key => true}) of
        #{skill := Ctx} ->
            Ctx2 = maps:put(skill_id, maps:get(id, Ctx), maps:remove(id, Ctx)),
            Mod = skill_module(Type),
            Mod:create(Ctx2)
    catch
        throw:#{field_name := <<"type">>} ->
            {error, unknown_type};
        throw:Error ->
            {error, Error}
    end;
do_create_skill(_) ->
    {error, {missing_field, <<"type">>}}.

do_destroy_skill(<<"message.publish">>, Id) -> emqx_agent_skill_publish:destroy(Id);
do_destroy_skill(<<"message.request">>, Id) -> emqx_agent_skill_mqtt_request:destroy(Id);
do_destroy_skill(<<"http">>, Id) -> emqx_agent_skill_http:destroy(Id);
do_destroy_skill(<<"kv.lookup">>, Id) -> emqx_agent_skill_kv:destroy_lookup(Id);
do_destroy_skill(<<"kv.put">>, Id) -> emqx_agent_skill_kv:destroy_put(Id);
do_destroy_skill(<<"postgresql.query">>, Id) -> emqx_agent_skill_postgresql:destroy(Id).

skill_to_map(#{type := Type} = Skill) ->
    Mod = skill_module(Type),
    Mod:to_map(Skill).

skill_module(<<"http">>) -> emqx_agent_skill_http;
skill_module(<<"message.publish">>) -> emqx_agent_skill_publish;
skill_module(<<"message.request">>) -> emqx_agent_skill_mqtt_request;
skill_module(<<"kv.lookup">>) -> emqx_agent_skill_kv;
skill_module(<<"kv.put">>) -> emqx_agent_skill_kv;
skill_module(<<"postgresql.query">>) -> emqx_agent_skill_postgresql.
