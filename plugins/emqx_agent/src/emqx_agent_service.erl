%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_agent_service).

-moduledoc """
Service layer for the agent subsystem.

Accepts raw binary-keyed maps and binary IDs from the API layer, applies
service-level checks such as dependency/in-use guards, then delegates CRUD to
emqx_agent_config. Config writes are persisted through the plugin config
boundary, where defaults and Avro-based validation are applied before runtime
reconciliation.

Returns:
  ok | {ok, term()} | {error, Reason}

Reasons include:
  not_found
  unknown_type
  already_exists
  invalid_skill
  invalid_connection
  invalid_pipeline
  pipeline_is_active
  {in_use, [binary()]}
  {missing_field, binary()}
  term()  (plugin config, Avro, schema, or reference validation errors)
""".

-export([
    %% Skills
    skill_list/0,
    skill_create/1,
    skill_get/2,
    skill_update/3,
    skill_delete/2,
    skill_statuses/0,
    %% Connections
    connection_list/0,
    connection_create/1,
    connection_get/1,
    connection_update/2,
    connection_delete/1,
    connection_start/1,
    connection_stop/1,
    connection_statuses/0,
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
    emqx_agent_config:list_skills().

-spec skill_create(map()) ->
    ok | {error, unknown_type | {missing_field, binary()} | term()}.
skill_create(Body) ->
    reconcile_skills_after(emqx_agent_config:create_skill(Body)).

-spec skill_get(binary(), binary()) -> {ok, map()} | {error, not_found}.
skill_get(Type, Id) ->
    emqx_agent_config:lookup_skill(Type, Id).

-spec skill_update(binary(), binary(), map()) ->
    {ok, map()} | {error, unknown_type | {missing_field, binary()} | not_found | term()}.
skill_update(Type, Id, Body) ->
    reconcile_skills_after(emqx_agent_config:update_skill(Type, Id, Body)).

-spec skill_delete(binary(), binary()) ->
    ok | {error, not_found | {in_use, [binary()]}}.
skill_delete(Type, Id) ->
    case emqx_agent_config:lookup_skill(Type, Id) of
        {error, not_found} ->
            {error, not_found};
        {ok, _Skill} ->
            Ref = <<Type/binary, "@", Id/binary>>,
            case pipelines_using_skill(Ref) of
                [] -> reconcile_skills_after(emqx_agent_config:delete_skill(Type, Id));
                Ids -> {error, {in_use, Ids}}
            end
    end.

-spec skill_statuses() -> map().
skill_statuses() ->
    emqx_agent_skill_registry:statuses().

%%--------------------------------------------------------------------
%% Connections
%%--------------------------------------------------------------------

-spec connection_list() -> [map()].
connection_list() ->
    emqx_agent_config:list_connections().

-spec connection_create(map()) -> ok | {error, term()}.
connection_create(Body) ->
    reconcile_connections_after(emqx_agent_config:create_connection(Body)).

-spec connection_get(binary()) -> {ok, map()} | {error, not_found}.
connection_get(ConnectionId) ->
    emqx_agent_config:lookup_connection(ConnectionId).

-spec connection_update(binary(), map()) -> {ok, map()} | {error, term()}.
connection_update(ConnectionId, Body) ->
    reconcile_connections_after(emqx_agent_config:update_connection(ConnectionId, Body)).

-spec connection_delete(binary()) -> ok | {error, not_found | {in_use, [binary()]} | term()}.
connection_delete(ConnectionId) ->
    case emqx_agent_config:lookup_connection(ConnectionId) of
        {error, not_found} ->
            {error, not_found};
        {ok, _} ->
            case skills_using_connection(ConnectionId) of
                [] ->
                    reconcile_connections_after(emqx_agent_config:delete_connection(ConnectionId));
                Ids ->
                    {error, {in_use, Ids}}
            end
    end.

-spec connection_start(binary()) -> {ok, map()} | {error, term()}.
connection_start(ConnectionId) ->
    update_connection_enable(ConnectionId, true).

-spec connection_stop(binary()) -> {ok, map()} | {error, term()}.
connection_stop(ConnectionId) ->
    update_connection_enable(ConnectionId, false).

-spec connection_statuses() -> map().
connection_statuses() ->
    maps:from_list([
        {ConnectionId, emqx_agent_skill_connections:status(Conn)}
     || #{<<"id">> := ConnectionId} = Conn <-
            emqx_agent_config:parsed_config([connections], [])
    ]).

%%--------------------------------------------------------------------
%% Pipelines
%%--------------------------------------------------------------------

-spec pipeline_list() -> [map()].
pipeline_list() ->
    emqx_agent_config:list_pipelines().

-spec pipeline_create(map()) -> ok | {error, {missing_field, binary()} | term()}.
pipeline_create(#{<<"pipeline_id">> := _} = Body) ->
    emqx_agent_config:create_pipeline(Body);
pipeline_create(_) ->
    {error, {missing_field, <<"pipeline_id">>}}.

-spec pipeline_get(binary()) -> {ok, map()} | {error, not_found}.
pipeline_get(Id) ->
    emqx_agent_config:lookup_pipeline(Id).

-spec pipeline_update(binary(), map()) -> {ok, map()} | {error, term()}.
pipeline_update(Id, Body) ->
    emqx_agent_config:update_pipeline(Id, Body).

-spec pipeline_delete(binary()) ->
    ok | {error, not_found | pipeline_is_active}.
pipeline_delete(Id) ->
    case emqx_agent_config:lookup_pipeline(Id) of
        {error, not_found} ->
            {error, not_found};
        {ok, #{<<"active">> := true}} ->
            {error, pipeline_is_active};
        {ok, _} ->
            emqx_agent_config:delete_pipeline(Id)
    end.

%%--------------------------------------------------------------------
%% Internal
%%--------------------------------------------------------------------

pipelines_using_skill(Ref) ->
    [
        maps:get(<<"pipeline_id">>, P)
     || P <- emqx_agent_config:list_pipelines(),
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

skills_using_connection(ConnectionId) ->
    [
        maps:get(<<"id">>, Skill)
     || S <- emqx_agent_config:list_skills(),
        Skill <- [unwrap_union(S)],
        #{<<"type">> := <<"postgresql__query">>, <<"resource">> := ConnectionId0} <- [Skill],
        ConnectionId0 =:= ConnectionId
    ].

reconcile_skills_after(ok) ->
    ok = emqx_agent_skill_registry:reconcile(),
    ok;
reconcile_skills_after({ok, _} = Result) ->
    ok = emqx_agent_skill_registry:reconcile(),
    Result;
reconcile_skills_after({error, _} = Error) ->
    Error.

reconcile_connections_after(ok) ->
    ok = emqx_agent_skill_connections:reconcile(),
    ok;
reconcile_connections_after({ok, _} = Result) ->
    ok = emqx_agent_skill_connections:reconcile(),
    Result;
reconcile_connections_after({error, _} = Error) ->
    Error.

update_connection_enable(ConnectionId, Enable) ->
    case emqx_agent_config:lookup_connection(ConnectionId) of
        {ok, Conn0} ->
            reconcile_connections_after(
                emqx_agent_config:update_connection(
                    ConnectionId, set_connection_enable(Conn0, Enable)
                )
            );
        {error, _} = Error ->
            Error
    end.

unwrap_union(Map) when is_map(Map), map_size(Map) =:= 1 ->
    case maps:to_list(Map) of
        [{Key, Value}] when is_binary(Key), is_map(Value) -> Value;
        _ -> Map
    end;
unwrap_union(Value) ->
    Value.

set_connection_enable(Conn, Enable) when is_map(Conn), map_size(Conn) =:= 1 ->
    case maps:to_list(Conn) of
        [{Key, Value}] when is_binary(Key), is_map(Value) ->
            #{Key => Value#{<<"enable">> => Enable}};
        _ ->
            Conn#{<<"enable">> => Enable}
    end;
set_connection_enable(Conn, Enable) ->
    Conn#{<<"enable">> => Enable}.
