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
  invalid_tool
  invalid_connection
  invalid_pipeline
  pipeline_is_active
  {in_use, [binary()]}
  {missing_field, binary()}
  term()  (plugin config, Avro, schema, or reference validation errors)
""".

-export([
    %% Tools
    tool_list/0,
    tool_create/1,
    tool_get/2,
    tool_update/3,
    tool_delete/2,
    tool_statuses/0,
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
    pipeline_delete/1,
    pipeline_update_retain_steps/2,
    pipeline_step_delete/2,
    pipeline_step_insert/4,
    pipeline_step_update/3
]).

%%--------------------------------------------------------------------
%% Tools
%%--------------------------------------------------------------------

-spec tool_list() -> [map()].
tool_list() ->
    emqx_agent_config:list_tools().

-spec tool_create(map()) ->
    ok | {error, unknown_type | {missing_field, binary()} | term()}.
tool_create(Body) ->
    reconcile_tools_after(emqx_agent_config:create_tool(Body)).

-spec tool_get(binary(), binary()) -> {ok, map()} | {error, not_found}.
tool_get(Type, Id) ->
    emqx_agent_config:lookup_tool(Type, Id).

-spec tool_update(binary(), binary(), map()) ->
    {ok, map()} | {error, unknown_type | {missing_field, binary()} | not_found | term()}.
tool_update(Type, Id, Body) ->
    reconcile_tools_after(emqx_agent_config:update_tool(Type, Id, Body)).

-spec tool_delete(binary(), binary()) ->
    ok | {error, not_found | {in_use, [binary()]}}.
tool_delete(Type, Id) ->
    case emqx_agent_config:lookup_tool(Type, Id) of
        {error, not_found} ->
            {error, not_found};
        {ok, _Tool} ->
            Ref = <<Type/binary, "@", Id/binary>>,
            case pipelines_using_tool(Ref) of
                [] -> reconcile_tools_after(emqx_agent_config:delete_tool(Type, Id));
                Ids -> {error, {in_use, Ids}}
            end
    end.

-spec tool_statuses() -> map().
tool_statuses() ->
    emqx_agent_tool_registry:statuses().

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
            case tools_using_connection(ConnectionId) of
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
        {ConnectionId, emqx_agent_tool_connections:status(Conn)}
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

%% Update a pipeline's non-step fields while keeping the existing steps list.
%% The LLM cannot accidentally replace or clear steps via this tool.
-spec pipeline_update_retain_steps(binary(), map()) ->
    {ok, map()} | {error, term()}.
pipeline_update_retain_steps(Id, Body) ->
    case emqx_agent_config:lookup_pipeline(Id) of
        {error, not_found} ->
            {error, not_found};
        {ok, Existing} ->
            Steps = maps:get(<<"steps">>, Existing, []),
            Update = maps:without([<<"steps">>, <<"pipeline_id">>], Body),
            Merged = maps:merge(Existing, Update#{<<"steps">> => Steps}),
            emqx_agent_config:update_pipeline(Id, Merged)
    end.

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

-spec pipeline_step_delete(binary(), binary()) ->
    {ok, map()} | {error, not_found | step_not_found}.
pipeline_step_delete(PipelineId, StepId) ->
    mutate_steps(PipelineId, fun(Steps) ->
        case remove_step(StepId, Steps) of
            {ok, NewSteps} -> {ok, NewSteps};
            {error, not_found} -> {error, step_not_found}
        end
    end).

-spec pipeline_step_insert(binary(), map(), first | last | {'after', binary()}, map()) ->
    {ok, map()} | {error, not_found | step_not_found | duplicate_step_id | term()}.
pipeline_step_insert(PipelineId, Step, Position, _Opts) ->
    StepId = maps:get(<<"id">>, Step, undefined),
    mutate_steps(PipelineId, fun(Steps) ->
        case StepId =/= undefined andalso step_exists(StepId, Steps) of
            true ->
                {error, duplicate_step_id};
            false ->
                insert_step_at(Position, Step, Steps)
        end
    end).

-spec pipeline_step_update(binary(), binary(), map()) ->
    {ok, map()} | {error, not_found | step_not_found | term()}.
pipeline_step_update(PipelineId, StepId, Step) ->
    mutate_steps(PipelineId, fun(Steps) ->
        case find_step(StepId, Steps) of
            {ok, Existing} ->
                Merged = maps:merge(Existing, Step#{<<"id">> => StepId}),
                replace_step(StepId, Merged, Steps);
            {error, not_found} ->
                {error, step_not_found}
        end
    end).

%%--------------------------------------------------------------------
%% Internal
%%--------------------------------------------------------------------

pipelines_using_tool(Ref) ->
    [
        maps:get(<<"pipeline_id">>, P)
     || P <- emqx_agent_config:list_pipelines(),
        tool_ref_in_pipeline(Ref, P)
    ].

tool_ref_in_pipeline(Ref, Pipeline) ->
    Steps = maps:get(<<"steps">>, Pipeline, []),
    lists:any(fun(Step) -> tool_ref_in_step(Ref, Step) end, Steps).

tool_ref_in_step(Ref, #{<<"type">> := <<"call_tool">>} = Step) ->
    maps:get(<<"tool">>, Step, undefined) =:= Ref;
tool_ref_in_step(Ref, #{<<"type">> := <<"llm_loop">>} = Step) ->
    lists:member(Ref, maps:get(<<"tools">>, Step, []));
tool_ref_in_step(_Ref, _Step) ->
    false.

tools_using_connection(ConnectionId) ->
    [
        maps:get(<<"id">>, Tool)
     || Tool <- emqx_agent_config:list_tools(),
        #{<<"type">> := <<"postgresql__query">>, <<"resource">> := ConnectionId0} <- [Tool],
        ConnectionId0 =:= ConnectionId
    ].

reconcile_tools_after(ok) ->
    ok = emqx_agent_tool_registry:reconcile(),
    ok;
reconcile_tools_after({ok, _} = Result) ->
    ok = emqx_agent_tool_registry:reconcile(),
    Result;
reconcile_tools_after({error, _} = Error) ->
    Error.

reconcile_connections_after(ok) ->
    ok = emqx_agent_tool_connections:reconcile(),
    ok;
reconcile_connections_after({ok, _} = Result) ->
    ok = emqx_agent_tool_connections:reconcile(),
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

mutate_steps(PipelineId, MutateFun) ->
    case emqx_agent_config:lookup_pipeline(PipelineId) of
        {error, not_found} ->
            {error, not_found};
        {ok, Pipeline} ->
            Steps = maps:get(<<"steps">>, Pipeline, []),
            case MutateFun(Steps) of
                {ok, NewSteps} ->
                    emqx_agent_config:update_pipeline(
                        PipelineId, Pipeline#{<<"steps">> => NewSteps}
                    );
                {error, _} = Error ->
                    Error
            end
    end.

remove_step(StepId, Steps) ->
    remove_step(StepId, Steps, []).

remove_step(_StepId, [], _Acc) ->
    {error, not_found};
remove_step(StepId, [Step | Rest], Acc) ->
    case step_id(Step) =:= StepId of
        true -> {ok, lists:reverse(Acc) ++ Rest};
        false -> remove_step(StepId, Rest, [Step | Acc])
    end.

insert_step_at(first, Step, Steps) ->
    {ok, [Step | Steps]};
insert_step_at(last, Step, Steps) ->
    {ok, Steps ++ [Step]};
insert_step_at({'after', AfterId}, Step, Steps) ->
    insert_after(AfterId, Step, Steps, []).

insert_after(_AfterId, _Step, [], _Acc) ->
    {error, step_not_found};
insert_after(AfterId, Step, [S | Rest], Acc) ->
    case step_id(S) =:= AfterId of
        true -> {ok, lists:reverse(Acc) ++ [S, Step | Rest]};
        false -> insert_after(AfterId, Step, Rest, [S | Acc])
    end.

replace_step(StepId, Step, Steps) ->
    replace_step(StepId, Step, Steps, []).

replace_step(_StepId, _Step, [], _Acc) ->
    {error, not_found};
replace_step(StepId, Step, [S | Rest], Acc) ->
    case step_id(S) =:= StepId of
        true -> {ok, lists:reverse(Acc) ++ [Step | Rest]};
        false -> replace_step(StepId, Step, Rest, [S | Acc])
    end.

find_step(StepId, Steps) ->
    case lists:filter(fun(S) -> step_id(S) =:= StepId end, Steps) of
        [Step | _] -> {ok, Step};
        [] -> {error, not_found}
    end.

step_exists(StepId, Steps) ->
    lists:any(fun(Step) -> step_id(Step) =:= StepId end, Steps).

step_id(Step) ->
    maps:get(<<"id">>, Step, undefined).

set_connection_enable(Conn, Enable) when is_map(Conn), map_size(Conn) =:= 1 ->
    case maps:to_list(Conn) of
        [{Key, Value}] when is_binary(Key), is_map(Value) ->
            #{Key => Value#{<<"enable">> => Enable}};
        _ ->
            Conn#{<<"enable">> => Enable}
    end;
set_connection_enable(Conn, Enable) ->
    Conn#{<<"enable">> => Enable}.
