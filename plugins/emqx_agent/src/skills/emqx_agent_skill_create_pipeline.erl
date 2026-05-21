%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% Management skill: create a pipeline definition at runtime.
%%
%% The skill instance carries no configuration — only an id.
%% When invoked, the LLM supplies the full pipeline payload as args.
%% The skill enforces active=false unconditionally — this flag is not
%% exposed in the input schema so the LLM cannot accidentally activate
%% an untested pipeline.
%%
%% Invoke topic:  $cap/agent__create_pipeline/<skill_id>/request/<req_id>
%% Reply  topic:  $cap/agent__create_pipeline/<skill_id>/response/<req_id>

-module(emqx_agent_skill_create_pipeline).

-behaviour(emqx_agent_skill).

-define(SKILL_TYPE, <<"agent__create_pipeline">>).

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
        display_name => <<"Create Pipeline">>,
        description =>
            <<"Create or overwrite a pipeline definition (upsert). Registered as inactive draft; activate via the API or admin UI.">>,
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

handle_invoke(_Context, Request) ->
    emqx_agent_builder_tool_server:call(fun() -> do_handle_invoke(Request) end).

do_handle_invoke(Request) ->
    Args = maps:get(<<"args">>, Request, #{}),
    %% Enforce active=false unconditionally — not exposed to the LLM.
    Body = Args#{<<"active">> => false},
    case emqx_agent_service:pipeline_create(Body) of
        ok ->
            {ok, #{
                <<"pipeline_id">> => maps:get(<<"pipeline_id">>, Args, <<>>),
                <<"active">> => false
            }};
        {error, Reason} ->
            {error, emqx_agent_skill_helpers:format_error(Reason)}
    end.

%%--------------------------------------------------------------------
%% Internal
%%--------------------------------------------------------------------

input_schema() ->
    emqx_agent_schema_oai_tool_converter:to_json_schema([pipelines, items]).
