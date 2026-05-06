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
%% Invoke topic:  cap/agent.delete_pipeline/<skill_id>/request
%% Reply  topic:  cap/agent.delete_pipeline/<skill_id>/response/<req_id>

-module(emqx_agent_skill_delete_pipeline).

-define(SKILL_TYPE, <<"agent.delete_pipeline">>).

-define(INPUT_SCHEMA, #{
    <<"type">> => <<"object">>,
    <<"properties">> => #{
        <<"id">> => #{<<"type">> => <<"string">>, <<"description">> => <<"Pipeline id">>}
    },
    <<"required">> => [<<"id">>]
}).

-define(OUTPUT_SCHEMA, #{
    <<"type">> => <<"object">>,
    <<"properties">> => #{
        <<"status">> => #{<<"type">> => <<"string">>, <<"enum">> => [<<"ok">>, <<"error">>]},
        <<"reason">> => #{
            <<"type">> => <<"string">>, <<"description">> => <<"Present when status=error">>
        }
    },
    <<"required">> => [<<"status">>]
}).

-export([init/0, deinit/0, create/1, destroy/1, to_map/1, handle_invoke/3]).

%%--------------------------------------------------------------------
%% Public API
%%--------------------------------------------------------------------

-spec init() -> ok.
init() ->
    emqx_agent_skill_registry:register_type(?SKILL_TYPE, ?MODULE).

-spec deinit() -> ok.
deinit() ->
    emqx_agent_skill_registry:unregister_type(?SKILL_TYPE).

-spec create(map()) -> ok | {error, term()}.
create(#{skill_id := SkillId}) ->
    emqx_agent_skill_registry:register(#{
        skill_id => SkillId,
        type => ?SKILL_TYPE,
        module => ?MODULE,
        display_name => <<"Delete Pipeline">>,
        description => <<"Delete a pipeline definition. Refused if the pipeline is active.">>,
        context => #{skill_id => SkillId},
        input_schema => ?INPUT_SCHEMA,
        output_schema => ?OUTPUT_SCHEMA
    }).

-spec destroy(binary()) -> ok.
destroy(SkillId) ->
    emqx_agent_skill_registry:unregister(?SKILL_TYPE, SkillId).

-spec to_map(map()) -> map().
to_map(#{skill_id := Id, description := Desc, input_schema := In, output_schema := Out}) ->
    #{
        <<"skill_id">> => Id,
        <<"type">> => ?SKILL_TYPE,
        <<"description">> => Desc,
        <<"input_schema">> => In,
        <<"output_schema">> => Out
    }.

%%--------------------------------------------------------------------
%% Internal
%%--------------------------------------------------------------------

handle_invoke(SkillId, _Context, Request) ->
    Args = maps:get(<<"args">>, Request, #{}),
    Result = do_delete(Args),
    reply(SkillId, Request, Result).

do_delete(#{<<"id">> := Id}) ->
    case emqx_agent_service:pipeline_delete(Id) of
        ok ->
            #{<<"status">> => <<"ok">>};
        {error, not_found} ->
            #{<<"status">> => <<"error">>, <<"reason">> => <<"pipeline not found">>};
        {error, pipeline_is_active} ->
            #{
                <<"status">> => <<"error">>,
                <<"reason">> => <<"pipeline is active; set active=false before deleting">>
            }
    end;
do_delete(_) ->
    #{<<"status">> => <<"error">>, <<"reason">> => <<"missing required field: id">>}.

reply(SkillId, Request, Data) ->
    emqx_agent_skill_helpers:publish_reply(?SKILL_TYPE, SkillId, Request, Data).
