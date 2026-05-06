%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% Management skill: list or introspect pipeline definitions.
%%
%% Args (all optional):
%%   id — pipeline id. Omit to list all pipelines.
%%
%% Invoke topic:  cap/agent.query_pipelines/<skill_id>/request
%% Reply  topic:  cap/agent.query_pipelines/<skill_id>/response/<req_id>

-module(emqx_agent_skill_query_pipelines).

-define(SKILL_TYPE, <<"agent.query_pipelines">>).

-define(INPUT_SCHEMA, #{
    <<"type">> => <<"object">>,
    <<"properties">> => #{
        <<"id">> => #{
            <<"type">> => <<"string">>,
            <<"description">> => <<"Pipeline id. Omit to list all pipelines.">>
        }
    }
}).

-define(OUTPUT_SCHEMA, #{
    <<"type">> => <<"object">>,
    <<"properties">> => #{
        <<"status">> => #{<<"type">> => <<"string">>, <<"enum">> => [<<"ok">>, <<"error">>]},
        <<"items">> => #{
            <<"type">> => <<"array">>, <<"description">> => <<"Present on list result">>
        },
        <<"item">> => #{
            <<"type">> => <<"object">>, <<"description">> => <<"Present on single-get result">>
        },
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
        display_name => <<"Query Pipelines">>,
        description => <<"List all pipeline definitions or look up a specific one by id">>,
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
    Result = query(Args),
    reply(SkillId, Request, Result).

query(#{<<"id">> := Id}) ->
    case emqx_agent_service:pipeline_get(Id) of
        {ok, Pipeline} ->
            #{<<"status">> => <<"ok">>, <<"item">> => Pipeline};
        {error, not_found} ->
            #{<<"status">> => <<"error">>, <<"reason">> => <<"not found">>}
    end;
query(_) ->
    Items = emqx_agent_service:pipeline_list(),
    #{<<"status">> => <<"ok">>, <<"items">> => Items}.

reply(SkillId, Request, Data) ->
    emqx_agent_skill_helpers:publish_reply(?SKILL_TYPE, SkillId, Request, Data).
