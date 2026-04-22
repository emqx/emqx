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
%% Invoke topic:  cap/agent.query_skills/<skill_id>/request
%% Reply  topic:  cap/agent.query_skills/<skill_id>/response/<req_id>

-module(emqx_agent_skill_query_skills).

-include_lib("emqx/include/emqx_hooks.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").

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

-export([init/0, deinit/0, create/1, destroy/1, to_map/1]).
-export([on_message_publish/1]).

%%--------------------------------------------------------------------
%% Public API
%%--------------------------------------------------------------------

-spec init() -> ok.
init() ->
    _ = emqx_hooks:add('message.publish', {?MODULE, on_message_publish, []}, ?HP_LOWEST),
    ok.

-spec deinit() -> ok.
deinit() ->
    emqx_hooks:del('message.publish', {?MODULE, on_message_publish}),
    ok.

-spec create(map()) -> ok.
create(#{skill_id := SkillId}) ->
    emqx_agent_skill_registry:register(#{
        skill_id => SkillId,
        type => ?SKILL_TYPE,
        display_name => <<"Query Skills">>,
        description => <<"List all registered skills or look up a specific one by type and id">>,
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
%% Hook callback
%%--------------------------------------------------------------------

on_message_publish(Msg) ->
    emqx_agent_skill_helpers:if_skill_request(
        ?SKILL_TYPE,
        fun(SkillId, #message{payload = Payload}) ->
            handle_invoke(SkillId, Payload)
        end,
        Msg
    ).

%%--------------------------------------------------------------------
%% Internal
%%--------------------------------------------------------------------

handle_invoke(SkillId, Payload) ->
    Request = emqx_utils_json:decode(Payload),
    Args = maps:get(<<"args">>, Request, #{}),
    Result = query(Args),
    reply(SkillId, Request, Result).

query(#{<<"type">> := Type, <<"id">> := Id}) ->
    case emqx_agent_service:skill_get(Type, Id) of
        {ok, Skill} ->
            #{<<"status">> => <<"ok">>, <<"item">> => Skill};
        {error, not_found} ->
            #{<<"status">> => <<"error">>, <<"reason">> => <<"not found">>}
    end;
query(#{<<"type">> := Type}) ->
    All = emqx_agent_service:skill_list(),
    Items = [S || S <- All, maps:get(<<"type">>, S, undefined) =:= Type],
    #{<<"status">> => <<"ok">>, <<"items">> => Items};
query(_) ->
    Items = emqx_agent_service:skill_list(),
    #{<<"status">> => <<"ok">>, <<"items">> => Items}.

reply(SkillId, Request, Data) ->
    emqx_agent_skill_helpers:publish_reply(?SKILL_TYPE, SkillId, Request, Data).
