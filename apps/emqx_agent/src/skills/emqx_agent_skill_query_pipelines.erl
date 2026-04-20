%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% Management skill: list or introspect pipeline definitions.
%%
%% Args (all optional):
%%   id — pipeline id. Omit to list all pipelines.
%%
%% Invoke topic:  cap/invoke/agent.query_pipelines/<skill_id>
%% Reply  topic:  cap/reply/<req_id>

-module(emqx_agent_skill_query_pipelines).

-include_lib("emqx/include/emqx_hooks.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").

-define(SKILL_TYPE, <<"agent.query_pipelines">>).
-define(REPLY_TOPIC_PREFIX, <<"cap/reply/">>).

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
%% Hook callback
%%--------------------------------------------------------------------

on_message_publish(
    #message{topic = <<"cap/invoke/agent.query_pipelines/", SkillId/binary>>, payload = Payload} =
        Msg
) ->
    handle_invoke(SkillId, Payload),
    {ok, Msg};
on_message_publish(Msg) ->
    {ok, Msg}.

%%--------------------------------------------------------------------
%% Internal
%%--------------------------------------------------------------------

handle_invoke(SkillId, Payload) ->
    Request = emqx_utils_json:decode(Payload),
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
    ReqId = maps:get(<<"req_id">>, Request),
    Reply = emqx_agent_skill_helpers:correlation(Request, #{
        <<"skill">> => #{<<"type">> => ?SKILL_TYPE, <<"id">> => SkillId},
        <<"frame">> => <<"unary">>,
        <<"data">> => Data
    }),
    ReplyTopic = <<?REPLY_TOPIC_PREFIX/binary, ReqId/binary>>,
    Msg = emqx_message:make(SkillId, ?QOS_0, ReplyTopic, emqx_utils_json:encode(Reply)),
    _ = emqx_broker:publish(Msg),
    ok.
