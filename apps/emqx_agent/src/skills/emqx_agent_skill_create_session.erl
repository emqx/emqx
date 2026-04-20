%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% Management skill: create a session profile at runtime.
%%
%% The skill instance carries no configuration — only an id.
%% When invoked, the LLM supplies the full session profile payload as args.
%% The skill delegates to emqx_agent_service:profile_create/1 and returns
%% a structured result so the LLM can detect and retry failures.
%%
%% Invoke topic:  cap/invoke/agent.create_session/<skill_id>
%% Reply  topic:  cap/reply/<req_id>

-module(emqx_agent_skill_create_session).

-include_lib("emqx/include/emqx_hooks.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").

-define(SKILL_TYPE, <<"agent.create_session">>).
-define(REPLY_TOPIC_PREFIX, <<"cap/reply/">>).

-define(INPUT_SCHEMA, #{
    <<"type">> => <<"object">>,
    <<"properties">> => #{
        <<"name">> => #{
            <<"type">> => <<"string">>,
            <<"description">> =>
                <<"Unique profile name, used to reference this profile in llm_loop steps">>
        },
        <<"api_key">> => #{
            <<"type">> => <<"string">>, <<"description">> => <<"LLM provider API key">>
        },
        <<"base_url">> => #{
            <<"type">> => <<"string">>,
            <<"description">> => <<"LLM API base URL, e.g. https://api.openai.com/v1">>
        },
        <<"output_schema">> => #{
            <<"type">> => <<"object">>,
            <<"description">> => <<"Optional JSON Schema constraining structured output">>
        }
    },
    <<"required">> => [<<"name">>, <<"api_key">>, <<"base_url">>]
}).

-define(OUTPUT_SCHEMA, #{
    <<"type">> => <<"object">>,
    <<"properties">> => #{
        <<"status">> => #{<<"type">> => <<"string">>, <<"enum">> => [<<"ok">>, <<"error">>]},
        <<"name">> => #{<<"type">> => <<"string">>},
        <<"reason">> => #{
            <<"type">> => <<"string">>,
            <<"description">> =>
                <<"Present when status=error. Describes what went wrong so the caller can fix and retry.">>
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
        display_name => <<"Create Session Profile">>,
        description =>
            <<"Create a new LLM session profile (api_key, base_url)">>,
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
    #message{topic = <<"cap/invoke/agent.create_session/", SkillId/binary>>, payload = Payload} =
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
    Result =
        case emqx_agent_service:profile_create(Args) of
            ok ->
                #{
                    <<"status">> => <<"ok">>,
                    <<"name">> => maps:get(<<"name">>, Args, <<>>)
                };
            {error, Reason} ->
                #{
                    <<"status">> => <<"error">>,
                    <<"reason">> => emqx_agent_skill_helpers:format_error(Reason)
                }
        end,
    reply(SkillId, Request, Result).

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
