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
%% Invoke topic:  cap/invoke/agent.create_pipeline/<skill_id>
%% Reply  topic:  cap/reply/<req_id>

-module(emqx_agent_skill_create_pipeline).

-include_lib("emqx/include/emqx_hooks.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").

-define(SKILL_TYPE, <<"agent.create_pipeline">>).
-define(REPLY_TOPIC_PREFIX, <<"cap/reply/">>).

-define(STEP_SCHEMA, #{
    <<"oneOf">> => [
        #{
            <<"title">> => <<"call_skill">>,
            <<"type">> => <<"object">>,
            <<"properties">> => #{
                <<"id">> => #{
                    <<"type">> => <<"string">>,
                    <<"description">> => <<"Unique step identifier within this pipeline">>
                },
                <<"type">> => #{<<"type">> => <<"string">>, <<"const">> => <<"call_skill">>},
                <<"skill">> => #{
                    <<"type">> => <<"string">>,
                    <<"description">> =>
                        <<"Skill ref as type@skill_id, e.g. message.publish@my-publisher">>
                },
                <<"args">> => #{
                    <<"type">> => <<"object">>,
                    <<"description">> =>
                        <<"Argument map. Values starting with $. are resolved from pipeline context">>
                },
                <<"result_path">> => #{
                    <<"type">> => <<"string">>,
                    <<"description">> =>
                        <<"Context path to write the skill result, e.g. $.notify_result">>
                }
            },
            <<"required">> => [<<"id">>, <<"type">>, <<"skill">>]
        },
        #{
            <<"title">> => <<"llm_loop">>,
            <<"type">> => <<"object">>,
            <<"properties">> => #{
                <<"id">> => #{<<"type">> => <<"string">>},
                <<"type">> => #{<<"type">> => <<"string">>, <<"const">> => <<"llm_loop">>},
                <<"session_profile">> => #{
                    <<"type">> => <<"string">>,
                    <<"description">> => <<"Name of a registered session profile">>
                },
                <<"stop_on_finish">> => #{
                    <<"type">> => <<"boolean">>,
                    <<"description">> =>
                        <<"true = ephemeral session (default), false = persistent across triggers">>
                },
                <<"tools">> => #{
                    <<"type">> => <<"array">>,
                    <<"items">> => #{<<"type">> => <<"string">>},
                    <<"description">> =>
                        <<"Skill refs available to the LLM, e.g. [\"message.publish@my-pub\"]">>
                },
                <<"input">> => #{
                    <<"type">> => <<"object">>,
                    <<"description">> =>
                        <<"Map of input keys to context paths or literals, e.g. {\"event\": \"$.event\"}">>
                },
                <<"set_result_schema">> => #{
                    <<"type">> => <<"object">>,
                    <<"description">> =>
                        <<"Optional JSON Schema for structured output via built-in set_result tool">>
                },
                <<"result_path">> => #{
                    <<"type">> => <<"string">>,
                    <<"description">> => <<"Context path to write the LLM result, e.g. $.analysis">>
                }
            },
            <<"required">> => [<<"id">>, <<"type">>, <<"session_profile">>]
        },
        #{
            <<"title">> => <<"wait_for_event">>,
            <<"type">> => <<"object">>,
            <<"properties">> => #{
                <<"id">> => #{<<"type">> => <<"string">>},
                <<"type">> => #{<<"type">> => <<"string">>, <<"const">> => <<"wait_for_event">>},
                <<"topic">> => #{
                    <<"type">> => <<"string">>,
                    <<"description">> =>
                        <<"MQTT topic filter to wait on, e.g. evt/cloud/incident.updated">>
                },
                <<"where">> => #{
                    <<"type">> => <<"string">>,
                    <<"description">> =>
                        <<"Optional filter expression, e.g. data.incident_id == $.triage.incident_id">>
                },
                <<"result_path">> => #{
                    <<"type">> => <<"string">>,
                    <<"description">> => <<"Context path to write the matched event payload">>
                }
            },
            <<"required">> => [<<"id">>, <<"type">>, <<"topic">>]
        },
        #{
            <<"title">> => <<"break">>,
            <<"type">> => <<"object">>,
            <<"properties">> => #{
                <<"id">> => #{<<"type">> => <<"string">>},
                <<"type">> => #{<<"type">> => <<"string">>, <<"const">> => <<"break">>},
                <<"path">> => #{
                    <<"type">> => <<"string">>,
                    <<"description">> =>
                        <<"JSONPath into context to evaluate, e.g. $.triage.should_escalate">>
                },
                <<"not">> => #{
                    <<"type">> => <<"boolean">>,
                    <<"description">> => <<"Negate the condition (default false)">>
                },
                <<"eq">> => #{<<"description">> => <<"Value to compare against (default true)">>}
            },
            <<"required">> => [<<"id">>, <<"type">>, <<"path">>]
        }
    ]
}).

-define(INPUT_SCHEMA, #{
    <<"type">> => <<"object">>,
    <<"properties">> => #{
        <<"pipeline_id">> => #{
            <<"type">> => <<"string">>, <<"description">> => <<"Unique pipeline identifier">>
        },
        <<"trigger">> => #{
            <<"type">> => <<"object">>,
            <<"properties">> => #{
                <<"topic">> => #{
                    <<"type">> => <<"string">>,
                    <<"description">> =>
                        <<"MQTT topic filter. Wildcards + and # supported. Must start with evt/.">>
                }
            },
            <<"required">> => [<<"topic">>]
        },
        <<"steps">> => #{
            <<"type">> => <<"array">>,
            <<"description">> => <<"Ordered list of pipeline steps">>,
            <<"items">> => ?STEP_SCHEMA
        }
    },
    <<"required">> => [<<"pipeline_id">>, <<"trigger">>, <<"steps">>]
}).

-define(OUTPUT_SCHEMA, #{
    <<"type">> => <<"object">>,
    <<"properties">> => #{
        <<"status">> => #{<<"type">> => <<"string">>, <<"enum">> => [<<"ok">>, <<"error">>]},
        <<"pipeline_id">> => #{<<"type">> => <<"string">>},
        <<"active">> => #{<<"type">> => <<"boolean">>},
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
        display_name => <<"Create Pipeline">>,
        description =>
            <<"Create a new pipeline definition (registered as inactive draft; activate via the API or a separate step)">>,
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
    #message{topic = <<"cap/invoke/agent.create_pipeline/", SkillId/binary>>, payload = Payload} =
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
    %% Enforce active=false unconditionally — not exposed to the LLM.
    Body = Args#{<<"active">> => false},
    Result =
        case emqx_agent_service:pipeline_create(Body) of
            ok ->
                #{
                    <<"status">> => <<"ok">>,
                    <<"pipeline_id">> => maps:get(<<"pipeline_id">>, Args, <<>>),
                    <<"active">> => false
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
