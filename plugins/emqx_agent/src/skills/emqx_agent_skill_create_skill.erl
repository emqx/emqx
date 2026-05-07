%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% Management skill: create any skill type at runtime.
%%
%% The skill instance itself carries no configuration — only an id.
%% When invoked, the LLM supplies the full skill creation payload as args.
%% The skill delegates to emqx_agent_service:skill_create/1 and returns
%% a structured result so the LLM can detect and retry failures.
%%
%% Invoke topic:  cap/agent.create_skill/<skill_id>/request
%% Reply  topic:  cap/agent.create_skill/<skill_id>/response/<req_id>

-module(emqx_agent_skill_create_skill).

-define(SKILL_TYPE, <<"agent.create_skill">>).

-define(INPUT_SCHEMA, #{
    <<"type">> => <<"object">>,
    <<"properties">> => #{
        <<"definition">> => #{
            <<"description">> => <<"Skill definition — shape depends on the type field">>,
            <<"oneOf">> => [
                #{
                    <<"title">> => <<"message.publish">>,
                    <<"properties">> => #{
                        <<"type">> => #{
                            <<"type">> => <<"string">>, <<"const">> => <<"message.publish">>
                        },
                        <<"id">> => #{
                            <<"type">> => <<"string">>,
                            <<"description">> => <<"Unique skill instance identifier">>
                        },
                        <<"desc">> => #{
                            <<"type">> => <<"string">>,
                            <<"description">> => <<"Human-readable description shown to the LLM">>
                        },
                        <<"topic_prefix">> => #{
                            <<"type">> => <<"string">>,
                            <<"description">> =>
                                <<"All publishes are restricted to this prefix, e.g. devices/room1/">>
                        },
                        <<"payload_schema">> => #{
                            <<"type">> => <<"string">>,
                            <<"description">> =>
                                <<"Optional JSON Schema for the payload field (as a JSON string)">>
                        }
                    },
                    <<"required">> => [<<"type">>, <<"id">>, <<"desc">>, <<"topic_prefix">>]
                },
                #{
                    <<"title">> => <<"message.request">>,
                    <<"properties">> => #{
                        <<"type">> => #{
                            <<"type">> => <<"string">>, <<"const">> => <<"message.request">>
                        },
                        <<"id">> => #{<<"type">> => <<"string">>},
                        <<"desc">> => #{<<"type">> => <<"string">>},
                        <<"topic_prefix">> => #{
                            <<"type">> => <<"string">>,
                            <<"description">> =>
                                <<"Request published to prefix + agent-supplied suffix. Response arrives via MQTT 5 Response-Topic.">>
                        },
                        <<"request_payload_schema">> => #{
                            <<"type">> => <<"string">>,
                            <<"description">> =>
                                <<"Optional JSON Schema for the outgoing request payload (as a JSON string)">>
                        },
                        <<"response_schema">> => #{
                            <<"type">> => <<"string">>,
                            <<"description">> =>
                                <<"Optional JSON Schema for the incoming response payload (as a JSON string)">>
                        }
                    },
                    <<"required">> => [<<"type">>, <<"id">>, <<"desc">>, <<"topic_prefix">>]
                },
                #{
                    <<"title">> => <<"http">>,
                    <<"properties">> => #{
                        <<"type">> => #{<<"type">> => <<"string">>, <<"const">> => <<"http">>},
                        <<"id">> => #{<<"type">> => <<"string">>},
                        <<"desc">> => #{<<"type">> => <<"string">>},
                        <<"method">> => #{
                            <<"type">> => <<"string">>,
                            <<"enum">> => [
                                <<"get">>, <<"post">>, <<"put">>, <<"patch">>, <<"delete">>
                            ]
                        },
                        <<"url">> => #{
                            <<"type">> => <<"string">>,
                            <<"description">> => <<"Full URL of the endpoint">>
                        },
                        <<"headers">> => #{
                            <<"type">> => <<"object">>,
                            <<"description">> => <<"Optional HTTP headers map">>
                        },
                        <<"input_schema">> => #{
                            <<"type">> => <<"string">>,
                            <<"description">> =>
                                <<"JSON Schema for tool call arguments (as a JSON string)">>
                        },
                        <<"output_schema">> => #{
                            <<"type">> => <<"string">>,
                            <<"description">> =>
                                <<"JSON Schema for the HTTP response body (as a JSON string)">>
                        }
                    },
                    <<"required">> => [
                        <<"type">>,
                        <<"id">>,
                        <<"desc">>,
                        <<"method">>,
                        <<"url">>,
                        <<"input_schema">>,
                        <<"output_schema">>
                    ]
                },
                #{
                    <<"title">> => <<"postgresql.query">>,
                    <<"properties">> => #{
                        <<"type">> => #{
                            <<"type">> => <<"string">>, <<"const">> => <<"postgresql.query">>
                        },
                        <<"id">> => #{<<"type">> => <<"string">>},
                        <<"desc">> => #{<<"type">> => <<"string">>},
                        <<"query">> => #{
                            <<"type">> => <<"string">>,
                            <<"description">> =>
                                <<
                                    "SQL using $1 $2 … positional placeholders. "
                                    "arg_keys maps named args to positions in order."
                                >>
                        },
                        <<"arg_keys">> => #{
                            <<"type">> => <<"array">>,
                            <<"items">> => #{<<"type">> => <<"string">>},
                            <<"description">> =>
                                <<"Ordered list of arg names, e.g. [\"device_id\",\"value\"] maps to $1, $2">>
                        },
                        <<"input_schema">> => #{
                            <<"type">> => <<"string">>,
                            <<"description">> =>
                                <<"JSON Schema for tool call arguments (as a JSON string)">>
                        },
                        <<"output_schema">> => #{
                            <<"type">> => <<"string">>,
                            <<"description">> =>
                                <<"JSON Schema for query result rows (as a JSON string)">>
                        }
                    },
                    <<"required">> => [
                        <<"type">>,
                        <<"id">>,
                        <<"desc">>,
                        <<"query">>,
                        <<"input_schema">>,
                        <<"output_schema">>
                    ]
                }
            ]
        }
    },
    <<"required">> => [<<"definition">>]
}).

-define(OUTPUT_SCHEMA, #{
    <<"type">> => <<"object">>,
    <<"properties">> => #{
        <<"status">> => #{<<"type">> => <<"string">>, <<"enum">> => [<<"ok">>, <<"error">>]},
        <<"skill_id">> => #{<<"type">> => <<"string">>},
        <<"type">> => #{<<"type">> => <<"string">>},
        <<"reason">> => #{
            <<"type">> => <<"string">>,
            <<"description">> =>
                <<"Present when status=error. Describes what went wrong so the caller can fix and retry.">>
        },
        <<"details">> => #{
            <<"description">> => <<"Additional validation error context when available">>
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
        display_name => <<"Create Skill">>,
        description =>
            <<"Create or overwrite a skill (upsert). Types: message.publish, message.request, http, postgresql.query">>,
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
    Args = maps:get(<<"definition">>, maps:get(<<"args">>, Request, #{}), #{}),
    Result =
        case emqx_agent_service:skill_create(Args) of
            ok ->
                #{
                    <<"status">> => <<"ok">>,
                    <<"skill_id">> => maps:get(<<"id">>, Args, <<>>),
                    <<"type">> => maps:get(<<"type">>, Args, <<>>)
                };
            {error, unknown_type} ->
                #{
                    <<"status">> => <<"error">>,
                    <<"reason">> => <<"unknown skill type">>,
                    <<"details">> =>
                        <<"valid types: message.publish, message.request, http, postgresql.query">>
                };
            {error, Reason} ->
                #{
                    <<"status">> => <<"error">>,
                    <<"reason">> => emqx_agent_skill_helpers:format_error(Reason)
                }
        end,
    reply(SkillId, Request, Result).

reply(SkillId, Request, Data) ->
    emqx_agent_skill_helpers:publish_reply(?SKILL_TYPE, SkillId, Request, Data).
