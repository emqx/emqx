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
%% Invoke topic:  cap/invoke/agent.create_skill/<skill_id>
%% Reply  topic:  cap/reply/<req_id>

-module(emqx_agent_skill_create_skill).

-include_lib("emqx/include/emqx_hooks.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").

-define(SKILL_TYPE, <<"agent.create_skill">>).
-define(REPLY_TOPIC_PREFIX, <<"cap/reply/">>).

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
                            <<"type">> => <<"object">>,
                            <<"description">> => <<"Optional JSON Schema for the payload field">>
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
                            <<"type">> => <<"object">>,
                            <<"description">> =>
                                <<"Optional JSON Schema for the outgoing request payload">>
                        },
                        <<"response_schema">> => #{
                            <<"type">> => <<"object">>,
                            <<"description">> =>
                                <<"Optional JSON Schema for the incoming response payload">>
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
                            <<"type">> => <<"object">>,
                            <<"description">> => <<"JSON Schema for tool call arguments">>
                        },
                        <<"output_schema">> => #{
                            <<"type">> => <<"object">>,
                            <<"description">> => <<"JSON Schema for the HTTP response body">>
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
                    <<"title">> => <<"kv.lookup">>,
                    <<"properties">> => #{
                        <<"type">> => #{<<"type">> => <<"string">>, <<"const">> => <<"kv.lookup">>},
                        <<"id">> => #{<<"type">> => <<"string">>},
                        <<"desc">> => #{<<"type">> => <<"string">>},
                        <<"data_schema">> => #{
                            <<"type">> => <<"object">>,
                            <<"description">> => <<"JSON Schema for the stored value">>
                        }
                    },
                    <<"required">> => [<<"type">>, <<"id">>, <<"desc">>, <<"data_schema">>]
                },
                #{
                    <<"title">> => <<"kv.put">>,
                    <<"properties">> => #{
                        <<"type">> => #{<<"type">> => <<"string">>, <<"const">> => <<"kv.put">>},
                        <<"id">> => #{<<"type">> => <<"string">>},
                        <<"desc">> => #{<<"type">> => <<"string">>},
                        <<"data_schema">> => #{
                            <<"type">> => <<"object">>,
                            <<"description">> => <<"JSON Schema for the value to store">>
                        }
                    },
                    <<"required">> => [<<"type">>, <<"id">>, <<"desc">>, <<"data_schema">>]
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
                            <<"type">> => <<"object">>,
                            <<"description">> => <<"JSON Schema for tool call arguments">>
                        },
                        <<"output_schema">> => #{
                            <<"type">> => <<"object">>,
                            <<"description">> => <<"JSON Schema for query result rows">>
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
        display_name => <<"Create Skill">>,
        description =>
            <<"Create a new agent skill (message.publish, message.request, http, kv.lookup, kv.put, postgresql.query)">>,
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
    #message{topic = <<"cap/invoke/agent.create_skill/", SkillId/binary>>, payload = Payload} = Msg
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
                        <<"valid types: message.publish, message.request, http, kv.lookup, kv.put, postgresql.query">>
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
