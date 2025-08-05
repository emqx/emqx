%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_ai_completion_provider_mock).

-export([start_link/2, stop/0]).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

start_link(Port, Handler) ->
    maybe
        {ok, _Pid} ?= emqx_utils_http_test_server:start_link(Port, "/[...]"),
        ok = set_handler(Handler),
        ok
    end.

stop() ->
    emqx_utils_http_test_server:stop().

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

set_handler(openai_chat_completion) ->
    set_handler(fun openai_chat_completion/2);
set_handler(anthropic_messages) ->
    set_handler(fun anthropic_messages/2);
set_handler(openai_models) ->
    set_handler(fun openai_models/2);
set_handler(anthropic_models) ->
    set_handler(fun anthropic_models/2);
set_handler(anthropic_models_paginated) ->
    set_handler(fun anthropic_models_paginated/2);
set_handler(Fun) ->
    emqx_utils_http_test_server:set_handler(Fun).

openai_chat_completion(#{path := <<"/v1/responses">>} = Req0, State) ->
    {ok, RawBody, Req1} = cowboy_req:read_body(Req0),
    Body = emqx_utils_json:decode(RawBody),
    #{
        <<"input">> := _,
        <<"instructions">> := _,
        <<"model">> := _
    } =
        Body,
    %% https://platform.openai.com/docs/api-reference/responses/create
    Data =
        #{
            <<"id">> => <<"resp_67ccd2bed1ec8190b14f964abc0542670bb6a6b452d3795b">>,
            <<"object">> => <<"response">>,
            <<"created_at">> => 1741476542,
            <<"status">> => <<"completed">>,
            <<"error">> => null,
            <<"incomplete_details">> => null,
            <<"instructions">> => null,
            <<"max_output_tokens">> => null,
            <<"model">> => <<"gpt-4.1-2025-04-14">>,
            <<"output">> => [
                #{
                    <<"type">> => <<"message">>,
                    <<"id">> => <<"msg_67ccd2bf17f0819081ff3bb2cf6508e60bb6a6b452d3795b">>,
                    <<"status">> => <<"completed">>,
                    <<"role">> => <<"assistant">>,
                    <<"content">> => [
                        #{
                            <<"type">> => <<"output_text">>,
                            <<"text">> => <<"some completion">>,
                            <<"annotations">> => []
                        }
                    ]
                }
            ],
            <<"parallel_tool_calls">> => true,
            <<"previous_response_id">> => null,
            <<"reasoning">> => #{
                <<"effort">> => null,
                <<"summary">> => null
            },
            <<"store">> => true,
            <<"temperature">> => 1.0,
            <<"text">> => #{
                <<"format">> => #{
                    <<"type">> => <<"text">>
                }
            },
            <<"tool_choice">> => <<"auto">>,
            <<"tools">> => [],
            <<"top_p">> => 1.0,
            <<"truncation">> => <<"disabled">>,
            <<"usage">> => #{
                <<"input_tokens">> => 36,
                <<"input_tokens_details">> => #{
                    <<"cached_tokens">> => 0
                },
                <<"output_tokens">> => 87,
                <<"output_tokens_details">> => #{
                    <<"reasoning_tokens">> => 0
                },
                <<"total_tokens">> => 123
            },
            <<"user">> => null,
            <<"metadata">> => #{}
        },
    reply_ok(Data, Req1, State).

anthropic_messages(#{path := <<"/v1/messages">>} = Req0, State) ->
    {ok, RawBody, Req1} = cowboy_req:read_body(Req0),
    Body = emqx_utils_json:decode(RawBody),
    #{
        <<"messages">> :=
            [
                #{<<"content">> := _, <<"role">> := <<"user">>}
            ],
        <<"system">> := _,
        <<"model">> := _
    } =
        Body,
    %% https://docs.anthropic.com/en/api/messages
    Data = #{
        <<"content">> => [
            #{
                <<"text">> => <<"some completion">>,
                <<"type">> => <<"text">>
            }
        ],
        <<"id">> => <<"msg_013Zva2CMHLNnXjNJJKqJ2EF">>,
        <<"model">> => <<"claude-3-7-sonnet-20250219">>,
        <<"role">> => <<"assistant">>,
        <<"stop_reason">> => <<"end_turn">>,
        <<"stop_sequence">> => null,
        <<"type">> => <<"message">>,
        <<"usage">> => #{
            <<"input_tokens">> => 2095,
            <<"output_tokens">> => 503
        }
    },
    reply_ok(Data, Req1, State).

openai_models(#{path := <<"/v1/models">>} = Req0, State) ->
    %% https://platform.openai.com/docs/api-reference/models/list
    Data = #{
        <<"object">> => <<"list">>,
        <<"data">> => [
            #{
                <<"id">> => <<"gpt-4-0613">>,
                <<"object">> => <<"model">>,
                <<"created">> => 1686588896,
                <<"owned_by">> => <<"openai">>
            },
            #{
                <<"id">> => <<"gpt-4">>,
                <<"object">> => <<"model">>,
                <<"created">> => 1687882411,
                <<"owned_by">> => <<"openai">>
            },
            #{
                <<"id">> => <<"gpt-3.5-turbo">>,
                <<"object">> => <<"model">>,
                <<"created">> => 1677610602,
                <<"owned_by">> => <<"openai">>
            }
        ]
    },
    reply_ok(Data, Req0, State).

anthropic_models(#{path := <<"/v1/models">>} = Req0, State) ->
    %% https://docs.anthropic.com/en/api/models-list
    Data = #{
        <<"data">> => [
            #{
                <<"type">> => <<"model">>,
                <<"id">> => <<"claude-opus-4-20250514">>,
                <<"display_name">> => <<"Claude Opus 4">>,
                <<"created_at">> => <<"2025-05-22T00:00:00Z">>
            },
            #{
                <<"type">> => <<"model">>,
                <<"id">> => <<"claude-3-opus-20240229">>,
                <<"display_name">> => <<"Claude Opus 3">>,
                <<"created_at">> => <<"2024-02-29T00:00:00Z">>
            }
        ],
        <<"has_more">> => false,
        <<"first_id">> => <<"claude-opus-4-20250514">>,
        <<"last_id">> => <<"claude-3-opus-20240229">>
    },
    reply_ok(Data, Req0, State).

anthropic_models_paginated(#{path := <<"/v1/models">>} = Req0, State) ->
    %% https://docs.anthropic.com/en/api/models-list
    Data =
        case cowboy_req:match_qs([{after_id, [], undefined}], Req0) of
            #{after_id := undefined} ->
                #{
                    <<"data">> => [
                        #{
                            <<"type">> => <<"model">>,
                            <<"id">> => <<"claude-opus-4-20250514">>,
                            <<"display_name">> => <<"Claude Opus 4">>,
                            <<"created_at">> => <<"2025-05-22T00:00:00Z">>
                        }
                    ],
                    <<"has_more">> => true,
                    <<"first_id">> => <<"claude-opus-4-20250514">>,
                    <<"last_id">> => <<"claude-opus-4-20250514">>
                };
            _ ->
                #{
                    <<"data">> => [
                        #{
                            <<"type">> => <<"model">>,
                            <<"id">> => <<"claude-3-opus-20240229">>,
                            <<"display_name">> => <<"Claude Opus 3">>,
                            <<"created_at">> => <<"2024-02-29T00:00:00Z">>
                        }
                    ],
                    <<"has_more">> => false,
                    <<"first_id">> => <<"claude-3-opus-20240229">>,
                    <<"last_id">> => <<"claude-3-opus-20240229">>
                }
        end,
    reply_ok(Data, Req0, State).

reply_ok(Data, Req0, State) ->
    Req = cowboy_req:reply(
        200,
        #{<<"content-type">> => <<"application/json">>},
        emqx_utils_json:encode(Data),
        Req0
    ),
    {ok, Req, State}.
