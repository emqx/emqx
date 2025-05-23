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
set_handler(Fun) ->
    emqx_utils_http_test_server:set_handler(Fun).

openai_chat_completion(#{path := <<"/v1/chat/completions">>} = Req0, State) ->
    {ok, RawBody, Req1} = cowboy_req:read_body(Req0),
    Body = emqx_utils_json:decode(RawBody),
    #{
        <<"messages">> :=
            [
                #{<<"content">> := _, <<"role">> := <<"system">>},
                #{<<"content">> := _, <<"role">> := <<"user">>}
            ],
        <<"model">> := _
    } =
        Body,
    %% https://platform.openai.com/docs/api-reference/chat/object
    Data = #{
        <<"id">> => <<"chatcmpl-B9MHDbslfkBeAs8l4bebGdFOJ6PeG">>,
        <<"object">> => <<"chat.completion">>,
        <<"created">> => 1741570283,
        <<"model">> => <<"gpt-4o-2024-08-06">>,
        <<"choices">> => [
            #{
                <<"index">> => 0,
                <<"message">> => #{
                    <<"role">> => <<"assistant">>,
                    <<"content">> => <<"some completion">>,
                    <<"refusal">> => null,
                    <<"annotations">> => []
                },
                <<"logprobs">> => null,
                <<"finish_reason">> => <<"stop">>
            }
        ],
        <<"usage">> => #{
            <<"prompt_tokens">> => 1117,
            <<"completion_tokens">> => 46,
            <<"total_tokens">> => 1163,
            <<"prompt_tokens_details">> => #{
                <<"cached_tokens">> => 0,
                <<"audio_tokens">> => 0
            },
            <<"completion_tokens_details">> => #{
                <<"reasoning_tokens">> => 0,
                <<"audio_tokens">> => 0,
                <<"accepted_prediction_tokens">> => 0,
                <<"rejected_prediction_tokens">> => 0
            }
        },
        <<"service_tier">> => <<"default">>,
        <<"system_fingerprint">> => <<"fp_fc9f1d7035">>
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

reply_ok(Data, Req0, State) ->
    Req = cowboy_req:reply(
        200,
        #{<<"content-type">> => <<"application/json">>},
        emqx_utils_json:encode(Data),
        Req0
    ),
    {ok, Req, State}.
