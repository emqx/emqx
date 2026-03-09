%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% Minimal OpenAI-compatible HTTP mock server for emqx_agent_session tests.
%%
%% Named handlers:
%%   simple_finish — responds with finish_reason=stop and JSON content
%%   tool_call     — responds with finish_reason=tool_calls, one tool call (id: call-001)
%%   http_error    — responds with HTTP 500

-module(emqx_agent_llm_mock).

-export([start_link/0, stop/0, set_handler/1]).
-export([simple_finish/2, tool_call/2, http_error/2]).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

start_link() ->
    case emqx_utils_http_test_server:start_link(random, "/[...]", false) of
        {ok, {Port, _Pid}} -> {ok, Port};
        Error -> Error
    end.

stop() ->
    emqx_utils_http_test_server:stop().

set_handler(simple_finish) -> set_handler(fun simple_finish/2);
set_handler(tool_call) -> set_handler(fun tool_call/2);
set_handler(http_error) -> set_handler(fun http_error/2);
set_handler(Fun) -> emqx_utils_http_test_server:set_handler(Fun).

%%--------------------------------------------------------------------
%% Named handlers
%%--------------------------------------------------------------------

%% Returns finish_reason=stop with a JSON result payload.
simple_finish(Req0, State) ->
    {ok, _Body, Req1} = cowboy_req:read_body(Req0),
    Data = #{
        <<"choices">> => [
            #{
                <<"message">> => #{
                    <<"role">> => <<"assistant">>,
                    <<"content">> => <<"{\"summary\": \"done\"}">>
                },
                <<"finish_reason">> => <<"stop">>
            }
        ],
        <<"usage">> => #{
            <<"prompt_tokens">> => 100,
            <<"completion_tokens">> => 20
        }
    },
    reply_ok(Data, Req1, State).

%% Returns finish_reason=tool_calls with a single call to "my_tool" (id: call-001).
tool_call(Req0, State) ->
    {ok, _Body, Req1} = cowboy_req:read_body(Req0),
    Data = #{
        <<"choices">> => [
            #{
                <<"message">> => #{
                    <<"role">> => <<"assistant">>,
                    <<"content">> => null,
                    <<"tool_calls">> => [
                        #{
                            <<"id">> => <<"call-001">>,
                            <<"type">> => <<"function">>,
                            <<"function">> => #{
                                <<"name">> => <<"my_tool">>,
                                <<"arguments">> => <<"{\"key\":\"val\"}">>
                            }
                        }
                    ]
                },
                <<"finish_reason">> => <<"tool_calls">>
            }
        ],
        <<"usage">> => #{
            <<"prompt_tokens">> => 50,
            <<"completion_tokens">> => 10
        }
    },
    reply_ok(Data, Req1, State).

%% Simulates an LLM API error.
http_error(Req0, State) ->
    {ok, _Body, Req1} = cowboy_req:read_body(Req0),
    Req = cowboy_req:reply(
        500,
        #{<<"content-type">> => <<"application/json">>},
        <<"{\"error\":\"internal_server_error\"}">>,
        Req1
    ),
    {ok, Req, State}.

%%--------------------------------------------------------------------
%% Internal
%%--------------------------------------------------------------------

reply_ok(Data, Req0, State) ->
    Req = cowboy_req:reply(
        200,
        #{<<"content-type">> => <<"application/json">>},
        emqx_utils_json:encode(Data),
        Req0
    ),
    {ok, Req, State}.
