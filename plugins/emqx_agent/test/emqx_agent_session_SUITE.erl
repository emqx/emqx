%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% Integration tests for emqx_agent_session against a real LLM.
%%
%% Infrastructure:
%%   - emqx:subscribe / emqx_broker:publish used for in/out traffic
%%     (bypasses MQTT auth, no emqtt client needed).
%%   - Unique SID per test case (the test case atom as binary) so that
%%     globally-registered gen_statem processes never collide.
%%
%% Timeouts are generous (60 s) because small models on CPU can be slow.

-module(emqx_agent_session_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("emqx/include/emqx.hrl").

-define(PROVIDER_NAME, <<"test-llm">>).
-define(BAD_PROVIDER_NAME, <<"bad-test-llm">>).
-define(LLM_TIMEOUT, 60_000).
-define(SHORT_TIMEOUT, 5_000).
%%--------------------------------------------------------------------
%% CT callbacks
%%--------------------------------------------------------------------

%% Pure SSE parser tests run without any LLM — they are always included.
%% LLM integration tests require API key and are skipped when it is absent.
-define(PARSER_TESTS, [
    t_sse_parser_simple_content,
    t_sse_parser_finish_reason_not_overwritten_by_null,
    t_sse_parser_tool_call_argument_fragments,
    t_sse_parser_multiple_tool_calls,
    t_sse_parser_crlf_endings,
    t_sse_parser_split_chunks,
    t_sse_parser_usage_chunk,
    t_request_queue_fifo
]).
-define(LLM_TESTS, [
    t_request_finish,
    t_request_with_tool_call,
    t_events_are_incorporated,
    t_persistent_keeps_session,
    t_persistent_compacts_history,
    t_persistent_request_iterations_reset_per_request,
    t_explicit_stop_terminates_session,
    t_llm_connection_error_terminates_session,
    t_request_while_busy_is_queued,
    t_non_session_topic_ignored
]).

all() ->
    ?PARSER_TESTS ++ ?LLM_TESTS.

init_per_suite(Config) ->
    Apps =
        emqx_cth_suite:start(
            [
                emqx,
                emqx_conf,
                emqx_resource,
                {emqx_ai_completion, #{config => "ai.providers = [], ai.completion_profiles = []"}},
                emqx_agent
            ],
            #{work_dir => emqx_cth_suite:work_dir(Config)}
        ),
    [{suite_apps, Apps}, {llm_available, llm_available()} | Config].

end_per_suite(Config) ->
    emqx_cth_suite:stop(?config(suite_apps, Config)).

init_per_testcase(TestCase, Config) ->
    case lists:member(TestCase, ?LLM_TESTS) andalso not ?config(llm_available, Config) of
        true ->
            {skip, emqx_agent_test_llm_helper:skip_reason("session")};
        false ->
            Sid = atom_to_binary(TestCase, utf8),
            maybe_create_test_providers(TestCase),
            case lists:member(TestCase, ?LLM_TESTS) of
                true ->
                    ct:timetrap({seconds, 60});
                false ->
                    ok
            end,
            emqx:subscribe(out_topic(Sid)),
            [{sid, Sid} | Config]
    end.

end_per_testcase(_TestCase, Config) ->
    _ = emqx_ai_completion_config:update_providers_raw({delete, ?PROVIDER_NAME}),
    _ = emqx_ai_completion_config:update_providers_raw({delete, ?BAD_PROVIDER_NAME}),
    case ?config(sid, Config) of
        undefined ->
            ok;
        Sid ->
            emqx:unsubscribe(out_topic(Sid))
    end.

%%--------------------------------------------------------------------
%% Pure SSE parser unit tests (no LLM required)
%%--------------------------------------------------------------------

%% Simple two-chunk content accumulation.
t_sse_parser_simple_content(_Config) ->
    Data =
        <<
            "data: {\"choices\":[{\"delta\":{\"content\":\"hello \"},\"finish_reason\":null}]}\n\n"
            "data: {\"choices\":[{\"delta\":{\"content\":\"world\"},\"finish_reason\":null}]}\n\n"
            "data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n"
            "data: [DONE]\n\n"
        >>,
    Acc0 = emqx_agent_session:init_stream_acc(),
    Acc = emqx_agent_session:test_feed_sse(Data, Acc0),
    ?assertMatch(#{content := <<"hello world">>}, Acc),
    ?assertEqual(<<"stop">>, maps:get(finish_reason, Acc)).

%% finish_reason must not be overwritten by null from intermediate chunks.
t_sse_parser_finish_reason_not_overwritten_by_null(_Config) ->
    Data =
        <<
            "data: {\"choices\":[{\"delta\":{\"content\":\"ok\"},\"finish_reason\":null}]}\n\n"
            "data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"tool_calls\"}]}\n\n"
            "data: [DONE]\n\n"
        >>,
    Acc0 = emqx_agent_session:init_stream_acc(),
    Acc = emqx_agent_session:test_feed_sse(Data, Acc0),
    ?assertEqual(<<"tool_calls">>, maps:get(finish_reason, Acc)).

%% Tool call arguments arrive as fragments; they must be concatenated.
t_sse_parser_tool_call_argument_fragments(_Config) ->
    Data =
        <<
            "data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call_1\",\"type\":\"function\",\"function\":{\"name\":\"add\",\"arguments\":\"\"}}]},\"finish_reason\":null}]}\n\n"
            "data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"function\":{\"arguments\":\"{\\\"a\\\"\"}}]},\"finish_reason\":null}]}\n\n"
            "data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"function\":{\"arguments\":\": 47, \\\"b\\\": 47}\"}}]},\"finish_reason\":null}]}\n\n"
            "data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"tool_calls\"}]}\n\n"
            "data: {\"choices\":[],\"usage\":{\"prompt_tokens\":10,\"completion_tokens\":5}}\n\n"
            "data: [DONE]\n\n"
        >>,
    Acc0 = emqx_agent_session:init_stream_acc(),
    Acc = emqx_agent_session:test_feed_sse(Data, Acc0),
    ?assertMatch(#{finish_reason := <<"tool_calls">>}, Acc),
    ?assertMatch(#{tokens_in := 10}, Acc),
    ?assertMatch(#{tokens_out := 5}, Acc),
    ToolCalls = maps:get(tool_calls, Acc),
    ?assert(maps:is_key(0, ToolCalls)),
    TC = maps:get(0, ToolCalls),
    Fun = maps:get(<<"function">>, TC, #{}),
    ?assertMatch(#{<<"name">> := <<"add">>}, Fun),
    Args = maps:get(<<"arguments">>, Fun),
    ?assertMatch({ok, #{<<"a">> := 47, <<"b">> := 47}}, emqx_utils_json:safe_decode(Args)).

%% Multiple tool calls with separate indices.
t_sse_parser_multiple_tool_calls(_Config) ->
    Data =
        <<
            "data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"c0\",\"type\":\"function\",\"function\":{\"name\":\"f0\",\"arguments\":\"{}\"}},{\"index\":1,\"id\":\"c1\",\"type\":\"function\",\"function\":{\"name\":\"f1\",\"arguments\":\"{}\"}}]},\"finish_reason\":null}]}\n\n"
            "data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"tool_calls\"}]}\n\n"
            "data: [DONE]\n\n"
        >>,
    Acc0 = emqx_agent_session:init_stream_acc(),
    Acc = emqx_agent_session:test_feed_sse(Data, Acc0),
    ToolCalls = maps:get(tool_calls, Acc),
    ?assertEqual(2, maps:size(ToolCalls)),
    ?assertMatch(#{<<"id">> := <<"c0">>}, maps:get(0, ToolCalls)),
    ?assertEqual(<<"c1">>, maps:get(<<"id">>, maps:get(1, ToolCalls))).

%% CRLF line endings must be normalised.
t_sse_parser_crlf_endings(_Config) ->
    Data =
        <<
            "data: {\"choices\":[{\"delta\":{\"content\":\"x\"},\"finish_reason\":null}]}\r\n\r\n"
            "data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\r\n\r\n"
            "data: [DONE]\r\n\r\n"
        >>,
    Acc0 = emqx_agent_session:init_stream_acc(),
    Acc = emqx_agent_session:test_feed_sse(Data, Acc0),
    ?assertMatch(#{content := <<"x">>}, Acc),
    ?assertEqual(<<"stop">>, maps:get(finish_reason, Acc)).

%% Events split across arbitrary byte boundaries (simulating small TCP packets).
%% Uses test_stream_chunks which threads the SSE buffer between calls,
%% exactly as stream_receive_loop does in production.
t_sse_parser_split_chunks(_Config) ->
    Full =
        <<
            "data: {\"choices\":[{\"delta\":{\"content\":\"ab\"},\"finish_reason\":null}]}\n\n"
            "data: {\"choices\":[{\"delta\":{\"content\":\"cd\"},\"finish_reason\":null}]}\n\n"
            "data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n"
            "data: [DONE]\n\n"
        >>,
    %% Chop into 20-byte pieces to simulate network fragmentation.
    Chunks = chop_binary(Full, 20),
    Acc = emqx_agent_session:test_stream_chunks(Chunks),
    ?assertMatch(#{content := <<"abcd">>}, Acc),
    ?assertEqual(<<"stop">>, maps:get(finish_reason, Acc)).

%% Usage tokens from the dedicated usage-only chunk.
t_sse_parser_usage_chunk(_Config) ->
    Data =
        <<
            "data: {\"choices\":[{\"delta\":{\"content\":\"hi\"},\"finish_reason\":null}]}\n\n"
            "data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n"
            "data: {\"choices\":[],\"usage\":{\"prompt_tokens\":42,\"completion_tokens\":7,\"total_tokens\":49}}\n\n"
            "data: [DONE]\n\n"
        >>,
    Acc0 = emqx_agent_session:init_stream_acc(),
    Acc = emqx_agent_session:test_feed_sse(Data, Acc0),
    ?assertMatch(#{tokens_in := 42}, Acc),
    ?assertEqual(7, maps:get(tokens_out, Acc)),
    ?assertEqual(49, maps:get(total_tokens, Acc)).

t_request_queue_fifo(_Config) ->
    Iids = emqx_agent_session:test_busy_request_queue_iids([
        #{<<"type">> => <<"request">>, <<"iid">> => <<"iid-1">>},
        #{<<"type">> => <<"request">>, <<"iid">> => <<"iid-2">>},
        #{<<"type">> => <<"request">>, <<"iid">> => <<"iid-3">>}
    ]),
    ?assertEqual([<<"iid-1">>, <<"iid-2">>, <<"iid-3">>], Iids).

%%--------------------------------------------------------------------
%% LLM integration test cases (skipped when LLM API is not reachable)
%%--------------------------------------------------------------------

%% Basic request/final round-trip: the LLM receives no tools so it must
%% respond directly.  We only assert on the frame shape, not on the text.
t_request_finish(Config) ->
    publish_in(
        Config,
        request(
            Config,
            #{
                <<"tools">> => [],
                <<"instructions">> =>
                    <<"Reply with a short JSON object, e.g. {\"answer\": \"42\"}.">>,
                <<"input">> => #{<<"question">> => <<"What is 6 times 7?">>}
            }
        )
    ),
    Final = recv_final(Config),
    ?assertMatch(
        #{
            <<"type">> := <<"final">>,
            <<"sid">> := _,
            <<"iid">> := <<"iid-1">>,
            <<"trace_id">> := <<"tr-1">>,
            <<"result">> := _,
            <<"usage">> := #{<<"iterations">> := 1}
        },
        Final
    ).

%% The LLM must call the `add` tool to answer a simple x+x question.
%% We supply a known result and then verify the final answer contains it.
%% Even the smallest model reliably delegates arithmetic to a tool when
%% the tool is the only way to answer and the numbers are obvious.
t_request_with_tool_call(Config) ->
    %% x + x — answer is trivially 94
    A = 47,
    B = 47,
    Sum = A + B,
    Tool =
        #{
            <<"name">> => <<"add">>,
            <<"description">> => <<"Add two integers and return their sum.">>,
            <<"parameters">> =>
                #{
                    <<"type">> => <<"object">>,
                    <<"properties">> =>
                        #{
                            <<"a">> => #{<<"type">> => <<"integer">>},
                            <<"b">> => #{<<"type">> => <<"integer">>}
                        },
                    <<"required">> => [<<"a">>, <<"b">>]
                }
        },
    publish_in(
        Config,
        request(
            Config,
            #{
                <<"tools">> => [Tool],
                <<"instructions">> =>
                    <<"Use the add tool to compute the answer. Do not answer directly.">>,
                <<"input">> => #{<<"question">> => <<"What is 47 + 47?">>}
            }
        )
    ),

    ToolReq = recv_tool_request(Config),
    ?assertMatch(
        #{
            <<"type">> := <<"tool_request">>,
            <<"call_id">> := _,
            <<"tool">> := <<"add">>
        },
        ToolReq
    ),

    CallId = maps:get(<<"call_id">>, ToolReq),
    publish_in(
        Config,
        #{
            <<"type">> => <<"tool_result">>,
            <<"call_id">> => CallId,
            <<"response">> =>
                #{<<"status">> => <<"ok">>, <<"result">> => #{<<"sum">> => Sum}}
        }
    ),

    Final = recv_final(Config),
    assert_final(Final),

    %% The model received the tool result (sum=94) and must mention it.
    ResultText = result_to_text(maps:get(<<"result">>, Final)),
    ?assert(
        binary:match(ResultText, integer_to_binary(Sum)) =/= nomatch,
        iolist_to_binary(io_lib:format("expected ~b in result: ~s", [Sum, ResultText]))
    ).

%% Events are buffered while the LLM is reasoning and forwarded on the
%% next LLM call.  We test this behaviourally: start with
%% persistent=true so the session stays in idle, push an event,
%% then send a second request and verify the session produces a second
%% An event arriving in idle state immediately restarts reasoning.
%% We inject a distinctive sentinel value and verify the final mentions it —
%% the LLM can only know the value if the event was part of its context.
%% No second request is needed: the event alone triggers the new LLM call.
t_events_are_incorporated(Config) ->
    %% unlikely to appear by chance
    Sentinel = 7331,
    %% First request — session answers and returns to idle (persistent=true)
    publish_in(
        Config,
        request(
            Config,
            #{
                <<"tools">> => [],
                <<"instructions">> =>
                    <<
                        "Answer briefly. When asked to report an event value, reply "
                        "with only the number."
                    >>,
                <<"input">> => #{<<"q">> => <<"What is 1+1?">>},
                <<"persistent">> => true
            }
        )
    ),
    Final1 = recv_final(Config),
    assert_final(Final1),

    %% Push an event — this alone triggers a new LLM call (idle → calling_llm)
    publish_in(
        Config,
        #{
            <<"type">> => <<"event">>,
            <<"event">> =>
                #{
                    <<"alert">> => <<"sensor_spike">>,
                    <<"value">> => Sentinel,
                    <<"instruction">> => <<"Report the numeric value from this event.">>
                }
        }
    ),

    %% Wait for the event-driven final — no second request sent
    Final2 = recv_final(Config),
    assert_final(Final2),

    ResultText = result_to_text(maps:get(<<"result">>, Final2)),
    ?assert(
        binary:match(ResultText, integer_to_binary(Sentinel)) =/= nomatch,
        iolist_to_binary(
            io_lib:format(
                "expected sentinel ~b in result (proves event was in LLM context): ~s",
                [Sentinel, ResultText]
            )
        )
    ).

%% With persistent=true the session returns to idle after publishing
%% final. Verify the process is still alive, then force the idle timeout so the
%% test does not leak a persistent session.
t_persistent_keeps_session(Config) ->
    publish_in(
        Config,
        request(
            Config,
            #{
                <<"tools">> => [],
                <<"instructions">> => <<"Answer briefly.">>,
                <<"input">> => #{<<"q">> => <<"What is 2+2?">>},
                <<"persistent">> => true
            }
        )
    ),
    _Final1 = recv_final(Config),

    Sid = ?config(sid, Config),
    Pid = emqx_agent_session:whereis(Sid),
    ?assertNotEqual(undefined, Pid),
    Ref = monitor(process, Pid),

    Pid ! persistent_idle_timeout,

    receive
        {'DOWN', Ref, process, Pid, normal} ->
            ok
    after ?SHORT_TIMEOUT ->
        ct:fail("session did not stop after persistent idle timeout")
    end.

t_persistent_compacts_history(Config) ->
    publish_in(
        Config,
        request(
            Config,
            #{
                <<"tools">> => [],
                <<"instructions">> => <<"Answer briefly.">>,
                <<"input">> =>
                    #{<<"q">> => <<"Remember the marker alpha-17 and answer ok.">>},
                <<"persistent">> => true,
                <<"max_total_tokens">> => 1
            }
        )
    ),
    _Final1 = recv_final(Config),

    publish_in(
        Config,
        request(
            Config,
            #{
                <<"tools">> => [],
                <<"instructions">> => <<"Answer briefly.">>,
                <<"input">> => #{<<"q">> => <<"What marker did I ask you to remember?">>},
                <<"persistent">> => true,
                <<"max_total_tokens">> => 1
            }
        )
    ),
    _Final2 = recv_final(Config),

    {ok, #{messages := History}} = emqx_agent_session:inspect(?config(sid, Config)),
    ?assertMatch(
        [
            #{<<"role">> := <<"system">>},
            #{
                <<"role">> := <<"assistant">>,
                <<"content">> := <<"Compacted prior conversation history:\n", _/binary>>
            },
            #{<<"role">> := <<"user">>},
            #{<<"role">> := <<"assistant">>}
        ],
        History
    ),
    ?assertEqual(4, length(History)).

t_persistent_request_iterations_reset_per_request(Config) ->
    Tool =
        #{
            <<"name">> => <<"add">>,
            <<"description">> => <<"Add two integers and return their sum.">>,
            <<"parameters">> =>
                #{
                    <<"type">> => <<"object">>,
                    <<"properties">> =>
                        #{
                            <<"a">> => #{<<"type">> => <<"integer">>},
                            <<"b">> => #{<<"type">> => <<"integer">>}
                        },
                    <<"required">> => [<<"a">>, <<"b">>]
                }
        },
    publish_in(
        Config,
        request(
            Config,
            #{
                <<"tools">> => [Tool],
                <<"instructions">> =>
                    <<"Use the add tool to compute the answer. Do not answer directly.">>,
                <<"input">> => #{<<"question">> => <<"What is 2 + 3?">>},
                <<"persistent">> => true
            }
        )
    ),

    ToolReq = recv_tool_request(Config),
    publish_in(
        Config,
        #{
            <<"type">> => <<"tool_result">>,
            <<"call_id">> => maps:get(<<"call_id">>, ToolReq),
            <<"response">> => #{<<"status">> => <<"ok">>, <<"result">> => #{<<"sum">> => 5}}
        }
    ),

    Final1 = recv_final(Config),
    assert_final(Final1),
    {ok, #{request_iterations := Iter1}} = emqx_agent_session:inspect(?config(sid, Config)),
    ?assert(Iter1 >= 2),

    publish_in(
        Config,
        request(
            Config,
            #{
                <<"tools">> => [],
                <<"instructions">> => <<"Answer with a short JSON object.">>,
                <<"input">> => #{<<"question">> => <<"What is 1 + 1?">>},
                <<"persistent">> => true
            }
        )
    ),

    Final2 = recv_final(Config),
    assert_final(Final2),
    {ok, #{messages := History, request_iterations := Iter2}} =
        emqx_agent_session:inspect(?config(sid, Config)),
    ?assertEqual(1, Iter2),
    ?assert(length(History) > 4),
    ?assert(maps:get(<<"iterations">>, maps:get(<<"usage">>, Final2)) > Iter2),

    stop_persistent_session(Config).

%% An explicit `stop` frame must terminate the session process.
t_explicit_stop_terminates_session(Config) ->
    publish_in(
        Config,
        request(
            Config,
            #{
                <<"tools">> => [],
                <<"instructions">> => <<"Answer briefly.">>,
                <<"input">> => #{<<"q">> => <<"What is 4+4?">>},
                <<"persistent">> => true
            }
        )
    ),
    _Final = recv_final(Config),

    Sid = ?config(sid, Config),
    Pid = emqx_agent_session:whereis(Sid),
    ?assertNotEqual(undefined, Pid),
    Ref = monitor(process, Pid),

    publish_in(Config, #{<<"type">> => <<"stop">>}),

    receive
        {'DOWN', Ref, process, Pid, normal} ->
            ok
    after ?SHORT_TIMEOUT ->
        ct:fail("session did not stop after stop frame")
    end.

%% A connection error to the LLM must cause the session to terminate.
t_llm_connection_error_terminates_session(Config) ->
    Sid = ?config(sid, Config),
    publish_in(
        Config,
        request(
            Config,
            #{
                <<"tools">> => [],
                <<"provider_name">> => ?BAD_PROVIDER_NAME,
                <<"input">> => #{<<"q">> => <<"hello">>},
                <<"persistent">> => true
            }
        )
    ),
    Pid = wait_for_session(Sid),
    Ref = monitor(process, Pid),
    receive
        {'DOWN', Ref, process, Pid, _} ->
            ok
    after ?SHORT_TIMEOUT ->
        ct:fail("session did not stop after LLM connection error")
    end.

%% A request arriving while the session is busy is queued and replayed after
%% the current reasoning turn finishes.  Make the busy state deterministic by
%% holding the first request in waiting_tools until after the second request is
%% published.
t_request_while_busy_is_queued(Config) ->
    Tool =
        #{
            <<"name">> => <<"add">>,
            <<"description">> => <<"Add two integers and return their sum.">>,
            <<"parameters">> =>
                #{
                    <<"type">> => <<"object">>,
                    <<"properties">> =>
                        #{
                            <<"a">> => #{<<"type">> => <<"integer">>},
                            <<"b">> => #{<<"type">> => <<"integer">>}
                        },
                    <<"required">> => [<<"a">>, <<"b">>]
                }
        },
    publish_in(
        Config,
        request(
            Config,
            #{
                <<"tools">> => [Tool],
                <<"instructions">> =>
                    <<"Use the add tool to compute the answer. Do not answer directly.">>,
                <<"input">> => #{<<"question">> => <<"What is 5 + 5?">>},
                <<"persistent">> => true
            }
        )
    ),

    ToolReq = recv_tool_request(Config),

    publish_in(
        Config,
        request(
            Config,
            #{
                <<"tools">> => [],
                <<"instructions">> => <<"Answer briefly.">>,
                <<"input">> => #{<<"q">> => <<"What is 6+6?">>}
            }
        )
    ),

    publish_in(
        Config,
        #{
            <<"type">> => <<"tool_result">>,
            <<"call_id">> => maps:get(<<"call_id">>, ToolReq),
            <<"response">> =>
                #{<<"status">> => <<"ok">>, <<"result">> => #{<<"sum">> => 10}}
        }
    ),

    Final1 = recv_final(Config),
    assert_final(Final1),
    Final2 = recv_final(Config),
    assert_final(Final2),

    Sid = ?config(sid, Config),
    Pid = emqx_agent_session:whereis(Sid),
    ?assertNotEqual(undefined, Pid),
    Ref = monitor(process, Pid),
    publish_in(Config, #{<<"type">> => <<"stop">>}),
    receive
        {'DOWN', Ref, process, Pid, normal} ->
            ok
    after ?SHORT_TIMEOUT ->
        ct:fail("session did not stop after queued request test")
    end.

%% Messages on topics that are not sess/in/<sid> must never trigger a
%% session process to be created.
t_non_session_topic_ignored(Config) ->
    Sid = ?config(sid, Config),
    Msg = emqx_message:make(?MODULE, 0, <<"unrelated/topic">>, <<"hello">>),
    emqx_broker:publish(Msg),
    ?assertEqual(undefined, emqx_agent_session:whereis(Sid)).

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

in_topic(Sid) ->
    <<"$sess/in/", Sid/binary>>.

out_topic(Sid) ->
    <<"$sess/out/", Sid/binary>>.

%% Base request; Overrides can replace any field (e.g. tools, instructions).
request(Config, Overrides) ->
    Sid = ?config(sid, Config),
    maps:merge(
        #{
            <<"type">> => <<"request">>,
            <<"sid">> => Sid,
            <<"iid">> => <<"iid-1">>,
            <<"trace_id">> => <<"tr-1">>,
            <<"provider_name">> => ?PROVIDER_NAME,
            <<"model">> => emqx_agent_test_llm_helper:default_model(),
            <<"tools">> => [],
            <<"input">> => #{},
            <<"instructions">> => <<"Answer briefly.">>
        },
        Overrides
    ).

publish_in(Config, Msg) ->
    Sid = ?config(sid, Config),
    Payload = emqx_utils_json:encode(Msg),
    emqx_broker:publish(
        emqx_message:make(?MODULE, 0, in_topic(Sid), Payload)
    ).

%% Wait for a `final` frame, transparently skipping `intermediate` chunks
%% (streaming partial content/reasoning) and any other non-final frames.
recv_final(Config) ->
    recv_frame_of_type(Config, <<"final">>, ?LLM_TIMEOUT).

%% Wait for a `tool_request` frame, skipping `intermediate` frames.
recv_tool_request(Config) ->
    recv_frame_of_type(Config, <<"tool_request">>, ?LLM_TIMEOUT).

recv_frame_of_type(Config, Type, Timeout) ->
    Sid = ?config(sid, Config),
    Topic = out_topic(Sid),
    receive
        #deliver{topic = Topic, message = #message{payload = P}} ->
            Frame = emqx_utils_json:decode(P),
            case maps:get(<<"type">>, Frame, undefined) of
                Type ->
                    Frame;
                _Other ->
                    recv_frame_of_type(Config, Type, Timeout)
            end
    after Timeout ->
        ct:fail("no ~s frame on ~s within ~b ms", [Type, Topic, Timeout])
    end.

assert_final(Frame) ->
    ?assertMatch(
        #{
            <<"type">> := <<"final">>,
            <<"sid">> := _,
            <<"trace_id">> := _,
            <<"result">> := _,
            <<"usage">> :=
                #{
                    <<"iterations">> := _,
                    <<"tokens_in">> := _,
                    <<"tokens_out">> := _
                }
        },
        Frame
    ).

%% Flatten a result term (map or binary) to a binary for substring checks.
result_to_text(Result) when is_binary(Result) ->
    Result;
result_to_text(Result) when is_map(Result) ->
    emqx_utils_json:encode(Result);
result_to_text(Result) ->
    iolist_to_binary(io_lib:format("~p", [Result])).

wait_for_session(Sid) ->
    wait_for_session(Sid, 50).

wait_for_session(_Sid, 0) ->
    ct:fail("session never started");
wait_for_session(Sid, N) ->
    case emqx_agent_session:whereis(Sid) of
        undefined ->
            timer:sleep(100),
            wait_for_session(Sid, N - 1);
        Pid ->
            Pid
    end.

stop_persistent_session(Config) ->
    Sid = ?config(sid, Config),
    Pid = emqx_agent_session:whereis(Sid),
    ?assertNotEqual(undefined, Pid),
    Ref = monitor(process, Pid),
    publish_in(Config, #{<<"type">> => <<"stop">>}),
    receive
        {'DOWN', Ref, process, Pid, normal} ->
            ok
    after ?SHORT_TIMEOUT ->
        ct:fail("session did not stop")
    end.

%% Probe the LLM endpoint; if it is not available the whole suite is
%% skipped so that normal unit-test runs (without docker) are unaffected.
llm_available() ->
    emqx_agent_test_llm_helper:available().

maybe_create_test_providers(TestCase) ->
    case lists:member(TestCase, ?LLM_TESTS) of
        true ->
            _ = emqx_ai_completion_config:update_providers_raw({delete, ?PROVIDER_NAME}),
            _ = emqx_ai_completion_config:update_providers_raw({delete, ?BAD_PROVIDER_NAME}),
            ok =
                emqx_ai_completion_config:update_providers_raw(
                    {add, emqx_agent_test_llm_helper:provider(?PROVIDER_NAME)}
                ),
            ok =
                emqx_ai_completion_config:update_providers_raw(
                    {add, #{
                        <<"name">> => ?BAD_PROVIDER_NAME,
                        <<"type">> => <<"openai">>,
                        <<"api_key">> =>
                            <<"bad-test-key">>,
                        <<"base_url">> =>
                            <<"http://127.0.0.1:1/v1">>,
                        <<"transport_options">> =>
                            #{
                                <<"connect_timeout">> =>
                                    <<"1s">>,
                                <<"recv_timeout">> =>
                                    <<"1s">>
                            }
                    }}
                );
        false ->
            ok
    end.

%% Split a binary into consecutive chunks of at most N bytes.
chop_binary(Bin, N) ->
    chop_binary(Bin, N, []).

chop_binary(<<>>, _N, Acc) ->
    lists:reverse(Acc);
chop_binary(Bin, N, Acc) when byte_size(Bin) =< N ->
    lists:reverse([Bin | Acc]);
chop_binary(Bin, N, Acc) ->
    <<Chunk:N/binary, Rest/binary>> = Bin,
    chop_binary(Rest, N, [Chunk | Acc]).
