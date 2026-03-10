%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-define(BASE_URL, <<"http://ollama:11434/v1">>).
-define(MODEL, <<"qwen2.5:0.5b">>).

-define(LLM_TIMEOUT, 60_000).
-define(SHORT_TIMEOUT, 5_000).

%%--------------------------------------------------------------------
%% CT callbacks
%%--------------------------------------------------------------------

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [emqx_agent],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    case llm_available() of
        false ->
            emqx_cth_suite:stop(Apps),
            {skip, "LLM API not reachable at http://ollama:11434/v1"};
        true ->
            [{suite_apps, Apps} | Config]
    end.

end_per_suite(Config) ->
    emqx_cth_suite:stop(?config(suite_apps, Config)).

init_per_testcase(TestCase, Config) ->
    Sid = atom_to_binary(TestCase, utf8),
    emqx:subscribe(out_topic(Sid)),
    [{sid, Sid} | Config].

end_per_testcase(_TestCase, Config) ->
    Sid = ?config(sid, Config),
    emqx:unsubscribe(out_topic(Sid)).

%%--------------------------------------------------------------------
%% Test cases
%%--------------------------------------------------------------------

%% Basic request/final round-trip: the LLM receives no tools so it must
%% respond directly.  We only assert on the frame shape, not on the text.
t_request_finish(Config) ->
    publish_in(
        Config,
        request(Config, #{
            <<"tools">> => [],
            <<"instructions">> => <<"Reply with a short JSON object, e.g. {\"answer\": \"42\"}.">>,
            <<"input">> => #{<<"question">> => <<"What is 6 times 7?">>}
        })
    ),
    Final = recv_out(Config),
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
    Tool = #{
        <<"name">> => <<"add">>,
        <<"description">> => <<"Add two integers and return their sum.">>,
        <<"parameters">> => #{
            <<"type">> => <<"object">>,
            <<"properties">> => #{
                <<"a">> => #{<<"type">> => <<"integer">>},
                <<"b">> => #{<<"type">> => <<"integer">>}
            },
            <<"required">> => [<<"a">>, <<"b">>]
        }
    },
    publish_in(
        Config,
        request(Config, #{
            <<"tools">> => [Tool],
            <<"instructions">> =>
                <<"Use the add tool to compute the answer. Do not answer directly.">>,
            <<"input">> => #{<<"question">> => <<"What is 47 + 47?">>}
        })
    ),

    ToolReq = recv_out(Config),
    ct:pal("tool_request frame: ~p", [ToolReq]),
    ?assertMatch(
        #{<<"type">> := <<"tool_request">>, <<"call_id">> := _, <<"tool">> := <<"add">>},
        ToolReq
    ),

    CallId = maps:get(<<"call_id">>, ToolReq),
    publish_in(Config, #{
        <<"type">> => <<"tool_result">>,
        <<"call_id">> => CallId,
        <<"ok">> => true,
        <<"data">> => #{<<"sum">> => Sum}
    }),

    Final = recv_out(Config),
    ct:pal("final frame: ~p", [Final]),
    assert_final(Final),

    %% The model received the tool result (sum=94) and must mention it.
    ResultText = result_to_text(maps:get(<<"result">>, Final)),
    ?assert(
        binary:match(ResultText, integer_to_binary(Sum)) =/= nomatch,
        iolist_to_binary(io_lib:format("expected ~b in result: ~s", [Sum, ResultText]))
    ).

%% Events are buffered while the LLM is reasoning and forwarded on the
%% next LLM call.  We test this behaviourally: start with
%% stop_on_finish=false so the session stays in idle, push an event,
%% then send a second request and verify the session produces a second
%% An event arriving in idle state immediately restarts reasoning.
%% We inject a distinctive sentinel value and verify the final mentions it —
%% the LLM can only know the value if the event was part of its context.
%% No second request is needed: the event alone triggers the new LLM call.
t_events_are_incorporated(Config) ->
    %% unlikely to appear by chance
    Sentinel = 7331,
    %% First request — session answers and returns to idle (stop_on_finish=false)
    publish_in(
        Config,
        request(Config, #{
            <<"tools">> => [],
            <<"instructions">> =>
                <<"Answer briefly. When asked to report an event value, reply with only the number.">>,
            <<"input">> => #{<<"q">> => <<"What is 1+1?">>},
            <<"stop_on_finish">> => false
        })
    ),
    Final1 = recv_out(Config),
    assert_final(Final1),

    %% Push an event — this alone triggers a new LLM call (idle → calling_llm)
    publish_in(Config, #{
        <<"type">> => <<"event">>,
        <<"event">> => #{
            <<"alert">> => <<"sensor_spike">>,
            <<"value">> => Sentinel,
            <<"instruction">> => <<"Report the numeric value from this event.">>
        }
    }),

    %% Wait for the event-driven final — no second request sent
    Final2 = recv_out(Config),
    ct:pal("event-driven final frame: ~p", [Final2]),
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

%% With stop_on_finish=false the session returns to idle after publishing
%% final.  Verify:
%%   1. Session process is still alive after the first final.
%%   2. A subsequent request frame is discarded (no second final arrives).
t_stop_on_finish_false_keeps_session(Config) ->
    publish_in(
        Config,
        request(Config, #{
            <<"tools">> => [],
            <<"instructions">> => <<"Answer briefly.">>,
            <<"input">> => #{<<"q">> => <<"What is 2+2?">>},
            <<"stop_on_finish">> => false
        })
    ),
    _Final1 = recv_out(Config),

    Sid = ?config(sid, Config),
    ?assertNotEqual(undefined, emqx_agent_session:whereis(Sid)),

    %% A new request must be discarded — no second final should arrive
    publish_in(
        Config,
        request(Config, #{
            <<"tools">> => [],
            <<"instructions">> => <<"Answer briefly.">>,
            <<"input">> => #{<<"q">> => <<"What is 3+3?">>}
        })
    ),
    ?assertEqual(timeout, recv_out_or_timeout(Config)).

%% An explicit `stop` frame must terminate the session process.
t_explicit_stop_terminates_session(Config) ->
    publish_in(
        Config,
        request(Config, #{
            <<"tools">> => [],
            <<"instructions">> => <<"Answer briefly.">>,
            <<"input">> => #{<<"q">> => <<"What is 4+4?">>},
            <<"stop_on_finish">> => false
        })
    ),
    _Final = recv_out(Config),

    Sid = ?config(sid, Config),
    Pid = emqx_agent_session:whereis(Sid),
    ?assertNotEqual(undefined, Pid),
    Ref = monitor(process, Pid),

    publish_in(Config, #{<<"type">> => <<"stop">>}),

    receive
        {'DOWN', Ref, process, Pid, normal} -> ok
    after ?SHORT_TIMEOUT ->
        ct:fail("session did not stop after stop frame")
    end.

%% A connection error to the LLM must cause the session to terminate.
t_llm_connection_error_terminates_session(Config) ->
    %% nothing listens here
    BadUrl = <<"http://127.0.0.1:1/v1">>,
    Sid = ?config(sid, Config),
    publish_in(
        Config,
        request(Config, #{
            <<"tools">> => [],
            <<"base_url">> => BadUrl,
            <<"input">> => #{<<"q">> => <<"hello">>}
        })
    ),
    Pid = wait_for_session(Sid),
    Ref = monitor(process, Pid),
    receive
        {'DOWN', Ref, process, Pid, _} -> ok
    after ?SHORT_TIMEOUT ->
        ct:fail("session did not stop after LLM connection error")
    end.

%% A second request arriving while the session is processing (waiting
%% for the LLM) must be silently dropped — only one final is produced.
t_duplicate_request_is_ignored(Config) ->
    publish_in(
        Config,
        request(Config, #{
            <<"tools">> => [],
            <<"instructions">> => <<"Answer briefly.">>,
            <<"input">> => #{<<"q">> => <<"What is 5+5?">>}
        })
    ),

    %% We do not know exactly when the session starts calling the LLM,
    %% but publishing a second request immediately is very likely to race.
    publish_in(
        Config,
        request(Config, #{
            <<"tools">> => [],
            <<"instructions">> => <<"Answer briefly.">>,
            <<"input">> => #{<<"q">> => <<"What is 6+6?">>}
        })
    ),

    Final = recv_out(Config),
    assert_final(Final),

    %% No second frame should arrive within a short window
    ?assertEqual(timeout, recv_out_or_timeout(Config)).

%% Messages on topics that are not sess/in/<sid>/ must never trigger a
%% session process to be created.
t_non_session_topic_ignored(Config) ->
    Sid = ?config(sid, Config),
    Msg = emqx_message:make(?MODULE, 0, <<"unrelated/topic">>, <<"hello">>),
    emqx_broker:publish(Msg),
    ?assertEqual(undefined, emqx_agent_session:whereis(Sid)).

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

in_topic(Sid) -> <<"sess/in/", Sid/binary, "/">>.
out_topic(Sid) -> <<"sess/out/", Sid/binary, "/">>.

%% Base request; Overrides can replace any field (e.g. tools, instructions).
request(Config, Overrides) ->
    Sid = ?config(sid, Config),
    maps:merge(
        #{
            <<"type">> => <<"request">>,
            <<"sid">> => Sid,
            <<"iid">> => <<"iid-1">>,
            <<"trace_id">> => <<"tr-1">>,
            <<"api_key">> => <<"ollama">>,
            <<"base_url">> => ?BASE_URL,
            <<"model">> => ?MODEL,
            <<"tools">> => [],
            <<"input">> => #{},
            <<"instructions">> => <<"Answer briefly.">>,
            <<"output_schema">> => #{<<"type">> => <<"object">>}
        },
        Overrides
    ).

publish_in(Config, Msg) ->
    Sid = ?config(sid, Config),
    Payload = emqx_utils_json:encode(Msg),
    emqx_broker:publish(emqx_message:make(?MODULE, 0, in_topic(Sid), Payload)).

recv_out(Config) ->
    Sid = ?config(sid, Config),
    Topic = out_topic(Sid),
    receive
        #deliver{topic = Topic, message = #message{payload = P}} ->
            emqx_utils_json:decode(P)
    after ?LLM_TIMEOUT ->
        ct:fail("no message on ~s within ~b ms", [Topic, ?LLM_TIMEOUT])
    end.

recv_out_or_timeout(Config) ->
    Sid = ?config(sid, Config),
    Topic = out_topic(Sid),
    receive
        #deliver{topic = Topic} -> received
    after 1000 ->
        timeout
    end.

assert_final(Frame) ->
    ?assertMatch(
        #{
            <<"type">> := <<"final">>,
            <<"sid">> := _,
            <<"trace_id">> := _,
            <<"result">> := _,
            <<"usage">> := #{
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

%% Probe the LLM endpoint; if it is not available the whole suite is
%% skipped so that normal unit-test runs (without docker) are unaffected.
llm_available() ->
    Url = <<?BASE_URL/binary, "/models">>,
    Opts = [with_body, {connect_timeout, 3_000}, {recv_timeout, 5_000}],
    case hackney:request(get, Url, [], <<>>, Opts) of
        {ok, 200, _, _} -> true;
        _ -> false
    end.
