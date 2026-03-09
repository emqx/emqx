%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% Integration tests for emqx_agent_session.
%%
%% Each test case:
%%   1. Starts a fresh emqx_agent_llm_mock HTTP server (random port).
%%   2. Subscribes the test process to the session out-topic via the
%%      internal broker API (no MQTT client / no auth checks needed).
%%   3. Triggers the session by publishing to the in-topic via
%%      emqx_broker:publish/1 — this fires the message.publish hook.
%%   4. Asserts the expected frame(s) arrive on the out-topic.
%%
%% Sessions are identified by a unique SID per test case (the test
%% case atom, as binary) so globally-registered gen_statem processes
%% never collide.

-module(emqx_agent_session_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("emqx/include/emqx.hrl").

-define(TIMEOUT, 5000).

%%--------------------------------------------------------------------
%% CT callbacks
%%--------------------------------------------------------------------

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [emqx_agent],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{suite_apps, Apps} | Config].

end_per_suite(Config) ->
    emqx_cth_suite:stop(?config(suite_apps, Config)).

init_per_testcase(TestCase, Config) ->
    {ok, Port} = emqx_agent_llm_mock:start_link(),
    BaseUrl = iolist_to_binary(io_lib:format("http://127.0.0.1:~b/v1", [Port])),
    Sid = atom_to_binary(TestCase, utf8),
    emqx:subscribe(out_topic(Sid)),
    [{port, Port}, {base_url, BaseUrl}, {sid, Sid} | Config].

end_per_testcase(_TestCase, Config) ->
    Sid = ?config(sid, Config),
    emqx:unsubscribe(out_topic(Sid)),
    emqx_agent_llm_mock:stop().

%%--------------------------------------------------------------------
%% Test cases
%%--------------------------------------------------------------------

%% Simple request → final (LLM answers immediately with no tool calls).
t_simple_request_finish(Config) ->
    ok = emqx_agent_llm_mock:set_handler(simple_finish),
    publish_in(Config, request(Config, #{})),
    Final = recv_out(Config),
    ?assertMatch(
        #{
            <<"type">> := <<"final">>,
            <<"sid">> := _,
            <<"iid">> := <<"iid-1">>,
            <<"trace_id">> := <<"tr-1">>,
            <<"result">> := #{<<"summary">> := <<"done">>},
            <<"usage">> := #{<<"iterations">> := 1}
        },
        Final
    ).

%% Request → tool_request (out) → tool_result (in) → final (out).
t_tool_call_round_trip(Config) ->
    ok = emqx_agent_llm_mock:set_handler(tool_call),
    publish_in(Config, request(Config, #{})),

    ToolReq = recv_out(Config),
    ?assertMatch(
        #{
            <<"type">> := <<"tool_request">>,
            <<"call_id">> := <<"call-001">>,
            <<"tool">> := <<"my_tool">>,
            <<"args">> := #{<<"key">> := <<"val">>}
        },
        ToolReq
    ),
    CallId = maps:get(<<"call_id">>, ToolReq),

    ok = emqx_agent_llm_mock:set_handler(simple_finish),
    publish_in(Config, #{
        <<"type">> => <<"tool_result">>,
        <<"call_id">> => CallId,
        <<"ok">> => true,
        <<"data">> => #{<<"value">> => 42}
    }),

    Final = recv_out(Config),
    ?assertMatch(
        #{
            <<"type">> := <<"final">>,
            <<"usage">> := #{<<"tool_calls">> := 1, <<"iterations">> := 2}
        },
        Final
    ).

%% Events sent while waiting for a tool result are buffered and forwarded
%% to the LLM as part of the next call.  We capture the second request
%% body and verify the event appears in the messages array.
t_event_is_buffered_and_flushed(Config) ->
    ok = emqx_agent_llm_mock:set_handler(tool_call),
    publish_in(Config, request(Config, #{})),

    ToolReq = recv_out(Config),
    CallId = maps:get(<<"call_id">>, ToolReq),

    %% Push an event while the session is in waiting_tools
    EventData = #{<<"metric">> => <<"temp">>, <<"value">> => 22.3},
    publish_in(Config, #{<<"type">> => <<"event">>, <<"event">> => EventData}),

    %% Capturing handler — sends the received LLM request body to the test process
    Self = self(),
    ok = emqx_agent_llm_mock:set_handler(fun(Req0, State) ->
        {ok, Raw, Req1} = cowboy_req:read_body(Req0),
        Self ! {llm_body, emqx_utils_json:decode(Raw)},
        emqx_agent_llm_mock:simple_finish(Req1, State)
    end),

    publish_in(Config, #{
        <<"type">> => <<"tool_result">>,
        <<"call_id">> => CallId,
        <<"ok">> => true,
        <<"data">> => #{}
    }),

    LLMBody =
        receive
            {llm_body, B} -> B
        after ?TIMEOUT -> ct:fail("no llm body")
        end,
    Messages = maps:get(<<"messages">>, LLMBody),
    %% The event should appear as a user-role message after the first turn
    UserContents = [
        maps:get(<<"content">>, M)
     || M <- Messages, maps:get(<<"role">>, M, undefined) =:= <<"user">>
    ],
    ?assert(length(UserContents) >= 2, "expected at least 2 user messages"),
    ?assert(
        lists:any(
            fun
                (C) when is_binary(C) ->
                    binary:match(C, <<"temp">>) =/= nomatch;
                (_) ->
                    false
            end,
            UserContents
        ),
        "event content not found in LLM messages"
    ),

    _Final = recv_out(Config).

%% stop_on_finish=false: after publishing final the session returns to
%% idle and accepts a second request.
t_stop_on_finish_false_keeps_session(Config) ->
    ok = emqx_agent_llm_mock:set_handler(simple_finish),
    publish_in(Config, request(Config, #{<<"stop_on_finish">> => false})),

    _Final1 = recv_out(Config),

    Sid = ?config(sid, Config),
    ?assertNotEqual(undefined, global:whereis_name({emqx_agent_session, Sid})),

    ok = emqx_agent_llm_mock:set_handler(simple_finish),
    publish_in(Config, request(Config, #{<<"stop_on_finish">> => true})),

    Final2 = recv_out(Config),
    ?assertMatch(#{<<"type">> := <<"final">>}, Final2).

%% An explicit `stop` frame terminates the session process.
t_explicit_stop_frame(Config) ->
    ok = emqx_agent_llm_mock:set_handler(simple_finish),
    publish_in(Config, request(Config, #{<<"stop_on_finish">> => false})),
    _Final = recv_out(Config),

    Sid = ?config(sid, Config),
    Pid = global:whereis_name({emqx_agent_session, Sid}),
    ?assertNotEqual(undefined, Pid),
    Ref = monitor(process, Pid),

    publish_in(Config, #{<<"type">> => <<"stop">>}),

    receive
        {'DOWN', Ref, process, Pid, normal} -> ok
    after ?TIMEOUT ->
        ct:fail("session did not stop after stop frame")
    end.

%% A 500 response from the LLM causes the session process to terminate.
t_llm_http_error_terminates_session(Config) ->
    ok = emqx_agent_llm_mock:set_handler(http_error),
    Sid = ?config(sid, Config),
    publish_in(Config, request(Config, #{})),

    Pid = wait_for_session(Sid),
    Ref = monitor(process, Pid),

    receive
        {'DOWN', Ref, process, Pid, _Reason} -> ok
    after ?TIMEOUT ->
        ct:fail("session did not stop after LLM HTTP error")
    end.

%% A duplicate request while processing is silently ignored; the session
%% completes the original round-trip and produces exactly one final frame.
t_duplicate_request_ignored(Config) ->
    ok = emqx_agent_llm_mock:set_handler(tool_call),
    publish_in(Config, request(Config, #{})),

    ToolReq = recv_out(Config),
    CallId = maps:get(<<"call_id">>, ToolReq),

    %% Duplicate request while the session is in waiting_tools state
    publish_in(Config, request(Config, #{})),

    ok = emqx_agent_llm_mock:set_handler(simple_finish),
    publish_in(Config, #{
        <<"type">> => <<"tool_result">>,
        <<"call_id">> => CallId,
        <<"ok">> => true,
        <<"data">> => #{}
    }),

    Final = recv_out(Config),
    ?assertMatch(#{<<"type">> := <<"final">>}, Final),

    %% No extra frames should arrive
    ?assertEqual(timeout, recv_out_or_timeout(Config)).

%% Messages on unrelated topics do not trigger session creation.
t_non_session_topic_ignored(Config) ->
    Sid = ?config(sid, Config),
    Msg = emqx_message:make(?MODULE, 0, <<"other/topic">>, <<"hello">>),
    emqx_broker:publish(Msg),
    ?assertEqual(undefined, global:whereis_name({emqx_agent_session, Sid})).

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

in_topic(Sid) -> <<"sess/", Sid/binary, "/in">>.
out_topic(Sid) -> <<"sess/", Sid/binary, "/out">>.

request(Config, Overrides) ->
    Sid = ?config(sid, Config),
    BaseUrl = ?config(base_url, Config),
    maps:merge(
        #{
            <<"type">> => <<"request">>,
            <<"sid">> => Sid,
            <<"iid">> => <<"iid-1">>,
            <<"trace_id">> => <<"tr-1">>,
            <<"api_key">> => <<"sk-test">>,
            <<"base_url">> => BaseUrl,
            <<"model">> => <<"gpt-4o">>,
            <<"tools">> => [],
            <<"input">> => #{<<"event">> => <<"test">>},
            <<"instructions">> => <<"Do something.">>,
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
    after ?TIMEOUT ->
        ct:fail("no message on ~s within ~b ms", [Topic, ?TIMEOUT])
    end.

recv_out_or_timeout(Config) ->
    Sid = ?config(sid, Config),
    Topic = out_topic(Sid),
    receive
        #deliver{topic = Topic} -> received
    after 500 ->
        timeout
    end.

wait_for_session(Sid) ->
    wait_for_session(Sid, 50).

wait_for_session(_Sid, 0) ->
    ct:fail("session never started");
wait_for_session(Sid, N) ->
    case global:whereis_name({emqx_agent_session, Sid}) of
        undefined ->
            timer:sleep(100),
            wait_for_session(Sid, N - 1);
        Pid ->
            Pid
    end.
