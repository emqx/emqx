%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% Integration tests for the pipeline engine.
%%
%% These tests are fully deterministic — no LLM required.
%%
%% Strategy
%%   - Register minimal pipeline definitions directly via the registry.
%%   - Publish trigger events through the broker (fires the hook).
%%   - Use emqx:subscribe to observe pipe/.../events and cap/... topics.
%%   - For call_skill tests the emqx_agent_skill_publish skill is used
%%     (it executes immediately and replies with cap/<type>/<id>/response/<req_id>).
%%   - wait_for_event tests publish the awaited event explicitly.

-module(emqx_agent_pipeline_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("emqx/include/emqx.hrl").
-include("emqx_agent_pipeline.hrl").

-define(SHORT_TIMEOUT, 5_000).
-define(PIPE_EVENTS_FILTER, <<"pipe/+/inst/+/events">>).

%%--------------------------------------------------------------------
%% CT callbacks
%%--------------------------------------------------------------------

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [
            emqx,
            emqx_conf,
            emqx_resource,
            {emqx_ai_completion, "ai {}"},
            emqx_agent
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{suite_apps, Apps} | Config].

end_per_suite(Config) ->
    emqx_cth_suite:stop(?config(suite_apps, Config)).

init_per_testcase(TestCase, Config) ->
    PipelineId = atom_to_binary(TestCase, utf8),
    emqx:subscribe(?PIPE_EVENTS_FILTER),
    [{pipeline_id, PipelineId} | Config].

end_per_testcase(TestCase, Config) ->
    PipelineId = ?config(pipeline_id, Config),
    emqx_agent_pipeline_registry:unregister(PipelineId),
    emqx:unsubscribe(?PIPE_EVENTS_FILTER),
    %% Also clean up any skill instances registered during the test.
    _ = emqx_agent_skill_registry:clear_runtime_for_test(),
    _ = emqx_ai_completion_config:update_providers_raw({delete, <<"test-provider">>}),
    ok.

%%--------------------------------------------------------------------
%% Test cases
%%--------------------------------------------------------------------

%% A trigger event on the registered topic must start a pipeline instance
%% and emit a pipeline_started event.
t_trigger_starts_pipeline(Config) ->
    PipelineId = ?config(pipeline_id, Config),
    register_pipeline(PipelineId, <<"evt/test/", PipelineId/binary>>, []),
    publish_evt(<<"evt/test/", PipelineId/binary>>, #{<<"x">> => 1}),
    Started = recv_pipe_event(PipelineId),
    ?assertEqual(<<"pipeline_started">>, maps:get(<<"type">>, Started)).

%% A pipeline with a single call_skill step should invoke the skill and
%% then complete.
t_call_skill_completes(Config) ->
    PipelineId = ?config(pipeline_id, Config),
    SkillId = PipelineId,
    TrigTopic = <<"evt/test/", PipelineId/binary>>,
    setup_publish_skill(SkillId),
    Step = #{
        <<"id">> => <<"notify">>,
        <<"type">> => <<"call_skill">>,
        <<"skill">> => <<"message__publish@", SkillId/binary>>,
        <<"args">> => #{
            <<"topic">> => <<"output">>,
            <<"payload">> => <<"hello">>
        },
        <<"result_path">> => <<"$.notify_result">>
    },
    register_pipeline(PipelineId, TrigTopic, [Step]),
    publish_evt(TrigTopic, #{<<"id">> => <<"e1">>}),
    %% pipeline_started
    Started = recv_pipe_event(PipelineId),
    ?assertMatch(#{<<"type">> := <<"pipeline_started">>}, Started),
    %% pipeline_completed
    Completed = recv_pipe_event(PipelineId),
    ?assertMatch(#{<<"type">> := <<"pipeline_completed">>}, Completed),
    %% The notify_result should be in context
    Ctx = maps:get(<<"context">>, Completed),
    ?assertMatch(
        #{<<"status">> := <<"ok">>},
        maps:get(<<"notify_result">>, Ctx, #{})
    ).

%% A two-step pipeline: call_skill → call_skill.
%% Both steps must complete and the final context must carry both results.
t_multi_step_pipeline(Config) ->
    PipelineId = ?config(pipeline_id, Config),
    SkillId = PipelineId,
    TrigTopic = <<"evt/test/", PipelineId/binary>>,
    setup_publish_skill(SkillId),
    Steps = [
        #{
            <<"id">> => <<"step1">>,
            <<"type">> => <<"call_skill">>,
            <<"skill">> => <<"message__publish@", SkillId/binary>>,
            <<"args">> => #{<<"topic">> => <<"s1">>, <<"payload">> => <<"p1">>},
            <<"result_path">> => <<"$.step1">>
        },
        #{
            <<"id">> => <<"step2">>,
            <<"type">> => <<"call_skill">>,
            <<"skill">> => <<"message__publish@", SkillId/binary>>,
            <<"args">> => #{<<"topic">> => <<"s2">>, <<"payload">> => <<"p2">>},
            <<"result_path">> => <<"$.step2">>
        }
    ],
    register_pipeline(PipelineId, TrigTopic, Steps),
    publish_evt(TrigTopic, #{<<"id">> => <<"e2">>}),
    _Started = recv_pipe_event(PipelineId),
    Completed = recv_pipe_event(PipelineId),
    ?assertMatch(#{<<"type">> := <<"pipeline_completed">>}, Completed),
    Ctx = maps:get(<<"context">>, Completed),
    ?assertMatch(#{<<"status">> := <<"ok">>}, maps:get(<<"step1">>, Ctx, #{})),
    ?assertMatch(#{<<"status">> := <<"ok">>}, maps:get(<<"step2">>, Ctx, #{})).

%% A pipeline with a wait_for_event step must pause execution until the
%% waited event arrives on the expected topic.
t_wait_for_event(Config) ->
    PipelineId = ?config(pipeline_id, Config),
    SkillId = PipelineId,
    TrigTopic = <<"evt/test/", PipelineId/binary>>,
    WaitTopic = <<"evt/test/waited/", PipelineId/binary>>,
    setup_publish_skill(SkillId),
    Steps = [
        #{
            <<"id">> => <<"step1">>,
            <<"type">> => <<"call_skill">>,
            <<"skill">> => <<"message__publish@", SkillId/binary>>,
            <<"args">> => #{<<"topic">> => <<"pre">>, <<"payload">> => <<"before">>},
            <<"result_path">> => <<"$.pre">>
        },
        #{
            <<"id">> => <<"wait">>,
            <<"type">> => <<"wait_for_event">>,
            <<"topic">> => WaitTopic,
            <<"result_path">> => <<"$.waited_event">>
        },
        #{
            <<"id">> => <<"step3">>,
            <<"type">> => <<"call_skill">>,
            <<"skill">> => <<"message__publish@", SkillId/binary>>,
            <<"args">> => #{<<"topic">> => <<"post">>, <<"payload">> => <<"after">>},
            <<"result_path">> => <<"$.post">>
        }
    ],
    register_pipeline(PipelineId, TrigTopic, Steps),
    publish_evt(TrigTopic, #{<<"id">> => <<"e3">>}),
    Started = recv_pipe_event(PipelineId),
    ?assertMatch(#{<<"type">> := <<"pipeline_started">>}, Started),
    %% Pipeline should NOT complete yet — it is blocked at wait_for_event.
    ?assertEqual(timeout, recv_pipe_event_or_timeout(PipelineId, 500)),
    %% Now publish the waited event.
    WaitedEvent = #{<<"id">> => <<"we-1">>, <<"data">> => #{<<"x">> => 42}},
    publish_evt(WaitTopic, WaitedEvent),
    %% Pipeline must now complete.
    Completed = recv_pipe_event(PipelineId),
    ?assertMatch(#{<<"type">> := <<"pipeline_completed">>}, Completed),
    Ctx = maps:get(<<"context">>, Completed),
    ?assertMatch(#{<<"status">> := <<"ok">>}, maps:get(<<"pre">>, Ctx, #{})),
    ?assertMatch(#{<<"status">> := <<"ok">>}, maps:get(<<"post">>, Ctx, #{})),
    %% waited_event should be the event we published
    WaitedCtx = maps:get(<<"waited_event">>, Ctx, #{}),
    ?assertEqual(<<"we-1">>, maps:get(<<"id">>, WaitedCtx, undefined)).

%% A wait_for_event step with a `where` filter must ignore events that do
%% not satisfy the condition, and resume only when a matching one arrives.
t_wait_for_event_with_where(Config) ->
    PipelineId = ?config(pipeline_id, Config),
    TrigTopic = <<"evt/test/", PipelineId/binary>>,
    WaitTopic = <<"evt/test/update/", PipelineId/binary>>,
    Steps = [
        #{
            <<"id">> => <<"wait">>,
            <<"type">> => <<"wait_for_event">>,
            <<"topic">> => WaitTopic,
            <<"where">> => <<"data.ref_id == $.event.id">>,
            <<"result_path">> => <<"$.update">>
        }
    ],
    register_pipeline(PipelineId, TrigTopic, Steps),
    publish_evt(TrigTopic, #{<<"id">> => <<"trigger-99">>}),
    _Started = recv_pipe_event(PipelineId),
    %% Wrong ref_id — must be ignored.
    publish_evt(WaitTopic, #{<<"data">> => #{<<"ref_id">> => <<"wrong">>}}),
    ?assertEqual(timeout, recv_pipe_event_or_timeout(PipelineId, 500)),
    %% Correct ref_id — pipeline must resume and complete.
    publish_evt(WaitTopic, #{<<"data">> => #{<<"ref_id">> => <<"trigger-99">>}}),
    Completed = recv_pipe_event(PipelineId),
    ?assertEqual(<<"pipeline_completed">>, maps:get(<<"type">>, Completed)).

%% The triggering event must be accessible as $.event inside the pipeline
%% context and reachable for arg resolution in subsequent steps.
t_context_propagation(Config) ->
    PipelineId = ?config(pipeline_id, Config),
    SkillId = PipelineId,
    TrigTopic = <<"evt/test/", PipelineId/binary>>,
    setup_publish_skill(SkillId),
    Steps = [
        #{
            <<"id">> => <<"echo">>,
            <<"type">> => <<"call_skill">>,
            <<"skill">> => <<"message__publish@", SkillId/binary>>,
            %% Resolve $.event (a map) — it becomes the args value.
            %% Only the <<"topic">> and <<"payload">> keys are used by the skill;
            %% we supply them as literals here.
            <<"args">> => #{
                <<"topic">> => <<"ctx_test">>,
                <<"payload">> => <<"ctx_payload">>
            },
            <<"result_path">> => <<"$.echo">>
        }
    ],
    register_pipeline(PipelineId, TrigTopic, Steps),
    publish_evt(TrigTopic, #{<<"id">> => <<"ctx-evt">>, <<"data">> => #{<<"v">> => 7}}),
    _Started = recv_pipe_event(PipelineId),
    Completed = recv_pipe_event(PipelineId),
    ?assertMatch(#{<<"type">> := <<"pipeline_completed">>}, Completed),
    %% $.event must be in the final context
    Ctx = maps:get(<<"context">>, Completed),
    ?assertMatch(
        #{<<"id">> := <<"ctx-evt">>},
        maps:get(<<"event">>, Ctx, #{})
    ).

%% A break step must finish the pipeline when the selected context value is true.
t_break_stops_pipeline_when_true(Config) ->
    PipelineId = ?config(pipeline_id, Config),
    SkillId = PipelineId,
    TrigTopic = <<"evt/test/", PipelineId/binary>>,
    setup_publish_skill(SkillId),
    Steps = [
        #{
            <<"id">> => <<"break1">>,
            <<"type">> => <<"break">>,
            <<"path">> => <<"$.event.data.stop">>
        },
        #{
            <<"id">> => <<"should_not_run">>,
            <<"type">> => <<"call_skill">>,
            <<"skill">> => <<"message__publish@", SkillId/binary>>,
            <<"args">> => #{<<"topic">> => <<"post-break">>, <<"payload">> => <<"x">>},
            <<"result_path">> => <<"$.post">>
        }
    ],
    register_pipeline(PipelineId, TrigTopic, Steps),
    publish_evt(TrigTopic, #{<<"id">> => <<"e-break-1">>, <<"data">> => #{<<"stop">> => true}}),
    _Started = recv_pipe_event(PipelineId),
    Completed = recv_pipe_event(PipelineId),
    ?assertMatch(#{<<"type">> := <<"pipeline_completed">>}, Completed),
    Ctx = maps:get(<<"context">>, Completed),
    ?assertEqual(undefined, maps:get(<<"post">>, Ctx, undefined)).

%% With not=true, break must finish when the selected value is not true.
t_break_with_not_stops_pipeline_when_not_true(Config) ->
    PipelineId = ?config(pipeline_id, Config),
    SkillId = PipelineId,
    TrigTopic = <<"evt/test/", PipelineId/binary>>,
    setup_publish_skill(SkillId),
    Steps = [
        #{
            <<"id">> => <<"break1">>,
            <<"type">> => <<"break">>,
            <<"path">> => <<"$.event.data.keep_going">>,
            <<"not">> => true
        },
        #{
            <<"id">> => <<"should_not_run">>,
            <<"type">> => <<"call_skill">>,
            <<"skill">> => <<"message__publish@", SkillId/binary>>,
            <<"args">> => #{<<"topic">> => <<"post-break-not">>, <<"payload">> => <<"x">>},
            <<"result_path">> => <<"$.post">>
        }
    ],
    register_pipeline(PipelineId, TrigTopic, Steps),
    publish_evt(TrigTopic, #{
        <<"id">> => <<"e-break-2">>, <<"data">> => #{<<"keep_going">> => false}
    }),
    _Started = recv_pipe_event(PipelineId),
    Completed = recv_pipe_event(PipelineId),
    ?assertMatch(#{<<"type">> := <<"pipeline_completed">>}, Completed),
    Ctx = maps:get(<<"context">>, Completed),
    ?assertEqual(undefined, maps:get(<<"post">>, Ctx, undefined)).

%% After completion the pipeline enters done state and stops when the idle
%% timer fires.  We override the timer by sending idle_timeout directly.
t_done_idle_stop(Config) ->
    PipelineId = ?config(pipeline_id, Config),
    TrigTopic = <<"evt/test/", PipelineId/binary>>,
    register_pipeline(PipelineId, TrigTopic, []),
    publish_evt(TrigTopic, #{<<"id">> => <<"e5">>}),
    Started = recv_pipe_event(PipelineId),
    ?assertMatch(#{<<"type">> := <<"pipeline_started">>}, Started),
    Completed = recv_pipe_event(PipelineId),
    ?assertMatch(#{<<"type">> := <<"pipeline_completed">>}, Completed),
    %% Find the pipeline process and send idle_timeout to force-stop it.
    Iid = maps:get(<<"iid">>, Completed),
    Pid = emqx_agent_pipeline:whereis(Iid),
    ?assertNotEqual(undefined, Pid),
    Ref = monitor(process, Pid),
    Pid ! idle_timeout,
    receive
        {'DOWN', Ref, process, Pid, normal} -> ok
    after ?SHORT_TIMEOUT ->
        ct:fail("pipeline did not stop after idle_timeout")
    end.

%% A message arriving at a done pipeline causes it to restart from step 0.
t_done_restarts_on_message(Config) ->
    PipelineId = ?config(pipeline_id, Config),
    SkillId = PipelineId,
    TrigTopic = <<"evt/test/", PipelineId/binary>>,
    setup_publish_skill(SkillId),
    Steps = [
        #{
            <<"id">> => <<"s">>,
            <<"type">> => <<"call_skill">>,
            <<"skill">> => <<"message__publish@", SkillId/binary>>,
            <<"args">> => #{<<"topic">> => <<"r">>, <<"payload">> => <<"x">>},
            <<"result_path">> => <<"$.r">>
        }
    ],
    register_pipeline(PipelineId, TrigTopic, Steps),
    publish_evt(TrigTopic, #{<<"id">> => <<"e6">>}),
    _S1 = recv_pipe_event(PipelineId),
    Completed1 = recv_pipe_event(PipelineId),
    ?assertMatch(#{<<"type">> := <<"pipeline_completed">>}, Completed1),
    Iid = maps:get(<<"iid">>, Completed1),
    Pid = emqx_agent_pipeline:whereis(Iid),
    %% Send a cast to trigger the "restart from done" path.
    gen_statem:cast(Pid, #pipe_evt{
        topic = <<"evt/test/something">>, event = #{<<"id">> => <<"restart">>}
    }),
    %% The pipeline should restart and produce another started + completed pair.
    Started2 = recv_pipe_event(PipelineId),
    ?assertMatch(#{<<"type">> := <<"pipeline_started">>}, Started2),
    Completed2 = recv_pipe_event(PipelineId),
    ?assertEqual(<<"pipeline_completed">>, maps:get(<<"type">>, Completed2)).

%% Step 1 writes its result to $.lookup.  Step 2 reads $.lookup.topic
%% out of context and uses it as the `topic` arg for the skill call.
%% This verifies that result_path + arg resolution actually chain across steps.
t_context_flows_between_steps(Config) ->
    PipelineId = ?config(pipeline_id, Config),
    SkillId = PipelineId,
    TrigTopic = <<"evt/test/", PipelineId/binary>>,
    setup_publish_skill(SkillId),
    Steps = [
        %% Step 1 — publishes to "first" and stores the skill reply at $.lookup.
        %% The reply from message__publish is #{status => ok, topic => "test/first"}.
        #{
            <<"id">> => <<"step1">>,
            <<"type">> => <<"call_skill">>,
            <<"skill">> => <<"message__publish@", SkillId/binary>>,
            <<"args">> => #{
                <<"topic">> => <<"first">>,
                <<"payload">> => <<"ping">>
            },
            <<"result_path">> => <<"$.lookup">>
        },
        %% Step 2 — uses $.lookup.result.topic (= "test/first") as the payload, proving
        %% that the previous step's result is visible to the next step's args.
        #{
            <<"id">> => <<"step2">>,
            <<"type">> => <<"call_skill">>,
            <<"skill">> => <<"message__publish@", SkillId/binary>>,
            <<"args">> => #{
                <<"topic">> => <<"second">>,
                <<"payload">> => <<"$.lookup.result.topic">>
            },
            <<"result_path">> => <<"$.echo">>
        }
    ],
    register_pipeline(PipelineId, TrigTopic, Steps),

    %% Subscribe to the raw publish-skill output so we can inspect what payload
    %% step 2 actually sent.
    emqx:subscribe(<<"test/second">>),

    publish_evt(TrigTopic, #{<<"id">> => <<"chain-1">>}),
    _Started = recv_pipe_event(PipelineId),
    Completed = recv_pipe_event(PipelineId),
    ?assertMatch(#{<<"type">> := <<"pipeline_completed">>}, Completed),

    %% Context must have both step results.
    Ctx = maps:get(<<"context">>, Completed),
    ?assertMatch(
        #{<<"status">> := <<"ok">>, <<"result">> := #{<<"topic">> := <<"test/first">>}},
        maps:get(<<"lookup">>, Ctx, #{})
    ),
    ?assertMatch(#{<<"status">> := <<"ok">>}, maps:get(<<"echo">>, Ctx, #{})),

    %% The message published by step 2 must carry the resolved topic string.
    receive
        #deliver{topic = <<"test/second">>, message = #message{payload = P}} ->
            ?assertEqual(<<"test/first">>, P)
    after ?SHORT_TIMEOUT ->
        ct:fail("step 2 never published to test/second")
    end.

%% An llm_loop step that declares a set_result_schema must accept a
%% set_result tool call from the LLM, store the args, and write them to
%% result_path when the session publishes the final frame.
%%
%% Strategy: the provider points at a closed local port, so the real session
%% cannot produce normal LLM frames.  We then drive the pipeline manually
%% by casting #sess_frame records directly, bypassing the LLM entirely.
%%
%% Gen_statem ordering guarantee: the pipeline processes its queued internal
%% `step` event (which transitions it to llm_loop) *before* it processes any
%% cast from our test process, so the state is always llm_loop when our casts
%% arrive.
t_set_result_writes_to_context(Config) ->
    PipelineId = ?config(pipeline_id, Config),
    TrigTopic = <<"evt/test/", PipelineId/binary>>,
    StepId = <<"llm">>,
    Sid = <<PipelineId/binary, "-", StepId/binary>>,
    ok = emqx_ai_completion_config:update_providers_raw(
        {add, #{
            <<"name">> => <<"test-provider">>,
            <<"type">> => <<"openai">>,
            <<"api_key">> => <<"test-key">>,
            <<"base_url">> => <<"http://127.0.0.1:1">>
        }}
    ),
    Step = #{
        <<"id">> => StepId,
        <<"type">> => <<"llm_loop">>,
        <<"model">> => <<"test-model">>,
        <<"instructions">> => <<"test">>,
        <<"provider_name">> => <<"test-provider">>,
        <<"tools">> => [],
        <<"input">> => #{<<"box_id">> => <<"b1">>},
        <<"set_result_schema">> => #{
            <<"type">> => <<"object">>,
            <<"properties">> => #{
                <<"status">> => #{<<"type">> => <<"string">>}
            },
            <<"required">> => [<<"status">>]
        },
        <<"result_path">> => <<"$.verdict">>
    },
    register_pipeline(PipelineId, TrigTopic, [Step]),
    publish_evt(TrigTopic, #{<<"id">> => <<"sr-1">>}),

    Started = recv_pipe_event(PipelineId),
    ?assertMatch(#{<<"type">> := <<"pipeline_started">>}, Started),
    Iid = maps:get(<<"iid">>, Started),
    Pid = emqx_agent_pipeline:whereis(Iid),
    ?assertNotEqual(undefined, Pid),

    %% Simulate the LLM calling set_result.  Gen_statem ordering guarantees
    %% the pipeline is in llm_loop when it processes this cast.
    gen_statem:cast(Pid, #sess_frame{
        sid = Sid,
        frame = #{
            <<"type">> => <<"tool_request">>,
            <<"call_id">> => <<"c-sr-1">>,
            <<"tool">> => <<"set_result">>,
            <<"args">> => #{<<"status">> => <<"approved">>}
        }
    }),

    %% Simulate the LLM finishing (set_result has already been stored).
    gen_statem:cast(Pid, #sess_frame{
        sid = Sid,
        frame = #{<<"type">> => <<"final">>}
    }),

    Completed = recv_pipe_event(PipelineId),
    ?assertMatch(#{<<"type">> := <<"pipeline_completed">>}, Completed),
    Ctx = maps:get(<<"context">>, Completed),
    Verdict = maps:get(<<"verdict">>, Ctx, #{}),
    ?assertEqual(<<"approved">>, maps:get(<<"status">>, Verdict, undefined)).

t_llm_loop_final_without_set_result_fails(Config) ->
    PipelineId = ?config(pipeline_id, Config),
    TrigTopic = <<"evt/test/", PipelineId/binary>>,
    StepId = <<"llm">>,
    Sid = <<PipelineId/binary, "-", StepId/binary>>,
    ok = emqx_ai_completion_config:update_providers_raw(
        {add, #{
            <<"name">> => <<"test-provider">>,
            <<"type">> => <<"openai">>,
            <<"api_key">> => <<"test-key">>,
            <<"base_url">> => <<"http://127.0.0.1:1">>
        }}
    ),
    Step = #{
        <<"id">> => StepId,
        <<"type">> => <<"llm_loop">>,
        <<"model">> => <<"test-model">>,
        <<"instructions">> => <<"test">>,
        <<"provider_name">> => <<"test-provider">>,
        <<"tools">> => [],
        <<"input">> => #{},
        <<"set_result_schema">> => #{
            <<"type">> => <<"object">>,
            <<"properties">> => #{
                <<"status">> => #{<<"type">> => <<"string">>}
            },
            <<"required">> => [<<"status">>]
        },
        <<"result_path">> => <<"$.verdict">>
    },
    register_pipeline(PipelineId, TrigTopic, [Step]),
    publish_evt(TrigTopic, #{<<"id">> => <<"sr-missing">>}),

    Started = recv_pipe_event(PipelineId),
    ?assertMatch(#{<<"type">> := <<"pipeline_started">>}, Started),
    Iid = maps:get(<<"iid">>, Started),
    Pid = emqx_agent_pipeline:whereis(Iid),
    ?assertNotEqual(undefined, Pid),

    gen_statem:cast(Pid, #sess_frame{
        sid = Sid,
        frame = #{
            <<"type">> => <<"final">>,
            <<"result">> => #{<<"summary">> => <<"could not complete">>}
        }
    }),

    Failed = recv_pipe_event(PipelineId),
    ?assertMatch(#{<<"type">> := <<"pipeline_failed">>}, Failed),
    ?assertEqual(<<"missing_set_result">>, maps:get(<<"reason">>, Failed)).

t_llm_loop_defaults_are_applied(Config) ->
    PipelineId = ?config(pipeline_id, Config),
    TrigTopic = <<"evt/test/", PipelineId/binary>>,
    Step = #{
        <<"id">> => <<"llm">>,
        <<"type">> => <<"llm_loop">>,
        <<"model">> => <<"test-model">>,
        <<"instructions">> => <<"test">>,
        <<"provider_name">> => <<"test-provider">>,
        <<"set_result_schema">> => #{
            <<"type">> => <<"object">>,
            <<"properties">> => #{<<"status">> => #{<<"type">> => <<"string">>}}
        },
        <<"result_path">> => <<"$.result">>
    },
    register_pipeline(PipelineId, TrigTopic, [Step]),
    {ok, #{<<"steps">> := [Stored]}} = emqx_agent_pipeline_registry:lookup(PipelineId),
    ?assertMatch(#{<<"tools">> := []}, Stored),
    ?assertMatch(#{<<"input">> := #{}}, Stored),
    ?assertMatch(#{<<"stop_on_finish">> := true}, Stored),
    ?assertEqual(2048, maps:get(<<"max_tokens">>, Stored)).

t_llm_loop_requires_set_result_schema(Config) ->
    PipelineId = ?config(pipeline_id, Config),
    TrigTopic = <<"evt/test/", PipelineId/binary>>,
    Step = #{
        <<"id">> => <<"llm">>,
        <<"type">> => <<"llm_loop">>,
        <<"model">> => <<"test-model">>,
        <<"instructions">> => <<"test">>,
        <<"provider_name">> => <<"test-provider">>,
        <<"result_path">> => <<"$.result">>
    },
    ?assertMatch(
        {error, {missing_step_field, 1, <<"set_result_schema">>}},
        emqx_agent_pipeline_registry:register(#{
            <<"pipeline_id">> => PipelineId,
            <<"active">> => true,
            <<"trigger">> => #{<<"topic">> => TrigTopic},
            <<"steps">> => [Step]
        })
    ).

t_llm_loop_requires_model(Config) ->
    PipelineId = ?config(pipeline_id, Config),
    TrigTopic = <<"evt/test/", PipelineId/binary>>,
    Step = #{
        <<"id">> => <<"llm">>,
        <<"type">> => <<"llm_loop">>,
        <<"instructions">> => <<"test">>,
        <<"provider_name">> => <<"test-provider">>,
        <<"set_result_schema">> => #{
            <<"type">> => <<"object">>,
            <<"properties">> => #{<<"status">> => #{<<"type">> => <<"string">>}}
        },
        <<"result_path">> => <<"$.result">>
    },
    ?assertMatch(
        {error, {missing_step_field, 1, <<"model">>}},
        emqx_agent_pipeline_registry:register(#{
            <<"pipeline_id">> => PipelineId,
            <<"active">> => true,
            <<"trigger">> => #{<<"topic">> => TrigTopic},
            <<"steps">> => [Step]
        })
    ).

%% Unregistered pipeline definitions must not trigger new instances.
t_unregistered_pipeline_not_triggered(Config) ->
    PipelineId = ?config(pipeline_id, Config),
    TrigTopic = <<"evt/test/", PipelineId/binary>>,
    %% Register then immediately unregister.
    register_pipeline(PipelineId, TrigTopic, []),
    emqx_agent_pipeline_registry:unregister(PipelineId),
    publish_evt(TrigTopic, #{<<"id">> => <<"e7">>}),
    ?assertEqual(timeout, recv_pipe_event_or_timeout(PipelineId, 500)).

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

register_pipeline(PipelineId, TrigTopic, Steps) ->
    Def = #{
        <<"pipeline_id">> => PipelineId,
        <<"active">> => true,
        <<"trigger">> => #{<<"topic">> => TrigTopic},
        <<"steps">> => Steps
    },
    ok = emqx_agent_pipeline_registry:register(Def).

setup_publish_skill(SkillId) ->
    {ok, Skill} = emqx_agent_skill_publish:create(#{
        skill_id => SkillId,
        desc => <<"test">>,
        topic_prefix => <<"test/">>
    }),
    emqx_agent_skill_registry:put_runtime_for_test(Skill).

publish_evt(Topic, Event) ->
    Payload = emqx_utils_json:encode(Event),
    Msg = emqx_message:make(?MODULE, 0, Topic, Payload),
    emqx_broker:publish(Msg).

recv_pipe_event(PipelineId) ->
    recv_pipe_event(PipelineId, ?SHORT_TIMEOUT).

recv_pipe_event(PipelineId, Timeout) ->
    receive
        #deliver{
            topic = <<"pipe/", _/binary>>,
            message = #message{payload = P}
        } ->
            Frame = emqx_utils_json:decode(P),
            case maps:get(<<"pipeline_id">>, Frame, undefined) of
                PipelineId ->
                    Frame;
                _Other ->
                    %% From a different pipeline (e.g. parallel test) — re-try.
                    recv_pipe_event(PipelineId, Timeout)
            end
    after Timeout ->
        ct:fail("no pipe event for pipeline ~s within ~b ms", [PipelineId, Timeout])
    end.

recv_pipe_event_or_timeout(PipelineId, Timeout) ->
    receive
        #deliver{
            topic = <<"pipe/", _/binary>>,
            message = #message{payload = P}
        } ->
            Frame = emqx_utils_json:decode(P),
            case maps:get(<<"pipeline_id">>, Frame, undefined) of
                PipelineId -> Frame;
                _Other -> recv_pipe_event_or_timeout(PipelineId, Timeout)
            end
    after Timeout ->
        timeout
    end.
