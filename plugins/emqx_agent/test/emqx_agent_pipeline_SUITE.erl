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

-module(emqx_agent_pipeline_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("emqx/include/emqx.hrl").
-include("emqx_agent_pipeline.hrl").

-define(SHORT_TIMEOUT, 5_000).
-define(PIPE_EVENTS_FILTER, <<"$pipe/+/inst/+/events">>).

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
    ok = emqx_agent_plugin_config_fixture:setup(),
    emqx:subscribe(?PIPE_EVENTS_FILTER),
    [{pipeline_id, PipelineId} | Config].

end_per_testcase(_TestCase, _Config) ->
    emqx:unsubscribe(?PIPE_EVENTS_FILTER),
    _ = emqx_ai_completion_config:update_providers_raw({delete, <<"test-provider">>}),
    ok = emqx_agent_plugin_config_fixture:teardown(),
    ok.

%%--------------------------------------------------------------------
%% Test cases
%%--------------------------------------------------------------------

%% A trigger event on the registered topic must start a pipeline instance
%% and emit a pipeline_started event.
t_trigger_starts_pipeline(Config) ->
    PipelineId = ?config(pipeline_id, Config),
    register_pipeline(PipelineId, <<"$evt/test/", PipelineId/binary>>, []),
    publish_evt(<<"$evt/test/", PipelineId/binary>>, #{<<"x">> => 1}),
    Started = recv_pipe_event(PipelineId),
    ?assertEqual(<<"pipeline_started">>, maps:get(<<"type">>, Started)).

%% A pipeline with a single call_skill step should invoke the skill and
%% then complete.
t_call_skill_completes(Config) ->
    PipelineId = ?config(pipeline_id, Config),
    SkillId = PipelineId,
    TrigTopic = <<"$evt/test/", PipelineId/binary>>,
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
    setup_publish_skill(SkillId),
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
    TrigTopic = <<"$evt/test/", PipelineId/binary>>,
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
    setup_publish_skill(SkillId),
    publish_evt(TrigTopic, #{<<"id">> => <<"e2">>}),
    _Started = recv_pipe_event(PipelineId),
    Completed = recv_pipe_event(PipelineId),
    ?assertMatch(#{<<"type">> := <<"pipeline_completed">>}, Completed),
    Ctx = maps:get(<<"context">>, Completed),
    ?assertMatch(#{<<"status">> := <<"ok">>}, maps:get(<<"step1">>, Ctx, #{})),
    ?assertMatch(#{<<"status">> := <<"ok">>}, maps:get(<<"step2">>, Ctx, #{})).

%% The triggering event must be accessible as $.event inside the pipeline
%% context and reachable for arg resolution in subsequent steps.
t_context_propagation(Config) ->
    PipelineId = ?config(pipeline_id, Config),
    SkillId = PipelineId,
    TrigTopic = <<"$evt/test/", PipelineId/binary>>,
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
    setup_publish_skill(SkillId),
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
    TrigTopic = <<"$evt/test/", PipelineId/binary>>,
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
    setup_publish_skill(SkillId),
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
    TrigTopic = <<"$evt/test/", PipelineId/binary>>,
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
    setup_publish_skill(SkillId),
    publish_evt(TrigTopic, #{
        <<"id">> => <<"e-break-2">>, <<"data">> => #{<<"keep_going">> => false}
    }),
    _Started = recv_pipe_event(PipelineId),
    Completed = recv_pipe_event(PipelineId),
    ?assertMatch(#{<<"type">> := <<"pipeline_completed">>}, Completed),
    Ctx = maps:get(<<"context">>, Completed),
    ?assertEqual(undefined, maps:get(<<"post">>, Ctx, undefined)).

%% A one-off pipeline terminates after publishing completion.
t_one_off_pipeline_stops_on_completion(Config) ->
    PipelineId = ?config(pipeline_id, Config),
    TrigTopic = <<"$evt/test/", PipelineId/binary>>,
    {ok, Pid} = start_pipeline_direct(PipelineId, TrigTopic, [], #{<<"id">> => <<"e5">>}),
    Ref = monitor(process, Pid),
    Started = recv_pipe_event(PipelineId),
    ?assertMatch(#{<<"type">> := <<"pipeline_started">>}, Started),
    Completed = recv_pipe_event(PipelineId),
    ?assertMatch(#{<<"type">> := <<"pipeline_completed">>}, Completed),
    receive
        {'DOWN', Ref, process, Pid, Reason} when Reason =:= normal; Reason =:= noproc -> ok
    after ?SHORT_TIMEOUT ->
        ct:fail("pipeline did not stop after completion")
    end,
    Ctx = maps:get(<<"context">>, Completed),
    ?assertEqual(TrigTopic, maps:get(<<"key">>, Ctx)),
    ?assertEqual(emqx_base62:encode(TrigTopic), maps:get(<<"key_base62">>, Ctx)).

%% Step 1 writes its result to $.lookup.  Step 2 reads $.lookup.topic
%% out of context and uses it as the `topic` arg for the skill call.
%% This verifies that result_path + arg resolution actually chain across steps.
t_context_flows_between_steps(Config) ->
    PipelineId = ?config(pipeline_id, Config),
    SkillId = PipelineId,
    TrigTopic = <<"$evt/test/", PipelineId/binary>>,
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
    setup_publish_skill(SkillId),

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
    TrigTopic = <<"$evt/test/", PipelineId/binary>>,
    StepId = <<"llm">>,
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
        <<"set_result_schema">> => set_result_schema(),
        <<"result_path">> => <<"$.verdict">>
    },
    {ok, Pid} = start_pipeline_direct(PipelineId, TrigTopic, [Step], #{<<"id">> => <<"sr-1">>}),

    Started = recv_pipe_event(PipelineId),
    ?assertMatch(#{<<"type">> := <<"pipeline_started">>}, Started),
    Iid = maps:get(<<"iid">>, Started),
    Sid = ephemeral_sid(Iid, StepId),

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
    TrigTopic = <<"$evt/test/", PipelineId/binary>>,
    StepId = <<"llm">>,
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
        <<"set_result_schema">> => set_result_schema(),
        <<"result_path">> => <<"$.verdict">>
    },
    {ok, Pid} = start_pipeline_direct(PipelineId, TrigTopic, [Step], #{<<"id">> => <<"sr-missing">>}),

    Started = recv_pipe_event(PipelineId),
    ?assertMatch(#{<<"type">> := <<"pipeline_started">>}, Started),
    Iid = maps:get(<<"iid">>, Started),
    Sid = ephemeral_sid(Iid, StepId),

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
    TrigTopic = <<"$evt/test/", PipelineId/binary>>,
    Step = #{
        <<"id">> => <<"llm">>,
        <<"type">> => <<"llm_loop">>,
        <<"model">> => <<"test-model">>,
        <<"instructions">> => <<"test">>,
        <<"provider_name">> => <<"test-provider">>,
        <<"set_result_schema">> => set_result_schema(),
        <<"result_path">> => <<"$.result">>
    },
    register_pipeline(PipelineId, TrigTopic, [Step]),
    {ok, #{<<"key_expression">> := KeyExpression, <<"steps">> := [Stored]}} =
        emqx_agent_config:lookup_pipeline(PipelineId),
    ?assertMatch(#{<<"tools">> := []}, Stored),
    ?assertMatch(#{<<"input">> := #{}}, Stored),
    ?assertMatch(#{<<"persistent">> := false}, Stored),
    ?assertEqual(<<"message.topic">>, KeyExpression),
    ?assertEqual(2048, maps:get(<<"max_tokens">>, Stored)),
    ?assertEqual(50000, maps:get(<<"max_total_tokens">>, Stored)).

t_llm_loop_requires_set_result_schema(Config) ->
    PipelineId = ?config(pipeline_id, Config),
    TrigTopic = <<"$evt/test/", PipelineId/binary>>,
    Step = #{
        <<"id">> => <<"llm">>,
        <<"type">> => <<"llm_loop">>,
        <<"model">> => <<"test-model">>,
        <<"instructions">> => <<"test">>,
        <<"provider_name">> => <<"test-provider">>,
        <<"result_path">> => <<"$.result">>
    },
    ?assertMatch(
        {error, _},
        emqx_agent_service:pipeline_create(#{
            <<"pipeline_id">> => PipelineId,
            <<"active">> => true,
            <<"trigger">> => #{<<"topic">> => TrigTopic},
            <<"steps">> => [Step]
        })
    ).

t_llm_loop_requires_model(Config) ->
    PipelineId = ?config(pipeline_id, Config),
    TrigTopic = <<"$evt/test/", PipelineId/binary>>,
    Step = #{
        <<"id">> => <<"llm">>,
        <<"type">> => <<"llm_loop">>,
        <<"instructions">> => <<"test">>,
        <<"provider_name">> => <<"test-provider">>,
        <<"set_result_schema">> => set_result_schema(),
        <<"result_path">> => <<"$.result">>
    },
    ?assertMatch(
        {error, _},
        emqx_agent_service:pipeline_create(#{
            <<"pipeline_id">> => PipelineId,
            <<"active">> => true,
            <<"trigger">> => #{<<"topic">> => TrigTopic},
            <<"steps">> => [Step]
        })
    ).

t_pipeline_rejects_invalid_key_expression(Config) ->
    PipelineId = ?config(pipeline_id, Config),
    TrigTopic = <<"$evt/test/", PipelineId/binary>>,
    ?assertMatch(
        {error, _},
        emqx_agent_service:pipeline_create(#{
            <<"pipeline_id">> => PipelineId,
            <<"active">> => true,
            <<"key_expression">> => <<"not existing fun(">>,
            <<"trigger">> => #{<<"topic">> => TrigTopic},
            <<"steps">> => []
        })
    ).

t_pipeline_custom_key_expression(Config) ->
    PipelineId = ?config(pipeline_id, Config),
    TrigTopic = <<"$evt/test/", PipelineId/binary>>,
    Def = #{
        <<"pipeline_id">> => PipelineId,
        <<"active">> => true,
        <<"key_expression">> => <<"message.from">>,
        <<"trigger">> => #{<<"topic">> => TrigTopic},
        <<"steps">> => []
    },
    Msg = trigger_message(TrigTopic, #{<<"id">> => <<"key-1">>}),
    {ok, _Pid} = emqx_agent_pipeline_sup:start_pipeline(Def, #{
        event => #{<<"id">> => <<"key-1">>},
        message => Msg
    }),
    _Started = recv_pipe_event(PipelineId),
    Completed = recv_pipe_event(PipelineId),
    Ctx = maps:get(<<"context">>, Completed),
    From = atom_to_binary(?MODULE, utf8),
    ?assertEqual(From, maps:get(<<"key">>, Ctx)),
    ?assertEqual(emqx_base62:encode(From), maps:get(<<"key_base62">>, Ctx)).

%% Unregistered pipeline definitions must not trigger new instances.
t_unregistered_pipeline_not_triggered(Config) ->
    PipelineId = ?config(pipeline_id, Config),
    TrigTopic = <<"$evt/test/", PipelineId/binary>>,
    %% Register then immediately unregister.
    register_pipeline(PipelineId, TrigTopic, []),
    ok = emqx_agent_config:delete_pipeline(PipelineId),
    publish_evt(TrigTopic, #{<<"id">> => <<"e7">>}),
    ?assertEqual(timeout, recv_pipe_event_or_timeout(PipelineId, 500)).

t_pipeline_reply_timeout_fails_and_stops(Config) ->
    PipelineId = ?config(pipeline_id, Config),
    TrigTopic = <<"$evt/test/", PipelineId/binary>>,
    ProviderName = <<PipelineId/binary, "-provider">>,
    {Port, Sock} = listen_on_random_port(),
    Acceptor = spawn(fun() -> accept_and_hold(Sock) end),
    try
        ok = emqx_ai_completion_config:update_providers_raw(
            {add, #{
                <<"name">> => ProviderName,
                <<"type">> => <<"openai">>,
                <<"api_key">> => <<"test-key">>,
                <<"base_url">> => iolist_to_binary(io_lib:format("http://127.0.0.1:~b", [Port]))
            }}
        ),
        Step = #{
            <<"id">> => <<"llm_timeout">>,
            <<"type">> => <<"llm_loop">>,
            <<"model">> => <<"test-model">>,
            <<"instructions">> => <<"test">>,
            <<"provider_name">> => ProviderName,
            <<"tools">> => [],
            <<"input">> => #{},
            <<"set_result_schema">> => set_result_schema(),
            <<"timeout_ms">> => 100,
            <<"result_path">> => <<"$.result">>
        },
        register_pipeline(PipelineId, TrigTopic, [Step]),
        publish_evt(TrigTopic, #{<<"id">> => <<"timeout-1">>}),

        Started = recv_pipe_event(PipelineId),
        ?assertMatch(#{<<"type">> := <<"pipeline_started">>}, Started),

        Failed = recv_pipe_event(PipelineId),
        ?assertMatch(#{<<"type">> := <<"pipeline_failed">>}, Failed),
        ?assertEqual(<<"llm_reply_timeout">>, maps:get(<<"reason">>, Failed))
    after
        _ = emqx_ai_completion_config:update_providers_raw({delete, ProviderName}),
        exit(Acceptor, kill),
        gen_tcp:close(Sock)
    end.

t_rejects_non_dollar_evt_trigger(_Config) ->
    {error, {invalid_trigger_topic, Topic}} = emqx_agent_service:pipeline_create(
        #{
            <<"pipeline_id">> => <<"bad-trigger">>,
            <<"trigger">> => #{<<"topic">> => <<"evt/legacy">>},
            <<"steps">> => []
        }
    ),
    ?assertEqual(<<"evt/legacy">>, Topic).

t_rejects_non_dollar_evt_trigger_update(_Config) ->
    PipelineId = <<"bad-trigger-update">>,
    ok = emqx_agent_config:create_pipeline(#{
        <<"pipeline_id">> => PipelineId,
        <<"trigger">> => #{<<"topic">> => <<"$evt/ok">>},
        <<"steps">> => []
    }),
    {error, {invalid_trigger_topic, Topic}} = emqx_agent_config:update_pipeline(
        PipelineId,
        #{
            <<"pipeline_id">> => PipelineId,
            <<"trigger">> => #{<<"topic">> => <<"evt/bad">>},
            <<"steps">> => []
        }
    ),
    ?assertEqual(<<"evt/bad">>, Topic),
    ok = emqx_agent_config:delete_pipeline(PipelineId).

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
    ok = emqx_agent_service:pipeline_create(Def).

start_pipeline_direct(PipelineId, TrigTopic, Steps, Event) ->
    Def = #{
        <<"pipeline_id">> => PipelineId,
        <<"active">> => true,
        <<"trigger">> => #{<<"topic">> => TrigTopic},
        <<"steps">> => Steps
    },
    emqx_agent_pipeline_sup:start_pipeline(Def, #{
        event => Event, message => trigger_message(TrigTopic, Event)
    }).

trigger_message(Topic, Event) ->
    emqx_message:make(?MODULE, 0, Topic, emqx_utils_json:encode(Event)).

ephemeral_sid(Iid, StepId) ->
    <<"pipe-", (emqx_base62:encode(<<Iid/binary, 0, StepId/binary>>))/binary>>.

setup_publish_skill(SkillId) ->
    emqx_agent_config:create_skill(#{
        <<"type">> => <<"message__publish">>,
        <<"id">> => SkillId,
        <<"desc">> => <<"test">>,
        <<"topic_prefix">> => <<"test/">>,
        <<"payload_schema">> => emqx_utils_json:encode(#{<<"type">> => <<"string">>})
    }).

set_result_schema() ->
    #{
        <<"type">> => <<"object">>,
        <<"properties">> => #{<<"status">> => #{<<"type">> => <<"string">>}},
        <<"required">> => [<<"status">>],
        <<"additionalProperties">> => false
    }.

publish_evt(Topic, Event) ->
    emqx_broker:publish(trigger_message(Topic, Event)).

recv_pipe_event(PipelineId) ->
    recv_pipe_event(PipelineId, ?SHORT_TIMEOUT).

recv_pipe_event(PipelineId, Timeout) ->
    receive
        #deliver{
            topic = <<"$pipe/", _/binary>>,
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
            topic = <<"$pipe/", _/binary>>,
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

listen_on_random_port() ->
    SockOpts = [binary, {active, false}, {packet, raw}, {reuseaddr, true}, {backlog, 1000}],
    {ok, Sock} = gen_tcp:listen(0, SockOpts),
    {ok, Port} = inet:port(Sock),
    {Port, Sock}.

accept_and_hold(Sock) ->
    case gen_tcp:accept(Sock) of
        {ok, Client} -> hold_socket(Client);
        {error, closed} -> ok
    end.

hold_socket(Client) ->
    receive
        stop -> gen_tcp:close(Client)
    after 30_000 ->
        gen_tcp:close(Client)
    end.
