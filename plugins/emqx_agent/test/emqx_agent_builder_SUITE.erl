%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% End-to-end integration test for the Pipeline Builder demo.
%%
%% Requires the API key for EMQX_AGENT_TEST_LLM_PROVIDER.
%%
%% What this suite tests:
%%   1. All 9 meta-skills + builder-reply skill are registered.
%%   2. The pipeline-builder AI provider and pipeline are created.
%%   3. A natural-language request is published to evt/builder/request.
%%   4. The LLM uses the meta-skills to create the apple-box-inspection
%%      pipeline (skills + provider reference + pipeline definition).
%%   5. The suite verifies the resulting pipeline structure matches the
%%      reference definition from demo_apple_box_init.py.

-module(emqx_agent_builder_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("emqx/include/emqx.hrl").

-define(BUILDER_PIPELINE_ID, <<"pipeline-builder">>).
-define(SHARED_PROVIDER, <<"openai">>).
-define(REPLY_TOPIC, <<"builder/reply/#">>).
-define(REQUEST_TOPIC, <<"evt/builder/request">>).

%% Target pipeline the LLM is asked to build
-define(TARGET_PIPELINE_ID, <<"apple-box-inspection">>).

%% The LLM makes ~7 tool calls; be generous.
-define(LLM_TIMEOUT, 180_000).

-define(BUILDER_INSTRUCTIONS,
    ~b"""
    You are a Pipeline Architect for EMQX Agent — an intelligent system that helps users \
    design, create, and manage event-driven automation pipelines over MQTT.

    You have access to tools that let you fully manage the EMQX Agent runtime: query, \
    create and delete skills and pipelines, and query AI providers.

    ═══════════════════════════════════════════════════════
    CORE CONCEPTS
    ═══════════════════════════════════════════════════════

    SKILLS are named capability instances registered in the skill registry.
    Each skill has a TYPE (what it does) and an ID (its name within that type).
    Skills are referenced in pipeline steps as "type@id", e.g. "message.publish@my-notifier".

    An AI PROVIDER holds LLM credentials.
    It is referenced by provider_name inside an llm_loop step.
    Providers are pre-created by administrators and are not modified by agent tools.

    A PIPELINE reacts to MQTT events and executes an ordered list of steps.
    Steps can call skills, run LLM reasoning loops, wait for more MQTT events,
    or break out early based on conditions.
    Every trigger event spawns a new pipeline INSTANCE that runs the steps in sequence.

    ═══════════════════════════════════════════════════════
    HOW SKILLS, PROVIDERS, AND PIPELINES FIT TOGETHER
    ═══════════════════════════════════════════════════════

    Building a working pipeline requires these steps:

      1. Create SKILLS — register the capabilities the pipeline will use.
      2. Choose an existing AI PROVIDER for llm_loop provider_name.
      3. Create the PIPELINE — wire everything together.

    ═══════════════════════════════════════════════════════
    SKILL TYPES
    ═══════════════════════════════════════════════════════

      message.publish   Publish to an MQTT topic.
                        Required: id, desc, topic_prefix

      message.request   MQTT request/reply.
                        Required: id, desc, topic_prefix
                        Optional: request_payload_schema

      http              HTTP call to an external service.
                        Required: id, desc, method, url, input_schema

      postgresql.query  Execute a parameterised SQL query.
                        Required: id, desc, query (using ${var} placeholders).
                        Placeholder names are extracted automatically and the input
                        schema is generated from them. No arg_keys or input_schema needed.

    ═══════════════════════════════════════════════════════
    PIPELINE STEP TYPES
    ═══════════════════════════════════════════════════════

    --- call_skill ---
      {"id": "notify", "type": "call_skill",
       "skill": "message.publish@my-notifier",
       "args": [{"name": "topic", "value": "$.event.device_id"},
                {"name": "payload", "value": "$.analysis"}],
       "result_path": "$.notify_result"}

    --- llm_loop ---
      {"id": "analyse", "type": "llm_loop",
       "provider_name": "my-provider",
       "stop_on_finish": true,
       "tools": ["<skill-type>@<skill-id>"],
       "input": [{"name": "box_id", "value": "$.event.box_id"}],
       "set_result_schema": "{\"type\":\"object\",\"properties\":{\"verdict\":{\"type\":\"string\"}},\"required\":[\"verdict\"],\"additionalProperties\":false}",
       "result_path": "$.analysis"}

    stop_on_finish: true = ephemeral session (default). false = persistent across triggers.

    When creating an llm_loop step, use model "gpt-5.4-mini" unless the user explicitly
    requests another model. Do not invent a model name.

    --- wait_for_event ---
      {"id": "wait", "type": "wait_for_event", "topic": "evt/device/+/ack",
       "where": "data.ref_id == $.event.id", "result_path": "$.ack"}

    --- break ---
      {"id": "check", "type": "break", "path": "$.triage.skip", "eq": true}

    ═══════════════════════════════════════════════════════
    PIPELINE CONTEXT AND JSONPATH
    ═══════════════════════════════════════════════════════

    $.event contains the trigger payload. Step results are written via result_path.
    Values starting with $. in args/input entries are resolved from context at runtime.
    args and input are arrays of {"name": string, "value": primitive} entries, not objects.

    ═══════════════════════════════════════════════════════
    YOUR WORKFLOW
    ═══════════════════════════════════════════════════════

    1. Understand what the user wants to automate.
    2. Query existing skills, AI providers, and pipelines to avoid duplication.
    3. Create skills first, then the pipeline.
    4. Confirm with a plain-language summary of what was created.

    ═══════════════════════════════════════════════════════
    REPLYING — MANDATORY
    ═══════════════════════════════════════════════════════

    You MUST call message_publish_builder-reply + set_ as the LAST action of every response,
    without exception. Do not stop without calling it.

      topic   — "response"
      payload — a plain-language summary of what was created and how to trigger it.

    Failure to call message_publish_builder-reply means the user never receives your answer.
    """
).

-define(BUILD_PROMPT,
    ~b"""
    We need an automated visual inspection pipeline for our apple conveyor line.

    When a box finishes a conveyor run, a device publishes to evt/conveyor/+/box/done
    with JSON payload containing box_id and conveyor_id. At that point we want the system
    to request a photo of the box, have an AI inspect it for defects, log the result
    to our database, and publish the final verdict.

    We have a camera that listens on box/shot/<box_id> and replies with a
    base64-encoded PNG in an image_url field. Defect alerts should go to box/alert/,
    and the final verdict to box/status/.

    Our PostgreSQL table is apple_box_inspections with columns
    conveyor_id, box_id, status, reason — write one row per box.

    Use the existing AI provider "openai" for the AI inspector.
    The inspector should request the photo, look for rotten/moldy/bruised apples,
    optionally raise an alert, then return a verdict: approved or rejected with a short reason.
    Pipeline id: apple-box-inspection.
    Run the inspection as an ephemeral LLM loop (fresh session per box),
    then persist the result to the database and broadcast the verdict. Make sure that
    instructions include proper description of how to use camera skill (topic structure).
    Do not make separate skill call for camera skill, let llm inspection step call it.
    Instruct llm step to _always_ publish warning if the box is rejected.
    The final verdict structure should be: {"status": "approved" | "rejected", "reason": "string"}.
    It should be done via explicit step, do not send it as part of the llm inspection step.
    """
).

%%--------------------------------------------------------------------
%% CT callbacks
%%--------------------------------------------------------------------

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    case emqx_agent_test_llm_helper:available() of
        false ->
            {skip, emqx_agent_test_llm_helper:skip_reason("builder")};
        true ->
            Apps = emqx_cth_suite:start(
                [
                    emqx,
                    emqx_conf,
                    {emqx_ai_completion, #{
                        config => "ai.providers = [], ai.completion_profiles = []"
                    }},
                    emqx_agent
                ],
                #{work_dir => emqx_cth_suite:work_dir(Config)}
            ),
            ok = register_shared_provider(),
            ok = register_builder_skills(),
            ok = register_builder_pipeline(),
            [{suite_apps, Apps} | Config]
    end.

end_per_suite(Config) ->
    _ = emqx_agent_service:pipeline_delete(?TARGET_PIPELINE_ID),
    _ = emqx_agent_service:skill_delete(<<"message.request">>, <<"box-shot">>),
    _ = emqx_agent_service:skill_delete(<<"message.publish">>, <<"box-alert">>),
    _ = emqx_agent_service:skill_delete(<<"message.publish">>, <<"box-status">>),
    _ = emqx_agent_service:skill_delete(<<"postgresql.query">>, <<"box-register">>),
    ok = cleanup_builder_infra(),
    emqx_cth_suite:stop(?config(suite_apps, Config)).

init_per_testcase(_TC, Config) ->
    ct:timetrap({seconds, 240}),
    ok = emqx:subscribe(?REPLY_TOPIC),
    Config.

end_per_testcase(_TC, _Config) ->
    ok = emqx:unsubscribe(?REPLY_TOPIC).

%%--------------------------------------------------------------------
%% Test cases
%%--------------------------------------------------------------------

t_builds_apple_box_pipeline(_Config) ->
    %% Send build request to the persistent builder session.
    publish_request(?BUILD_PROMPT),

    %% Wait for the builder to reply — it publishes to builder/reply/response
    %% after completing all tool calls.
    ct:print("Waiting up to ~w ms for builder reply...", [?LLM_TIMEOUT]),
    _Reply = await_reply(),
    ct:print("Builder reply received — verifying pipeline structure"),

    %% Verify the pipeline was created with the expected structure.
    assert_pipeline().

%%--------------------------------------------------------------------
%% Assertions
%%--------------------------------------------------------------------

assert_pipeline() ->
    {ok, Pipeline} = emqx_agent_service:pipeline_get(?TARGET_PIPELINE_ID),
    ?assertEqual(
        <<"evt/conveyor/+/box/done">>,
        maps:get(<<"topic">>, maps:get(<<"trigger">>, Pipeline))
    ),
    Steps = maps:get(<<"steps">>, Pipeline),

    %% Must contain exactly one llm_loop step with the right tools, prompt, and schema.
    LLMSteps = [S || S <- Steps, maps:get(<<"type">>, S) =:= <<"llm_loop">>],
    ?assert(length(LLMSteps) >= 1, {no_llm_loop_step, Steps}),
    [Inspect | _] = LLMSteps,
    Tools = maps:get(<<"tools">>, Inspect),
    ?assert(
        lists:any(fun(T) -> skill_type(T) =:= <<"message.request">> end, Tools),
        {no_message_request_tool, Tools}
    ),
    ?assert(
        lists:any(fun(T) -> skill_type(T) =:= <<"message.publish">> end, Tools),
        {no_message_publish_tool, Tools}
    ),
    Instructions = maps:get(<<"instructions">>, Inspect, <<>>),
    ?assert(byte_size(Instructions) > 0, {no_instructions, Inspect}),
    SRS = maps:get(<<"set_result_schema">>, Inspect),
    SRSProps = maps:get(<<"properties">>, SRS),
    ?assert(is_map_key(<<"status">>, SRSProps)),
    ?assert(is_map_key(<<"reason">>, SRSProps)),

    %% Must contain at least one call_skill using postgresql.query.
    ?assert(
        lists:any(
            fun(S) ->
                maps:get(<<"type">>, S) =:= <<"call_skill">> andalso
                    skill_type(maps:get(<<"skill">>, S, <<>>)) =:= <<"postgresql.query">>
            end,
            Steps
        ),
        {no_postgresql_step, Steps}
    ),

    %% Must contain at least one call_skill using message.publish.
    ?assert(
        lists:any(
            fun(S) ->
                maps:get(<<"type">>, S) =:= <<"call_skill">> andalso
                    skill_type(maps:get(<<"skill">>, S, <<>>)) =:= <<"message.publish">>
            end,
            Steps
        ),
        {no_message_publish_step, Steps}
    ).

%% Extract the type portion from "type@id" or just return the whole value.
skill_type(Spec) ->
    case binary:split(Spec, <<"@">>) of
        [Type, _] -> Type;
        _ -> Spec
    end.

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

publish_request(Message) ->
    Payload = emqx_utils_json:encode(#{<<"message">> => Message}),
    Msg = emqx_message:make(?MODULE, 0, ?REQUEST_TOPIC, Payload),
    _ = emqx_broker:publish(Msg),
    ok.

await_reply() ->
    receive
        #deliver{message = #message{payload = P}} ->
            emqx_utils_json:decode(P)
    after ?LLM_TIMEOUT ->
        ct:fail("no builder reply within ~w ms", [?LLM_TIMEOUT])
    end.

%%--------------------------------------------------------------------
%% Builder infrastructure setup
%%--------------------------------------------------------------------

register_builder_skills() ->
    ok = emqx_agent_skill_create_skill:create(#{skill_id => <<"builder-create-skill">>}),
    ok = emqx_agent_skill_create_pipeline:create(#{skill_id => <<"builder-create-pipeline">>}),
    ok = emqx_agent_skill_query_skills:create(#{skill_id => <<"builder-query-skills">>}),
    ok = emqx_agent_skill_query_providers:create(#{skill_id => <<"builder-query-providers">>}),
    ok = emqx_agent_skill_query_pipelines:create(#{skill_id => <<"builder-query-pipelines">>}),
    ok = emqx_agent_skill_delete_skill:create(#{skill_id => <<"builder-delete-skill">>}),
    ok = emqx_agent_skill_delete_pipeline:create(#{skill_id => <<"builder-delete-pipeline">>}),
    ok = emqx_agent_skill_publish:create(#{
        skill_id => <<"builder-reply">>,
        desc => <<"Send a reply from the pipeline builder back to the chat UI">>,
        topic_prefix => <<"builder/reply/">>
    }).

cleanup_builder_infra() ->
    _ = emqx_agent_pipeline_registry:unregister(?BUILDER_PIPELINE_ID),
    _ = emqx_ai_completion_config:update_providers_raw({delete, ?SHARED_PROVIDER}),
    _ = emqx_agent_skill_create_skill:destroy(<<"builder-create-skill">>),
    _ = emqx_agent_skill_create_pipeline:destroy(<<"builder-create-pipeline">>),
    _ = emqx_agent_skill_query_skills:destroy(<<"builder-query-skills">>),
    _ = emqx_agent_skill_query_providers:destroy(<<"builder-query-providers">>),
    _ = emqx_agent_skill_query_pipelines:destroy(<<"builder-query-pipelines">>),
    _ = emqx_agent_skill_delete_skill:destroy(<<"builder-delete-skill">>),
    _ = emqx_agent_skill_delete_pipeline:destroy(<<"builder-delete-pipeline">>),
    _ = emqx_agent_skill_publish:destroy(<<"builder-reply">>),
    ok.

register_shared_provider() ->
    emqx_ai_completion_config:update_providers_raw(
        {add, emqx_agent_test_llm_helper:provider(?SHARED_PROVIDER)}
    ).

register_builder_pipeline() ->
    Model = emqx_agent_test_llm_helper:default_model(),
    emqx_agent_pipeline_registry:register(#{
        <<"pipeline_id">> => ?BUILDER_PIPELINE_ID,
        <<"active">> => true,
        <<"trigger">> => #{<<"topic">> => <<"evt/builder/request">>},
        <<"steps">> => [
            #{
                <<"id">> => <<"build">>,
                <<"type">> => <<"llm_loop">>,
                <<"provider_name">> => ?SHARED_PROVIDER,
                <<"model">> => Model,
                <<"max_tokens">> => 8192,
                <<"instructions">> => ?BUILDER_INSTRUCTIONS,
                <<"stop_on_finish">> => false,
                <<"tools">> => [
                    <<"agent.create_skill@builder-create-skill">>,
                    <<"agent.create_pipeline@builder-create-pipeline">>,
                    <<"agent.query_skills@builder-query-skills">>,
                    <<"agent.query_providers@builder-query-providers">>,
                    <<"agent.query_pipelines@builder-query-pipelines">>,
                    <<"agent.delete_skill@builder-delete-skill">>,
                    <<"agent.delete_pipeline@builder-delete-pipeline">>,
                    <<"message.publish@builder-reply">>
                ],
                <<"set_result_schema">> => #{
                    <<"type">> => <<"object">>,
                    <<"properties">> => #{<<"summary">> => #{<<"type">> => <<"string">>}},
                    <<"required">> => [<<"summary">>]
                },
                <<"input">> => #{<<"message">> => <<"$.event.message">>},
                <<"result_path">> => <<"$.build_result">>
            }
        ]
    }).
