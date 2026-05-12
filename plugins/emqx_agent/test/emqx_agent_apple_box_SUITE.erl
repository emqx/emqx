%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% Integration test for the Apple Box Conveyor Quality Inspector demo.
%%
%% Requires the API key for EMQX_AGENT_TEST_LLM_PROVIDER.
%% Requires: PostgreSQL reachable at pgsql:5432 (the standard EMQX test docker setup).
%%
%% What this suite tests end-to-end:
%%   1. Trigger event on evt/conveyor/+/box/done starts the pipeline.
%%   2. Pipeline llm_loop sends a message__request to box/shot/<box_id>.
%%   3. This suite acts as the SPA: receives the shot request, reads the
%%      Response-Topic MQTT 5 property, publishes the fixture image as a
%%      {"image_url":"data:image/png;base64,..."} JSON payload.
%%   4. The session vision support detects the image_url and sends a
%%      multimodal content array to GPT-4o for visual inspection.
%%   5. GPT-4o returns {status, reason}; pipeline writes to PostgreSQL and
%%      publishes to box/status/<box_id>.
%%   6. Suite asserts status, optional alert, and DB row.

-module(emqx_agent_apple_box_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("emqx/include/emqx.hrl").

-define(PIPELINE_ID, <<"apple-box-inspection">>).
-define(PROVIDER_NAME, <<"apple-inspector">>).
-define(CONNECTION_ID, <<"apple-box-pg">>).
-define(PIPE_EVENTS_FILTER, <<"pipe/+/inst/+/events">>).
%% LLM calls may take up to 60 s; give generous headroom.
-define(LLM_TIMEOUT, 90_000).

%%--------------------------------------------------------------------
%% CT callbacks
%%--------------------------------------------------------------------

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    case emqx_agent_test_llm_helper:available() of
        false ->
            {skip, emqx_agent_test_llm_helper:skip_reason("apple-box")};
        true ->
            Apps = emqx_cth_suite:start(
                [
                    emqx,
                    emqx_connector,
                    emqx_resource,
                    emqx_redis,
                    emqx_postgresql,
                    emqx_conf,
                    {emqx_ai_completion, #{
                        config => "ai.providers = [], ai.completion_profiles = []"
                    }},
                    emqx_agent
                ],
                #{work_dir => emqx_cth_suite:work_dir(Config)}
            ),
            %% PostgreSQL skill init must run before we can use the resource.
            ok = emqx_agent_skill_postgresql:init(),
            ok = register_provider(),
            [{suite_apps, Apps} | Config]
    end.

end_per_suite(Config) ->
    _ = emqx_ai_completion_config:update_providers_raw({delete, ?PROVIDER_NAME}),
    ok = emqx_agent_skill_postgresql:deinit(),
    emqx_cth_suite:stop(?config(suite_apps, Config)).

init_per_testcase(_TC, Config) ->
    ct:timetrap({seconds, 180}),
    ok = emqx_agent_plugin_config_fixture:setup(),
    ok = create_connection(),
    ok = create_table(),
    ok = register_skills(),
    ok = register_pipeline(),
    ConvId = <<"conv-", (integer_to_binary(erlang:unique_integer([positive, monotonic])))/binary>>,
    BoxId = <<"box-", (integer_to_binary(erlang:unique_integer([positive, monotonic])))/binary>>,
    ok = emqx:subscribe(?PIPE_EVENTS_FILTER),
    ok = emqx:subscribe(<<"box/status/", BoxId/binary>>),
    ok = emqx:subscribe(<<"box/alert/", BoxId/binary>>),
    ok = emqx:subscribe(<<"box/shot/", BoxId/binary>>),
    [{conveyor_id, ConvId}, {box_id, BoxId} | Config].

end_per_testcase(_TC, Config) ->
    BoxId = ?config(box_id, Config),
    ok = emqx:unsubscribe(?PIPE_EVENTS_FILTER),
    ok = emqx:unsubscribe(<<"box/status/", BoxId/binary>>),
    ok = emqx:unsubscribe(<<"box/alert/", BoxId/binary>>),
    ok = emqx:unsubscribe(<<"box/shot/", BoxId/binary>>),
    ok = drop_table(),
    ok = emqx_agent_pipeline_registry:unregister(?PIPELINE_ID),
    ok = emqx_agent_skill_registry:clear_runtime_for_test(),
    ok = emqx_agent_service:connection_delete(?CONNECTION_ID),
    ok = emqx_agent_plugin_config_fixture:teardown().

%%--------------------------------------------------------------------
%% Test cases
%%--------------------------------------------------------------------

%% A box containing only healthy apples must be approved.
t_good_box_approved(Config) ->
    BoxId = ?config(box_id, Config),
    ConvId = ?config(conveyor_id, Config),
    publish_done(ConvId, BoxId, 3),
    serve_shot(BoxId, fixture("box-no-bad-apple.png")),
    Status = await_status(BoxId),
    ?assertMatch(#{<<"status">> := <<"approved">>}, Status),
    assert_db_row(BoxId).

%% A box containing a rotten apple must be rejected.
%% The LLM may also publish a box_alert; we collect it if it arrives but
%% do not fail the test if it doesn't (LLM behaviour is non-deterministic).
t_bad_box_rejected(Config) ->
    BoxId = ?config(box_id, Config),
    ConvId = ?config(conveyor_id, Config),
    publish_done(ConvId, BoxId, 2),
    serve_shot(BoxId, fixture("box-bad-apple.png")),
    Status = await_status(BoxId),
    ?assertMatch(#{<<"status">> := <<"rejected">>}, Status),
    _MaybeAlert = collect_alert_if_any(BoxId),
    assert_db_row(BoxId).

%% An empty box — pipeline must complete regardless of the LLM's verdict.
t_empty_box(Config) ->
    BoxId = ?config(box_id, Config),
    ConvId = ?config(conveyor_id, Config),
    publish_done(ConvId, BoxId, 0),
    serve_shot(BoxId, fixture("box.png")),
    %% Just assert pipeline completes; verdict is up to the LLM.
    _Status = await_status(BoxId),
    assert_db_row(BoxId).

%%--------------------------------------------------------------------
%% Helpers — SPA emulation
%%--------------------------------------------------------------------

%% Block until a shot request arrives on box/shot/<BoxId>, then publish the
%% image at ImagePath back to the Response-Topic MQTT5 property.
%% The test process itself holds the subscription (set up in init_per_testcase),
%% so #deliver messages are delivered directly here — no spawn needed.
serve_shot(BoxId, ImagePath) ->
    ShotTopic = <<"box/shot/", BoxId/binary>>,
    receive
        #deliver{topic = ShotTopic, message = Msg} ->
            Props = emqx_message:get_header(properties, Msg, #{}),
            ResponseTopic = maps:get('Response-Topic', Props),
            {ok, ImageData} = file:read_file(ImagePath),
            B64 = base64:encode(ImageData),
            Payload = emqx_utils_json:encode(#{
                <<"image_url">> => <<"data:image/png;base64,", B64/binary>>
            }),
            RespMsg = emqx_message:make(?MODULE, 0, ResponseTopic, Payload),
            _ = emqx_broker:publish(RespMsg)
    after ?LLM_TIMEOUT ->
        ct:fail("no shot request for box ~s within ~w ms", [BoxId, ?LLM_TIMEOUT])
    end.

%%--------------------------------------------------------------------
%% Helpers — await messages
%%--------------------------------------------------------------------

await_status(BoxId) ->
    StatusTopic = <<"box/status/", BoxId/binary>>,
    receive
        #deliver{topic = StatusTopic, message = #message{payload = P}} ->
            emqx_utils_json:decode(P)
    after ?LLM_TIMEOUT ->
        ct:fail("no status for box ~s within ~w ms", [BoxId, ?LLM_TIMEOUT])
    end.

collect_alert_if_any(BoxId) ->
    AlertTopic = <<"box/alert/", BoxId/binary>>,
    receive
        #deliver{topic = AlertTopic, message = #message{payload = P}} ->
            emqx_utils_json:decode(P)
    after 3000 ->
        undefined
    end.

%%--------------------------------------------------------------------
%% Helpers — pipeline trigger
%%--------------------------------------------------------------------

publish_done(ConvId, BoxId, AppleCount) ->
    Topic = <<"evt/conveyor/", ConvId/binary, "/box/done">>,
    Payload = emqx_utils_json:encode(#{
        <<"box_id">> => BoxId,
        <<"conveyor_id">> => ConvId,
        <<"apple_count">> => AppleCount
    }),
    _ = emqx_broker:publish(emqx_message:make(?MODULE, 0, Topic, Payload)),
    ok.

%%--------------------------------------------------------------------
%% Helpers — DB assertion
%%--------------------------------------------------------------------

assert_db_row(BoxId) ->
    ResId = emqx_agent_skill_postgresql:resource_id(?CONNECTION_ID),
    SQL = <<"SELECT status FROM apple_box_inspections WHERE box_id = $1">>,
    Result = emqx_resource:simple_sync_query(ResId, {query, SQL, [BoxId]}),
    case Result of
        {ok, _Cols, Rows} ->
            ?assertNotEqual([], Rows, <<"expected DB row for box_id ", BoxId/binary>>);
        {ok, Rows} ->
            ?assertNotEqual([], Rows, <<"expected DB row for box_id ", BoxId/binary>>)
    end.

%%--------------------------------------------------------------------
%% Helpers — fixture path
%%--------------------------------------------------------------------

fixture(File) ->
    %% Navigate from the compiled source path to the fixtures directory.
    %% module_info(compile) contains the original source path.
    SrcFile = proplists:get_value(source, ?MODULE:module_info(compile), ""),
    Dir = filename:dirname(SrcFile),
    filename:join([Dir, "fixtures", File]).

%%--------------------------------------------------------------------
%% Setup helpers
%%--------------------------------------------------------------------

create_table() ->
    ResId = emqx_agent_skill_postgresql:resource_id(?CONNECTION_ID),
    SQL = <<
        "CREATE TABLE IF NOT EXISTS apple_box_inspections ("
        "  id SERIAL PRIMARY KEY,"
        "  conveyor_id TEXT NOT NULL,"
        "  box_id TEXT NOT NULL,"
        "  status TEXT NOT NULL,"
        "  reason TEXT,"
        "  inspected_at TIMESTAMPTZ DEFAULT NOW()"
        ")"
    >>,
    expect_query_ok(emqx_resource:simple_sync_query(ResId, {query, SQL})).

drop_table() ->
    ResId = emqx_agent_skill_postgresql:resource_id(?CONNECTION_ID),
    _ = emqx_resource:simple_sync_query(
        ResId, {query, <<"DROP TABLE IF EXISTS apple_box_inspections">>}
    ),
    ok.

expect_query_ok({ok, _, _}) -> ok;
expect_query_ok({ok, _}) -> ok;
expect_query_ok(ok) -> ok;
expect_query_ok({error, Reason}) -> error({db_error, Reason}).

create_connection() ->
    _ = emqx_agent_service:connection_delete(?CONNECTION_ID),
    ok = emqx_agent_service:connection_create(#{
        <<"id">> => ?CONNECTION_ID,
        <<"type">> => <<"postgresql">>,
        <<"enable">> => true,
        <<"config">> => #{
            <<"server">> => <<"pgsql:5432">>,
            <<"database">> => <<"mqtt">>,
            <<"username">> => <<"root">>,
            <<"password">> => <<"public">>,
            <<"pool_size">> => 1,
            <<"connect_timeout">> => 5000,
            <<"disable_prepared_statements">> => true,
            <<"ssl">> => #{<<"enable">> => false}
        }
    }).

register_skills() ->
    ok = register_skill(emqx_agent_skill_mqtt_request, #{
        skill_id => <<"box-shot">>,
        desc => <<"Request a box snapshot from the SPA">>,
        topic_prefix => <<"box/shot/">>,
        request_payload_schema => #{<<"type">> => <<"object">>}
    }),
    ok = register_skill(emqx_agent_skill_publish, #{
        skill_id => <<"box-alert">>,
        desc => <<"Publish a box quality alert">>,
        topic_prefix => <<"box/alert/">>
    }),
    ok = register_skill(emqx_agent_skill_publish, #{
        skill_id => <<"box-status">>,
        desc => <<"Publish final box inspection status">>,
        topic_prefix => <<"box/status/">>
    }),
    ok = register_skill(emqx_agent_skill_postgresql, #{
        skill_id => <<"box-register">>,
        desc => <<"Record inspection result in the database">>,
        resource => ?CONNECTION_ID,
        query => <<
            "INSERT INTO apple_box_inspections(conveyor_id, box_id, status, reason) "
            "VALUES(${conveyor_id}, ${box_id}, ${status}, ${reason})"
        >>
    }).

register_skill(Module, Context) ->
    {ok, Skill} = Module:create(Context),
    emqx_agent_skill_registry:put_runtime_for_test(Skill).

register_provider() ->
    emqx_ai_completion_config:update_providers_raw(
        {add, emqx_agent_test_llm_helper:provider(?PROVIDER_NAME)}
    ).

register_pipeline() ->
    Model = emqx_agent_test_llm_helper:default_model(),
    Def = #{
        <<"pipeline_id">> => ?PIPELINE_ID,
        <<"active">> => true,
        <<"trigger">> => #{<<"topic">> => <<"evt/conveyor/+/box/done">>},
        <<"steps">> => [
            #{
                <<"id">> => <<"inspect">>,
                <<"type">> => <<"llm_loop">>,
                <<"provider_name">> => ?PROVIDER_NAME,
                <<"model">> => Model,
                <<"instructions">> => <<
                    "You are an apple quality inspector. You will receive box_id and conveyor_id. "
                    "Use the message_request_box_shot tool with topic=box_id to request a photo of the box. "
                    "Examine the photo carefully for rotten, moldy, bruised, or damaged apples. "
                    "If you detect any defects, call message_publish_box_alert with reason, "
                    "defect_type (list of strings), and severity (low/medium/high). "
                    "When you have reached a verdict, call set_result with: "
                    "  status: 'approved' if all apples look fresh, 'rejected' if any defect found; "
                    "  reason: a short sentence explaining your decision."
                >>,
                <<"stop_on_finish">> => true,
                <<"tools">> => [
                    <<"message__request@box-shot">>,
                    <<"message__publish@box-alert">>
                ],
                <<"input">> => #{
                    <<"box_id">> => <<"$.event.box_id">>,
                    <<"conveyor_id">> => <<"$.event.conveyor_id">>
                },
                <<"set_result_schema">> => #{
                    <<"type">> => <<"object">>,
                    <<"properties">> => #{
                        <<"status">> => #{
                            <<"type">> => <<"string">>,
                            <<"enum">> => [<<"approved">>, <<"rejected">>]
                        },
                        <<"reason">> => #{<<"type">> => <<"string">>}
                    },
                    <<"required">> => [<<"status">>, <<"reason">>]
                },
                <<"result_path">> => <<"$.inspection">>
            },
            #{
                <<"id">> => <<"register">>,
                <<"type">> => <<"call_skill">>,
                <<"skill">> => <<"postgresql__query@box-register">>,
                <<"args">> => #{
                    <<"conveyor_id">> => <<"$.event.conveyor_id">>,
                    <<"box_id">> => <<"$.event.box_id">>,
                    <<"status">> => <<"$.inspection.status">>,
                    <<"reason">> => <<"$.inspection.reason">>
                },
                <<"result_path">> => <<"$.db_result">>
            },
            #{
                <<"id">> => <<"notify">>,
                <<"type">> => <<"call_skill">>,
                <<"skill">> => <<"message__publish@box-status">>,
                <<"args">> => #{
                    <<"topic">> => <<"$.event.box_id">>,
                    <<"payload">> => <<"$.inspection">>
                },
                <<"result_path">> => <<"$.notify_result">>
            }
        ]
    },
    emqx_agent_pipeline_registry:register(Def).
