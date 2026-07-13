%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_agent_attachment_llm_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("emqx/include/emqx.hrl").

-define(PROVIDER_NAME, <<"attachment-vision">>).
-define(PIPE_EVENTS_FILTER, <<"$pipe/+/inst/+/events">>).
-define(LLM_TIMEOUT, 120_000).

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    case emqx_agent_test_llm_helper:available() of
        false ->
            {skip, emqx_agent_test_llm_helper:skip_reason("attachment-llm")};
        true ->
            Apps = emqx_cth_suite:start(
                [
                    emqx,
                    emqx_conf,
                    emqx_resource,
                    {emqx_ai_completion, #{
                        config => "ai.providers = [], ai.completion_profiles = []"
                    }},
                    emqx_agent
                ],
                #{work_dir => emqx_cth_suite:work_dir(Config)}
            ),
            ok = register_provider(),
            [{suite_apps, Apps} | Config]
    end.

end_per_suite(Config) ->
    _ = emqx_ai_completion_config:update_providers_raw({delete, ?PROVIDER_NAME}),
    emqx_cth_suite:stop(?config(suite_apps, Config)).

init_per_testcase(TestCase, Config) ->
    ct:timetrap({seconds, 240}),
    ok = emqx_agent_plugin_config_fixture:setup(),
    {ok, {Port, _Pid}} = emqx_utils_http_test_server:start_link(random, "/[...]", false),
    BaseUrl = iolist_to_binary(io_lib:format("http://127.0.0.1:~p", [Port])),
    ok = emqx:subscribe(?PIPE_EVENTS_FILTER),
    [{tc_id, atom_to_binary(TestCase, utf8)}, {http_base_url, BaseUrl} | Config].

end_per_testcase(_TestCase, Config) ->
    ok = emqx:unsubscribe(?PIPE_EVENTS_FILTER),
    cleanup(?config(tc_id, Config)),
    _ = emqx_utils_http_test_server:stop(),
    ok = emqx_agent_plugin_config_fixture:teardown().

t_mqtt_json_data_url_auto(Config) ->
    run_mqtt_case(Config, json, true, []).

t_mqtt_json_data_url_by_images(Config) ->
    run_mqtt_case(Config, json, false, [<<".image_url">>]).

t_http_json_data_url_auto(Config) ->
    run_http_json_case(Config, true, []).

t_http_json_data_url_by_images(Config) ->
    run_http_json_case(Config, false, [<<".image_url">>]).

t_mqtt_binary_data_url_auto(Config) ->
    run_mqtt_case(Config, binary_data_url, true, []).

t_mqtt_binary_data_url_by_images(Config) ->
    run_mqtt_case(Config, binary_data_url, false, [<<".">>]).

t_http_raw_binary_auto(Config) ->
    run_http_raw_case(Config, image_content_type, true, []).

t_http_raw_binary_by_images(Config) ->
    run_http_raw_case(Config, text_content_type, false, [<<".">>]).

t_llm_fetches_url_from_json_tool(Config) ->
    TcId = ?config(tc_id, Config),
    PipelineId = pipeline_id(TcId),
    UrlToolId = tool_id(TcId, <<"url">>),
    HttpToolId = tool_id(TcId, <<"http">>),
    Topic = mqtt_topic(TcId),
    BaseUrl = ?config(http_base_url, Config),
    ImageUrl = <<BaseUrl/binary, "/raw-image">>,
    ok = register_mqtt_tool(UrlToolId, #{
        <<"payload_type">> => <<"json">>,
        <<"autodiscover_images">> => false,
        <<"images">> => []
    }),
    ok = register_http_tool(HttpToolId, <<BaseUrl/binary, "/fetch-url">>, #{
        <<"payload_type">> => <<"binary">>,
        <<"autodiscover_images">> => true,
        <<"images">> => [],
        <<"input_schema">> => emqx_utils_json:encode(url_input_schema())
    }),
    install_http_fetch_url_handler(),
    Responder = start_mqtt_responder(Topic, json_url_payload(ImageUrl)),
    ok = register_pipeline(
        PipelineId,
        TcId,
        [
            <<"message__request@", UrlToolId/binary>>,
            <<"http@", HttpToolId/binary>>
        ],
        <<
            "You are testing an image-fetch workflow. First call the message_request tool "
            "with topic=request and an empty payload to get JSON containing an image URL. "
            "Then call the HTTP tool with argument url equal to that URL. Inspect the fetched image. "
            "Finish by calling set_result."
        >>
    ),
    Result = trigger_and_await(PipelineId, TcId),
    stop_responder(Responder),
    assert_apple(Result).

run_mqtt_case(Config, Mode, Autodiscover, Images) ->
    TcId = ?config(tc_id, Config),
    PipelineId = pipeline_id(TcId),
    ToolId = tool_id(TcId, <<"mqtt">>),
    Topic = mqtt_topic(TcId),
    PayloadType =
        case Mode of
            json -> <<"json">>;
            binary_data_url -> <<"binary">>
        end,
    ok = register_mqtt_tool(ToolId, #{
        <<"payload_type">> => PayloadType,
        <<"autodiscover_images">> => Autodiscover,
        <<"images">> => Images
    }),
    Responder = start_mqtt_responder(Topic, mqtt_payload(Mode)),
    ok = register_pipeline(PipelineId, TcId, [<<"message__request@", ToolId/binary>>], <<
        "Call the message_request tool with topic=request and an empty payload. "
        "Inspect the returned image. Finish by calling set_result."
    >>),
    Result = trigger_and_await(PipelineId, TcId),
    stop_responder(Responder),
    assert_apple(Result).

run_http_json_case(Config, Autodiscover, Images) ->
    TcId = ?config(tc_id, Config),
    PipelineId = pipeline_id(TcId),
    ToolId = tool_id(TcId, <<"http-json">>),
    BaseUrl = ?config(http_base_url, Config),
    ok = register_http_tool(ToolId, <<BaseUrl/binary, "/json-image">>, #{
        <<"payload_type">> => <<"json">>,
        <<"autodiscover_images">> => Autodiscover,
        <<"images">> => Images
    }),
    install_http_json_image_handler(),
    ok = register_pipeline(PipelineId, TcId, [<<"http@", ToolId/binary>>], <<
        "Call the HTTP tool with empty arguments. "
        "Inspect the image returned in the tool response. Finish by calling set_result."
    >>),
    assert_apple(trigger_and_await(PipelineId, TcId)).

run_http_raw_case(Config, ContentMode, Autodiscover, Images) ->
    TcId = ?config(tc_id, Config),
    PipelineId = pipeline_id(TcId),
    ToolId = tool_id(TcId, <<"http-raw">>),
    BaseUrl = ?config(http_base_url, Config),
    ok = register_http_tool(ToolId, <<BaseUrl/binary, "/raw-image">>, #{
        <<"payload_type">> => <<"binary">>,
        <<"autodiscover_images">> => Autodiscover,
        <<"images">> => Images
    }),
    install_http_raw_image_handler(ContentMode),
    ok = register_pipeline(PipelineId, TcId, [<<"http@", ToolId/binary>>], <<
        "Call the HTTP tool with empty arguments. "
        "Inspect the image returned in the tool response. Finish by calling set_result."
    >>),
    assert_apple(trigger_and_await(PipelineId, TcId)).

register_provider() ->
    emqx_ai_completion_config:update_providers_raw(
        {add, emqx_agent_test_llm_helper:provider(?PROVIDER_NAME)}
    ).

register_mqtt_tool(ToolId, Overrides) ->
    emqx_agent_config:create_tool(
        maps:merge(
            #{
                <<"type">> => <<"message__request">>,
                <<"id">> => ToolId,
                <<"desc">> => <<"Return an image over MQTT request/reply">>,
                <<"topic_prefix">> => mqtt_prefix(),
                <<"request_payload_schema">> => emqx_utils_json:encode(empty_object_schema())
            },
            Overrides
        )
    ).

register_http_tool(ToolId, Url, Overrides) ->
    emqx_agent_config:create_tool(
        maps:merge(
            #{
                <<"type">> => <<"http">>,
                <<"id">> => ToolId,
                <<"desc">> => <<"Fetch an image or image metadata by HTTP GET">>,
                <<"method">> => <<"get">>,
                <<"url">> => Url,
                <<"headers">> => [],
                <<"input_schema">> => emqx_utils_json:encode(empty_object_schema())
            },
            Overrides
        )
    ).

register_pipeline(PipelineId, TcId, Tools, Instructions) ->
    emqx_agent_service:pipeline_create(#{
        <<"pipeline_id">> => PipelineId,
        <<"active">> => true,
        <<"trigger">> => #{<<"topic">> => event_topic(TcId)},
        <<"steps">> => [
            #{
                <<"id">> => <<"inspect">>,
                <<"type">> => <<"llm_loop">>,
                <<"provider_name">> => ?PROVIDER_NAME,
                <<"model">> => emqx_agent_test_llm_helper:default_model(),
                <<"instructions">> => <<
                    "Inspect the image returned by the tool. ",
                    Instructions/binary,
                    " Identify the primary visible object and provide a short evidence string. "
                    "Finish by calling set_result."
                >>,
                <<"persistent">> => false,
                <<"tools">> => Tools,
                <<"input">> => #{<<"case_id">> => TcId},
                <<"set_result_schema">> => result_schema(),
                <<"result_path">> => <<"$.inspection">>
            }
        ]
    }).

trigger_and_await(PipelineId, TcId) ->
    Payload = emqx_utils_json:encode(#{<<"case_id">> => TcId}),
    _ = emqx_broker:publish(emqx_message:make(?MODULE, 0, event_topic(TcId), Payload)),
    await_pipeline_result(PipelineId).

await_pipeline_result(PipelineId) ->
    receive
        #deliver{message = #message{payload = Payload}} ->
            Event = emqx_utils_json:decode(Payload),
            case
                {
                    maps:get(<<"pipeline_id">>, Event, undefined),
                    maps:get(<<"type">>, Event, undefined)
                }
            of
                {PipelineId, <<"pipeline_completed">>} ->
                    maps:get(<<"inspection">>, maps:get(<<"context">>, Event));
                {PipelineId, <<"pipeline_failed">>} ->
                    ct:fail("pipeline failed: ~p", [Event]);
                _ ->
                    await_pipeline_result(PipelineId)
            end
    after ?LLM_TIMEOUT ->
        ct:fail("no pipeline completion for ~s within ~w ms", [PipelineId, ?LLM_TIMEOUT])
    end.

assert_apple(Result) ->
    Object = string:lowercase(binary_to_list(maps:get(<<"object">>, Result, <<>>))),
    ?assertNotEqual(nomatch, string:find(Object, "apple")),
    ok.

start_mqtt_responder(Topic, Payload) ->
    Parent = self(),
    Pid = spawn_link(fun() ->
        ok = emqx:subscribe(Topic),
        Parent ! {mqtt_responder_ready, self()},
        receive
            #deliver{topic = Topic, message = Msg} ->
                Props = emqx_message:get_header(properties, Msg, #{}),
                ResponseTopic = maps:get('Response-Topic', Props),
                _ = emqx_broker:publish(emqx_message:make(?MODULE, 0, ResponseTopic, Payload)),
                ok = emqx:unsubscribe(Topic),
                Parent ! {mqtt_responder_done, self()}
        after ?LLM_TIMEOUT ->
            ok = emqx:unsubscribe(Topic),
            Parent ! {mqtt_responder_timeout, self()}
        end
    end),
    receive
        {mqtt_responder_ready, Pid} -> Pid
    after 3000 ->
        ct:fail(mqtt_responder_not_ready)
    end.

stop_responder(Pid) ->
    receive
        {mqtt_responder_done, Pid} -> ok;
        {mqtt_responder_timeout, Pid} -> ct:fail(mqtt_responder_timeout)
    after 0 ->
        ok
    end.

install_http_json_image_handler() ->
    emqx_utils_http_test_server:set_handler(fun(Req0, State) ->
        reply_json(200, #{<<"image_url">> => data_url()}, Req0, State)
    end).

install_http_raw_image_handler(ContentMode) ->
    emqx_utils_http_test_server:set_handler(fun(Req0, State) ->
        ContentType =
            case ContentMode of
                image_content_type -> <<"image/png">>;
                text_content_type -> <<"text/plain">>
            end,
        Req = cowboy_req:reply(200, #{<<"content-type">> => ContentType}, image_data(), Req0),
        {ok, Req, State}
    end).

install_http_fetch_url_handler() ->
    emqx_utils_http_test_server:set_handler(fun(Req0, State) ->
        Path = cowboy_req:path(Req0),
        case Path of
            <<"/fetch-url">> ->
                QS = maps:from_list(cowboy_req:parse_qs(Req0)),
                case maps:get(<<"url">>, QS, <<>>) of
                    <<"http://", _/binary>> ->
                        Req = cowboy_req:reply(
                            200, #{<<"content-type">> => <<"image/png">>}, image_data(), Req0
                        ),
                        {ok, Req, State};
                    _ ->
                        reply_json(400, #{<<"error">> => <<"missing url">>}, Req0, State)
                end;
            <<"/raw-image">> ->
                Req = cowboy_req:reply(
                    200, #{<<"content-type">> => <<"image/png">>}, image_data(), Req0
                ),
                {ok, Req, State};
            _ ->
                reply_json(404, #{<<"error">> => <<"not found">>}, Req0, State)
        end
    end).

reply_json(Status, Data, Req0, State) ->
    Req = cowboy_req:reply(
        Status,
        #{<<"content-type">> => <<"application/json">>},
        emqx_utils_json:encode(Data),
        Req0
    ),
    {ok, Req, State}.

mqtt_payload(json) ->
    emqx_utils_json:encode(#{<<"image_url">> => data_url()});
mqtt_payload(binary_data_url) ->
    data_url().

json_url_payload(Url) ->
    emqx_utils_json:encode(#{<<"url">> => Url}).

data_url() ->
    <<"data:image/png;base64,", (base64:encode(image_data()))/binary>>.

image_data() ->
    {ok, Data} = file:read_file(image_path()),
    Data.

image_path() ->
    SrcFile = proplists:get_value(source, ?MODULE:module_info(compile), ""),
    filename:join([filename:dirname(SrcFile), "fixtures", "object.png"]).

result_schema() ->
    #{
        <<"type">> => <<"object">>,
        <<"properties">> => #{
            <<"object">> => #{<<"type">> => <<"string">>},
            <<"evidence">> => #{<<"type">> => <<"string">>}
        },
        <<"required">> => [<<"object">>, <<"evidence">>],
        <<"additionalProperties">> => false
    }.

empty_object_schema() ->
    #{
        <<"type">> => <<"object">>,
        <<"properties">> => #{},
        <<"required">> => [],
        <<"additionalProperties">> => false
    }.

url_input_schema() ->
    #{
        <<"type">> => <<"object">>,
        <<"properties">> => #{
            <<"url">> => #{<<"type">> => <<"string">>}
        },
        <<"required">> => [<<"url">>],
        <<"additionalProperties">> => false
    }.

cleanup(TcId) ->
    _ = emqx_agent_config:delete_pipeline(pipeline_id(TcId)),
    lists:foreach(
        fun(Suffix) ->
            _ = emqx_agent_config:delete_tool(<<"message__request">>, tool_id(TcId, Suffix)),
            _ = emqx_agent_config:delete_tool(<<"http">>, tool_id(TcId, Suffix))
        end,
        [<<"mqtt">>, <<"http-json">>, <<"http-raw">>, <<"url">>, <<"http">>]
    ),
    _ = emqx:unsubscribe(mqtt_topic(TcId)),
    ok.

pipeline_id(TcId) ->
    <<"att-", TcId/binary>>.

tool_id(TcId, Suffix) ->
    <<"att-", Suffix/binary, "-", TcId/binary>>.

event_topic(TcId) ->
    <<"$evt/attachment/", TcId/binary>>.

mqtt_prefix() ->
    <<"att/mqtt/">>.

mqtt_topic(_TcId) ->
    <<"att/mqtt/request">>.
