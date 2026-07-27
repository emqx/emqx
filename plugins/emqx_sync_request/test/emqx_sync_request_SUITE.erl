%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_sync_request_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include("emqx_sync_request.hrl").
-include_lib("emqx/include/asserts.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("emqx_utils/include/emqx_api_key_scopes.hrl").
-include_lib("emqx_utils/include/emqx_message.hrl").

-define(REQ_TOPIC, <<"sync_request/device/1001/request">>).
-define(RESP_TOPIC, <<"sync_request/device/1001/response">>).
-define(REQ_ID, <<"request-id-1">>).
-define(REQ_PAYLOAD, <<"{\"cmd\":\"reboot\"}">>).
-define(RESP_PAYLOAD, <<"{\"result\":\"ok\"}">>).

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    WorkDir = emqx_cth_suite:work_dir(Config),
    InstallDir = filename:join([WorkDir, "plugins"]),
    Apps = emqx_cth_suite:start(
        [
            emqx_conf,
            emqx,
            {emqx_plugins, #{config => #{plugins => #{install_dir => InstallDir}}}},
            emqx_management,
            emqx_mgmt_api_test_util:emqx_dashboard()
        ],
        #{work_dir => WorkDir}
    ),
    ok = filelib:ensure_path(filename:join([InstallDir, "dummy"])),
    try
        Package = plugin_package(),
        {ok, PackageBin} = file:read_file(Package),
        NameVsn = filename:basename(Package, ".tar.gz"),
        [
            {apps, Apps},
            {plugin_name_vsn, NameVsn},
            {plugin_package_bin, PackageBin}
            | Config
        ]
    catch
        error:{plugin_package_build_failed, _Package, Output} ->
            ct:log("plugin_package build failed: ~s", [Output]),
            {skip, "Run 'make emqx-enterprise-compile' first to build plugin dependencies."}
    end.

end_per_suite(Config) ->
    ok = emqx_cth_suite:stop(?config(apps, Config)).

init_per_testcase(_TestCase, Config) ->
    ok = cleanup_plugin(Config),
    ok = install_and_start_plugin(Config),
    Config.

end_per_testcase(_TestCase, Config) ->
    ok = cleanup_plugin(Config).

t_plugin_install_start_stop_uninstall_controls_api_route(Config) ->
    NameVsn = ?config(plugin_name_vsn, Config),
    ok = cleanup_plugin(Config),
    ?assertEqual(undefined, whereis(?SERVICE)),
    ?assertEqual(404, api_status(#{request => #{}})),

    ok = install_and_start_plugin(Config),
    ?assertEqual(400, api_status(#{request => #{}})),

    {StopTime, ok} = timer:tc(fun() -> emqx_plugins:ensure_stopped(NameVsn) end),
    ?assert(StopTime < timer:seconds(2) * 1000),
    ?assertEqual(404, api_status(#{request => #{}})),

    ok = emqx_dashboard:stop_listeners(),
    ok = emqx_dashboard:start_listeners(),
    ?assertEqual(404, api_status(#{request => #{}})),

    ok = emqx_plugins:ensure_started(NameVsn),
    ?assertEqual(400, api_status(#{request => #{}})),

    ok = emqx_plugins:ensure_stopped(NameVsn),
    ok = emqx_plugins:ensure_uninstalled(NameVsn),
    ?assertEqual(404, api_status(#{request => #{}})).

t_plugin_health_check_reports_ok(Config) ->
    NameVsn = ?config(plugin_name_vsn, Config),
    ?assertMatch(
        {ok, #{health_status := #{status := ok, message := <<"">>}}},
        emqx_plugins:describe(NameVsn, #{fill_readme => false, health_check => true})
    ).

t_cli_status_reports_node_local_status(_Config) ->
    ?assertEqual({ok, {emqx_sync_request_cli, cmd}}, emqx_ctl:lookup_command(sync_request)),
    Before = emqx_sync_request:status(),
    Body = request_body(
        <<"sync_request/cli-status/request">>,
        <<"sync_request/cli-status/response">>,
        <<"cli-status-request-id">>,
        #{}
    ),
    ?assertMatch({404, _}, do_http_request(Body)),
    After = emqx_sync_request:status(),
    ?assertEqual(maps:get(requests_total, Before) + 1, maps:get(requests_total, After)),
    ?assertEqual(maps:get(requests_failed, Before) + 1, maps:get(requests_failed, After)),
    ?assertEqual(
        maps:get(requests_no_subscribers, Before) + 1,
        maps:get(requests_no_subscribers, After)
    ),
    ?assertEqual(0, maps:get(inflight_requests, After)),
    ?assertEqual(0, maps:get(pending_responses, After)),
    mock_ctl_print(),
    try
        Output = emqx_sync_request_cli:cmd(["status"]),
        ?assertMatch({match, _}, re:run(Output, "Counters since plugin start:")),
        ?assertMatch({match, _}, re:run(Output, "sync_request.requests.total:")),
        ?assertMatch({match, _}, re:run(Output, "sync_request.requests.no_subscribers:")),
        ?assertMatch({match, _}, re:run(Output, "Current gauges:")),
        ?assertMatch({match, _}, re:run(Output, "sync_request.inflight_requests:")),
        ?assertMatch({match, _}, re:run(Output, "sync_request.pending_responses:"))
    after
        unmock_ctl_print()
    end.

t_api_spec_lists_conflict_and_unavailable_responses(_Config) ->
    #{
        post := Post = #{
            description :=
                <<"Publish one MQTT request and wait for the first matching response.">>,
            responses := Responses
        }
    } =
        emqx_sync_request_api:schema("/plugin_api/emqx_sync_request/request"),
    ?assertNot(maps:is_key(summary, Post)),
    ?assertNot(maps:is_key(tags, Post)),
    ?assert(maps:is_key(409, Responses)),
    ?assert(maps:is_key(503, Responses)).

t_plugin_config_rejects_invalid_values(Config) ->
    NameVsn = ?config(plugin_name_vsn, Config),
    {200, OriginalConfig} = plugin_config_request(get, NameVsn, #{}),
    InvalidValues = [
        {<<"default_timeout">>, <<"not-a-duration">>, <<"invalid_duration">>},
        {<<"default_timeout">>, <<"0ms">>, <<"invalid_duration">>},
        {<<"max_timeout">>, <<"not-a-duration">>, <<"invalid_duration">>},
        {<<"max_timeout">>, <<"0ms">>, <<"invalid_duration">>},
        {<<"max_payload_size">>, <<"not-a-size">>, <<"invalid_bytesize">>},
        {<<"max_payload_size">>, <<"0B">>, <<"invalid_bytesize">>},
        {<<"max_inflight_requests">>, 0, <<"invalid_positive_integer">>},
        {<<"max_inflight_requests">>, -1, <<"invalid_positive_integer">>}
    ],
    lists:foreach(
        fun({Field, Value, ExpectedReason}) ->
            InvalidConfig = OriginalConfig#{Field => Value},
            {400, #{
                <<"code">> := <<"BAD_CONFIG">>,
                <<"message">> := Message
            }} = plugin_config_request(put, NameVsn, InvalidConfig),
            ?assertNotEqual(nomatch, binary:match(Message, ExpectedReason)),
            ?assertEqual({200, OriginalConfig}, plugin_config_request(get, NameVsn, #{}))
        end,
        InvalidValues
    ).

t_plugin_config_accepts_positive_boundaries(Config) ->
    NameVsn = ?config(plugin_name_vsn, Config),
    {200, OriginalConfig} = plugin_config_request(get, NameVsn, #{}),
    BoundaryConfig = #{
        <<"default_timeout">> => <<"1ms">>,
        <<"max_timeout">> => <<"1ms">>,
        <<"max_inflight_requests">> => 1,
        <<"max_payload_size">> => <<"1B">>
    },
    try
        ?assertEqual({204, []}, plugin_config_request(put, NameVsn, BoundaryConfig)),
        ?assertEqual({200, BoundaryConfig}, plugin_config_request(get, NameVsn, #{}))
    after
        {204, _} = plugin_config_request(put, NameVsn, OriginalConfig)
    end.

t_plugin_api_callback_uses_gateway_contract(_Config) ->
    ?assertEqual(
        {error, not_found},
        emqx_sync_request_app:on_handle_api_call(get, [<<"request">>], #{}, #{})
    ),
    ?assertMatch(
        {error, 400, #{}, #{code := ?CODE_BAD_REQUEST}},
        emqx_sync_request_app:on_handle_api_call(
            post, [<<"request">>], #{body => #{<<"request">> => #{}}}, #{}
        )
    ).

t_plugin_api_callback_rejects_non_object_request(_Config) ->
    ?assertMatch(
        {error, 400, #{}, #{code := ?CODE_BAD_REQUEST}},
        emqx_sync_request_app:on_handle_api_call(
            post, [<<"request">>], #{body => #{<<"request">> => <<"oops">>}}, #{}
        )
    ).

t_plugin_stop_unblocks_inflight_request(Config) ->
    Parent = self(),
    NameVsn = ?config(plugin_name_vsn, Config),
    ReqTopic = <<"sync_request/stopping/request">>,
    RespTopic = <<"sync_request/stopping/response">>,
    {ok, Responder} = start_blackhole_responder(
        <<"sync_request_stopping_blackhole">>,
        ReqTopic,
        fun(Payload) -> Parent ! {stopping_request_seen, Payload} end
    ),
    try
        ok = wait_for_subscribers(ReqTopic, 1),
        Ref = async_http_request(
            request_body(ReqTopic, RespTopic, <<"stopping-request-id">>, #{timeout => <<"60s">>})
        ),
        ?assertReceive({stopping_request_seen, ?REQ_PAYLOAD}, 5000),
        {StopTime, ok} = timer:tc(fun() -> emqx_plugins:ensure_stopped(NameVsn) end),
        ?assert(StopTime < timer:seconds(2) * 1000),
        ?assertMatch({503, _}, receive_async_response(Ref, 5000))
    after
        stop_client(Responder)
    end.

t_http_request_rejects_non_object_body(_Config) ->
    {Status, ResponseMap} = do_http_request(<<"not-an-object">>),
    ?assertEqual(415, Status),
    ?assertMatch(#{<<"code">> := <<"UNSUPPORTED_MEDIA_TYPE">>}, ResponseMap).

t_http_request_schema_rejects_invalid_payload_type(_Config) ->
    Body0 = request_body(
        <<"sync_request/schema/request">>,
        <<"sync_request/schema/response">>,
        <<"schema-request-id">>,
        #{}
    ),
    #{request := Request0} = Body0,
    Body = Body0#{request := Request0#{payload := #{unexpected => true}}},
    {Status, ResponseMap} = do_http_request(Body),
    ?assertEqual(400, Status),
    ?assertMatch(
        #{
            <<"code">> := <<"BAD_REQUEST">>,
            <<"message">> := #{<<"kind">> := <<"validation_error">>}
        },
        ResponseMap
    ).

t_delivered_message_registers_pending_with_local_timeout(_Config) ->
    ReqRef = make_ref(),
    ResponseTopic = <<"sync_request/local-timeout/response">>,
    CorrelationData = <<"local-timeout-request-id">>,
    TimeoutMs = 1000,
    true = ets:insert_new(?REQ_TAB, {ReqRef, #{waiter => self()}}),
    Message = #message{
        headers = #{
            properties => #{
                'Response-Topic' => ResponseTopic,
                'Correlation-Data' => CorrelationData
            },
            ?HEADER => #{req_ref => ReqRef, timeout => TimeoutMs}
        }
    },
    try
        ?assertEqual({ok, Message}, emqx_sync_request:on_message_delivered(#{}, Message)),
        [{ResponseTopic, _Seq, ReqRef, CorrelationData, Deadline}] =
            ets:lookup(?PENDING_TAB, ResponseTopic),
        ?assert(Deadline > erlang:monotonic_time(millisecond))
    after
        ets:delete(?REQ_TAB, ReqRef),
        emqx_sync_request:cleanup_remote_pending(ReqRef)
    end.

t_delivered_after_request_gone_does_not_register_pending(_Config) ->
    ReqRef = make_ref(),
    ResponseTopic = <<"sync_request/gone/response">>,
    CorrelationData = <<"gone-request-id">>,
    Message = #message{
        headers = #{
            properties => #{
                'Response-Topic' => ResponseTopic,
                'Correlation-Data' => CorrelationData
            },
            ?HEADER => #{req_ref => ReqRef, timeout => 1000}
        }
    },
    ?assertEqual(false, emqx_sync_request:is_request_inflight(ReqRef)),
    ?assertEqual({ok, Message}, emqx_sync_request:on_message_delivered(#{}, Message)),
    ?assertEqual([], ets:lookup(?PENDING_TAB, ResponseTopic)),
    ?assertEqual([], ets:lookup(?PENDING_BY_REQ_TAB, ReqRef)).

t_waiter_exit_cleans_inflight_and_pending(_Config) ->
    Parent = self(),
    ReqTopic = <<"sync_request/waiter-exit/request">>,
    RespTopic = <<"sync_request/waiter-exit/response">>,
    {ok, Responder} = start_blackhole_responder(
        <<"sync_request_waiter_exit_blackhole">>,
        ReqTopic,
        fun(Payload) -> Parent ! {waiter_exit_request_seen, Payload} end
    ),
    try
        ok = wait_for_subscribers(ReqTopic, 1),
        Waiter = spawn(fun() ->
            Parent ! {waiter_ready, self()},
            _ = emqx_sync_request:request(
                request_body_bin(ReqTopic, RespTopic, <<"waiter-exit-request-id">>, #{
                    timeout => <<"5s">>
                })
            ),
            Parent ! waiter_finished
        end),
        Waiter =
            receive
                {waiter_ready, Pid} -> Pid
            after 5000 ->
                error(waiter_not_ready)
            end,
        ?assertReceive({waiter_exit_request_seen, ?REQ_PAYLOAD}, 5000),
        ok = wait_until(
            fun() ->
                ets:info(?REQ_TAB, size) >= 1 andalso ets:info(?PENDING_TAB, size) >= 1
            end,
            50
        ),
        true = erlang:exit(Waiter, kill),
        ok = wait_until(
            fun() ->
                ets:info(?REQ_TAB, size) =:= 0 andalso ets:info(?PENDING_TAB, size) =:= 0
            end,
            50
        ),
        ?assertEqual(0, ets:info(?REQ_TAB, size)),
        ?assertEqual(0, ets:info(?PENDING_TAB, size)),
        ?assertNotReceive(waiter_finished, 100)
    after
        stop_client(Responder)
    end.

t_http_request_returns_first_mqtt5_response(_Config) ->
    Parent = self(),
    {ok, Responder} = emqx_request_handler:start_link(
        ?REQ_TOPIC,
        ?QOS_0,
        fun(CorrelationData, RequestPayload) ->
            Parent ! {request_seen, CorrelationData, RequestPayload},
            ?RESP_PAYLOAD
        end,
        [{clientid, <<"sync_request_responder">>}, {proto_ver, v5}]
    ),
    try
        RequestId = ?REQ_ID,
        Body = request_body(?REQ_TOPIC, ?RESP_TOPIC, RequestId, #{}),
        {Status, ResponseMap} = do_http_request(Body),
        ?assertEqual(200, Status),
        ?assertMatch(
            #{
                <<"code">> := <<"OK">>,
                <<"message">> := <<"OK">>,
                <<"response">> := #{
                    <<"topic">> := ?RESP_TOPIC,
                    <<"request_id">> := RequestId,
                    <<"payload_encoding">> := <<"base64">>
                }
            },
            ResponseMap
        ),
        #{<<"response">> := #{<<"payload">> := EncodedPayload}} = ResponseMap,
        ?assertEqual(?RESP_PAYLOAD, base64:decode(EncodedPayload)),
        ?assertReceive({request_seen, ?REQ_ID, ?REQ_PAYLOAD}, 5000)
    after
        emqx_request_handler:stop(Responder)
    end.

t_http_request_sets_mqtt5_properties_and_keeps_payload_opaque(_Config) ->
    Parent = self(),
    ReqTopic = <<"sync_request/properties/request">>,
    RespTopic = <<"sync_request/properties/response">>,
    RequestId = <<"properties-request-id">>,
    Payload = <<"{\"business\":\"payload\",\"response_topic\":\"not-used\"}">>,
    {ok, Responder} = start_v5_responder(
        <<"sync_request_properties_responder">>,
        ReqTopic,
        fun(_Client, #{properties := Props, payload := RequestPayload}) ->
            Parent ! {request_props_seen, Props, RequestPayload},
            Corr = maps:get('Correlation-Data', Props),
            ResponseProps = #{
                'Correlation-Data' => Corr,
                'Content-Type' => <<"application/vnd.response+json">>
            },
            {maps:get('Response-Topic', Props), ResponseProps, ?RESP_PAYLOAD}
        end
    ),
    try
        Body = request_body_with_request_overrides(
            ReqTopic,
            RespTopic,
            RequestId,
            #{payload => Payload, content_type => <<"application/vnd.request+json">>},
            #{}
        ),
        {Status, ResponseMap} = do_http_request(Body),
        ?assertEqual(200, Status),
        ?assertMatch(
            #{
                <<"code">> := <<"OK">>,
                <<"message">> := <<"OK">>,
                <<"response">> := #{
                    <<"topic">> := RespTopic,
                    <<"request_id">> := RequestId,
                    <<"content_type">> := <<"application/vnd.response+json">>
                }
            },
            ResponseMap
        ),
        ?assertReceive(
            {request_props_seen,
                #{
                    'Response-Topic' := RespTopic,
                    'Correlation-Data' := RequestId,
                    'Content-Type' := <<"application/vnd.request+json">>
                },
                Payload},
            5000
        )
    after
        stop_client(Responder)
    end.

t_http_request_accepts_string_qos_and_optional_content_type(_Config) ->
    Parent = self(),
    ReqTopic = <<"sync_request/string-qos/request">>,
    RespTopic = <<"sync_request/string-qos/response">>,
    RespPayload = <<"string-qos-response">>,
    {ok, Responder} = start_v5_responder(
        <<"sync_request_string_qos_responder">>,
        ReqTopic,
        fun(_Client, #{properties := Props, payload := RequestPayload}) ->
            Corr = maps:get('Correlation-Data', Props),
            Parent ! {string_qos_seen, Corr, Props, RequestPayload},
            ResponseProps = #{'Correlation-Data' => Corr},
            {maps:get('Response-Topic', Props), ResponseProps, RespPayload}
        end
    ),
    try
        lists:foreach(
            fun(QoS) ->
                ReqId = <<"string-qos-request-id-", QoS/binary>>,
                Body0 = request_body_with_request_overrides(
                    ReqTopic,
                    RespTopic,
                    ReqId,
                    #{qos => QoS},
                    #{}
                ),
                #{request := Request0} = Body0,
                Body = Body0#{request := maps:remove(content_type, Request0)},
                {Status, ResponseMap} = do_http_request(Body),
                ?assertEqual(200, Status),
                assert_response_payload(ResponseMap, RespPayload),
                #{<<"response">> := Response} = ResponseMap,
                ?assertEqual(false, maps:is_key(<<"content_type">>, Response)),
                receive
                    {string_qos_seen, ReqId, Props, ?REQ_PAYLOAD} ->
                        ?assertEqual(false, maps:is_key('Content-Type', Props))
                after 5000 ->
                    error({request_not_seen, ReqId})
                end
            end,
            [<<"0">>, <<"1">>, <<"2">>]
        )
    after
        stop_client(Responder)
    end.

t_http_request_decodes_base64_payload(_Config) ->
    Parent = self(),
    ReqTopic = <<"sync_request/base64/request">>,
    RespTopic = <<"sync_request/base64/response">>,
    Payload = <<"opaque-binary", 0, 1, 2, "payload">>,
    {ok, Responder} = start_v5_responder(
        <<"sync_request_base64_responder">>,
        ReqTopic,
        fun(_Client, #{properties := Props, payload := RequestPayload}) ->
            Parent ! {base64_payload_seen, RequestPayload},
            ResponseProps = #{'Correlation-Data' => maps:get('Correlation-Data', Props)},
            {maps:get('Response-Topic', Props), ResponseProps, ?RESP_PAYLOAD}
        end
    ),
    try
        Body = request_body_with_request_overrides(
            ReqTopic,
            RespTopic,
            <<"base64-request-id">>,
            #{
                payload_encoding => base64,
                payload => base64:encode(Payload)
            },
            #{}
        ),
        {Status, ResponseMap} = do_http_request(Body),
        ?assertEqual(200, Status),
        assert_response_payload(ResponseMap, ?RESP_PAYLOAD),
        ?assertReceive({base64_payload_seen, Payload}, 5000)
    after
        stop_client(Responder)
    end.

t_http_request_rejects_invalid_request_boundaries(_Config) ->
    Base = request_body(
        <<"sync_request/boundary/request">>,
        <<"sync_request/boundary/response">>,
        <<"boundary-request-id">>,
        #{}
    ),
    #{request := Request} = Base,
    Cases = [
        {missing_request, maps:remove(request, Base)},
        {missing_topic, Base#{request := maps:remove(topic, Request)}},
        {missing_response_topic, Base#{request := maps:remove(response_topic, Request)}},
        {missing_payload, Base#{request := maps:remove(payload, Request)}},
        {invalid_topic, Base#{request := Request#{topic => <<"sync_request/+/request">>}}},
        {invalid_response_topic, Base#{request := Request#{response_topic => <<"sync_request/#">>}}},
        {invalid_qos, Base#{request := Request#{qos => 3}}},
        {invalid_payload_encoding, Base#{request := Request#{payload_encoding => <<"hex">>}}},
        {invalid_base64_payload, Base#{
            request := Request#{payload_encoding => base64, payload => <<"not-base64">>}
        }},
        {invalid_timeout_format, Base#{timeout => <<"not-a-duration">>}},
        {invalid_timeout_zero, Base#{timeout => <<"0ms">>}}
    ],
    lists:foreach(
        fun({Name, Body}) ->
            {Status, ResponseMap} = do_http_request(Body),
            ?assertEqual({Name, 400}, {Name, Status}),
            ?assertMatch(#{<<"code">> := <<"BAD_REQUEST">>}, ResponseMap),
            ?assertEqual(false, maps:is_key(<<"response">>, ResponseMap))
        end,
        Cases
    ).

t_http_request_requires_request_id(_Config) ->
    Body0 = request_body(
        <<"sync_request/required/request">>,
        <<"sync_request/required/response">>,
        <<"required-request-id">>,
        #{}
    ),
    #{request := Request0} = Body0,
    Body = Body0#{request := maps:remove(request_id, Request0)},
    {Status, ResponseMap} = do_http_request(Body),
    ?assertEqual(400, Status),
    ?assertMatch(#{<<"code">> := <<"BAD_REQUEST">>}, ResponseMap),
    ?assertEqual(false, maps:is_key(<<"response">>, ResponseMap)).

t_http_request_rejects_request_id_too_large(_Config) ->
    Body = request_body(
        <<"sync_request/large-request-id/request">>,
        <<"sync_request/large-request-id/response">>,
        binary:copy(<<"x">>, 129),
        #{}
    ),
    {Status, ResponseMap} = do_http_request(Body),
    ?assertEqual(400, Status),
    ?assertMatch(
        #{
            <<"code">> := <<"BAD_REQUEST">>,
            <<"message">> := <<"request.request_id must be no longer than 128 bytes.">>
        },
        ResponseMap
    ),
    ?assertEqual(false, maps:is_key(<<"response">>, ResponseMap)).

t_http_request_rejects_request_payload_too_large(Config) ->
    with_config(
        Config,
        #{<<"max_payload_size">> => <<"8B">>},
        fun() ->
            Body = request_body_with_request_overrides(
                <<"sync_request/large-request/request">>,
                <<"sync_request/large-request/response">>,
                <<"large-request-id">>,
                #{payload => <<"123456789">>},
                #{}
            ),
            {Status, ResponseMap} = do_http_request(Body),
            ?assertEqual(400, Status),
            ?assertMatch(
                #{
                    <<"code">> := <<"BAD_REQUEST">>,
                    <<"message">> := <<"request.payload exceeds max_payload_size.">>
                },
                ResponseMap
            )
        end
    ).

t_http_request_rejects_invalid_timeout_above_max(Config) ->
    with_config(
        Config,
        #{<<"max_timeout">> => <<"100ms">>},
        fun() ->
            Body = request_body(
                <<"sync_request/invalid-timeout/request">>,
                <<"sync_request/invalid-timeout/response">>,
                <<"invalid-timeout-request-id">>,
                #{timeout => <<"101ms">>}
            ),
            {Status, ResponseMap} = do_http_request(Body),
            ?assertEqual(400, Status),
            ?assertMatch(
                #{
                    <<"code">> := <<"BAD_REQUEST">>,
                    <<"message">> :=
                        <<"timeout must be greater than 0 and no more than max_timeout.">>
                },
                ResponseMap
            )
        end
    ).

t_http_request_requires_management_api_auth(_Config) ->
    Body = request_body(
        <<"sync_request/auth/request">>,
        <<"sync_request/auth/response">>,
        <<"auth-request-id">>,
        #{}
    ),
    Path = emqx_mgmt_api_test_util:api_path([
        "plugin_api", "emqx_sync_request", "request"
    ]),
    InvalidAuth = emqx_common_test_http:auth_header("invalid", "password"),
    {Status, _ResponseMap} =
        emqx_mgmt_api_test_util:simplify_decode_result(
            emqx_mgmt_api_test_util:request_api(post, Path, "", InvalidAuth, Body, #{
                return_all => true
            })
        ),
    ?assertEqual(401, Status).

t_http_request_accepts_api_key_with_publish_scope(_Config) ->
    Parent = self(),
    Name = <<"sync-request-api-key-publish">>,
    ReqTopic = <<"sync_request/api-key/request">>,
    RespTopic = <<"sync_request/api-key/response">>,
    RespPayload = <<"api-key-response">>,
    {ok, Responder} = start_v5_responder(
        <<"sync_request_api_key_responder">>,
        ReqTopic,
        fun(_Client, #{properties := Props, payload := Payload}) ->
            Parent ! {api_key_request_seen, Payload},
            {
                maps:get('Response-Topic', Props),
                #{'Correlation-Data' => maps:get('Correlation-Data', Props)},
                RespPayload
            }
        end
    ),
    try
        {ok, #{<<"api_key">> := ApiKey, <<"api_secret">> := ApiSecret}} =
            create_api_key(Name, [?SCOPE_PUBLISH]),
        Auth = emqx_common_test_http:auth_header(binary_to_list(ApiKey), binary_to_list(ApiSecret)),
        Body = request_body(ReqTopic, RespTopic, <<"api-key-request-id">>, #{}),
        {Status, ResponseMap} =
            do_http_request(emqx_mgmt_api_test_util:default_server(), Auth, Body),
        ?assertEqual(200, Status),
        assert_response_payload(ResponseMap, RespPayload),
        ?assertReceive({api_key_request_seen, ?REQ_PAYLOAD}, 5000)
    after
        delete_api_key(Name),
        stop_client(Responder)
    end.

t_http_request_rejects_invalid_or_unscoped_api_key(_Config) ->
    Name = <<"sync-request-api-key-no-publish">>,
    {ok, #{<<"api_key">> := ApiKey, <<"api_secret">> := ApiSecret}} =
        create_api_key(Name, [?SCOPE_CONNECTIONS]),
    Body = request_body(
        <<"sync_request/api-key/reject/request">>,
        <<"sync_request/api-key/reject/response">>,
        <<"api-key-reject-request-id">>,
        #{timeout => <<"100ms">>}
    ),
    try
        BadSecretAuth = emqx_common_test_http:auth_header(
            binary_to_list(ApiKey), binary_to_list(<<ApiSecret/binary, "-wrong">>)
        ),
        {BadSecretStatus, BadSecretResponse} =
            do_http_request(emqx_mgmt_api_test_util:default_server(), BadSecretAuth, Body),
        ?assertEqual(401, BadSecretStatus),
        ?assertMatch(#{<<"code">> := <<"BAD_API_KEY_OR_SECRET">>}, BadSecretResponse),

        NoPublishAuth = emqx_common_test_http:auth_header(
            binary_to_list(ApiKey), binary_to_list(ApiSecret)
        ),
        {NoPublishStatus, NoPublishResponse} =
            do_http_request(emqx_mgmt_api_test_util:default_server(), NoPublishAuth, Body),
        ?assertEqual(403, NoPublishStatus),
        ?assertMatch(#{<<"code">> := <<"UNAUTHORIZED_ROLE">>}, NoPublishResponse)
    after
        delete_api_key(Name)
    end.

t_http_request_returns_offline_without_subscribers(_Config) ->
    Body = request_body(
        <<"sync_request/offline/request">>,
        <<"sync_request/offline/response">>,
        <<"offline-request-id">>,
        #{}
    ),
    {Status, ResponseMap} = do_http_request(Body),
    ?assertEqual(404, Status),
    ?assertMatch(
        #{
            <<"code">> := <<"NO_SUBSCRIBERS">>,
            <<"message">> := <<"No exact subscriber is online for the request topic.">>
        },
        ResponseMap
    ),
    ?assertEqual(false, maps:is_key(<<"response">>, ResponseMap)).

t_http_request_does_not_match_wildcard_subscriber(_Config) ->
    Parent = self(),
    ReqTopic = <<"sync_request/wildcard/request">>,
    RespTopic = <<"sync_request/wildcard/response">>,
    {ok, Responder} = start_blackhole_responder(
        <<"sync_request_wildcard_blackhole">>,
        <<"sync_request/wildcard/#">>,
        fun(Payload) -> Parent ! {wildcard_request_seen, Payload} end
    ),
    try
        Body = request_body(
            ReqTopic,
            RespTopic,
            <<"wildcard-request-id">>,
            #{timeout => <<"100ms">>}
        ),
        {Status, ResponseMap} = do_http_request(Body),
        ?assertEqual(404, Status),
        ?assertMatch(
            #{
                <<"code">> := <<"NO_SUBSCRIBERS">>,
                <<"message">> := <<"No exact subscriber is online for the request topic.">>
            },
            ResponseMap
        ),
        ?assertEqual(false, maps:is_key(<<"response">>, ResponseMap)),
        ?assertNotReceive({wildcard_request_seen, _}, 200)
    after
        stop_client(Responder)
    end.

t_http_request_rejects_shared_subscription(_Config) ->
    Parent = self(),
    ReqTopic = <<"sync_request/shared/request">>,
    RespTopic = <<"sync_request/shared/response">>,
    {ok, Responder} = start_blackhole_responder(
        <<"sync_request_shared_blackhole">>,
        <<"$share/sync_request_group/sync_request/shared/request">>,
        fun(Payload) -> Parent ! {shared_request_seen, Payload} end
    ),
    try
        Body = request_body(
            ReqTopic,
            RespTopic,
            <<"shared-request-id">>,
            #{timeout => <<"100ms">>}
        ),
        {Status, ResponseMap} = do_http_request(Body),
        ?assertEqual(409, Status),
        ?assertMatch(
            #{
                <<"code">> := <<"CONFLICT">>,
                <<"message">> :=
                    <<"The request topic has a shared subscription or more than one exact subscriber.">>
            },
            ResponseMap
        ),
        ?assertEqual(false, maps:is_key(<<"response">>, ResponseMap)),
        ?assertNotReceive({shared_request_seen, _}, 200)
    after
        stop_client(Responder)
    end.

t_http_request_times_out_without_response(_Config) ->
    Parent = self(),
    ReqTopic = <<"sync_request/timeout/request">>,
    RespTopic = <<"sync_request/timeout/response">>,
    {ok, Responder} = start_blackhole_responder(
        <<"sync_request_blackhole">>,
        ReqTopic,
        fun(Payload) -> Parent ! {request_seen_timeout, Payload} end
    ),
    try
        Body = request_body(
            ReqTopic,
            RespTopic,
            <<"timeout-request-id">>,
            #{timeout => <<"100ms">>}
        ),
        {Status, ResponseMap} = do_http_request(Body),
        ?assertEqual(504, Status),
        ?assertMatch(
            #{
                <<"code">> := <<"TIMEOUT">>,
                <<"message">> := <<"Timed out waiting for a matching MQTT response.">>
            },
            ResponseMap
        ),
        ?assertEqual(false, maps:is_key(<<"response">>, ResponseMap)),
        ?assertReceive({request_seen_timeout, ?REQ_PAYLOAD}, 5000)
    after
        stop_client(Responder)
    end.

t_http_request_rejects_response_payload_too_large(Config) ->
    with_config(
        Config,
        #{<<"max_payload_size">> => <<"8B">>},
        fun() ->
            ReqTopic = <<"sync_request/large-response/request">>,
            RespTopic = <<"sync_request/large-response/response">>,
            {ok, Responder} = emqx_request_handler:start_link(
                ReqTopic,
                ?QOS_0,
                fun(_CorrelationData, _RequestPayload) ->
                    <<"123456789">>
                end,
                [{clientid, <<"sync_request_large_response_responder">>}, {proto_ver, v5}]
            ),
            try
                Body = request_body(
                    ReqTopic,
                    RespTopic,
                    <<"large-response-request-id">>,
                    #{
                        request => #{
                            topic => ReqTopic,
                            response_topic => RespTopic,
                            qos => 0,
                            request_id => <<"large-response-request-id">>,
                            payload_encoding => plain,
                            payload => <<"12345678">>,
                            content_type => <<"application/json">>
                        }
                    }
                ),
                {Status, ResponseMap} = do_http_request(Body),
                ?assertEqual(400, Status),
                ?assertMatch(
                    #{
                        <<"code">> := <<"BAD_REQUEST">>,
                        <<"message">> := <<"MQTT response payload exceeds max_payload_size.">>
                    },
                    ResponseMap
                ),
                ?assertEqual(false, maps:is_key(<<"response">>, ResponseMap))
            after
                emqx_request_handler:stop(Responder)
            end
        end
    ).

t_http_request_rejects_when_http_inflight_limit_reached(Config) ->
    with_config(
        Config,
        #{<<"max_inflight_requests">> => 1},
        fun() ->
            Parent = self(),
            ReqTopic = <<"sync_request/http-inflight/request">>,
            RespTopic = <<"sync_request/http-inflight/response">>,
            ReqId1 = <<"http-inflight-request-id-1">>,
            {ok, Responder} = start_blackhole_responder(
                <<"sync_request_http_inflight_blackhole">>,
                ReqTopic,
                fun(Payload) -> Parent ! {http_inflight_request_seen, Payload} end
            ),
            {ok, Publisher} = start_client(<<"sync_request_http_inflight_publisher">>, v5),
            try
                Body1 = request_body(
                    ReqTopic,
                    RespTopic,
                    ReqId1,
                    #{timeout => <<"5s">>}
                ),
                Ref1 = async_http_request(Body1),
                ?assertReceive({http_inflight_request_seen, ?REQ_PAYLOAD}, 5000),
                Body2 = request_body(
                    ReqTopic,
                    RespTopic,
                    <<"http-inflight-request-id-2">>,
                    #{timeout => <<"250ms">>}
                ),
                {Status2, ResponseMap2} = do_http_request(Body2),
                ?assertEqual(429, Status2),
                ?assertMatch(
                    #{
                        <<"code">> := <<"TOO_MANY_REQUESTS">>,
                        <<"message">> := <<"Too many sync requests are waiting for responses.">>
                    },
                    ResponseMap2
                ),
                ok = normalize_publish(
                    emqtt:publish(
                        Publisher,
                        RespTopic,
                        #{'Correlation-Data' => ReqId1},
                        <<"http-inflight-release">>,
                        [{qos, ?QOS_0}]
                    )
                ),
                {Status1, ResponseMap1} = receive_async_response(Ref1, 5000),
                ?assertEqual(200, Status1),
                assert_response_payload(ResponseMap1, <<"http-inflight-release">>)
            after
                stop_client(Publisher),
                stop_client(Responder)
            end
        end
    ).

t_http_request_enforces_http_inflight_limit_concurrently(Config) ->
    with_config(
        Config,
        #{<<"max_inflight_requests">> => 1},
        fun() ->
            Parent = self(),
            Before = emqx_sync_request:status(),
            ReqTopic = <<"sync_request/http-inflight-concurrent/request">>,
            RespTopic = <<"sync_request/http-inflight-concurrent/response">>,
            {ok, Responder} = start_blackhole_responder(
                <<"sync_request_http_inflight_concurrent_blackhole">>,
                ReqTopic,
                fun(Payload) -> Parent ! {http_inflight_concurrent_request_seen, Payload} end
            ),
            {ok, Publisher} = start_client(
                <<"sync_request_http_inflight_concurrent_publisher">>, v5
            ),
            try
                Requests = [
                    begin
                        Suffix = integer_to_binary(I),
                        ReqId = <<"http-inflight-concurrent-", Suffix/binary>>,
                        Payload = <<"payload-", Suffix/binary>>,
                        Body = request_body_with_request_overrides(
                            ReqTopic,
                            RespTopic,
                            ReqId,
                            #{payload => Payload},
                            #{timeout => <<"1s">>}
                        ),
                        {ReqId, Payload, async_http_request(Body)}
                    end
                 || I <- lists:seq(1, 8)
                ],
                {ReqId, _AcceptedPayload, _Ref} =
                    receive
                        {http_inflight_concurrent_request_seen, Payload0} ->
                            lists:keyfind(Payload0, 2, Requests)
                    after 5000 ->
                        error(http_inflight_request_not_seen)
                    end,
                ok = wait_until(fun() -> ets:info(?PENDING_TAB, size) =:= 1 end, 50),
                ok = wait_until(
                    fun() ->
                        maps:get(requests_total, emqx_sync_request:status()) >=
                            maps:get(requests_total, Before) + 7
                    end,
                    50
                ),
                ok = normalize_publish(
                    emqtt:publish(
                        Publisher,
                        RespTopic,
                        #{'Correlation-Data' => ReqId},
                        <<"http-inflight-concurrent-release">>,
                        [{qos, ?QOS_0}]
                    )
                ),
                Statuses = [
                    Status
                 || {_ReqId, _RequestPayload, Ref} <- Requests,
                    {Status, _Response} <- [receive_async_response(Ref, 5000)]
                ],
                ?assertEqual(1, length([ok || 200 <- Statuses])),
                ?assertEqual(7, length([ok || 429 <- Statuses]))
            after
                stop_client(Publisher),
                stop_client(Responder)
            end
        end
    ).

t_http_request_rejects_multiple_exact_subscribers(_Config) ->
    Parent = self(),
    ReqTopic = <<"sync_request/multiple-subscribers/request">>,
    RespTopic = <<"sync_request/multiple-subscribers/response">>,
    {ok, Responder1} = start_blackhole_responder(
        <<"sync_request_multiple_subscribers_blackhole_1">>,
        ReqTopic,
        fun(Payload) -> Parent ! {multiple_subscribers_seen, Payload} end
    ),
    {ok, Responder2} = start_blackhole_responder(
        <<"sync_request_multiple_subscribers_blackhole_2">>,
        ReqTopic,
        fun(Payload) -> Parent ! {multiple_subscribers_seen, Payload} end
    ),
    try
        ok = wait_for_subscribers(ReqTopic, 2),
        Body = request_body(
            ReqTopic,
            RespTopic,
            <<"multiple-subscribers-request-id">>,
            #{timeout => <<"100ms">>}
        ),
        {Status, ResponseMap} = do_http_request(Body),
        ?assertEqual(409, Status),
        ?assertMatch(
            #{
                <<"code">> := <<"CONFLICT">>,
                <<"message">> :=
                    <<"The request topic has a shared subscription or more than one exact subscriber.">>
            },
            ResponseMap
        ),
        ?assertEqual(false, maps:is_key(<<"response">>, ResponseMap)),
        ?assertNotReceive({multiple_subscribers_seen, _}, 200)
    after
        stop_client(Responder1),
        stop_client(Responder2)
    end.

t_http_request_rejects_sharded_exact_subscribers(_Config) ->
    Parent = self(),
    ReqTopic = <<"sync_request/sharded-subscribers/request">>,
    RespTopic = <<"sync_request/sharded-subscribers/response">>,
    ForcedShards = force_nonzero_subscription_shard(ReqTopic),
    {ok, Responder1} = start_blackhole_responder(
        <<"sync_request_sharded_subscribers_blackhole_1">>,
        ReqTopic,
        fun(Payload) -> Parent ! {sharded_subscribers_seen, Payload} end
    ),
    {ok, Responder2} = start_blackhole_responder(
        <<"sync_request_sharded_subscribers_blackhole_2">>,
        ReqTopic,
        fun(Payload) -> Parent ! {sharded_subscribers_seen, Payload} end
    ),
    try
        ?assert(
            lists:any(
                fun({Shard, Count}) -> Shard > 0 andalso Count > 0 end,
                emqx_broker_helper:assigned_sub_shards(ReqTopic)
            )
        ),
        Body = request_body(
            ReqTopic,
            RespTopic,
            <<"sharded-subscribers-request-id">>,
            #{timeout => <<"100ms">>}
        ),
        {Status, ResponseMap} = do_http_request(Body),
        ?assertEqual(409, Status),
        ?assertMatch(
            #{
                <<"code">> := <<"CONFLICT">>,
                <<"message">> :=
                    <<"The request topic has a shared subscription or more than one exact subscriber.">>
            },
            ResponseMap
        ),
        ?assertEqual(false, maps:is_key(<<"response">>, ResponseMap)),
        ?assertNotReceive({sharded_subscribers_seen, _}, 200)
    after
        stop_client(Responder1),
        stop_client(Responder2),
        release_forced_subscription_shards(ReqTopic, ForcedShards)
    end.

t_mqtt5_response_ignores_mismatched_correlation_data(_Config) ->
    ReqTopic = <<"sync_request/correlation/request">>,
    RespTopic = <<"sync_request/correlation/response">>,
    GoodPayload = <<"good-correlation-response">>,
    BadPayload = <<"bad-correlation-response">>,
    {ok, Responder} = start_v5_responder(
        <<"sync_request_correlation_responder">>,
        ReqTopic,
        fun(Client, #{properties := Props}) ->
            RespTopic0 = maps:get('Response-Topic', Props),
            Corr = maps:get('Correlation-Data', Props),
            publish_from_handler(
                Client,
                RespTopic0,
                #{'Correlation-Data' => <<"wrong-correlation">>},
                BadPayload,
                ?QOS_0
            ),
            timer:sleep(50),
            publish_from_handler(
                Client,
                RespTopic0,
                #{'Correlation-Data' => Corr},
                GoodPayload,
                ?QOS_0
            ),
            noreply
        end
    ),
    try
        RequestId = <<"correlation-request-id">>,
        Body = request_body(ReqTopic, RespTopic, RequestId, #{}),
        {Status, ResponseMap} = do_http_request(Body),
        ?assertEqual(200, Status),
        assert_response_payload(ResponseMap, GoodPayload)
    after
        stop_client(Responder)
    end.

t_http_request_matches_mqtt3_response_by_topic_sequence(_Config) ->
    Parent = self(),
    ReqTopic = <<"sync_request/v3/request">>,
    RespTopic = <<"sync_request/v3/response">>,
    {ok, Responder} = start_mqtt3_responder(
        <<"sync_request_v3_responder">>,
        ReqTopic,
        RespTopic,
        fun(Payload) -> Parent ! {request_seen_v3, Payload} end
    ),
    try
        RequestId = <<"mqtt3-request-id">>,
        Body = request_body(ReqTopic, RespTopic, RequestId, #{}),
        {Status, ResponseMap} = do_http_request(Body),
        ?assertEqual(200, Status),
        ?assertMatch(
            #{
                <<"code">> := <<"OK">>,
                <<"message">> := <<"OK">>,
                <<"response">> := #{
                    <<"topic">> := RespTopic,
                    <<"request_id">> := RequestId,
                    <<"payload_encoding">> := <<"base64">>
                }
            },
            ResponseMap
        ),
        #{<<"response">> := #{<<"payload">> := EncodedPayload}} = ResponseMap,
        ?assertEqual(?RESP_PAYLOAD, base64:decode(EncodedPayload)),
        #{<<"response">> := Response} = ResponseMap,
        ?assertEqual(false, maps:is_key(<<"content_type">>, Response)),
        ?assertReceive({request_seen_v3, ?REQ_PAYLOAD}, 5000)
    after
        stop_client(Responder)
    end.

t_mqtt3_concurrent_requests_match_response_topic_sequence(_Config) ->
    Parent = self(),
    ReqTopic1 = <<"sync_request/v3-sequence/request/1">>,
    ReqTopic2 = <<"sync_request/v3-sequence/request/2">>,
    RespTopic = <<"sync_request/v3-sequence/response">>,
    ReqId1 = <<"mqtt3-sequence-request-id-1">>,
    ReqId2 = <<"mqtt3-sequence-request-id-2">>,
    RespPayload1 = <<"mqtt3-sequence-response-1">>,
    RespPayload2 = <<"mqtt3-sequence-response-2">>,
    {ok, Responder} = start_blackhole_responder(
        v3,
        <<"sync_request_v3_sequence_responder">>,
        ReqTopic1,
        fun(Payload) -> Parent ! {v3_sequence_request_seen, Payload} end
    ),
    {ok, Responder2} = start_blackhole_responder(
        v3,
        <<"sync_request_v3_sequence_responder_2">>,
        ReqTopic2,
        fun(Payload) -> Parent ! {v3_sequence_request_seen, Payload} end
    ),
    {ok, Publisher} = start_client(<<"sync_request_v3_sequence_publisher">>, v3),
    try
        ok = wait_for_subscribers(ReqTopic1, 1),
        ok = wait_for_subscribers(ReqTopic2, 1),
        Body1 = request_body(ReqTopic1, RespTopic, ReqId1, #{timeout => <<"5s">>}),
        Ref1 = async_http_request(Body1),
        ?assertReceive({v3_sequence_request_seen, ?REQ_PAYLOAD}, 5000),
        Body2 = request_body(ReqTopic2, RespTopic, ReqId2, #{timeout => <<"5s">>}),
        Ref2 = async_http_request(Body2),
        ?assertReceive({v3_sequence_request_seen, ?REQ_PAYLOAD}, 5000),
        ok = normalize_publish(emqtt:publish(Publisher, RespTopic, RespPayload1, ?QOS_0)),
        ok = normalize_publish(emqtt:publish(Publisher, RespTopic, RespPayload2, ?QOS_0)),
        {Status1, ResponseMap1} = receive_async_response(Ref1, 5000),
        {Status2, ResponseMap2} = receive_async_response(Ref2, 5000),
        ?assertEqual(200, Status1),
        ?assertEqual(200, Status2),
        ?assertMatch(#{<<"response">> := #{<<"request_id">> := ReqId1}}, ResponseMap1),
        ?assertMatch(#{<<"response">> := #{<<"request_id">> := ReqId2}}, ResponseMap2),
        assert_response_payload(ResponseMap1, RespPayload1),
        assert_response_payload(ResponseMap2, RespPayload2)
    after
        stop_client(Publisher),
        stop_client(Responder),
        stop_client(Responder2)
    end.

t_first_response_wins_and_late_response_is_dropped(_Config) ->
    ReqTopic = <<"sync_request/first-response/request">>,
    RespTopic = <<"sync_request/first-response/response">>,
    FirstPayload = <<"first-response">>,
    LatePayload = <<"late-response">>,
    {ok, Responder} = start_v5_responder(
        <<"sync_request_first_response_responder">>,
        ReqTopic,
        fun(Client, #{properties := Props}) ->
            RespTopic0 = maps:get('Response-Topic', Props),
            Corr = maps:get('Correlation-Data', Props),
            publish_from_handler(
                Client,
                RespTopic0,
                #{'Correlation-Data' => Corr},
                FirstPayload,
                ?QOS_0
            ),
            timer:sleep(50),
            publish_from_handler(
                Client,
                RespTopic0,
                #{'Correlation-Data' => Corr},
                LatePayload,
                ?QOS_0
            ),
            noreply
        end
    ),
    try
        Body = request_body(
            ReqTopic,
            RespTopic,
            <<"first-response-request-id">>,
            #{}
        ),
        {Status, ResponseMap} = do_http_request(Body),
        ?assertEqual(200, Status),
        assert_response_payload(ResponseMap, FirstPayload),
        timer:sleep(100)
    after
        stop_client(Responder)
    end.

request_body(ReqTopic, RespTopic, RequestId, Overrides) ->
    Base = #{
        timeout => <<"5s">>,
        request => #{
            topic => ReqTopic,
            response_topic => RespTopic,
            qos => 0,
            request_id => RequestId,
            payload_encoding => plain,
            payload => ?REQ_PAYLOAD,
            content_type => <<"application/json">>
        }
    },
    maps:merge(Base, Overrides).

request_body_bin(ReqTopic, RespTopic, RequestId, Overrides) ->
    emqx_utils_json:decode(
        emqx_utils_json:encode(request_body(ReqTopic, RespTopic, RequestId, Overrides))
    ).

request_body_with_request_overrides(
    ReqTopic, RespTopic, RequestId, RequestOverrides, BodyOverrides
) ->
    Body0 = request_body(ReqTopic, RespTopic, RequestId, #{}),
    #{request := Request0} = Body0,
    maps:merge(Body0#{request := maps:merge(Request0, RequestOverrides)}, BodyOverrides).

do_http_request(Body) ->
    do_http_request(
        emqx_mgmt_api_test_util:default_server(),
        emqx_mgmt_api_test_util:auth_header_(),
        Body
    ).

do_http_request(Host, Auth, Body) ->
    Path = emqx_mgmt_api_test_util:api_path(Host, [
        "plugin_api", "emqx_sync_request", "request"
    ]),
    Headers = [Auth, {"Connection", "close"}, {"Content-Type", "application/json"}],
    emqx_mgmt_api_test_util:simplify_decode_result(
        emqx_mgmt_api_test_util:request_api(post, Path, "", Headers, Body, #{return_all => true})
    ).

plugin_config_request(Method, NameVsn, Body) ->
    plugin_config_request(
        emqx_mgmt_api_test_util:default_server(),
        emqx_mgmt_api_test_util:auth_header_(),
        Method,
        NameVsn,
        Body
    ).

plugin_config_request(Host, Auth, Method, NameVsn, Body) ->
    Path = emqx_mgmt_api_test_util:api_path(Host, ["plugins", NameVsn, "config"]),
    Headers = [Auth, {"Connection", "close"}, {"Content-Type", "application/json"}],
    RequestBody =
        case Method of
            get -> [];
            _ -> Body
        end,
    emqx_mgmt_api_test_util:simplify_decode_result(
        emqx_mgmt_api_test_util:request_api(Method, Path, "", Headers, RequestBody, #{
            return_all => true
        })
    ).

api_status(Body) ->
    {Status, _Response} = do_http_request(Body),
    Status.

async_http_request(Body) ->
    Parent = self(),
    Ref = make_ref(),
    _ = spawn_link(fun() ->
        Parent ! {sync_request_http_result, Ref, do_http_request(Body)}
    end),
    Ref.

receive_async_response(Ref, Timeout) ->
    receive
        {sync_request_http_result, Ref, Result} ->
            Result
    after Timeout ->
        error({timeout_waiting_for_http_result, Ref})
    end.

with_config(CTConfig, Config, Fun) ->
    NameVsn = ?config(plugin_name_vsn, CTConfig),
    OldConfig = emqx_plugins:get_config(NameVsn),
    ok = emqx_plugins:update_config(NameVsn, Config),
    try
        Fun()
    after
        ok = emqx_plugins:update_config(NameVsn, OldConfig)
    end.

create_api_key(Name, Scopes) ->
    delete_api_key(Name),
    Path = emqx_mgmt_api_test_util:api_path(["api_key"]),
    Body = #{
        name => Name,
        desc => <<"sync request test">>,
        enable => true,
        expired_at => <<"infinity">>,
        scopes => Scopes
    },
    case
        emqx_mgmt_api_test_util:request_api(
            post, Path, "", emqx_dashboard_SUITE:auth_header_(), Body
        )
    of
        {ok, Res} -> {ok, emqx_utils_json:decode(Res)};
        Error -> Error
    end.

delete_api_key(Name) ->
    Path = emqx_mgmt_api_test_util:api_path(["api_key", Name]),
    _ = emqx_mgmt_api_test_util:request_api(delete, Path, emqx_dashboard_SUITE:auth_header_()),
    ok.

assert_response_payload(ResponseMap, ExpectedPayload) ->
    #{<<"response">> := #{<<"payload">> := EncodedPayload}} = ResponseMap,
    ?assertEqual(ExpectedPayload, base64:decode(EncodedPayload)).

plugin_package() ->
    Root = emqx_common_test_helpers:proj_root(),
    Vsn = string:trim(read_file(filename:join([Root, "plugins", "emqx_sync_request", "VERSION"]))),
    Package = filename:join([Root, "_build", "plugins", "emqx_sync_request-" ++ Vsn ++ ".tar.gz"]),
    _ = file:delete(Package),
    build_in_tree_plugin_package(Root, Package).

build_in_tree_plugin_package(Root, Package) ->
    Output = os:cmd(
        "cd " ++ Root ++
            " && PROFILE=emqx-enterprise make plugin-emqx_sync_request 2>&1"
    ),
    case filelib:is_regular(Package) of
        true ->
            Package;
        false ->
            error({plugin_package_build_failed, Package, Output})
    end.

read_file(Path) ->
    {ok, Bin} = file:read_file(Path),
    binary_to_list(Bin).

mock_ctl_print() ->
    catch meck:unload(emqx_ctl),
    meck:new(emqx_ctl, [non_strict, passthrough]),
    meck:expect(emqx_ctl, print, fun(Arg) -> emqx_ctl:format(Arg, []) end),
    meck:expect(emqx_ctl, print, fun(Msg, Args) -> emqx_ctl:format(Msg, Args) end).

unmock_ctl_print() ->
    meck:unload(emqx_ctl).

install_and_start_plugin(Config) ->
    NameVsn = ?config(plugin_name_vsn, Config),
    PackageBin = ?config(plugin_package_bin, Config),
    ok = emqx_plugins:write_package(NameVsn, PackageBin),
    ok = emqx_plugins:allow_installation(
        NameVsn,
        binary:encode_hex(crypto:hash(sha256, PackageBin), lowercase)
    ),
    ok = emqx_plugins:ensure_installed(NameVsn, fresh_install),
    ok = emqx_plugins:ensure_started(NameVsn),
    ?assertEqual(400, api_status(#{request => #{}})),
    ok.

cleanup_plugin(Config) ->
    NameVsn = ?config(plugin_name_vsn, Config),
    case emqx_plugins:describe(NameVsn, #{fill_readme => false, health_check => false}) of
        {ok, _Plugin} ->
            _ = emqx_plugins:ensure_stopped(NameVsn),
            _ = emqx_plugins:ensure_disabled(NameVsn),
            _ = emqx_plugins:ensure_uninstalled(NameVsn);
        {error, _Reason} ->
            ok
    end,
    _ = emqx_plugins:delete_package(NameVsn),
    _ = emqx_plugins:forget_allowed_installation(NameVsn),
    ok.

wait_for_subscribers(Topic, ExpectedCount) ->
    wait_for_subscribers(Topic, ExpectedCount, 50).

wait_for_subscribers(Topic, ExpectedCount, Attempts) when Attempts > 0 ->
    case length(emqx:subscribers(Topic)) >= ExpectedCount of
        true ->
            ok;
        false ->
            timer:sleep(20),
            wait_for_subscribers(Topic, ExpectedCount, Attempts - 1)
    end;
wait_for_subscribers(Topic, ExpectedCount, 0) ->
    error({subscribers_not_ready, Topic, ExpectedCount, emqx:subscribers(Topic)}).

wait_until(Fun, Attempts) when Attempts > 0 ->
    case Fun() of
        true ->
            ok;
        false ->
            timer:sleep(20),
            wait_until(Fun, Attempts - 1)
    end;
wait_until(Fun, 0) ->
    error({wait_until_timeout, Fun}).

force_nonzero_subscription_shard(Topic) ->
    [
        emqx_broker_helper:assign_sub_shard(Topic)
     || _ <- lists:seq(1, emqx_broker_helper:shard_capacity() + 1)
    ].

release_forced_subscription_shards(Topic, Shards) ->
    lists:foreach(
        fun(Shard) -> _ = emqx_broker_helper:unassign_sub_shard(Topic, Shard) end,
        Shards
    ).

start_blackhole_responder(ClientId, ReqTopic, OnRequest) ->
    start_blackhole_responder(v5, ClientId, ReqTopic, OnRequest).

start_blackhole_responder(ProtoVer, ClientId, ReqTopic, OnRequest) ->
    MsgHandler = #{
        publish => fun(#{payload := Payload}) ->
            OnRequest(Payload),
            ok
        end,
        puback => fun(_Ack) -> ok end,
        disconnected => fun(_Reason) -> ok end
    },
    start_subscriber(ClientId, ProtoVer, ReqTopic, MsgHandler).

start_v5_responder(ClientId, ReqTopic, OnRequest) ->
    MsgHandler = #{
        publish => fun(Msg) ->
            Client = self(),
            case OnRequest(Client, Msg) of
                {RespTopic, RespProps, RespPayload} ->
                    publish_from_handler(
                        Client, RespTopic, RespProps, RespPayload, maps:get(qos, Msg, ?QOS_0)
                    );
                noreply ->
                    ok
            end
        end,
        puback => fun(_Ack) -> ok end,
        disconnected => fun(_Reason) -> ok end
    },
    start_subscriber(ClientId, v5, ReqTopic, MsgHandler).

start_mqtt3_responder(ClientId, ReqTopic, RespTopic, OnRequest) ->
    MsgHandler = #{
        publish => fun(#{payload := Payload}) ->
            OnRequest(Payload),
            Client = self(),
            _ = spawn_link(fun() ->
                ok = emqtt:publish(Client, RespTopic, ?RESP_PAYLOAD, ?QOS_0)
            end),
            ok
        end,
        puback => fun(_Ack) -> ok end,
        disconnected => fun(_Reason) -> ok end
    },
    start_subscriber(ClientId, v3, ReqTopic, MsgHandler).

publish_from_handler(Client, RespTopic, RespProps, RespPayload, QoS) ->
    _ = spawn_link(fun() ->
        ok = normalize_publish(
            emqtt:publish(Client, RespTopic, RespProps, RespPayload, [{qos, QoS}])
        )
    end),
    ok.

start_subscriber(ClientId, ProtoVer, ReqTopic, MsgHandler) ->
    {ok, Client} = start_client(ClientId, ProtoVer, MsgHandler),
    {ok, _Props, [?QOS_0]} = emqtt:subscribe(Client, ReqTopic, ?QOS_0),
    {ok, Client}.

start_client(ClientId, ProtoVer) ->
    start_client(ClientId, ProtoVer, #{
        publish => fun(_Msg) -> ok end,
        puback => fun(_Ack) -> ok end,
        disconnected => fun(_Reason) -> ok end
    }).

start_client(ClientId, ProtoVer, MsgHandler) ->
    start_client(ClientId, ProtoVer, MsgHandler, []).

start_client(ClientId, ProtoVer, MsgHandler, ExtraOpts) ->
    {ok, Client} = emqtt:start_link(
        [
            {clientid, ClientId},
            {proto_ver, ProtoVer},
            {msg_handler, MsgHandler}
        ] ++ ExtraOpts
    ),
    {ok, _} = emqtt:connect(Client),
    {ok, Client}.

stop_client(Client) ->
    catch emqtt:disconnect(Client),
    ok.

normalize_publish(ok) -> ok;
normalize_publish({ok, _}) -> ok;
normalize_publish({error, Reason}) -> error({publish_failed, Reason}).
