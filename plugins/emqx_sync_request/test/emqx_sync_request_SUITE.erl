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

-define(REQ_TOPIC, <<"sync_request/device/1001/request">>).
-define(RESP_TOPIC, <<"sync_request/device/1001/response">>).
-define(REQ_ID, <<"request-id-1">>).
-define(REQ_PAYLOAD, <<"{\"cmd\":\"reboot\"}">>).
-define(RESP_PAYLOAD, <<"{\"result\":\"ok\"}">>).

all() ->
    [
        t_plugin_install_start_stop_uninstall_controls_api_route,
        t_plugin_health_check_reports_ok,
        t_http_request_rejects_non_object_body,
        t_http_request_returns_first_mqtt5_response,
        t_http_request_sets_mqtt5_properties_and_keeps_payload_opaque,
        t_http_request_accepts_string_qos_and_optional_content_type,
        t_http_request_decodes_base64_payload,
        t_http_request_rejects_invalid_request_boundaries,
        t_http_request_requires_request_id,
        t_http_request_rejects_request_id_too_large,
        t_http_request_rejects_request_payload_too_large,
        t_http_request_rejects_invalid_timeout_above_max,
        t_http_request_requires_management_api_auth,
        t_http_request_returns_offline_without_subscribers,
        t_http_request_times_out_without_response,
        t_http_request_rejects_response_payload_too_large,
        t_http_request_rejects_when_http_inflight_limit_reached,
        t_http_request_allows_multiple_pending_deliveries_per_http_request,
        t_mqtt5_response_ignores_mismatched_correlation_data,
        t_http_request_matches_mqtt3_response_by_topic_sequence,
        t_mqtt3_concurrent_requests_match_response_topic_sequence,
        t_first_response_wins_and_late_response_is_dropped
    ].

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
    Package = plugin_package(),
    {ok, PackageBin} = file:read_file(Package),
    NameVsn = filename:basename(Package, ".tar.gz"),
    [
        {apps, Apps},
        {install_dir, InstallDir},
        {plugin_cover_file, filename:join([WorkDir, "emqx_sync_request.coverdata"])},
        {plugin_name_vsn, NameVsn},
        {plugin_package, Package},
        {plugin_package_bin, PackageBin}
        | Config
    ].

end_per_suite(Config) ->
    ok = maybe_finalize_cover(Config),
    ok = emqx_cth_suite:stop(?config(apps, Config)).

init_per_testcase(_TestCase, Config) ->
    ok = cleanup_plugin(Config),
    ok = install_and_start_plugin(Config),
    Config.

end_per_testcase(_TestCase, Config) ->
    ok = maybe_export_cover(Config),
    ok = cleanup_plugin(Config).

t_plugin_install_start_stop_uninstall_controls_api_route(Config) ->
    NameVsn = ?config(plugin_name_vsn, Config),
    ok = maybe_export_cover(Config),
    ok = cleanup_plugin(Config),
    ?assertEqual(404, api_status(#{request => #{}})),

    ok = install_and_start_plugin(Config),
    ?assertEqual(400, api_status(#{request => #{}})),

    ok = emqx_plugins:ensure_stopped(NameVsn),
    ok = maybe_export_cover(Config),
    ?assertEqual(404, api_status(#{request => #{}})),

    ok = emqx_plugins:ensure_started(NameVsn),
    ok = maybe_cover_plugin_modules(Config),
    ?assertEqual(400, api_status(#{request => #{}})),

    ok = emqx_plugins:ensure_stopped(NameVsn),
    ok = maybe_export_cover(Config),
    ok = emqx_plugins:ensure_uninstalled(NameVsn),
    ?assertEqual(404, api_status(#{request => #{}})).

t_plugin_health_check_reports_ok(Config) ->
    NameVsn = ?config(plugin_name_vsn, Config),
    ?assertMatch(
        {ok, #{health_status := #{status := ok, message := <<"">>}}},
        emqx_plugins:describe(NameVsn, #{fill_readme => false, health_check => true})
    ).

t_http_request_rejects_non_object_body(_Config) ->
    {Status, ResponseMap} = do_http_request(<<"not-an-object">>),
    ?assertEqual(400, Status),
    ?assertMatch(
        #{
            <<"status">> := <<"UNKNOWN">>,
            <<"reason">> := <<"invalid_request_body">>
        },
        ResponseMap
    ).

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
                <<"status">> := <<"OK">>,
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
                'Content-Type' => <<"application/json">>
            },
            {maps:get('Response-Topic', Props), ResponseProps, ?RESP_PAYLOAD}
        end
    ),
    try
        Body = request_body_with_request_overrides(
            ReqTopic,
            RespTopic,
            RequestId,
            #{payload => Payload, content_type => <<"application/json">>},
            #{}
        ),
        {Status, ResponseMap} = do_http_request(Body),
        ?assertEqual(200, Status),
        ?assertMatch(
            #{
                <<"status">> := <<"OK">>,
                <<"response">> := #{
                    <<"topic">> := RespTopic,
                    <<"request_id">> := RequestId,
                    <<"content_type">> := <<"application/json">>
                }
            },
            ResponseMap
        ),
        ?assertReceive(
            {request_props_seen,
                #{
                    'Response-Topic' := RespTopic,
                    'Correlation-Data' := RequestId,
                    'Content-Type' := <<"application/json">>
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
        {missing_request, maps:remove(request, Base), <<"request_required">>},
        {missing_topic, Base#{request := maps:remove(topic, Request)}, <<"topic_required">>},
        {missing_response_topic, Base#{request := maps:remove(response_topic, Request)},
            <<"response_topic_required">>},
        {missing_payload, Base#{request := maps:remove(payload, Request)}, <<"payload_required">>},
        {invalid_topic, Base#{request := Request#{topic => <<"sync_request/+/request">>}},
            <<"invalid_topic">>},
        {invalid_response_topic, Base#{request := Request#{response_topic => <<"sync_request/#">>}},
            <<"invalid_topic">>},
        {invalid_qos, Base#{request := Request#{qos => 3}}, <<"invalid_qos">>},
        {invalid_payload_encoding, Base#{request := Request#{payload_encoding => <<"hex">>}},
            <<"invalid_payload_encoding">>},
        {invalid_base64_payload,
            Base#{request := Request#{payload_encoding => base64, payload => <<"not-base64">>}},
            <<"invalid_base64_payload">>},
        {invalid_timeout_format, Base#{timeout => <<"not-a-duration">>}, <<"invalid_duration">>},
        {invalid_timeout_zero, Base#{timeout => <<"0ms">>}, <<"invalid_timeout">>}
    ],
    lists:foreach(
        fun({Name, Body, Reason}) ->
            {Status, ResponseMap} = do_http_request(Body),
            ?assertEqual({Name, 400}, {Name, Status}),
            ?assertMatch(
                #{
                    <<"status">> := <<"UNKNOWN">>,
                    <<"reason">> := Reason
                },
                ResponseMap
            ),
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
    ?assertMatch(
        #{
            <<"status">> := <<"UNKNOWN">>,
            <<"reason">> := <<"request_id_required">>
        },
        ResponseMap
    ),
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
            <<"status">> := <<"UNKNOWN">>,
            <<"reason">> := <<"request_id_too_large">>
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
                    <<"status">> := <<"UNKNOWN">>,
                    <<"reason">> := <<"request_payload_too_large">>
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
                    <<"status">> := <<"UNKNOWN">>,
                    <<"reason">> := <<"invalid_timeout">>
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
            <<"status">> := <<"OFFLINE">>,
            <<"reason">> := <<"no_subscribers">>
        },
        ResponseMap
    ),
    ?assertEqual(false, maps:is_key(<<"response">>, ResponseMap)).

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
                <<"status">> := <<"TIMEOUT">>,
                <<"reason">> := <<"timeout">>
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
                ?assertEqual(500, Status),
                ?assertMatch(
                    #{
                        <<"status">> := <<"UNKNOWN">>,
                        <<"reason">> := <<"response_payload_too_large">>
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
                        <<"status">> := <<"UNKNOWN">>,
                        <<"reason">> := <<"too_many_inflight_requests">>
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

t_http_request_allows_multiple_pending_deliveries_per_http_request(Config) ->
    with_config(
        Config,
        #{<<"max_inflight_requests">> => 1},
        fun() ->
            Parent = self(),
            ReqTopic = <<"sync_request/pending-limit/request">>,
            RespTopic = <<"sync_request/pending-limit/response">>,
            ReqId = <<"pending-limit-request-id">>,
            {ok, Responder1} = start_blackhole_responder(
                <<"sync_request_pending_limit_blackhole_1">>,
                ReqTopic,
                fun(Payload) -> Parent ! {pending_limit_request_seen, Payload} end
            ),
            {ok, Responder2} = start_blackhole_responder(
                <<"sync_request_pending_limit_blackhole_2">>,
                ReqTopic,
                fun(Payload) -> Parent ! {pending_limit_request_seen, Payload} end
            ),
            {ok, Responder3} = start_blackhole_responder(
                <<"sync_request_pending_limit_blackhole_3">>,
                ReqTopic,
                fun(Payload) -> Parent ! {pending_limit_request_seen, Payload} end
            ),
            {ok, Publisher} = start_client(<<"sync_request_pending_limit_publisher">>, v5),
            try
                ok = wait_for_subscribers(ReqTopic, 3),
                Body = request_body(
                    ReqTopic,
                    RespTopic,
                    ReqId,
                    #{timeout => <<"5s">>}
                ),
                Ref = async_http_request(Body),
                ?assertReceive({pending_limit_request_seen, ?REQ_PAYLOAD}, 5000),
                ?assertReceive({pending_limit_request_seen, ?REQ_PAYLOAD}, 5000),
                ?assertReceive({pending_limit_request_seen, ?REQ_PAYLOAD}, 5000),
                ok = normalize_publish(
                    emqtt:publish(
                        Publisher,
                        RespTopic,
                        #{'Correlation-Data' => ReqId},
                        <<"pending-limit-response">>,
                        [{qos, ?QOS_0}]
                    )
                ),
                {Status, ResponseMap} = receive_async_response(Ref, 5000),
                ?assertEqual(200, Status),
                assert_response_payload(ResponseMap, <<"pending-limit-response">>)
            after
                stop_client(Publisher),
                stop_client(Responder1),
                stop_client(Responder2),
                stop_client(Responder3)
            end
        end
    ).

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
                <<"status">> := <<"OK">>,
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
    Headers = [Auth, {"Connection", "close"}],
    emqx_mgmt_api_test_util:simplify_decode_result(
        emqx_mgmt_api_test_util:request_api(post, Path, "", Headers, Body, #{return_all => true})
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

assert_response_payload(ResponseMap, ExpectedPayload) ->
    #{<<"response">> := #{<<"payload">> := EncodedPayload}} = ResponseMap,
    ?assertEqual(ExpectedPayload, base64:decode(EncodedPayload)).

plugin_package() ->
    case os:getenv("PLUGIN_PACKAGE") of
        false ->
            in_tree_plugin_package();
        "" ->
            in_tree_plugin_package();
        Package ->
            case filelib:is_regular(Package) of
                true -> Package;
                false -> error({missing_plugin_package, Package})
            end
    end.

in_tree_plugin_package() ->
    Root = repo_root(),
    Vsn = string:trim(read_file(filename:join([Root, "plugins", "emqx_sync_request", "VERSION"]))),
    Package = filename:join([Root, "_build", "plugins", "emqx_sync_request-" ++ Vsn ++ ".tar.gz"]),
    case filelib:is_regular(Package) of
        true ->
            Package;
        false ->
            build_in_tree_plugin_package(Root, Package)
    end.

build_in_tree_plugin_package(Root, Package) ->
    Output = os:cmd(
        "cd " ++ Root ++ " && PROFILE=test ./scripts/build-plugin.sh emqx_sync_request 2>&1"
    ),
    case filelib:is_regular(Package) of
        true ->
            Package;
        false ->
            error({plugin_package_build_failed, Package, Output})
    end.

repo_root() ->
    Candidates = [
        os:getenv("PWD"),
        cwd(),
        emqx_common_test_helpers:proj_root()
    ],
    case [Root || Root <- Candidates, is_repo_root(Root)] of
        [Root | _] ->
            Root;
        [] ->
            error({missing_repo_root, Candidates})
    end.

cwd() ->
    {ok, Cwd} = file:get_cwd(),
    Cwd.

is_repo_root(false) ->
    false;
is_repo_root(Root) ->
    filelib:is_regular(filename:join([Root, "plugins", "emqx_sync_request", "VERSION"])).

read_file(Path) ->
    {ok, Bin} = file:read_file(Path),
    binary_to_list(Bin).

install_and_start_plugin(Config) ->
    NameVsn = ?config(plugin_name_vsn, Config),
    PackageBin = ?config(plugin_package_bin, Config),
    ok = emqx_plugins:write_package(NameVsn, PackageBin),
    ok = emqx_plugins:allow_installation(
        NameVsn,
        binary:encode_hex(crypto:hash(sha256, PackageBin), lowercase)
    ),
    ok = emqx_plugins:ensure_installed(NameVsn, fresh_install),
    ok = maybe_cover_plugin_modules(Config),
    ok = emqx_plugins:ensure_started(NameVsn),
    ?assertEqual(400, api_status(#{request => #{}})),
    ok.

plugin_modules() ->
    [
        emqx_sync_request,
        emqx_sync_request_api,
        emqx_sync_request_app,
        emqx_sync_request_sup
    ].

maybe_cover_plugin_modules(Config) ->
    with_cover(fun() ->
        lists:foreach(fun cover_plugin_module/1, plugin_modules()),
        maybe_import_cover(Config)
    end).

maybe_export_cover(Config) ->
    with_cover(fun() -> cover:export(?config(plugin_cover_file, Config)) end).

maybe_finalize_cover(Config) ->
    with_cover(fun() ->
        case filelib:is_regular(?config(plugin_cover_file, Config)) of
            true ->
                lists:foreach(fun cover_plugin_module/1, plugin_modules()),
                maybe_import_cover(Config);
            false ->
                ok
        end
    end).

with_cover(Fun) ->
    case erlang:whereis(cover_server) of
        undefined -> ok;
        _Pid -> Fun()
    end.

cover_plugin_module(Module) ->
    Beam = filename:join([plugin_build_ebin(), atom_to_list(Module) ++ ".beam"]),
    case cover:compile_beam(Beam) of
        {ok, Module} ->
            ok;
        {error, Reason} ->
            error({cover_compile_failed, Module, Beam, Reason})
    end.

plugin_build_ebin() ->
    filename:join([filename:dirname(filename:dirname(code:which(?MODULE))), "ebin"]).

maybe_import_cover(Config) ->
    File = ?config(plugin_cover_file, Config),
    case filelib:is_regular(File) of
        true -> cover:import(File);
        false -> ok
    end.

cleanup_plugin(Config) ->
    NameVsn = ?config(plugin_name_vsn, Config),
    case emqx_plugins:describe(NameVsn, #{fill_readme => false, health_check => false}) of
        {ok, _Plugin} ->
            _ = emqx_plugins:ensure_stopped(NameVsn),
            ok = maybe_export_cover(Config),
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
