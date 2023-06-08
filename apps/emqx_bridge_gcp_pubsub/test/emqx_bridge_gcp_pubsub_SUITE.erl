%%--------------------------------------------------------------------
%% Copyright (c) 2022-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_gcp_pubsub_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("jose/include/jose_jwt.hrl").
-include_lib("jose/include/jose_jws.hrl").

-define(BRIDGE_TYPE, gcp_pubsub).
-define(BRIDGE_TYPE_BIN, <<"gcp_pubsub">>).

-import(emqx_common_test_helpers, [on_exit/1]).

%%------------------------------------------------------------------------------
%% CT boilerplate
%%------------------------------------------------------------------------------

all() ->
    SimpleTCs = single_config_tests(),
    [
        {group, with_batch},
        {group, without_batch}
        | SimpleTCs
    ].

groups() ->
    TCs = emqx_common_test_helpers:all(?MODULE),
    SimpleTCs = single_config_tests(),
    MatrixTCs = TCs -- SimpleTCs,
    SynchronyGroups = [
        {group, sync_query},
        {group, async_query}
    ],
    ResourceGroups = [{group, gcp_pubsub}],
    [
        {with_batch, SynchronyGroups},
        {without_batch, SynchronyGroups},
        {sync_query, ResourceGroups},
        {async_query, ResourceGroups},
        {gcp_pubsub, MatrixTCs}
    ].

%% these should not be influenced by the batch/no
%% batch/sync/async/queueing matrix.
single_config_tests() ->
    [
        t_not_a_json,
        t_not_of_service_account_type,
        t_json_missing_fields,
        t_invalid_private_key,
        t_truncated_private_key,
        t_jose_error_tuple,
        t_jose_other_error,
        t_stop,
        t_get_status_ok,
        t_get_status_down,
        t_get_status_no_worker,
        t_get_status_timeout_calling_workers,
        t_on_start_ehttpc_pool_already_started
    ].

init_per_suite(Config) ->
    ok = emqx_common_test_helpers:start_apps([emqx_conf]),
    ok = emqx_connector_test_helpers:start_apps([emqx_resource, emqx_bridge, emqx_rule_engine]),
    {ok, _} = application:ensure_all_started(emqx_connector),
    emqx_mgmt_api_test_util:init_suite(),
    Config.

end_per_suite(_Config) ->
    emqx_mgmt_api_test_util:end_suite(),
    ok = emqx_common_test_helpers:stop_apps([emqx_conf]),
    ok = emqx_connector_test_helpers:stop_apps([emqx_bridge, emqx_resource, emqx_rule_engine]),
    _ = application:stop(emqx_connector),
    ok.

init_per_group(sync_query, Config) ->
    [{query_mode, sync} | Config];
init_per_group(async_query, Config) ->
    [{query_mode, async} | Config];
init_per_group(with_batch, Config) ->
    [{batch_size, 100} | Config];
init_per_group(without_batch, Config) ->
    [{batch_size, 1} | Config];
init_per_group(_Group, Config) ->
    Config.

end_per_group(gcp_pubsub, Config) ->
    delete_bridge(Config),
    ok;
end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(TestCase, Config0) when
    TestCase =:= t_publish_success_batch
->
    ct:timetrap({seconds, 30}),
    case ?config(batch_size, Config0) of
        1 ->
            [{skip_due_to_no_batching, true}];
        _ ->
            delete_all_bridges(),
            Tid = install_telemetry_handler(TestCase),
            Config = generate_config(Config0),
            put(telemetry_table, Tid),
            {ok, HttpServer} = start_echo_http_server(),
            [{telemetry_table, Tid}, {http_server, HttpServer} | Config]
    end;
init_per_testcase(TestCase, Config0) ->
    ct:timetrap({seconds, 30}),
    {ok, HttpServer} = start_echo_http_server(),
    delete_all_bridges(),
    Tid = install_telemetry_handler(TestCase),
    Config = generate_config(Config0),
    put(telemetry_table, Tid),
    [{telemetry_table, Tid}, {http_server, HttpServer} | Config].

end_per_testcase(_TestCase, _Config) ->
    ok = snabbkaffe:stop(),
    delete_all_bridges(),
    ok = stop_echo_http_server(),
    emqx_common_test_helpers:call_janitor(),
    ok.

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

generate_config(Config0) ->
    #{
        name := Name,
        config_string := ConfigString,
        pubsub_config := PubSubConfig,
        service_account_json := ServiceAccountJSON
    } = gcp_pubsub_config(Config0),
    ResourceId = emqx_bridge_resource:resource_id(?BRIDGE_TYPE_BIN, Name),
    BridgeId = emqx_bridge_resource:bridge_id(?BRIDGE_TYPE_BIN, Name),
    [
        {gcp_pubsub_name, Name},
        {gcp_pubsub_config, PubSubConfig},
        {gcp_pubsub_config_string, ConfigString},
        {service_account_json, ServiceAccountJSON},
        {resource_id, ResourceId},
        {bridge_id, BridgeId}
        | Config0
    ].

delete_all_bridges() ->
    ct:pal("deleting all bridges"),
    lists:foreach(
        fun(#{name := Name, type := Type}) ->
            emqx_bridge:remove(Type, Name)
        end,
        emqx_bridge:list()
    ).

delete_bridge(Config) ->
    Type = ?BRIDGE_TYPE,
    Name = ?config(gcp_pubsub_name, Config),
    ct:pal("deleting bridge ~p", [{Type, Name}]),
    emqx_bridge:remove(Type, Name).

create_bridge(Config) ->
    create_bridge(Config, _GCPPubSubConfigOverrides = #{}).

create_bridge(Config, GCPPubSubConfigOverrides) ->
    TypeBin = ?BRIDGE_TYPE_BIN,
    Name = ?config(gcp_pubsub_name, Config),
    GCPPubSubConfig0 = ?config(gcp_pubsub_config, Config),
    GCPPubSubConfig = emqx_utils_maps:deep_merge(GCPPubSubConfig0, GCPPubSubConfigOverrides),
    ct:pal("creating bridge: ~p", [GCPPubSubConfig]),
    Res = emqx_bridge:create(TypeBin, Name, GCPPubSubConfig),
    ct:pal("bridge creation result: ~p", [Res]),
    Res.

create_bridge_http(Config) ->
    create_bridge_http(Config, _GCPPubSubConfigOverrides = #{}).

create_bridge_http(Config, GCPPubSubConfigOverrides) ->
    TypeBin = ?BRIDGE_TYPE_BIN,
    Name = ?config(gcp_pubsub_name, Config),
    GCPPubSubConfig0 = ?config(gcp_pubsub_config, Config),
    GCPPubSubConfig = emqx_utils_maps:deep_merge(GCPPubSubConfig0, GCPPubSubConfigOverrides),
    Params = GCPPubSubConfig#{<<"type">> => TypeBin, <<"name">> => Name},
    Path = emqx_mgmt_api_test_util:api_path(["bridges"]),
    AuthHeader = emqx_mgmt_api_test_util:auth_header_(),
    ProbePath = emqx_mgmt_api_test_util:api_path(["bridges_probe"]),
    ProbeResult = emqx_mgmt_api_test_util:request_api(post, ProbePath, "", AuthHeader, Params),
    ct:pal("creating bridge (via http): ~p", [Params]),
    ct:pal("probe result: ~p", [ProbeResult]),
    Res =
        case emqx_mgmt_api_test_util:request_api(post, Path, "", AuthHeader, Params) of
            {ok, Res0} -> {ok, emqx_utils_json:decode(Res0, [return_maps])};
            Error -> Error
        end,
    ct:pal("bridge creation result: ~p", [Res]),
    ?assertEqual(element(1, ProbeResult), element(1, Res)),
    Res.

create_rule_and_action_http(Config) ->
    GCPPubSubName = ?config(gcp_pubsub_name, Config),
    BridgeId = emqx_bridge_resource:bridge_id(?BRIDGE_TYPE_BIN, GCPPubSubName),
    Params = #{
        enable => true,
        sql => <<"SELECT * FROM \"t/topic\"">>,
        actions => [BridgeId]
    },
    Path = emqx_mgmt_api_test_util:api_path(["rules"]),
    AuthHeader = emqx_mgmt_api_test_util:auth_header_(),
    case emqx_mgmt_api_test_util:request_api(post, Path, "", AuthHeader, Params) of
        {ok, Res} -> {ok, emqx_utils_json:decode(Res, [return_maps])};
        Error -> Error
    end.

success_http_handler() ->
    TestPid = self(),
    fun(Req0, State) ->
        {ok, Body, Req} = cowboy_req:read_body(Req0),
        TestPid ! {http, cowboy_req:headers(Req), Body},
        Rep = cowboy_req:reply(
            200,
            #{<<"content-type">> => <<"application/json">>},
            emqx_utils_json:encode(#{messageIds => [<<"6058891368195201">>]}),
            Req
        ),
        {ok, Rep, State}
    end.

start_echo_http_server() ->
    HTTPHost = "localhost",
    HTTPPath = <<"/v1/projects/myproject/topics/mytopic:publish">>,
    ServerSSLOpts =
        [
            {verify, verify_none},
            {versions, ['tlsv1.2', 'tlsv1.3']},
            {ciphers, ["ECDHE-RSA-AES256-GCM-SHA384", "TLS_CHACHA20_POLY1305_SHA256"]}
        ] ++ certs(),
    {ok, {HTTPPort, _Pid}} = emqx_connector_web_hook_server:start_link(
        random, HTTPPath, ServerSSLOpts
    ),
    ok = emqx_connector_web_hook_server:set_handler(success_http_handler()),
    HTTPHost = "localhost",
    HostPort = HTTPHost ++ ":" ++ integer_to_list(HTTPPort),
    true = os:putenv("PUBSUB_EMULATOR_HOST", HostPort),
    {ok, #{
        host_port => HostPort,
        host => HTTPHost,
        port => HTTPPort
    }}.

stop_echo_http_server() ->
    os:unsetenv("PUBSUB_EMULATOR_HOST"),
    ok = emqx_connector_web_hook_server:stop().

certs() ->
    CertsPath = emqx_common_test_helpers:deps_path(emqx, "etc/certs"),
    [
        {keyfile, filename:join([CertsPath, "key.pem"])},
        {certfile, filename:join([CertsPath, "cert.pem"])},
        {cacertfile, filename:join([CertsPath, "cacert.pem"])}
    ].

gcp_pubsub_config(Config) ->
    QueryMode = proplists:get_value(query_mode, Config, sync),
    BatchSize = proplists:get_value(batch_size, Config, 100),
    BatchTime = proplists:get_value(batch_time, Config, <<"20ms">>),
    PayloadTemplate = proplists:get_value(payload_template, Config, ""),
    PubSubTopic = proplists:get_value(pubsub_topic, Config, <<"mytopic">>),
    PipelineSize = proplists:get_value(pipeline_size, Config, 100),
    ServiceAccountJSON = proplists:get_value(pubsub_topic, Config, generate_service_account_json()),
    ServiceAccountJSONStr = emqx_utils_json:encode(ServiceAccountJSON),
    GUID = emqx_guid:to_hexstr(emqx_guid:gen()),
    Name = <<(atom_to_binary(?MODULE))/binary, (GUID)/binary>>,
    ConfigString =
        io_lib:format(
            "bridges.gcp_pubsub.~s {\n"
            "  enable = true\n"
            "  connect_timeout = 1s\n"
            "  service_account_json = ~s\n"
            "  payload_template = ~p\n"
            "  pubsub_topic = ~s\n"
            "  pool_size = 1\n"
            "  pipelining = ~b\n"
            "  resource_opts = {\n"
            "    request_ttl = 500ms\n"
            "    metrics_flush_interval = 700ms\n"
            "    worker_pool_size = 1\n"
            "    query_mode = ~s\n"
            "    batch_size = ~b\n"
            "    batch_time = \"~s\"\n"
            "  }\n"
            "}\n",
            [
                Name,
                ServiceAccountJSONStr,
                PayloadTemplate,
                PubSubTopic,
                PipelineSize,
                QueryMode,
                BatchSize,
                BatchTime
            ]
        ),
    #{
        name => Name,
        config_string => ConfigString,
        pubsub_config => parse_and_check(ConfigString, Name),
        service_account_json => ServiceAccountJSON
    }.

parse_and_check(ConfigString, Name) ->
    {ok, RawConf} = hocon:binary(ConfigString, #{format => map}),
    TypeBin = <<"gcp_pubsub">>,
    hocon_tconf:check_plain(emqx_bridge_schema, RawConf, #{required => false, atom_key => false}),
    #{<<"bridges">> := #{TypeBin := #{Name := Config}}} = RawConf,
    Config.

generate_service_account_json() ->
    PrivateKeyPEM = generate_private_key_pem(),
    service_account_json(PrivateKeyPEM).

generate_private_key_pem() ->
    PublicExponent = 65537,
    Size = 2048,
    Key = public_key:generate_key({rsa, Size, PublicExponent}),
    DERKey = public_key:der_encode('PrivateKeyInfo', Key),
    public_key:pem_encode([{'PrivateKeyInfo', DERKey, not_encrypted}]).

service_account_json(PrivateKeyPEM) ->
    #{
        <<"type">> => <<"service_account">>,
        <<"project_id">> => <<"myproject">>,
        <<"private_key_id">> => <<"kid">>,
        <<"private_key">> => PrivateKeyPEM,
        <<"client_email">> => <<"test@myproject.iam.gserviceaccount.com">>,
        <<"client_id">> => <<"123812831923812319190">>,
        <<"auth_uri">> => <<"https://accounts.google.com/o/oauth2/auth">>,
        <<"token_uri">> => <<"https://oauth2.googleapis.com/token">>,
        <<"auth_provider_x509_cert_url">> => <<"https://www.googleapis.com/oauth2/v1/certs">>,
        <<"client_x509_cert_url">> =>
            <<"https://www.googleapis.com/robot/v1/metadata/x509/test%40myproject.iam.gserviceaccount.com">>
    }.

metrics_mapping() ->
    #{
        dropped => fun emqx_resource_metrics:dropped_get/1,
        dropped_expired => fun emqx_resource_metrics:dropped_expired_get/1,
        dropped_other => fun emqx_resource_metrics:dropped_other_get/1,
        dropped_queue_full => fun emqx_resource_metrics:dropped_queue_full_get/1,
        dropped_resource_not_found => fun emqx_resource_metrics:dropped_resource_not_found_get/1,
        dropped_resource_stopped => fun emqx_resource_metrics:dropped_resource_stopped_get/1,
        late_reply => fun emqx_resource_metrics:late_reply_get/1,
        failed => fun emqx_resource_metrics:failed_get/1,
        inflight => fun emqx_resource_metrics:inflight_get/1,
        matched => fun emqx_resource_metrics:matched_get/1,
        queuing => fun emqx_resource_metrics:queuing_get/1,
        retried => fun emqx_resource_metrics:retried_get/1,
        retried_failed => fun emqx_resource_metrics:retried_failed_get/1,
        retried_success => fun emqx_resource_metrics:retried_success_get/1,
        success => fun emqx_resource_metrics:success_get/1
    }.

current_metrics(ResourceId) ->
    Mapping = metrics_mapping(),
    maps:from_list([
        {Metric, F(ResourceId)}
     || {Metric, F} <- maps:to_list(Mapping)
    ]).

assert_metrics(ExpectedMetrics, ResourceId) ->
    Mapping = metrics_mapping(),
    Metrics =
        lists:foldl(
            fun(Metric, Acc) ->
                #{Metric := Fun} = Mapping,
                Value = Fun(ResourceId),
                Acc#{Metric => Value}
            end,
            #{},
            maps:keys(ExpectedMetrics)
        ),
    CurrentMetrics = current_metrics(ResourceId),
    TelemetryTable = get(telemetry_table),
    RecordedEvents = ets:tab2list(TelemetryTable),
    ?assertEqual(ExpectedMetrics, Metrics, #{
        current_metrics => CurrentMetrics, recorded_events => RecordedEvents
    }),
    ok.

assert_empty_metrics(ResourceId) ->
    Mapping = metrics_mapping(),
    ExpectedMetrics =
        lists:foldl(
            fun(Metric, Acc) ->
                Acc#{Metric => 0}
            end,
            #{},
            maps:keys(Mapping)
        ),
    assert_metrics(ExpectedMetrics, ResourceId).

verify_token(ServiceAccountJSON, JWTBin) ->
    #{
        <<"private_key">> := PrivateKeyPEM,
        <<"private_key_id">> := KId,
        <<"client_email">> := ServiceAccountEmail
    } = ServiceAccountJSON,
    JWK = jose_jwk:from_pem(PrivateKeyPEM),
    {IsValid, JWT, JWS} = jose_jwt:verify(JWK, JWTBin),
    ?assertMatch(
        #jose_jwt{
            fields =
                #{
                    <<"aud">> := <<"https://pubsub.googleapis.com/">>,
                    <<"exp">> := _,
                    <<"iat">> := _,
                    <<"iss">> := ServiceAccountEmail,
                    <<"sub">> := ServiceAccountEmail
                }
        },
        JWT
    ),
    #jose_jwt{
        fields =
            #{
                <<"exp">> := Exp,
                <<"iat">> := Iat
            }
    } = JWT,
    ?assertEqual(Iat + 60 * 60, Exp),
    ?assert(Iat =< erlang:system_time(seconds)),
    ?assertMatch(
        #jose_jws{
            alg = {_Module, 'RS256'},
            fields =
                #{
                    <<"kid">> := KId,
                    <<"typ">> := <<"JWT">>
                }
        },
        JWS
    ),
    ?assert(IsValid, #{
        jwt => JWT,
        jws => JWS
    }),
    ok.

assert_valid_request_headers(Headers, ServiceAccountJSON) ->
    case Headers of
        #{<<"authorization">> := <<"Bearer ", JWT/binary>>} ->
            verify_token(ServiceAccountJSON, JWT),
            ok;
        _ ->
            %% better to raise a value than to use `ct:fail'
            %% because it doesn't output very well...
            error({expected_bearer_authn_header, #{headers => Headers}})
    end.

assert_valid_request_body(Body) ->
    BodyMap = emqx_utils_json:decode(Body, [return_maps]),
    ?assertMatch(#{<<"messages">> := [_ | _]}, BodyMap),
    #{<<"messages">> := Messages} = BodyMap,
    lists:map(
        fun(Msg) ->
            ?assertMatch(#{<<"data">> := <<_/binary>>}, Msg),
            #{<<"data">> := Content64} = Msg,
            Content = base64:decode(Content64),
            Decoded = emqx_utils_json:decode(Content, [return_maps]),
            ct:pal("decoded payload: ~p", [Decoded]),
            ?assert(is_map(Decoded)),
            Decoded
        end,
        Messages
    ).

assert_http_request(ServiceAccountJSON) ->
    receive
        {http, Headers, Body} ->
            assert_valid_request_headers(Headers, ServiceAccountJSON),
            assert_valid_request_body(Body)
    after 5_000 ->
        {messages, Mailbox} = process_info(self(), messages),
        error({timeout, #{mailbox => Mailbox}})
    end.

install_telemetry_handler(TestCase) ->
    Tid = ets:new(TestCase, [ordered_set, public]),
    HandlerId = TestCase,
    TestPid = self(),
    _ = telemetry:attach_many(
        HandlerId,
        emqx_resource_metrics:events(),
        fun(EventName, Measurements, Metadata, _Config) ->
            Data = #{
                name => EventName,
                measurements => Measurements,
                metadata => Metadata
            },
            ets:insert(Tid, {erlang:monotonic_time(), Data}),
            TestPid ! {telemetry, Data},
            ok
        end,
        unused_config
    ),
    on_exit(fun() ->
        telemetry:detach(HandlerId),
        ets:delete(Tid)
    end),
    Tid.

wait_until_gauge_is(GaugeName, ExpectedValue, Timeout) ->
    Events = receive_all_events(GaugeName, Timeout),
    case length(Events) > 0 andalso lists:last(Events) of
        #{measurements := #{gauge_set := ExpectedValue}} ->
            ok;
        #{measurements := #{gauge_set := Value}} ->
            ct:pal("events: ~p", [Events]),
            ct:fail(
                "gauge ~p didn't reach expected value ~p; last value: ~p",
                [GaugeName, ExpectedValue, Value]
            );
        false ->
            ct:pal("no ~p gauge events received!", [GaugeName])
    end.

receive_all_events(EventName, Timeout) ->
    receive_all_events(EventName, Timeout, _MaxEvents = 10, _Count = 0, _Acc = []).

receive_all_events(_EventName, _Timeout, MaxEvents, Count, Acc) when Count >= MaxEvents ->
    lists:reverse(Acc);
receive_all_events(EventName, Timeout, MaxEvents, Count, Acc) ->
    receive
        {telemetry, #{name := [_, _, EventName]} = Event} ->
            receive_all_events(EventName, Timeout, MaxEvents, Count + 1, [Event | Acc])
    after Timeout ->
        lists:reverse(Acc)
    end.

wait_telemetry_event(TelemetryTable, EventName, ResourceId) ->
    wait_telemetry_event(TelemetryTable, EventName, ResourceId, #{timeout => 5_000, n_events => 1}).

wait_telemetry_event(
    TelemetryTable,
    EventName,
    ResourceId,
    _Opts = #{
        timeout := Timeout,
        n_events := NEvents
    }
) ->
    wait_n_events(TelemetryTable, ResourceId, NEvents, Timeout, EventName).

wait_n_events(_TelemetryTable, _ResourceId, NEvents, _Timeout, _EventName) when NEvents =< 0 ->
    ok;
wait_n_events(TelemetryTable, ResourceId, NEvents, Timeout, EventName) ->
    receive
        {telemetry, #{name := [_, _, EventName], measurements := #{counter_inc := Inc}} = Event} ->
            ct:pal("telemetry event: ~p", [Event]),
            wait_n_events(TelemetryTable, ResourceId, NEvents - Inc, Timeout, EventName)
    after Timeout ->
        RecordedEvents = ets:tab2list(TelemetryTable),
        CurrentMetrics = current_metrics(ResourceId),
        ct:pal("recorded events: ~p", [RecordedEvents]),
        ct:pal("current metrics: ~p", [CurrentMetrics]),
        error({timeout_waiting_for_telemetry, EventName})
    end.

%%------------------------------------------------------------------------------
%% Testcases
%%------------------------------------------------------------------------------

t_publish_success(Config) ->
    ResourceId = ?config(resource_id, Config),
    ServiceAccountJSON = ?config(service_account_json, Config),
    TelemetryTable = ?config(telemetry_table, Config),
    Topic = <<"t/topic">>,
    ?assertMatch({ok, _}, create_bridge(Config)),
    {ok, #{<<"id">> := RuleId}} = create_rule_and_action_http(Config),
    on_exit(fun() -> ok = emqx_rule_engine:delete_rule(RuleId) end),
    assert_empty_metrics(ResourceId),
    Payload = <<"payload">>,
    Message = emqx_message:make(Topic, Payload),
    emqx:publish(Message),
    DecodedMessages = assert_http_request(ServiceAccountJSON),
    ?assertMatch(
        [
            #{
                <<"topic">> := Topic,
                <<"payload">> := Payload,
                <<"metadata">> := #{<<"rule_id">> := RuleId}
            }
        ],
        DecodedMessages
    ),
    %% to avoid test flakiness
    wait_telemetry_event(TelemetryTable, success, ResourceId),
    wait_until_gauge_is(queuing, 0, 500),
    wait_until_gauge_is(inflight, 0, 500),
    assert_metrics(
        #{
            dropped => 0,
            failed => 0,
            inflight => 0,
            matched => 1,
            queuing => 0,
            retried => 0,
            success => 1
        },
        ResourceId
    ),
    ok.

t_publish_success_infinity_timeout(Config) ->
    ServiceAccountJSON = ?config(service_account_json, Config),
    Topic = <<"t/topic">>,
    {ok, _} = create_bridge(Config, #{
        <<"resource_opts">> => #{<<"request_ttl">> => <<"infinity">>}
    }),
    {ok, #{<<"id">> := RuleId}} = create_rule_and_action_http(Config),
    on_exit(fun() -> ok = emqx_rule_engine:delete_rule(RuleId) end),
    Payload = <<"payload">>,
    Message = emqx_message:make(Topic, Payload),
    emqx:publish(Message),
    DecodedMessages = assert_http_request(ServiceAccountJSON),
    ?assertMatch(
        [
            #{
                <<"topic">> := Topic,
                <<"payload">> := Payload,
                <<"metadata">> := #{<<"rule_id">> := RuleId}
            }
        ],
        DecodedMessages
    ),
    ok.

t_publish_success_local_topic(Config) ->
    ResourceId = ?config(resource_id, Config),
    ServiceAccountJSON = ?config(service_account_json, Config),
    TelemetryTable = ?config(telemetry_table, Config),
    LocalTopic = <<"local/topic">>,
    {ok, _} = create_bridge(Config, #{<<"local_topic">> => LocalTopic}),
    assert_empty_metrics(ResourceId),
    Payload = <<"payload">>,
    Message = emqx_message:make(LocalTopic, Payload),
    emqx:publish(Message),
    DecodedMessages = assert_http_request(ServiceAccountJSON),
    ?assertMatch(
        [
            #{
                <<"topic">> := LocalTopic,
                <<"payload">> := Payload
            }
        ],
        DecodedMessages
    ),
    %% to avoid test flakiness
    wait_telemetry_event(TelemetryTable, success, ResourceId),
    wait_until_gauge_is(queuing, 0, 500),
    wait_until_gauge_is(inflight, 0, 500),
    assert_metrics(
        #{
            dropped => 0,
            failed => 0,
            inflight => 0,
            matched => 1,
            queuing => 0,
            retried => 0,
            success => 1
        },
        ResourceId
    ),
    ok.

t_create_via_http(Config) ->
    ?assertMatch({ok, _}, create_bridge_http(Config)),
    ok.

t_publish_templated(Config) ->
    ResourceId = ?config(resource_id, Config),
    ServiceAccountJSON = ?config(service_account_json, Config),
    TelemetryTable = ?config(telemetry_table, Config),
    Topic = <<"t/topic">>,
    PayloadTemplate = <<
        "{\"payload\": \"${payload}\","
        " \"pub_props\": ${pub_props}}"
    >>,
    ?assertMatch(
        {ok, _},
        create_bridge(
            Config,
            #{<<"payload_template">> => PayloadTemplate}
        )
    ),
    {ok, #{<<"id">> := RuleId}} = create_rule_and_action_http(Config),
    on_exit(fun() -> ok = emqx_rule_engine:delete_rule(RuleId) end),
    assert_empty_metrics(ResourceId),
    Payload = <<"payload">>,
    Message =
        emqx_message:set_header(
            properties,
            #{'User-Property' => #{'Correlation-Data' => <<"321">>}},
            emqx_message:make(Topic, Payload)
        ),
    emqx:publish(Message),
    DecodedMessages = assert_http_request(ServiceAccountJSON),
    ?assertMatch(
        [
            #{
                <<"payload">> := Payload,
                <<"pub_props">> := #{
                    <<"User-Property">> :=
                        #{
                            <<"Correlation-Data">> :=
                                <<"321">>
                        }
                }
            }
        ],
        DecodedMessages
    ),
    %% to avoid test flakiness
    wait_telemetry_event(TelemetryTable, success, ResourceId),
    wait_until_gauge_is(queuing, 0, 500),
    wait_until_gauge_is(inflight, 0, 500),
    assert_metrics(
        #{
            dropped => 0,
            failed => 0,
            inflight => 0,
            matched => 1,
            queuing => 0,
            retried => 0,
            success => 1
        },
        ResourceId
    ),
    ok.

t_publish_success_batch(Config) ->
    case proplists:get_bool(skip_due_to_no_batching, Config) of
        true ->
            ct:pal("this test case is skipped due to non-applicable config"),
            ok;
        false ->
            test_publish_success_batch(Config)
    end.

test_publish_success_batch(Config) ->
    ResourceId = ?config(resource_id, Config),
    ServiceAccountJSON = ?config(service_account_json, Config),
    TelemetryTable = ?config(telemetry_table, Config),
    Topic = <<"t/topic">>,
    BatchSize = 5,
    %% to give it time to form a batch
    BatchTime = <<"2s">>,
    ?assertMatch(
        {ok, _},
        create_bridge(
            Config,
            #{
                <<"resource_opts">> =>
                    #{
                        <<"batch_size">> => BatchSize,
                        <<"batch_time">> => BatchTime
                    }
            }
        )
    ),
    {ok, #{<<"id">> := RuleId}} = create_rule_and_action_http(Config),
    on_exit(fun() -> ok = emqx_rule_engine:delete_rule(RuleId) end),
    assert_empty_metrics(ResourceId),
    NumMessages = BatchSize * 2,
    Messages = [emqx_message:make(Topic, integer_to_binary(N)) || N <- lists:seq(1, NumMessages)],
    %% publish in parallel to avoid each client blocking and then
    %% making 1-sized batches.  also important to note that the pool
    %% size for the resource (replayq buffering) must be set to 1 to
    %% avoid further segmentation of batches.
    emqx_utils:pmap(fun emqx:publish/1, Messages),
    DecodedMessages0 = assert_http_request(ServiceAccountJSON),
    ?assertEqual(BatchSize, length(DecodedMessages0)),
    DecodedMessages1 = assert_http_request(ServiceAccountJSON),
    ?assertEqual(BatchSize, length(DecodedMessages1)),
    PublishedPayloads =
        sets:from_list(
            [P || #{<<"payload">> := P} <- DecodedMessages0 ++ DecodedMessages1],
            [{version, 2}]
        ),
    ExpectedPayloads =
        sets:from_list(
            lists:map(fun integer_to_binary/1, lists:seq(1, NumMessages)),
            [{version, 2}]
        ),
    ?assertEqual(ExpectedPayloads, PublishedPayloads),
    wait_telemetry_event(
        TelemetryTable,
        success,
        ResourceId,
        #{timeout => 15_000, n_events => NumMessages}
    ),
    wait_until_gauge_is(queuing, 0, _Timeout = 400),
    wait_until_gauge_is(inflight, 0, _Timeout = 400),
    assert_metrics(
        #{
            dropped => 0,
            failed => 0,
            inflight => 0,
            matched => NumMessages,
            queuing => 0,
            retried => 0,
            success => NumMessages
        },
        ResourceId
    ),
    ok.

t_not_a_json(Config) ->
    ?assertMatch(
        {error, #{
            kind := validation_error,
            reason := #{exception := {error, {badmap, "not a json"}}},
            %% should be censored as it contains secrets
            value := <<"******">>
        }},
        create_bridge(
            Config,
            #{
                <<"service_account_json">> => <<"not a json">>
            }
        )
    ),
    ok.

t_not_of_service_account_type(Config) ->
    ?assertMatch(
        {error, #{
            kind := validation_error,
            reason := {wrong_type, <<"not a service account">>},
            %% should be censored as it contains secrets
            value := <<"******">>
        }},
        create_bridge(
            Config,
            #{
                <<"service_account_json">> => #{<<"type">> => <<"not a service account">>}
            }
        )
    ),
    ok.

t_json_missing_fields(Config) ->
    GCPPubSubConfig0 = ?config(gcp_pubsub_config, Config),
    ?assertMatch(
        {error, #{
            kind := validation_error,
            reason :=
                {missing_keys, [
                    <<"client_email">>,
                    <<"private_key">>,
                    <<"private_key_id">>,
                    <<"project_id">>,
                    <<"type">>
                ]},
            %% should be censored as it contains secrets
            value := <<"******">>
        }},
        create_bridge([
            {gcp_pubsub_config, GCPPubSubConfig0#{<<"service_account_json">> := #{}}}
            | Config
        ])
    ),
    ok.

t_invalid_private_key(Config) ->
    InvalidPrivateKeyPEM = <<"xxxxxx">>,
    ?check_trace(
        begin
            {Res, {ok, _Event}} =
                ?wait_async_action(
                    create_bridge(
                        Config,
                        #{
                            <<"service_account_json">> =>
                                #{<<"private_key">> => InvalidPrivateKeyPEM}
                        }
                    ),
                    #{?snk_kind := gcp_pubsub_connector_startup_error},
                    20_000
                ),
            Res
        end,
        fun(Res, Trace) ->
            ?assertMatch({ok, _}, Res),
            ?assertMatch(
                [#{error := empty_key}],
                ?of_kind(gcp_pubsub_connector_startup_error, Trace)
            ),
            ok
        end
    ),
    ok.

t_truncated_private_key(Config) ->
    InvalidPrivateKeyPEM = <<"-----BEGIN PRIVATE KEY-----\nMIIEvQI...">>,
    ?check_trace(
        begin
            {Res, {ok, _Event}} =
                ?wait_async_action(
                    create_bridge(
                        Config,
                        #{
                            <<"service_account_json">> =>
                                #{<<"private_key">> => InvalidPrivateKeyPEM}
                        }
                    ),
                    #{?snk_kind := gcp_pubsub_connector_startup_error},
                    20_000
                ),
            Res
        end,
        fun(Res, Trace) ->
            ?assertMatch({ok, _}, Res),
            ?assertMatch(
                [#{error := {error, function_clause}}],
                ?of_kind(gcp_pubsub_connector_startup_error, Trace)
            ),
            ok
        end
    ),
    ok.

t_jose_error_tuple(Config) ->
    ?check_trace(
        begin
            {Res, {ok, _Event}} =
                ?wait_async_action(
                    emqx_common_test_helpers:with_mock(
                        jose_jwk,
                        from_pem,
                        fun(_PrivateKeyPEM) -> {error, some_error} end,
                        fun() -> create_bridge(Config) end
                    ),
                    #{?snk_kind := gcp_pubsub_connector_startup_error},
                    20_000
                ),
            Res
        end,
        fun(Res, Trace) ->
            ?assertMatch({ok, _}, Res),
            ?assertMatch(
                [#{error := {invalid_private_key, some_error}}],
                ?of_kind(gcp_pubsub_connector_startup_error, Trace)
            ),
            ok
        end
    ),
    ok.

t_jose_other_error(Config) ->
    ?check_trace(
        begin
            {Res, {ok, _Event}} =
                ?wait_async_action(
                    emqx_common_test_helpers:with_mock(
                        jose_jwk,
                        from_pem,
                        fun(_PrivateKeyPEM) -> {unknown, error} end,
                        fun() -> create_bridge(Config) end
                    ),
                    #{?snk_kind := gcp_pubsub_connector_startup_error},
                    20_000
                ),
            Res
        end,
        fun(Res, Trace) ->
            ?assertMatch({ok, _}, Res),
            ?assertMatch(
                [#{error := {invalid_private_key, {unknown, error}}}],
                ?of_kind(gcp_pubsub_connector_startup_error, Trace)
            ),
            ok
        end
    ),
    ok.

t_publish_econnrefused(Config) ->
    ResourceId = ?config(resource_id, Config),
    %% set pipelining to 1 so that one of the 2 requests is `pending'
    %% in ehttpc.
    {ok, _} = create_bridge(
        Config,
        #{
            <<"pipelining">> => 1,
            <<"resource_opts">> => #{<<"resume_interval">> => <<"15s">>}
        }
    ),
    {ok, #{<<"id">> := RuleId}} = create_rule_and_action_http(Config),
    on_exit(fun() -> ok = emqx_rule_engine:delete_rule(RuleId) end),
    assert_empty_metrics(ResourceId),
    ok = emqx_connector_web_hook_server:stop(),
    do_econnrefused_or_timeout_test(Config, econnrefused).

t_publish_timeout(Config) ->
    ResourceId = ?config(resource_id, Config),
    %% set pipelining to 1 so that one of the 2 requests is `pending'
    %% in ehttpc. also, we set the batch size to 1 to also ensure the
    %% requests are done separately.
    {ok, _} = create_bridge(Config, #{
        <<"pipelining">> => 1,
        <<"resource_opts">> => #{
            <<"batch_size">> => 1,
            <<"resume_interval">> => <<"15s">>
        }
    }),
    {ok, #{<<"id">> := RuleId}} = create_rule_and_action_http(Config),
    on_exit(fun() -> ok = emqx_rule_engine:delete_rule(RuleId) end),
    assert_empty_metrics(ResourceId),
    TestPid = self(),
    TimeoutHandler =
        fun(Req0, State) ->
            {ok, Body, Req} = cowboy_req:read_body(Req0),
            TestPid ! {http, cowboy_req:headers(Req), Body},
            %% NOTE: cannot just hang forever; ehttpc will never
            %% reply `sent' requests, so the callback will never
            %% be called...  We just delay responding so that a
            %% late response is delivered.
            timer:sleep(timer:seconds(3)),
            Rep = cowboy_req:reply(
                200,
                #{<<"content-type">> => <<"application/json">>},
                emqx_utils_json:encode(#{messageIds => [<<"6058891368195201">>]}),
                Req
            ),
            {ok, Rep, State}
        end,
    ok = emqx_connector_web_hook_server:set_handler(TimeoutHandler),
    do_econnrefused_or_timeout_test(Config, timeout).

do_econnrefused_or_timeout_test(Config, Error) ->
    ResourceId = ?config(resource_id, Config),
    TelemetryTable = ?config(telemetry_table, Config),
    Topic = <<"t/topic">>,
    Payload = <<"payload">>,
    Message = emqx_message:make(Topic, Payload),
    ?check_trace(
        begin
            case Error of
                econnrefused ->
                    %% at the time of writing, async requests
                    %% are never considered expired by ehttpc
                    %% (even if they arrive late, or never
                    %% arrive at all).
                    %% so, we set pipelining to 1 and shoot 2
                    %% requests, so that the second one may expire.
                    {_, {ok, _}} =
                        ?wait_async_action(
                            begin
                                emqx:publish(Message),
                                emqx:publish(Message)
                            end,
                            #{
                                ?snk_kind := gcp_pubsub_request_failed,
                                query_mode := async,
                                recoverable_error := true
                            },
                            15_000
                        );
                timeout ->
                    %% at the time of writing, async requests
                    %% are never considered expired by ehttpc
                    %% (even if they arrive late, or never
                    %% arrive at all).
                    %% with the timeout delay, this'll succeed.
                    emqx:publish(Message),
                    emqx:publish(Message),
                    {ok, _} = snabbkaffe:block_until(
                        ?match_n_events(2, #{
                            ?snk_kind := gcp_pubsub_response,
                            query_mode := async
                        }),
                        _Timeout1 = 15_000
                    )
            end
        end,
        fun(Trace) ->
            case Error of
                econnrefused ->
                    case ?of_kind(gcp_pubsub_request_failed, Trace) of
                        [#{reason := Error, connector := ResourceId} | _] ->
                            ok;
                        [#{reason := {closed, _Msg}, connector := ResourceId} | _] ->
                            %% _Msg = "The connection was lost."
                            ok;
                        Trace0 ->
                            error({unexpected_trace, Trace0})
                    end;
                timeout ->
                    ?assertMatch(
                        [_, _ | _],
                        ?of_kind(gcp_pubsub_response, Trace)
                    )
            end,
            ok
        end
    ),

    case Error of
        %% apparently, async with disabled queue doesn't mark the
        %% message as dropped; and since it never considers the
        %% response expired, this succeeds.
        econnrefused ->
            %% even waiting, hard to avoid flakiness... simpler to just sleep
            %% a bit until stabilization.
            ct:sleep(200),
            CurrentMetrics = current_metrics(ResourceId),
            RecordedEvents = ets:tab2list(TelemetryTable),
            ct:pal("telemetry events: ~p", [RecordedEvents]),
            ?assertMatch(
                #{
                    dropped := Dropped,
                    failed := Failed,
                    inflight := Inflight,
                    matched := Matched,
                    queuing := Queueing,
                    retried := 0,
                    success := 0
                } when Matched >= 1 andalso Inflight + Queueing + Dropped + Failed =< 2,
                CurrentMetrics
            );
        timeout ->
            wait_until_gauge_is(inflight, 0, _Timeout = 1_000),
            wait_until_gauge_is(queuing, 0, _Timeout = 1_000),
            assert_metrics(
                #{
                    dropped => 0,
                    failed => 0,
                    inflight => 0,
                    matched => 2,
                    queuing => 0,
                    retried => 0,
                    success => 0,
                    late_reply => 2
                },
                ResourceId
            )
    end,

    ok.

%% for complete coverage; pubsub actually returns a body with message
%% ids
t_success_no_body(Config) ->
    TestPid = self(),
    SuccessNoBodyHandler =
        fun(Req0, State) ->
            {ok, Body, Req} = cowboy_req:read_body(Req0),
            TestPid ! {http, cowboy_req:headers(Req), Body},
            Rep = cowboy_req:reply(
                200,
                #{<<"content-type">> => <<"application/json">>},
                <<>>,
                Req
            ),
            {ok, Rep, State}
        end,
    ok = emqx_connector_web_hook_server:set_handler(SuccessNoBodyHandler),
    Topic = <<"t/topic">>,
    {ok, _} = create_bridge(Config),
    {ok, #{<<"id">> := RuleId}} = create_rule_and_action_http(Config),
    on_exit(fun() -> ok = emqx_rule_engine:delete_rule(RuleId) end),
    Payload = <<"payload">>,
    Message = emqx_message:make(Topic, Payload),
    ?check_trace(
        {_, {ok, _}} =
            ?wait_async_action(
                emqx:publish(Message),
                #{?snk_kind := gcp_pubsub_response},
                5_000
            ),
        fun(Trace) ->
            ?assertMatch(
                [#{response := {ok, 200, _Headers}}],
                ?of_kind(gcp_pubsub_response, Trace)
            ),
            ok
        end
    ),
    ok.

t_failure_with_body(Config) ->
    TestPid = self(),
    FailureWithBodyHandler =
        fun(Req0, State) ->
            {ok, Body, Req} = cowboy_req:read_body(Req0),
            TestPid ! {http, cowboy_req:headers(Req), Body},
            Rep = cowboy_req:reply(
                400,
                #{<<"content-type">> => <<"application/json">>},
                emqx_utils_json:encode(#{}),
                Req
            ),
            {ok, Rep, State}
        end,
    ok = emqx_connector_web_hook_server:set_handler(FailureWithBodyHandler),
    Topic = <<"t/topic">>,
    {ok, _} = create_bridge(Config),
    {ok, #{<<"id">> := RuleId}} = create_rule_and_action_http(Config),
    on_exit(fun() -> ok = emqx_rule_engine:delete_rule(RuleId) end),
    Payload = <<"payload">>,
    Message = emqx_message:make(Topic, Payload),
    ?check_trace(
        {_, {ok, _}} =
            ?wait_async_action(
                emqx:publish(Message),
                #{?snk_kind := gcp_pubsub_response},
                5_000
            ),
        fun(Trace) ->
            ?assertMatch(
                [#{response := {ok, 400, _Headers, _Body}}],
                ?of_kind(gcp_pubsub_response, Trace)
            ),
            ok
        end
    ),
    ok.

t_failure_no_body(Config) ->
    TestPid = self(),
    FailureNoBodyHandler =
        fun(Req0, State) ->
            {ok, Body, Req} = cowboy_req:read_body(Req0),
            TestPid ! {http, cowboy_req:headers(Req), Body},
            Rep = cowboy_req:reply(
                400,
                #{<<"content-type">> => <<"application/json">>},
                <<>>,
                Req
            ),
            {ok, Rep, State}
        end,
    ok = emqx_connector_web_hook_server:set_handler(FailureNoBodyHandler),
    Topic = <<"t/topic">>,
    {ok, _} = create_bridge(Config),
    {ok, #{<<"id">> := RuleId}} = create_rule_and_action_http(Config),
    on_exit(fun() -> ok = emqx_rule_engine:delete_rule(RuleId) end),
    Payload = <<"payload">>,
    Message = emqx_message:make(Topic, Payload),
    ?check_trace(
        {_, {ok, _}} =
            ?wait_async_action(
                emqx:publish(Message),
                #{?snk_kind := gcp_pubsub_response},
                5_000
            ),
        fun(Trace) ->
            ?assertMatch(
                [#{response := {ok, 400, _Headers}}],
                ?of_kind(gcp_pubsub_response, Trace)
            ),
            ok
        end
    ),
    ok.

t_unrecoverable_error(Config) ->
    ResourceId = ?config(resource_id, Config),
    TestPid = self(),
    FailureNoBodyHandler =
        fun(Req0, State) ->
            {ok, Body, Req} = cowboy_req:read_body(Req0),
            TestPid ! {http, cowboy_req:headers(Req), Body},
            %% kill the gun process while it's waiting for the
            %% response so we provoke an `{error, _}' response from
            %% ehttpc.
            lists:foreach(
                fun(Pid) -> exit(Pid, kill) end,
                [Pid || {_, Pid, _, _} <- supervisor:which_children(gun_sup)]
            ),
            Rep = cowboy_req:reply(
                200,
                #{<<"content-type">> => <<"application/json">>},
                <<>>,
                Req
            ),
            {ok, Rep, State}
        end,
    ok = emqx_connector_web_hook_server:set_handler(FailureNoBodyHandler),
    Topic = <<"t/topic">>,
    {ok, _} = create_bridge(Config),
    assert_empty_metrics(ResourceId),
    {ok, #{<<"id">> := RuleId}} = create_rule_and_action_http(Config),
    on_exit(fun() -> ok = emqx_rule_engine:delete_rule(RuleId) end),
    Payload = <<"payload">>,
    Message = emqx_message:make(Topic, Payload),
    ?check_trace(
        {_, {ok, _}} =
            ?wait_async_action(
                emqx:publish(Message),
                #{?snk_kind := gcp_pubsub_response},
                5_000
            ),
        fun(Trace) ->
            ?assertMatch(
                [#{response := {error, killed}}],
                ?of_kind(gcp_pubsub_response, Trace)
            ),
            ok
        end
    ),

    wait_until_gauge_is(queuing, 0, _Timeout = 400),
    %% TODO: once temporary clause in
    %% `emqx_resource_buffer_worker:is_unrecoverable_error'
    %% that marks all unknown errors as unrecoverable is
    %% removed, this inflight should be 1, because we retry if
    %% the worker is killed.
    wait_until_gauge_is(inflight, 0, _Timeout = 400),
    assert_metrics(
        #{
            dropped => 0,
            %% FIXME: see comment above; failed should be 0
            %% and inflight should be 1.
            failed => 1,
            inflight => 0,
            matched => 1,
            queuing => 0,
            retried => 0,
            success => 0
        },
        ResourceId
    ),
    ok.

t_stop(Config) ->
    Name = ?config(gcp_pubsub_name, Config),
    {ok, _} = create_bridge(Config),
    ?check_trace(
        ?wait_async_action(
            emqx_bridge_resource:stop(?BRIDGE_TYPE, Name),
            #{?snk_kind := gcp_pubsub_stop},
            5_000
        ),
        fun(Res, Trace) ->
            ?assertMatch({ok, {ok, _}}, Res),
            ?assertMatch([_], ?of_kind(gcp_pubsub_stop, Trace)),
            ?assertMatch([_ | _], ?of_kind(connector_jwt_deleted, Trace)),
            ok
        end
    ),
    ok.

t_get_status_ok(Config) ->
    ResourceId = ?config(resource_id, Config),
    {ok, _} = create_bridge(Config),
    ?assertEqual({ok, connected}, emqx_resource_manager:health_check(ResourceId)),
    ok.

t_get_status_no_worker(Config) ->
    ResourceId = ?config(resource_id, Config),
    {ok, _} = create_bridge(Config),
    emqx_common_test_helpers:with_mock(
        ehttpc,
        workers,
        fun(_Poolname) -> [] end,
        fun() ->
            ?assertEqual({ok, disconnected}, emqx_resource_manager:health_check(ResourceId)),
            ok
        end
    ),
    ok.

t_get_status_down(Config) ->
    ResourceId = ?config(resource_id, Config),
    {ok, _} = create_bridge(Config),
    emqx_common_test_helpers:with_mock(
        ehttpc,
        health_check,
        fun(_Worker, _Timeout) ->
            {error, connect_timeout}
        end,
        fun() ->
            ?assertEqual({ok, disconnected}, emqx_resource_manager:health_check(ResourceId)),
            ok
        end
    ),
    ok.

t_get_status_timeout_calling_workers(Config) ->
    ResourceId = ?config(resource_id, Config),
    {ok, _} = create_bridge(Config),
    emqx_common_test_helpers:with_mock(
        ehttpc,
        health_check,
        fun(_Worker, _Timeout) ->
            receive
            after infinity -> error(impossible)
            end
        end,
        fun() ->
            ?assertEqual({ok, disconnected}, emqx_resource_manager:health_check(ResourceId)),
            ok
        end
    ),
    ok.

t_on_start_ehttpc_pool_already_started(Config) ->
    ?check_trace(
        begin
            ?force_ordering(
                #{?snk_kind := pool_started},
                #{?snk_kind := gcp_pubsub_starting_ehttpc_pool}
            ),
            {ok, SubRef} =
                snabbkaffe:subscribe(
                    fun
                        (#{?snk_kind := gcp_pubsub_on_start_before_starting_pool}) -> true;
                        (_) -> false
                    end,
                    5_000
                ),
            spawn_link(fun() -> {ok, _} = create_bridge(Config) end),
            {ok, [#{pool_name := PoolName, pool_opts := PoolOpts}]} = snabbkaffe:receive_events(
                SubRef
            ),
            ?assertMatch({ok, _}, ehttpc_sup:start_pool(PoolName, PoolOpts)),
            ?tp(pool_started, #{}),
            ?block_until(#{?snk_kind := gcp_pubsub_ehttpc_pool_already_started}, 2_000),
            PoolName
        end,
        fun(PoolName, Trace) ->
            ?assertMatch(
                [#{pool_name := PoolName}],
                ?of_kind(gcp_pubsub_ehttpc_pool_already_started, Trace)
            ),
            ok
        end
    ),
    ok.

t_on_start_ehttpc_pool_start_failure(Config) ->
    ?check_trace(
        emqx_common_test_helpers:with_mock(
            ehttpc_sup,
            start_pool,
            fun(_PoolName, _PoolOpts) -> {error, some_error} end,
            fun() ->
                {ok, _} = create_bridge(Config)
            end
        ),
        fun(Trace) ->
            ?assertMatch(
                [#{reason := some_error}],
                ?of_kind(gcp_pubsub_ehttpc_pool_start_failure, Trace)
            ),
            ok
        end
    ),
    ok.
