%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_kinesis_impl_producer_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-define(BRIDGE_TYPE, kinesis_producer).
-define(BRIDGE_TYPE_BIN, <<"kinesis_producer">>).
-define(BRIDGE_V2_TYPE_BIN, <<"kinesis">>).
-define(KINESIS_PORT, 4566).
-define(KINESIS_ACCESS_KEY, "aws_access_key_id").
-define(KINESIS_SECRET_KEY, "aws_secret_access_key").
-define(TOPIC, <<"t/topic">>).

%%------------------------------------------------------------------------------
%% CT boilerplate
%%------------------------------------------------------------------------------

all() ->
    [
        {group, with_batch},
        {group, without_batch}
    ].

groups() ->
    TCs = emqx_common_test_helpers:all(?MODULE),
    [
        {with_batch, TCs},
        {without_batch, TCs}
    ].

init_per_suite(Config) ->
    ProxyHost = os:getenv("PROXY_HOST", "toxiproxy.emqx.net"),
    ProxyPort = list_to_integer(os:getenv("PROXY_PORT", "8474")),
    ProxyName = "kinesis",
    SecretFile = filename:join(?config(priv_dir, Config), "secret"),
    ok = file:write_file(SecretFile, <<?KINESIS_SECRET_KEY>>),
    Apps = emqx_cth_suite:start(
        [
            emqx,
            emqx_conf,
            emqx_bridge_kinesis,
            emqx_connector,
            emqx_bridge,
            emqx_rule_engine,
            emqx_management,
            emqx_mgmt_api_test_util:emqx_dashboard()
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [
        {apps, Apps},
        {proxy_host, ProxyHost},
        {proxy_port, ProxyPort},
        {kinesis_port, list_to_integer(os:getenv("KINESIS_PORT", integer_to_list(?KINESIS_PORT)))},
        {kinesis_secretfile, SecretFile},
        {proxy_name, ProxyName}
        | Config
    ].

end_per_suite(Config) ->
    Apps = ?config(apps, Config),
    emqx_cth_suite:stop(Apps),
    ok.

init_per_group(with_batch, Config) ->
    [{batch_size, 100} | Config];
init_per_group(without_batch, Config) ->
    [{batch_size, 1} | Config];
init_per_group(_Group, Config) ->
    Config.

end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(TestCase, Config0) ->
    ok = snabbkaffe:start_trace(),
    ProxyHost = ?config(proxy_host, Config0),
    ProxyPort = ?config(proxy_port, Config0),
    emqx_common_test_helpers:reset_proxy(ProxyHost, ProxyPort),
    TimeTrap =
        case TestCase of
            t_wrong_server -> 60;
            _ -> 30
        end,
    ct:timetrap({seconds, TimeTrap}),
    delete_all_bridges(),
    Tid = install_telemetry_handler(TestCase),
    put(telemetry_table, Tid),
    Config = generate_config(Config0),
    create_stream(Config),
    [{telemetry_table, Tid} | Config].

end_per_testcase(_TestCase, Config) ->
    ok = snabbkaffe:stop(),
    delete_all_bridges(),
    delete_stream(Config),
    emqx_common_test_helpers:call_janitor(),
    ok.

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

generate_config(Config0) ->
    #{
        name := Name,
        config_string := ConfigString,
        kinesis_config := KinesisConfig
    } = kinesis_config(Config0),
    Endpoint = map_get(<<"endpoint">>, KinesisConfig),
    #{scheme := Scheme, hostname := Host, port := Port} =
        emqx_schema:parse_server(
            Endpoint,
            #{
                default_port => 443,
                supported_schemes => ["http", "https"]
            }
        ),
    ErlcloudConfig = erlcloud_kinesis:new("access_key", "secret", Host, Port, Scheme ++ "://"),
    ResourceId = connector_resource_id(?BRIDGE_V2_TYPE_BIN, Name),
    BridgeId = emqx_bridge_resource:bridge_id(?BRIDGE_TYPE_BIN, Name),
    [
        {kinesis_name, Name},
        {connection_scheme, Scheme},
        {kinesis_config, KinesisConfig},
        {kinesis_config_string, ConfigString},
        {resource_id, ResourceId},
        {bridge_id, BridgeId},
        {erlcloud_config, ErlcloudConfig}
        | Config0
    ].

connector_resource_id(BridgeType, Name) ->
    <<"connector:", BridgeType/binary, ":", Name/binary>>.

kinesis_config(Config) ->
    QueryMode = proplists:get_value(query_mode, Config, async),
    Scheme = proplists:get_value(connection_scheme, Config, "http"),
    ProxyHost = proplists:get_value(proxy_host, Config),
    KinesisPort = proplists:get_value(kinesis_port, Config),
    SecretFile = proplists:get_value(kinesis_secretfile, Config),
    BatchSize = proplists:get_value(batch_size, Config, 100),
    BatchTime = proplists:get_value(batch_time, Config, <<"500ms">>),
    PayloadTemplate = proplists:get_value(payload_template, Config, "${payload}"),
    StreamName = proplists:get_value(stream_name, Config, <<"mystream">>),
    PartitionKey = proplists:get_value(partition_key, Config, <<"key">>),
    MaxRetries = proplists:get_value(max_retries, Config, 3),
    GUID = emqx_guid:to_hexstr(emqx_guid:gen()),
    Name = <<(atom_to_binary(?MODULE))/binary, (GUID)/binary>>,
    ConfigString =
        io_lib:format(
            "bridges.kinesis_producer.~s {"
            "\n   enable = true"
            "\n   aws_access_key_id = ~p"
            "\n   aws_secret_access_key = ~p"
            "\n   endpoint = \"~s://~s:~b\""
            "\n   stream_name = \"~s\""
            "\n   partition_key = \"~s\""
            "\n   payload_template = \"~s\""
            "\n   max_retries = ~b"
            "\n   pool_size = 1"
            "\n   resource_opts = {"
            "\n     health_check_interval = \"3s\""
            "\n     request_ttl = 30s"
            "\n     resume_interval = 1s"
            "\n     metrics_flush_interval = \"700ms\""
            "\n     worker_pool_size = 1"
            "\n     query_mode = ~s"
            "\n     batch_size = ~b"
            "\n     batch_time = \"~s\""
            "\n   }"
            "\n }",
            [
                Name,
                ?KINESIS_ACCESS_KEY,
                %% NOTE: using file-based secrets with HOCON configs.
                "file://" ++ SecretFile,
                Scheme,
                ProxyHost,
                KinesisPort,
                StreamName,
                PartitionKey,
                PayloadTemplate,
                MaxRetries,
                QueryMode,
                BatchSize,
                BatchTime
            ]
        ),
    #{
        name => Name,
        config_string => ConfigString,
        kinesis_config => parse_and_check(ConfigString, Name)
    }.

parse_and_check(ConfigString, Name) ->
    {ok, RawConf} = hocon:binary(ConfigString, #{format => map}),
    TypeBin = <<"kinesis_producer">>,
    hocon_tconf:check_plain(emqx_bridge_schema, RawConf, #{required => false, atom_key => false}),
    #{<<"bridges">> := #{TypeBin := #{Name := Config}}} = RawConf,
    Config.

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
    Name = ?config(kinesis_name, Config),
    ct:pal("deleting bridge ~p", [{Type, Name}]),
    emqx_bridge:remove(Type, Name).

create_bridge_http(Config, KinesisConfigOverrides) ->
    TypeBin = ?BRIDGE_TYPE_BIN,
    Name = ?config(kinesis_name, Config),
    KinesisConfig0 = ?config(kinesis_config, Config),
    KinesisConfig = emqx_utils_maps:deep_merge(KinesisConfig0, KinesisConfigOverrides),
    Params = KinesisConfig#{<<"type">> => TypeBin, <<"name">> => Name},
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

create_bridge(Config) ->
    create_bridge(Config, #{}, []).

create_bridge(Config, KinesisConfigOverrides) ->
    create_bridge(Config, KinesisConfigOverrides, []).

create_bridge(Config, KinesisConfigOverrides, Removes) ->
    TypeBin = ?BRIDGE_TYPE_BIN,
    Name = ?config(kinesis_name, Config),
    KinesisConfig0 = ?config(kinesis_config, Config),
    KinesisConfig1 = emqx_utils_maps:deep_merge(KinesisConfig0, KinesisConfigOverrides),
    KinesisConfig = emqx_utils_maps:deep_remove(Removes, KinesisConfig1),
    ct:pal("creating bridge: ~p", [KinesisConfig]),
    Res = emqx_bridge:create(TypeBin, Name, KinesisConfig),
    ct:pal("bridge creation result: ~p", [Res]),
    Res.

create_rule_and_action_http(Config) ->
    BridgeId = ?config(bridge_id, Config),
    Params = #{
        enable => true,
        sql => <<"SELECT * FROM \"", ?TOPIC/binary, "\"">>,
        actions => [BridgeId]
    },
    Path = emqx_mgmt_api_test_util:api_path(["rules"]),
    AuthHeader = emqx_mgmt_api_test_util:auth_header_(),
    case emqx_mgmt_api_test_util:request_api(post, Path, "", AuthHeader, Params) of
        {ok, Res} -> {ok, emqx_utils_json:decode(Res, [return_maps])};
        Error -> Error
    end.

create_stream(Config) ->
    KinesisConfig = ?config(kinesis_config, Config),
    ErlcloudConfig = ?config(erlcloud_config, Config),
    StreamName = map_get(<<"stream_name">>, KinesisConfig),
    {ok, _} = application:ensure_all_started(erlcloud),
    delete_stream(StreamName, ErlcloudConfig),
    {ok, _} = erlcloud_kinesis:create_stream(StreamName, 1, ErlcloudConfig),
    ?retry(
        _Sleep = 100,
        _Attempts = 10,
        begin
            {ok, [{<<"StreamDescription">>, StreamInfo}]} =
                erlcloud_kinesis:describe_stream(StreamName, ErlcloudConfig),
            ?assertEqual(
                <<"ACTIVE">>,
                proplists:get_value(<<"StreamStatus">>, StreamInfo)
            )
        end
    ),
    ok.

delete_stream(Config) ->
    KinesisConfig = ?config(kinesis_config, Config),
    ErlcloudConfig = ?config(erlcloud_config, Config),
    StreamName = map_get(<<"stream_name">>, KinesisConfig),
    {ok, _} = application:ensure_all_started(erlcloud),
    delete_stream(StreamName, ErlcloudConfig),
    ok.

delete_stream(StreamName, ErlcloudConfig) ->
    case erlcloud_kinesis:delete_stream(StreamName, ErlcloudConfig) of
        {ok, _} ->
            ?retry(
                _Sleep = 100,
                _Attempts = 10,
                ?assertMatch(
                    {error, {<<"ResourceNotFoundException">>, _}},
                    erlcloud_kinesis:describe_stream(StreamName, ErlcloudConfig)
                )
            );
        _ ->
            ok
    end,
    ok.

wait_record(Config, ShardIt, Timeout, Attempts) ->
    [Record] = wait_records(Config, ShardIt, 1, Timeout, Attempts),
    Record.

wait_records(Config, ShardIt, Count, Timeout, Attempts) ->
    ErlcloudConfig = ?config(erlcloud_config, Config),
    ?retry(
        Timeout,
        Attempts,
        begin
            {ok, Ret} = erlcloud_kinesis:get_records(ShardIt, ErlcloudConfig),
            Records = proplists:get_value(<<"Records">>, Ret),
            Count = length(Records),
            Records
        end
    ).

get_shard_iterator(Config) ->
    get_shard_iterator(Config, 1).

get_shard_iterator(Config, Index) ->
    KinesisConfig = ?config(kinesis_config, Config),
    ErlcloudConfig = ?config(erlcloud_config, Config),
    StreamName = map_get(<<"stream_name">>, KinesisConfig),
    {ok, [{<<"Shards">>, Shards}]} = erlcloud_kinesis:list_shards(StreamName, ErlcloudConfig),
    Shard = lists:nth(Index, lists:sort(Shards)),
    ShardId = proplists:get_value(<<"ShardId">>, Shard),
    {ok, [{<<"ShardIterator">>, ShardIt}]} =
        erlcloud_kinesis:get_shard_iterator(StreamName, ShardId, <<"LATEST">>, ErlcloudConfig),
    ShardIt.

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
    emqx_common_test_helpers:on_exit(fun() ->
        telemetry:detach(HandlerId),
        ets:delete(Tid)
    end),
    Tid.

current_metrics(ResourceId) ->
    Mapping = metrics_mapping(),
    maps:from_list([
        {Metric, F(ResourceId)}
     || {Metric, F} <- maps:to_list(Mapping)
    ]).

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

to_str(List) when is_list(List) ->
    List;
to_str(Bin) when is_binary(Bin) ->
    erlang:binary_to_list(Bin);
to_str(Int) when is_integer(Int) ->
    erlang:integer_to_list(Int).

to_bin(Str) when is_list(Str) ->
    erlang:list_to_binary(Str).

%%------------------------------------------------------------------------------
%% Testcases
%%------------------------------------------------------------------------------

t_create_via_http(Config) ->
    Overrides = #{
        %% NOTE: using literal secret with HTTP API requests.
        <<"aws_secret_access_key">> => <<?KINESIS_SECRET_KEY>>
    },
    ?assertMatch({ok, _}, create_bridge_http(Config, Overrides)),
    ok.

t_start_failed_then_fix(Config) ->
    ProxyPort = ?config(proxy_port, Config),
    ProxyHost = ?config(proxy_host, Config),
    ProxyName = ?config(proxy_name, Config),
    Name = ?config(kinesis_name, Config),
    emqx_common_test_helpers:with_failure(down, ProxyName, ProxyHost, ProxyPort, fun() ->
        ct:sleep(1000),
        ?wait_async_action(
            create_bridge(Config),
            #{?snk_kind := emqx_bridge_kinesis_impl_producer_start_failed},
            20_000
        )
    end),
    ?retry(
        _Sleep1 = 1_000,
        _Attempts1 = 30,
        ?assertMatch(#{status := connected}, emqx_bridge_v2:health_check(?BRIDGE_V2_TYPE_BIN, Name))
    ),
    ok.

t_stop(Config) ->
    Name = ?config(kinesis_name, Config),
    {ok, _} = create_bridge(Config),
    ?check_trace(
        ?wait_async_action(
            emqx_bridge_resource:stop(?BRIDGE_TYPE, Name),
            #{?snk_kind := kinesis_stop},
            5_000
        ),
        fun(Trace) ->
            ?assertMatch([_], ?of_kind(kinesis_stop, Trace)),
            ok
        end
    ),
    ok.

t_get_status_ok(Config) ->
    Name = ?config(kinesis_name, Config),
    {ok, _} = create_bridge(Config),
    ?assertMatch(#{status := connected}, emqx_bridge_v2:health_check(?BRIDGE_V2_TYPE_BIN, Name)),
    ok.

t_create_unhealthy(Config) ->
    delete_stream(Config),
    Name = ?config(kinesis_name, Config),
    {ok, _} = create_bridge(Config),
    ?assertMatch(
        #{
            status := disconnected,
            error := {unhealthy_target, _}
        },
        emqx_bridge_v2:health_check(?BRIDGE_V2_TYPE_BIN, Name)
    ),
    ok.

t_get_status_unhealthy(Config) ->
    Name = ?config(kinesis_name, Config),
    {ok, _} = create_bridge(Config),
    ?assertMatch(
        #{
            status := connected
        },
        emqx_bridge_v2:health_check(?BRIDGE_V2_TYPE_BIN, Name)
    ),
    delete_stream(Config),
    ?retry(
        100,
        100,
        fun() ->
            ?assertMatch(
                #{
                    status := disconnected,
                    error := {unhealthy_target, _}
                },
                emqx_bridge_v2:health_check(?BRIDGE_V2_TYPE_BIN, Name)
            )
        end
    ),
    ok.

t_publish_success(Config) ->
    ResourceId = ?config(resource_id, Config),
    TelemetryTable = ?config(telemetry_table, Config),
    Name = ?config(kinesis_name, Config),
    ?assertMatch({ok, _}, create_bridge(Config)),
    {ok, #{<<"id">> := RuleId}} = create_rule_and_action_http(Config),
    emqx_common_test_helpers:on_exit(fun() -> ok = emqx_rule_engine:delete_rule(RuleId) end),
    ActionId = emqx_bridge_v2:id(?BRIDGE_V2_TYPE_BIN, Name),
    assert_empty_metrics(ActionId),
    ShardIt = get_shard_iterator(Config),
    Payload = <<"payload">>,
    Message = emqx_message:make(?TOPIC, Payload),
    emqx:publish(Message),
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
        ActionId
    ),
    Record = wait_record(Config, ShardIt, 100, 10),
    ?assertEqual(Payload, proplists:get_value(<<"Data">>, Record)),
    ok.

t_publish_success_with_template(Config) ->
    ResourceId = ?config(resource_id, Config),
    TelemetryTable = ?config(telemetry_table, Config),
    Name = ?config(kinesis_name, Config),
    Overrides =
        #{
            <<"payload_template">> => <<"${payload.data}">>,
            <<"partition_key">> => <<"${payload.key}">>
        },
    ?assertMatch({ok, _}, create_bridge(Config, Overrides)),
    {ok, #{<<"id">> := RuleId}} = create_rule_and_action_http(Config),
    emqx_common_test_helpers:on_exit(fun() -> ok = emqx_rule_engine:delete_rule(RuleId) end),
    ActionId = emqx_bridge_v2:id(?BRIDGE_V2_TYPE_BIN, Name),
    assert_empty_metrics(ActionId),
    ShardIt = get_shard_iterator(Config),
    Payload = <<"{\"key\":\"my_key\", \"data\":\"my_data\"}">>,
    Message = emqx_message:make(?TOPIC, Payload),
    emqx:publish(Message),
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
        ActionId
    ),
    Record = wait_record(Config, ShardIt, 100, 10),
    ?assertEqual(<<"my_data">>, proplists:get_value(<<"Data">>, Record)),
    ok.

t_publish_multiple_msgs_success(Config) ->
    ResourceId = ?config(resource_id, Config),
    TelemetryTable = ?config(telemetry_table, Config),
    Name = ?config(kinesis_name, Config),
    ?assertMatch({ok, _}, create_bridge(Config)),
    {ok, #{<<"id">> := RuleId}} = create_rule_and_action_http(Config),
    emqx_common_test_helpers:on_exit(fun() -> ok = emqx_rule_engine:delete_rule(RuleId) end),
    ActionId = emqx_bridge_v2:id(?BRIDGE_V2_TYPE_BIN, Name),
    assert_empty_metrics(ActionId),
    ShardIt = get_shard_iterator(Config),
    lists:foreach(
        fun(I) ->
            Payload = "payload_" ++ to_str(I),
            Message = emqx_message:make(?TOPIC, Payload),
            emqx:publish(Message)
        end,
        lists:seq(1, 10)
    ),
    Records = wait_records(Config, ShardIt, 10, 100, 10),
    ReceivedPayloads =
        lists:map(fun(Record) -> proplists:get_value(<<"Data">>, Record) end, Records),
    lists:foreach(
        fun(I) ->
            ExpectedPayload = to_bin("payload_" ++ to_str(I)),
            ?assertEqual(
                {ExpectedPayload, true},
                {ExpectedPayload, lists:member(ExpectedPayload, ReceivedPayloads)}
            )
        end,
        lists:seq(1, 10)
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
            matched => 10,
            queuing => 0,
            retried => 0,
            success => 10
        },
        ActionId
    ),
    ok.

t_publish_unhealthy(Config) ->
    ResourceId = ?config(resource_id, Config),
    TelemetryTable = ?config(telemetry_table, Config),
    Name = ?config(kinesis_name, Config),
    ?assertMatch({ok, _}, create_bridge(Config)),
    {ok, #{<<"id">> := RuleId}} = create_rule_and_action_http(Config),
    emqx_common_test_helpers:on_exit(fun() -> ok = emqx_rule_engine:delete_rule(RuleId) end),
    ActionId = emqx_bridge_v2:id(?BRIDGE_V2_TYPE_BIN, Name),
    assert_empty_metrics(ActionId),
    ShardIt = get_shard_iterator(Config),
    Payload = <<"payload">>,
    Message = emqx_message:make(?TOPIC, Payload),
    delete_stream(Config),
    emqx:publish(Message),
    ?assertError(
        {badmatch, {error, {<<"ResourceNotFoundException">>, _}}},
        wait_record(Config, ShardIt, 100, 10)
    ),
    %% to avoid test flakiness
    wait_telemetry_event(TelemetryTable, failed, ResourceId),
    wait_until_gauge_is(queuing, 0, 500),
    wait_until_gauge_is(inflight, 0, 500),
    assert_metrics(
        #{
            dropped => 0,
            failed => 1,
            inflight => 0,
            matched => 1,
            queuing => 0,
            retried => 0,
            success => 0
        },
        ActionId
    ),
    ?assertMatch(
        #{
            status := disconnected,
            error := {unhealthy_target, _}
        },
        emqx_bridge_v2:health_check(?BRIDGE_V2_TYPE_BIN, Name)
    ),
    ok.

t_publish_big_msg(Config) ->
    ResourceId = ?config(resource_id, Config),
    TelemetryTable = ?config(telemetry_table, Config),
    Name = ?config(kinesis_name, Config),
    ?assertMatch({ok, _}, create_bridge(Config)),
    {ok, #{<<"id">> := RuleId}} = create_rule_and_action_http(Config),
    emqx_common_test_helpers:on_exit(fun() -> ok = emqx_rule_engine:delete_rule(RuleId) end),
    ActionId = emqx_bridge_v2:id(?BRIDGE_V2_TYPE_BIN, Name),
    assert_empty_metrics(ActionId),
    % Maximum size is 1MB. Using 1MB + 1 here.
    Payload = binary:copy(<<"a">>, 1 * 1024 * 1024 + 1),
    Message = emqx_message:make(?TOPIC, Payload),
    emqx:publish(Message),
    %% to avoid test flakiness
    wait_telemetry_event(TelemetryTable, failed, ResourceId),
    wait_until_gauge_is(queuing, 0, 500),
    wait_until_gauge_is(inflight, 0, 500),
    assert_metrics(
        #{
            dropped => 0,
            failed => 1,
            inflight => 0,
            matched => 1,
            queuing => 0,
            retried => 0,
            success => 0
        },
        ActionId
    ),
    ok.

t_publish_connection_down(Config0) ->
    Config = generate_config([{max_retries, 2} | Config0]),
    ProxyPort = ?config(proxy_port, Config),
    ProxyHost = ?config(proxy_host, Config),
    ProxyName = ?config(proxy_name, Config),
    ResourceId = ?config(resource_id, Config),
    TelemetryTable = ?config(telemetry_table, Config),
    Name = ?config(kinesis_name, Config),
    ?assertMatch({ok, _}, create_bridge(Config)),
    {ok, #{<<"id">> := RuleId}} = create_rule_and_action_http(Config),
    ?retry(
        _Sleep1 = 1_000,
        _Attempts1 = 30,
        ?assertMatch(
            #{status := connected},
            emqx_bridge_v2:health_check(?BRIDGE_V2_TYPE_BIN, Name)
        )
    ),
    emqx_common_test_helpers:on_exit(fun() -> ok = emqx_rule_engine:delete_rule(RuleId) end),
    ActionId = emqx_bridge_v2:id(?BRIDGE_V2_TYPE_BIN, Name),
    assert_empty_metrics(ActionId),
    ShardIt = get_shard_iterator(Config),
    Payload = <<"payload">>,
    Message = emqx_message:make(?TOPIC, Payload),
    Kind =
        case proplists:get_value(batch_size, Config) of
            1 -> emqx_bridge_kinesis_impl_producer_sync_query;
            _ -> emqx_bridge_kinesis_impl_producer_sync_batch_query
        end,
    emqx_common_test_helpers:with_failure(down, ProxyName, ProxyHost, ProxyPort, fun() ->
        ct:sleep(1000),
        ?wait_async_action(
            emqx:publish(Message),
            #{?snk_kind := Kind},
            5_000
        ),
        ct:sleep(1000)
    end),
    % Wait for reconnection.
    ?retry(
        _Sleep3 = 1_000,
        _Attempts3 = 20,
        ?assertMatch(
            #{status := connected},
            emqx_bridge_v2:health_check(?BRIDGE_V2_TYPE_BIN, Name)
        )
    ),
    Record = wait_record(Config, ShardIt, 2000, 10),
    %% to avoid test flakiness
    wait_telemetry_event(TelemetryTable, retried_success, ResourceId),
    wait_until_gauge_is(queuing, 0, 500),
    wait_until_gauge_is(inflight, 0, 500),
    assert_metrics(
        #{
            dropped => 0,
            failed => 0,
            inflight => 0,
            matched => 1,
            queuing => 0,
            retried => 1,
            success => 1,
            retried_success => 1
        },
        ActionId
    ),
    Data = proplists:get_value(<<"Data">>, Record),
    ?assertEqual(Payload, Data),
    ok.

t_wrong_server(Config) ->
    TypeBin = ?BRIDGE_TYPE_BIN,
    Name = ?config(kinesis_name, Config),
    KinesisConfig0 = ?config(kinesis_config, Config),
    ResourceId = ?config(resource_id, Config),
    Overrides =
        #{
            <<"max_retries">> => 0,
            <<"endpoint">> => <<"https://wrong_server:12345">>,
            <<"resource_opts">> => #{
                <<"health_check_interval">> => <<"60s">>
            }
        },
    % probe
    KinesisConfig = emqx_utils_maps:deep_merge(KinesisConfig0, Overrides),
    Params = KinesisConfig#{<<"type">> => TypeBin, <<"name">> => Name},
    ProbePath = emqx_mgmt_api_test_util:api_path(["bridges_probe"]),
    AuthHeader = emqx_mgmt_api_test_util:auth_header_(),
    ?assertMatch(
        {error, {_, 400, _}},
        emqx_mgmt_api_test_util:request_api(post, ProbePath, "", AuthHeader, Params)
    ),
    % create
    ?wait_async_action(
        create_bridge(Config, Overrides),
        #{?snk_kind := start_pool_failed},
        30_000
    ),
    ?assertMatch(
        {ok, _, #{error := {start_pool_failed, ResourceId, _}}},
        emqx_resource_manager:lookup_cached(ResourceId)
    ),
    ok.

t_access_denied(Config) ->
    TypeBin = ?BRIDGE_TYPE_BIN,
    Name = ?config(kinesis_name, Config),
    KinesisConfig = ?config(kinesis_config, Config),
    ResourceId = ?config(resource_id, Config),
    AccessError = {<<"AccessDeniedException">>, <<>>},
    Params = KinesisConfig#{<<"type">> => TypeBin, <<"name">> => Name},
    ProbePath = emqx_mgmt_api_test_util:api_path(["bridges_probe"]),
    AuthHeader = emqx_mgmt_api_test_util:auth_header_(),
    emqx_common_test_helpers:with_mock(
        erlcloud_kinesis,
        list_streams,
        fun() -> {error, AccessError} end,
        fun() ->
            % probe
            ?assertMatch(
                {error, {_, 400, _}},
                emqx_mgmt_api_test_util:request_api(post, ProbePath, "", AuthHeader, Params)
            ),
            % create
            ?wait_async_action(
                create_bridge(Config),
                #{?snk_kind := kinesis_init_failed},
                30_000
            ),
            ?assertMatch(
                {ok, _, #{error := {start_pool_failed, ResourceId, AccessError}}},
                emqx_resource_manager:lookup_cached(ResourceId)
            ),
            ok
        end
    ),
    ok.

t_empty_payload_template(Config) ->
    ResourceId = ?config(resource_id, Config),
    TelemetryTable = ?config(telemetry_table, Config),
    Removes = [<<"payload_template">>],
    Name = ?config(kinesis_name, Config),
    ?assertMatch({ok, _}, create_bridge(Config, #{}, Removes)),
    {ok, #{<<"id">> := RuleId}} = create_rule_and_action_http(Config),
    emqx_common_test_helpers:on_exit(fun() -> ok = emqx_rule_engine:delete_rule(RuleId) end),
    ActionId = emqx_bridge_v2:id(?BRIDGE_V2_TYPE_BIN, Name),
    assert_empty_metrics(ResourceId),
    ShardIt = get_shard_iterator(Config),
    Payload = <<"payload">>,
    Message = emqx_message:make(?TOPIC, Payload),
    emqx:publish(Message),
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
        ActionId
    ),
    Record = wait_record(Config, ShardIt, 100, 10),
    Data = proplists:get_value(<<"Data">>, Record),
    ?assertMatch(
        #{<<"payload">> := <<"payload">>, <<"topic">> := ?TOPIC},
        emqx_utils_json:decode(Data, [return_maps])
    ),
    ok.

t_validate_static_constraints(Config) ->
    % From <https://docs.aws.amazon.com/kinesis/latest/APIReference/API_PutRecords.html>:
    % "Each PutRecords request can support up to 500 records.
    %  Each record in the request can be as large as 1 MiB,
    %  up to a limit of 5 MiB for the entire request, including partition keys."
    %
    % Message size and request size shall be controlled by user, so there is no validators
    % for them - if exceeded, it will fail like on `t_publish_big_msg` test.
    ?assertThrow(
        {emqx_bridge_schema, [#{kind := validation_error, value := 501}]},
        generate_config([{batch_size, 501} | Config])
    ),
    ok.
