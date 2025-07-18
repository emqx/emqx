%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_v2_kafka_producer_SUITE).

-feature(maybe_expr, enable).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("brod/include/brod.hrl").
-include_lib("emqx_resource/include/emqx_resource.hrl").
-include_lib("emqx/include/asserts.hrl").
-include_lib("emqx_utils/include/emqx_message.hrl").
-include_lib("kafka_protocol/include/kpro.hrl").
-include_lib("emqx/include/emqx_config.hrl").

-import(emqx_common_test_helpers, [on_exit/1]).

-define(TYPE, kafka_producer).
-define(TELEMETRY_PREFIX, emqx, resource).

%%------------------------------------------------------------------------------
%% CT boilerplate
%%------------------------------------------------------------------------------

all() ->
    All0 = emqx_common_test_helpers:all(?MODULE),
    All = All0 -- matrix_cases(),
    Groups = lists:map(fun({G, _, _}) -> {group, G} end, groups()),
    Groups ++ All.

groups() ->
    emqx_common_test_helpers:matrix_to_groups(?MODULE, matrix_cases()).

matrix_cases() ->
    lists:filter(
        fun(TestCase) ->
            maybe
                true ?= erlang:function_exported(?MODULE, TestCase, 0),
                {matrix, true} ?= proplists:lookup(matrix, ?MODULE:TestCase()),
                true
            else
                _ -> false
            end
        end,
        emqx_common_test_helpers:all(?MODULE)
    ).

init_per_suite(Config) ->
    emqx_common_test_helpers:clear_screen(),
    ProxyHost = os:getenv("PROXY_HOST", "toxiproxy"),
    ProxyPort = list_to_integer(os:getenv("PROXY_PORT", "8474")),
    KafkaHost = os:getenv("KAFKA_PLAIN_HOST", "toxiproxy.emqx.net"),
    KafkaPort = list_to_integer(os:getenv("KAFKA_PLAIN_PORT", "9292")),
    ProxyName = "kafka_plain",
    ProxyName2 = "kafka_2_plain",
    DirectKafkaHost = os:getenv("KAFKA_DIRECT_PLAIN_HOST", "kafka-1.emqx.net"),
    DirectKafkaPort = list_to_integer(os:getenv("KAFKA_DIRECT_PLAIN_PORT", "9092")),
    emqx_common_test_helpers:reset_proxy(ProxyHost, ProxyPort),
    Apps = emqx_cth_suite:start(
        [
            emqx,
            emqx_conf,
            emqx_connector,
            emqx_bridge_kafka,
            emqx_bridge,
            emqx_rule_engine,
            emqx_management,
            emqx_mgmt_api_test_util:emqx_dashboard()
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    emqx_bridge_kafka_impl_producer_SUITE:wait_until_kafka_is_up(),
    [
        {apps, Apps},
        {proxy_host, ProxyHost},
        {proxy_port, ProxyPort},
        {proxy_name, ProxyName},
        {proxy_name_2, ProxyName2},
        {kafka_host, KafkaHost},
        {kafka_port, KafkaPort},
        {direct_kafka_host, DirectKafkaHost},
        {direct_kafka_port, DirectKafkaPort}
        | Config
    ].

end_per_suite(Config) ->
    Apps = ?config(apps, Config),
    ProxyHost = ?config(proxy_host, Config),
    ProxyPort = ?config(proxy_port, Config),
    emqx_common_test_helpers:reset_proxy(ProxyHost, ProxyPort),
    emqx_cth_suite:stop(Apps),
    ok.

init_per_testcase(t_ancient_v1_config_migration_with_local_topic = TestCase, Config) ->
    Cluster = setup_cluster_ancient_config(TestCase, Config, #{with_local_topic => true}),
    [{cluster, Cluster} | Config];
init_per_testcase(t_ancient_v1_config_migration_without_local_topic = TestCase, Config) ->
    Cluster = setup_cluster_ancient_config(TestCase, Config, #{with_local_topic => false}),
    [{cluster, Cluster} | Config];
init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(TestCase, Config) when
    TestCase =:= t_ancient_v1_config_migration_with_local_topic;
    TestCase =:= t_ancient_v1_config_migration_without_local_topic
->
    Cluster = ?config(cluster, Config),
    ok = emqx_cth_cluster:stop(Cluster),
    ok;
end_per_testcase(_TestCase, Config) ->
    ProxyHost = ?config(proxy_host, Config),
    ProxyPort = ?config(proxy_port, Config),
    emqx_common_test_helpers:reset_proxy(ProxyHost, ProxyPort),
    emqx_bridge_v2_testlib:delete_all_bridges_and_connectors(),
    emqx_common_test_helpers:call_janitor(60_000),
    ok.

%%-------------------------------------------------------------------------------------
%% Helper fns
%%-------------------------------------------------------------------------------------

basic_node_conf(WorkDir) ->
    #{
        <<"node">> => #{
            <<"cookie">> => erlang:get_cookie(),
            <<"data_dir">> => unicode:characters_to_binary(WorkDir)
        }
    }.

setup_cluster_ancient_config(TestCase, Config, #{with_local_topic := WithLocalTopic}) ->
    AncientIOList = emqx_bridge_kafka_tests:kafka_producer_old_hocon(WithLocalTopic),
    {ok, AncientCfg0} = hocon:binary(AncientIOList),
    WorkDir = emqx_cth_suite:work_dir(TestCase, Config),
    BasicConf = basic_node_conf(WorkDir),
    AncientCfg = emqx_utils_maps:deep_merge(BasicConf, AncientCfg0),
    Apps = [
        emqx,
        emqx_conf,
        emqx_connector,
        emqx_bridge_kafka,
        {emqx_bridge, #{schema_mod => emqx_enterprise_schema, config => AncientCfg}}
    ],
    emqx_cth_cluster:start(
        [{kafka_producer_ancient_cfg1, #{apps => Apps}}],
        #{work_dir => WorkDir}
    ).

check_send_message_with_bridge(BridgeName) ->
    #{offset := Offset, payload := Payload} = send_message(BridgeName),
    %% ######################################
    %% Check if message is sent to Kafka
    %% ######################################
    check_kafka_message_payload(Offset, Payload).

send_message(ActionName) ->
    send_message(?TYPE, ActionName).

send_message(Type, ActionName) ->
    %% ######################################
    %% Create Kafka message
    %% ######################################
    Time = erlang:unique_integer(),
    BinTime = integer_to_binary(Time),
    Payload = list_to_binary("payload" ++ integer_to_list(Time)),
    Msg = #{
        clientid => BinTime,
        payload => Payload,
        timestamp => Time
    },
    Offset = resolve_kafka_offset(),
    %% ######################################
    %% Send message
    %% ######################################
    %% todo: messages should be sent via rules in tests...
    Res = emqx_bridge_v2:send_message(?global_ns, Type, ActionName, Msg, #{}),
    #{offset => Offset, payload => Payload, result => Res}.

resolve_kafka_offset() ->
    KafkaTopic = emqx_bridge_kafka_impl_producer_SUITE:test_topic_one_partition(),
    resolve_kafka_offset(KafkaTopic).

resolve_kafka_offset(KafkaTopic) ->
    Partition = 0,
    Hosts = emqx_bridge_kafka_impl_producer_SUITE:kafka_hosts(),
    {ok, Offset0} = emqx_bridge_kafka_impl_producer_SUITE:resolve_kafka_offset(
        Hosts, KafkaTopic, Partition
    ),
    Offset0.

check_kafka_message_payload(Offset, ExpectedPayload) ->
    KafkaTopic = emqx_bridge_kafka_impl_producer_SUITE:test_topic_one_partition(),
    check_kafka_message_payload(KafkaTopic, Offset, ExpectedPayload).

check_kafka_message_payload(KafkaTopic, Offset, ExpectedPayload) ->
    Partition = 0,
    Hosts = emqx_bridge_kafka_impl_producer_SUITE:kafka_hosts(),
    {ok, {_, [KafkaMsg0]}} = brod:fetch(Hosts, KafkaTopic, Partition, Offset),
    ?assertMatch(#kafka_message{value = ExpectedPayload}, KafkaMsg0).

fetch_since(Hosts, KafkaTopic, Partition, Offset) ->
    {ok, {_, Msgs}} = brod:fetch(Hosts, KafkaTopic, Partition, Offset),
    Msgs.

ensure_kafka_topic(KafkaTopic) ->
    TopicConfigs = [
        #{
            name => KafkaTopic,
            num_partitions => 1,
            replication_factor => 1,
            assignments => [],
            configs => []
        }
    ],
    RequestConfig = #{timeout => 5_000},
    ConnConfig = #{},
    Endpoints = emqx_bridge_kafka_impl_producer_SUITE:kafka_hosts(),
    case brod:create_topics(Endpoints, TopicConfigs, RequestConfig, ConnConfig) of
        ok -> ok;
        {error, topic_already_exists} -> ok
    end.

delete_kafka_topic(KafkaTopic) ->
    Timeout = 1_000,
    ConnConfig = #{},
    Endpoints = emqx_bridge_kafka_impl_producer_SUITE:kafka_hosts(),
    case brod:delete_topics(Endpoints, [KafkaTopic], Timeout, ConnConfig) of
        ok -> ok;
        {error, unknown_topic_or_partition} -> ok
    end.

action_config(ConnectorName) ->
    action_config(ConnectorName, _Overrides = #{}).

action_config(ConnectorName, Overrides) ->
    Cfg0 = bridge_v2_config(ConnectorName),
    Cfg1 = emqx_utils_maps:rename(<<"kafka">>, <<"parameters">>, Cfg0),
    emqx_utils_maps:deep_merge(Cfg1, Overrides).

bridge_v2_config(ConnectorName) ->
    KafkaTopic = emqx_bridge_kafka_impl_producer_SUITE:test_topic_one_partition(),
    bridge_v2_config(ConnectorName, KafkaTopic).

bridge_v2_config(ConnectorName, KafkaTopic) ->
    #{
        <<"connector">> => ConnectorName,
        <<"enable">> => true,
        <<"kafka">> => #{
            <<"buffer">> => #{
                <<"memory_overload_protection">> => false,
                <<"mode">> => <<"memory">>,
                <<"per_partition_limit">> => <<"2GB">>,
                <<"segment_bytes">> => <<"100MB">>
            },
            <<"compression">> => <<"no_compression">>,
            <<"kafka_header_value_encode_mode">> => <<"none">>,
            <<"max_linger_time">> => <<"0ms">>,
            <<"max_linger_bytes">> => <<"10MB">>,
            <<"max_batch_bytes">> => <<"896KB">>,
            <<"max_inflight">> => 10,
            <<"message">> => #{
                <<"key">> => <<"${.clientid}">>,
                <<"timestamp">> => <<"${.timestamp}">>,
                <<"value">> => <<"${.payload}">>
            },
            <<"partition_count_refresh_interval">> => <<"60s">>,
            <<"partition_strategy">> => <<"random">>,
            <<"query_mode">> => <<"sync">>,
            <<"required_acks">> => <<"all_isr">>,
            <<"sync_query_timeout">> => <<"5s">>,
            <<"topic">> => list_to_binary(KafkaTopic)
        },
        <<"local_topic">> => <<"kafka_t/#">>,
        <<"resource_opts">> => #{
            <<"health_check_interval">> => <<"15s">>
        }
    }.

connector_config_toxiproxy(Config) ->
    BootstrapHosts = toxiproxy_bootstrap_hosts(Config),
    Overrides = #{<<"bootstrap_hosts">> => BootstrapHosts},
    connector_config(Overrides).

connector_config() ->
    connector_config(_Overrides = #{}).

connector_config(Overrides) ->
    Defaults =
        #{
            <<"authentication">> => <<"none">>,
            <<"bootstrap_hosts">> => iolist_to_binary(kafka_hosts_string()),
            <<"connect_timeout">> => <<"5s">>,
            <<"enable">> => true,
            <<"metadata_request_timeout">> => <<"5s">>,
            <<"min_metadata_refresh_interval">> => <<"3s">>,
            <<"socket_opts">> =>
                #{
                    <<"recbuf">> => <<"1024KB">>,
                    <<"sndbuf">> => <<"1024KB">>,
                    <<"tcp_keepalive">> => <<"none">>
                },
            <<"ssl">> =>
                #{
                    <<"ciphers">> => [],
                    <<"depth">> => 10,
                    <<"enable">> => false,
                    <<"hibernate_after">> => <<"5s">>,
                    <<"log_level">> => <<"notice">>,
                    <<"reuse_sessions">> => true,
                    <<"secure_renegotiate">> => true,
                    <<"verify">> => <<"verify_peer">>,
                    <<"versions">> => [<<"tlsv1.3">>, <<"tlsv1.2">>]
                }
        },
    emqx_utils_maps:deep_merge(Defaults, Overrides).

kafka_hosts_string() ->
    KafkaHost = os:getenv("KAFKA_PLAIN_HOST", "kafka-1.emqx.net"),
    KafkaPort = os:getenv("KAFKA_PLAIN_PORT", "9092"),
    KafkaHost ++ ":" ++ KafkaPort.

toxiproxy_bootstrap_hosts(Config) ->
    Host = ?config(kafka_host, Config),
    %% assert
    "toxiproxy" ++ _ = Host,
    Port = ?config(kafka_port, Config),
    iolist_to_binary([Host, ":", integer_to_binary(Port)]).

create_connector(Name, Config) ->
    Res = emqx_connector:create(?global_ns, ?TYPE, Name, Config),
    on_exit(fun() -> emqx_connector:remove(?global_ns, ?TYPE, Name) end),
    Res.

create_connector_api(ConnectorParams) ->
    simplify_result(emqx_bridge_v2_testlib:create_connector_api(ConnectorParams)).

create_action(Name, Config) ->
    Res = emqx_bridge_v2_testlib:create_kind_api([
        {bridge_kind, action},
        {action_type, ?TYPE},
        {action_name, Name},
        {action_config, Config}
    ]),
    on_exit(fun() -> remove(Name) end),
    Res.

remove(Name) ->
    remove(?TYPE, Name).

remove(Type, Name) ->
    {204, _} = emqx_bridge_v2_testlib:delete_kind_api(action, Type, Name, #{
        query_params => #{<<"also_delete_dep_actions">> => <<"true">>}
    }),
    ok.

bridge_api_spec_props_for_get() ->
    #{
        <<"bridge_kafka.get_producer">> :=
            #{<<"properties">> := Props}
    } =
        emqx_bridge_v2_testlib:bridges_api_spec_schemas(),
    Props.

action_api_spec_props_for_get() ->
    #{
        <<"bridge_kafka.get_bridge_v2">> :=
            #{<<"properties">> := Props}
    } =
        emqx_bridge_v2_testlib:actions_api_spec_schemas(),
    Props.

assert_status_api(Line, Type, Name, Status) ->
    ?assertMatch(
        {ok,
            {{_, 200, _}, _, #{
                <<"status">> := Status,
                <<"node_status">> := [#{<<"status">> := Status}]
            }}},
        emqx_bridge_v2_testlib:get_bridge_api(Type, Name),
        #{line => Line, name => Name, expected_status => Status}
    ).
-define(assertStatusAPI(TYPE, NAME, STATUS), assert_status_api(?LINE, TYPE, NAME, STATUS)).

get_rule_metrics(RuleId) ->
    emqx_metrics_worker:get_metrics(rule_metrics, RuleId).

reset_rule_metrics(RuleId) ->
    emqx_metrics_worker:reset_metrics(rule_metrics, RuleId).

tap_telemetry(HandlerId) ->
    TestPid = self(),
    telemetry:attach_many(
        HandlerId,
        emqx_resource_metrics:events(),
        fun(EventName, Measurements, Metadata, _Config) ->
            Data = #{
                name => EventName,
                measurements => Measurements,
                metadata => Metadata
            },
            TestPid ! {telemetry, Data},
            ok
        end,
        unused_config
    ),
    on_exit(fun() -> telemetry:detach(HandlerId) end),
    ok.

-define(tapTelemetry(), tap_telemetry(?FUNCTION_NAME)).

simplify_result(Res) ->
    emqx_bridge_v2_testlib:simplify_result(Res).

with_brokers_down(Config, Fun) ->
    ProxyName1 = ?config(proxy_name, Config),
    ProxyName2 = ?config(proxy_name_2, Config),
    ProxyHost = ?config(proxy_host, Config),
    ProxyPort = ?config(proxy_port, Config),
    emqx_common_test_helpers:with_failure(
        down, ProxyName1, ProxyHost, ProxyPort, fun() ->
            emqx_common_test_helpers:with_failure(down, ProxyName2, ProxyHost, ProxyPort, Fun)
        end
    ).

mock_iam_metadata_v2_calls() ->
    ok = meck:new(erlcloud_ec2_meta, [passthrough]),
    ok = meck:expect(
        erlcloud_ec2_meta,
        get_metadata_v2_session_token,
        fun(_Cfg) -> {ok, <<"mocked token">>} end
    ),
    ok = meck:expect(
        erlcloud_ec2_meta,
        get_instance_metadata_v2,
        fun(Path, _Cfg, _Opts) ->
            case Path of
                "placement/region" ->
                    {ok, <<"sa-east-1">>};
                "iam/security-credentials/" ->
                    {ok, <<"mocked_role\n">>};
                "iam/security-credentials/mocked_role" ->
                    Resp = #{
                        <<"AccessKeyId">> => <<"mockedkeyid">>,
                        <<"SecretAccessKey">> => <<"mockedsecretkey">>,
                        <<"Token">> => <<"mockedtoken">>
                    },
                    {ok, emqx_utils_json:encode(Resp)}
            end
        end
    ).

health_check(Type, Name) ->
    emqx_bridge_v2_testlib:force_health_check(#{
        type => Type,
        name => Name,
        resource_namespace => ?global_ns,
        kind => action
    }).

id(Type, Name) ->
    emqx_bridge_v2_testlib:lookup_chan_id_in_conf(#{
        kind => action,
        type => Type,
        name => Name
    }).

id(Type, Name, ConnName) ->
    emqx_bridge_v2_testlib:make_chan_id(#{
        kind => action,
        type => Type,
        name => Name,
        connector_name => ConnName
    }).

%%------------------------------------------------------------------------------
%% Test cases
%%------------------------------------------------------------------------------

%% Test sending a message to a bridge V2
t_send_message(_) ->
    BridgeV2Config = bridge_v2_config(<<"test_connector2">>),
    ConnectorConfig = connector_config(),
    {ok, _} = emqx_connector:create(?global_ns, ?TYPE, test_connector2, ConnectorConfig),
    {ok, _} = create_action(test_bridge_v2_1, BridgeV2Config),
    %% Use the bridge to send a message
    check_send_message_with_bridge(test_bridge_v2_1),
    %% Create a few more bridges with the same connector and test them
    BridgeNames1 = [
        list_to_atom("test_bridge_v2_" ++ integer_to_list(I))
     || I <- lists:seq(2, 10)
    ],
    lists:foreach(
        fun(BridgeName) ->
            {ok, _} = create_action(BridgeName, BridgeV2Config),
            check_send_message_with_bridge(BridgeName)
        end,
        BridgeNames1
    ),
    BridgeNames = [test_bridge_v2_1 | BridgeNames1],
    %% Send more messages to the bridges
    lists:foreach(
        fun(BridgeName) ->
            lists:foreach(
                fun(_) ->
                    check_send_message_with_bridge(BridgeName)
                end,
                lists:seq(1, 10)
            )
        end,
        BridgeNames
    ),
    ok.

%% Test that we can get the status of the bridge V2
t_health_check(_) ->
    BridgeV2Config = bridge_v2_config(<<"test_connector3">>),
    ConnectorConfig = connector_config(),
    {ok, _} = emqx_bridge_v2_testlib:create_connector_api(test_connector3, ?TYPE, ConnectorConfig),
    {ok, _} = create_action(test_bridge_v2, BridgeV2Config),
    #{status := connected} = health_check(?TYPE, test_bridge_v2),
    {204, _} = emqx_bridge_v2_testlib:delete_kind_api(action, ?TYPE, test_bridge_v2),
    %% Check behaviour when bridge does not exist
    {error, bridge_not_found} = health_check(?TYPE, test_bridge_v2),
    {204, _} = emqx_bridge_v2_testlib:delete_connector_api([
        {connector_type, ?TYPE},
        {connector_name, test_connector3}
    ]),
    ok.

t_local_topic(_) ->
    BridgeV2Config = bridge_v2_config(<<"test_connector">>),
    ConnectorConfig = connector_config(),
    {ok, _} = emqx_connector:create(?global_ns, ?TYPE, test_connector, ConnectorConfig),
    {ok, _} = create_action(test_bridge, BridgeV2Config),
    %% Send a message to the local topic
    Payload = <<"local_topic_payload">>,
    Offset = resolve_kafka_offset(),
    emqx:publish(emqx_message:make(<<"kafka_t/hej">>, Payload)),
    check_kafka_message_payload(Offset, Payload),
    ok.

t_message_too_large(_) ->
    BridgeV2Config = bridge_v2_config(<<"test_connector4">>, "max-100-bytes"),
    ConnectorConfig = connector_config(),
    {ok, _} = emqx_connector:create(?global_ns, ?TYPE, test_connector4, ConnectorConfig),
    BridgeName = test_bridge4,
    {ok, _} = create_action(BridgeName, BridgeV2Config),
    BridgeV2Id = id(?TYPE, BridgeName),
    TooLargePayload = iolist_to_binary(lists:duplicate(100, 100)),
    ?assertEqual(0, emqx_resource_metrics:failed_get(BridgeV2Id)),
    emqx:publish(emqx_message:make(<<"kafka_t/hej">>, TooLargePayload)),
    ?retry(
        _Sleep0 = 50,
        _Attempts0 = 100,
        begin
            ?assertEqual(1, emqx_resource_metrics:failed_get(BridgeV2Id)),
            ok
        end
    ),
    ok.

t_unknown_topic(_Config) ->
    ConnectorName = <<"test_connector">>,
    BridgeName = <<"test_bridge">>,
    BridgeV2Config0 = bridge_v2_config(ConnectorName),
    BridgeV2Config = emqx_utils_maps:deep_put(
        [<<"kafka">>, <<"topic">>],
        BridgeV2Config0,
        <<"nonexistent">>
    ),
    ConnectorConfig = connector_config(),
    {ok, _} = emqx_connector:create(?global_ns, ?TYPE, ConnectorName, ConnectorConfig),
    {ok, _} = create_action(BridgeName, BridgeV2Config),
    Payload = <<"will be dropped">>,
    emqx:publish(emqx_message:make(<<"kafka_t/local">>, Payload)),
    BridgeV2Id = id(?TYPE, BridgeName),
    ?retry(
        _Sleep0 = 50,
        _Attempts0 = 100,
        begin
            ?assertEqual(1, emqx_resource_metrics:matched_get(BridgeV2Id)),
            ?assertEqual(1, emqx_resource_metrics:dropped_get(BridgeV2Id)),
            ?assertEqual(1, emqx_resource_metrics:dropped_resource_stopped_get(BridgeV2Id)),
            ok
        end
    ),
    ?assertMatch(
        {ok,
            {{_, 200, _}, _, [
                #{
                    <<"status">> := <<"disconnected">>,
                    <<"node_status">> := [#{<<"status">> := <<"disconnected">>}]
                }
            ]}},
        emqx_bridge_v2_testlib:list_bridges_api()
    ),
    ?assertMatch(
        {ok,
            {{_, 200, _}, _, #{
                <<"status">> := <<"disconnected">>,
                <<"node_status">> := [#{<<"status">> := <<"disconnected">>}]
            }}},
        emqx_bridge_v2_testlib:get_bridge_api(?TYPE, BridgeName)
    ),
    ok.

t_bad_url(_Config) ->
    ConnectorName = <<"test_connector">>,
    ActionName = <<"test_action">>,
    ActionConfig = bridge_v2_config(<<"test_connector">>),
    ConnectorConfig0 = connector_config(),
    ConnectorConfig = ConnectorConfig0#{<<"bootstrap_hosts">> := <<"bad_host:9092">>},
    ?assertMatch({ok, _}, create_connector(ConnectorName, ConnectorConfig)),
    ?assertMatch({ok, _}, create_action(ActionName, ActionConfig)),
    ?assertMatch(
        {ok, #{
            resource_data :=
                #{
                    status := ?status_disconnected,
                    error := [#{reason := unresolvable_hostname}]
                }
        }},
        emqx_connector:lookup(?global_ns, ?TYPE, ConnectorName)
    ),
    ?assertMatch(
        {ok, #{status := ?status_disconnected}},
        emqx_bridge_v2:lookup(?global_ns, actions, ?TYPE, ActionName)
    ),
    ok.

t_parameters_key_api_spec(_Config) ->
    BridgeProps = bridge_api_spec_props_for_get(),
    ?assert(is_map_key(<<"kafka">>, BridgeProps), #{bridge_props => BridgeProps}),
    ?assertNot(is_map_key(<<"parameters">>, BridgeProps), #{bridge_props => BridgeProps}),

    ActionProps = action_api_spec_props_for_get(),
    ?assertNot(is_map_key(<<"kafka">>, ActionProps), #{action_props => ActionProps}),
    ?assert(is_map_key(<<"parameters">>, ActionProps), #{action_props => ActionProps}),

    ok.

t_http_api_get(_Config) ->
    ConnectorName = <<"test_connector">>,
    ActionName = <<"test_action">>,
    ActionConfig = bridge_v2_config(<<"test_connector">>),
    ConnectorConfig = connector_config(),
    ?assertMatch({ok, _}, create_connector(ConnectorName, ConnectorConfig)),
    ?assertMatch({ok, _}, create_action(ActionName, ActionConfig)),
    %% v1 api; no mangling of configs; has `kafka' top level config key
    ?assertMatch(
        {ok, {{_, 200, _}, _, [#{<<"kafka">> := _}]}},
        emqx_bridge_testlib:list_bridges_api()
    ),
    ok.

t_create_connector_while_connection_is_down(Config) ->
    KafkaHost = ?config(kafka_host, Config),
    KafkaPort = ?config(kafka_port, Config),
    Host = iolist_to_binary([KafkaHost, ":", integer_to_binary(KafkaPort)]),
    ?check_trace(
        begin
            Type = ?TYPE,
            ConnectorConfig = connector_config(#{
                <<"bootstrap_hosts">> => Host,
                <<"resource_opts">> =>
                    #{<<"health_check_interval">> => <<"500ms">>}
            }),
            ConnectorName = <<"c1">>,
            ConnectorId = emqx_connector_resource:resource_id(Type, ConnectorName),
            ConnectorParams = [
                {connector_config, ConnectorConfig},
                {connector_name, ConnectorName},
                {connector_type, Type}
            ],
            ActionName = ConnectorName,
            ActionId = id(?TYPE, ActionName, ConnectorName),
            ActionConfig = action_config(
                ConnectorName
            ),
            ActionParams = [
                {action_config, ActionConfig},
                {action_name, ActionName},
                {action_type, Type}
            ],
            Disconnected = atom_to_binary(?status_disconnected),
            %% Initially, the connection cannot be stablished.  Messages are not buffered,
            %% hence the status is `?status_disconnected'.
            with_brokers_down(Config, fun() ->
                {ok, {{_, 201, _}, _, #{<<"status">> := Disconnected}}} =
                    emqx_bridge_v2_testlib:create_connector_api(ConnectorParams),
                {ok, {{_, 201, _}, _, #{<<"status">> := Disconnected}}} =
                    emqx_bridge_v2_testlib:create_action_api(ActionParams),
                #{offset := Offset1} = send_message(ActionName),
                #{offset := Offset2} = send_message(ActionName),
                #{offset := Offset3} = send_message(ActionName),
                ?assertEqual([Offset1], lists:usort([Offset1, Offset2, Offset3])),
                ?assertEqual(3, emqx_resource_metrics:matched_get(ActionId)),
                ?assertEqual(3, emqx_resource_metrics:failed_get(ActionId)),
                ?assertEqual(0, emqx_resource_metrics:queuing_get(ActionId)),
                ?assertEqual(0, emqx_resource_metrics:inflight_get(ActionId)),
                ?assertEqual(0, emqx_resource_metrics:dropped_get(ActionId)),
                ok
            end),
            %% Let the connector and action recover
            Connected = atom_to_binary(?status_connected),
            ?retry(
                _Sleep0 = 1_100,
                _Attempts0 = 10,
                begin
                    _ = emqx_resource:health_check(ConnectorId),
                    _ = emqx_resource:health_check(ActionId),
                    ?assertMatch(
                        {ok, #{
                            status := ?status_connected,
                            resource_data :=
                                #{
                                    status := ?status_connected,
                                    added_channels :=
                                        #{
                                            ActionId := #{
                                                status := ?status_connected
                                            }
                                        }
                                }
                        }},
                        emqx_bridge_v2:lookup(?global_ns, actions, Type, ActionName),
                        #{action_id => ActionId}
                    ),
                    ?assertMatch(
                        {ok, {{_, 200, _}, _, #{<<"status">> := Connected}}},
                        emqx_bridge_v2_testlib:get_action_api(ActionParams)
                    )
                end
            ),
            %% Now the connection drops again; this time, status should be
            %% `?status_connecting' to avoid destroying wolff_producers and their replayq
            %% buffers.
            Connecting = atom_to_binary(?status_connecting),
            with_brokers_down(Config, fun() ->
                ?retry(
                    _Sleep0 = 1_100,
                    _Attempts0 = 10,
                    begin
                        _ = emqx_resource:health_check(ConnectorId),
                        _ = emqx_resource:health_check(ActionId),
                        ?assertMatch(
                            {ok, #{
                                status := ?status_connecting,
                                resource_data :=
                                    #{
                                        status := ?status_connecting,
                                        added_channels :=
                                            #{
                                                ActionId := #{
                                                    status := ?status_connecting
                                                }
                                            }
                                    }
                            }},
                            emqx_bridge_v2:lookup(?global_ns, actions, Type, ActionName),
                            #{action_id => ActionId}
                        ),
                        ?assertMatch(
                            {ok, {{_, 200, _}, _, #{<<"status">> := Connecting}}},
                            emqx_bridge_v2_testlib:get_action_api(ActionParams)
                        )
                    end
                ),
                %% This should get enqueued by wolff producers.
                spawn_link(fun() -> send_message(ActionName) end),
                PreviousMatched = 3,
                PreviousFailed = 3,
                ?retry(
                    _Sleep2 = 100,
                    _Attempts2 = 10,
                    ?assertEqual(PreviousMatched + 1, emqx_resource_metrics:matched_get(ActionId))
                ),
                ?assertEqual(PreviousFailed, emqx_resource_metrics:failed_get(ActionId)),
                ?assertEqual(1, emqx_resource_metrics:queuing_get(ActionId)),
                QueuingBytes = emqx_resource_metrics:queuing_bytes_get(ActionId),
                ?assert(QueuingBytes > 0, #{bytes => QueuingBytes}),
                ?assertEqual(0, emqx_resource_metrics:inflight_get(ActionId)),
                ?assertEqual(0, emqx_resource_metrics:dropped_get(ActionId)),
                ?assertEqual(0, emqx_resource_metrics:success_get(ActionId)),
                ok
            end),
            ?retry(
                _Sleep2 = 600,
                _Attempts2 = 20,
                begin
                    _ = emqx_resource:health_check(ConnectorId),
                    _ = emqx_resource:health_check(ActionId),
                    ?assertEqual(1, emqx_resource_metrics:success_get(ActionId), #{
                        metrics => emqx_bridge_v2:get_metrics(Type, ActionName)
                    }),
                    ok
                end
            ),
            ok
        end,
        []
    ),
    ok.

t_ancient_v1_config_migration_with_local_topic(Config) ->
    %% Simply starting this test case successfully is enough, as the core of the test is
    %% to be able to successfully start the node with the ancient config.
    [Node] = ?config(cluster, Config),
    ?assertMatch(
        [#{type := <<"kafka_producer">>}],
        erpc:call(Node, fun emqx_bridge_v2:list/0)
    ),
    ok.

t_ancient_v1_config_migration_without_local_topic(Config) ->
    %% Simply starting this test case successfully is enough, as the core of the test is
    %% to be able to successfully start the node with the ancient config.
    [Node] = ?config(cluster, Config),
    ?assertMatch(
        [#{type := <<"kafka_producer">>}],
        erpc:call(Node, fun emqx_bridge_v2:list/0)
    ),
    ok.

t_connector_health_check_topic(_Config) ->
    ?check_trace(
        begin
            %% We create a connector pointing to a broker that expects authentication, but
            %% we don't provide it in the config.
            %% Without a health check topic, a dummy topic name is used to probe
            %% post-auth connectivity, so the status is "disconnected"
            Type = ?TYPE,
            Name = ?FUNCTION_NAME,
            PlainAuthBootstrapHost = <<"kafka-1.emqx.net:9093">>,
            ConnectorConfig0 = connector_config(#{
                <<"bootstrap_hosts">> => PlainAuthBootstrapHost
            }),
            ?assertMatch(
                {ok, {{_, 201, _}, _, #{<<"status">> := <<"disconnected">>}}},
                emqx_bridge_v2_testlib:create_connector_api([
                    {connector_type, Type},
                    {connector_name, Name},
                    {connector_config, ConnectorConfig0}
                ])
            ),

            %% By providing a health check topic, we should detect it's disconnected
            %% without the need for an action.
            ConnectorConfig1 = connector_config(#{
                <<"bootstrap_hosts">> => PlainAuthBootstrapHost,
                <<"health_check_topic">> =>
                    emqx_bridge_kafka_impl_producer_SUITE:test_topic_one_partition()
            }),
            ?assertMatch(
                {ok, {{_, 200, _}, _, #{<<"status">> := <<"disconnected">>}}},
                emqx_bridge_v2_testlib:update_connector_api(Name, Type, ConnectorConfig1)
            ),

            %% By providing an inexistent health check topic, we should detect it's
            %% disconnected without the need for an action.
            ConnectorConfig2 = connector_config(#{
                <<"bootstrap_hosts">> => iolist_to_binary(kafka_hosts_string()),
                <<"health_check_topic">> => <<"i-dont-exist-999">>
            }),
            ?assertMatch(
                {ok,
                    {{_, 200, _}, _, #{
                        <<"status">> := <<"disconnected">>,
                        <<"status_reason">> := <<"Unknown topic or partition", _/binary>>
                    }}},
                emqx_bridge_v2_testlib:update_connector_api(Name, Type, ConnectorConfig2)
            ),

            ok
        end,
        []
    ),
    ok.

%% Checks that, if Kafka raises `invalid_partition_count' error, we bump the corresponding
%% failure rule action metric.
t_invalid_partition_count_metrics(Config) ->
    Type = proplists:get_value(type, Config, ?TYPE),
    ConnectorName = proplists:get_value(connector_name, Config, <<"c">>),
    ConnectorConfig = proplists:get_value(connector_config, Config, connector_config()),
    ActionConfig1 = proplists:get_value(action_config, Config, action_config(ConnectorName)),
    ?check_trace(
        #{timetrap => 10_000},
        begin
            ConnectorParams = [
                {connector_config, ConnectorConfig},
                {connector_name, ConnectorName},
                {connector_type, Type}
            ],
            ActionName = <<"a">>,
            ActionParams = [
                {action_config, ActionConfig1},
                {action_name, ActionName},
                {action_type, Type}
            ],
            {ok, {{_, 201, _}, _, #{}}} =
                emqx_bridge_v2_testlib:create_connector_api(ConnectorParams),

            {ok, C} = emqtt:start_link([]),
            {ok, _} = emqtt:connect(C),

            %%--------------------------------------------
            ?tp(notice, "sync", #{}),
            %%--------------------------------------------
            %% Artificially force sync query to be used; otherwise, it's only used when the
            %% resource is blocked and retrying.
            ok = meck:new(emqx_bridge_kafka_impl_producer, [passthrough, no_history]),
            on_exit(fun() -> catch meck:unload() end),
            ok = meck:expect(emqx_bridge_kafka_impl_producer, query_mode, 1, simple_sync),

            {ok, {{_, 201, _}, _, #{}}} =
                emqx_bridge_v2_testlib:create_action_api(ActionParams),
            RuleTopic = <<"t/a">>,
            {ok, #{<<"id">> := RuleId}} =
                emqx_bridge_v2_testlib:create_rule_and_action_http(Type, RuleTopic, [
                    {bridge_name, ActionName}
                ]),

            %% Simulate `invalid_partition_count'
            emqx_common_test_helpers:with_mock(
                wolff,
                send_sync2,
                fun(_Producers, _Topic, _Msgs, _Timeout) ->
                    throw(#{
                        cause => invalid_partition_count,
                        count => 0,
                        partitioner => partitioner
                    })
                end,
                fun() ->
                    {{ok, _}, {ok, _}} =
                        ?wait_async_action(
                            emqtt:publish(C, RuleTopic, <<"hi">>, 2),
                            #{
                                ?snk_kind := "kafka_producer_invalid_partition_count",
                                query_mode := sync
                            }
                        ),
                    ?assertMatch(
                        #{
                            counters := #{
                                'actions.total' := 1,
                                'actions.failed' := 1
                            }
                        },
                        get_rule_metrics(RuleId)
                    ),
                    ok
                end
            ),

            %%--------------------------------------------
            %% Same thing, but async call
            ?tp(notice, "async", #{}),
            %%--------------------------------------------
            ok = meck:expect(
                emqx_bridge_kafka_impl_producer,
                query_mode,
                fun(Conf) -> meck:passthrough([Conf]) end
            ),
            %% Force remove action, but leave rule so that its metrics fail below.
            ok = emqx_bridge_v2:remove(?global_ns, actions, Type, ActionName),
            {ok, {{_, 201, _}, _, #{}}} =
                emqx_bridge_v2_testlib:create_action_api(
                    ActionParams,
                    #{<<"parameters">> => #{<<"query_mode">> => <<"async">>}}
                ),

            %% Simulate `invalid_partition_count'
            emqx_common_test_helpers:with_mock(
                wolff,
                send2,
                fun(_Producers, _Topic, _Msgs, _AckCallback) ->
                    throw(#{
                        cause => invalid_partition_count,
                        count => 0,
                        partitioner => partitioner
                    })
                end,
                fun() ->
                    {{ok, _}, {ok, _}} =
                        ?wait_async_action(
                            emqtt:publish(C, RuleTopic, <<"hi">>, 2),
                            #{?snk_kind := "rule_engine_applied_all_rules"}
                        ),
                    ?assertMatch(
                        #{
                            counters := #{
                                'actions.total' := 2,
                                'actions.failed' := 2
                            }
                        },
                        get_rule_metrics(RuleId)
                    ),
                    ok
                end
            ),

            ok
        end,
        fun(Trace) ->
            ?assertMatch(
                [#{query_mode := sync}, #{query_mode := async} | _],
                ?of_kind("kafka_producer_invalid_partition_count", Trace)
            ),
            ok
        end
    ),
    ok.

%% Tests that deleting/disabling an action that share the same Kafka topic with other
%% actions do not disturb the latter.
t_multiple_actions_sharing_topic(Config) ->
    Type = proplists:get_value(type, Config, ?TYPE),
    ConnectorName = proplists:get_value(connector_name, Config, <<"c">>),
    ConnectorConfig = proplists:get_value(connector_config, Config, connector_config()),
    ActionConfig = proplists:get_value(action_config, Config, action_config(ConnectorName)),
    ?check_trace(
        begin
            ConnectorParams = [
                {connector_config, ConnectorConfig},
                {connector_name, ConnectorName},
                {connector_type, Type}
            ],
            ActionName1 = <<"a1">>,
            ActionParams1 = [
                {action_config, ActionConfig},
                {action_name, ActionName1},
                {action_type, Type}
            ],
            ActionName2 = <<"a2">>,
            ActionParams2 = [
                {action_config, ActionConfig},
                {action_name, ActionName2},
                {action_type, Type}
            ],
            {ok, {{_, 201, _}, _, #{}}} =
                emqx_bridge_v2_testlib:create_connector_api(ConnectorParams),
            {ok, {{_, 201, _}, _, #{}}} =
                emqx_bridge_v2_testlib:create_action_api(ActionParams1),
            {ok, {{_, 201, _}, _, #{}}} =
                emqx_bridge_v2_testlib:create_action_api(ActionParams2),
            RuleTopic = <<"t/a2">>,
            {ok, _} = emqx_bridge_v2_testlib:create_rule_and_action_http(Type, RuleTopic, Config),

            ?assertStatusAPI(Type, ActionName1, <<"connected">>),
            ?assertStatusAPI(Type, ActionName2, <<"connected">>),

            %% Disabling a1 shouldn't disturb a2.
            ?assertMatch(
                {204, _}, emqx_bridge_v2_testlib:disable_kind_api(action, Type, ActionName1)
            ),

            ?assertStatusAPI(Type, ActionName1, <<"disconnected">>),
            ?assertStatusAPI(Type, ActionName2, <<"connected">>),

            ?assertMatch(#{result := ok}, send_message(Type, ActionName2)),
            ?assertStatusAPI(Type, ActionName2, <<"connected">>),

            ?assertMatch(
                {204, _},
                emqx_bridge_v2_testlib:enable_kind_api(action, Type, ActionName1)
            ),
            ?assertStatusAPI(Type, ActionName1, <<"connected">>),
            ?assertStatusAPI(Type, ActionName2, <<"connected">>),
            ?assertMatch(#{result := ok}, send_message(Type, ActionName2)),

            %% Deleting also shouldn't disrupt a2.
            ?assertMatch(
                {204, _},
                emqx_bridge_v2_testlib:delete_kind_api(action, Type, ActionName1)
            ),
            ?assertStatusAPI(Type, ActionName2, <<"connected">>),
            ?assertMatch(#{result := ok}, send_message(Type, ActionName2)),

            ok
        end,
        fun(Trace) ->
            ?assertEqual([], ?of_kind("kafka_producer_invalid_partition_count", Trace)),
            ok
        end
    ),
    ok.

%% Smoke tests for using a templated topic and adynamic kafka topics.
t_dynamic_topics(Config) ->
    Type = proplists:get_value(type, Config, ?TYPE),
    ConnectorName = proplists:get_value(connector_name, Config, <<"c">>),
    ConnectorConfig = proplists:get_value(connector_config, Config, connector_config()),
    ActionName = <<"dynamic_topics">>,
    ActionConfig1 = proplists:get_value(action_config, Config, action_config(ConnectorName)),
    PreConfiguredTopic1 = <<"pct1">>,
    PreConfiguredTopic2 = <<"pct2">>,
    ensure_kafka_topic(PreConfiguredTopic1),
    ensure_kafka_topic(PreConfiguredTopic2),
    ActionConfig = emqx_bridge_v2_testlib:parse_and_check(
        action,
        Type,
        ActionName,
        emqx_utils_maps:deep_merge(
            ActionConfig1,
            #{
                <<"parameters">> => #{
                    <<"topic">> => <<"pct${.payload.n}">>,
                    <<"message">> => #{
                        <<"key">> => <<"${.clientid}">>,
                        <<"value">> => <<"${.payload.p}">>
                    }
                }
            }
        )
    ),
    ?check_trace(
        #{timetrap => 7_000},
        begin
            ConnectorParams = [
                {connector_config, ConnectorConfig},
                {connector_name, ConnectorName},
                {connector_type, Type}
            ],
            ActionParams = [
                {action_config, ActionConfig},
                {action_name, ActionName},
                {action_type, Type}
            ],
            {ok, {{_, 201, _}, _, #{}}} =
                emqx_bridge_v2_testlib:create_connector_api(ConnectorParams),

            {ok, {{_, 201, _}, _, #{}}} =
                emqx_bridge_v2_testlib:create_action_api(ActionParams),
            RuleTopic = <<"pct">>,
            {ok, _} = emqx_bridge_v2_testlib:create_rule_and_action_http(
                Type,
                RuleTopic,
                [
                    {bridge_name, ActionName}
                ]
            ),
            ?assertStatusAPI(Type, ActionName, <<"connected">>),

            ?tapTelemetry(),

            {ok, C} = emqtt:start_link(#{}),
            {ok, _} = emqtt:connect(C),
            Payload = fun(Map) -> emqx_utils_json:encode(Map) end,
            Offset1 = resolve_kafka_offset(PreConfiguredTopic1),
            Offset2 = resolve_kafka_offset(PreConfiguredTopic2),
            {ok, _} = emqtt:publish(C, RuleTopic, Payload(#{n => 1, p => <<"p1">>}), [{qos, 1}]),
            {ok, _} = emqtt:publish(C, RuleTopic, Payload(#{n => 2, p => <<"p2">>}), [{qos, 1}]),

            check_kafka_message_payload(PreConfiguredTopic1, Offset1, <<"p1">>),
            check_kafka_message_payload(PreConfiguredTopic2, Offset2, <<"p2">>),

            ActionId = id(Type, ActionName),
            ?assertEqual(2, emqx_resource_metrics:matched_get(ActionId)),
            ?assertEqual(2, emqx_resource_metrics:success_get(ActionId)),
            ?assertEqual(0, emqx_resource_metrics:queuing_get(ActionId)),

            ?assertReceive(
                {telemetry, #{
                    measurements := #{gauge_set := _},
                    metadata := #{worker_id := _, resource_id := ActionId}
                }}
            ),

            %% If there isn't enough information in the context to resolve to a topic, it
            %% should be an unrecoverable error.
            ?assertMatch(
                {_, {ok, _}},
                ?wait_async_action(
                    emqtt:publish(C, RuleTopic, Payload(#{not_enough => <<"info">>}), [{qos, 1}]),
                    #{?snk_kind := "kafka_producer_failed_to_render_topic"}
                )
            ),

            %% If it's possible to render the topic, but it isn't in the pre-configured
            %% list, it should be an unrecoverable error.
            ?assertMatch(
                {_, {ok, _}},
                ?wait_async_action(
                    emqtt:publish(C, RuleTopic, Payload(#{n => 99}), [{qos, 1}]),
                    #{?snk_kind := "kafka_producer_resolved_to_unknown_topic"}
                )
            ),

            ok
        end,
        []
    ),
    ok.

%% Checks that messages accumulated in disk mode for a fixed topic producer are kicked off
%% when the action is later restarted and kafka is online.
t_fixed_topic_recovers_in_disk_mode(Config) ->
    Type = proplists:get_value(type, Config, ?TYPE),
    ConnectorName = proplists:get_value(connector_name, Config, <<"c">>),
    ConnectorConfig = proplists:get_value(
        connector_config, Config, connector_config_toxiproxy(Config)
    ),
    ActionName = <<"fixed_topic_disk_recover">>,
    ActionConfig1 = proplists:get_value(action_config, Config, action_config(ConnectorName)),
    ActionConfig = emqx_bridge_v2_testlib:parse_and_check(
        action,
        Type,
        ActionName,
        emqx_utils_maps:deep_merge(
            ActionConfig1,
            #{
                <<"parameters">> => #{
                    <<"query_mode">> => <<"async">>,
                    <<"buffer">> => #{
                        <<"mode">> => <<"disk">>
                    }
                }
            }
        )
    ),
    Topic = emqx_utils_maps:deep_get([<<"parameters">>, <<"topic">>], ActionConfig),
    Hosts = kpro:parse_endpoints(
        binary_to_list(maps:get(<<"bootstrap_hosts">>, ConnectorConfig))
    ),
    ?check_trace(
        #{timetrap => 7_000},
        begin
            ConnectorParams = [
                {connector_config, ConnectorConfig},
                {connector_name, ConnectorName},
                {connector_type, Type}
            ],
            ActionParams = [
                {action_config, ActionConfig},
                {action_name, ActionName},
                {action_type, Type}
            ],
            {ok, {{_, 201, _}, _, #{}}} =
                emqx_bridge_v2_testlib:create_connector_api(ConnectorParams),

            {ok, {{_, 201, _}, _, #{}}} =
                emqx_bridge_v2_testlib:create_action_api(ActionParams),
            RuleTopic = <<"fixed/disk/recover">>,
            {ok, _} = emqx_bridge_v2_testlib:create_rule_and_action_http(
                Type,
                RuleTopic,
                [
                    {bridge_name, ActionName}
                ]
            ),
            %% Cut connection to kafka and enqueue some messages
            ActionId = id(Type, ActionName),
            SentMessages = with_brokers_down(Config, fun() ->
                ct:sleep(100),
                SentMessages = [send_message(ActionName) || _ <- lists:seq(1, 5)],
                ?assertEqual(5, emqx_resource_metrics:matched_get(ActionId)),
                ?retry(
                    _Sleep = 200,
                    _Attempts = 20,
                    ?assertEqual(5, emqx_resource_metrics:queuing_get(ActionId))
                ),
                ?assertEqual(0, emqx_resource_metrics:success_get(ActionId)),
                %% Turn off action, restore kafka connection
                ?assertMatch(
                    {204, _},
                    emqx_bridge_v2_testlib:disable_kind_api(action, Type, ActionName)
                ),
                SentMessages
            end),
            %% Restart action; should've shot enqueued messages
            ?tapTelemetry(),
            ?assertMatch(
                {204, _},
                emqx_bridge_v2_testlib:enable_kind_api(action, Type, ActionName)
            ),
            ?assertReceive(
                {telemetry, #{
                    name := [?TELEMETRY_PREFIX, inflight],
                    measurements := #{gauge_set := 0}
                }}
            ),
            %% Success metrics are not bumped because wolff does not store callbacks in
            %% disk.
            ?assertEqual(0, emqx_resource_metrics:success_get(ActionId)),
            ?retry(
                _Sleep1 = 200,
                _Attempts1 = 20,
                ?assertEqual(0, emqx_resource_metrics:queuing_get(ActionId))
            ),
            [#{offset := Offset} | _] = SentMessages,
            Partition = 0,
            Messages = fetch_since(Hosts, Topic, Partition, Offset),
            ?assertMatch([_, _, _, _, _], Messages),
            ok
        end,
        []
    ),
    ok.

%% Verifies that we disallow disk mode when the kafka topic is dynamic.
t_disallow_disk_mode_for_dynamic_topic(Config) ->
    Type = proplists:get_value(type, Config, ?TYPE),
    ConnectorName = proplists:get_value(connector_name, Config, <<"c">>),
    ActionName = <<"dynamic_topic_disk">>,
    ActionConfig = proplists:get_value(action_config, Config, action_config(ConnectorName)),
    ?assertMatch(
        #{},
        emqx_bridge_v2_testlib:parse_and_check(
            action,
            Type,
            ActionName,
            emqx_utils_maps:deep_merge(
                ActionConfig,
                #{
                    <<"parameters">> => #{
                        <<"topic">> => <<"dynamic-${.payload.n}">>,
                        <<"buffer">> => #{
                            <<"mode">> => <<"hybrid">>
                        }
                    }
                }
            )
        )
    ),
    ?assertThrow(
        {_SchemaMod, [
            #{
                reason := <<"disk-mode buffering is disallowed when using dynamic topics">>,
                kind := validation_error
            }
        ]},
        emqx_bridge_v2_testlib:parse_and_check(
            action,
            Type,
            ActionName,
            emqx_utils_maps:deep_merge(
                ActionConfig,
                #{
                    <<"parameters">> => #{
                        <<"topic">> => <<"dynamic-${.payload.n}">>,
                        <<"buffer">> => #{
                            <<"mode">> => <<"disk">>
                        }
                    }
                }
            )
        )
    ),
    ok.

%% In wolff < 2.0.0, replayq filepath was computed differently than current versions,
%% after dynamic topics were introduced.  This verifies that we migrate older directories
%% if we detect them when starting the producer.
t_migrate_old_replayq_dir(Config) ->
    Type = proplists:get_value(type, Config, ?TYPE),
    ConnectorName = proplists:get_value(connector_name, Config, <<"c">>),
    ConnectorConfig = proplists:get_value(
        connector_config, Config, connector_config_toxiproxy(Config)
    ),
    ActionName = atom_to_binary(?FUNCTION_NAME),
    ActionConfig1 = proplists:get_value(action_config, Config, action_config(ConnectorName)),
    ActionConfig = emqx_bridge_v2_testlib:parse_and_check(
        action,
        Type,
        ActionName,
        emqx_utils_maps:deep_merge(
            ActionConfig1,
            #{
                <<"parameters">> => #{
                    <<"buffer">> => #{
                        <<"mode">> => <<"disk">>
                    }
                }
            }
        )
    ),
    #{<<"parameters">> := #{<<"topic">> := Topic}} = ActionConfig,
    ReplayqDir = emqx_bridge_kafka_impl_producer:replayq_dir(Type, ActionName),
    OldWolffDir = filename:join([ReplayqDir, Topic]),
    %% simulate partition sub-directories
    NumPartitions = 3,
    OldDirs = lists:map(
        fun(N) ->
            filename:join([OldWolffDir, integer_to_binary(N)])
        end,
        lists:seq(1, NumPartitions)
    ),
    lists:foreach(
        fun(D) ->
            ok = filelib:ensure_path(D)
        end,
        OldDirs
    ),
    ConnectorParams = [
        {connector_config, ConnectorConfig},
        {connector_name, ConnectorName},
        {connector_type, Type}
    ],
    ActionParams = [
        {action_config, ActionConfig},
        {action_name, ActionName},
        {action_type, Type}
    ],
    {ok, {{_, 201, _}, _, #{}}} =
        emqx_bridge_v2_testlib:create_connector_api(ConnectorParams),
    ?check_trace(
        begin
            {ok, {{_, 201, _}, _, #{}}} =
                emqx_bridge_v2_testlib:create_action_api(ActionParams),
            %% Old directories have been moved
            lists:foreach(
                fun(D) ->
                    ?assertNot(filelib:is_dir(D))
                end,
                OldDirs
            ),
            ok
        end,
        fun(Trace) ->
            ?assertMatch([#{from := OldWolffDir}], ?of_kind("migrating_old_wolff_dirs", Trace)),
            ok
        end
    ),
    ok.

%% Checks that we don't report a producer as `?status_disconnected' if it's already
%% created.
t_inexistent_topic_after_created(Config) ->
    Type = proplists:get_value(type, Config, ?TYPE),
    ConnectorName = proplists:get_value(connector_name, Config, <<"c">>),
    ConnectorConfig = proplists:get_value(
        connector_config, Config, connector_config_toxiproxy(Config)
    ),
    ActionName = atom_to_binary(?FUNCTION_NAME),
    ActionConfig1 = proplists:get_value(action_config, Config, action_config(ConnectorName)),
    Topic = atom_to_binary(?FUNCTION_NAME),
    ActionConfig = emqx_bridge_v2_testlib:parse_and_check(
        action,
        Type,
        ActionName,
        emqx_utils_maps:deep_merge(
            ActionConfig1,
            #{
                <<"parameters">> => #{
                    <<"topic">> => Topic
                },
                <<"resource_opts">> => #{
                    <<"health_check_interval">> => <<"1s">>
                }
            }
        )
    ),
    ConnectorParams = [
        {connector_config, ConnectorConfig},
        {connector_name, ConnectorName},
        {connector_type, Type}
    ],
    ActionParams = [
        {action_config, ActionConfig},
        {action_name, ActionName},
        {action_type, Type}
    ],
    ?check_trace(
        #{timetrap => 7_000},
        begin
            ensure_kafka_topic(Topic),
            {201, #{<<"status">> := <<"connected">>}} =
                simplify_result(emqx_bridge_v2_testlib:create_connector_api(ConnectorParams)),

            %% Initially connected
            ?assertMatch(
                {201, #{<<"status">> := <<"connected">>}},
                simplify_result(emqx_bridge_v2_testlib:create_action_api(ActionParams))
            ),

            %% After deleting the topic and a health check, it becomes connecting.
            {ok, {ok, _}} =
                ?wait_async_action(
                    delete_kafka_topic(Topic),
                    #{?snk_kind := "kafka_producer_action_unknown_topic"}
                ),
            ?assertMatch(
                {200, #{<<"status">> := <<"connecting">>}},
                simplify_result(emqx_bridge_v2_testlib:get_action_api(ActionParams))
            ),

            %% Recovers after topic is back
            {ok, {ok, _}} =
                ?wait_async_action(
                    ensure_kafka_topic(Topic),
                    #{?snk_kind := "kafka_producer_action_connected"}
                ),
            ?assertMatch(
                {200, #{<<"status">> := <<"connected">>}},
                simplify_result(emqx_bridge_v2_testlib:get_action_api(ActionParams))
            ),

            ok
        end,
        []
    ),
    ok.

%% When the connector is disabled but the action is enabled, we should bump the rule
%% metrics accordingly, bumping only `actions.out_of_service'.
t_metrics_out_of_service(Config) ->
    Type = proplists:get_value(type, Config, ?TYPE),
    ConnectorName = proplists:get_value(connector_name, Config, <<"c">>),
    ConnectorConfig0 = proplists:get_value(
        connector_config, Config, connector_config_toxiproxy(Config)
    ),
    ConnectorConfig = ConnectorConfig0#{<<"enable">> := false},
    ActionName = atom_to_binary(?FUNCTION_NAME),
    ActionConfig1 = proplists:get_value(action_config, Config, action_config(ConnectorName)),
    ActionConfig = emqx_bridge_v2_testlib:parse_and_check(
        action,
        Type,
        ActionName,
        emqx_utils_maps:deep_merge(
            ActionConfig1,
            #{<<"parameters">> => #{<<"query_mode">> => <<"async">>}}
        )
    ),
    ConnectorParams = [
        {connector_config, ConnectorConfig},
        {connector_name, ConnectorName},
        {connector_type, Type}
    ],
    ActionParams = [
        {action_config, ActionConfig},
        {action_name, ActionName},
        {action_type, Type}
    ],
    {201, #{<<"enable">> := false}} =
        simplify_result(emqx_bridge_v2_testlib:create_connector_api(ConnectorParams)),
    {201, _} = simplify_result(emqx_bridge_v2_testlib:create_action_api(ActionParams)),
    RuleTopic = <<"t/k/oos">>,
    {ok, #{<<"id">> := RuleId}} =
        emqx_bridge_v2_testlib:create_rule_and_action_http(Type, RuleTopic, [
            {bridge_name, ActionName}
        ]),
    %% Async query
    emqx:publish(emqx_message:make(RuleTopic, <<"a">>)),
    ?retry(
        100,
        10,
        ?assertMatch(
            #{
                counters :=
                    #{
                        'matched' := 1,
                        'failed' := 0,
                        'passed' := 1,
                        'actions.success' := 0,
                        'actions.failed' := 1,
                        'actions.failed.out_of_service' := 1,
                        'actions.failed.unknown' := 0,
                        'actions.discarded' := 0
                    }
            },
            get_rule_metrics(RuleId)
        )
    ),
    %% Sync query
    {200, _} = simplify_result(
        emqx_bridge_v2_testlib:update_bridge_api(
            ActionParams,
            #{<<"parameters">> => #{<<"query_mode">> => <<"sync">>}}
        )
    ),
    emqx:publish(emqx_message:make(RuleTopic, <<"a">>)),
    ?retry(
        100,
        10,
        ?assertMatch(
            #{
                counters :=
                    #{
                        'matched' := 2,
                        'failed' := 0,
                        'passed' := 2,
                        'actions.success' := 0,
                        'actions.failed' := 2,
                        'actions.failed.out_of_service' := 2,
                        'actions.failed.unknown' := 0,
                        'actions.discarded' := 0
                    }
            },
            get_rule_metrics(RuleId)
        )
    ),
    ok.

%% Verifies that the `actions.failed' and `actions.failed.unknown' counters are bumped
%% when a message is dropped due to buffer overflow (both sync and async).
t_overflow_rule_metrics(Config) ->
    Type = proplists:get_value(type, Config, ?TYPE),
    ConnectorName = proplists:get_value(connector_name, Config, <<"c">>),
    ConnectorConfig = proplists:get_value(
        connector_config, Config, connector_config_toxiproxy(Config)
    ),
    ActionName = atom_to_binary(?FUNCTION_NAME),
    ActionConfig1 = proplists:get_value(action_config, Config, action_config(ConnectorName)),
    ActionConfig = emqx_bridge_v2_testlib:parse_and_check(
        action,
        Type,
        ActionName,
        emqx_utils_maps:deep_merge(
            ActionConfig1,
            #{
                <<"parameters">> => #{
                    <<"query_mode">> => <<"async">>,
                    <<"buffer">> => #{
                        <<"segment_bytes">> => <<"1B">>,
                        <<"per_partition_limit">> => <<"2B">>
                    }
                }
            }
        )
    ),
    ConnectorParams = [
        {connector_config, ConnectorConfig},
        {connector_name, ConnectorName},
        {connector_type, Type}
    ],
    ActionParams = [
        {action_config, ActionConfig},
        {action_name, ActionName},
        {action_type, Type}
    ],
    {201, _} = simplify_result(emqx_bridge_v2_testlib:create_connector_api(ConnectorParams)),
    {201, _} = simplify_result(emqx_bridge_v2_testlib:create_action_api(ActionParams)),

    RuleTopic = <<"t/k/overflow">>,
    {ok, #{<<"id">> := RuleId}} =
        emqx_bridge_v2_testlib:create_rule_and_action_http(Type, RuleTopic, [
            {bridge_name, ActionName}
        ]),

    %% Async
    emqx:publish(emqx_message:make(RuleTopic, <<"aaaaaaaaaaaaaa">>)),
    ?retry(
        100,
        10,
        ?assertMatch(
            #{
                counters := #{
                    'dropped' := 1,
                    'dropped.queue_full' := 1,
                    'dropped.expired' := 0,
                    'dropped.other' := 0,
                    'matched' := 1,
                    'success' := 0,
                    'failed' := 0,
                    'late_reply' := 0,
                    'retried' := 0
                }
            },
            emqx_bridge_v2_testlib:get_metrics(#{
                type => Type,
                name => ActionName
            })
        )
    ),
    ?retry(
        100,
        10,
        ?assertMatch(
            #{
                counters :=
                    #{
                        'matched' := 1,
                        'failed' := 0,
                        'passed' := 1,
                        'actions.success' := 0,
                        'actions.failed' := 1,
                        'actions.failed.out_of_service' := 0,
                        'actions.failed.unknown' := 1,
                        'actions.discarded' := 0
                    }
            },
            get_rule_metrics(RuleId)
        )
    ),

    %% Sync
    {200, _} = simplify_result(
        emqx_bridge_v2_testlib:update_bridge_api(
            ActionParams,
            #{<<"parameters">> => #{<<"query_mode">> => <<"sync">>}}
        )
    ),
    ok = reset_rule_metrics(RuleId),
    emqx:publish(emqx_message:make(RuleTopic, <<"aaaaaaaaaaaaaa">>)),
    ?retry(
        100,
        10,
        ?assertMatch(
            #{
                counters := #{
                    'dropped' := 1,
                    'dropped.queue_full' := 1,
                    'dropped.expired' := 0,
                    'dropped.other' := 0,
                    'matched' := 1,
                    'success' := 0,
                    'failed' := 0,
                    'late_reply' := 0,
                    'retried' := 0
                }
            },
            emqx_bridge_v2_testlib:get_metrics(#{
                type => Type, name => ActionName
            })
        )
    ),
    ?retry(
        100,
        10,
        ?assertMatch(
            #{
                counters :=
                    #{
                        'matched' := 1,
                        'failed' := 0,
                        'passed' := 1,
                        'actions.success' := 0,
                        'actions.failed' := 1,
                        'actions.failed.out_of_service' := 0,
                        'actions.failed.unknown' := 1,
                        'actions.discarded' := 0
                    }
            },
            get_rule_metrics(RuleId)
        )
    ),

    ok.

%% Smoke integration test to check that fallback action are triggered.  This Action is
%% particularly interesting for this test because it uses an internal buffer.
t_fallback_actions() ->
    [{matrix, true}].
t_fallback_actions(matrix) ->
    [[sync], [async]];
t_fallback_actions(Config) when is_list(Config) ->
    [QueryMode] = emqx_common_test_helpers:group_path(Config),
    Type = proplists:get_value(type, Config, ?TYPE),
    ConnectorName = proplists:get_value(connector_name, Config, <<"c">>),
    ConnectorConfig = proplists:get_value(
        connector_config, Config, connector_config_toxiproxy(Config)
    ),
    ActionName = atom_to_binary(?FUNCTION_NAME),
    ActionConfig1 = proplists:get_value(action_config, Config, action_config(ConnectorName)),
    RepublishTopic = <<"republish/fallback">>,
    RepublishArgs = #{
        <<"topic">> => RepublishTopic,
        <<"qos">> => 1,
        <<"retain">> => false,
        <<"payload">> => <<"${payload}">>,
        <<"mqtt_properties">> => #{},
        <<"user_properties">> => <<"${pub_props.'User-Property'}">>,
        <<"direct_dispatch">> => false
    },
    ActionConfig = emqx_bridge_v2_testlib:parse_and_check(
        action,
        Type,
        ActionName,
        emqx_utils_maps:deep_merge(
            ActionConfig1,
            #{
                <<"fallback_actions">> => [
                    #{
                        <<"kind">> => <<"republish">>,
                        <<"args">> => RepublishArgs
                    }
                ],
                <<"parameters">> => #{
                    <<"query_mode">> => atom_to_binary(QueryMode),
                    %% Simple way to make the requests fail: make the buffer overflow
                    <<"buffer">> => #{
                        <<"segment_bytes">> => <<"1B">>,
                        <<"per_partition_limit">> => <<"2B">>
                    }
                }
            }
        )
    ),
    ConnectorParams = [
        {connector_config, ConnectorConfig},
        {connector_name, ConnectorName},
        {connector_type, Type}
    ],
    ActionParams = [
        {action_config, ActionConfig},
        {action_name, ActionName},
        {action_type, Type}
    ],
    {201, _} = simplify_result(emqx_bridge_v2_testlib:create_connector_api(ConnectorParams)),
    {201, _} = simplify_result(emqx_bridge_v2_testlib:create_action_api(ActionParams)),

    RuleTopic = <<"fallback/actions">>,
    {ok, #{<<"id">> := _RuleId}} =
        emqx_bridge_v2_testlib:create_rule_and_action_http(Type, RuleTopic, [
            {bridge_name, ActionName}
        ]),

    emqx:subscribe(RepublishTopic),
    Payload = <<"aaaaaaaaaaaaaa">>,
    emqx:publish(emqx_message:make(RuleTopic, Payload)),

    ?assertReceive({deliver, RepublishTopic, #message{payload = Payload}}),

    ok.

%% Exercises the code path where we use MSK IAM authentication.
%% Unfortunately, there seems to be no good way to accurately test this as it would
%% require running this in an EC2 instance to pass, and also to have a Kafka cluster
%% configured to use MSK IAM authentication.
t_msk_iam_authn(Config) ->
    Type = proplists:get_value(type, Config, ?TYPE),
    ConnectorName = proplists:get_value(connector_name, Config, <<"c">>),
    ConnectorConfig0 = proplists:get_value(
        connector_config, Config, connector_config()
    ),
    ConnectorConfig = emqx_bridge_v2_testlib:parse_and_check_connector(
        Type,
        ConnectorName,
        emqx_utils_maps:deep_merge(
            ConnectorConfig0,
            #{<<"authentication">> => <<"msk_iam">>}
        )
    ),
    ConnectorParams = [
        {connector_config, ConnectorConfig},
        {connector_name, ConnectorName},
        {connector_type, Type}
    ],
    %% We mock the innermost call with the SASL authentication made by the OAuth plugin to
    %% try and exercise most of the code.
    mock_iam_metadata_v2_calls(),
    ok = meck:new(emqx_bridge_kafka_msk_iam_authn, [passthrough]),
    TestPid = self(),
    emqx_common_test_helpers:with_mock(
        kpro_lib,
        send_and_recv,
        fun(Req, Sock, Mod, ClientId, Timeout) ->
            case Req of
                #kpro_req{api = sasl_handshake} ->
                    #{error_code => no_error};
                #kpro_req{api = sasl_authenticate} ->
                    TestPid ! sasl_auth,
                    #{error_code => no_error, session_lifetime_ms => 2_000};
                _ ->
                    meck:passthrough([Req, Sock, Mod, ClientId, Timeout])
            end
        end,
        fun() ->
            ?assertMatch(
                {201, #{<<"status">> := <<"connected">>}},
                create_connector_api(ConnectorParams)
            ),
            %% Must have called our callback once
            ?assertMatch(
                [{_, {_, token_callback, _}, {ok, #{token := <<_/binary>>}}}],
                meck:history(emqx_bridge_kafka_msk_iam_authn)
            ),
            receive
                sasl_auth -> ok
            after 0 -> ct:fail("the impossible has happened!?")
            end,
            %% Should renew auth after according to `session_lifetime_ms`.
            ct:pal("waiting for renewal"),
            receive
                sasl_auth -> ok
            after 3_000 -> ct:fail("did not renew sasl auth")
            end,
            ?assertMatch(
                [
                    {_, {_, token_callback, _}, {ok, #{token := A}}},
                    {_, {_, token_callback, _}, {ok, #{token := B}}}
                ] when A /= B,
                meck:history(emqx_bridge_kafka_msk_iam_authn)
            )
        end
    ),
    ok.
