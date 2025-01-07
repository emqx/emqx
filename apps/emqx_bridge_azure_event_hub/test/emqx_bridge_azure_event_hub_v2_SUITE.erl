%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_azure_event_hub_v2_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-define(BRIDGE_TYPE, azure_event_hub_producer).
-define(BRIDGE_TYPE_BIN, <<"azure_event_hub_producer">>).
-define(CONNECTOR_TYPE, azure_event_hub_producer).
-define(CONNECTOR_TYPE_BIN, <<"azure_event_hub_producer">>).
-define(KAFKA_BRIDGE_TYPE, kafka_producer).

-import(emqx_common_test_helpers, [on_exit/1]).

%%------------------------------------------------------------------------------
%% CT boilerplate
%%------------------------------------------------------------------------------

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    KafkaHost = os:getenv("KAFKA_SASL_SSL_HOST", "toxiproxy.emqx.net"),
    KafkaPort = list_to_integer(os:getenv("KAFKA_SASL_SSL_PORT", "9295")),
    ProxyHost = os:getenv("PROXY_HOST", "toxiproxy"),
    ProxyPort = list_to_integer(os:getenv("PROXY_PORT", "8474")),
    ProxyName = "kafka_sasl_ssl",
    emqx_common_test_helpers:reset_proxy(ProxyHost, ProxyPort),
    case emqx_common_test_helpers:is_tcp_server_available(KafkaHost, KafkaPort) of
        true ->
            Apps = emqx_cth_suite:start(
                [
                    emqx_conf,
                    emqx,
                    emqx_management,
                    emqx_resource,
                    %% Just for test helpers
                    brod,
                    emqx_bridge_azure_event_hub,
                    emqx_bridge,
                    emqx_rule_engine,
                    {emqx_dashboard, "dashboard.listeners.http { enable = true, bind = 18083 }"}
                ],
                #{work_dir => ?config(priv_dir, Config)}
            ),
            {ok, Api} = emqx_common_test_http:create_default_app(),
            [
                {tc_apps, Apps},
                {api, Api},
                {proxy_name, ProxyName},
                {proxy_host, ProxyHost},
                {proxy_port, ProxyPort},
                {kafka_host, KafkaHost},
                {kafka_port, KafkaPort}
                | Config
            ];
        false ->
            case os:getenv("IS_CI") of
                "yes" ->
                    throw(no_kafka);
                _ ->
                    {skip, no_kafka}
            end
    end.

end_per_suite(Config) ->
    Apps = ?config(tc_apps, Config),
    emqx_cth_suite:stop(Apps),
    ok.

init_per_testcase(TestCase, Config) ->
    common_init_per_testcase(TestCase, Config).

common_init_per_testcase(TestCase, Config) ->
    ct:timetrap(timer:seconds(60)),
    emqx_bridge_v2_testlib:delete_all_bridges_and_connectors(),
    emqx_config:delete_override_conf_files(),
    UniqueNum = integer_to_binary(erlang:unique_integer()),
    Name = iolist_to_binary([atom_to_binary(TestCase), UniqueNum]),
    KafkaHost = ?config(kafka_host, Config),
    KafkaPort = ?config(kafka_port, Config),
    KafkaTopic = Name,
    ConnectorConfig = connector_config(Name, KafkaHost, KafkaPort),
    {BridgeConfig, ExtraConfig} = bridge_config(Name, Name, KafkaTopic),
    ensure_topic(Config, KafkaTopic, _Opts = #{}),
    ok = snabbkaffe:start_trace(),
    ExtraConfig ++
        [
            {connector_type, ?CONNECTOR_TYPE},
            {connector_name, Name},
            {connector_config, ConnectorConfig},
            {action_type, ?BRIDGE_TYPE},
            {action_name, Name},
            {action_config, BridgeConfig},
            {bridge_type, ?BRIDGE_TYPE},
            {bridge_name, Name},
            {bridge_config, BridgeConfig}
            | Config
        ].

end_per_testcase(_Testcase, Config) ->
    ProxyHost = ?config(proxy_host, Config),
    ProxyPort = ?config(proxy_port, Config),
    emqx_common_test_helpers:reset_proxy(ProxyHost, ProxyPort),
    emqx_bridge_v2_testlib:delete_all_bridges_and_connectors(),
    emqx_common_test_helpers:call_janitor(60_000),
    ok = snabbkaffe:stop(),
    ok.

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

connector_config(Name, KafkaHost, KafkaPort) ->
    InnerConfigMap0 =
        #{
            <<"enable">> => true,
            <<"bootstrap_hosts">> => iolist_to_binary([KafkaHost, ":", integer_to_binary(KafkaPort)]),
            <<"authentication">> =>
                #{
                    <<"mechanism">> => <<"plain">>,
                    <<"username">> => <<"emqxuser">>,
                    <<"password">> => <<"password">>
                },
            <<"connect_timeout">> => <<"5s">>,
            <<"socket_opts">> =>
                #{
                    <<"nodelay">> => true,
                    <<"recbuf">> => <<"1024KB">>,
                    <<"sndbuf">> => <<"1024KB">>,
                    <<"tcp_keepalive">> => <<"none">>
                },
            <<"ssl">> =>
                #{
                    <<"cacertfile">> => shared_secret(client_cacertfile),
                    <<"certfile">> => shared_secret(client_certfile),
                    <<"keyfile">> => shared_secret(client_keyfile),
                    <<"ciphers">> => [],
                    <<"depth">> => 10,
                    <<"enable">> => true,
                    <<"hibernate_after">> => <<"5s">>,
                    <<"log_level">> => <<"notice">>,
                    <<"reuse_sessions">> => true,
                    <<"secure_renegotiate">> => true,
                    <<"server_name_indication">> => <<"disable">>,
                    %% currently, it seems our CI kafka certs fail peer verification
                    <<"verify">> => <<"verify_none">>,
                    <<"versions">> => [<<"tlsv1.3">>, <<"tlsv1.2">>]
                }
        },
    InnerConfigMap = serde_roundtrip(InnerConfigMap0),
    parse_and_check_connector_config(InnerConfigMap, Name).

parse_and_check_connector_config(InnerConfigMap, Name) ->
    TypeBin = ?CONNECTOR_TYPE_BIN,
    RawConf = #{<<"connectors">> => #{TypeBin => #{Name => InnerConfigMap}}},
    #{<<"connectors">> := #{TypeBin := #{Name := Config}}} =
        hocon_tconf:check_plain(emqx_connector_schema, RawConf, #{
            required => false, atom_key => false
        }),
    ct:pal("parsed config: ~p", [Config]),
    InnerConfigMap.

bridge_config(Name, ConnectorId, KafkaTopic) ->
    InnerConfigMap0 =
        #{
            <<"enable">> => true,
            <<"connector">> => ConnectorId,
            <<"parameters">> =>
                #{
                    <<"buffer">> =>
                        #{
                            <<"memory_overload_protection">> => true,
                            <<"mode">> => <<"memory">>,
                            <<"per_partition_limit">> => <<"2GB">>,
                            <<"segment_bytes">> => <<"100MB">>
                        },
                    <<"compression">> => <<"no_compression">>,
                    <<"kafka_header_value_encode_mode">> => <<"none">>,
                    <<"max_batch_bytes">> => <<"896KB">>,
                    <<"max_inflight">> => <<"10">>,
                    <<"message">> =>
                        #{
                            <<"key">> => <<"${.clientid}">>,
                            <<"value">> => <<"${.}">>
                        },
                    <<"partition_count_refresh_interval">> => <<"60s">>,
                    <<"partition_strategy">> => <<"random">>,
                    <<"query_mode">> => <<"async">>,
                    <<"required_acks">> => <<"all_isr">>,
                    <<"sync_query_timeout">> => <<"5s">>,
                    <<"topic">> => KafkaTopic
                },
            <<"local_topic">> => <<"t/aeh">>
            %%,
        },
    InnerConfigMap = serde_roundtrip(InnerConfigMap0),
    ExtraConfig =
        [{kafka_topic, KafkaTopic}],
    {parse_and_check_bridge_config(InnerConfigMap, Name), ExtraConfig}.

%% check it serializes correctly
serde_roundtrip(InnerConfigMap0) ->
    IOList = hocon_pp:do(InnerConfigMap0, #{}),
    {ok, InnerConfigMap} = hocon:binary(IOList),
    InnerConfigMap.

parse_and_check_bridge_config(InnerConfigMap, Name) ->
    emqx_bridge_v2_testlib:parse_and_check(?BRIDGE_TYPE_BIN, Name, InnerConfigMap).

shared_secret_path() ->
    os:getenv("CI_SHARED_SECRET_PATH", "/var/lib/secret").

shared_secret(client_keyfile) ->
    filename:join([shared_secret_path(), "client.key"]);
shared_secret(client_certfile) ->
    filename:join([shared_secret_path(), "client.crt"]);
shared_secret(client_cacertfile) ->
    filename:join([shared_secret_path(), "ca.crt"]);
shared_secret(rig_keytab) ->
    filename:join([shared_secret_path(), "rig.keytab"]).

ensure_topic(Config, KafkaTopic, Opts) ->
    KafkaHost = ?config(kafka_host, Config),
    KafkaPort = ?config(kafka_port, Config),
    NumPartitions = maps:get(num_partitions, Opts, 3),
    Endpoints = [{KafkaHost, KafkaPort}],
    TopicConfigs = [
        #{
            name => KafkaTopic,
            num_partitions => NumPartitions,
            replication_factor => 1,
            assignments => [],
            configs => []
        }
    ],
    RequestConfig = #{timeout => 5_000},
    ConnConfig =
        #{
            ssl => emqx_tls_lib:to_client_opts(
                #{
                    keyfile => shared_secret(client_keyfile),
                    certfile => shared_secret(client_certfile),
                    cacertfile => shared_secret(client_cacertfile),
                    verify => verify_none,
                    enable => true
                }
            ),
            sasl => {plain, <<"emqxuser">>, <<"password">>}
        },
    case brod:create_topics(Endpoints, TopicConfigs, RequestConfig, ConnConfig) of
        ok -> ok;
        {error, topic_already_exists} -> ok
    end.

make_message() ->
    Time = erlang:unique_integer(),
    BinTime = integer_to_binary(Time),
    Payload = emqx_guid:to_hexstr(emqx_guid:gen()),
    #{
        clientid => BinTime,
        payload => Payload,
        timestamp => Time
    }.

bridge_api_spec_props_for_get() ->
    #{
        <<"bridge_azure_event_hub.get_producer">> :=
            #{<<"properties">> := Props}
    } =
        emqx_bridge_v2_testlib:bridges_api_spec_schemas(),
    Props.

action_api_spec_props_for_get() ->
    #{
        <<"bridge_azure_event_hub.get_bridge_v2">> :=
            #{<<"properties">> := Props}
    } =
        emqx_bridge_v2_testlib:actions_api_spec_schemas(),
    Props.

%%------------------------------------------------------------------------------
%% Testcases
%%------------------------------------------------------------------------------

t_start_stop(Config) ->
    emqx_bridge_v2_testlib:t_start_stop(Config, kafka_producer_stopped),
    ok.

t_create_via_http(Config) ->
    emqx_bridge_v2_testlib:t_create_via_http(Config),
    ok.

t_on_get_status(Config) ->
    emqx_bridge_v2_testlib:t_on_get_status(Config, #{failure_status => connecting}),
    ok.

t_sync_query(Config) ->
    ok = emqx_bridge_v2_testlib:t_sync_query(
        Config,
        fun make_message/0,
        fun(Res) -> ?assertEqual(ok, Res) end,
        emqx_bridge_kafka_impl_producer_sync_query
    ),
    ok.

t_same_name_azure_kafka_bridges(Config) ->
    BridgeName = ?config(bridge_name, Config),
    TracePoint = emqx_bridge_kafka_impl_producer_sync_query,
    %% creates the AEH bridge and check it's working
    ok = emqx_bridge_v2_testlib:t_sync_query(
        Config,
        fun make_message/0,
        fun(Res) -> ?assertEqual(ok, Res) end,
        TracePoint
    ),

    %% then create a Kafka bridge with same name and delete it after creation
    ConfigKafka0 = lists:keyreplace(action_type, 1, Config, {action_type, ?KAFKA_BRIDGE_TYPE}),
    ConfigKafka = lists:keyreplace(
        connector_type, 1, ConfigKafka0, {connector_type, ?KAFKA_BRIDGE_TYPE}
    ),
    ok = emqx_bridge_v2_testlib:t_create_via_http(ConfigKafka),

    AehResourceId = emqx_bridge_v2_testlib:resource_id(Config),
    KafkaResourceId = emqx_bridge_v2_testlib:resource_id(ConfigKafka),
    %% check that both bridges are healthy
    ?assertEqual({ok, connected}, emqx_resource_manager:health_check(AehResourceId)),
    ?assertEqual({ok, connected}, emqx_resource_manager:health_check(KafkaResourceId)),
    ?assertMatch(
        {{ok, _}, {ok, _}},
        ?wait_async_action(
            emqx_connector:disable_enable(disable, ?KAFKA_BRIDGE_TYPE, BridgeName),
            #{?snk_kind := kafka_producer_stopped},
            5_000
        )
    ),
    % check that AEH bridge is still working
    ?check_trace(
        begin
            BridgeId = emqx_bridge_v2_testlib:bridge_id(Config),
            Message = {BridgeId, make_message()},
            ?assertEqual(ok, emqx_resource:simple_sync_query(AehResourceId, Message)),
            ok
        end,
        fun(Trace) ->
            ?assertMatch([#{instance_id := AehResourceId}], ?of_kind(TracePoint, Trace))
        end
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

t_http_api_get(Config) ->
    ?assertMatch({ok, _}, emqx_bridge_v2_testlib:create_bridge(Config)),
    %% v1 api; no mangling of configs; has `kafka' top level config key
    ?assertMatch(
        {ok, {{_, 200, _}, _, [#{<<"kafka">> := _}]}},
        emqx_bridge_testlib:list_bridges_api()
    ),
    ok.

t_multiple_actions_sharing_topic(Config) ->
    ActionConfig0 = ?config(action_config, Config),
    ActionConfig =
        emqx_utils_maps:deep_merge(
            ActionConfig0,
            #{<<"parameters">> => #{<<"query_mode">> => <<"sync">>}}
        ),
    ok =
        emqx_bridge_v2_kafka_producer_SUITE:?FUNCTION_NAME(
            [
                {type, ?BRIDGE_TYPE_BIN},
                {connector_name, ?config(connector_name, Config)},
                {connector_config, ?config(connector_config, Config)},
                {action_config, ActionConfig}
            ]
        ),
    ok.

t_dynamic_topics(Config) ->
    ActionConfig0 = ?config(action_config, Config),
    ActionConfig =
        emqx_utils_maps:deep_merge(
            ActionConfig0,
            #{<<"parameters">> => #{<<"query_mode">> => <<"sync">>}}
        ),
    ok =
        emqx_bridge_v2_kafka_producer_SUITE:?FUNCTION_NAME(
            [
                {type, ?BRIDGE_TYPE_BIN},
                {connector_name, ?config(connector_name, Config)},
                {connector_config, ?config(connector_config, Config)},
                {action_config, ActionConfig}
            ]
        ),
    ok.

t_disallow_disk_mode_for_dynamic_topic(Config) ->
    ActionConfig = ?config(action_config, Config),
    ok =
        emqx_bridge_v2_kafka_producer_SUITE:?FUNCTION_NAME(
            [
                {type, ?BRIDGE_TYPE_BIN},
                {connector_name, ?config(connector_name, Config)},
                {connector_config, ?config(connector_config, Config)},
                {action_config, ActionConfig}
            ]
        ),
    ok.
