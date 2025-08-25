%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_kafka_action_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("emqx/include/asserts.hrl").
-include_lib("emqx_resource/include/emqx_resource.hrl").
-include_lib("kafka_protocol/include/kpro.hrl").
-include_lib("emqx/include/emqx_config.hrl").
-include_lib("emqx_utils/include/emqx_message.hrl").

%%------------------------------------------------------------------------------
%% Defs
%%------------------------------------------------------------------------------

-import(emqx_common_test_helpers, [on_exit/1]).

-define(CONNECTOR_TYPE, kafka_producer).
-define(CONNECTOR_TYPE_BIN, <<"kafka_producer">>).
-define(ACTION_TYPE, kafka_producer).
-define(ACTION_TYPE_BIN, <<"kafka_producer">>).

-define(PROXY_HOST, "toxiproxy").
-define(PROXY_PORT, 8474).

-define(tcp_plain, tcp_plain).
-define(tcp_sasl, tcp_sasl).
-define(tls_plain, tls_plain).
-define(tls_sasl, tls_sasl).

-define(no_auth, no_auth).
-define(plain_auth, plain_auth).
-define(scram_sha256, scram_sha256).
-define(scram_sha512, scram_sha512).
-define(kerberos, kerberos).

-define(key_dispatch, key_dispatch).
-define(random_dispatch, random_dispatch).

-define(sync, sync).
-define(async, async).

-define(TELEMETRY_PREFIX, emqx, resource).

%%------------------------------------------------------------------------------
%% CT boilerplate
%%------------------------------------------------------------------------------

all() ->
    emqx_common_test_helpers:all_with_matrix(?MODULE).

groups() ->
    emqx_common_test_helpers:groups_with_matrix(?MODULE).

init_per_suite(TCConfig) ->
    reset_proxy(),
    Apps = emqx_cth_suite:start(
        [
            emqx,
            emqx_conf,
            emqx_bridge_kafka,
            emqx_bridge,
            emqx_rule_engine,
            emqx_management,
            emqx_mgmt_api_test_util:emqx_dashboard()
        ],
        #{work_dir => emqx_cth_suite:work_dir(TCConfig)}
    ),
    emqx_bridge_kafka_testlib:wait_until_kafka_is_up(),
    [
        {apps, Apps}
        | TCConfig
    ].

end_per_suite(TCConfig) ->
    Apps = ?config(apps, TCConfig),
    emqx_cth_suite:stop(Apps),
    reset_proxy(),
    ok.

init_per_group(?tcp_plain, TCConfig) ->
    Host = os:getenv("KAFKA_PLAIN_HOST", "toxiproxy.emqx.net"),
    Port = list_to_integer(os:getenv("KAFKA_PLAIN_PORT", "9292")),
    [
        {kafka_host, Host},
        {kafka_port, Port},
        {enable_tls, false},
        {proxy_name, "kafka_plain"},
        {proxy_name_2, "kafka_2_plain"},
        {proxy_host, ?PROXY_HOST},
        {proxy_port, ?PROXY_PORT}
        | TCConfig
    ];
init_per_group(?tcp_sasl, TCConfig) ->
    Host = os:getenv("KAFKA_SASL_PLAIN_HOST", "toxiproxy.emqx.net"),
    Port = list_to_integer(os:getenv("KAFKA_SASL_PORT", "9293")),
    [
        {kafka_host, Host},
        {kafka_port, Port},
        {enable_tls, false},
        {tls_config, no_tls()}
        | TCConfig
    ];
init_per_group(?tls_plain, TCConfig) ->
    Host = os:getenv("KAFKA_SSL_HOST", "toxiproxy.emqx.net"),
    Port = list_to_integer(os:getenv("KAFKA_SSL_PORT", "9294")),
    [
        {kafka_host, Host},
        {kafka_port, Port},
        {enable_tls, true},
        {tls_config, emqx_bridge_kafka_testlib:valid_ssl_settings()}
        | TCConfig
    ];
init_per_group(?tls_sasl, TCConfig) ->
    Host = os:getenv("KAFKA_SSL_SASL_HOST", "toxiproxy.emqx.net"),
    Port = list_to_integer(os:getenv("KAFKA_SSL_SASL_PORT", "9295")),
    [
        {kafka_host, Host},
        {kafka_port, Port},
        {enable_tls, true},
        {tls_config, emqx_bridge_kafka_testlib:valid_ssl_settings()}
        | TCConfig
    ];
init_per_group(?no_auth, TCConfig) ->
    [{auth, no_auth()} | TCConfig];
init_per_group(?plain_auth, TCConfig) ->
    [{auth, plain_auth()} | TCConfig];
init_per_group(?scram_sha256, TCConfig) ->
    [{auth, scram_sha256_auth()} | TCConfig];
init_per_group(?scram_sha512, TCConfig) ->
    [{auth, scram_sha512_auth()} | TCConfig];
init_per_group(?kerberos, TCConfig) ->
    %% Kerberos auth doesn't work with toxiproxy
    %% If one tries, errors like:
    %% ```
    %% SASL(-1): generic failure: GSSAPI Error: Unspecified GSS failure.  Minor code may
    %% provide more information (Server krbtgt/EMQX.NET@KDC.EMQX.NET not found in Kerberos
    %% database)
    %% ```
    %% ... may appear
    Host = "kafka-1.emqx.net",
    Port = 9095,
    [
        {kafka_host, Host},
        {kafka_port, Port},
        {enable_tls, true},
        {tls_config, emqx_bridge_kafka_testlib:valid_ssl_settings()},
        {auth, kerberos_auth()}
        | TCConfig
    ];
init_per_group(?key_dispatch, TCConfig) ->
    [{partition_strategy, <<"key_dispatch">>} | TCConfig];
init_per_group(?random_dispatch, TCConfig) ->
    [{partition_strategy, <<"random">>} | TCConfig];
init_per_group(?sync, TCConfig) ->
    [{query_mode, sync} | TCConfig];
init_per_group(?async, TCConfig) ->
    [{query_mode, async} | TCConfig];
init_per_group(_Group, TCConfig) ->
    TCConfig.

end_per_group(_Group, _TCConfig) ->
    ok.

init_per_testcase(TestCase, TCConfig) ->
    reset_proxy(),
    Path = group_path(TCConfig, no_groups),
    ct:pal(asciiart:visible($%, "~p - ~s", [Path, TestCase])),
    ConnectorName = atom_to_binary(TestCase),
    ConnectorConfig = connector_config(#{
        <<"authentication">> => get_config(auth, TCConfig, no_auth()),
        <<"bootstrap_hosts">> => bootstrap_hosts_of(TCConfig),
        <<"ssl">> => get_config(tls_config, TCConfig, no_tls())
    }),
    ActionName = ConnectorName,
    Topic = ActionName,
    ActionConfig = action_config(#{
        <<"connector">> => ConnectorName,
        <<"parameters">> => #{
            <<"topic">> => Topic,
            <<"partition_strategy">> => get_config(partition_strategy, TCConfig, <<"random">>),
            <<"query_mode">> => bin(get_config(query_mode, TCConfig, <<"sync">>))
        }
    }),
    on_exit(fun() -> delete_kafka_topic(Topic) end),
    ensure_kafka_topic(Topic),
    snabbkaffe:start_trace(),
    [
        {bridge_kind, action},
        {connector_type, ?CONNECTOR_TYPE},
        {connector_name, ConnectorName},
        {connector_config, ConnectorConfig},
        {action_type, ?ACTION_TYPE},
        {action_name, ActionName},
        {action_config, ActionConfig}
        | TCConfig
    ].

end_per_testcase(_TestCase, _TCConfig) ->
    snabbkaffe:stop(),
    emqx_bridge_v2_testlib:delete_all_rules(),
    emqx_bridge_v2_testlib:delete_all_bridges_and_connectors(),
    emqx_common_test_helpers:call_janitor(),
    ok.

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

bootstrap_hosts_of(TCConfig) ->
    Host = get_config(kafka_host, TCConfig, <<"toxiproxy.emqx.net">>),
    Port = get_config(kafka_port, TCConfig, 9292),
    emqx_bridge_v2_testlib:fmt(<<"${h}:${p}">>, #{h => Host, p => Port}).

no_tls() ->
    #{<<"enable">> => false}.

connector_config(Overrides) ->
    Defaults = #{
        <<"enable">> => true,
        <<"description">> => <<"my connector">>,
        <<"tags">> => [<<"some">>, <<"tags">>],
        <<"bootstrap_hosts">> => <<"kafka-1.emqx.net:9092">>,
        <<"connect_timeout">> => <<"5s">>,
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
            },
        <<"resource_opts">> =>
            emqx_bridge_v2_testlib:common_connector_resource_opts()
    },
    InnerConfigMap = emqx_utils_maps:deep_merge(Defaults, Overrides),
    emqx_bridge_v2_testlib:parse_and_check_connector(?CONNECTOR_TYPE_BIN, <<"x">>, InnerConfigMap).

action_config(Overrides) ->
    Defaults = #{
        <<"enable">> => true,
        <<"description">> => <<"my action">>,
        <<"tags">> => [<<"some">>, <<"tags">>],
        <<"parameters">> => #{
            <<"buffer">> => #{
                <<"memory_overload_protection">> => false,
                <<"mode">> => <<"memory">>,
                <<"per_partition_limit">> => <<"2GB">>,
                <<"segment_bytes">> => <<"100MB">>
            },
            <<"compression">> => <<"no_compression">>,
            <<"kafka_header_value_encode_mode">> => <<"json">>,
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
            <<"topic">> => <<"test-topic-one-partition">>
        },
        <<"resource_opts">> =>
            maps:without(
                [
                    <<"batch_size">>,
                    <<"batch_time">>,
                    <<"buffer_mode">>,
                    <<"buffer_seg_bytes">>,
                    <<"health_check_interval_jitter">>,
                    <<"inflight_window">>,
                    <<"max_buffer_bytes">>,
                    <<"metrics_flush_interval">>,
                    <<"query_mode">>,
                    <<"request_ttl">>,
                    <<"resume_interval">>,
                    <<"worker_pool_size">>
                ],
                emqx_bridge_v2_testlib:common_action_resource_opts()
            )
    },
    InnerConfigMap = emqx_utils_maps:deep_merge(Defaults, Overrides),
    emqx_bridge_v2_testlib:parse_and_check(action, ?ACTION_TYPE_BIN, <<"x">>, InnerConfigMap).

no_auth() ->
    emqx_bridge_kafka_testlib:no_auth().

plain_auth() ->
    emqx_bridge_kafka_testlib:plain_auth().

scram_sha256_auth() ->
    emqx_bridge_kafka_testlib:scram_sha256_auth().

scram_sha512_auth() ->
    emqx_bridge_kafka_testlib:scram_sha512_auth().

kerberos_auth() ->
    emqx_bridge_kafka_testlib:kerberos_auth().

msk_iam_auth() ->
    emqx_bridge_kafka_testlib:msk_iam_auth().

get_config(K, TCConfig) -> emqx_bridge_v2_testlib:get_value(K, TCConfig).
get_config(K, TCConfig, Default) -> proplists:get_value(K, TCConfig, Default).

bin(X) -> emqx_utils_conv:bin(X).

group_path(TCConfig, Default) ->
    case emqx_common_test_helpers:group_path(TCConfig) of
        [] -> Default;
        Path -> Path
    end.

get_tc_prop(TestCase, Key, Default) ->
    maybe
        true ?= erlang:function_exported(?MODULE, TestCase, 0),
        {Key, Val} ?= proplists:lookup(Key, ?MODULE:TestCase()),
        Val
    else
        _ -> Default
    end.

reset_proxy() ->
    emqx_common_test_helpers:reset_proxy(?PROXY_HOST, ?PROXY_PORT).

%% N.B.: Only works with plain TCP endpoint
with_brokers_down(Config, Fun) ->
    ProxyName1 = get_config(proxy_name, Config),
    ProxyName2 = get_config(proxy_name_2, Config),
    ProxyHost = get_config(proxy_host, Config),
    ProxyPort = get_config(proxy_port, Config),
    emqx_common_test_helpers:with_failure(
        down, ProxyName1, ProxyHost, ProxyPort, fun() ->
            emqx_common_test_helpers:with_failure(down, ProxyName2, ProxyHost, ProxyPort, Fun)
        end
    ).

kafka_hosts_direct() ->
    kpro:parse_endpoints("kafka-1.emqx.net:9092").

ensure_kafka_topic(KafkaTopic) ->
    emqx_bridge_kafka_testlib:ensure_kafka_topic(KafkaTopic).

delete_kafka_topic(KafkaTopic) ->
    emqx_bridge_kafka_testlib:delete_kafka_topic(KafkaTopic).

resolve_kafka_offset(TCConfig, Opts) ->
    Topic = emqx_utils_maps:get_lazy(topic, Opts, fun() ->
        #{<<"parameters">> := #{<<"topic">> := Topic}} =
            get_config(action_config, TCConfig),
        Topic
    end),
    resolve_kafka_offset(kafka_hosts_direct(), Topic, _Partition = 0).

resolve_kafka_offset(TCConfig) when is_list(TCConfig) ->
    resolve_kafka_offset(TCConfig, _Opts = #{}).

resolve_kafka_offset(Hosts, Topic, Partition) ->
    brod:resolve_offset(Hosts, Topic, Partition, latest).

get_kafka_messages(Opts, TCConfig) ->
    #{<<"parameters">> := #{<<"topic">> := Topic}} =
        get_config(action_config, TCConfig),
    #{offset := Offset} = Opts,
    Partition = maps:get(partition, Opts, 0),
    Hosts = kafka_hosts_direct(),
    maybe
        {ok, {NewOffset, Msgs0}} ?= brod:fetch(Hosts, Topic, Partition, Offset),
        Msgs = lists:map(fun kpro_message_to_map/1, Msgs0),
        {ok, {NewOffset, Msgs}}
    end.

kpro_message_to_map(#kafka_message{} = Msg) ->
    lists:foldl(
        fun({I, Name}, Acc) ->
            Acc#{Name => element(I, Msg)}
        end,
        #{},
        lists:enumerate(2, record_info(fields, kafka_message))
    ).

check_kafka_message_payload(KafkaTopic, Offset, ExpectedPayload) ->
    Partition = 0,
    Hosts = kafka_hosts_direct(),
    {ok, {_, [KafkaMsg0]}} = brod:fetch(Hosts, KafkaTopic, Partition, Offset),
    ?assertMatch(
        #{value := ExpectedPayload},
        kpro_message_to_map(KafkaMsg0),
        #{expected_payload => ExpectedPayload}
    ).

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

create_connector_api(TCConfig, Overrides) ->
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:create_connector_api(TCConfig, Overrides)
    ).

update_connector_api(TCConfig, Overrides) ->
    #{
        connector_type := Type,
        connector_name := Name,
        connector_config := Cfg0
    } =
        emqx_bridge_v2_testlib:get_common_values_with_configs(TCConfig),
    Cfg = emqx_utils_maps:deep_merge(Cfg0, Overrides),
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:update_connector_api(Name, Type, Cfg)
    ).

get_action_metrics_api(TCConfig) ->
    emqx_bridge_v2_testlib:get_action_metrics_api(TCConfig).

create_action_api(TCConfig, Overrides) ->
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:create_action_api(TCConfig, Overrides)
    ).

get_action_api(TCConfig) ->
    emqx_bridge_v2_testlib:get_action_api2(TCConfig).

delete_action_api(TCConfig) ->
    #{kind := Kind, type := Type, name := Name} =
        emqx_bridge_v2_testlib:get_common_values(TCConfig),
    emqx_bridge_v2_testlib:delete_kind_api(Kind, Type, Name).

assert_status_api(Line, Name, Status, TCConfig) ->
    ?assertMatch(
        {200, #{
            <<"status">> := Status,
            <<"node_status">> := [#{<<"status">> := Status}]
        }},
        get_action_api([{action_name, Name} | TCConfig]),
        #{line => Line, name => Name, expected_status => Status}
    ).
-define(assertStatusAPI(NAME, STATUS, TCCONFIG), assert_status_api(?LINE, NAME, STATUS, TCCONFIG)).

update_action_api(Config, Overrides) ->
    emqx_bridge_v2_testlib:update_bridge_api2(Config, Overrides).

disable_action_api(TCConfig) ->
    #{kind := Kind, type := Type, name := Name} =
        emqx_bridge_v2_testlib:get_common_values(TCConfig),
    emqx_bridge_v2_testlib:disable_kind_api(Kind, Type, Name).

enable_action_api(TCConfig) ->
    #{kind := Kind, type := Type, name := Name} =
        emqx_bridge_v2_testlib:get_common_values(TCConfig),
    emqx_bridge_v2_testlib:enable_kind_api(Kind, Type, Name).

simple_create_rule_api(TCConfig) ->
    emqx_bridge_v2_testlib:simple_create_rule_api(TCConfig).

get_rule_metrics(RuleId) ->
    emqx_bridge_v2_testlib:get_rule_metrics(RuleId).

reset_rule_metrics(RuleId) ->
    emqx_metrics_worker:reset_metrics(rule_metrics, RuleId).

start_client() ->
    start_client(_Opts = #{}).

start_client(Opts0) ->
    Opts = maps:merge(#{proto_ver => v5}, Opts0),
    {ok, C} = emqtt:start_link(Opts),
    on_exit(fun() -> emqtt:stop(C) end),
    {ok, _} = emqtt:connect(C),
    C.

unique_payload() ->
    integer_to_binary(erlang:unique_integer()).

mock_iam_metadata_v2_calls() ->
    on_exit(fun meck:unload/0),
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

full_matrix() ->
    [
        [Conn, Auth, Sync, Partition]
     || [Conn, Auth] <- conn_matrix(),
        Sync <- [?sync, ?async],
        Partition <- [?key_dispatch, ?random_dispatch]
    ].

conn_matrix() ->
    [
        [?tcp_plain, ?no_auth],
        [?tcp_sasl, ?plain_auth],
        [?tls_plain, ?no_auth],
        [?tls_sasl, ?plain_auth],
        [?tls_sasl, ?scram_sha256],
        [?tls_sasl, ?scram_sha512],
        [?tls_sasl, ?kerberos]
    ].

inspect(X) ->
    iolist_to_binary(io_lib:format("~p", [X])).

%%------------------------------------------------------------------------------
%% Test cases
%%------------------------------------------------------------------------------

t_start_stop(TCConfig) when is_list(TCConfig) ->
    emqx_bridge_v2_testlib:t_start_stop(TCConfig, kafka_producer_stopped).

t_on_get_status() ->
    [{matrix, true}].
t_on_get_status(matrix) ->
    [[?tcp_plain, ?no_auth]];
t_on_get_status(TCConfig) when is_list(TCConfig) ->
    emqx_bridge_v2_testlib:t_on_get_status(TCConfig, #{failure_status => ?status_connecting}).

t_rule_action() ->
    [{matrix, true}].
t_rule_action(matrix) ->
    [
        [?tcp_plain, ?no_auth, ?sync, ?key_dispatch],
        [?tcp_plain, ?no_auth, ?sync, ?random_dispatch],
        [?tcp_plain, ?no_auth, ?async, ?random_dispatch],
        [?tcp_sasl, ?plain_auth, ?sync, ?random_dispatch],
        [?tls_plain, ?no_auth, ?sync, ?random_dispatch],
        [?tls_sasl, ?plain_auth, ?sync, ?random_dispatch],
        [?tls_sasl, ?scram_sha256, ?sync, ?random_dispatch],
        [?tls_sasl, ?scram_sha512, ?sync, ?random_dispatch],
        [?tls_sasl, ?kerberos, ?sync, ?random_dispatch]
    ];
t_rule_action(TCConfig) when is_list(TCConfig) ->
    #{<<"parameters">> := #{<<"topic">> := KafkaTopic}} =
        get_config(action_config, TCConfig),
    PrePublishFn = fun(Context) ->
        {ok, Offset} = resolve_kafka_offset(kafka_hosts_direct(), KafkaTopic, _Partition = 0),
        Context#{offset => Offset}
    end,
    PostPublishFn = fun(Context) ->
        #{offset := Offset, payload := Payload} = Context,
        check_kafka_message_payload(KafkaTopic, Offset, Payload)
    end,
    Opts = #{
        pre_publish_fn => PrePublishFn,
        post_publish_fn => PostPublishFn
    },
    emqx_bridge_v2_testlib:t_rule_action(TCConfig, Opts).

t_smoke_metrics(TCConfig) ->
    {201, _} = create_connector_api(TCConfig, #{}),
    {201, _} = create_action_api(TCConfig, #{}),
    #{id := RuleId, topic := Topic} = simple_create_rule_api(TCConfig),
    C = start_client(),
    %% counters should be empty before
    ?assertMatch(
        {200, #{
            <<"metrics">> := #{
                <<"matched">> := 0,
                <<"success">> := 0,
                <<"dropped">> := 0,
                <<"failed">> := 0,
                <<"inflight">> := 0,
                <<"queuing">> := 0,
                <<"dropped.other">> := 0,
                <<"dropped.queue_full">> := 0,
                <<"dropped.resource_not_found">> := 0,
                <<"dropped.resource_stopped">> := 0,
                <<"retried">> := 0
            }
        }},
        get_action_metrics_api(TCConfig)
    ),
    emqtt:publish(C, Topic, <<"hey">>),
    ?retry(
        200,
        10,
        ?assertMatch(
            {200, #{
                <<"metrics">> := #{
                    <<"matched">> := 1,
                    <<"success">> := 1,
                    <<"dropped">> := 0,
                    <<"failed">> := 0,
                    <<"inflight">> := 0,
                    <<"queuing">> := 0,
                    <<"dropped.other">> := 0,
                    <<"dropped.queue_full">> := 0,
                    <<"dropped.resource_not_found">> := 0,
                    <<"dropped.resource_stopped">> := 0,
                    <<"retried">> := 0
                }
            }},
            get_action_metrics_api(TCConfig)
        )
    ),
    ?retry(
        200,
        10,
        ?assertMatch(
            #{
                counters := #{
                    'matched' := 1,
                    'failed' := 0,
                    'passed' := 1,
                    'actions.success' := 1,
                    'actions.failed' := 0,
                    'actions.discarded' := 0
                }
            },
            get_rule_metrics(RuleId)
        )
    ),
    %% Success counter should be reset
    {204, _} = disable_action_api(TCConfig),
    ?retry(
        200,
        10,
        ?assertMatch(
            {200, #{
                <<"metrics">> := #{
                    <<"matched">> := 0,
                    <<"success">> := 0
                }
            }},
            get_action_metrics_api(TCConfig)
        )
    ),
    ?retry(
        200,
        10,
        ?assertMatch(
            #{
                counters := #{
                    'matched' := 1,
                    'failed' := 0,
                    'passed' := 1
                }
            },
            get_rule_metrics(RuleId)
        )
    ),
    emqtt:publish(C, Topic, <<"hey">>),
    ?retry(
        200,
        10,
        ?assertMatch(
            {200, #{
                <<"metrics">> := #{
                    <<"success">> := 0
                }
            }},
            get_action_metrics_api(TCConfig)
        )
    ),
    ?retry(
        200,
        10,
        ?assertMatch(
            #{
                counters := #{
                    'actions.success' := 1,
                    'actions.failed' := 0,
                    'actions.discarded' := 1
                }
            },
            get_rule_metrics(RuleId)
        )
    ),
    %% Success counter should increase after reenabling and publishing
    {204, _} = enable_action_api(TCConfig),
    ?retry(
        200,
        10,
        ?assertMatch(
            {200, #{
                <<"metrics">> := #{
                    <<"success">> := 0
                }
            }},
            get_action_metrics_api(TCConfig)
        )
    ),
    emqtt:publish(C, Topic, <<"hey">>),
    ?retry(
        200,
        10,
        ?assertMatch(
            {200, #{
                <<"metrics">> := #{
                    <<"success">> := 1
                }
            }},
            get_action_metrics_api(TCConfig)
        )
    ),
    ?retry(
        200,
        10,
        ?assertMatch(
            #{
                counters := #{
                    'actions.success' := 2
                }
            },
            get_rule_metrics(RuleId)
        )
    ),
    ok.

t_custom_timestamp(TCConfig) ->
    {201, _} = create_connector_api(TCConfig, #{}),
    {201, _} = create_action_api(TCConfig, #{
        <<"parameters">> => #{
            <<"message">> => #{<<"timestamp">> => <<"123">>}
        }
    }),
    #{topic := Topic} = simple_create_rule_api(TCConfig),
    C = start_client(),
    {ok, Offset} = resolve_kafka_offset(TCConfig),
    emqtt:publish(C, Topic, <<"hey">>),
    ?retry(
        200,
        10,
        ?assertMatch(
            {ok, {_, [#{ts := 123, ts_type := create}]}},
            get_kafka_messages(#{offset => Offset}, TCConfig)
        )
    ),
    ok.

%% Need to stop the already running client; otherwise, the
%% next `on_start' call will try to ensure the client
%% exists and it will.  This is specially bad if the
%% original crash was due to misconfiguration and we are
%% trying to fix it...
t_failed_creation_then_fix() ->
    [{matrix, true}].
t_failed_creation_then_fix(matrix) ->
    [[?tcp_sasl, ?plain_auth]];
t_failed_creation_then_fix(TCConfig) ->
    %% creates, but fails to start producers
    ?assertMatch(
        {201, #{<<"status">> := <<"disconnected">>}},
        create_connector_api(TCConfig, #{
            <<"authentication">> => #{<<"password">> => <<"wrong">>}
        })
    ),
    %% before throwing, it should cleanup the client process.  we
    %% retry because the supervisor might need some time to really
    %% remove it from its tree.
    ?retry(
        _Sleep0 = 50,
        _Attempts0 = 10,
        ?assertEqual([], supervisor:which_children(wolff_producers_sup))
    ),
    %% must succeed with correct config
    ?assertMatch(
        {200, #{<<"status">> := <<"connected">>}},
        update_connector_api(TCConfig, #{})
    ),
    {201, _} = create_action_api(TCConfig, #{
        <<"parameters">> => #{
            <<"message">> => #{<<"value">> => <<"${.payload}">>}
        }
    }),
    #{topic := Topic} = simple_create_rule_api(TCConfig),
    C = start_client(),
    {ok, Offset} = resolve_kafka_offset(TCConfig),
    Payload = unique_payload(),
    emqtt:publish(C, Topic, Payload),
    ?retry(
        200,
        10,
        ?assertMatch(
            {ok, {_, [#{value := Payload}]}},
            get_kafka_messages(#{offset => Offset}, TCConfig)
        )
    ),
    ok.

t_nonexistent_topic(TCConfig) ->
    {201, _} = create_connector_api(TCConfig, #{}),
    ExpectedMsg = inspect(
        {unhealthy_target, <<"Unknown topic or partition: undefined-test-topic">>}
    ),
    ?assertMatch(
        {201, #{
            <<"status">> := <<"disconnected">>,
            <<"status_reason">> := ExpectedMsg
        }},
        create_action_api(TCConfig, #{
            <<"parameters">> => #{
                <<"topic">> => <<"undefined-test-topic">>
            }
        }),
        #{expected => ExpectedMsg}
    ),
    #{topic := Topic} = simple_create_rule_api(TCConfig),
    C = start_client(),
    emqtt:publish(C, Topic, <<"hey">>),
    ?retry(
        200,
        10,
        ?assertMatch(
            {200, #{
                <<"metrics">> := #{
                    <<"matched">> := 1,
                    <<"success">> := 0,
                    <<"dropped">> := 1,
                    <<"dropped.resource_stopped">> := 1
                }
            }},
            get_action_metrics_api(TCConfig)
        )
    ),
    ok.

t_send_message_with_headers() ->
    [{matrix, true}].
t_send_message_with_headers(matrix) ->
    [[?sync], [?async]];
t_send_message_with_headers(TCConfig) ->
    {201, _} = create_connector_api(TCConfig, #{}),
    {201, _} = create_action_api(TCConfig, #{
        <<"parameters">> => #{
            <<"message">> => #{<<"key">> => <<"${payload.key}">>},
            <<"kafka_headers">> => <<"${payload.header}">>,
            <<"kafka_ext_headers">> => [
                #{
                    <<"kafka_ext_header_key">> => <<"clientid">>,
                    <<"kafka_ext_header_value">> => <<"${clientid}">>
                },
                #{
                    <<"kafka_ext_header_key">> => <<"ext_header_val">>,
                    <<"kafka_ext_header_value">> => <<"${payload.ext_header_val}">>
                }
            ]
        }
    }),
    #{topic := Topic} = simple_create_rule_api(TCConfig),
    ClientId = atom_to_binary(?FUNCTION_NAME),
    C = start_client(#{clientid => ClientId}),

    Key1 = unique_payload(),
    Payload1 = emqx_utils_json:encode(
        #{
            <<"key">> => Key1,
            <<"header">> => #{
                <<"foo">> => <<"bar">>
            },
            <<"ext_header_val">> => <<"ext header ok">>
        }
    ),
    Key2 = unique_payload(),
    Payload2 = emqx_utils_json:encode(
        #{
            <<"key">> => Key2,
            <<"header">> => [
                #{
                    <<"key">> => <<"foo1">>,
                    <<"value">> => <<"bar1">>
                },
                #{
                    <<"key">> => <<"foo2">>,
                    <<"value">> => <<"bar2">>
                }
            ],
            <<"ext_header_val">> => <<"ext header ok">>
        }
    ),
    {ok, Offset} = resolve_kafka_offset(TCConfig),
    ct:pal("base offset before testing ~p", [Offset]),
    emqtt:publish(C, Topic, Payload1, [{qos, 1}]),
    emqtt:publish(C, Topic, Payload2, [{qos, 1}]),
    ?retry(
        200,
        10,
        ?assertMatch(
            {ok,
                {_, [
                    #{
                        headers := [
                            {<<"foo">>, <<"\"bar\"">>},
                            {<<"clientid">>, _},
                            {<<"ext_header_val">>, <<"\"ext header ok\"">>}
                        ],
                        key := Key1
                    },
                    #{
                        headers := [
                            {<<"foo1">>, <<"\"bar1\"">>},
                            {<<"foo2">>, <<"\"bar2\"">>},
                            {<<"clientid">>, _},
                            {<<"ext_header_val">>, <<"\"ext header ok\"">>}
                        ],
                        key := Key2
                    }
                ]}},
            get_kafka_messages(#{offset => Offset}, TCConfig)
        )
    ),
    ok.

t_wrong_headers(TCConfig) ->
    {201, _} = create_connector_api(TCConfig, #{}),
    ?assertMatch(
        {400, #{
            <<"message">> := #{
                <<"kind">> := <<"validation_error">>,
                <<"reason">> :=
                    <<"The 'kafka_headers' must be a single placeholder like ${pub_props}">>
            }
        }},
        create_action_api(TCConfig, #{
            <<"parameters">> => #{
                <<"kafka_headers">> => <<"wrong_header">>
            }
        })
    ),
    ?assertMatch(
        {400, #{
            <<"message">> := #{
                <<"kind">> := <<"validation_error">>,
                <<"reason">> :=
                    <<
                        "The value of 'kafka_ext_headers' must either be a "
                        "single placeholder like ${foo}, or a simple string."
                    >>
            }
        }},
        create_action_api(TCConfig, #{
            <<"parameters">> => #{
                <<"kafka_ext_headers">> => [
                    #{
                        <<"kafka_ext_header_key">> => <<"clientid">>,
                        <<"kafka_ext_header_value">> => <<"wrong ${header}">>
                    }
                ]
            }
        })
    ),
    ok.

t_wrong_headers_from_message() ->
    [{matrix, true}].
t_wrong_headers_from_message(matrix) ->
    [[?sync], [?async]];
t_wrong_headers_from_message(TCConfig) ->
    {201, _} = create_connector_api(TCConfig, #{}),
    {201, _} = create_action_api(TCConfig, #{
        <<"parameters">> => #{
            <<"kafka_headers">> => <<"${payload}">>
        }
    }),
    #{topic := Topic} = simple_create_rule_api(TCConfig),
    ClientId = atom_to_binary(?FUNCTION_NAME),
    C = start_client(#{clientid => ClientId}),
    Payload1 = <<"wrong_header">>,
    {_, {ok, _}} =
        snabbkaffe:wait_async_action(
            fun() -> emqtt:publish(C, Topic, Payload1, [{qos, 1}]) end,
            ?match_event(
                #{?snk_kind := K} when
                    K == emqx_bridge_kafka_impl_producer_sync_query_failed orelse
                        K == emqx_bridge_kafka_impl_producer_async_query_failed
            ),
            10_000
        ),
    Payload2 = <<"[{\"foo\":\"bar\"}, {\"foo2\":\"bar2\"}]">>,
    {_, {ok, _}} =
        snabbkaffe:wait_async_action(
            fun() -> emqtt:publish(C, Topic, Payload2, [{qos, 1}]) end,
            ?match_event(
                #{?snk_kind := K} when
                    K == emqx_bridge_kafka_impl_producer_sync_query_failed orelse
                        K == emqx_bridge_kafka_impl_producer_async_query_failed
            ),
            10_000
        ),
    Payload3 = <<"[{\"key\":\"foo\"}, {\"value\":\"bar\"}]">>,
    {_, {ok, _}} =
        snabbkaffe:wait_async_action(
            fun() -> emqtt:publish(C, Topic, Payload3, [{qos, 1}]) end,
            ?match_event(
                #{?snk_kind := K} when
                    K == emqx_bridge_kafka_impl_producer_sync_query_failed orelse
                        K == emqx_bridge_kafka_impl_producer_async_query_failed
            ),
            10_000
        ),
    ok.

t_message_too_large(TCConfig) ->
    {201, _} = create_connector_api(TCConfig, #{}),
    {201, _} = create_action_api(TCConfig, #{
        <<"parameters">> => #{
            <<"topic">> => <<"max-100-bytes">>
        }
    }),
    #{topic := Topic} = simple_create_rule_api(TCConfig),
    C = start_client(),
    TooLargePayload = iolist_to_binary(lists:duplicate(100, 100)),
    emqtt:publish(C, Topic, TooLargePayload, [{qos, 1}]),
    ?retry(
        200,
        10,
        ?assertMatch(
            {200, #{
                <<"metrics">> := #{
                    <<"failed">> := 1
                }
            }},
            get_action_metrics_api(TCConfig)
        )
    ),
    ok.

t_bad_url(TCConfig) ->
    BadHost = <<"bad_host:9092">>,
    ExpectedMsg = inspect([#{reason => unresolvable_hostname, host => BadHost}]),
    ?assertMatch(
        {201, #{
            <<"status">> := <<"disconnected">>,
            <<"status_reason">> := ExpectedMsg
        }},
        create_connector_api(TCConfig, #{
            <<"bootstrap_hosts">> => BadHost
        })
    ),
    ok.

t_create_connector_while_connection_is_down() ->
    [{matrix, true}].
t_create_connector_while_connection_is_down(matrix) ->
    [[?tcp_plain, ?no_auth]];
t_create_connector_while_connection_is_down(TCConfig) ->
    #{topic := Topic} = simple_create_rule_api(TCConfig),
    C = start_client(),
    ?check_trace(
        begin
            Disconnected = atom_to_binary(?status_disconnected),
            %% Initially, the connection cannot be stablished.  Messages are not buffered,
            %% hence the status is `?status_disconnected'.
            with_brokers_down(TCConfig, fun() ->
                {201, #{<<"status">> := Disconnected}} =
                    create_connector_api(TCConfig, #{}),
                {201, #{<<"status">> := Disconnected}} =
                    create_action_api(TCConfig, #{}),
                {ok, Offset1} = resolve_kafka_offset(TCConfig),
                emqtt:publish(C, Topic, <<"hey">>, [{qos, 1}]),
                {ok, Offset2} = resolve_kafka_offset(TCConfig),
                emqtt:publish(C, Topic, <<"hey">>, [{qos, 1}]),
                {ok, Offset3} = resolve_kafka_offset(TCConfig),
                emqtt:publish(C, Topic, <<"hey">>, [{qos, 1}]),
                ?assertEqual([Offset1], lists:usort([Offset1, Offset2, Offset3])),
                ?retry(
                    200,
                    10,
                    ?assertMatch(
                        {200, #{
                            <<"metrics">> := #{
                                <<"matched">> := 3,
                                <<"failed">> := 3,
                                <<"inflight">> := 0,
                                <<"queuing">> := 0,
                                <<"dropped">> := 0
                            }
                        }},
                        get_action_metrics_api(TCConfig)
                    )
                ),
                ok
            end),
            %% Let the connector and action recover
            Connected = atom_to_binary(?status_connected),
            ?retry(
                _Sleep0 = 1_100,
                _Attempts0 = 10,
                ?assertMatch(
                    {200, #{<<"status">> := Connected}},
                    get_action_api(TCConfig)
                )
            ),
            %% Now the connection drops again; this time, status should be
            %% `?status_connecting' to avoid destroying wolff_producers and their replayq
            %% buffers.
            Connecting = atom_to_binary(?status_connecting),
            with_brokers_down(TCConfig, fun() ->
                ?retry(
                    _Sleep0 = 1_100,
                    _Attempts0 = 10,
                    ?assertMatch(
                        {200, #{<<"status">> := Connecting}},
                        get_action_api(TCConfig)
                    )
                ),
                %% This should get enqueued by wolff producers.
                spawn_link(fun() -> emqtt:publish(C, Topic, <<"hey">>, [{qos, 1}]) end),
                ?retry(
                    200,
                    10,
                    ?assertMatch(
                        {200, #{
                            <<"metrics">> := #{
                                <<"matched">> := 4,
                                <<"inflight">> := 0,
                                <<"queuing">> := 1,
                                <<"queuing_bytes">> := QueuingBytes,
                                <<"dropped">> := 0,
                                <<"success">> := 0
                            }
                        }} when QueuingBytes > 0,
                        get_action_metrics_api(TCConfig)
                    )
                ),
                ok
            end),
            ?retry(
                _Sleep2 = 600,
                _Attempts2 = 20,
                begin
                    ?retry(
                        200,
                        10,
                        ?assertMatch(
                            {200, #{
                                <<"metrics">> := #{
                                    <<"success">> := 1
                                }
                            }},
                            get_action_metrics_api(TCConfig)
                        )
                    ),
                    ok
                end
            ),
            ok
        end,
        []
    ),
    ok.

t_connector_health_check_topic() ->
    [{matrix, true}].
t_connector_health_check_topic(matrix) ->
    [[?tcp_sasl, ?no_auth]];
t_connector_health_check_topic(TCConfig) ->
    ?check_trace(
        begin
            %% We create a connector pointing to a broker that expects authentication, but
            %% we don't provide it in the config.
            %% Without a health check topic, a dummy topic name is used to probe
            %% post-auth connectivity, so the status is "disconnected"
            ?assertMatch(
                {201, #{<<"status">> := <<"disconnected">>}},
                create_connector_api(TCConfig, #{})
            ),

            %% By providing a health check topic, we should detect it's disconnected
            %% without the need for an action.
            ?assertMatch(
                {200, #{<<"status">> := <<"disconnected">>}},
                update_connector_api(TCConfig, #{
                    <<"health_check_topic">> =>
                        emqx_bridge_kafka_testlib:test_topic_one_partition()
                })
            ),

            %% By providing an inexistent health check topic, we should detect it's
            %% disconnected without the need for an action.
            ?assertMatch(
                {200, #{
                    <<"status">> := <<"disconnected">>,
                    <<"status_reason">> := <<"Unknown topic or partition", _/binary>>
                }},
                update_connector_api(TCConfig, #{
                    <<"bootstrap_hosts">> => <<"kafka-1.emqx.net:9092">>,
                    <<"health_check_topic">> => <<"i-dont-exist-999">>
                })
            ),

            ok
        end,
        []
    ),
    ok.

%% Checks that, if Kafka raises `invalid_partition_count' error, we bump the corresponding
%% failure rule action metric.
t_invalid_partition_count_metrics(TCConfig) ->
    ActionType = get_config(action_type, TCConfig),
    ActionName = get_config(action_name, TCConfig),
    ?check_trace(
        #{timetrap => 10_000},
        begin
            {201, _} = create_connector_api(TCConfig, #{}),

            C = start_client(),

            %%--------------------------------------------
            ?tp(notice, "sync", #{}),
            %%--------------------------------------------
            %% Artificially force sync query to be used; otherwise, it's only used when the
            %% resource is blocked and retrying.
            ok = meck:new(emqx_bridge_kafka_impl_producer, [passthrough, no_history]),
            on_exit(fun() -> catch meck:unload() end),
            ok = meck:expect(emqx_bridge_kafka_impl_producer, query_mode, 1, simple_sync),

            {201, _} = create_action_api(TCConfig, #{}),
            #{topic := RuleTopic, id := RuleId} = simple_create_rule_api(TCConfig),

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
            ok = emqx_bridge_v2:remove(?global_ns, actions, ActionType, ActionName),
            {201, _} =
                create_action_api(
                    TCConfig,
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
t_multiple_actions_sharing_topic(TCConfig) ->
    ?check_trace(
        begin
            ActionName1 = <<"a1">>,
            ActionName2 = <<"a2">>,
            {201, _} = create_connector_api(TCConfig, #{}),
            {201, _} = create_action_api([{action_name, ActionName1} | TCConfig], #{}),
            {201, _} = create_action_api([{action_name, ActionName2} | TCConfig], #{}),
            #{topic := RuleTopic} = simple_create_rule_api([{action_name, ActionName2} | TCConfig]),
            C = start_client(),

            ?assertStatusAPI(ActionName1, <<"connected">>, TCConfig),
            ?assertStatusAPI(ActionName2, <<"connected">>, TCConfig),

            %% Disabling a1 shouldn't disturb a2.
            ?assertMatch(
                {204, _}, disable_action_api([{action_name, ActionName1} | TCConfig])
            ),

            ?assertStatusAPI(ActionName1, <<"disconnected">>, TCConfig),
            ?assertStatusAPI(ActionName2, <<"connected">>, TCConfig),

            emqtt:publish(C, RuleTopic, <<"hey">>, [{qos, 1}]),
            ?retry(
                200,
                10,
                ?assertMatch(
                    {200, #{
                        <<"metrics">> := #{
                            <<"success">> := 1
                        }
                    }},
                    get_action_metrics_api([{action_name, ActionName2} | TCConfig])
                )
            ),
            ?assertStatusAPI(ActionName2, <<"connected">>, TCConfig),

            ?assertMatch(
                {204, _},
                enable_action_api([{action_name, ActionName1} | TCConfig])
            ),
            ?assertStatusAPI(ActionName1, <<"connected">>, TCConfig),
            ?assertStatusAPI(ActionName2, <<"connected">>, TCConfig),
            emqtt:publish(C, RuleTopic, <<"hey">>, [{qos, 1}]),
            ?retry(
                200,
                10,
                ?assertMatch(
                    {200, #{
                        <<"metrics">> := #{
                            <<"success">> := 2
                        }
                    }},
                    get_action_metrics_api([{action_name, ActionName2} | TCConfig])
                )
            ),

            %% Deleting also shouldn't disrupt a2.
            ?assertMatch(
                {204, _},
                delete_action_api([{action_name, ActionName1} | TCConfig])
            ),
            ?assertStatusAPI(ActionName2, <<"connected">>, TCConfig),
            emqtt:publish(C, RuleTopic, <<"hey">>, [{qos, 1}]),
            ?retry(
                200,
                10,
                ?assertMatch(
                    {200, #{
                        <<"metrics">> := #{
                            <<"success">> := 3
                        }
                    }},
                    get_action_metrics_api([{action_name, ActionName2} | TCConfig])
                )
            ),

            ok
        end,
        fun(Trace) ->
            ?assertEqual([], ?of_kind("kafka_producer_invalid_partition_count", Trace)),
            ok
        end
    ),
    ok.

%% Smoke tests for using a templated topic and adynamic kafka topics.
t_dynamic_topics(TCConfig) ->
    ActionName = get_config(action_name, TCConfig),
    PreTCConfiguredTopic1 = <<"pct1">>,
    PreTCConfiguredTopic2 = <<"pct2">>,
    ensure_kafka_topic(PreTCConfiguredTopic1),
    ensure_kafka_topic(PreTCConfiguredTopic2),
    ?check_trace(
        #{timetrap => 7_000},
        begin
            {201, _} = create_connector_api(TCConfig, #{}),
            {201, _} = create_action_api(TCConfig, #{
                <<"parameters">> => #{
                    <<"topic">> => <<"pct${.payload.n}">>,
                    <<"message">> => #{
                        <<"key">> => <<"${.clientid}">>,
                        <<"value">> => <<"${.payload.p}">>
                    }
                }
            }),
            #{topic := RuleTopic} = simple_create_rule_api(TCConfig),
            ?assertStatusAPI(ActionName, <<"connected">>, TCConfig),

            ?tapTelemetry(),

            C = start_client(),
            Payload = fun(Map) -> emqx_utils_json:encode(Map) end,
            {ok, Offset1} = resolve_kafka_offset(TCConfig, #{topic => PreTCConfiguredTopic1}),
            {ok, Offset2} = resolve_kafka_offset(TCConfig, #{topic => PreTCConfiguredTopic2}),
            emqtt:publish(C, RuleTopic, Payload(#{n => 1, p => <<"p1">>}), [{qos, 1}]),
            emqtt:publish(C, RuleTopic, Payload(#{n => 2, p => <<"p2">>}), [{qos, 1}]),

            check_kafka_message_payload(PreTCConfiguredTopic1, Offset1, <<"p1">>),
            check_kafka_message_payload(PreTCConfiguredTopic2, Offset2, <<"p2">>),

            ?retry(
                200,
                10,
                ?assertMatch(
                    {200, #{
                        <<"metrics">> := #{
                            <<"matched">> := 2,
                            <<"success">> := 2,
                            <<"queuing">> := 0
                        }
                    }},
                    get_action_metrics_api(TCConfig)
                )
            ),

            ?assertReceive(
                {telemetry, #{
                    measurements := #{gauge_set := _},
                    metadata := #{worker_id := _, resource_id := _}
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
t_fixed_topic_recovers_in_disk_mode() ->
    [{matrix, true}].
t_fixed_topic_recovers_in_disk_mode(matrix) ->
    [[?tcp_plain, ?no_auth]];
t_fixed_topic_recovers_in_disk_mode(TCConfig) ->
    ?check_trace(
        #{timetrap => 7_000},
        begin
            {201, _} = create_connector_api(TCConfig, #{}),
            {201, _} = create_action_api(TCConfig, #{
                <<"parameters">> => #{
                    <<"query_mode">> => <<"async">>,
                    <<"buffer">> => #{
                        <<"mode">> => <<"disk">>
                    }
                }
            }),
            #{topic := RuleTopic} = simple_create_rule_api(TCConfig),
            C = start_client(),

            %% Cut connection to kafka and enqueue some messages
            SentMessages = with_brokers_down(TCConfig, fun() ->
                ct:sleep(100),
                SentMessages = lists:map(
                    fun(_) ->
                        {ok, Offset} = resolve_kafka_offset(TCConfig),
                        emqtt:publish(C, RuleTopic, <<"hey">>, [{qos, 1}]),
                        #{offset => Offset, payload => <<"hey">>}
                    end,
                    lists:seq(1, 5)
                ),
                ?retry(
                    200,
                    10,
                    ?assertMatch(
                        {200, #{
                            <<"metrics">> := #{
                                <<"matched">> := 5,
                                <<"success">> := 0,
                                <<"queuing">> := 5
                            }
                        }},
                        get_action_metrics_api(TCConfig)
                    )
                ),
                %% Turn off action, restore kafka connection
                ?assertMatch(
                    {204, _},
                    disable_action_api(TCConfig)
                ),
                SentMessages
            end),
            %% Restart action; should've shot enqueued messages
            ?tapTelemetry(),
            ?assertMatch(
                {204, _},
                enable_action_api(TCConfig)
            ),
            ?assertReceive(
                {telemetry, #{
                    name := [?TELEMETRY_PREFIX, inflight],
                    measurements := #{gauge_set := 0}
                }}
            ),
            %% Success metrics are not bumped because wolff does not store callbacks in
            %% disk.
            ?retry(
                200,
                10,
                ?assertMatch(
                    {200, #{
                        <<"metrics">> := #{
                            <<"success">> := 0,
                            <<"queuing">> := 0
                        }
                    }},
                    get_action_metrics_api(TCConfig)
                )
            ),
            [#{offset := Offset} | _] = SentMessages,
            ?assertMatch(
                {ok, {_, [_, _, _, _, _]}},
                get_kafka_messages(#{offset => Offset}, TCConfig)
            ),
            ok
        end,
        []
    ),
    ok.

%% Verifies that we disallow disk mode when the kafka topic is dynamic.
t_disallow_disk_mode_for_dynamic_topic(TCConfig) ->
    #{
        type := Type,
        name := Name,
        config := ActionConfig
    } = emqx_bridge_v2_testlib:get_common_values_with_configs(TCConfig),
    ?assertMatch(
        #{},
        emqx_bridge_v2_testlib:parse_and_check(
            action,
            Type,
            Name,
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
            Name,
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
t_migrate_old_replayq_dir(TCConfig) ->
    #{
        type := Type,
        name := Name,
        config := ActionConfig
    } = emqx_bridge_v2_testlib:get_common_values_with_configs(TCConfig),
    #{<<"parameters">> := #{<<"topic">> := Topic}} = ActionConfig,
    ReplayqDir = emqx_bridge_kafka_impl_producer:replayq_dir(Type, Name),
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
    {201, _} = create_connector_api(TCConfig, #{}),
    ?check_trace(
        begin
            {201, _} = create_action_api(TCConfig, #{
                <<"parameters">> => #{
                    <<"buffer">> => #{
                        <<"mode">> => <<"disk">>
                    }
                }
            }),
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
t_inexistent_topic_after_created(TCConfig) ->
    Topic = atom_to_binary(?FUNCTION_NAME),
    ?check_trace(
        #{timetrap => 7_000},
        begin
            ensure_kafka_topic(Topic),
            {201, #{<<"status">> := <<"connected">>}} = create_connector_api(TCConfig, #{}),

            %% Initially connected
            ?assertMatch(
                {201, #{<<"status">> := <<"connected">>}},
                create_action_api(TCConfig, #{
                    <<"parameters">> => #{
                        <<"topic">> => Topic
                    },
                    <<"resource_opts">> => #{
                        <<"health_check_interval">> => <<"1s">>
                    }
                })
            ),

            %% After deleting the topic and a health check, it becomes connecting.
            {ok, {ok, _}} =
                ?wait_async_action(
                    delete_kafka_topic(Topic),
                    #{?snk_kind := "kafka_producer_action_unknown_topic"}
                ),
            ?assertMatch(
                {200, #{<<"status">> := <<"connecting">>}},
                get_action_api(TCConfig)
            ),

            %% Recovers after topic is back
            {ok, {ok, _}} =
                ?wait_async_action(
                    ensure_kafka_topic(Topic),
                    #{?snk_kind := "kafka_producer_action_connected"}
                ),
            ?assertMatch(
                {200, #{<<"status">> := <<"connected">>}},
                get_action_api(TCConfig)
            ),

            ok
        end,
        []
    ),
    ok.

%% When the connector is disabled but the action is enabled, we should bump the rule
%% metrics accordingly, bumping only `actions.out_of_service'.
t_metrics_out_of_service(TCConfig) ->
    {201, #{<<"enable">> := false}} = create_connector_api(TCConfig, #{
        <<"enable">> => false
    }),
    {201, _} = create_action_api(TCConfig, #{
        <<"parameters">> => #{<<"query_mode">> => <<"async">>}
    }),
    #{topic := RuleTopic, id := RuleId} = simple_create_rule_api(TCConfig),
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
    {200, _} = update_action_api(
        TCConfig,
        #{<<"parameters">> => #{<<"query_mode">> => <<"sync">>}}
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
t_overflow_rule_metrics(TCConfig) ->
    {201, _} = create_connector_api(TCConfig, #{}),
    {201, _} = create_action_api(TCConfig, #{
        <<"parameters">> => #{
            <<"query_mode">> => <<"async">>,
            <<"buffer">> => #{
                <<"segment_bytes">> => <<"1B">>,
                <<"per_partition_limit">> => <<"2B">>
            }
        }
    }),
    #{topic := RuleTopic, id := RuleId} = simple_create_rule_api(TCConfig),
    %% Async
    emqx:publish(emqx_message:make(RuleTopic, <<"aaaaaaaaaaaaaa">>)),
    ?retry(
        200,
        10,
        ?assertMatch(
            {200, #{
                <<"metrics">> := #{
                    <<"dropped">> := 1,
                    <<"dropped.queue_full">> := 1,
                    <<"dropped.expired">> := 0,
                    <<"dropped.other">> := 0,
                    <<"matched">> := 1,
                    <<"success">> := 0,
                    <<"failed">> := 0,
                    <<"late_reply">> := 0,
                    <<"retried">> := 0
                }
            }},
            get_action_metrics_api(TCConfig)
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
    {200, _} = update_action_api(
        TCConfig,
        #{
            <<"parameters">> => #{
                <<"query_mode">> => <<"sync">>,
                <<"buffer">> => #{
                    <<"segment_bytes">> => <<"1B">>,
                    <<"per_partition_limit">> => <<"2B">>
                }
            }
        }
    ),
    ok = reset_rule_metrics(RuleId),
    emqx:publish(emqx_message:make(RuleTopic, <<"aaaaaaaaaaaaaa">>)),
    ?retry(
        200,
        10,
        ?assertMatch(
            {200, #{
                <<"metrics">> := #{
                    <<"dropped">> := 1,
                    <<"dropped.queue_full">> := 1,
                    <<"dropped.expired">> := 0,
                    <<"dropped.other">> := 0,
                    <<"matched">> := 1,
                    <<"success">> := 0,
                    <<"failed">> := 0,
                    <<"late_reply">> := 0,
                    <<"retried">> := 0
                }
            }},
            get_action_metrics_api(TCConfig)
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
    [[?sync], [?async]];
t_fallback_actions(TCConfig) when is_list(TCConfig) ->
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
    {201, _} = create_connector_api(TCConfig, #{}),
    {201, _} = create_action_api(TCConfig, #{
        <<"fallback_actions">> => [
            #{
                <<"kind">> => <<"republish">>,
                <<"args">> => RepublishArgs
            }
        ],
        <<"parameters">> => #{
            %% Simple way to make the requests fail: make the buffer overflow
            <<"buffer">> => #{
                <<"segment_bytes">> => <<"1B">>,
                <<"per_partition_limit">> => <<"2B">>
            }
        }
    }),

    #{topic := RuleTopic} = simple_create_rule_api(TCConfig),

    emqx:subscribe(RepublishTopic),
    Payload = <<"aaaaaaaaaaaaaa">>,
    emqx:publish(emqx_message:make(RuleTopic, Payload)),

    ?assertReceive({deliver, RepublishTopic, #message{payload = Payload}}),

    ok.

%% Exercises the code path where we use MSK IAM authentication.
%% Unfortunately, there seems to be no good way to accurately test this as it would
%% require running this in an EC2 instance to pass, and also to have a Kafka cluster
%% configured to use MSK IAM authentication.
t_msk_iam_authn(TCConfig) ->
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
                create_connector_api(TCConfig, #{<<"authentication">> => <<"msk_iam">>})
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
