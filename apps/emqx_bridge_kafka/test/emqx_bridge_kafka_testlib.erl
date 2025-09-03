%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_kafka_testlib).

-compile(nowarn_export_all).
-compile(export_all).

-export([
    wait_until_kafka_is_up/0,

    action_connector_config/1,
    source_connector_config/1,
    action_config/1,
    source_config/1,

    shared_secret_path/0,
    shared_secret/1,

    valid_ssl_settings/0,

    no_auth/0,
    plain_auth/0,
    scram_sha256_auth/0,
    scram_sha512_auth/0,
    kerberos_auth/0,

    kafka_hosts_direct/0,

    ensure_kafka_topic/1,
    ensure_kafka_topic/2,
    delete_kafka_topic/1,
    test_topic_one_partition/0
]).

-import(emqx_common_test_helpers, [on_exit/1]).

-define(ACTION_CONNECTOR_TYPE_BIN, <<"kafka_producer">>).
-define(ACTION_TYPE_BIN, <<"kafka_producer">>).
-define(SOURCE_CONNECTOR_TYPE_BIN, <<"kafka_consumer">>).
-define(SOURCE_TYPE_BIN, <<"kafka_consumer">>).

action_connector_config(Overrides) ->
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
    emqx_bridge_v2_testlib:parse_and_check_connector(
        ?ACTION_CONNECTOR_TYPE_BIN, <<"x">>, InnerConfigMap
    ).

source_connector_config(Overrides) ->
    Defaults = #{
        <<"enable">> => true,
        <<"description">> => <<"my connector">>,
        <<"tags">> => [<<"some">>, <<"tags">>],
        <<"bootstrap_hosts">> => <<"kafka-1.emqx.net:9092">>,
        <<"authentication">> => no_auth(),
        <<"connect_timeout">> => <<"5s">>,
        <<"metadata_request_timeout">> => <<"5s">>,
        <<"min_metadata_refresh_interval">> => <<"3s">>,
        <<"resource_opts">> =>
            emqx_bridge_v2_testlib:common_connector_resource_opts()
    },
    InnerConfigMap = emqx_utils_maps:deep_merge(Defaults, Overrides),
    emqx_bridge_v2_testlib:parse_and_check_connector(
        ?SOURCE_CONNECTOR_TYPE_BIN, <<"x">>, InnerConfigMap
    ).

action_config(Overrides) ->
    Defaults = #{
        <<"enable">> => true,
        <<"description">> => <<"my action">>,
        <<"tags">> => [<<"some">>, <<"tags">>],
        <<"connector">> => <<"please override">>,
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

source_config(Overrides) ->
    Defaults = #{
        <<"enable">> => true,
        <<"description">> => <<"my action">>,
        <<"tags">> => [<<"some">>, <<"tags">>],
        <<"connector">> => <<"please override">>,
        <<"parameters">> => #{
            <<"key_encoding_mode">> => <<"none">>,
            <<"max_batch_bytes">> => <<"896KB">>,
            <<"max_wait_time">> => <<"500ms">>,
            <<"max_rejoin_attempts">> => <<"5">>,
            <<"offset_reset_policy">> => <<"earliest">>,
            <<"topic">> => <<"please override">>,
            <<"value_encoding_mode">> => <<"none">>
        },
        <<"resource_opts">> =>
            emqx_bridge_v2_testlib:common_source_resource_opts()
    },
    InnerConfigMap = emqx_utils_maps:deep_merge(Defaults, Overrides),
    emqx_bridge_v2_testlib:parse_and_check(source, ?SOURCE_TYPE_BIN, <<"x">>, InnerConfigMap).

shared_secret_path() ->
    os:getenv("CI_SHARED_SECRET_PATH", "/var/lib/secret").

shared_secret(client_keyfile) ->
    bin(filename:join([shared_secret_path(), "client.key"]));
shared_secret(client_certfile) ->
    bin(filename:join([shared_secret_path(), "client.crt"]));
shared_secret(client_cacertfile) ->
    bin(filename:join([shared_secret_path(), "ca.crt"]));
shared_secret(rig_keytab) ->
    bin(filename:join([shared_secret_path(), "rig.keytab"])).

valid_ssl_settings() ->
    #{
        <<"verify">> => <<"verify_none">>,
        <<"cacertfile">> => shared_secret(client_cacertfile),
        <<"certfile">> => shared_secret(client_certfile),
        <<"keyfile">> => shared_secret(client_keyfile),
        <<"enable">> => true
    }.

test_topic_one_partition() ->
    <<"test-topic-one-partition">>.

%% In CI, Kafka may take a while longer to reach an usable state.  If we run the test
%% suites too soon, we may get some flaky failures with error messages like these:
%% "The coordinator is not available."
wait_until_kafka_is_up() ->
    wait_until_kafka_is_up(0).

wait_until_kafka_is_up(300) ->
    ct:fail("Kafka is not up even though we have waited for a while");
wait_until_kafka_is_up(Attempts) ->
    KafkaTopic = test_topic_one_partition(),
    case brod:resolve_offset(kafka_hosts_direct(), KafkaTopic, _Partition = 0, latest) of
        {ok, _} ->
            ok;
        _ ->
            timer:sleep(1_000),
            wait_until_kafka_is_up(Attempts + 1)
    end.

kafka_hosts_direct() ->
    kpro:parse_endpoints("kafka-1.emqx.net:9092").

ensure_kafka_topic(KafkaTopic) ->
    ensure_kafka_topic(KafkaTopic, _Opts = #{}).

ensure_kafka_topic(KafkaTopic, Opts) ->
    NumPartitions = maps:get(num_partitions, Opts, 1),
    on_exit(fun() -> delete_kafka_topic(KafkaTopic) end),
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
    ConnConfig = #{},
    Endpoints = kafka_hosts_direct(),
    case brod:create_topics(Endpoints, TopicConfigs, RequestConfig, ConnConfig) of
        ok ->
            ok;
        {error, topic_already_exists} ->
            ok;
        {error, Reason} when is_binary(Reason) ->
            {match, Reason} = {re:run(Reason, <<"already exists">>, [{capture, none}]), Reason},
            ok
    end.

delete_kafka_topic(KafkaTopic) ->
    Timeout = 1_000,
    ConnConfig = #{},
    Endpoints = kafka_hosts_direct(),
    case brod:delete_topics(Endpoints, [KafkaTopic], Timeout, ConnConfig) of
        ok -> ok;
        {error, unknown_topic_or_partition} -> ok
    end.

no_auth() ->
    <<"none">>.

plain_auth() ->
    #{
        <<"mechanism">> => <<"plain">>,
        <<"username">> => <<"emqxuser">>,
        <<"password">> => <<"password">>
    }.

scram_sha256_auth() ->
    #{
        <<"mechanism">> => <<"scram_sha_256">>,
        <<"username">> => <<"emqxuser">>,
        <<"password">> => <<"password">>
    }.

scram_sha512_auth() ->
    #{
        <<"mechanism">> => <<"scram_sha_512">>,
        <<"username">> => <<"emqxuser">>,
        <<"password">> => <<"password">>
    }.

kerberos_auth() ->
    #{
        <<"kerberos_principal">> => <<"rig@KDC.EMQX.NET">>,
        <<"kerberos_keytab_file">> => shared_secret(rig_keytab)
    }.

msk_iam_auth() ->
    <<"msk_iam">>.

bin(X) -> emqx_utils_conv:bin(X).
