%%--------------------------------------------------------------------
%% Copyright (c) 2022-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_v2_gcp_pubsub_consumer_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-define(CONNECTOR_TYPE_BIN, <<"gcp_pubsub_consumer">>).
-define(SOURCE_TYPE_BIN, <<"gcp_pubsub_consumer">>).

%%------------------------------------------------------------------------------
%% CT boilerplate
%%------------------------------------------------------------------------------

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    emqx_bridge_gcp_pubsub_consumer_SUITE:init_per_suite(Config).

end_per_suite(Config) ->
    emqx_bridge_gcp_pubsub_consumer_SUITE:end_per_suite(Config).

init_per_testcase(TestCase, Config) ->
    common_init_per_testcase(TestCase, Config).

common_init_per_testcase(TestCase, Config0) ->
    ct:timetrap(timer:seconds(60)),
    ServiceAccountJSON =
        #{<<"project_id">> := ProjectId} =
        emqx_bridge_gcp_pubsub_utils:generate_service_account_json(),
    UniqueNum = integer_to_binary(erlang:unique_integer()),
    Name = <<(atom_to_binary(TestCase))/binary, UniqueNum/binary>>,
    ConnectorConfig = connector_config(Name, ServiceAccountJSON),
    PubsubTopic = Name,
    SourceConfig = source_config(#{
        connector => Name,
        parameters => #{topic => PubsubTopic}
    }),
    Config = [
        {bridge_kind, source},
        {source_type, ?SOURCE_TYPE_BIN},
        {source_name, Name},
        {source_config, SourceConfig},
        {connector_name, Name},
        {connector_type, ?CONNECTOR_TYPE_BIN},
        {connector_config, ConnectorConfig},
        {service_account_json, ServiceAccountJSON},
        {project_id, ProjectId},
        {pubsub_topic, PubsubTopic}
        | Config0
    ],
    ok = emqx_bridge_gcp_pubsub_consumer_SUITE:ensure_topic(Config, PubsubTopic),
    Config.

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

connector_config(Name, ServiceAccountJSON) ->
    InnerConfigMap0 =
        #{
            <<"enable">> => true,
            <<"tags">> => [<<"bridge">>],
            <<"description">> => <<"my cool bridge">>,
            <<"connect_timeout">> => <<"5s">>,
            <<"pool_size">> => 8,
            <<"pipelining">> => <<"100">>,
            <<"max_retries">> => <<"2">>,
            <<"service_account_json">> => ServiceAccountJSON,
            <<"resource_opts">> =>
                #{
                    <<"health_check_interval">> => <<"1s">>,
                    <<"start_after_created">> => true,
                    <<"start_timeout">> => <<"5s">>
                }
        },
    emqx_bridge_v2_testlib:parse_and_check_connector(?SOURCE_TYPE_BIN, Name, InnerConfigMap0).

source_config(Overrides0) ->
    Overrides = emqx_utils_maps:binary_key_map(Overrides0),
    CommonConfig =
        #{
            <<"enable">> => true,
            <<"connector">> => <<"please override">>,
            <<"parameters">> =>
                #{
                    <<"topic">> => <<"my-topic">>
                },
            <<"resource_opts">> => #{
                <<"health_check_interval">> => <<"1s">>,
                <<"request_ttl">> => <<"1s">>,
                <<"resume_interval">> => <<"1s">>
            }
        },
    maps:merge(CommonConfig, Overrides).

%%------------------------------------------------------------------------------
%% Testcases
%%------------------------------------------------------------------------------

t_start_stop(Config) ->
    ok = emqx_bridge_v2_testlib:t_start_stop(Config, gcp_pubsub_stop),
    ok.

t_create_via_http(Config) ->
    ok = emqx_bridge_v2_testlib:t_create_via_http(Config),
    ok.

t_consume(Config) ->
    Topic = ?config(pubsub_topic, Config),
    Payload = #{<<"key">> => <<"value">>},
    Attributes = #{<<"hkey">> => <<"hval">>},
    ProduceFn = fun() ->
        emqx_bridge_gcp_pubsub_consumer_SUITE:pubsub_publish(
            Config,
            Topic,
            [
                #{
                    <<"data">> => Payload,
                    <<"orderingKey">> => <<"ok">>,
                    <<"attributes">> => Attributes
                }
            ]
        )
    end,
    Encoded = emqx_utils_json:encode(Payload),
    CheckFn = fun(Message) ->
        ?assertMatch(
            #{
                attributes := Attributes,
                message_id := _,
                ordering_key := <<"ok">>,
                publish_time := _,
                topic := Topic,
                value := Encoded
            },
            Message
        )
    end,
    ok = emqx_bridge_v2_testlib:t_consume(
        Config,
        #{
            consumer_ready_tracepoint => ?match_event(
                #{?snk_kind := "gcp_pubsub_consumer_worker_subscription_ready"}
            ),
            produce_fn => ProduceFn,
            check_fn => CheckFn,
            produce_tracepoint => ?match_event(
                #{
                    ?snk_kind := "gcp_pubsub_consumer_worker_handle_message",
                    ?snk_span := {complete, _}
                }
            )
        }
    ),
    ok.
