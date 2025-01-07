%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

assert_persisted_service_account_json_is_binary(ConnectorName) ->
    %% ensure cluster.hocon has a binary encoded json string as the value
    {ok, Hocon} = hocon:files([application:get_env(emqx, cluster_hocon_file, undefined)]),
    ?assertMatch(
        Bin when is_binary(Bin),
        emqx_utils_maps:deep_get(
            [
                <<"connectors">>,
                <<"gcp_pubsub_consumer">>,
                ConnectorName,
                <<"service_account_json">>
            ],
            Hocon
        )
    ),
    ok.

%%------------------------------------------------------------------------------
%% Testcases
%%------------------------------------------------------------------------------

t_start_stop(Config) ->
    ok = emqx_bridge_v2_testlib:t_start_stop(Config, gcp_pubsub_stop),
    ok.

t_create_via_http(Config) ->
    ok = emqx_bridge_v2_testlib:t_create_via_http(Config),
    ok.

t_create_via_http_json_object_service_account(Config0) ->
    %% After the config goes through the roundtrip with `hocon_tconf:check_plain', service
    %% account json comes back as a binary even if the input is a json object.
    ConnectorName = ?config(connector_name, Config0),
    ConnConfig0 = ?config(connector_config, Config0),
    Config1 = proplists:delete(connector_config, Config0),
    ConnConfig1 = maps:update_with(
        <<"service_account_json">>,
        fun(X) ->
            ?assert(is_binary(X), #{json => X}),
            JSON = emqx_utils_json:decode(X, [return_maps]),
            ?assert(is_map(JSON)),
            JSON
        end,
        ConnConfig0
    ),
    Config = [{connector_config, ConnConfig1} | Config1],
    ok = emqx_bridge_v2_testlib:t_create_via_http(Config),
    assert_persisted_service_account_json_is_binary(ConnectorName),
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

t_update_topic(Config) ->
    %% Tests that, if a bridge originally has the legacy field `topic_mapping' filled in
    %% and later is updated using v2 APIs, then the legacy field is cleared and the new
    %% `topic' field is used.
    ConnectorConfig = ?config(connector_config, Config),
    SourceConfig = ?config(source_config, Config),
    Name = ?config(source_name, Config),
    V1Config0 = emqx_action_info:connector_action_config_to_bridge_v1_config(
        ?SOURCE_TYPE_BIN,
        ConnectorConfig,
        SourceConfig
    ),
    V1Config = emqx_utils_maps:deep_put(
        [<<"consumer">>, <<"topic_mapping">>],
        V1Config0,
        [
            #{
                <<"pubsub_topic">> => <<"old_topic">>,
                <<"mqtt_topic">> => <<"">>,
                <<"qos">> => 2,
                <<"payload_template">> => <<"template">>
            }
        ]
    ),
    %% Note: using v1 API
    {ok, {{_, 201, _}, _, _}} = emqx_bridge_testlib:create_bridge_api(
        ?SOURCE_TYPE_BIN,
        Name,
        V1Config
    ),
    ?assertMatch(
        {ok, {{_, 200, _}, _, #{<<"parameters">> := #{<<"topic">> := <<"old_topic">>}}}},
        emqx_bridge_v2_testlib:get_source_api(?SOURCE_TYPE_BIN, Name)
    ),
    %% Note: we don't add `topic_mapping' again here to the parameters.
    {ok, {{_, 200, _}, _, _}} = emqx_bridge_v2_testlib:update_bridge_api(
        Config,
        #{<<"parameters">> => #{<<"topic">> => <<"new_topic">>}}
    ),
    ?assertMatch(
        {ok, {{_, 200, _}, _, #{<<"parameters">> := #{<<"topic">> := <<"new_topic">>}}}},
        emqx_bridge_v2_testlib:get_source_api(?SOURCE_TYPE_BIN, Name)
    ),
    ok.
