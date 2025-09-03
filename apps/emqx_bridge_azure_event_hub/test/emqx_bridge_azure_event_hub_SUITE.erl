%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_azure_event_hub_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("emqx/include/asserts.hrl").
-include_lib("emqx_resource/include/emqx_resource.hrl").

%%------------------------------------------------------------------------------
%% Defs
%%------------------------------------------------------------------------------

-import(emqx_common_test_helpers, [on_exit/1]).

-define(ACTION_TYPE, azure_event_hub_producer).
-define(ACTION_TYPE_BIN, <<"azure_event_hub_producer">>).
-define(CONNECTOR_TYPE, azure_event_hub_producer).
-define(CONNECTOR_TYPE_BIN, <<"azure_event_hub_producer">>).

-define(PROXY_NAME, "kafka_sasl_ssl").
-define(PROXY_HOST, "toxiproxy").
-define(PROXY_PORT, 8474).

-define(KAFKA_CONNECTOR_TYPE, kafka_producer).
-define(KAFKA_ACTION_TYPE, kafka_producer).

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
            emqx_bridge_azure_event_hub,
            emqx_bridge,
            emqx_rule_engine,
            emqx_management,
            emqx_mgmt_api_test_util:emqx_dashboard()
        ],
        #{work_dir => emqx_cth_suite:work_dir(TCConfig)}
    ),
    KafkaHost = os:getenv("KAFKA_SASL_SSL_HOST", "toxiproxy.emqx.net"),
    KafkaPort = list_to_integer(os:getenv("KAFKA_SASL_SSL_PORT", "9295")),
    [
        {apps, Apps},
        {proxy_host, ?PROXY_HOST},
        {proxy_port, ?PROXY_PORT},
        {proxy_name, ?PROXY_NAME},
        {kafka_host, KafkaHost},
        {kafka_port, KafkaPort}
        | TCConfig
    ].

end_per_suite(TCConfig) ->
    Apps = get_config(apps, TCConfig),
    emqx_cth_suite:stop(Apps),
    reset_proxy(),
    ok.

init_per_testcase(TestCase, TCConfig) ->
    reset_proxy(),
    Path = group_path(TCConfig, no_groups),
    ct:pal(asciiart:visible($%, "~p - ~s", [Path, TestCase])),
    ConnectorName = atom_to_binary(TestCase),
    ConnectorConfig = connector_config(#{}),
    ActionName = ConnectorName,
    ActionConfig = action_config(#{
        <<"connector">> => ConnectorName
    }),
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

connector_config(Overrides) ->
    Defaults = #{
        <<"enable">> => true,
        <<"description">> => <<"my connector">>,
        <<"tags">> => [<<"some">>, <<"tags">>],
        <<"bootstrap_hosts">> => <<"toxiproxy.emqx.net:9295">>,
        <<"authentication">> =>
            emqx_bridge_kafka_testlib:plain_auth(),
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
                <<"cacertfile">> => emqx_bridge_kafka_testlib:shared_secret(client_cacertfile),
                <<"certfile">> => emqx_bridge_kafka_testlib:shared_secret(client_certfile),
                <<"keyfile">> => emqx_bridge_kafka_testlib:shared_secret(client_keyfile),
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

get_config(K, TCConfig) -> emqx_bridge_v2_testlib:get_value(K, TCConfig).
get_config(K, TCConfig, Default) -> proplists:get_value(K, TCConfig, Default).

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

with_failure(FailureType, Fn) ->
    emqx_common_test_helpers:with_failure(FailureType, ?PROXY_NAME, ?PROXY_HOST, ?PROXY_PORT, Fn).

create_connector_api(TCConfig, Overrides) ->
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:create_connector_api(TCConfig, Overrides)
    ).

create_action_api(TCConfig, Overrides) ->
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:create_action_api(TCConfig, Overrides)
    ).

get_action_api(TCConfig) ->
    emqx_bridge_v2_testlib:get_action_api2(TCConfig).

disable_connector_api(TCConfig) ->
    #{connector_type := Type, connector_name := Name} =
        emqx_bridge_v2_testlib:get_common_values(TCConfig),
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:disable_connector_api(Type, Name)
    ).

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

start_client() ->
    start_client(_Opts = #{}).

start_client(Opts0) ->
    Opts = maps:merge(#{proto_ver => v5}, Opts0),
    {ok, C} = emqtt:start_link(Opts),
    on_exit(fun() -> emqtt:stop(C) end),
    {ok, _} = emqtt:connect(C),
    C.

make_message() ->
    Time = erlang:unique_integer(),
    BinTime = integer_to_binary(Time),
    Payload = emqx_guid:to_hexstr(emqx_guid:gen()),
    #{
        clientid => BinTime,
        payload => Payload,
        timestamp => Time
    }.

%%------------------------------------------------------------------------------
%% Test cases
%%------------------------------------------------------------------------------

t_start_stop(TCConfig) when is_list(TCConfig) ->
    emqx_bridge_v2_testlib:t_start_stop(TCConfig, kafka_producer_stopped).

t_on_get_status(TCConfig) when is_list(TCConfig) ->
    emqx_bridge_v2_testlib:t_on_get_status(TCConfig, #{failure_status => ?status_connecting}).

t_rule_action(TCConfig0) when is_list(TCConfig0) ->
    TCConfig = emqx_bridge_v2_testlib:proplist_update(TCConfig0, action_config, fun(Cfg) ->
        emqx_utils_maps:deep_merge(
            Cfg,
            #{<<"parameters">> => #{<<"message">> => #{<<"value">> => <<"${.payload}">>}}}
        )
    end),
    emqx_bridge_kafka_action_SUITE:t_rule_action(TCConfig).

t_same_name_azure_kafka_bridges(TCConfig) ->
    TCConfigKafka = maps:fold(
        fun(K, V, Acc) ->
            lists:keyreplace(K, 1, Acc, {K, V})
        end,
        TCConfig,
        #{
            action_type => ?KAFKA_ACTION_TYPE,
            connector_type => ?KAFKA_CONNECTOR_TYPE
        }
    ),
    %% creates the AEH bridge and check it's working
    {201, #{<<"status">> := <<"connected">>}} = create_connector_api(TCConfig, #{}),
    {201, #{<<"status">> := <<"connected">>}} = create_action_api(TCConfig, #{
        <<"parameters">> => #{<<"query_mode">> => <<"sync">>}
    }),
    #{topic := Topic} = simple_create_rule_api(TCConfig),
    C = start_client(),
    %% than creates a Kafka bridge with same name
    {201, #{<<"status">> := <<"connected">>}} = create_connector_api(TCConfigKafka, #{}),
    {201, #{<<"status">> := <<"connected">>}} = create_action_api(TCConfigKafka, #{}),
    %% check that both bridges are healthy
    ?assertMatch(
        {200, #{<<"status">> := <<"connected">>}},
        get_action_api(TCConfig)
    ),
    ?assertMatch(
        {200, #{<<"status">> := <<"connected">>}},
        get_action_api(TCConfigKafka)
    ),

    ?assertMatch(
        {{204, _}, {ok, _}},
        ?wait_async_action(
            disable_connector_api(TCConfigKafka),
            #{?snk_kind := kafka_producer_stopped},
            5_000
        )
    ),
    % check that AEH bridge is still working
    AEHConnResId = emqx_bridge_v2_testlib:connector_resource_id(TCConfig),
    ?check_trace(
        {_, {ok, _}} =
            ?wait_async_action(
                emqtt:publish(C, Topic, <<"hey">>, [{qos, 1}]),
                #{?snk_kind := "rule_runtime_action_success"},
                5_000
            ),
        fun(Trace) ->
            ?assertMatch(
                [#{instance_id := AEHConnResId}],
                ?of_kind(emqx_bridge_kafka_impl_producer_async_query, Trace)
            )
        end
    ),
    ok.

t_multiple_actions_sharing_topic(TCConfig) ->
    ActionConfig0 = ?config(action_config, TCConfig),
    ActionConfig =
        emqx_utils_maps:deep_merge(
            ActionConfig0,
            #{<<"parameters">> => #{<<"query_mode">> => <<"sync">>}}
        ),
    ok =
        emqx_bridge_kafka_action_SUITE:?FUNCTION_NAME([{action_config, ActionConfig} | TCConfig]),
    ok.

t_dynamic_topics(TCConfig) ->
    ActionConfig0 = get_config(action_config, TCConfig),
    ActionConfig =
        emqx_utils_maps:deep_merge(
            ActionConfig0,
            #{<<"parameters">> => #{<<"query_mode">> => <<"sync">>}}
        ),
    ok =
        emqx_bridge_kafka_action_SUITE:?FUNCTION_NAME(
            [{action_config, ActionConfig} | TCConfig]
        ),
    ok.

t_disallow_disk_mode_for_dynamic_topic(TCConfig) ->
    emqx_bridge_kafka_action_SUITE:?FUNCTION_NAME(TCConfig).
