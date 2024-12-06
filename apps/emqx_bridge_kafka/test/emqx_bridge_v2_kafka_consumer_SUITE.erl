%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_v2_kafka_consumer_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-import(emqx_common_test_helpers, [on_exit/1]).

-define(CONNECTOR_TYPE_BIN, <<"kafka_consumer">>).
-define(SOURCE_TYPE_BIN, <<"kafka_consumer">>).

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
    [
        t_start_stop
    ].

init_per_suite(Config) ->
    emqx_bridge_kafka_impl_consumer_SUITE:init_per_suite(Config).

end_per_suite(Config) ->
    emqx_bridge_kafka_impl_consumer_SUITE:end_per_suite(Config).

init_per_testcase(TestCase, Config) ->
    common_init_per_testcase(TestCase, Config).

common_init_per_testcase(TestCase, Config0) ->
    ct:timetrap({seconds, 60}),
    UniqueNum = integer_to_binary(erlang:unique_integer()),
    Name = <<(atom_to_binary(TestCase))/binary, UniqueNum/binary>>,
    ConnectorConfig = connector_config(Name, Config0),
    Topic = Name,
    SourceConfig = source_config(#{
        connector => Name,
        parameters => #{topic => Topic}
    }),
    Config1 = ensure_topic_and_producers(ConnectorConfig, SourceConfig, TestCase, Config0),
    ct:comment(get_matrix_params(Config1)),
    [
        {kafka_topic, Topic},
        {bridge_kind, source},
        {source_type, ?SOURCE_TYPE_BIN},
        {source_name, Name},
        {source_config, SourceConfig},
        {connector_name, Name},
        {connector_type, ?CONNECTOR_TYPE_BIN},
        {connector_config, ConnectorConfig},
        {proxy_host, "toxiproxy"},
        {proxy_port, 8474}
        | Config1
    ].

end_per_testcase(TestCase, Config) ->
    emqx_bridge_v2_testlib:end_per_testcase(TestCase, Config),
    ok.

auth_config(Config) ->
    AuthType0 = maps:get(auth, get_matrix_params(Config)),
    AuthType =
        case AuthType0 of
            none -> none;
            sasl_auth_plain -> plain;
            sasl_auth_scram256 -> scram_sha_256;
            sasl_auth_scram512 -> scram_sha_512;
            sasl_auth_kerberos -> kerberos
        end,
    {ok, #{<<"authentication">> := Auth}} =
        hocon:binary(emqx_bridge_kafka_impl_consumer_SUITE:authentication(AuthType)),
    Auth.

get_matrix_params(Config) ->
    case group_path(Config) of
        undefined ->
            #{
                host => <<"toxiproxy.emqx.net">>,
                port => 9292,
                tls => plain,
                auth => none,
                proxy_name => "kafka_plain"
            };
        [TLS, Auth | _] ->
            #{
                host => <<"toxiproxy.emqx.net">>,
                port => toxiproxy_kafka_port(#{tls => TLS, auth => Auth}),
                tls => TLS,
                auth => Auth,
                proxy_name => toxiproxy_proxy_name(#{tls => TLS, auth => Auth})
            }
    end.

toxiproxy_kafka_port(#{tls := plain, auth := none}) -> 9292;
toxiproxy_kafka_port(#{tls := tls, auth := none}) -> 9294;
toxiproxy_kafka_port(#{tls := tls, auth := sasl_auth_kerberos}) -> 9095;
toxiproxy_kafka_port(#{tls := plain, auth := sasl_auth_kerberos}) -> 9093;
toxiproxy_kafka_port(#{tls := plain, auth := _}) -> 9293;
toxiproxy_kafka_port(#{tls := tls, auth := _}) -> 9295.

toxiproxy_proxy_name(#{tls := plain, auth := none}) -> "kafka_plain";
toxiproxy_proxy_name(#{tls := tls, auth := none}) -> "kafka_ssl";
toxiproxy_proxy_name(#{tls := plain, auth := _}) -> "kafka_sasl_plain";
toxiproxy_proxy_name(#{tls := tls, auth := _}) -> "kafka_sasl_ssl".

toxiproxy_host(#{auth := sasl_auth_kerberos}) -> <<"kafka-1.emqx.net">>;
toxiproxy_host(_) -> <<"toxiproxy.emqx.net">>.

group_path(Config) ->
    case emqx_common_test_helpers:group_path(Config) of
        [] ->
            undefined;
        Path ->
            Path
    end.

merge(Maps) ->
    lists:foldl(fun(M, Acc) -> emqx_utils_maps:deep_merge(Acc, M) end, #{}, Maps).

ensure_topic_and_producers(ConnectorConfig, SourceConfig, TestCase, TCConfig) ->
    #{tls := TLS, auth := Auth} = get_matrix_params(TCConfig),
    Topic = emqx_utils_maps:deep_get([<<"parameters">>, <<"topic">>], SourceConfig),
    [{Host, Port}] = emqx_bridge_kafka_impl:hosts(maps:get(<<"bootstrap_hosts">>, ConnectorConfig)),
    CreateConfig = maps:to_list(#{
        topic_mapping => [#{kafka_topic => Topic}],
        kafka_host => Host,
        kafka_port => Port,
        direct_kafka_host => Host,
        direct_kafka_port => Port,
        use_tls => TLS =:= tls,
        use_sasl => Auth =/= none,
        num_partitions => 1
    }),
    ok = emqx_bridge_kafka_impl_consumer_SUITE:ensure_topics(CreateConfig),
    ProducerConfigs = emqx_bridge_kafka_impl_consumer_SUITE:start_producers(TestCase, CreateConfig),
    [{kafka_producers, ProducerConfigs} | TCConfig].

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

connector_config(Name, Config) ->
    connector_config1(
        Name,
        connector_overrides(Config)
    ).

connector_config1(Name, Overrides0 = #{}) ->
    Overrides = emqx_utils_maps:binary_key_map(Overrides0),
    InnerConfigMap0 =
        #{
            <<"enable">> => true,
            <<"tags">> => [<<"bridge">>],
            <<"description">> => <<"my cool bridge">>,

            <<"authentication">> => <<"please override">>,
            <<"bootstrap_hosts">> => <<"please override">>,
            <<"connect_timeout">> => <<"5s">>,
            <<"metadata_request_timeout">> => <<"5s">>,
            <<"min_metadata_refresh_interval">> => <<"3s">>,

            <<"resource_opts">> =>
                #{
                    <<"health_check_interval">> => <<"2s">>,
                    <<"start_after_created">> => true,
                    <<"start_timeout">> => <<"5s">>
                }
        },
    InnerConfigMap = emqx_utils_maps:deep_merge(InnerConfigMap0, Overrides),
    emqx_bridge_v2_testlib:parse_and_check_connector(?SOURCE_TYPE_BIN, Name, InnerConfigMap).

connector_overrides(TCConfig) ->
    MatrixParams = #{tls := TLS} = get_matrix_params(TCConfig),
    Host = toxiproxy_host(MatrixParams),
    Port = toxiproxy_kafka_port(MatrixParams),
    BootstrapHosts = <<Host/binary, ":", (integer_to_binary(Port))/binary>>,
    AuthConfig = auth_config(TCConfig),
    #{
        <<"bootstrap_hosts">> => BootstrapHosts,
        <<"authentication">> => AuthConfig,
        <<"ssl">> => #{<<"enable">> => TLS =:= tls}
    }.

source_config(Overrides0) ->
    Overrides = emqx_utils_maps:binary_key_map(Overrides0),
    CommonConfig =
        #{
            <<"enable">> => true,
            <<"connector">> => <<"please override">>,
            <<"parameters">> =>
                #{
                    <<"key_encoding_mode">> => <<"none">>,
                    <<"max_batch_bytes">> => <<"896KB">>,
                    <<"max_wait_time">> => <<"500ms">>,
                    <<"max_rejoin_attempts">> => <<"5">>,
                    <<"offset_reset_policy">> => <<"earliest">>,
                    <<"topic">> => <<"please override">>,
                    <<"value_encoding_mode">> => <<"none">>
                },
            <<"resource_opts">> => #{
                <<"health_check_interval">> => <<"2s">>,
                <<"resume_interval">> => <<"2s">>
            }
        },
    emqx_utils_maps:deep_merge(CommonConfig, Overrides).

%%------------------------------------------------------------------------------
%% Testcases
%%------------------------------------------------------------------------------

t_start_stop(matrix) ->
    [
        [plain, none],
        [plain, sasl_auth_plain],
        [plain, sasl_auth_scram256],
        [plain, sasl_auth_scram512],
        [plain, sasl_auth_kerberos],
        [tls, none],
        [tls, sasl_auth_plain]
    ];
t_start_stop(Config) ->
    ok = emqx_bridge_v2_testlib:t_start_stop(Config, kafka_consumer_subcriber_and_client_stopped),
    ok.

t_create_via_http(Config) ->
    ok = emqx_bridge_v2_testlib:t_create_via_http(Config),
    ok.

t_consume(Config) ->
    Topic = ?config(kafka_topic, Config),
    NumPartitions = 1,
    Key = <<"mykey">>,
    Payload = #{<<"key">> => <<"value">>},
    Encoded = emqx_utils_json:encode(Payload),
    Headers = [{<<"hkey">>, <<"hvalue">>}],
    HeadersMap = maps:from_list(Headers),
    ProduceFn = fun() ->
        emqx_bridge_kafka_impl_consumer_SUITE:publish(
            Config,
            Topic,
            [
                #{
                    key => Key,
                    value => Encoded,
                    headers => Headers
                }
            ]
        )
    end,
    CheckFn = fun(Message) ->
        ?assertMatch(
            #{
                headers := HeadersMap,
                key := Key,
                offset := _,
                topic := Topic,
                ts := _,
                ts_type := _,
                value := Encoded
            },
            Message
        )
    end,
    ok = emqx_bridge_v2_testlib:t_consume(
        Config,
        #{
            test_timeout => timer:seconds(20),
            consumer_ready_tracepoint => ?match_n_events(
                NumPartitions,
                #{?snk_kind := kafka_consumer_subscriber_init}
            ),
            produce_fn => ProduceFn,
            check_fn => CheckFn,
            produce_tracepoint => ?match_event(
                #{
                    ?snk_kind := kafka_consumer_handle_message,
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
        [<<"kafka">>, <<"topic_mapping">>],
        V1Config0,
        [
            #{
                <<"kafka_topic">> => <<"old_topic">>,
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

t_bad_bootstrap_host(Config) ->
    ?assertMatch(
        {error, {{_, 400, _}, _, _}},
        emqx_bridge_v2_testlib:probe_connector_api(
            Config,
            #{
                <<"bootstrap_hosts">> => <<"bad_host:9999">>
            }
        )
    ),
    ok.

%% Checks that a group id is automatically generated if a custom one is not provided in
%% the config.
t_absent_group_id(Config) ->
    ?check_trace(
        begin
            #{<<"bootstrap_hosts">> := BootstrapHosts} = ?config(connector_config, Config),
            SourceConfig = ?config(source_config, Config),
            SourceName = ?config(source_name, Config),
            ?assertEqual(
                undefined,
                emqx_utils_maps:deep_get(
                    [<<"parameters">>, <<"group_id">>],
                    SourceConfig,
                    undefined
                )
            ),
            {ok, {{_, 201, _}, _, _}} = emqx_bridge_v2_testlib:create_bridge_api(Config),
            [Endpoint] = emqx_bridge_kafka_impl:hosts(BootstrapHosts),
            GroupId = emqx_bridge_kafka_impl_consumer:consumer_group_id(#{}, SourceName),
            ct:pal("generated group id: ~p", [GroupId]),
            ?retry(100, 10, begin
                {ok, Groups} = brod:list_groups(Endpoint, _ConnOpts = #{}),
                ?assertMatch(
                    [_],
                    [Group || Group = {_, Id, _} <- Groups, Id == GroupId],
                    #{groups => Groups}
                )
            end),
            ok
        end,
        []
    ),
    ok.

%% Checks that a group id is automatically generated if an empty string is provided in the
%% config.
t_empty_group_id(Config) ->
    ?check_trace(
        begin
            #{<<"bootstrap_hosts">> := BootstrapHosts} = ?config(connector_config, Config),
            SourceName = ?config(source_name, Config),
            {ok, {{_, 201, _}, _, _}} =
                emqx_bridge_v2_testlib:create_bridge_api(
                    Config,
                    #{<<"parameters">> => #{<<"group_id">> => <<"">>}}
                ),
            [Endpoint] = emqx_bridge_kafka_impl:hosts(BootstrapHosts),
            GroupId = emqx_bridge_kafka_impl_consumer:consumer_group_id(#{}, SourceName),
            ct:pal("generated group id: ~p", [GroupId]),
            ?retry(100, 10, begin
                {ok, Groups} = brod:list_groups(Endpoint, _ConnOpts = #{}),
                ?assertMatch(
                    [_],
                    [Group || Group = {_, Id, _} <- Groups, Id == GroupId],
                    #{groups => Groups}
                )
            end),
            ok
        end,
        []
    ),
    ok.

t_custom_group_id(Config) ->
    ?check_trace(
        begin
            #{<<"bootstrap_hosts">> := BootstrapHosts} = ?config(connector_config, Config),
            CustomGroupId = <<"my_group_id">>,
            {ok, {{_, 201, _}, _, _}} =
                emqx_bridge_v2_testlib:create_bridge_api(
                    Config,
                    #{<<"parameters">> => #{<<"group_id">> => CustomGroupId}}
                ),
            [Endpoint] = emqx_bridge_kafka_impl:hosts(BootstrapHosts),
            ?retry(100, 10, begin
                {ok, Groups} = brod:list_groups(Endpoint, _ConnOpts = #{}),
                ?assertMatch(
                    [_],
                    [Group || Group = {_, Id, _} <- Groups, Id == CustomGroupId],
                    #{groups => Groups}
                )
            end),
            ok
        end,
        []
    ),
    ok.

%% Currently, brod treats a consumer process to a specific topic as a singleton (per
%% client id / connector), meaning that the first subscriber to a given topic will define
%% the consumer options for all other consumers, and those options persist even after the
%% original consumer group is terminated.  We enforce that, if the user wants to consume
%% multiple times from the same topic, then they must create a different connector.
t_repeated_topics(Config) ->
    ?check_trace(
        begin
            %% first source is fine
            {ok, {{_, 201, _}, _, _}} =
                emqx_bridge_v2_testlib:create_bridge_api(Config),
            %% second source fails to create
            Name2 = <<"duplicated">>,
            {201, #{<<"error">> := Error}} =
                emqx_bridge_v2_testlib:create_source_api([{source_name, Name2} | Config]),
            ?assertEqual(
                match,
                re:run(Error, <<"Topics .* already exist in other sources">>, [{capture, none}]),
                #{error => Error}
            ),
            ok
        end,
        []
    ),
    ok.
