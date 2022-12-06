%%--------------------------------------------------------------------
%% Copyright (c) 2022 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_impl_kafka_consumer_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-import(emqx_common_test_helpers, [on_exit/1]).

-define(BRIDGE_TYPE_BIN, <<"kafka_consumer">>).

%%------------------------------------------------------------------------------
%% CT boilerplate
%%------------------------------------------------------------------------------

all() ->
    [
        {group, plain},
        {group, ssl},
        {group, sasl_plain},
        {group, sasl_ssl}
    ].

groups() ->
    TCs = emqx_common_test_helpers:all(?MODULE),
    SASLAuths = [
        sasl_auth_plain,
        sasl_auth_scram256,
        sasl_auth_scram512,
        sasl_auth_kerberos
    ],
    SASLAuthGroups = [{group, Type} || Type <- SASLAuths],
    SASLTests = [{Group, TCs} || Group <- SASLAuths],
    [
        {plain, TCs},
        {ssl, TCs},
        {sasl_plain, SASLAuthGroups},
        {sasl_ssl, SASLAuthGroups}
    ] ++ SASLTests.

sasl_only_tests() ->
    [t_failed_creation_then_fixed].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    emqx_mgmt_api_test_util:end_suite(),
    ok = emqx_common_test_helpers:stop_apps([emqx_conf]),
    ok = emqx_connector_test_helpers:stop_apps([emqx_bridge, emqx_resource]),
    _ = application:stop(emqx_connector),
    ok.

init_per_group(plain = Type, Config) ->
    KafkaHost = os:getenv("KAFKA_PLAIN_HOST", "kafka-1.emqx.net"),
    KafkaPort = list_to_integer(os:getenv("KAFKA_PLAIN_PORT", "9092")),
    ProxyName = "kafka_plain",
    case emqx_common_test_helpers:is_tcp_server_available(KafkaHost, KafkaPort) of
        true ->
            Config1 = common_init_per_group(),
            [
                {proxy_name, ProxyName},
                {kafka_host, KafkaHost},
                {kafka_port, KafkaPort},
                {kafka_type, Type},
                {use_sasl, false},
                {use_tls, false}
                | Config1 ++ Config
            ];
        false ->
            {skip, no_kafka}
    end;
init_per_group(sasl_plain = Type, Config) ->
    KafkaHost = os:getenv("KAFKA_SASL_PLAIN_HOST", "kafka-1.emqx.net"),
    KafkaPort = list_to_integer(os:getenv("KAFKA_SASL_PLAIN_PORT", "9093")),
    ProxyName = "kafka_sasl_plain",
    case emqx_common_test_helpers:is_tcp_server_available(KafkaHost, KafkaPort) of
        true ->
            Config1 = common_init_per_group(),
            [
                {proxy_name, ProxyName},
                {kafka_host, KafkaHost},
                {kafka_port, KafkaPort},
                {kafka_type, Type},
                {use_sasl, true},
                {use_tls, false}
                | Config1 ++ Config
            ];
        false ->
            {skip, no_kafka}
    end;
init_per_group(ssl = Type, Config) ->
    KafkaHost = os:getenv("KAFKA_SSL_HOST", "kafka-1.emqx.net"),
    KafkaPort = list_to_integer(os:getenv("KAFKA_SSL_PORT", "9094")),
    ProxyName = "kafka_ssl",
    case emqx_common_test_helpers:is_tcp_server_available(KafkaHost, KafkaPort) of
        true ->
            Config1 = common_init_per_group(),
            [
                {proxy_name, ProxyName},
                {kafka_host, KafkaHost},
                {kafka_port, KafkaPort},
                {kafka_type, Type},
                {use_sasl, false},
                {use_tls, true}
                | Config1 ++ Config
            ];
        false ->
            {skip, no_kafka}
    end;
init_per_group(sasl_ssl = Type, Config) ->
    KafkaHost = os:getenv("KAFKA_SASL_SSL_HOST", "kafka-1.emqx.net"),
    KafkaPort = list_to_integer(os:getenv("KAFKA_SASL_SSL_PORT", "9095")),
    ProxyName = "kafka_sasl_ssl",
    case emqx_common_test_helpers:is_tcp_server_available(KafkaHost, KafkaPort) of
        true ->
            Config1 = common_init_per_group(),
            [
                {proxy_name, ProxyName},
                {kafka_host, KafkaHost},
                {kafka_port, KafkaPort},
                {kafka_type, Type},
                {use_sasl, true},
                {use_tls, true}
                | Config1 ++ Config
            ];
        false ->
            {skip, no_kafka}
    end;
init_per_group(sasl_auth_plain, Config) ->
    [{sasl_auth_mechanism, plain} | Config];
init_per_group(sasl_auth_scram256, Config) ->
    [{sasl_auth_mechanism, scram_sha_256} | Config];
init_per_group(sasl_auth_scram512, Config) ->
    [{sasl_auth_mechanism, scram_sha_512} | Config];
init_per_group(sasl_auth_kerberos, Config) ->
    [{sasl_auth_mechanism, kerberos} | Config];
init_per_group(_Group, Config) ->
    Config.

common_init_per_group() ->
    ProxyHost = os:getenv("PROXY_HOST", "toxiproxy"),
    ProxyPort = list_to_integer(os:getenv("PROXY_PORT", "8474")),
    emqx_common_test_helpers:reset_proxy(ProxyHost, ProxyPort),
    ensure_loaded(),
    application:load(emqx_bridge),
    ok = emqx_common_test_helpers:start_apps([emqx_conf]),
    ok = emqx_connector_test_helpers:start_apps([emqx_resource, emqx_bridge]),
    {ok, _} = application:ensure_all_started(emqx_connector),
    emqx_mgmt_api_test_util:init_suite(),
    [
        {proxy_host, ProxyHost},
        {proxy_port, ProxyPort},
        {mqtt_topic, <<"mqtt/topic">>},
        {mqtt_qos, 0},
        {mqtt_payload, full_message},
        {pool_size, 1},
        {kafka_topic, <<"test-topic-three-partitions">>}
    ].

end_per_group(Group, Config) when
    Group =:= plain;
    Group =:= ssl;
    Group =:= sasl_plain;
    Group =:= sasl_ssl
->
    ProxyHost = ?config(proxy_host, Config),
    ProxyPort = ?config(proxy_port, Config),
    emqx_common_test_helpers:reset_proxy(ProxyHost, ProxyPort),
    delete_all_bridges(),
    ok;
end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(TestCase, Config) when
    TestCase =:= t_failed_creation_then_fixed
->
    KafkaType = ?config(kafka_type, Config),
    AuthMechanism = ?config(sasl_auth_mechanism, Config),
    IsSASL = lists:member(KafkaType, [sasl_plain, sasl_ssl]),
    case {IsSASL, AuthMechanism} of
        {true, kerberos} ->
            {skip, does_not_apply};
        {true, _} ->
            common_init_per_testcase(TestCase, Config);
        {false, _} ->
            {skip, does_not_apply}
    end;
init_per_testcase(TestCase, Config) ->
    common_init_per_testcase(TestCase, Config).

common_init_per_testcase(TestCase, Config) ->
    ct:timetrap(timer:seconds(60)),
    delete_all_bridges(),
    KafkaType = ?config(kafka_type, Config),
    {Name, ConfigString, KafkaConfig} = kafka_config(
        TestCase, KafkaType, Config
    ),
    #{
        producers := Producers,
        clientid := KafkaClientId
    } = start_producer(TestCase, Config),
    [
        {kafka_name, Name},
        {kafka_config_string, ConfigString},
        {kafka_config, KafkaConfig},
        {kafka_producers, Producers},
        {kafka_clientid, KafkaClientId}
        | Config
    ].

end_per_testcase(_Testcase, Config) ->
    ProxyHost = ?config(proxy_host, Config),
    ProxyPort = ?config(proxy_port, Config),
    Producers = ?config(kafka_producers, Config),
    KafkaClientId = ?config(kafka_clientid, Config),
    ok = snabbkaffe:stop(),
    emqx_common_test_helpers:reset_proxy(ProxyHost, ProxyPort),
    delete_all_bridges(),
    ok = wolff:stop_and_delete_supervised_producers(Producers),
    ok = wolff:stop_and_delete_supervised_client(KafkaClientId),
    emqx_common_test_helpers:call_janitor(),
    ok.

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

ensure_loaded() ->
    _ = application:load(emqx_ee_bridge),
    _ = emqx_ee_bridge:module_info(),
    ok.

start_producer(TestCase, Config) ->
    KafkaTopic = ?config(kafka_topic, Config),
    KafkaClientId = <<"test-client-", (atom_to_binary(TestCase))/binary>>,
    KafkaHost = ?config(kafka_host, Config),
    KafkaPort = ?config(kafka_port, Config),
    UseTLS = ?config(use_tls, Config),
    UseSASL = ?config(use_sasl, Config),
    Hosts = emqx_bridge_impl_kafka:hosts(KafkaHost ++ ":" ++ integer_to_list(KafkaPort)),
    SSL =
        case UseTLS of
            true ->
                %% hint: when running locally, need to
                %% `chmod og+rw` those files to be readable.
                emqx_tls_lib:to_client_opts(
                    #{
                        keyfile => shared_secret(client_keyfile),
                        certfile => shared_secret(client_certfile),
                        cacertfile => shared_secret(client_cacertfile),
                        verify => verify_none,
                        enable => true
                    }
                );
            false ->
                []
        end,
    SASL =
        case UseSASL of
            true -> {plain, <<"emqxuser">>, <<"password">>};
            false -> undefined
        end,
    ClientConfig = #{
        min_metadata_refresh_interval => 5_000,
        connect_timeout => 5_000,
        client_id => KafkaClientId,
        request_timeout => 1_000,
        sasl => SASL,
        ssl => SSL
    },
    {ok, Clients} = wolff:ensure_supervised_client(KafkaClientId, Hosts, ClientConfig),
    ProducerConfig =
        #{
            name => test_producer,
            partitioner => random,
            partition_count_refresh_interval_seconds => 1_000,
            replayq_max_total_bytes => 10_000,
            replayq_seg_bytes => 9_000,
            drop_if_highmem => false,
            required_acks => leader_only,
            max_batch_bytes => 900_000,
            max_send_ahead => 0,
            compression => no_compression,
            telemetry_meta_data => #{}
        },
    {ok, Producers} = wolff:ensure_supervised_producers(KafkaClientId, KafkaTopic, ProducerConfig),
    #{
        producers => Producers,
        clients => Clients,
        clientid => KafkaClientId
    }.

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

publish(Config, Messages) ->
    Producers = ?config(kafka_producers, Config),
    ct:pal("publishing: ~p", [Messages]),
    {_Partition, _OffsetReply} = wolff:send_sync(Producers, Messages, 10_000).

kafka_config(TestCase, _KafkaType, Config) ->
    KafkaHost = ?config(kafka_host, Config),
    KafkaPort = ?config(kafka_port, Config),
    KafkaTopic = ?config(kafka_topic, Config),
    AuthType = proplists:get_value(sasl_auth_mechanism, Config, none),
    UseTLS = proplists:get_value(use_tls, Config, false),
    Name = <<
        (atom_to_binary(TestCase))/binary, (integer_to_binary(erlang:unique_integer()))/binary
    >>,
    MQTTTopic = proplists:get_value(mqtt_topic, Config, <<"mqtt/topic">>),
    MQTTQoS = proplists:get_value(mqtt_qos, Config, 0),
    MQTTPayload = proplists:get_value(mqtt_payload, Config, full_message),
    PoolSize = proplists:get_value(pool_size, Config, 1),
    ConfigString =
        io_lib:format(
            "bridges.kafka_consumer.~s {\n"
            "  enable = true\n"
            "  bootstrap_hosts = \"~p:~b\"\n"
            "  connect_timeout = 5s\n"
            "  min_metadata_refresh_interval = 3s\n"
            "  metadata_request_timeout = 5s\n"
            "~s"
            "  kafka {\n"
            "    topic = ~s\n"
            "    max_batch_bytes = 896KB\n"
            %% todo: matrix this
            "    offset_reset_policy = reset_to_latest\n"
            "  }\n"
            "  mqtt {\n"
            "    topic = \"~s\"\n"
            "    qos = ~b\n"
            "    payload = ~p\n"
            "  }\n"
            "  pool_size = ~b\n"
            "  ssl {\n"
            "    enable = ~p\n"
            "    verify = verify_none\n"
            "    server_name_indication = \"auto\"\n"
            "  }\n"
            "}\n",
            [
                Name,
                KafkaHost,
                KafkaPort,
                authentication(AuthType),
                KafkaTopic,
                MQTTTopic,
                MQTTQoS,
                MQTTPayload,
                PoolSize,
                UseTLS
            ]
        ),
    {Name, ConfigString, parse_and_check(ConfigString, Name)}.

authentication(Type) when
    Type =:= scram_sha_256;
    Type =:= scram_sha_512;
    Type =:= plain
->
    io_lib:format(
        "  authentication = {\n"
        "    mechanism = ~p\n"
        "    username = emqxuser\n"
        "    password = password\n"
        "  }\n",
        [Type]
    );
authentication(kerberos) ->
    %% TODO: how to make this work locally outside docker???
    io_lib:format(
        "  authentication = {\n"
        "    kerberos_principal = rig@KDC.EMQX.NET\n"
        "    kerberos_keytab_file = \"~s\"\n"
        "  }\n",
        [shared_secret(rig_keytab)]
    );
authentication(_) ->
    "  authentication = none\n".

parse_and_check(ConfigString, Name) ->
    {ok, RawConf} = hocon:binary(ConfigString, #{format => map}),
    TypeBin = ?BRIDGE_TYPE_BIN,
    hocon_tconf:check_plain(emqx_bridge_schema, RawConf, #{required => false, atom_key => false}),
    #{<<"bridges">> := #{TypeBin := #{Name := Config}}} = RawConf,
    Config.

create_bridge(Config) ->
    create_bridge(Config, _Overrides = #{}).

create_bridge(Config, Overrides) ->
    Type = ?BRIDGE_TYPE_BIN,
    Name = ?config(kafka_name, Config),
    KafkaConfig0 = ?config(kafka_config, Config),
    KafkaConfig = emqx_map_lib:deep_merge(KafkaConfig0, Overrides),
    emqx_bridge:create(Type, Name, KafkaConfig).

delete_bridge(Config) ->
    Type = ?BRIDGE_TYPE_BIN,
    Name = ?config(kafka_name, Config),
    emqx_bridge:remove(Type, Name).

delete_all_bridges() ->
    lists:foreach(
        fun(#{name := Name, type := Type}) ->
            emqx_bridge:remove(Type, Name)
        end,
        emqx_bridge:list()
    ).

update_bridge_api(Config) ->
    update_bridge_api(Config, _Overrides = #{}).

update_bridge_api(Config, Overrides) ->
    TypeBin = ?BRIDGE_TYPE_BIN,
    Name = ?config(kafka_name, Config),
    KafkaConfig0 = ?config(kafka_config, Config),
    KafkaConfig = emqx_map_lib:deep_merge(KafkaConfig0, Overrides),
    BridgeId = emqx_bridge_resource:bridge_id(TypeBin, Name),
    Params = KafkaConfig#{<<"type">> => TypeBin, <<"name">> => Name},
    Path = emqx_mgmt_api_test_util:api_path(["bridges", BridgeId]),
    AuthHeader = emqx_mgmt_api_test_util:auth_header_(),
    ct:pal("updating bridge (via http): ~p", [Params]),
    Res =
        case emqx_mgmt_api_test_util:request_api(put, Path, "", AuthHeader, Params) of
            {ok, Res0} -> {ok, emqx_json:decode(Res0, [return_maps])};
            Error -> Error
        end,
    ct:pal("bridge creation result: ~p", [Res]),
    Res.

send_message(Config, Payload) ->
    Name = ?config(kafka_name, Config),
    Type = ?BRIDGE_TYPE_BIN,
    BridgeId = emqx_bridge_resource:bridge_id(Type, Name),
    emqx_bridge:send_message(BridgeId, Payload).

resource_id(Config) ->
    Type = ?BRIDGE_TYPE_BIN,
    Name = ?config(kafka_name, Config),
    emqx_bridge_resource:resource_id(Type, Name).

instance_id(Config) ->
    ResourceId = resource_id(Config),
    [{_, InstanceId}] = ets:lookup(emqx_resource_manager, {owner, ResourceId}),
    InstanceId.

receive_published() ->
    receive_published(#{}).

receive_published(Opts0) ->
    Default = #{n => 1, timeout => 10_000},
    Opts = maps:merge(Default, Opts0),
    receive_published(Opts, []).

receive_published(#{n := N, timeout := _Timeout}, Acc) when N =< 0 ->
    lists:reverse(Acc);
receive_published(#{n := N, timeout := Timeout} = Opts, Acc) ->
    receive
        {publish, Msg} ->
            receive_published(Opts#{n := N - 1}, [Msg | Acc])
    after Timeout ->
        error(
            {timeout, #{
                msgs_so_far => Acc,
                mailbox => process_info(self(), messages),
                expected_remaining => N
            }}
        )
    end.

ensure_connected(Config) ->
    ?assertMatch({ok, _}, get_client_connection(Config)),
    ok.

get_client_connection(Config) ->
    KafkaName = ?config(kafka_name, Config),
    KafkaHost = ?config(kafka_host, Config),
    KafkaPort = ?config(kafka_port, Config),
    ClientID = binary_to_atom(emqx_bridge_impl_kafka:make_client_id(KafkaName)),
    brod_client:get_connection(ClientID, KafkaHost, KafkaPort).

%%------------------------------------------------------------------------------
%% Testcases
%%------------------------------------------------------------------------------

t_start_and_consume_ok(Config) ->
    MQTTTopic = ?config(mqtt_topic, Config),
    MQTTQoS = ?config(mqtt_qos, Config),
    KafkaTopic = ?config(kafka_topic, Config),
    KafkaName = ?config(kafka_name, Config),
    ResourceId = emqx_bridge_resource:resource_id(kafka_consumer, KafkaName),
    ?assertMatch(
        {ok, _},
        create_bridge(Config)
    ),
    ensure_connected(Config),
    %% FIXME: the consumer is tremendously flaky without this
    %% sleep (rarely receives the published message, sometimes
    %% it does), specially if TLS is used... why???
    ct:sleep(5_000),
    {ok, C} = emqtt:start_link(),
    on_exit(fun() -> emqtt:stop(C) end),
    {ok, _} = emqtt:connect(C),
    {ok, _, [0]} = emqtt:subscribe(C, MQTTTopic),
    Payload = emqx_guid:to_hexstr(emqx_guid:gen()),

    ?check_trace(
        begin
            {Res, {ok, _}} =
                ?wait_async_action(
                    publish(Config, [
                        #{
                            key => <<"mykey">>,
                            value => Payload,
                            headers => [{<<"hkey">>, <<"hvalue">>}]
                        }
                    ]),
                    #{?snk_kind := kafka_consumer_handle_message_enter},
                    20_000
                ),
            Res
        end,
        fun({_Partition, OffsetReply}, Trace) ->
            ?assertMatch([_], ?of_kind(kafka_consumer_handle_message_enter, Trace)),
            Published = receive_published(),
            ?assertMatch(
                [
                    #{
                        qos := MQTTQoS,
                        topic := MQTTTopic,
                        payload := _
                    }
                ],
                Published
            ),
            [#{payload := PayloadBin}] = Published,
            ?assertMatch(
                #{
                    <<"value">> := Payload,
                    <<"key">> := <<"mykey">>,
                    <<"topic">> := KafkaTopic,
                    <<"offset">> := OffsetReply,
                    <<"headers">> := #{<<"hkey">> := <<"hvalue">>}
                },
                emqx_json:decode(PayloadBin, [return_maps]),
                #{
                    offset_reply => OffsetReply,
                    kafka_topic => KafkaTopic,
                    payload => Payload
                }
            ),
            ?assertEqual(1, emqx_resource_metrics:received_get(ResourceId)),
            ok
        end
    ),
    ok.

%% ensure that we can create and use the bridge successfully after
%% creating it with bad config.
t_failed_creation_then_fixed(Config) ->
    MQTTTopic = ?config(mqtt_topic, Config),
    MQTTQoS = ?config(mqtt_qos, Config),
    KafkaTopic = ?config(kafka_topic, Config),
    {ok, _} = create_bridge(Config, #{
        <<"authentication">> => #{<<"password">> => <<"wrong password">>}
    }),
    ct:sleep(5_000),
    ClientConn0 = get_client_connection(Config),
    case ClientConn0 of
        {error, client_down} ->
            ok;
        {error, {client_down, _Stacktrace}} ->
            ok;
        _ ->
            error({client_should_be_down, ClientConn0})
    end,
    %% now, update with the correct configuration
    ok = snabbkaffe:start_trace(),
    ?assertMatch(
        {{ok, _}, {ok, _}},
        ?wait_async_action(
            update_bridge_api(Config),
            #{?snk_kind := kafka_consumer_start_pool_started},
            10_000
        )
    ),
    ensure_connected(Config),
    %% FIXME: the consumer is tremendously flaky without this
    %% sleep (rarely receives the published message, sometimes
    %% it does), specially if TLS is used... why???
    ct:sleep(5_000),

    {ok, C} = emqtt:start_link(),
    on_exit(fun() -> emqtt:stop(C) end),
    {ok, _} = emqtt:connect(C),
    {ok, _, [0]} = emqtt:subscribe(C, MQTTTopic),
    Payload = emqx_guid:to_hexstr(emqx_guid:gen()),

    {_, {ok, _}} =
        ?wait_async_action(
            publish(Config, [
                #{
                    key => <<"mykey">>,
                    value => Payload,
                    headers => [{<<"hkey">>, <<"hvalue">>}]
                }
            ]),
            #{?snk_kind := kafka_consumer_handle_message_enter},
            20_000
        ),
    Published = receive_published(),
    ?assertMatch(
        [
            #{
                qos := MQTTQoS,
                topic := MQTTTopic,
                payload := _
            }
        ],
        Published
    ),
    [#{payload := PayloadBin}] = Published,
    ?assertMatch(
        #{
            <<"value">> := Payload,
            <<"key">> := <<"mykey">>,
            <<"topic">> := KafkaTopic,
            <<"offset">> := _,
            <<"headers">> := #{<<"hkey">> := <<"hvalue">>}
        },
        emqx_json:decode(PayloadBin, [return_maps]),
        #{
            kafka_topic => KafkaTopic,
            payload => Payload
        }
    ),
    ok.
