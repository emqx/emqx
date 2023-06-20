%%--------------------------------------------------------------------
%% Copyright (c) 2022-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_gcp_pubsub_consumer_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("jose/include/jose_jwt.hrl").
-include_lib("jose/include/jose_jws.hrl").

-define(BRIDGE_TYPE, gcp_pubsub_consumer).
-define(BRIDGE_TYPE_BIN, <<"gcp_pubsub_consumer">>).
-define(REPUBLISH_TOPIC, <<"republish/t">>).

-import(emqx_common_test_helpers, [on_exit/1]).

%%------------------------------------------------------------------------------
%% CT boilerplate
%%------------------------------------------------------------------------------

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    GCPEmulatorHost = os:getenv("GCP_EMULATOR_HOST", "toxiproxy"),
    GCPEmulatorPortStr = os:getenv("GCP_EMULATOR_PORT", "8085"),
    GCPEmulatorPort = list_to_integer(GCPEmulatorPortStr),
    ProxyHost = os:getenv("PROXY_HOST", "toxiproxy"),
    ProxyPort = list_to_integer(os:getenv("PROXY_PORT", "8474")),
    ProxyName = "gcp_emulator",
    case emqx_common_test_helpers:is_tcp_server_available(GCPEmulatorHost, GCPEmulatorPort) of
        true ->
            emqx_common_test_helpers:reset_proxy(ProxyHost, ProxyPort),
            ok = emqx_common_test_helpers:start_apps([emqx_conf]),
            ok = emqx_connector_test_helpers:start_apps([
                emqx_resource, emqx_bridge, emqx_rule_engine
            ]),
            {ok, _} = application:ensure_all_started(emqx_connector),
            emqx_mgmt_api_test_util:init_suite(),
            HostPort = GCPEmulatorHost ++ ":" ++ GCPEmulatorPortStr,
            true = os:putenv("PUBSUB_EMULATOR_HOST", HostPort),
            Client = start_control_connector(),
            [
                {proxy_name, ProxyName},
                {proxy_host, ProxyHost},
                {proxy_port, ProxyPort},
                {gcp_emulator_host, GCPEmulatorHost},
                {gcp_emulator_port, GCPEmulatorPort},
                {client, Client}
                | Config
            ];
        false ->
            case os:getenv("IS_CI") of
                "yes" ->
                    throw(no_gcp_emulator);
                _ ->
                    {skip, no_gcp_emulator}
            end
    end.

end_per_suite(Config) ->
    Client = ?config(client, Config),
    stop_control_connector(Client),
    emqx_mgmt_api_test_util:end_suite(),
    ok = emqx_common_test_helpers:stop_apps([emqx_conf]),
    ok = emqx_connector_test_helpers:stop_apps([emqx_bridge, emqx_resource, emqx_rule_engine]),
    _ = application:stop(emqx_connector),
    os:unsetenv("PUBSUB_EMULATOR_HOST"),
    ok.

init_per_testcase(TestCase, Config) ->
    common_init_per_testcase(TestCase, Config).

common_init_per_testcase(TestCase, Config0) ->
    ct:timetrap(timer:seconds(60)),
    emqx_bridge_testlib:delete_all_bridges(),
    emqx_config:delete_override_conf_files(),
    ConsumerTopic =
        <<
            (atom_to_binary(TestCase))/binary,
            (integer_to_binary(erlang:unique_integer()))/binary
        >>,
    UniqueNum = integer_to_binary(erlang:unique_integer()),
    MQTTTopic = proplists:get_value(mqtt_topic, Config0, <<"mqtt/topic/", UniqueNum/binary>>),
    MQTTQoS = proplists:get_value(mqtt_qos, Config0, 0),
    DefaultTopicMapping = [
        #{
            pubsub_topic => ConsumerTopic,
            mqtt_topic => MQTTTopic,
            qos => MQTTQoS,
            payload_template => <<"${.}">>
        }
    ],
    TopicMapping = proplists:get_value(topic_mapping, Config0, DefaultTopicMapping),
    ServiceAccountJSON =
        #{<<"project_id">> := ProjectId} =
        emqx_bridge_gcp_pubsub_utils:generate_service_account_json(),
    Config = [
        {consumer_topic, ConsumerTopic},
        {topic_mapping, TopicMapping},
        {service_account_json, ServiceAccountJSON},
        {project_id, ProjectId}
        | Config0
    ],
    {Name, ConfigString, ConsumerConfig} = consumer_config(TestCase, Config),
    ensure_topics(Config),
    ok = snabbkaffe:start_trace(),
    [
        {consumer_name, Name},
        {consumer_config_string, ConfigString},
        {consumer_config, ConsumerConfig}
        | Config
    ].

end_per_testcase(_Testcase, Config) ->
    case proplists:get_bool(skip_does_not_apply, Config) of
        true ->
            ok;
        false ->
            ProxyHost = ?config(proxy_host, Config),
            ProxyPort = ?config(proxy_port, Config),
            emqx_common_test_helpers:reset_proxy(ProxyHost, ProxyPort),
            emqx_bridge_testlib:delete_all_bridges(),
            emqx_common_test_helpers:call_janitor(60_000),
            ok = snabbkaffe:stop(),
            ok
    end.

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

consumer_config(TestCase, Config) ->
    UniqueNum = integer_to_binary(erlang:unique_integer()),
    ConsumerTopic = ?config(consumer_topic, Config),
    ServiceAccountJSON = ?config(service_account_json, Config),
    Name = <<
        (atom_to_binary(TestCase))/binary, UniqueNum/binary
    >>,
    ServiceAccountJSONStr = emqx_utils_json:encode(ServiceAccountJSON),
    MQTTTopic = proplists:get_value(mqtt_topic, Config, <<"mqtt/topic/", UniqueNum/binary>>),
    MQTTQoS = proplists:get_value(mqtt_qos, Config, 0),
    ConsumerWorkersPerTopic = proplists:get_value(consumer_workers_per_topic, Config, 1),
    DefaultTopicMapping = [
        #{
            pubsub_topic => ConsumerTopic,
            mqtt_topic => MQTTTopic,
            qos => MQTTQoS,
            payload_template => <<"${.}">>
        }
    ],
    TopicMapping0 = proplists:get_value(topic_mapping, Config, DefaultTopicMapping),
    TopicMappingStr = topic_mapping(TopicMapping0),
    ConfigString =
        io_lib:format(
            "bridges.gcp_pubsub_consumer.~s {\n"
            "  enable = true\n"
            %% gcp pubsub emulator doesn't do pipelining very well...
            "  pipelining = 1\n"
            "  connect_timeout = \"15s\"\n"
            "  service_account_json = ~s\n"
            "  consumer {\n"
            "    ack_retry_interval = \"5s\"\n"
            "    pull_max_messages = 10\n"
            "    consumer_workers_per_topic = ~b\n"
            %% topic mapping
            "~s"
            "  }\n"
            "  max_retries = 2\n"
            "  pipelining = 100\n"
            "  pool_size = 8\n"
            "  resource_opts {\n"
            "    health_check_interval = \"1s\"\n"
            "    request_ttl = \"15s\"\n"
            "  }\n"
            "}\n",
            [
                Name,
                ServiceAccountJSONStr,
                ConsumerWorkersPerTopic,
                TopicMappingStr
            ]
        ),
    {Name, ConfigString, parse_and_check(ConfigString, Name)}.

parse_and_check(ConfigString, Name) ->
    {ok, RawConf} = hocon:binary(ConfigString, #{format => map}),
    TypeBin = ?BRIDGE_TYPE_BIN,
    hocon_tconf:check_plain(emqx_bridge_schema, RawConf, #{required => false, atom_key => false}),
    #{<<"bridges">> := #{TypeBin := #{Name := Config}}} = RawConf,
    Config.

topic_mapping(TopicMapping0) ->
    Template0 = <<
        "{pubsub_topic = \"{{ pubsub_topic }}\","
        " mqtt_topic = \"{{ mqtt_topic }}\","
        " qos = {{ qos }},"
        " payload_template = \"{{{ payload_template }}}\" }"
    >>,
    Template = bbmustache:parse_binary(Template0),
    Entries =
        lists:map(
            fun(Params) ->
                bbmustache:compile(Template, Params, [{key_type, atom}])
            end,
            TopicMapping0
        ),
    iolist_to_binary(
        [
            "  topic_mapping = [",
            lists:join(<<",\n">>, Entries),
            "]\n"
        ]
    ).

ensure_topics(Config) ->
    TopicMapping = ?config(topic_mapping, Config),
    lists:foreach(
        fun(#{pubsub_topic := T}) ->
            ensure_topic(Config, T)
        end,
        TopicMapping
    ).

ensure_topic(Config, Topic) ->
    ProjectId = ?config(project_id, Config),
    Client = ?config(client, Config),
    Method = put,
    Path = <<"/v1/projects/", ProjectId/binary, "/topics/", Topic/binary>>,
    Body = <<"{}">>,
    Res = emqx_bridge_gcp_pubsub_client:query_sync(
        {prepared_request, {Method, Path, Body}},
        Client
    ),
    case Res of
        {ok, _} ->
            ok;
        {error, #{status_code := 409}} ->
            %% already exists
            ok
    end,
    ok.

start_control_connector() ->
    RawServiceAccount = emqx_bridge_gcp_pubsub_utils:generate_service_account_json(),
    ServiceAccount = emqx_utils_maps:unsafe_atom_key_map(RawServiceAccount),
    ConnectorConfig =
        #{
            connect_timeout => 5_000,
            max_retries => 0,
            pool_size => 1,
            resource_opts => #{request_ttl => 5_000},
            service_account_json => ServiceAccount
        },
    PoolName = <<"control_connector">>,
    {ok, Client} = emqx_bridge_gcp_pubsub_client:start(PoolName, ConnectorConfig),
    Client.

stop_control_connector(Client) ->
    ok = emqx_bridge_gcp_pubsub_client:stop(Client),
    ok.

pubsub_publish(Config, Topic, Messages0) ->
    Client = ?config(client, Config),
    ProjectId = ?config(project_id, Config),
    Method = post,
    Path = <<"/v1/projects/", ProjectId/binary, "/topics/", Topic/binary, ":publish">>,
    Messages =
        lists:map(
            fun(Msg) ->
                emqx_utils_maps:update_if_present(
                    <<"data">>,
                    fun
                        (D) when is_binary(D) -> base64:encode(D);
                        (M) when is_map(M) -> base64:encode(emqx_utils_json:encode(M))
                    end,
                    Msg
                )
            end,
            Messages0
        ),
    Body = emqx_utils_json:encode(#{<<"messages">> => Messages}),
    {ok, _} = emqx_bridge_gcp_pubsub_client:query_sync(
        {prepared_request, {Method, Path, Body}},
        Client
    ),
    ok.

create_bridge(Config) ->
    create_bridge(Config, _Overrides = #{}).

create_bridge(Config, Overrides) ->
    Type = ?BRIDGE_TYPE_BIN,
    Name = ?config(consumer_name, Config),
    BridgeConfig0 = ?config(consumer_config, Config),
    BridgeConfig = emqx_utils_maps:deep_merge(BridgeConfig0, Overrides),
    emqx_bridge:create(Type, Name, BridgeConfig).

create_bridge_api(Config) ->
    create_bridge_api(Config, _Overrides = #{}).

create_bridge_api(Config, Overrides) ->
    TypeBin = ?BRIDGE_TYPE_BIN,
    Name = ?config(consumer_name, Config),
    BridgeConfig0 = ?config(consumer_config, Config),
    BridgeConfig = emqx_utils_maps:deep_merge(BridgeConfig0, Overrides),
    Params = BridgeConfig#{<<"type">> => TypeBin, <<"name">> => Name},
    Path = emqx_mgmt_api_test_util:api_path(["bridges"]),
    AuthHeader = emqx_mgmt_api_test_util:auth_header_(),
    Opts = #{return_all => true},
    ct:pal("creating bridge (via http): ~p", [Params]),
    Res =
        case emqx_mgmt_api_test_util:request_api(post, Path, "", AuthHeader, Params, Opts) of
            {ok, {Status, Headers, Body0}} ->
                {ok, {Status, Headers, emqx_utils_json:decode(Body0, [return_maps])}};
            Error ->
                Error
        end,
    ct:pal("bridge create result: ~p", [Res]),
    Res.

probe_bridge_api(Config) ->
    TypeBin = ?BRIDGE_TYPE_BIN,
    Name = ?config(consumer_name, Config),
    ConsumerConfig = ?config(consumer_config, Config),
    Params = ConsumerConfig#{<<"type">> => TypeBin, <<"name">> => Name},
    Path = emqx_mgmt_api_test_util:api_path(["bridges_probe"]),
    AuthHeader = emqx_mgmt_api_test_util:auth_header_(),
    Opts = #{return_all => true},
    ct:pal("probing bridge (via http): ~p", [Params]),
    Res =
        case emqx_mgmt_api_test_util:request_api(post, Path, "", AuthHeader, Params, Opts) of
            {ok, {{_, 204, _}, _Headers, _Body0} = Res0} -> {ok, Res0};
            Error -> Error
        end,
    ct:pal("bridge probe result: ~p", [Res]),
    Res.

start_and_subscribe_mqtt(Config) ->
    TopicMapping = ?config(topic_mapping, Config),
    {ok, C} = emqtt:start_link([{proto_ver, v5}]),
    on_exit(fun() -> emqtt:stop(C) end),
    {ok, _} = emqtt:connect(C),
    lists:foreach(
        fun(#{mqtt_topic := MQTTTopic}) ->
            {ok, _, [2]} = emqtt:subscribe(C, MQTTTopic, _QoS = 2)
        end,
        TopicMapping
    ),
    ok.

resource_id(Config) ->
    Type = ?BRIDGE_TYPE_BIN,
    Name = ?config(consumer_name, Config),
    emqx_bridge_resource:resource_id(Type, Name).

receive_published() ->
    receive_published(#{}).

receive_published(Opts0) ->
    Default = #{n => 1, timeout => 20_000},
    Opts = maps:merge(Default, Opts0),
    receive_published(Opts, []).

receive_published(#{n := N, timeout := _Timeout}, Acc) when N =< 0 ->
    {ok, lists:reverse(Acc)};
receive_published(#{n := N, timeout := Timeout} = Opts, Acc) ->
    receive
        {publish, Msg0 = #{payload := Payload}} ->
            Msg =
                case emqx_utils_json:safe_decode(Payload, [return_maps]) of
                    {ok, Decoded} -> Msg0#{payload := Decoded};
                    {error, _} -> Msg0
                end,
            receive_published(Opts#{n := N - 1}, [Msg | Acc])
    after Timeout ->
        {timeout, #{
            msgs_so_far => Acc,
            mailbox => process_info(self(), messages),
            expected_remaining => N
        }}
    end.

create_rule_and_action_http(Config) ->
    ConsumerName = ?config(consumer_name, Config),
    BridgeId = emqx_bridge_resource:bridge_id(?BRIDGE_TYPE_BIN, ConsumerName),
    ActionFn = <<(atom_to_binary(?MODULE))/binary, ":action_response">>,
    Params = #{
        enable => true,
        sql => <<"SELECT * FROM \"$bridges/", BridgeId/binary, "\"">>,
        actions =>
            [
                #{
                    <<"function">> => <<"republish">>,
                    <<"args">> =>
                        #{
                            <<"topic">> => ?REPUBLISH_TOPIC,
                            <<"payload">> => <<>>,
                            <<"qos">> => 0,
                            <<"retain">> => false,
                            <<"user_properties">> => <<"${headers}">>
                        }
                },
                #{<<"function">> => ActionFn}
            ]
    },
    Path = emqx_mgmt_api_test_util:api_path(["rules"]),
    AuthHeader = emqx_mgmt_api_test_util:auth_header_(),
    ct:pal("rule action params: ~p", [Params]),
    case emqx_mgmt_api_test_util:request_api(post, Path, "", AuthHeader, Params) of
        {ok, Res = #{<<"id">> := RuleId}} ->
            on_exit(fun() -> ok = emqx_rule_engine:delete_rule(RuleId) end),
            {ok, emqx_utils_json:decode(Res, [return_maps])};
        Error ->
            Error
    end.

action_response(Selected, Envs, Args) ->
    ?tp(action_response, #{
        selected => Selected,
        envs => Envs,
        args => Args
    }),
    ok.

assert_non_received_metrics(BridgeName) ->
    Metrics = emqx_bridge:get_metrics(?BRIDGE_TYPE, BridgeName),
    #{counters := Counters0, gauges := Gauges} = Metrics,
    Counters = maps:remove(received, Counters0),
    ?assert(lists:all(fun(V) -> V == 0 end, maps:values(Counters)), #{metrics => Metrics}),
    ?assert(lists:all(fun(V) -> V == 0 end, maps:values(Gauges)), #{metrics => Metrics}),
    ok.

%%------------------------------------------------------------------------------
%% Trace properties
%%------------------------------------------------------------------------------

prop_pulled_only_once(Trace) ->
    PulledIds = ?projection(
        message_id, ?of_kind("gcp_pubsub_consumer_worker_handle_message", Trace)
    ),
    NumPulled = length(PulledIds),
    UniqueNumPulled = sets:size(sets:from_list(PulledIds, [{version, 2}])),
    ?assertEqual(UniqueNumPulled, NumPulled),
    ok.

prop_all_pulled_are_acked(Trace) ->
    PulledAckIds = ?projection(
        ack_id, ?of_kind("gcp_pubsub_consumer_worker_handle_message", Trace)
    ),
    AckedIds0 = ?projection(ack_ids, ?of_kind(gcp_pubsub_consumer_worker_acknowledged, Trace)),
    AckedIds = lists:flatten(AckedIds0),
    ?assertEqual(
        sets:from_list(PulledAckIds, [{version, 2}]),
        sets:from_list(AckedIds, [{version, 2}])
    ),
    ok.

%%------------------------------------------------------------------------------
%% Testcases
%%------------------------------------------------------------------------------

t_consume_ok(Config) ->
    BridgeName = ?config(consumer_name, Config),
    TopicMapping = ?config(topic_mapping, Config),
    ResourceId = resource_id(Config),
    ?check_trace(
        begin
            start_and_subscribe_mqtt(Config),
            ?assertMatch(
                {{ok, _}, {ok, _}},
                ?wait_async_action(
                    create_bridge(Config),
                    #{?snk_kind := "gcp_pubsub_consumer_worker_subscription_ready"},
                    40_000
                )
            ),
            [
                #{
                    pubsub_topic := Topic,
                    mqtt_topic := MQTTTopic,
                    qos := QoS
                }
            ] = TopicMapping,
            Payload0 = emqx_guid:to_hexstr(emqx_guid:gen()),
            Messages0 = [
                #{
                    <<"data">> => Data0 = #{<<"value">> => Payload0},
                    <<"attributes">> => Attributes0 = #{<<"key">> => <<"value">>},
                    <<"orderingKey">> => <<"some_ordering_key">>
                }
            ],
            pubsub_publish(Config, Topic, Messages0),
            {ok, Published0} = receive_published(),
            EncodedData0 = emqx_utils_json:encode(Data0),
            ?assertMatch(
                [
                    #{
                        qos := QoS,
                        topic := MQTTTopic,
                        payload :=
                            #{
                                <<"attributes">> := Attributes0,
                                <<"message_id">> := MsgId,
                                <<"ordering_key">> := <<"some_ordering_key">>,
                                <<"publish_time">> := PubTime,
                                <<"topic">> := Topic,
                                <<"value">> := EncodedData0
                            }
                    }
                ] when is_binary(MsgId) andalso is_binary(PubTime),
                Published0
            ),
            %% no need to check return value; we check the property in
            %% the check phase.  this is just to give it a chance to do
            %% so and avoid flakiness.  should be fast.
            ?block_until(#{?snk_kind := gcp_pubsub_consumer_worker_acknowledged}, 1_000),
            ?retry(
                _Interval = 200,
                _NAttempts = 20,
                ?assertEqual(1, emqx_resource_metrics:received_get(ResourceId))
            ),

            %% Batch with only data and only attributes
            Payload1 = emqx_guid:to_hexstr(emqx_guid:gen()),
            Messages1 = [
                #{<<"data">> => Data1 = #{<<"val">> => Payload1}},
                #{<<"attributes">> => Attributes1 = #{<<"other_key">> => <<"other_value">>}}
            ],
            pubsub_publish(Config, Topic, Messages1),
            {ok, Published1} = receive_published(#{n => 2}),
            EncodedData1 = emqx_utils_json:encode(Data1),
            ?assertMatch(
                [
                    #{
                        qos := QoS,
                        topic := MQTTTopic,
                        payload :=
                            #{
                                <<"message_id">> := _,
                                <<"publish_time">> := _,
                                <<"topic">> := Topic,
                                <<"value">> := EncodedData1
                            }
                    },
                    #{
                        qos := QoS,
                        topic := MQTTTopic,
                        payload :=
                            #{
                                <<"attributes">> := Attributes1,
                                <<"message_id">> := _,
                                <<"publish_time">> := _,
                                <<"topic">> := Topic
                            }
                    }
                ],
                Published1
            ),
            ?assertNotMatch(
                [
                    #{payload := #{<<"attributes">> := _, <<"ordering_key">> := _}},
                    #{payload := #{<<"value">> := _, <<"ordering_key">> := _}}
                ],
                Published1
            ),
            %% no need to check return value; we check the property in
            %% the check phase.  this is just to give it a chance to do
            %% so and avoid flakiness.  should be fast.
            ?block_until(
                #{?snk_kind := gcp_pubsub_consumer_worker_acknowledged, ack_ids := [_, _]}, 1_000
            ),
            ?retry(
                _Interval = 200,
                _NAttempts = 20,
                ?assertEqual(3, emqx_resource_metrics:received_get(ResourceId))
            ),

            %% Check that the bridge probe API doesn't leak atoms.
            ProbeRes0 = probe_bridge_api(Config),
            ?assertMatch({ok, {{_, 204, _}, _Headers, _Body}}, ProbeRes0),
            AtomsBefore = erlang:system_info(atom_count),
            %% Probe again; shouldn't have created more atoms.
            ProbeRes1 = probe_bridge_api(Config),
            ?assertMatch({ok, {{_, 204, _}, _Headers, _Body}}, ProbeRes1),
            AtomsAfter = erlang:system_info(atom_count),
            ?assertEqual(AtomsBefore, AtomsAfter),

            assert_non_received_metrics(BridgeName),

            ok
        end,
        [
            {"all pulled ack ids are acked", fun ?MODULE:prop_all_pulled_are_acked/1},
            {"all pulled message ids are unique", fun ?MODULE:prop_pulled_only_once/1}
        ]
    ),
    ok.

t_bridge_rule_action_source(Config) ->
    BridgeName = ?config(consumer_name, Config),
    TopicMapping = ?config(topic_mapping, Config),
    ResourceId = resource_id(Config),
    ?check_trace(
        begin
            ?assertMatch(
                {{ok, _}, {ok, _}},
                ?wait_async_action(
                    create_bridge(Config),
                    #{?snk_kind := "gcp_pubsub_consumer_worker_subscription_ready"},
                    40_000
                )
            ),
            {ok, _} = create_rule_and_action_http(Config),

            [#{pubsub_topic := PubSubTopic}] = TopicMapping,
            {ok, C} = emqtt:start_link([{proto_ver, v5}]),
            on_exit(fun() -> emqtt:stop(C) end),
            {ok, _} = emqtt:connect(C),
            {ok, _, [0]} = emqtt:subscribe(C, ?REPUBLISH_TOPIC),

            Payload0 = emqx_guid:to_hexstr(emqx_guid:gen()),
            Messages0 = [
                #{
                    <<"data">> => Data0 = #{<<"payload">> => Payload0},
                    <<"attributes">> => Attributes0 = #{<<"key">> => <<"value">>}
                }
            ],
            {_, {ok, _}} =
                ?wait_async_action(
                    pubsub_publish(Config, PubSubTopic, Messages0),
                    #{?snk_kind := action_response},
                    5_000
                ),
            Published0 = receive_published(),
            EncodedData0 = emqx_utils_json:encode(Data0),
            ?assertMatch(
                {ok, [
                    #{
                        topic := ?REPUBLISH_TOPIC,
                        qos := 0,
                        payload := #{
                            <<"event">> := <<"$bridges/", _/binary>>,
                            <<"message_id">> := _,
                            <<"metadata">> := #{<<"rule_id">> := _},
                            <<"publish_time">> := _,
                            <<"topic">> := PubSubTopic,
                            <<"attributes">> := Attributes0,
                            <<"value">> := EncodedData0
                        }
                    }
                ]},
                Published0
            ),
            ?retry(
                _Interval = 200,
                _NAttempts = 20,
                ?assertEqual(1, emqx_resource_metrics:received_get(ResourceId))
            ),

            assert_non_received_metrics(BridgeName),

            #{payload => Payload0}
        end,
        [{"all pulled message ids are unique", fun ?MODULE:prop_pulled_only_once/1}]
    ),
    ok.

%% TODO TEST:
%%   * multi-topic mapping
%%   * get status
%%   * 2+ pull workers do not duplicate delivered messages
%%   * inexistent topic
%%   * connection cut then restored
%%   * pull worker death
%%   * async worker death mid-pull
%%   * ensure subscription creation error
%%   * cluster subscription
%%   * connection down during pull
%%   * connection down during ack
%%   * topic deleted while consumer is running
%%   * subscription deleted while consumer is running
%%   * ensure client is terminated when bridge stops
