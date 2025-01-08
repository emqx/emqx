%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_kafka_impl_producer_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("brod/include/brod.hrl").

-define(PRODUCER, emqx_bridge_kafka_impl_producer).

%%------------------------------------------------------------------------------
%% Things for REST API tests
%%------------------------------------------------------------------------------

-import(
    emqx_common_test_http,
    [
        request_api/3,
        request_api/5,
        get_http_data/1
    ]
).

-include_lib("eunit/include/eunit.hrl").

-define(HOST, "http://127.0.0.1:18083").
-define(BASE_PATH, "/api/v5").

%% NOTE: it's "kafka", but not "kafka_producer"
%% because we want to test the v1 interface
-define(BRIDGE_TYPE, "kafka").
-define(BRIDGE_TYPE_V2, "kafka_producer").
-define(BRIDGE_TYPE_BIN, <<"kafka">>).

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
        t_rest_api,
        t_publish,
        t_send_message_with_headers,
        t_wrong_headers_from_message
    ].

test_topic_one_partition() ->
    "test-topic-one-partition".

wait_until_kafka_is_up() ->
    wait_until_kafka_is_up(0).

wait_until_kafka_is_up(300) ->
    ct:fail("Kafka is not up even though we have waited for a while");
wait_until_kafka_is_up(Attempts) ->
    KafkaTopic = test_topic_one_partition(),
    case resolve_kafka_offset(kafka_hosts(), KafkaTopic, 0) of
        {ok, _} ->
            ok;
        _ ->
            timer:sleep(1000),
            wait_until_kafka_is_up(Attempts + 1)
    end.

init_per_suite(Config0) ->
    Config =
        case os:getenv("DEBUG_CASE") of
            [_ | _] = DebugCase ->
                CaseName = list_to_atom(DebugCase),
                [{debug_case, CaseName} | Config0];
            _ ->
                Config0
        end,
    Apps = emqx_cth_suite:start(
        [
            emqx,
            emqx_conf,
            emqx_connector,
            emqx_bridge_kafka,
            emqx_bridge,
            emqx_rule_engine,
            {emqx_dashboard, "dashboard.listeners.http { enable = true, bind = 18083 }"}
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    wait_until_kafka_is_up(),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    Apps = ?config(apps, Config),
    emqx_mgmt_api_test_util:end_suite(),
    ok = emqx_cth_suite:stop(Apps),
    ok.

init_per_testcase(TestCase, Config) ->
    case proplists:get_value(debug_case, Config) of
        TestCase ->
            emqx_logger:set_log_level(debug);
        _ ->
            ok
    end,
    Config.

end_per_testcase(_TestCase, _Config) ->
    delete_all_bridges(),
    ok.

set_special_configs(emqx_dashboard) ->
    emqx_dashboard_api_test_helpers:set_default_config(),
    ok;
set_special_configs(_) ->
    ok.

%%------------------------------------------------------------------------------
%% Test case for the query_mode parameter
%%------------------------------------------------------------------------------

t_query_mode_sync(CtConfig) ->
    %% We need this because on_query_async is in a different group
    ?check_trace(
        begin
            test_publish(kafka_hosts_string(), #{"query_mode" => "sync"}, CtConfig)
        end,
        fun(Trace) ->
            %% We should have a sync Snabbkaffe trace
            ?assertMatch([_], ?of_kind(simple_sync_internal_buffer_query, Trace))
        end
    ).

t_query_mode_async(CtConfig) ->
    ?check_trace(
        begin
            test_publish(kafka_hosts_string(), #{"query_mode" => "async"}, CtConfig)
        end,
        fun(Trace) ->
            %% We should have an async Snabbkaffe trace
            ?assertMatch([_], ?of_kind(emqx_bridge_kafka_impl_producer_async_query, Trace))
        end
    ).

%%------------------------------------------------------------------------------
%% Test cases for all combinations of SSL, no SSL and authentication types
%%------------------------------------------------------------------------------

t_publish(matrix) ->
    {publish, [
        [tcp, none, key_dispatch, sync],
        [ssl, plain_passfile, random, sync],
        [ssl, scram_sha512, random, async],
        [ssl, kerberos, random, sync]
    ]};
t_publish(Config) ->
    Path = group_path(Config),
    ct:comment(Path),
    [Transport, Auth, Partitioner, QueryMode] = Path,
    Hosts = kafka_hosts_string(Transport, Auth),
    SSL =
        case Transport of
            tcp ->
                #{"enable" => "false"};
            ssl ->
                valid_ssl_settings()
        end,
    Auth1 =
        case Auth of
            none ->
                "none";
            plain_passfile ->
                Passfile = filename:join(?config(priv_dir, Config), "passfile"),
                valid_sasl_plain_passfile_settings(Passfile);
            scram_sha512 ->
                valid_sasl_scram512_settings();
            kerberos ->
                valid_sasl_kerberos_settings()
        end,
    ConnCfg = #{
        "bootstrap_hosts" => Hosts,
        "ssl" => SSL,
        "authentication" => Auth1,
        "partition_strategy" => atom_to_list(Partitioner),
        "query_mode" => atom_to_list(QueryMode)
    },
    ok = test_publish(Hosts, ConnCfg, Config).

%%------------------------------------------------------------------------------
%% Test cases for REST api
%%------------------------------------------------------------------------------

t_rest_api(matrix) ->
    {rest_api, [
        [tcp, none],
        [tcp, plain],
        [ssl, scram_sha256],
        [ssl, kerberos]
    ]};
t_rest_api(Config) ->
    Path = group_path(Config),
    ct:comment(Path),
    [Transport, Auth] = Path,
    Hosts = kafka_hosts_string(Transport, Auth),
    SSL =
        case Transport of
            tcp ->
                bin_map(#{"enable" => "false"});
            ssl ->
                bin_map(valid_ssl_settings())
        end,
    Auth1 =
        case Auth of
            none -> <<"none">>;
            plain -> bin_map(valid_sasl_plain_settings());
            scram_sha256 -> bin_map(valid_sasl_scram256_settings());
            kerberos -> bin_map(valid_sasl_kerberos_settings())
        end,
    Cfg = #{
        <<"ssl">> => SSL,
        <<"authentication">> => Auth1,
        <<"bootstrap_hosts">> => Hosts
    },
    ok = kafka_bridge_rest_api_helper(Cfg).

http_get_bridges(UrlPath, Name0) ->
    Name = iolist_to_binary(Name0),
    {ok, _Code, BridgesData} = http_get(UrlPath),
    Bridges = json(BridgesData),
    lists:filter(
        fun
            (#{<<"name">> := N}) when N =:= Name -> true;
            (_) -> false
        end,
        Bridges
    ).

kafka_bridge_rest_api_helper(Config) ->
    BridgeType = ?BRIDGE_TYPE,
    BridgeName = "my_kafka_bridge",
    BridgeID = emqx_bridge_resource:bridge_id(
        list_to_binary(BridgeType),
        list_to_binary(BridgeName)
    ),
    UrlEscColon = "%3A",
    BridgesProbeParts = ["bridges_probe"],
    BridgeIdUrlEnc = BridgeType ++ UrlEscColon ++ BridgeName,
    BridgesParts = ["bridges"],
    BridgesPartsIdDeleteAlsoActions = ["bridges", BridgeIdUrlEnc ++ "?also_delete_dep_actions"],
    OpUrlFun = fun(OpName) -> ["bridges", BridgeIdUrlEnc, OpName] end,
    EnableFun = fun(Enable) -> ["bridges", BridgeIdUrlEnc, "enable", Enable] end,
    BridgesPartsOpDisable = EnableFun("false"),
    BridgesPartsOpEnable = EnableFun("true"),
    BridgesPartsOpRestart = OpUrlFun("restart"),
    BridgesPartsOpStop = OpUrlFun("stop"),
    %% List bridges
    %% Delete if my_kafka_bridge exists
    case http_get_bridges(BridgesParts, BridgeName) of
        [_] ->
            %% Delete the bridge my_kafka_bridge
            {ok, 204, <<>>} = http_delete(BridgesPartsIdDeleteAlsoActions);
        [] ->
            ok
    end,
    try
        ?assertEqual([], http_get_bridges(BridgesParts, BridgeName)),
        %% Create new Kafka bridge
        KafkaTopic = test_topic_one_partition(),
        CreateBodyTmp = #{
            <<"type">> => <<?BRIDGE_TYPE>>,
            <<"name">> => <<"my_kafka_bridge">>,
            <<"bootstrap_hosts">> => iolist_to_binary(maps:get(<<"bootstrap_hosts">>, Config)),
            <<"enable">> => true,
            <<"authentication">> => maps:get(<<"authentication">>, Config),
            <<"local_topic">> => <<"t/#">>,
            <<"kafka">> => #{
                <<"topic">> => iolist_to_binary(KafkaTopic),
                <<"buffer">> => #{<<"memory_overload_protection">> => <<"false">>},
                <<"message">> => #{
                    <<"key">> => <<"${clientid}">>,
                    <<"value">> => <<"${.payload}">>
                }
            }
        },
        CreateBody = CreateBodyTmp#{<<"ssl">> => maps:get(<<"ssl">>, Config)},
        {ok, 201, _Data} = http_post(BridgesParts, CreateBody),
        %% Check that the new bridge is in the list of bridges
        ?assertMatch([#{<<"type">> := <<"kafka">>}], http_get_bridges(BridgesParts, BridgeName)),
        %% Probe should work
        %% no extra atoms should be created when probing
        AtomsBefore = erlang:system_info(atom_count),
        {ok, 204, _} = http_post(BridgesProbeParts, CreateBody),
        AtomsAfter = erlang:system_info(atom_count),
        ?assertEqual(AtomsBefore, AtomsAfter),
        {ok, 204, _X} = http_post(BridgesProbeParts, CreateBody),
        %% Create a rule that uses the bridge
        {ok, 201, Rule} = http_post(
            ["rules"],
            #{
                <<"name">> => <<"kafka_bridge_rest_api_helper_rule">>,
                <<"enable">> => true,
                <<"actions">> => [BridgeID],
                <<"sql">> => <<"SELECT * from \"kafka_bridge_topic/#\"">>
            }
        ),
        #{<<"id">> := RuleId} = emqx_utils_json:decode(Rule, [return_maps]),
        BridgeV2Id = emqx_bridge_v2:id(
            list_to_binary(?BRIDGE_TYPE_V2),
            list_to_binary(BridgeName)
        ),
        %% counters should be empty before
        ?assertEqual(0, emqx_resource_metrics:matched_get(BridgeV2Id)),
        ?assertEqual(0, emqx_resource_metrics:success_get(BridgeV2Id)),
        ?assertEqual(0, emqx_resource_metrics:dropped_get(BridgeV2Id)),
        ?assertEqual(0, emqx_resource_metrics:failed_get(BridgeV2Id)),
        ?assertEqual(0, emqx_resource_metrics:inflight_get(BridgeV2Id)),
        ?assertEqual(0, emqx_resource_metrics:queuing_get(BridgeV2Id)),
        ?assertEqual(0, emqx_resource_metrics:dropped_other_get(BridgeV2Id)),
        ?assertEqual(0, emqx_resource_metrics:dropped_queue_full_get(BridgeV2Id)),
        ?assertEqual(0, emqx_resource_metrics:dropped_resource_not_found_get(BridgeV2Id)),
        ?assertEqual(0, emqx_resource_metrics:dropped_resource_stopped_get(BridgeV2Id)),
        ?assertEqual(0, emqx_resource_metrics:retried_get(BridgeV2Id)),
        ?assertEqual(0, emqx_resource_metrics:retried_failed_get(BridgeV2Id)),
        ?assertEqual(0, emqx_resource_metrics:retried_success_get(BridgeV2Id)),
        %% Get offset before sending message
        {ok, Offset} = resolve_kafka_offset(kafka_hosts(), KafkaTopic, 0),
        %% Send message to topic and check that it got forwarded to Kafka
        Body = <<"message from EMQX">>,
        emqx:publish(emqx_message:make(<<"kafka_bridge_topic/1">>, Body)),
        %% Give Kafka some time to get message
        timer:sleep(100),
        % %% Check that Kafka got message
        BrodOut = brod:fetch(kafka_hosts(), KafkaTopic, 0, Offset),
        {ok, {_, [KafkaMsg]}} = BrodOut,
        Body = KafkaMsg#kafka_message.value,
        %% Check crucial counters and gauges
        ?assertEqual(1, emqx_resource_metrics:matched_get(BridgeV2Id)),
        ?assertEqual(1, emqx_resource_metrics:success_get(BridgeV2Id)),
        ?assertEqual(1, emqx_metrics_worker:get(rule_metrics, RuleId, 'actions.success')),
        ?assertEqual(0, emqx_metrics_worker:get(rule_metrics, RuleId, 'actions.failed')),
        ?assertEqual(0, emqx_metrics_worker:get(rule_metrics, RuleId, 'actions.discarded')),
        ?assertEqual(0, emqx_resource_metrics:dropped_get(BridgeV2Id)),
        ?assertEqual(0, emqx_resource_metrics:failed_get(BridgeV2Id)),
        ?assertEqual(0, emqx_resource_metrics:inflight_get(BridgeV2Id)),
        ?assertEqual(0, emqx_resource_metrics:queuing_get(BridgeV2Id)),
        ?assertEqual(0, emqx_resource_metrics:dropped_other_get(BridgeV2Id)),
        ?assertEqual(0, emqx_resource_metrics:dropped_queue_full_get(BridgeV2Id)),
        ?assertEqual(0, emqx_resource_metrics:dropped_resource_not_found_get(BridgeV2Id)),
        ?assertEqual(0, emqx_resource_metrics:dropped_resource_stopped_get(BridgeV2Id)),
        ?assertEqual(0, emqx_resource_metrics:retried_get(BridgeV2Id)),
        ?assertEqual(0, emqx_resource_metrics:retried_failed_get(BridgeV2Id)),
        ?assertEqual(0, emqx_resource_metrics:retried_success_get(BridgeV2Id)),
        % %% Perform operations
        {ok, 204, _} = http_put(BridgesPartsOpDisable, #{}),
        %% Success counter should be reset
        ?assertEqual(0, emqx_resource_metrics:success_get(BridgeV2Id)),
        emqx:publish(emqx_message:make(<<"kafka_bridge_topic/1">>, Body)),
        timer:sleep(100),
        ?assertEqual(0, emqx_resource_metrics:success_get(BridgeV2Id)),
        ?assertEqual(1, emqx_metrics_worker:get(rule_metrics, RuleId, 'actions.success')),
        ?assertEqual(0, emqx_metrics_worker:get(rule_metrics, RuleId, 'actions.failed')),
        ?assertEqual(1, emqx_metrics_worker:get(rule_metrics, RuleId, 'actions.discarded')),
        {ok, 204, _} = http_put(BridgesPartsOpDisable, #{}),
        {ok, 204, _} = http_put(BridgesPartsOpEnable, #{}),
        ?assertEqual(0, emqx_resource_metrics:success_get(BridgeV2Id)),
        %% Success counter should increase but
        emqx:publish(emqx_message:make(<<"kafka_bridge_topic/1">>, Body)),
        timer:sleep(100),
        ?assertEqual(1, emqx_resource_metrics:success_get(BridgeV2Id)),
        ?assertEqual(2, emqx_metrics_worker:get(rule_metrics, RuleId, 'actions.success')),
        {ok, 204, _} = http_put(BridgesPartsOpEnable, #{}),
        {ok, 204, _} = http_post(BridgesPartsOpStop, #{}),
        %% TODO: This is a bit tricky with the compatibility layer. Currently one
        %% can send a message even to a stopped channel. How shall we handle this?
        ?assertEqual(0, emqx_resource_metrics:success_get(BridgeV2Id)),
        {ok, 204, _} = http_post(BridgesPartsOpStop, #{}),
        {ok, 204, _} = http_post(BridgesPartsOpRestart, #{}),
        %% Success counter should increase
        timer:sleep(500),
        emqx:publish(emqx_message:make(<<"kafka_bridge_topic/1">>, Body)),
        timer:sleep(100),
        ?assertEqual(1, emqx_resource_metrics:success_get(BridgeV2Id)),
        ?assertEqual(3, emqx_metrics_worker:get(rule_metrics, RuleId, 'actions.success'))
    after
        %% Cleanup
        % this delete should not be necessary beause of the also_delete_dep_actions flag
        % {ok, 204, _} = http_delete(["rules", RuleId]),
        {ok, 204, _} = http_delete(BridgesPartsIdDeleteAlsoActions),
        Remain = http_get_bridges(BridgesParts, BridgeName),
        delete_all_bridges(),
        ?assertEqual([], Remain)
    end,
    ok.

%%------------------------------------------------------------------------------
%% Other tests
%%------------------------------------------------------------------------------

%% Need to stop the already running client; otherwise, the
%% next `on_start' call will try to ensure the client
%% exists and it will.  This is specially bad if the
%% original crash was due to misconfiguration and we are
%% trying to fix it...
%% DONE
t_failed_creation_then_fix(Config) ->
    %% TODO change this back to SASL_PLAINTEXT when we have figured out why that is not working
    HostsString = kafka_hosts_string(),
    %% valid_sasl_plain_settings()
    ValidAuthSettings = "none",
    WrongAuthSettings = (valid_sasl_plain_settings())#{"password" := "wrong"},
    Hash = erlang:phash2([HostsString, ?FUNCTION_NAME]),
    Type = ?BRIDGE_TYPE,
    Name = "kafka_bridge_name_" ++ erlang:integer_to_list(Hash),
    KafkaTopic = test_topic_one_partition(),
    WrongConf = config(#{
        "authentication" => WrongAuthSettings,
        "kafka_hosts_string" => HostsString,
        "kafka_topic" => KafkaTopic,
        "bridge_name" => Name,
        "ssl" => #{}
    }),
    ValidConf = config(#{
        "authentication" => ValidAuthSettings,
        "kafka_hosts_string" => HostsString,
        "kafka_topic" => KafkaTopic,
        "bridge_name" => Name,
        "producer" => #{
            "kafka" => #{
                "buffer" => #{
                    "memory_overload_protection" => false
                }
            }
        },
        "ssl" => #{}
    }),
    %% creates, but fails to start producers
    {ok, #{config := _WrongConfigAtom1}} = emqx_bridge:create(
        list_to_atom(Type), list_to_atom(Name), WrongConf
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
    {ok, #{config := _ValidConfigAtom1}} = emqx_bridge:create(
        list_to_atom(Type), list_to_atom(Name), ValidConf
    ),
    Time = erlang:unique_integer(),
    BinTime = integer_to_binary(Time),
    Msg = #{
        clientid => BinTime,
        payload => <<"payload">>,
        timestamp => Time
    },
    {ok, Offset} = resolve_kafka_offset(kafka_hosts(), KafkaTopic, 0),
    ct:pal("base offset before testing ~p", [Offset]),
    BridgeV2Id = emqx_bridge_v2:id(bin(?BRIDGE_TYPE_V2), bin(Name)),
    ResourceId = emqx_bridge_v2:extract_connector_id_from_bridge_v2_id(BridgeV2Id),
    {ok, _Group, #{state := State}} = emqx_resource:get_instance(ResourceId),
    ok = send(Config, ResourceId, Msg, State, BridgeV2Id),
    {ok, {_, [KafkaMsg]}} = brod:fetch(kafka_hosts(), KafkaTopic, 0, Offset),
    ?assertMatch(#kafka_message{key = BinTime}, KafkaMsg),
    % %% TODO: refactor those into init/end per testcase
    ok = emqx_bridge:remove(list_to_atom(Type), list_to_atom(Name)),
    delete_all_bridges(),
    ?assertEqual([], supervisor:which_children(wolff_client_sup)),
    ?assertEqual([], supervisor:which_children(wolff_producers_sup)),
    ok.

t_custom_timestamp(_Config) ->
    HostsString = kafka_hosts_string(),
    AuthSettings = "none",
    Hash = erlang:phash2([HostsString, ?FUNCTION_NAME]),
    Type = ?BRIDGE_TYPE,
    Name = "kafka_bridge_name_" ++ erlang:integer_to_list(Hash),
    KafkaTopic = test_topic_one_partition(),
    MQTTTopic = <<"t/local/kafka">>,
    emqx:subscribe(MQTTTopic),
    Conf0 = config(#{
        "authentication" => AuthSettings,
        "kafka_hosts_string" => HostsString,
        "local_topic" => MQTTTopic,
        "kafka_topic" => KafkaTopic,
        "bridge_name" => Name,
        "ssl" => #{}
    }),
    Conf = emqx_utils_maps:deep_put(
        [<<"kafka">>, <<"message">>, <<"timestamp">>],
        Conf0,
        <<"123">>
    ),
    {ok, _} = emqx_bridge:create(list_to_atom(Type), list_to_atom(Name), Conf),
    {ok, Offset} = resolve_kafka_offset(kafka_hosts(), KafkaTopic, 0),
    ct:pal("base offset before testing ~p", [Offset]),
    Time = erlang:unique_integer(),
    BinTime = integer_to_binary(Time),
    Msg = #{
        clientid => BinTime,
        payload => <<"payload">>,
        timestamp => Time
    },
    emqx:publish(emqx_message:make(MQTTTopic, emqx_utils_json:encode(Msg))),
    {ok, {_, [KafkaMsg]}} =
        ?retry(
            _Interval = 500,
            _NAttempts = 20,
            {ok, {_, [_]}} = brod:fetch(kafka_hosts(), KafkaTopic, _Partition = 0, Offset)
        ),
    ?assertMatch(#kafka_message{ts = 123, ts_type = create}, KafkaMsg),
    delete_all_bridges(),
    ok.

t_nonexistent_topic(_Config) ->
    HostsString = kafka_hosts_string(),
    AuthSettings = "none",
    Hash = erlang:phash2([HostsString, ?FUNCTION_NAME]),
    Type = ?BRIDGE_TYPE,
    Name = "kafka_bridge_name_" ++ erlang:integer_to_list(Hash),
    KafkaTopic = "undefined-test-topic",
    Conf = config(#{
        "authentication" => AuthSettings,
        "kafka_hosts_string" => HostsString,
        "kafka_topic" => KafkaTopic,
        "bridge_name" => Name,
        "producer" => #{
            "kafka" => #{
                "buffer" => #{
                    "memory_overload_protection" => false
                }
            }
        },
        "ssl" => #{}
    }),
    {ok, #{config := _ValidConfigAtom1}} = emqx_bridge:create(
        erlang:list_to_atom(Type), erlang:list_to_atom(Name), Conf
    ),
    % TODO: make sure the user facing APIs for Bridge V1 also get this error
    ?assertMatch(
        #{
            status := disconnected,
            error := {unhealthy_target, <<"Unknown topic or partition: undefined-test-topic">>}
        },
        emqx_bridge_v2:health_check(
            ?BRIDGE_TYPE_V2, list_to_atom(Name)
        )
    ),
    ok = emqx_bridge:remove(list_to_atom(Type), list_to_atom(Name)),
    delete_all_bridges(),
    ok.

t_send_message_with_headers(matrix) ->
    {query_mode, [[sync], [async]]};
t_send_message_with_headers(Config) ->
    [Mode] = group_path(Config),
    ct:comment(Mode),
    HostsString = kafka_hosts_string_sasl(),
    AuthSettings = valid_sasl_plain_settings(),
    Hash = erlang:phash2([HostsString, ?FUNCTION_NAME]),
    Type = ?BRIDGE_TYPE,
    Name = "kafka_bridge_name_" ++ erlang:integer_to_list(Hash),
    %ResourceId = emqx_bridge_resource:resource_id(Type, Name),
    %BridgeId = emqx_bridge_resource:bridge_id(Type, Name),
    KafkaTopic = test_topic_one_partition(),
    Conf = config_with_headers(#{
        "authentication" => AuthSettings,
        "kafka_hosts_string" => HostsString,
        "kafka_topic" => KafkaTopic,
        "bridge_name" => Name,
        "kafka_headers" => <<"${payload.header}">>,
        "kafka_ext_headers" => emqx_utils_json:encode(
            [
                #{
                    <<"kafka_ext_header_key">> => <<"clientid">>,
                    <<"kafka_ext_header_value">> => <<"${clientid}">>
                },
                #{
                    <<"kafka_ext_header_key">> => <<"ext_header_val">>,
                    <<"kafka_ext_header_value">> => <<"${payload.ext_header_val}">>
                }
            ]
        ),
        "producer" => #{
            "kafka" => #{
                "buffer" => #{
                    "memory_overload_protection" => false
                }
            }
        },
        "query_mode" => Mode,
        "ssl" => #{}
    }),
    {ok, _} = emqx_bridge:create(
        list_to_atom(Type), list_to_atom(Name), Conf
    ),
    ResourceId = emqx_bridge_resource:resource_id(bin(Type), bin(Name)),
    BridgeV2Id = emqx_bridge_v2:id(bin(?BRIDGE_TYPE_V2), bin(Name)),
    {ok, _Group, #{state := State}} = emqx_resource:get_instance(ResourceId),
    Time1 = erlang:unique_integer(),
    BinTime1 = integer_to_binary(Time1),
    Payload1 = emqx_utils_json:encode(
        #{
            <<"header">> => #{
                <<"foo">> => <<"bar">>
            },
            <<"ext_header_val">> => <<"ext header ok">>
        }
    ),
    Msg1 = #{
        clientid => BinTime1,
        payload => Payload1,
        timestamp => Time1
    },
    Time2 = erlang:unique_integer(),
    BinTime2 = integer_to_binary(Time2),
    Payload2 = emqx_utils_json:encode(
        #{
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
    Msg2 = #{
        clientid => BinTime2,
        payload => Payload2,
        timestamp => Time2
    },
    {ok, Offset} = resolve_kafka_offset(kafka_hosts(), KafkaTopic, 0),
    ct:pal("base offset before testing ~p", [Offset]),
    Kind =
        case Mode of
            sync -> emqx_bridge_kafka_impl_producer_sync_query;
            async -> emqx_bridge_kafka_impl_producer_async_query
        end,
    ?check_trace(
        begin
            ok = send(Config, ResourceId, Msg1, State, BridgeV2Id),
            ok = send(Config, ResourceId, Msg2, State, BridgeV2Id)
        end,
        fun(Trace) ->
            ?assertMatch(
                [
                    #{
                        headers_config := #{
                            ext_headers_tokens := [
                                {
                                    [{str, <<"clientid">>}],
                                    [{var, [<<"clientid">>]}]
                                },
                                {
                                    [{str, <<"ext_header_val">>}],
                                    [{var, [<<"payload">>, <<"ext_header_val">>]}]
                                }
                            ],
                            headers_tokens := [{var, [<<"payload">>, <<"header">>]}],
                            headers_val_encode_mode := json
                        }
                    },
                    #{
                        headers_config := #{
                            ext_headers_tokens := [
                                {
                                    [{str, <<"clientid">>}],
                                    [{var, [<<"clientid">>]}]
                                },
                                {
                                    [{str, <<"ext_header_val">>}],
                                    [{var, [<<"payload">>, <<"ext_header_val">>]}]
                                }
                            ],
                            headers_tokens := [{var, [<<"payload">>, <<"header">>]}],
                            headers_val_encode_mode := json
                        }
                    }
                ],
                ?of_kind(Kind, Trace)
            )
        end
    ),
    {ok, {_, KafkaMsgs}} = brod:fetch(kafka_hosts(), KafkaTopic, 0, Offset),
    ?assertMatch(
        [
            #kafka_message{
                headers = [
                    {<<"foo">>, <<"\"bar\"">>},
                    {<<"clientid">>, _},
                    {<<"ext_header_val">>, <<"\"ext header ok\"">>}
                ],
                key = BinTime1
            },
            #kafka_message{
                headers = [
                    {<<"foo1">>, <<"\"bar1\"">>},
                    {<<"foo2">>, <<"\"bar2\"">>},
                    {<<"clientid">>, _},
                    {<<"ext_header_val">>, <<"\"ext header ok\"">>}
                ],
                key = BinTime2
            }
        ],
        KafkaMsgs
    ),
    %% TODO: refactor those into init/end per testcase
    ok = ?PRODUCER:on_stop(ResourceId, State),
    ?assertEqual([], supervisor:which_children(wolff_client_sup)),
    ?assertEqual([], supervisor:which_children(wolff_producers_sup)),
    ok = emqx_bridge:remove(list_to_atom(Name), list_to_atom(Type)),
    delete_all_bridges(),
    ok.

%% DONE
t_wrong_headers(_Config) ->
    HostsString = kafka_hosts_string_sasl(),
    AuthSettings = valid_sasl_plain_settings(),
    Hash = erlang:phash2([HostsString, ?FUNCTION_NAME]),
    % Type = ?BRIDGE_TYPE,
    Name = "kafka_bridge_name_" ++ erlang:integer_to_list(Hash),
    KafkaTopic = test_topic_one_partition(),
    ?assertThrow(
        {
            emqx_bridge_schema,
            [
                #{
                    kind := validation_error,
                    reason := "The 'kafka_headers' must be a single placeholder like ${pub_props}"
                }
            ]
        },
        config_with_headers(#{
            "authentication" => AuthSettings,
            "kafka_hosts_string" => HostsString,
            "kafka_topic" => KafkaTopic,
            "bridge_name" => Name,
            "kafka_headers" => <<"wrong_header">>,
            "kafka_ext_headers" => <<"[]">>,
            "producer" => #{
                "kafka" => #{
                    "buffer" => #{
                        "memory_overload_protection" => false
                    }
                }
            },
            "ssl" => #{}
        })
    ),
    ?assertThrow(
        {
            emqx_bridge_schema,
            [
                #{
                    kind := validation_error,
                    reason :=
                        "The value of 'kafka_ext_headers' must either be a single "
                        "placeholder like ${foo}, or a simple string."
                }
            ]
        },
        config_with_headers(#{
            "authentication" => AuthSettings,
            "kafka_hosts_string" => HostsString,
            "kafka_topic" => KafkaTopic,
            "bridge_name" => Name,
            "kafka_headers" => <<"${pub_props}">>,
            "kafka_ext_headers" => emqx_utils_json:encode(
                [
                    #{
                        <<"kafka_ext_header_key">> => <<"clientid">>,
                        <<"kafka_ext_header_value">> => <<"wrong ${header}">>
                    }
                ]
            ),
            "producer" => #{
                "kafka" => #{
                    "buffer" => #{
                        "memory_overload_protection" => false
                    }
                }
            },
            "ssl" => #{}
        })
    ),
    ok.

t_wrong_headers_from_message(matrix) ->
    {query_mode, [[sync], [async]]};
t_wrong_headers_from_message(Config) ->
    [Mode] = group_path(Config),
    ct:comment(Mode),
    HostsString = kafka_hosts_string(),
    AuthSettings = "none",
    Hash = erlang:phash2([HostsString, ?FUNCTION_NAME]),
    Type = ?BRIDGE_TYPE,
    Name = "kafka_bridge_name_" ++ erlang:integer_to_list(Hash),
    KafkaTopic = test_topic_one_partition(),
    Conf = config_with_headers(#{
        "authentication" => AuthSettings,
        "kafka_hosts_string" => HostsString,
        "kafka_topic" => KafkaTopic,
        "bridge_name" => Name,
        "kafka_headers" => <<"${payload}">>,
        "producer" => #{
            "kafka" => #{
                "buffer" => #{
                    "memory_overload_protection" => false
                }
            }
        },
        "query_mode" => Mode,
        "ssl" => #{}
    }),
    {ok, _} = emqx_bridge:create(
        list_to_atom(Type), list_to_atom(Name), Conf
    ),
    ResourceId = emqx_bridge_resource:resource_id(bin(Type), bin(Name)),
    {ok, _Group, #{state := State}} = emqx_resource:get_instance(ResourceId),
    Time1 = erlang:unique_integer(),
    Payload1 = <<"wrong_header">>,
    Msg1 = #{
        clientid => integer_to_binary(Time1),
        payload => Payload1,
        timestamp => Time1
    },
    BridgeV2Id = emqx_bridge_v2:id(bin(?BRIDGE_TYPE_V2), bin(Name)),
    ?assertError(
        {badmatch, {error, {unrecoverable_error, {bad_kafka_headers, Payload1}}}},
        send(Config, ResourceId, Msg1, State, BridgeV2Id)
    ),
    Time2 = erlang:unique_integer(),
    Payload2 = <<"[{\"foo\":\"bar\"}, {\"foo2\":\"bar2\"}]">>,
    Msg2 = #{
        clientid => integer_to_binary(Time2),
        payload => Payload2,
        timestamp => Time2
    },
    ?assertError(
        {badmatch, {error, {unrecoverable_error, {bad_kafka_header, #{<<"foo">> := <<"bar">>}}}}},
        send(Config, ResourceId, Msg2, State, BridgeV2Id)
    ),
    Time3 = erlang:unique_integer(),
    Payload3 = <<"[{\"key\":\"foo\"}, {\"value\":\"bar\"}]">>,
    Msg3 = #{
        clientid => integer_to_binary(Time3),
        payload => Payload3,
        timestamp => Time3
    },
    ?assertError(
        {badmatch, {error, {unrecoverable_error, {bad_kafka_header, #{<<"key">> := <<"foo">>}}}}},
        send(Config, ResourceId, Msg3, State, BridgeV2Id)
    ),
    %% TODO: refactor those into init/end per testcase
    ok = ?PRODUCER:on_stop(ResourceId, State),
    ?assertEqual([], supervisor:which_children(wolff_client_sup)),
    ?assertEqual([], supervisor:which_children(wolff_producers_sup)),
    ok = emqx_bridge:remove(list_to_atom(Type), list_to_atom(Name)),
    delete_all_bridges(),
    ok.

%%------------------------------------------------------------------------------
%% Helper functions
%%------------------------------------------------------------------------------

send(Config, ResourceId, Msg, State, BridgeV2Id) when is_list(Config) ->
    Ref = make_ref(),
    ok = do_send(Ref, Config, ResourceId, Msg, State, BridgeV2Id),
    receive
        {ack, Ref} ->
            ok
    after 10000 ->
        error(timeout)
    end.

do_send(Ref, Config, ResourceId, Msg, State, BridgeV2Id) when is_list(Config) ->
    Caller = self(),
    F = fun(ok) ->
        Caller ! {ack, Ref},
        ok
    end,
    case group_path(Config) of
        [async] ->
            {ok, _} = ?PRODUCER:on_query_async(ResourceId, {BridgeV2Id, Msg}, {F, []}, State),
            ok;
        _ ->
            ok = ?PRODUCER:on_query(ResourceId, {BridgeV2Id, Msg}, State),
            F(ok)
    end.

test_publish(HostsString, BridgeConfig, _CtConfig) ->
    delete_all_bridges(),
    Hash = erlang:phash2([HostsString]),
    Name = "kafka_bridge_name_" ++ erlang:integer_to_list(Hash),
    KafkaTopic = test_topic_one_partition(),
    Conf = config(
        #{
            "authentication" => "none",
            "ssl" => #{},
            "bridge_name" => Name,
            "kafka_hosts_string" => HostsString,
            "kafka_topic" => KafkaTopic,
            "local_topic" => <<"mqtt/local">>
        },
        BridgeConfig
    ),
    {ok, _} = emqx_bridge:create(
        <<?BRIDGE_TYPE>>, list_to_binary(Name), Conf
    ),
    Partition = 0,
    %% test that it forwards from local mqtt topic as well
    %% TODO Make sure that local topic works for bridge_v2
    {ok, Offset1} = resolve_kafka_offset(kafka_hosts(), KafkaTopic, Partition),
    ct:pal("base offset before testing (2) ~p", [Offset1]),
    emqx:publish(emqx_message:make(<<"mqtt/local">>, <<"payload">>)),
    ct:sleep(2_000),
    {ok, {_, [KafkaMsg1]}} = brod:fetch(kafka_hosts(), KafkaTopic, Partition, Offset1),
    ?assertMatch(#kafka_message{value = <<"payload">>}, KafkaMsg1),

    delete_all_bridges(),
    ok.

default_config() ->
    #{"partition_strategy" => "random"}.

config(Args) ->
    config(Args, #{}).

config(Args0, More) ->
    config(Args0, More, fun hocon_config_template/0).

config_with_headers(Args) ->
    config(Args, #{}, fun hocon_config_template_with_headers/0).

config(Args0, More, ConfigTemplateFun) ->
    Args1 = maps:merge(default_config(), Args0),
    Args = maps:merge(Args1, More),
    ConfText = hocon_config(Args, ConfigTemplateFun),
    {ok, Conf} = hocon:binary(ConfText, #{format => map}),
    Name = bin(maps:get("bridge_name", Args)),
    %% TODO can we skip this old check?
    ct:pal("Running tests with conf:\n~p", [Conf]),
    % % InstId = maps:get("instance_id", Args),
    TypeBin = ?BRIDGE_TYPE_BIN,
    % <<"connector:", BridgeId/binary>> = InstId,
    % {Type, Name} = emqx_bridge_resource:parse_bridge_id(BridgeId, #{atom_name => false}),
    hocon_tconf:check_plain(
        emqx_bridge_schema,
        Conf,
        #{atom_key => false, required => false}
    ),
    #{<<"bridges">> := #{TypeBin := #{Name := Parsed}}} = Conf,
    Parsed.

hocon_config(Args, ConfigTemplateFun) ->
    BridgeName = maps:get("bridge_name", Args),
    AuthConf = maps:get("authentication", Args),
    AuthTemplate = iolist_to_binary(hocon_config_template_authentication(AuthConf)),
    AuthConfRendered = bbmustache:render(AuthTemplate, AuthConf),
    SSLConf = maps:get("ssl", Args, #{}),
    SSLTemplate = iolist_to_binary(hocon_config_template_ssl(SSLConf)),
    QueryMode = maps:get("query_mode", Args, <<"async">>),
    SSLConfRendered = bbmustache:render(SSLTemplate, SSLConf),
    KafkaHeaders = maps:get("kafka_headers", Args, undefined),
    KafkaExtHeaders = maps:get("kafka_ext_headers", Args, <<"[]">>),
    Hocon = bbmustache:render(
        iolist_to_binary(ConfigTemplateFun()),
        Args#{
            "authentication" => AuthConfRendered,
            "bridge_name" => BridgeName,
            "ssl" => SSLConfRendered,
            "query_mode" => QueryMode,
            "kafka_headers" => KafkaHeaders,
            "kafka_ext_headers" => KafkaExtHeaders
        }
    ),
    Hocon.

hocon_config_template() ->
    "bridges.kafka.{{ bridge_name }} {"
    "\n   bootstrap_hosts = \"{{ kafka_hosts_string }}\""
    "\n   enable = true"
    "\n   authentication = {{{ authentication }}}"
    "\n   ssl = {{{ ssl }}}"
    "\n   local_topic = \"{{ local_topic }}\""
    "\n   kafka = {"
    "\n     message = {"
    "\n       key = \"${clientid}\""
    "\n       value = \"${.payload}\""
    "\n       timestamp = \"${timestamp}\""
    "\n     }"
    "\n     buffer = {"
    "\n       memory_overload_protection = false"
    "\n     }"
    "\n     partition_strategy = {{ partition_strategy }}"
    "\n     topic = \"{{ kafka_topic }}\""
    "\n     query_mode = {{ query_mode }}"
    "\n   }"
    "\n   metadata_request_timeout = 5s"
    "\n   min_metadata_refresh_interval = 3s"
    "\n   socket_opts {"
    "\n     nodelay = true"
    "\n   }"
    "\n   connect_timeout = 5s"
    "\n }".

hocon_config_template_with_headers() ->
    "bridges.kafka.{{ bridge_name }} {"
    "\n   bootstrap_hosts = \"{{ kafka_hosts_string }}\""
    "\n   enable = true"
    "\n   authentication = {{{ authentication }}}"
    "\n   ssl = {{{ ssl }}}"
    "\n   local_topic = \"{{ local_topic }}\""
    "\n   kafka = {"
    "\n     message = {"
    "\n       key = \"${clientid}\""
    "\n       value = \"${.payload}\""
    "\n       timestamp = \"${timestamp}\""
    "\n     }"
    "\n     buffer = {"
    "\n       memory_overload_protection = false"
    "\n     }"
    "\n     kafka_headers = \"{{ kafka_headers }}\""
    "\n     kafka_header_value_encode_mode: json"
    "\n     kafka_ext_headers: {{{ kafka_ext_headers }}}"
    "\n     partition_strategy = {{ partition_strategy }}"
    "\n     topic = \"{{ kafka_topic }}\""
    "\n     query_mode = {{ query_mode }}"
    "\n   }"
    "\n   metadata_request_timeout = 5s"
    "\n   min_metadata_refresh_interval = 3s"
    "\n   socket_opts {"
    "\n     nodelay = true"
    "\n   }"
    "\n   connect_timeout = 5s"
    "\n }".

hocon_config_template_authentication("none") ->
    "none";
hocon_config_template_authentication(#{"mechanism" := _}) ->
    "{"
    "\n     mechanism = {{ mechanism }}"
    "\n     password = \"{{ password }}\""
    "\n     username = \"{{ username }}\""
    "\n }";
hocon_config_template_authentication(#{"kerberos_principal" := _}) ->
    "{"
    "\n     kerberos_principal = \"{{ kerberos_principal }}\""
    "\n     kerberos_keytab_file = \"{{ kerberos_keytab_file }}\""
    "\n }".

hocon_config_template_ssl(Map) when map_size(Map) =:= 0 ->
    "{ enable = false }";
hocon_config_template_ssl(#{"enable" := "false"}) ->
    "{ enable = false }";
hocon_config_template_ssl(#{"enable" := "true"}) ->
    "{      enable = true"
    "\n     cacertfile = \"{{{cacertfile}}}\""
    "\n     certfile = \"{{{certfile}}}\""
    "\n     keyfile = \"{{{keyfile}}}\""
    "\n }".

kafka_hosts_string(tcp, none) ->
    kafka_hosts_string();
kafka_hosts_string(tcp, plain) ->
    kafka_hosts_string_sasl();
kafka_hosts_string(ssl, none) ->
    kafka_hosts_string_ssl();
kafka_hosts_string(ssl, _) ->
    kafka_hosts_string_ssl_sasl().

kafka_hosts_string() ->
    KafkaHost = os:getenv("KAFKA_PLAIN_HOST", "kafka-1.emqx.net"),
    KafkaPort = os:getenv("KAFKA_PLAIN_PORT", "9092"),
    KafkaHost ++ ":" ++ KafkaPort ++ ",".

kafka_hosts_string_sasl() ->
    KafkaHost = os:getenv("KAFKA_SASL_PLAIN_HOST", "kafka-1.emqx.net"),
    KafkaPort = os:getenv("KAFKA_SASL_PLAIN_PORT", "9093"),
    KafkaHost ++ ":" ++ KafkaPort ++ ",".

kafka_hosts_string_ssl() ->
    KafkaHost = os:getenv("KAFKA_SSL_HOST", "kafka-1.emqx.net"),
    KafkaPort = os:getenv("KAFKA_SSL_PORT", "9094"),
    KafkaHost ++ ":" ++ KafkaPort ++ ",".

kafka_hosts_string_ssl_sasl() ->
    KafkaHost = os:getenv("KAFKA_SASL_SSL_HOST", "kafka-1.emqx.net"),
    KafkaPort = os:getenv("KAFKA_SASL_SSL_PORT", "9095"),
    KafkaHost ++ ":" ++ KafkaPort ++ ",".

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

valid_ssl_settings() ->
    #{
        "cacertfile" => shared_secret(client_cacertfile),
        "certfile" => shared_secret(client_certfile),
        "keyfile" => shared_secret(client_keyfile),
        "enable" => "true"
    }.

valid_sasl_plain_settings() ->
    #{
        "mechanism" => "plain",
        "username" => "emqxuser",
        "password" => "password"
    }.

valid_sasl_scram256_settings() ->
    (valid_sasl_plain_settings())#{
        "mechanism" => "scram_sha_256"
    }.

valid_sasl_scram512_settings() ->
    (valid_sasl_plain_settings())#{
        "mechanism" => "scram_sha_512"
    }.

valid_sasl_kerberos_settings() ->
    #{
        "kerberos_principal" => "rig@KDC.EMQX.NET",
        "kerberos_keytab_file" => shared_secret(rig_keytab)
    }.

valid_sasl_plain_passfile_settings(Passfile) ->
    Auth = valid_sasl_plain_settings(),
    ok = file:write_file(Passfile, maps:get("password", Auth)),
    Auth#{
        "password" := "file://" ++ Passfile
    }.

kafka_hosts() ->
    kpro:parse_endpoints(kafka_hosts_string()).

resolve_kafka_offset(Hosts, Topic, Partition) ->
    brod:resolve_offset(Hosts, Topic, Partition, latest).

%%------------------------------------------------------------------------------
%% Internal functions rest API helpers
%%------------------------------------------------------------------------------

bin(X) -> iolist_to_binary(X).

random_num() ->
    erlang:system_time(nanosecond).

http_get(Parts) ->
    request_api(get, api_path(Parts), auth_header_()).

http_delete(Parts) ->
    request_api(delete, api_path(Parts), auth_header_()).

http_post(Parts, Body) ->
    request_api(post, api_path(Parts), [], auth_header_(), Body).

http_put(Parts, Body) ->
    request_api(put, api_path(Parts), [], auth_header_(), Body).

request_dashboard(Method, Url, Auth) ->
    Request = {Url, [Auth]},
    do_request_dashboard(Method, Request).
request_dashboard(Method, Url, QueryParams, Auth) ->
    Request = {Url ++ "?" ++ QueryParams, [Auth]},
    do_request_dashboard(Method, Request).
do_request_dashboard(Method, Request) ->
    ct:pal("Method: ~p, Request: ~p", [Method, Request]),
    case httpc:request(Method, Request, [], []) of
        {error, socket_closed_remotely} ->
            {error, socket_closed_remotely};
        {ok, {{"HTTP/1.1", Code, _}, _Headers, Return}} when
            Code >= 200 andalso Code =< 299
        ->
            {ok, Return};
        {ok, {Reason, _, _}} ->
            {error, Reason}
    end.

auth_header_() ->
    auth_header_(<<"admin">>, <<"public">>).

auth_header_(Username, Password) ->
    {ok, #{token := Token}} = emqx_dashboard_admin:sign_token(Username, Password),
    {"Authorization", "Bearer " ++ binary_to_list(Token)}.

api_path(Parts) ->
    ?HOST ++ filename:join([?BASE_PATH | Parts]).

json(Data) ->
    {ok, Jsx} = emqx_utils_json:safe_decode(Data, [return_maps]),
    Jsx.

delete_all_bridges() ->
    lists:foreach(
        fun(#{name := Name, type := Type}) ->
            ok = emqx_bridge:remove(Type, Name)
        end,
        emqx_bridge:list()
    ),
    %% at some point during the tests, sometimes `emqx_bridge:list()'
    %% returns an empty list, but `emqx:get_config([bridges])' returns
    %% a bunch of orphan test bridges...
    lists:foreach(fun emqx_resource:remove_local/1, emqx_resource:list_instances()),
    emqx_config:put([bridges], #{}),
    ok.

bin_map(Map) ->
    maps:from_list([
        {erlang:iolist_to_binary(K), erlang:iolist_to_binary(V)}
     || {K, V} <- maps:to_list(Map)
    ]).

%% return the path (reverse of the stack) of the test groups.
%% root group is discarded.
group_path(Config) ->
    case emqx_common_test_helpers:group_path(Config) of
        [] ->
            undefined;
        Path ->
            tl(Path)
    end.
