%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_rabbitmq_v2_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

%%------------------------------------------------------------------------------
%% Defs
%%------------------------------------------------------------------------------

-import(emqx_common_test_helpers, [on_exit/1]).

-define(tcp, tcp).
-define(tls, tls).

-define(TYPE, <<"rabbitmq">>).

-define(USER, <<"guest">>).
-define(PASSWORD, <<"guest">>).
-define(EXCHANGE, <<"messages">>).
-define(QUEUE, <<"test_queue">>).
-define(ROUTING_KEY, <<"test_routing_key">>).

%%------------------------------------------------------------------------------
%% CT boilerplate
%%------------------------------------------------------------------------------

all() ->
    [
        {group, ?tcp},
        {group, ?tls}
    ].

groups() ->
    AllTCs = emqx_common_test_helpers:all(?MODULE),
    [
        {?tcp, AllTCs},
        {?tls, AllTCs}
    ].

init_per_group(Group, Config) ->
    emqx_bridge_rabbitmq_test_utils:init_per_group(Group, Config).

end_per_group(Group, Config) ->
    emqx_bridge_rabbitmq_test_utils:end_per_group(Group, Config).

init_per_testcase(TestCase, TCConfig) ->
    ConnectorName = atom_to_binary(TestCase),
    TLS =
        case is_tls(TCConfig) of
            ?tls ->
                emqx_utils_maps:binary_key_map(
                    emqx_bridge_rabbitmq_test_utils:ssl_options(true)
                );
            ?tcp ->
                emqx_utils_maps:binary_key_map(
                    emqx_bridge_rabbitmq_test_utils:ssl_options(false)
                )
        end,
    ConnectorConfig = connector_config(#{<<"ssl">> => TLS}),
    ActionName = ConnectorName,
    SourceName = ConnectorName,
    ActionConfig = action_config(#{<<"connector">> => ConnectorName}),
    SourceConfig = source_config(#{<<"connector">> => ConnectorName}),
    [
        {bridge_kind, action},
        {connector_type, ?TYPE},
        {connector_name, ConnectorName},
        {connector_config, ConnectorConfig},
        {action_type, ?TYPE},
        {action_name, ActionName},
        {action_config, ActionConfig},
        {source_type, ?TYPE},
        {source_name, SourceName},
        {source_config, SourceConfig}
        | TCConfig
    ].

end_per_testcase(_TestCase, _Config) ->
    emqx_bridge_v2_testlib:delete_all_rules(),
    emqx_bridge_v2_testlib:delete_all_bridges_and_connectors(),
    ok.

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

is_tls(TCConfig) ->
    emqx_common_test_helpers:get_matrix_prop(TCConfig, [?tcp, ?tls], ?tcp).

connector_config(Overrides) ->
    Default = #{
        <<"enable">> => true,
        <<"ssl">> => #{<<"enable">> => false},
        <<"server">> => <<"rabbitmq">>,
        <<"port">> => 5672,
        <<"username">> => ?USER,
        <<"password">> => ?PASSWORD
    },
    InnerConfigMap = emqx_utils_maps:deep_merge(Default, Overrides),
    emqx_bridge_v2_testlib:parse_and_check_connector(?TYPE, <<"x">>, InnerConfigMap).

action_config(Overrides) ->
    Default = #{
        <<"connector">> => <<"please override">>,
        <<"enable">> => true,
        <<"parameters">> => #{
            <<"exchange">> => ?EXCHANGE,
            <<"payload_template">> => <<"${.payload}">>,
            <<"routing_key">> => ?ROUTING_KEY,
            <<"delivery_mode">> => <<"non_persistent">>,
            <<"publish_confirmation_timeout">> => <<"30s">>,
            <<"wait_for_publish_confirmations">> => true
        },
        <<"resource_opts">> => emqx_bridge_v2_testlib:common_action_resource_opts()
    },
    InnerConfigMap = emqx_utils_maps:deep_merge(Default, Overrides),
    emqx_bridge_v2_testlib:parse_and_check(action, ?TYPE, <<"x">>, InnerConfigMap).

source_config(Overrides) ->
    Default = #{
        <<"connector">> => <<"please override">>,
        <<"enable">> => true,
        <<"parameters">> => #{
            <<"no_ack">> => true,
            <<"queue">> => <<"test_queue">>,
            <<"wait_for_publish_confirmations">> => true
        },
        <<"resource_opts">> => emqx_bridge_v2_testlib:common_source_resource_opts()
    },
    InnerConfigMap = emqx_utils_maps:deep_merge(Default, Overrides),
    emqx_bridge_v2_testlib:parse_and_check(source, ?TYPE, <<"x">>, InnerConfigMap).

create_connector_api(Config, Overrides) ->
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:create_connector_api(Config, Overrides)
    ).

delete_source(Name) ->
    {204, _} = emqx_bridge_v2_testlib:delete_kind_api(source, ?TYPE, Name, #{
        query_params => #{<<"also_delete_dep_actions">> => <<"true">>}
    }),
    ok.

create_action_api(Config, Overrides) ->
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:create_action_api(Config, Overrides)
    ).

create_source_api(Config, Overrides) ->
    emqx_bridge_v2_testlib:create_source_api(Config, Overrides).

delete_action(Name) ->
    {204, _} = emqx_bridge_v2_testlib:delete_kind_api(action, ?TYPE, Name, #{
        query_params => #{<<"also_delete_dep_actions">> => <<"true">>}
    }),
    ok.

waiting_for_disconnected_alarms(InstanceId) ->
    ?retry(
        _TimeOut = 100,
        _Times = 20,
        case
            lists:any(
                fun
                    (
                        #{
                            message :=
                                <<"resource down: #{error => not_connected,status => disconnected}">>,
                            name := InstanceId0
                        }
                    ) ->
                        InstanceId0 =:= InstanceId;
                    (_) ->
                        false
                end,
                emqx_alarm:get_alarms(activated)
            )
        of
            true ->
                ok;
            false ->
                throw(not_receive_disconnect_alarm)
        end
    ).

waiting_for_dropped_count(InstanceId) ->
    ?retry(
        400,
        10,
        ?assertMatch(
            #{
                counters := #{
                    dropped := 1,
                    success := 0,
                    matched := 1,
                    failed := 0,
                    received := 0
                }
            },
            emqx_resource:get_metrics(InstanceId)
        )
    ).

receive_messages(Count) ->
    receive_messages(Count, []).
receive_messages(0, Msgs) ->
    Msgs;
receive_messages(Count, Msgs) ->
    receive
        {publish, Msg} ->
            ct:log("Msg: ~p ~n", [Msg]),
            receive_messages(Count - 1, [Msg | Msgs]);
        Other ->
            ct:log("Other Msg: ~p~n", [Other]),
            receive_messages(Count, Msgs)
    after 2000 ->
        Msgs
    end.

payload() ->
    payload(_Overrides = #{}).

payload(Overrides) ->
    maps:merge(
        #{<<"key">> => 42, <<"data">> => <<"RabbitMQ">>, <<"timestamp">> => 10000},
        Overrides
    ).

send_test_message_to_rabbitmq(Config) ->
    #{channel := Channel} = emqx_bridge_rabbitmq_test_utils:get_channel_connection(Config),
    MessageProperties = #'P_basic'{
        headers = [],
        delivery_mode = 1
    },
    Method = #'basic.publish'{
        exchange = ?EXCHANGE,
        routing_key = ?ROUTING_KEY
    },
    amqp_channel:cast(
        Channel,
        Method,
        #amqp_msg{
            payload = emqx_utils_json:encode(payload()),
            props = MessageProperties
        }
    ),
    ok.

instance_id(Type, Name) ->
    ConnectorId = emqx_bridge_resource:resource_id(Type, ?TYPE, Name),
    BridgeId = emqx_bridge_resource:bridge_id(?TYPE, Name),
    TypeBin =
        case Type of
            sources -> <<"source:">>;
            actions -> <<"action:">>
        end,
    <<TypeBin/binary, BridgeId/binary, ":", ConnectorId/binary>>.

prepare_conf_file(Name, Content, TCConfig) ->
    emqx_config_SUITE:prepare_conf_file(Name, Content, TCConfig).

get_value(Key, TCConfig) ->
    emqx_bridge_v2_testlib:get_value(Key, TCConfig).

%%------------------------------------------------------------------------------
%% Test Cases
%%------------------------------------------------------------------------------

t_source(Config0) ->
    Config = emqx_bridge_v2_testlib:proplist_update(Config0, bridge_kind, fun(_) -> source end),
    Name = get_value(source_name, Config),
    {201, _} = create_connector_api(Config, #{}),
    {201, _} = create_source_api(Config, #{}),
    Topic = <<"tesldkafd">>,
    {201, _} = emqx_bridge_v2_testlib:create_rule_api2(
        #{
            <<"sql">> =>
                <<
                    "select *, queue as payload.queue, exchange as payload.exchange,"
                    "routing_key as payload.routing_key from \"$bridges/rabbitmq:",
                    Name/binary,
                    "\""
                >>,
            <<"id">> => atom_to_binary(?FUNCTION_NAME),
            <<"actions">> => [
                #{
                    <<"args">> => #{
                        <<"topic">> => Topic,
                        <<"mqtt_properties">> => #{},
                        <<"payload">> => <<"${payload}">>,
                        <<"qos">> => 0,
                        <<"retain">> => false,
                        <<"user_properties">> => [],
                        <<"direct_dispatch">> => false
                    },
                    <<"function">> => <<"republish">>
                }
            ],
            <<"description">> => <<"bridge_v2 republish rule">>
        }
    ),
    {ok, C1} = emqtt:start_link([{clean_start, true}]),
    {ok, _} = emqtt:connect(C1),
    {ok, #{}, [0]} = emqtt:subscribe(C1, Topic, [{qos, 0}, {rh, 0}]),
    send_test_message_to_rabbitmq(Config),

    Received = receive_messages(1),
    ?assertMatch(
        [
            #{
                dup := false,
                properties := undefined,
                topic := Topic,
                qos := 0,
                payload := _,
                retain := false
            }
        ],
        Received
    ),
    [#{payload := ReceivedPayload}] = Received,
    Meta = #{
        <<"exchange">> => ?EXCHANGE,
        <<"routing_key">> => ?ROUTING_KEY,
        <<"queue">> => ?QUEUE
    },
    ExpectedPayload = maps:merge(payload(), Meta),
    ?assertMatch(ExpectedPayload, emqx_utils_json:decode(ReceivedPayload)),

    ok = emqtt:disconnect(C1),
    InstanceId = instance_id(sources, Name),
    #{counters := Counters} = emqx_resource:get_metrics(InstanceId),
    ?assertMatch(
        #{
            dropped := 0,
            success := 0,
            matched := 0,
            failed := 0,
            received := 1
        },
        Counters
    ),
    ok.

t_source_probe(TCConfig) ->
    SourceConfig = get_value(source_config, TCConfig),
    {201, _} = create_connector_api(TCConfig, #{}),
    ?assertMatch(
        {ok, {{_, 204, _}, _, _}},
        emqx_bridge_v2_testlib:probe_bridge_api(source, ?TYPE, <<"x">>, SourceConfig)
    ),
    ok.

t_action_probe(TCConfig) ->
    ActionConfig = get_value(action_config, TCConfig),
    {201, _} = create_connector_api(TCConfig, #{}),
    ?assertMatch(
        {ok, {{_, 204, _}, _, _}},
        emqx_bridge_v2_testlib:probe_bridge_api(action, ?TYPE, <<"x">>, ActionConfig)
    ),
    ok.

t_action(Config) ->
    Name = get_value(action_name, Config),
    {201, _} = create_connector_api(Config, #{}),
    {201, _} = create_action_api(Config, #{}),
    Actions = emqx_bridge_v2:list(actions),
    Any = fun(#{name := BName}) -> BName =:= Name end,
    ?assert(lists:any(Any, Actions), Actions),
    Topic = <<"lkadfdaction">>,
    {201, _} = emqx_bridge_v2_testlib:create_rule_api2(
        #{
            <<"sql">> => <<"select * from \"", Topic/binary, "\"">>,
            <<"id">> => atom_to_binary(?FUNCTION_NAME),
            <<"actions">> => [<<"rabbitmq:", Name/binary>>],
            <<"description">> => <<"bridge_v2 send msg to rabbitmq action">>
        }
    ),
    {ok, C1} = emqtt:start_link([{clean_start, true}]),
    {ok, _} = emqtt:connect(C1),
    Payload = payload(),
    PayloadBin = emqx_utils_json:encode(Payload),
    {ok, _} = emqtt:publish(C1, Topic, #{}, PayloadBin, [{qos, 1}, {retain, false}]),
    #{payload := Msg} = emqx_bridge_rabbitmq_test_utils:receive_message_from_rabbitmq(Config),
    ?assertMatch(Payload, Msg),
    ok = emqtt:disconnect(C1),
    InstanceId = instance_id(actions, Name),
    #{counters := Counters} = emqx_resource:get_metrics(InstanceId),
    ok = delete_action(Name),
    ActionsAfterDelete = emqx_bridge_v2:list(actions),
    ?assertNot(lists:any(Any, ActionsAfterDelete), ActionsAfterDelete),
    ?assertMatch(
        #{
            dropped := 0,
            success := 0,
            matched := 1,
            failed := 0,
            received := 0
        },
        Counters
    ),
    ok.

t_action_stop(Config) ->
    Name = get_value(action_name, Config),
    {201, _} = create_connector_api(Config, #{}),
    {201, _} = create_action_api(Config, #{}),

    %% Emulate channel close hitting the timeout
    meck:new(amqp_channel, [passthrough, no_history]),
    meck:expect(amqp_channel, close, fun(_Pid) -> timer:sleep(infinity) end),

    %% Delete action should not exceed connector's ?CHANNEL_CLOSE_TIMEOUT
    {Time, _} = timer:tc(fun() -> delete_action(Name) end),
    ?assert(Time < 4_500_000),

    meck:unload(amqp_channel),
    ok.

t_action_inexistent_exchange(Config) ->
    Name = get_value(action_name, Config),
    {201, _} = create_connector_api(Config, #{}),
    {201, _} = create_action_api(Config, #{
        <<"parameters">> => #{<<"exchange">> => <<"inexistent_exchange">>},
        <<"resource_opts">> => #{<<"request_ttl">> => <<"1s">>}
    }),
    Actions = emqx_bridge_v2:list(actions),
    Any = fun(#{name := BName}) -> BName =:= Name end,
    ?assert(lists:any(Any, Actions), Actions),
    Topic = <<"lkadfaodik">>,
    {201, _} = emqx_bridge_v2_testlib:create_rule_api2(
        #{
            <<"sql">> => <<"select * from \"", Topic/binary, "\"">>,
            <<"id">> => atom_to_binary(?FUNCTION_NAME),
            <<"actions">> => [<<"rabbitmq:", Name/binary>>],
            <<"description">> => <<"bridge_v2 send msg to rabbitmq action failed">>
        }
    ),
    ok = snabbkaffe:start_trace(),
    {ok, C1} = emqtt:start_link([{clean_start, true}]),
    {ok, _} = emqtt:connect(C1),
    Payload = payload(),
    PayloadBin = emqx_utils_json:encode(Payload),
    {ok, _} = emqtt:publish(C1, Topic, #{}, PayloadBin, [{qos, 1}, {retain, false}]),
    ok = emqtt:disconnect(C1),
    InstanceId = instance_id(actions, Name),
    ct:pal("waiting for alarms"),
    waiting_for_disconnected_alarms(InstanceId),
    ct:pal("waiting for dropped counts"),
    waiting_for_dropped_count(InstanceId),
    ct:pal("done"),
    #{counters := Counters} = emqx_resource:get_metrics(InstanceId),
    %% dropped + 1
    ?assertMatch(
        #{
            dropped := 1,
            success := 0,
            matched := 1,
            failed := 0,
            received := 0
        },
        Counters
    ),
    Trace = snabbkaffe:collect_trace(50),
    ?assert(
        lists:any(
            fun
                (#{msg := emqx_bridge_rabbitmq_connector_rabbit_publish_failed_with_msg}) ->
                    true;
                (#{msg := emqx_bridge_rabbitmq_connector_rabbit_publish_failed_con_not_ready}) ->
                    true;
                (_) ->
                    false
            end,
            Trace
        ),
        Trace
    ).

t_replace_action_source(Config) ->
    ConnectorName = get_value(connector_name, Config),
    ActionConfig = get_value(action_config, Config),
    SourceConfig = get_value(source_config, Config),
    ConnectorConfig = get_value(connector_config, Config),
    Action = #{<<"rabbitmq">> => #{<<"my_action">> => ActionConfig}},
    Source = #{<<"rabbitmq">> => #{<<"my_source">> => SourceConfig}},
    Connector = #{<<"rabbitmq">> => #{ConnectorName => ConnectorConfig}},
    Rabbitmq = #{
        <<"actions">> => Action,
        <<"sources">> => Source,
        <<"connectors">> => Connector
    },
    ConfBin0 = hocon_pp:do(Rabbitmq, #{}),
    ConfFile0 = prepare_conf_file(?FUNCTION_NAME, ConfBin0, Config),
    ?assertMatch(ok, emqx_conf_cli:conf(["load", "--replace", ConfFile0])),
    ?assertMatch(
        #{<<"rabbitmq">> := #{<<"my_action">> := _}},
        emqx_config:get_raw([<<"actions">>]),
        Action
    ),
    ?assertMatch(
        #{<<"rabbitmq">> := #{<<"my_source">> := _}},
        emqx_config:get_raw([<<"sources">>]),
        Source
    ),
    ?assertMatch(
        #{<<"rabbitmq">> := #{ConnectorName := _}},
        emqx_config:get_raw([<<"connectors">>]),
        Connector
    ),

    Empty = #{
        <<"actions">> => #{},
        <<"sources">> => #{},
        <<"connectors">> => #{}
    },
    ConfBin1 = hocon_pp:do(Empty, #{}),
    ConfFile1 = prepare_conf_file(?FUNCTION_NAME, ConfBin1, Config),
    ?assertMatch(ok, emqx_conf_cli:conf(["load", "--replace", ConfFile1])),

    ?assertEqual(#{}, emqx_config:get_raw([<<"actions">>])),
    ?assertEqual(#{}, emqx_config:get_raw([<<"sources">>])),
    ?assertMatch(#{}, emqx_config:get_raw([<<"connectors">>])),

    %% restore connectors
    Rabbitmq2 = #{<<"connectors">> => Connector},
    ConfBin2 = hocon_pp:do(Rabbitmq2, #{}),
    ConfFile2 = prepare_conf_file(?FUNCTION_NAME, ConfBin2, Config),
    ?assertMatch(ok, emqx_conf_cli:conf(["load", "--replace", ConfFile2])),
    ?assertMatch(
        #{<<"rabbitmq">> := #{ConnectorName := _}},
        emqx_config:get_raw([<<"connectors">>]),
        Connector
    ),
    ok.

t_action_dynamic(Config) ->
    Name = get_value(action_name, Config),
    {201, _} = create_connector_api(Config, #{}),
    {201, _} = create_action_api(Config, #{
        <<"parameters">> => #{
            <<"exchange">> => <<"${payload.e}">>,
            <<"routing_key">> => <<"${payload.r}">>
        }
    }),
    Topic = <<"rabbitdynaction">>,
    {201, _} = emqx_bridge_v2_testlib:create_rule_api2(
        #{
            <<"sql">> => <<"select * from \"", Topic/binary, "\"">>,
            <<"id">> => atom_to_binary(?FUNCTION_NAME),
            <<"actions">> => [<<"rabbitmq:", Name/binary>>],
            <<"description">> => <<"bridge_v2 send msg to rabbitmq action">>
        }
    ),
    {ok, C1} = emqtt:start_link([{clean_start, true}]),
    {ok, _} = emqtt:connect(C1),
    Payload = payload(#{<<"e">> => ?EXCHANGE, <<"r">> => ?ROUTING_KEY}),
    PayloadBin = emqx_utils_json:encode(Payload),
    {ok, _} = emqtt:publish(C1, Topic, #{}, PayloadBin, [{qos, 1}, {retain, false}]),
    #{payload := Msg} = emqx_bridge_rabbitmq_test_utils:receive_message_from_rabbitmq(Config),
    ?assertMatch(Payload, Msg),
    ok = emqtt:disconnect(C1),
    InstanceId = instance_id(actions, Name),
    ?retry(
        _Interval0 = 500,
        _NAttempts0 = 10,
        begin
            #{counters := Counters} = emqx_resource:get_metrics(InstanceId),
            ?assertMatch(
                #{
                    dropped := 0,
                    success := 1,
                    matched := 1,
                    failed := 0,
                    received := 0
                },
                Counters
            )
        end
    ),
    ok.

t_action_use_default_exchange(Config) ->
    Name = get_value(action_name, Config),
    {201, _} = create_connector_api(Config, #{}),
    {201, _} = create_action_api(Config, #{
        <<"parameters">> => #{
            <<"exchange">> => <<"">>,
            <<"routing_key">> => ?QUEUE
        }
    }),
    Topic = <<"rabbit/use/default/exchange">>,
    {201, _} = emqx_bridge_v2_testlib:create_rule_api2(
        #{
            <<"sql">> => <<"select * from \"", Topic/binary, "\"">>,
            <<"id">> => atom_to_binary(?FUNCTION_NAME),
            <<"actions">> => [<<"rabbitmq:", Name/binary>>],
            <<"description">> => <<"bridge_v2 send msg to rabbitmq action">>
        }
    ),
    {ok, C1} = emqtt:start_link([{clean_start, true}]),
    {ok, _} = emqtt:connect(C1),
    Payload = payload(#{<<"e">> => <<"">>, <<"r">> => <<"test_queue">>}),
    PayloadBin = emqx_utils_json:encode(Payload),
    {ok, _} = emqtt:publish(C1, Topic, #{}, PayloadBin, [{qos, 1}, {retain, false}]),
    #{payload := Msg} = emqx_bridge_rabbitmq_test_utils:receive_message_from_rabbitmq(Config),
    ?assertMatch(Payload, Msg),
    ok = emqtt:disconnect(C1),
    InstanceId = instance_id(actions, Name),
    ?retry(
        _Interval0 = 500,
        _NAttempts0 = 10,
        begin
            #{counters := Counters} = emqx_resource:get_metrics(InstanceId),
            ?assertMatch(
                #{
                    dropped := 0,
                    success := 1,
                    matched := 1,
                    failed := 0,
                    received := 0
                },
                Counters
            )
        end
    ),
    ok.

-doc """
Smoke tests for settings message header and property templates.
""".
t_header_props_templates(TCConfig) ->
    Name = get_value(action_name, TCConfig),
    {201, _} = create_connector_api(TCConfig, #{}),
    {201, _} = create_action_api(TCConfig, #{
        <<"parameters">> => #{
            <<"headers_template">> => [
                #{<<"key">> => <<"${.payload.hk1}">>, <<"value">> => <<"${.payload.hv1}">>},
                #{<<"key">> => <<"${.payload.hk2}">>, <<"value">> => <<"${.payload.hv2}">>}
            ],
            <<"properties_template">> => [
                %% Correlation id
                #{<<"key">> => <<"${.payload.pk1}">>, <<"value">> => <<"${.payload.pv1}">>},
                %% Message id
                #{<<"key">> => <<"${.payload.pk2}">>, <<"value">> => <<"${.payload.pv2}">>},
                #{<<"key">> => <<"content_type">>, <<"value">> => <<"application/json">>},
                #{<<"key">> => <<"content_encoding">>, <<"value">> => <<"utf8">>},
                #{<<"key">> => <<"expiration">>, <<"value">> => <<"600000">>},
                #{<<"key">> => <<"type">>, <<"value">> => <<"orders.created">>},
                #{<<"key">> => <<"user_id">>, <<"value">> => ?USER},
                #{<<"key">> => <<"app_id">>, <<"value">> => <<"myapp">>},
                #{<<"key">> => <<"cluster_id">>, <<"value">> => <<"mycluster">>},
                #{<<"key">> => <<"timestamp">>, <<"value">> => <<"${publish_received_at}">>},
                #{<<"key">> => <<"unknown">>, <<"value">> => <<"${.does.not.matter}">>}
            ]
        },
        <<"resource_opts">> => #{<<"request_ttl">> => <<"1s">>}
    }),
    RuleTopic = <<"header/props">>,
    BridgeId = emqx_bridge_resource:bridge_id(?TYPE, Name),
    {201, _} = emqx_bridge_v2_testlib:create_rule_api2(
        #{
            <<"sql">> =>
                emqx_bridge_v2_testlib:fmt(
                    <<"select * from \"${t}\" ">>,
                    #{t => RuleTopic}
                ),
            <<"id">> => atom_to_binary(?FUNCTION_NAME),
            <<"actions">> => [BridgeId],
            <<"description">> => <<"">>
        }
    ),

    {ok, C1} = emqtt:start_link(),
    {ok, _} = emqtt:connect(C1),
    CorrelationId = <<"123456_correlation">>,
    MessageId = <<"some_message_id">>,
    Payload = payload(#{
        <<"hk1">> => <<"header_key1">>,
        <<"hv1">> => <<"header_val1">>,
        <<"hk2">> => <<"header_key2">>,
        <<"hv2">> => <<"header_val2">>,
        <<"pk1">> => <<"correlation_id">>,
        <<"pv1">> => CorrelationId,
        <<"pk2">> => <<"message_id">>,
        <<"pv2">> => MessageId
    }),
    PayloadBin = emqx_utils_json:encode(Payload),
    {ok, _} = emqtt:publish(C1, RuleTopic, #{}, PayloadBin, [{qos, 1}, {retain, false}]),
    ok = emqtt:disconnect(C1),
    Msg = emqx_bridge_rabbitmq_test_utils:receive_message_from_rabbitmq(TCConfig),
    ?assertMatch(
        #{
            payload := Payload,
            props := #{
                app_id := <<"myapp">>,
                cluster_id := <<"mycluster">>,
                user_id := ?USER,
                content_type := <<"application/json">>,
                content_encoding := <<"utf8">>,
                expiration := <<"600000">>,
                message_id := MessageId,
                correlation_id := CorrelationId,
                type := <<"orders.created">>,
                timestamp := TS
            },
            headers := [
                {<<"header_key1">>, binary, <<"header_val1">>},
                {<<"header_key2">>, binary, <<"header_val2">>}
            ]
        } when TS /= undefined,
        Msg
    ),
    ok.
