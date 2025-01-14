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

-import(emqx_config_SUITE, [prepare_conf_file/3]).

-import(emqx_bridge_rabbitmq_test_utils, [
    rabbit_mq_exchange/0,
    rabbit_mq_routing_key/0,
    rabbit_mq_queue/0,
    rabbit_mq_host/0,
    rabbit_mq_port/0,
    get_rabbitmq/1,
    get_tls/1,
    ssl_options/1,
    get_channel_connection/1,
    parse_and_check/4,
    receive_message_from_rabbitmq/1
]).
-import(emqx_common_test_helpers, [on_exit/1]).

-define(TYPE, <<"rabbitmq">>).

all() ->
    [
        {group, tcp},
        {group, tls}
    ].

groups() ->
    AllTCs = emqx_common_test_helpers:all(?MODULE),
    [
        {tcp, AllTCs},
        {tls, AllTCs}
    ].

init_per_group(Group, Config) ->
    Config1 = emqx_bridge_rabbitmq_test_utils:init_per_group(Group, Config),
    Name = atom_to_binary(?MODULE),
    create_connector(Name, get_rabbitmq(Config1)),
    Config1.

end_per_group(Group, Config) ->
    Name = atom_to_binary(?MODULE),
    delete_connector(Name),
    emqx_bridge_rabbitmq_test_utils:end_per_group(Group, Config).

rabbitmq_connector(Config) ->
    UseTLS = maps:get(tls, Config, false),
    Name = atom_to_binary(?MODULE),
    Server = maps:get(server, Config, rabbit_mq_host()),
    Port = maps:get(port, Config, rabbit_mq_port()),
    Connector = #{
        <<"connectors">> => #{
            <<"rabbitmq">> => #{
                Name => #{
                    <<"enable">> => true,
                    <<"ssl">> => ssl_options(UseTLS),
                    <<"server">> => Server,
                    <<"port">> => Port,
                    <<"username">> => <<"guest">>,
                    <<"password">> => <<"guest">>
                }
            }
        }
    },
    parse_and_check(<<"connectors">>, emqx_connector_schema, Connector, Name).

rabbitmq_source() ->
    Name = atom_to_binary(?MODULE),
    Source = #{
        <<"sources">> => #{
            <<"rabbitmq">> => #{
                Name => #{
                    <<"enable">> => true,
                    <<"connector">> => Name,
                    <<"parameters">> => #{
                        <<"no_ack">> => true,
                        <<"queue">> => rabbit_mq_queue(),
                        <<"wait_for_publish_confirmations">> => true
                    }
                }
            }
        }
    },
    parse_and_check(<<"sources">>, emqx_bridge_v2_schema, Source, Name).

rabbitmq_action(TestCase) ->
    rabbitmq_action(TestCase, rabbit_mq_exchange(TestCase)).

rabbitmq_action(TestCase, Exchange) ->
    Name = atom_to_binary(?MODULE),
    Action = #{
        <<"actions">> => #{
            <<"rabbitmq">> => #{
                Name => #{
                    <<"connector">> => Name,
                    <<"enable">> => true,
                    <<"parameters">> => #{
                        <<"exchange">> => Exchange,
                        <<"payload_template">> => <<"${.payload}">>,
                        <<"routing_key">> => rabbit_mq_routing_key(TestCase),
                        <<"delivery_mode">> => <<"non_persistent">>,
                        <<"publish_confirmation_timeout">> => <<"30s">>,
                        <<"wait_for_publish_confirmations">> => true
                    },
                    <<"resource_opts">> => #{
                        <<"health_check_interval">> => <<"1s">>,
                        <<"inflight_window">> => 1
                    }
                }
            }
        }
    },
    parse_and_check(<<"actions">>, emqx_bridge_v2_schema, Action, Name).

create_connector(Name, Config) ->
    Connector = rabbitmq_connector(Config),
    {ok, _} = emqx_connector:create(?TYPE, Name, Connector).

delete_connector(Name) ->
    ok = emqx_connector:remove(?TYPE, Name).

create_source(Name) ->
    Source = rabbitmq_source(),
    {ok, _} = emqx_bridge_v2:create(sources, ?TYPE, Name, Source).

delete_source(Name) ->
    ok = emqx_bridge_v2:remove(sources, ?TYPE, Name).

create_action(TestCase, Name) ->
    create_action(TestCase, Name, rabbit_mq_exchange(TestCase)).

create_action(TestCase, Name, Exchange) ->
    Action = rabbitmq_action(TestCase, Exchange),
    {ok, _} = emqx_bridge_v2:create(actions, ?TYPE, Name, Action).

delete_action(Name) ->
    ok = emqx_bridge_v2:remove(actions, ?TYPE, Name).

%%------------------------------------------------------------------------------
%% Test Cases
%%------------------------------------------------------------------------------

t_source(Config) ->
    Name = atom_to_binary(?FUNCTION_NAME),
    create_source(Name),
    Sources = emqx_bridge_v2:list(sources),
    %% Don't show rabbitmq source in bridge_v1_list
    ?assertEqual([], emqx_bridge_v2:bridge_v1_list_and_transform()),
    Any = fun(#{name := BName}) -> BName =:= Name end,
    ?assert(lists:any(Any, Sources), Sources),
    Topic = <<"tesldkafd">>,
    {ok, #{id := RuleId}} = emqx_rule_engine:create_rule(
        #{
            sql =>
                <<
                    "select *, queue as payload.queue, exchange as payload.exchange,"
                    "routing_key as payload.routing_key from \"$bridges/rabbitmq:",
                    Name/binary,
                    "\""
                >>,
            id => atom_to_binary(?FUNCTION_NAME),
            actions => [
                #{
                    args => #{
                        topic => Topic,
                        mqtt_properties => #{},
                        payload => <<"${payload}">>,
                        qos => 0,
                        retain => false,
                        user_properties => [],
                        direct_dispatch => false
                    },
                    function => republish
                }
            ],
            description => <<"bridge_v2 republish rule">>
        }
    ),
    on_exit(fun() -> emqx_rule_engine:delete_rule(RuleId) end),
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
        <<"exchange">> => rabbit_mq_exchange(),
        <<"routing_key">> => rabbit_mq_routing_key(),
        <<"queue">> => rabbit_mq_queue()
    },
    ExpectedPayload = maps:merge(payload(), Meta),
    ?assertMatch(ExpectedPayload, emqx_utils_json:decode(ReceivedPayload)),

    ok = emqtt:disconnect(C1),
    InstanceId = instance_id(sources, Name),
    #{counters := Counters} = emqx_resource:get_metrics(InstanceId),
    ok = delete_source(Name),
    SourcesAfterDelete = emqx_bridge_v2:list(sources),
    ?assertNot(lists:any(Any, SourcesAfterDelete), SourcesAfterDelete),
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

t_source_probe(_Config) ->
    Name = atom_to_binary(?FUNCTION_NAME),
    Source = rabbitmq_source(),
    {ok, Res0} = emqx_bridge_v2_testlib:probe_bridge_api(source, ?TYPE, Name, Source),
    ?assertMatch({{_, 204, _}, _, _}, Res0),
    ok.

t_action_probe(_Config) ->
    Name = atom_to_binary(?FUNCTION_NAME),
    Action = rabbitmq_action(?FUNCTION_NAME),
    {ok, Res0} = emqx_bridge_v2_testlib:probe_bridge_api(action, ?TYPE, Name, Action),
    ?assertMatch({{_, 204, _}, _, _}, Res0),
    ok.

t_action(Config) ->
    Name = atom_to_binary(?FUNCTION_NAME),
    create_action(?FUNCTION_NAME, Name),
    Actions = emqx_bridge_v2:list(actions),
    Any = fun(#{name := BName}) -> BName =:= Name end,
    ?assert(lists:any(Any, Actions), Actions),
    Topic = <<"lkadfdaction">>,
    {ok, #{id := RuleId}} = emqx_rule_engine:create_rule(
        #{
            sql => <<"select * from \"", Topic/binary, "\"">>,
            id => atom_to_binary(?FUNCTION_NAME),
            actions => [<<"rabbitmq:", Name/binary>>],
            description => <<"bridge_v2 send msg to rabbitmq action">>
        }
    ),
    on_exit(fun() -> emqx_rule_engine:delete_rule(RuleId) end),
    {ok, C1} = emqtt:start_link([{clean_start, true}]),
    {ok, _} = emqtt:connect(C1),
    Payload = payload(),
    PayloadBin = emqx_utils_json:encode(Payload),
    {ok, _} = emqtt:publish(C1, Topic, #{}, PayloadBin, [{qos, 1}, {retain, false}]),
    Msg = receive_message_from_rabbitmq(Config),
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

t_action_not_exist_exchange(_Config) ->
    Name = atom_to_binary(?FUNCTION_NAME),
    create_action(?FUNCTION_NAME, Name, <<"not_exist_exchange">>),
    Actions = emqx_bridge_v2:list(actions),
    Any = fun(#{name := BName}) -> BName =:= Name end,
    ?assert(lists:any(Any, Actions), Actions),
    Topic = <<"lkadfaodik">>,
    {ok, #{id := RuleId}} = emqx_rule_engine:create_rule(
        #{
            sql => <<"select * from \"", Topic/binary, "\"">>,
            id => atom_to_binary(?FUNCTION_NAME),
            actions => [<<"rabbitmq:", Name/binary>>],
            description => <<"bridge_v2 send msg to rabbitmq action failed">>
        }
    ),
    ok = snabbkaffe:start_trace(),
    on_exit(fun() ->
        snabbkaffe:stop(),
        emqx_rule_engine:delete_rule(RuleId),
        _ = delete_action(Name)
    end),
    {ok, C1} = emqtt:start_link([{clean_start, true}]),
    {ok, _} = emqtt:connect(C1),
    Payload = payload(),
    PayloadBin = emqx_utils_json:encode(Payload),
    {ok, _} = emqtt:publish(C1, Topic, #{}, PayloadBin, [{qos, 1}, {retain, false}]),
    ok = emqtt:disconnect(C1),
    InstanceId = instance_id(actions, Name),
    waiting_for_disconnected_alarms(InstanceId),
    waiting_for_dropped_count(InstanceId),
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
    ok = delete_action(Name),
    ActionsAfterDelete = emqx_bridge_v2:list(actions),
    ?assertNot(lists:any(Any, ActionsAfterDelete), ActionsAfterDelete),
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
    Action = #{<<"rabbitmq">> => #{<<"my_action">> => rabbitmq_action(?FUNCTION_NAME)}},
    Source = #{<<"rabbitmq">> => #{<<"my_source">> => rabbitmq_source()}},
    ConnectorName = atom_to_binary(?MODULE),
    Connector = #{<<"rabbitmq">> => #{ConnectorName => rabbitmq_connector(get_rabbitmq(Config))}},
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
    Name = atom_to_binary(?FUNCTION_NAME),
    create_action(?FUNCTION_NAME, Name),
    Actions = emqx_bridge_v2:list(actions),
    Any = fun(#{name := BName}) -> BName =:= Name end,
    ?assert(lists:any(Any, Actions), Actions),
    Topic = <<"rabbitdynaction">>,
    {ok, #{id := RuleId}} = emqx_rule_engine:create_rule(
        #{
            sql => <<"select * from \"", Topic/binary, "\"">>,
            id => atom_to_binary(?FUNCTION_NAME),
            actions => [<<"rabbitmq:", Name/binary>>],
            description => <<"bridge_v2 send msg to rabbitmq action">>
        }
    ),
    on_exit(fun() -> emqx_rule_engine:delete_rule(RuleId) end),
    {ok, C1} = emqtt:start_link([{clean_start, true}]),
    {ok, _} = emqtt:connect(C1),
    Payload = payload(?FUNCTION_NAME),
    PayloadBin = emqx_utils_json:encode(Payload),
    {ok, _} = emqtt:publish(C1, Topic, #{}, PayloadBin, [{qos, 1}, {retain, false}]),
    Msg = receive_message_from_rabbitmq(Config),
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

    ok = delete_action(Name),
    ActionsAfterDelete = emqx_bridge_v2:list(actions),
    ?assertNot(lists:any(Any, ActionsAfterDelete), ActionsAfterDelete),

    ok.

waiting_for_disconnected_alarms(InstanceId) ->
    waiting_for_disconnected_alarms(InstanceId, 0).

waiting_for_disconnected_alarms(_InstanceId, 200) ->
    throw(not_receive_disconnect_alarm);
waiting_for_disconnected_alarms(InstanceId, Count) ->
    case emqx_alarm:get_alarms(activated) of
        [] ->
            ct:sleep(100),
            waiting_for_disconnected_alarms(InstanceId, Count + 1);
        [Alarm] ->
            ?assertMatch(
                #{
                    message :=
                        <<"resource down: #{error => not_connected,status => disconnected}">>,
                    name := InstanceId
                },
                Alarm
            )
    end.

waiting_for_dropped_count(InstanceId) ->
    waiting_for_dropped_count(InstanceId, 0).

waiting_for_dropped_count(_InstanceId, 400) ->
    throw(not_receive_dropped_count);
waiting_for_dropped_count(InstanceId, Count) ->
    #{
        counters := #{
            dropped := Dropped,
            success := 0,
            matched := 1,
            failed := 0,
            received := 0
        }
    } = emqx_resource:get_metrics(InstanceId),
    case Dropped of
        1 ->
            ok;
        0 ->
            ct:sleep(400),
            waiting_for_dropped_count(InstanceId, Count + 1)
    end.

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
    #{<<"key">> => 42, <<"data">> => <<"RabbitMQ">>, <<"timestamp">> => 10000}.

payload(t_action_dynamic) ->
    Payload = payload(),
    Payload#{<<"e">> => rabbit_mq_exchange(), <<"r">> => rabbit_mq_routing_key()}.

send_test_message_to_rabbitmq(Config) ->
    #{channel := Channel} = get_channel_connection(Config),
    MessageProperties = #'P_basic'{
        headers = [],
        delivery_mode = 1
    },
    Method = #'basic.publish'{
        exchange = rabbit_mq_exchange(),
        routing_key = rabbit_mq_routing_key()
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

rabbit_mq_exchange(t_action_dynamic) ->
    <<"${payload.e}">>;
rabbit_mq_exchange(_) ->
    rabbit_mq_exchange().

rabbit_mq_routing_key(t_action_dynamic) ->
    <<"${payload.r}">>;
rabbit_mq_routing_key(_) ->
    rabbit_mq_routing_key().
