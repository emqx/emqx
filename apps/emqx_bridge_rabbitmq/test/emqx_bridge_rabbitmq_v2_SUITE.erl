%%--------------------------------------------------------------------
%% Copyright (c) 2024-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_rabbitmq_v2_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("emqx_connector/include/emqx_connector.hrl").
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
    parse_and_check/4
]).
-import(emqx_common_test_helpers, [on_exit/1]).

-define(TYPE, <<"rabbitmq">>).
-define(CONNECTOR_TYPE, rabbitmq).
-define(CONNECTOR_TYPE_BIN, <<"rabbitmq">>).
-define(ACTION_TYPE, rabbitmq).
-define(ACTION_TYPE_BIN, <<"rabbitmq">>).
-define(SOURCE_TYPE, rabbitmq).
-define(SOURCE_TYPE_BIN, <<"rabbitmq">>).
-define(USER, <<"guest">>).
-define(PASSWORD, <<"guest">>).
-define(EXCHANGE, <<"messages">>).
-define(QUEUE, <<"test_queue">>).
-define(ROUTING_KEY, <<"test_routing_key">>).

-define(tpal(MSG, ARGS), begin
    ct:pal(lists:flatten(io_lib:format(MSG, ARGS))),
    ?tp(notice, lists:flatten(io_lib:format(MSG, ARGS)), #{})
end).

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
    emqx_bridge_rabbitmq_test_utils:init_per_group(Group, Config).

end_per_group(Group, Config) ->
    emqx_bridge_rabbitmq_test_utils:end_per_group(Group, Config).

init_per_testcase(TestCase, TCConfig) ->
    ConnectorName = atom_to_binary(TestCase),
    ConnectorConfig = connector_config(#{}),
    ActionName = ConnectorName,
    ActionConfig = action_config(#{
        <<"connector">> => ConnectorName
    }),
    SourceName = ConnectorName,
    SourceConfig = source_config(#{
        <<"connector">> => ConnectorName
    }),
    ClientOpts = #{
        host => get_config(host, TCConfig, <<"rabbitmq">>),
        port => get_config(port, TCConfig, 5672),
        use_tls => get_config(enable_tls, TCConfig, false),
        exchange => ?EXCHANGE,
        queue => ?QUEUE,
        routing_key => ?ROUTING_KEY
    },
    emqx_bridge_rabbitmq_testlib:connect_and_setup_exchange_and_queue(ClientOpts),
    snabbkaffe:start_trace(),
    [
        {connector_type, ?CONNECTOR_TYPE},
        {connector_name, ConnectorName},
        {connector_config, ConnectorConfig},
        {action_type, ?ACTION_TYPE},
        {action_name, ActionName},
        {action_config, ActionConfig},
        {source_type, ?SOURCE_TYPE},
        {source_name, SourceName},
        {source_config, SourceConfig},
        {client_opts, ClientOpts}
        | TCConfig
    ].

end_per_testcase(_TestCase, TCConfig) ->
    snabbkaffe:stop(),
    ClientOpts = get_config(client_opts, TCConfig),
    emqx_bridge_rabbitmq_testlib:cleanup_client_and_queue(ClientOpts),
    emqx_bridge_v2_testlib:delete_all_rules(),
    emqx_bridge_v2_testlib:delete_all_bridges_and_connectors(),
    emqx_common_test_helpers:call_janitor(),
    ok.

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
    on_exit(fun() -> delete_connector(Name) end),
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

get_config(K, TCConfig) -> emqx_bridge_v2_testlib:get_value(K, TCConfig).
get_config(K, TCConfig, Default) -> proplists:get_value(K, TCConfig, Default).

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
            receive_messages(Count - 1, [Msg | Msgs])
    after 2000 ->
        Msgs
    end.

payload() ->
    #{<<"key">> => 42, <<"data">> => <<"RabbitMQ">>, <<"timestamp">> => 10000}.

payload(t_action_dynamic) ->
    Payload = payload(),
    Payload#{<<"e">> => rabbit_mq_exchange(), <<"r">> => rabbit_mq_routing_key()}.

send_test_message_to_rabbitmq(Config) ->
    ClientOpts = ?config(client_opts, Config),
    emqx_bridge_rabbitmq_testlib:publish_message(emqx_utils_json:encode(payload()), ClientOpts).

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

connector_config(Overrides) ->
    emqx_bridge_rabbitmq_testlib:connector_config(Overrides).

action_config(Overrides) ->
    emqx_bridge_rabbitmq_testlib:action_config(Overrides).

source_config(Overrides) ->
    emqx_bridge_rabbitmq_testlib:source_config(Overrides).

%% Attempts to list all rabbitmq connections processes (`amqp_connection:start`) without
%% relying on details about how it's started/managed by emqx.
list_rabbitmq_connection_processes() ->
    [
        Pid
     || Pid <- erlang:processes(),
        case proc_lib:initial_call(Pid) of
            {amqp_gen_connection, init, [_]} ->
                true;
            _ ->
                false
        end
    ].

%% Attempts to list all rabbitmq channel processes (`amqp_connection:open_channel`)
%% without relying on details about how it's started/managed by emqx.
list_rabbitmq_channel_processes() ->
    [
        Pid
     || Pid <- erlang:processes(),
        case proc_lib:initial_call(Pid) of
            {amqp_channel, init, [_]} ->
                true;
            _ ->
                false
        end
    ].

random(List) ->
    lists:nth(rand:uniform(length(List)), List).

create_connector_api(TCConfig, Overrides) ->
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:create_connector_api(TCConfig, Overrides)
    ).

create_action_api(TCConfig, Overrides) ->
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:create_action_api([{bridge_kind, action} | TCConfig], Overrides)
    ).

create_source_api(TCConfig, Overrides) ->
    emqx_bridge_v2_testlib:create_source_api([{bridge_kind, source} | TCConfig], Overrides).

get_connector_api(TCConfig) ->
    #{connector_type := Type, connector_name := Name} =
        emqx_bridge_v2_testlib:get_common_values(TCConfig),
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:get_connector_api(Type, Name)
    ).

get_action_api(TCConfig) ->
    emqx_bridge_v2_testlib:get_action_api2(TCConfig).

get_source_api(TCConfig) ->
    #{type := Type, name := Name} =
        emqx_bridge_v2_testlib:get_common_values(TCConfig),
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:get_source_api(Type, Name)
    ).

get_common_values_action(TCConfig) ->
    emqx_bridge_v2_testlib:get_common_values([{bridge_kind, action} | TCConfig]).

get_common_values_source(TCConfig) ->
    emqx_bridge_v2_testlib:get_common_values([{bridge_kind, source} | TCConfig]).

%% Must be called after action and source are created.
snk_subscribe_to_health_checks(TCConfig) ->
    #{connector_name := ConnectorName} = get_common_values_action(TCConfig),
    #{name := SourceName} = get_common_values_source(TCConfig),
    ActionResId = emqx_bridge_v2_testlib:bridge_id(TCConfig),
    SourceResId = emqx_bridge_v2:source_id(?SOURCE_TYPE, SourceName, ConnectorName),
    {ok, SRef1} = snabbkaffe:subscribe(
        ?match_event(#{?snk_kind := "rabbitmq_on_get_status_enter0"})
    ),
    {ok, SRef2} = snabbkaffe:subscribe(
        ?match_event(#{
            ?snk_kind := "rabbitmq_on_get_channel_status_enter",
            channel_id := ActionResId
        })
    ),
    {ok, SRef3} = snabbkaffe:subscribe(
        ?match_event(#{
            ?snk_kind := "rabbitmq_on_get_channel_status_enter",
            channel_id := SourceResId
        })
    ),
    #{connector => SRef1, action => SRef2, source => SRef3}.

snk_receive_health_check_events(SRefs) ->
    maps:map(
        fun(K, SRef) ->
            ct:pal("waiting for ~p", [K]),
            Res = snabbkaffe:receive_events(SRef),
            ct:pal("~p got ~p", [K, Res]),
            Res
        end,
        SRefs
    ).

receive_message_from_rabbitmq(TCConfig) ->
    ClientOpts = ?config(client_opts, TCConfig),
    emqx_bridge_rabbitmq_testlib:receive_message(ClientOpts).

%%------------------------------------------------------------------------------
%% Test Cases
%%------------------------------------------------------------------------------

t_source(Config) ->
    Name = atom_to_binary(?FUNCTION_NAME),
    create_connector(?MODULE, get_rabbitmq(Config)),
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

t_source_probe(Config) ->
    Name = atom_to_binary(?FUNCTION_NAME),
    create_connector(?MODULE, get_rabbitmq(Config)),
    Source = rabbitmq_source(),
    {ok, Res0} = emqx_bridge_v2_testlib:probe_bridge_api(source, ?TYPE, Name, Source),
    ?assertMatch({{_, 204, _}, _, _}, Res0),
    ok.

t_action_probe(Config) ->
    Name = atom_to_binary(?FUNCTION_NAME),
    create_connector(?MODULE, get_rabbitmq(Config)),
    Action = rabbitmq_action(?FUNCTION_NAME),
    {ok, Res0} = emqx_bridge_v2_testlib:probe_bridge_api(action, ?TYPE, Name, Action),
    ?assertMatch({{_, 204, _}, _, _}, Res0),
    ok.

t_action(Config) ->
    Name = atom_to_binary(?FUNCTION_NAME),
    create_connector(?MODULE, get_rabbitmq(Config)),
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
    ?assertMatch(#{payload := Payload}, Msg),
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
    Name = atom_to_binary(?FUNCTION_NAME),
    create_connector(?MODULE, get_rabbitmq(Config)),
    create_action(?FUNCTION_NAME, Name),

    %% Emulate channel close hitting the timeout
    meck:new(amqp_channel, [passthrough, no_history]),
    meck:expect(amqp_channel, close, fun(_Pid) -> timer:sleep(infinity) end),

    %% Delete action should not exceed connector's ?CHANNEL_CLOSE_TIMEOUT
    {Time, _} = timer:tc(fun() -> delete_action(Name) end),
    ?assert(Time < 4_500_000),

    meck:unload(amqp_channel),
    ok.

t_action_not_exist_exchange(Config) ->
    Name = atom_to_binary(?FUNCTION_NAME),
    create_connector(?MODULE, get_rabbitmq(Config)),
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
    create_connector(?MODULE, get_rabbitmq(Config)),
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
    ?assertMatch(#{payload := Payload}, Msg),
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

%% Verifies that actions/sources/connectors recovers themselves when a rabbitmq
%% connection process crashes.
t_connection_crash_recovery(TCConfig) ->
    test_crash_recovery(_CrashWhat = connection_pid, TCConfig).

%% Verifies that actions/sources/connectors recovers themselves when a rabbitmq action
%% channel process crashes.
t_action_channel_crash_recovery(TCConfig) ->
    test_crash_recovery(_CrashWhat = action_chan_pid, TCConfig).

%% Verifies that actions/sources/connectors recovers themselves when a rabbitmq source
%% channel process crashes.
t_source_channel_crash_recovery(TCConfig) ->
    test_crash_recovery(_CrashWhat = source_chan_pid, TCConfig).

%% Verifies that actions/sources/connectors recovers themselves when a rabbitmq connection or
%% channel process crashes.
test_crash_recovery(CrashWhat, TCConfig) ->
    %% Sanity check
    ?assertEqual([], list_rabbitmq_channel_processes()),
    ?assertEqual([], list_rabbitmq_connection_processes()),

    {201, #{<<"status">> := <<"connected">>}} = create_connector_api(TCConfig, #{}),
    ConnectionPidsBefore = list_rabbitmq_connection_processes(),
    {201, #{<<"status">> := <<"connected">>}} = create_action_api(TCConfig, #{}),
    ActionChanPidsBefore = list_rabbitmq_channel_processes(),
    {201, #{<<"status">> := <<"connected">>}} = create_source_api(TCConfig, #{}),
    ChanPidsBefore = list_rabbitmq_channel_processes(),
    SourceChanPidsBefore = ChanPidsBefore -- ActionChanPidsBefore,
    ?assertMatch([_ | _], ChanPidsBefore),
    ?assertMatch([_ | _], ConnectionPidsBefore),

    ct:timetrap({seconds, 15}),
    ?check_trace(
        emqx_bridge_v2_testlib:snk_timetrap(),
        begin
            %% ∙ Connection/channel crashes and is detected by connector/channel health check.
            PidToKill =
                case CrashWhat of
                    connection_pid ->
                        random(ConnectionPidsBefore);
                    action_chan_pid ->
                        random(ActionChanPidsBefore);
                    source_chan_pid ->
                        random(SourceChanPidsBefore)
                end,
            ?force_ordering(
                #{?snk_kind := "rabbitmq_on_get_status_enter0"},
                #{?snk_kind := kill_connection, ?snk_span := start}
            ),
            ?force_ordering(
                #{?snk_kind := kill_connection, ?snk_span := {complete, _}},
                #{?snk_kind := "rabbitmq_on_get_status_enter1"}
            ),
            SRefs1 = snk_subscribe_to_health_checks(TCConfig),
            spawn_link(fun() ->
                ?tp_span(kill_connection, #{}, begin
                    ?tpal("killing pid ~p", [PidToKill]),
                    exit(PidToKill, boom),
                    %% Allow some time for exit signal to propagate to ecpool
                    %% worker (assuming it's monitoring the pid).  Otherwise,
                    %% assertion might succeed because (current) `gen_server` call
                    %% could fail due to the same exit signal, propagating to the
                    %% health check pid instead.
                    ct:sleep(100)
                end)
            end),
            ?assertMatch(
                #{
                    connector := {ok, _},
                    action := {ok, _},
                    source := {ok, _}
                },
                snk_receive_health_check_events(SRefs1)
            ),
            ?retry(
                100,
                20,
                ?assertMatch(
                    {200, #{<<"status">> := <<"disconnected">>}},
                    get_action_api(TCConfig)
                )
            ),
            ?retry(
                100,
                20,
                ?assertMatch(
                    {200, #{<<"status">> := <<"disconnected">>}},
                    get_source_api(TCConfig)
                )
            ),
            %%   ∙ Should be eventually restarted/recover.
            ?retry(
                200,
                10,
                ?assertEqual(
                    length(ConnectionPidsBefore),
                    length(list_rabbitmq_connection_processes())
                )
            ),
            ?retry(
                200,
                10,
                ?assertEqual(
                    length(ChanPidsBefore),
                    length(list_rabbitmq_channel_processes())
                )
            ),
            ?retry(
                200,
                10,
                ?assertMatch(
                    {200, #{<<"status">> := <<"connected">>}},
                    get_connector_api(TCConfig)
                )
            ),
            ?retry(
                200,
                10,
                ?assertMatch(
                    {200, #{<<"status">> := <<"connected">>}},
                    get_action_api(TCConfig)
                )
            ),
            ?retry(
                200,
                10,
                ?assertMatch(
                    {200, #{<<"status">> := <<"connected">>}},
                    get_source_api(TCConfig)
                )
            ),

            ok
        end,
        []
    ),
    ok.

%% Asserts that we don't leak channel processes if, for whatever reason, creating a list of
%% channels fails midway.
t_start_channel_no_leak(TCConfig) ->
    %% Sanity check
    ?assertEqual([], list_rabbitmq_channel_processes()),
    ?assertEqual([], list_rabbitmq_connection_processes()),

    {201, #{
        <<"status">> := <<"connected">>,
        <<"pool_size">> := PoolSize
    }} = create_connector_api(TCConfig, #{}),
    ct:timetrap({seconds, 5}),
    ?check_trace(
        emqx_bridge_v2_testlib:snk_timetrap(),
        begin
            ?inject_crash(
                #{?snk_kind := "rabbitmq_will_confirm_channel"},
                snabbkaffe_nemesis:periodic_crash(
                    _Period = PoolSize,
                    _DutyCycle = 0.5,
                    _Phase = 0
                )
            ),
            {201, #{<<"status">> := <<"disconnected">>}} = create_action_api(TCConfig, #{
                <<"resource_opts">> => #{
                    <<"health_check_interval">> => <<"2s">>
                }
            }),
            ?assertEqual(0, length(list_rabbitmq_channel_processes())),
            ok
        end,
        []
    ),
    ok.
