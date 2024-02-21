%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_rabbitmq_v2_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("emqx_connector/include/emqx_connector.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("stdlib/include/assert.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

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

rabbitmq_action() ->
    Name = atom_to_binary(?MODULE),
    Action = #{
        <<"actions">> => #{
            <<"rabbitmq">> => #{
                Name => #{
                    <<"connector">> => Name,
                    <<"enable">> => true,
                    <<"parameters">> => #{
                        <<"exchange">> => rabbit_mq_exchange(),
                        <<"payload_template">> => <<"${.payload}">>,
                        <<"routing_key">> => rabbit_mq_routing_key(),
                        <<"delivery_mode">> => <<"non_persistent">>,
                        <<"publish_confirmation_timeout">> => <<"30s">>,
                        <<"wait_for_publish_confirmations">> => true
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

create_action(Name) ->
    Action = rabbitmq_action(),
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
            sql => <<"select * from \"$bridges/rabbitmq:", Name/binary, "\"">>,
            id => atom_to_binary(?FUNCTION_NAME),
            actions => [
                #{
                    args => #{
                        topic => Topic,
                        mqtt_properties => #{},
                        payload => <<"${payload}">>,
                        qos => 0,
                        retain => false,
                        user_properties => []
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
    PayloadBin = emqx_utils_json:encode(payload()),
    ?assertMatch(
        [
            #{
                dup := false,
                properties := undefined,
                topic := Topic,
                qos := 0,
                payload := PayloadBin,
                retain := false
            }
        ],
        receive_messages(1)
    ),
    ok = emqtt:disconnect(C1),
    ok = delete_source(Name),
    SourcesAfterDelete = emqx_bridge_v2:list(sources),
    ?assertNot(lists:any(Any, SourcesAfterDelete), SourcesAfterDelete),
    ok.

t_action(Config) ->
    Name = atom_to_binary(?FUNCTION_NAME),
    create_action(Name),
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
    ok = delete_action(Name),
    ActionsAfterDelete = emqx_bridge_v2:list(actions),
    ?assertNot(lists:any(Any, ActionsAfterDelete), ActionsAfterDelete),
    ok.

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
