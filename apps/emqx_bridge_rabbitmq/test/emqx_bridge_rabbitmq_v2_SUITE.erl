%%--------------------------------------------------------------------
%% Copyright (c) 2022-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_rabbitmq_v2_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("emqx_connector/include/emqx_connector.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("stdlib/include/assert.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-define(TYPE, <<"rabbitmq">>).
-import(emqx_common_test_helpers, [on_exit/1]).

rabbit_mq_host() ->
    <<"rabbitmq">>.

rabbit_mq_port() ->
    5672.

rabbit_mq_exchange() ->
    <<"messages">>.

rabbit_mq_queue() ->
    <<"test_queue">>.

rabbit_mq_routing_key() ->
    <<"test_routing_key">>.

get_channel_connection(Config) ->
    proplists:get_value(channel_connection, Config).

get_rabbitmq(Config) ->
    proplists:get_value(rabbitmq, Config).

%%------------------------------------------------------------------------------
%% Common Test Setup, Tear down and Testcase List
%%------------------------------------------------------------------------------

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    RabbitMQHost = os:getenv("RABBITMQ_PLAIN_HOST", "rabbitmq"),
    RabbitMQPort = list_to_integer(os:getenv("RABBITMQ_PLAIN_PORT", "5672")),
    case emqx_common_test_helpers:is_tcp_server_available(RabbitMQHost, RabbitMQPort) of
        true ->
            Config1 = common_init(#{
                host => RabbitMQHost, port => RabbitMQPort
            }),
            Name = atom_to_binary(?MODULE),
            Config2 = [{connector, Name} | Config1 ++ Config],
            create_connector(Name, get_rabbitmq(Config2)),
            Config2;
        false ->
            case os:getenv("IS_CI") of
                "yes" ->
                    throw(no_rabbitmq);
                _ ->
                    {skip, no_rabbitmq}
            end
    end.

common_init(Opts) ->
    emqx_common_test_helpers:render_and_load_app_config(emqx_conf),
    ok = emqx_common_test_helpers:start_apps([
        emqx_conf, emqx_bridge, emqx_bridge_rabbitmq, emqx_rule_engine
    ]),
    ok = emqx_connector_test_helpers:start_apps([emqx_resource]),
    {ok, _} = application:ensure_all_started(emqx_connector),
    {ok, _} = application:ensure_all_started(amqp_client),
    emqx_mgmt_api_test_util:init_suite(),
    #{host := Host, port := Port} = Opts,
    ChannelConnection = setup_rabbit_mq_exchange_and_queue(Host, Port),
    [
        {channel_connection, ChannelConnection},
        {rabbitmq, #{server => Host, port => Port}}
    ].

setup_rabbit_mq_exchange_and_queue(Host, Port) ->
    %% Create an exchange and a queue
    {ok, Connection} =
        amqp_connection:start(#amqp_params_network{
            host = Host,
            port = Port,
            ssl_options = none
        }),
    {ok, Channel} = amqp_connection:open_channel(Connection),
    %% Create an exchange
    #'exchange.declare_ok'{} =
        amqp_channel:call(
            Channel,
            #'exchange.declare'{
                exchange = rabbit_mq_exchange(),
                type = <<"topic">>
            }
        ),
    %% Create a queue
    #'queue.declare_ok'{} =
        amqp_channel:call(
            Channel,
            #'queue.declare'{queue = rabbit_mq_queue()}
        ),
    %% Bind the queue to the exchange
    #'queue.bind_ok'{} =
        amqp_channel:call(
            Channel,
            #'queue.bind'{
                queue = rabbit_mq_queue(),
                exchange = rabbit_mq_exchange(),
                routing_key = rabbit_mq_routing_key()
            }
        ),
    #{
        connection => Connection,
        channel => Channel
    }.

end_per_suite(Config) ->
    delete_connector(proplists:get_value(connector, Config)),
    #{
        connection := Connection,
        channel := Channel
    } = get_channel_connection(Config),
    emqx_mgmt_api_test_util:end_suite(),
    ok = emqx_common_test_helpers:stop_apps([emqx_conf, emqx_bridge_rabbitmq, emqx_rule_engine]),
    ok = emqx_connector_test_helpers:stop_apps([emqx_resource]),
    _ = application:stop(emqx_connector),
    _ = application:stop(emqx_bridge),
    %% Close the channel
    ok = amqp_channel:close(Channel),
    %% Close the connection
    ok = amqp_connection:close(Connection).

rabbitmq_connector(Config) ->
    Name = atom_to_binary(?MODULE),
    Server = maps:get(server, Config, rabbit_mq_host()),
    Port = maps:get(port, Config, rabbit_mq_port()),
    ConfigStr =
        io_lib:format(
            "connectors.rabbitmq.~s {\n"
            "  enable = true\n"
            "  ssl = {enable = false}\n"
            "  server = \"~s\"\n"
            "  port = ~p\n"
            "  username = \"guest\"\n"
            "  password = \"guest\"\n"
            "}\n",
            [
                Name,
                Server,
                Port
            ]
        ),
    ct:pal(ConfigStr),
    parse_and_check(<<"connectors">>, emqx_connector_schema, ConfigStr, <<"rabbitmq">>, Name).

rabbitmq_source() ->
    Name = atom_to_binary(?MODULE),
    ConfigStr =
        io_lib:format(
            "sources.rabbitmq.~s {\n"
            "connector = ~s\n"
            "enable = true\n"
            "parameters {\n"
            "no_ack = true\n"
            "queue = ~s\n"
            "wait_for_publish_confirmations = true\n"
            "}}\n",
            [
                Name,
                Name,
                rabbit_mq_queue()
            ]
        ),
    ct:pal(ConfigStr),
    parse_and_check(<<"sources">>, emqx_bridge_v2_schema, ConfigStr, <<"rabbitmq">>, Name).

rabbitmq_action() ->
    Name = atom_to_binary(?MODULE),
    ConfigStr =
        io_lib:format(
            "actions.rabbitmq.~s {\n"
            "connector = ~s\n"
            "enable = true\n"
            "parameters {\n"
            "exchange: ~s\n"
            "payload_template: \"${.payload}\"\n"
            "routing_key: ~s\n"
            "delivery_mode: non_persistent\n"
            "publish_confirmation_timeout: 30s\n"
            "wait_for_publish_confirmations = true\n"
            "}}\n",
            [
                Name,
                Name,
                rabbit_mq_exchange(),
                rabbit_mq_routing_key()
            ]
        ),
    ct:pal(ConfigStr),
    parse_and_check(<<"actions">>, emqx_bridge_v2_schema, ConfigStr, <<"rabbitmq">>, Name).

parse_and_check(Key, Mod, ConfigStr, Type, Name) ->
    {ok, RawConf} = hocon:binary(ConfigStr, #{format => map}),
    hocon_tconf:check_plain(Mod, RawConf, #{required => false, atom_key => false}),
    #{Key := #{Type := #{Name := RetConfig}}} = RawConf,
    RetConfig.

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
    Msg = receive_test_message_from_rabbitmq(Config),
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

receive_test_message_from_rabbitmq(Config) ->
    #{channel := Channel} = get_channel_connection(Config),
    #'basic.consume_ok'{consumer_tag = ConsumerTag} =
        amqp_channel:call(
            Channel,
            #'basic.consume'{
                queue = rabbit_mq_queue()
            }
        ),
    receive
        %% This is the first message received
        #'basic.consume_ok'{} ->
            ok
    end,
    receive
        {#'basic.deliver'{delivery_tag = DeliveryTag}, Content} ->
            %% Ack the message
            amqp_channel:cast(Channel, #'basic.ack'{delivery_tag = DeliveryTag}),
            %% Cancel the consumer
            #'basic.cancel_ok'{consumer_tag = ConsumerTag} =
                amqp_channel:call(Channel, #'basic.cancel'{consumer_tag = ConsumerTag}),
            emqx_utils_json:decode(Content#amqp_msg.payload)
    after 5000 ->
        ?assert(false, "Did not receive message within 5 second")
    end.
