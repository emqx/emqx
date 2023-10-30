%%--------------------------------------------------------------------
%% Copyright (c) 2022-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_rabbitmq_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("emqx_connector/include/emqx_connector.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("stdlib/include/assert.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

%% See comment in
%% apps/emqx_bridge_rabbitmq/test/emqx_bridge_rabbitmq_connector_SUITE.erl for how to
%% run this without bringing up the whole CI infrastucture

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

%%------------------------------------------------------------------------------
%% Common Test Setup, Teardown and Testcase List
%%------------------------------------------------------------------------------

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

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(tcp, Config) ->
    RabbitMQHost = os:getenv("RABBITMQ_PLAIN_HOST", "rabbitmq"),
    RabbitMQPort = list_to_integer(os:getenv("RABBITMQ_PLAIN_PORT", "5672")),
    case emqx_common_test_helpers:is_tcp_server_available(RabbitMQHost, RabbitMQPort) of
        true ->
            Config1 = common_init_per_group(#{
                host => RabbitMQHost, port => RabbitMQPort, tls => false
            }),
            Config1 ++ Config;
        false ->
            case os:getenv("IS_CI") of
                "yes" ->
                    throw(no_rabbitmq);
                _ ->
                    {skip, no_rabbitmq}
            end
    end;
init_per_group(tls, Config) ->
    RabbitMQHost = os:getenv("RABBITMQ_TLS_HOST", "rabbitmq"),
    RabbitMQPort = list_to_integer(os:getenv("RABBITMQ_TLS_PORT", "5671")),
    case emqx_common_test_helpers:is_tcp_server_available(RabbitMQHost, RabbitMQPort) of
        true ->
            Config1 = common_init_per_group(#{
                host => RabbitMQHost, port => RabbitMQPort, tls => true
            }),
            Config1 ++ Config;
        false ->
            case os:getenv("IS_CI") of
                "yes" ->
                    throw(no_rabbitmq);
                _ ->
                    {skip, no_rabbitmq}
            end
    end;
init_per_group(_Group, Config) ->
    Config.

common_init_per_group(Opts) ->
    emqx_common_test_helpers:render_and_load_app_config(emqx_conf),
    ok = emqx_common_test_helpers:start_apps([emqx_conf, emqx_bridge]),
    ok = emqx_connector_test_helpers:start_apps([emqx_resource]),
    {ok, _} = application:ensure_all_started(emqx_connector),
    {ok, _} = application:ensure_all_started(amqp_client),
    emqx_mgmt_api_test_util:init_suite(),
    ChannelConnection = setup_rabbit_mq_exchange_and_queue(Opts),
    [{channel_connection, ChannelConnection}].

setup_rabbit_mq_exchange_and_queue(#{host := RabbitMQHost, port := RabbitMQPort, tls := UseTLS}) ->
    SSLOptions =
        case UseTLS of
            false ->
                none;
            true ->
                CertsDir = filename:join([
                    emqx_common_test_helpers:proj_root(),
                    ".ci",
                    "docker-compose-file",
                    "certs"
                ]),
                emqx_tls_lib:to_client_opts(
                    #{
                        enable => true,
                        cacertfile => filename:join([CertsDir, "ca.crt"]),
                        certfile => filename:join([CertsDir, "client.pem"]),
                        keyfile => filename:join([CertsDir, "client.key"])
                    }
                )
        end,
    %% Create an exachange and a queue
    {ok, Connection} =
        amqp_connection:start(#amqp_params_network{
            host = RabbitMQHost,
            port = RabbitMQPort,
            ssl_options = SSLOptions
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

end_per_group(_Group, Config) ->
    #{
        connection := Connection,
        channel := Channel
    } = get_channel_connection(Config),
    emqx_mgmt_api_test_util:end_suite(),
    ok = emqx_common_test_helpers:stop_apps([emqx_conf]),
    ok = emqx_connector_test_helpers:stop_apps([emqx_resource]),
    _ = application:stop(emqx_connector),
    _ = application:stop(emqx_bridge),
    %% Close the channel
    ok = amqp_channel:close(Channel),
    %% Close the connection
    ok = amqp_connection:close(Connection).

init_per_testcase(_, Config) ->
    Config.

end_per_testcase(_, _Config) ->
    ok.

rabbitmq_config(Config) ->
    %%SQL = maps:get(sql, Config, sql_insert_template_for_bridge()),
    BatchSize = maps:get(batch_size, Config, 1),
    BatchTime = maps:get(batch_time_ms, Config, 0),
    Name = atom_to_binary(?MODULE),
    Server = maps:get(server, Config, rabbit_mq_host()),
    Port = maps:get(port, Config, rabbit_mq_port()),
    Template = maps:get(payload_template, Config, <<"">>),
    ConfigString =
        io_lib:format(
            "bridges.rabbitmq.~s {\n"
            "  enable = true\n"
            "  server = \"~s\"\n"
            "  port = ~p\n"
            "  username = \"guest\"\n"
            "  password = \"guest\"\n"
            "  routing_key = \"~s\"\n"
            "  exchange = \"~s\"\n"
            "  payload_template = \"~s\"\n"
            "  resource_opts = {\n"
            "    batch_size = ~b\n"
            "    batch_time = ~bms\n"
            "  }\n"
            "}\n",
            [
                Name,
                Server,
                Port,
                rabbit_mq_routing_key(),
                rabbit_mq_exchange(),
                Template,
                BatchSize,
                BatchTime
            ]
        ),
    ct:pal(ConfigString),
    parse_and_check(ConfigString, <<"rabbitmq">>, Name).

parse_and_check(ConfigString, BridgeType, Name) ->
    {ok, RawConf} = hocon:binary(ConfigString, #{format => map}),
    hocon_tconf:check_plain(emqx_bridge_schema, RawConf, #{required => false, atom_key => false}),
    #{<<"bridges">> := #{BridgeType := #{Name := RetConfig}}} = RawConf,
    RetConfig.

make_bridge(Config) ->
    Type = <<"rabbitmq">>,
    Name = atom_to_binary(?MODULE),
    BridgeConfig = rabbitmq_config(Config),
    {ok, _} = emqx_bridge:create(
        Type,
        Name,
        BridgeConfig
    ),
    emqx_bridge_resource:bridge_id(Type, Name).

delete_bridge() ->
    Type = <<"rabbitmq">>,
    Name = atom_to_binary(?MODULE),
    ok = emqx_bridge:remove(Type, Name).

%%------------------------------------------------------------------------------
%% Test Cases
%%------------------------------------------------------------------------------

t_make_delete_bridge(_Config) ->
    make_bridge(#{}),
    %% Check that the new brige is in the list of bridges
    Bridges = emqx_bridge:list(),
    Name = atom_to_binary(?MODULE),
    IsRightName =
        fun
            (#{name := BName}) when BName =:= Name ->
                true;
            (_) ->
                false
        end,
    ?assert(lists:any(IsRightName, Bridges)),
    delete_bridge(),
    BridgesAfterDelete = emqx_bridge:list(),
    ?assertNot(lists:any(IsRightName, BridgesAfterDelete)),
    ok.

t_make_delete_bridge_non_existing_server(_Config) ->
    make_bridge(#{server => <<"non_existing_server">>, port => 3174}),
    %% Check that the new brige is in the list of bridges
    Bridges = emqx_bridge:list(),
    Name = atom_to_binary(?MODULE),
    IsRightName =
        fun
            (#{name := BName}) when BName =:= Name ->
                true;
            (_) ->
                false
        end,
    ?assert(lists:any(IsRightName, Bridges)),
    delete_bridge(),
    BridgesAfterDelete = emqx_bridge:list(),
    ?assertNot(lists:any(IsRightName, BridgesAfterDelete)),
    ok.

t_send_message_query(Config) ->
    BridgeID = make_bridge(#{batch_size => 1}),
    Payload = #{<<"key">> => 42, <<"data">> => <<"RabbitMQ">>, <<"timestamp">> => 10000},
    %% This will use the SQL template included in the bridge
    emqx_bridge:send_message(BridgeID, Payload),
    %% Check that the data got to the database
    ?assertEqual(Payload, receive_simple_test_message(Config)),
    delete_bridge(),
    ok.

t_send_message_query_with_template(Config) ->
    BridgeID = make_bridge(#{
        batch_size => 1,
        payload_template =>
            <<
                "{"
                "      \\\"key\\\": ${key},"
                "      \\\"data\\\": \\\"${data}\\\","
                "      \\\"timestamp\\\": ${timestamp},"
                "      \\\"secret\\\": 42"
                "}"
            >>
    }),
    Payload = #{
        <<"key">> => 7,
        <<"data">> => <<"RabbitMQ">>,
        <<"timestamp">> => 10000
    },
    emqx_bridge:send_message(BridgeID, Payload),
    %% Check that the data got to the database
    ExpectedResult = Payload#{
        <<"secret">> => 42
    },
    ?assertEqual(ExpectedResult, receive_simple_test_message(Config)),
    delete_bridge(),
    ok.

t_send_simple_batch(Config) ->
    BridgeConf =
        #{
            batch_size => 100
        },
    BridgeID = make_bridge(BridgeConf),
    Payload = #{<<"key">> => 42, <<"data">> => <<"RabbitMQ">>, <<"timestamp">> => 10000},
    emqx_bridge:send_message(BridgeID, Payload),
    ?assertEqual(Payload, receive_simple_test_message(Config)),
    delete_bridge(),
    ok.

t_send_simple_batch_with_template(Config) ->
    BridgeConf =
        #{
            batch_size => 100,
            payload_template =>
                <<
                    "{"
                    "      \\\"key\\\": ${key},"
                    "      \\\"data\\\": \\\"${data}\\\","
                    "      \\\"timestamp\\\": ${timestamp},"
                    "      \\\"secret\\\": 42"
                    "}"
                >>
        },
    BridgeID = make_bridge(BridgeConf),
    Payload = #{
        <<"key">> => 7,
        <<"data">> => <<"RabbitMQ">>,
        <<"timestamp">> => 10000
    },
    emqx_bridge:send_message(BridgeID, Payload),
    ExpectedResult = Payload#{
        <<"secret">> => 42
    },
    ?assertEqual(ExpectedResult, receive_simple_test_message(Config)),
    delete_bridge(),
    ok.

t_heavy_batching(Config) ->
    NumberOfMessages = 20000,
    BridgeConf = #{
        batch_size => 10173,
        batch_time_ms => 50
    },
    BridgeID = make_bridge(BridgeConf),
    SendMessage = fun(Key) ->
        Payload = #{
            <<"key">> => Key
        },
        emqx_bridge:send_message(BridgeID, Payload)
    end,
    [SendMessage(Key) || Key <- lists:seq(1, NumberOfMessages)],
    AllMessages = lists:foldl(
        fun(_, Acc) ->
            Message = receive_simple_test_message(Config),
            #{<<"key">> := Key} = Message,
            Acc#{Key => true}
        end,
        #{},
        lists:seq(1, NumberOfMessages)
    ),
    ?assertEqual(NumberOfMessages, maps:size(AllMessages)),
    delete_bridge(),
    ok.

receive_simple_test_message(Config) ->
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
    end.

rabbitmq_config() ->
    Config =
        #{
            server => rabbit_mq_host(),
            port => 5672,
            exchange => rabbit_mq_exchange(),
            routing_key => rabbit_mq_routing_key()
        },
    #{<<"config">> => Config}.

test_data() ->
    #{<<"msg_field">> => <<"Hello">>}.
