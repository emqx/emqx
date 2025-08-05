%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_rabbitmq_test_utils).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

init_per_group(tcp = Group, Config) ->
    RabbitMQHost = os:getenv("RABBITMQ_PLAIN_HOST", "rabbitmq"),
    RabbitMQPort = list_to_integer(os:getenv("RABBITMQ_PLAIN_PORT", "5672")),
    case emqx_common_test_helpers:is_tcp_server_available(RabbitMQHost, RabbitMQPort) of
        true ->
            Config1 = common_init_per_group(#{
                group => Group,
                tc_config => Config,
                host => RabbitMQHost,
                port => RabbitMQPort,
                tls => false
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
init_per_group(tls = Group, Config) ->
    RabbitMQHost = os:getenv("RABBITMQ_TLS_HOST", "rabbitmq"),
    RabbitMQPort = list_to_integer(os:getenv("RABBITMQ_TLS_PORT", "5671")),
    case emqx_common_test_helpers:is_tcp_server_available(RabbitMQHost, RabbitMQPort) of
        true ->
            Config1 = common_init_per_group(#{
                group => Group,
                tc_config => Config,
                host => RabbitMQHost,
                port => RabbitMQPort,
                tls => true
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
    #{group := Group, tc_config := Config} = Opts,
    Apps = emqx_cth_suite:start(
        [
            emqx,
            emqx_conf,
            emqx_connector,
            emqx_bridge_rabbitmq,
            emqx_bridge,
            emqx_rule_engine,
            emqx_management,
            emqx_mgmt_api_test_util:emqx_dashboard()
        ],
        #{work_dir => emqx_cth_suite:work_dir(Group, Config)}
    ),
    #{host := Host, port := Port, tls := UseTLS} = Opts,
    ChannelConnection = setup_rabbit_mq_exchange_and_queue(Host, Port, UseTLS),
    [
        {apps, Apps},
        {channel_connection, ChannelConnection},
        {rabbitmq, #{server => Host, port => Port, tls => UseTLS}}
    ].

setup_rabbit_mq_exchange_and_queue(Host, Port, UseTLS) ->
    SSLOptions =
        case UseTLS of
            false -> none;
            true -> emqx_tls_lib:to_client_opts(ssl_options(UseTLS))
        end,
    %% Create an exchange and a queue
    {ok, Connection} =
        amqp_connection:start(#amqp_params_network{
            host = Host,
            port = Port,
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
    #{channel := Channel} = get_channel_connection(Config),
    amqp_channel:call(Channel, #'queue.purge'{queue = rabbit_mq_queue()}),
    Apps = ?config(apps, Config),
    %% Stops AMQP channels and clients as well.
    emqx_cth_suite:stop(Apps).

rabbit_mq_host() ->
    list_to_binary(os:getenv("RABBITMQ_PLAIN_HOST", "rabbitmq")).

rabbit_mq_port() ->
    list_to_integer(os:getenv("RABBITMQ_PLAIN_PORT", "5672")).

rabbit_mq_exchange() ->
    <<"messages">>.

rabbit_mq_default_exchange() ->
    <<>>.

rabbit_mq_queue() ->
    <<"test_queue">>.

rabbit_mq_routing_key() ->
    <<"test_routing_key">>.

get_rabbitmq(Config) ->
    proplists:get_value(rabbitmq, Config).

get_channel_connection(Config) ->
    proplists:get_value(channel_connection, Config).

ssl_options(true) ->
    CertsDir = filename:join([
        emqx_common_test_helpers:proj_root(),
        ".ci",
        "docker-compose-file",
        "certs"
    ]),
    #{
        enable => true,
        cacertfile => filename:join([CertsDir, "ca.crt"]),
        certfile => filename:join([CertsDir, "client.pem"]),
        keyfile => filename:join([CertsDir, "client.key"])
    };
ssl_options(false) ->
    #{
        enable => false
    }.

%% todo: delete this and use testlib directly
parse_and_check(_, emqx_connector_schema, Conf, Name) ->
    emqx_bridge_v2_testlib:parse_and_check_connector(<<"rabbitmq">>, Name, Conf);
parse_and_check(<<"sources">>, emqx_bridge_v2_schema, Conf, Name) ->
    emqx_bridge_v2_testlib:parse_and_check(source, <<"rabbitmq">>, Name, Conf);
parse_and_check(<<"actions">>, emqx_bridge_v2_schema, Conf, Name) ->
    emqx_bridge_v2_testlib:parse_and_check(action, <<"rabbitmq">>, Name, Conf).

receive_message_from_rabbitmq(Config) ->
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
            #amqp_msg{
                props = #'P_basic'{
                    app_id = AppId,
                    cluster_id = ClusterId,
                    content_encoding = ContentEncoding,
                    content_type = ContentType,
                    correlation_id = CorrelationId,
                    expiration = Expiration,
                    headers = Headers,
                    message_id = MessageId,
                    reply_to = ReplyTo,
                    timestamp = Timestamp,
                    type = Type,
                    user_id = UserId
                },
                payload = Payload
            } = Content,
            case emqx_utils_json:safe_decode(Payload) of
                {ok, DecodedPayload} ->
                    #{
                        payload => DecodedPayload,
                        headers => Headers,
                        props => #{
                            app_id => AppId,
                            cluster_id => ClusterId,
                            content_encoding => ContentEncoding,
                            content_type => ContentType,
                            correlation_id => CorrelationId,
                            expiration => Expiration,
                            message_id => MessageId,
                            reply_to => ReplyTo,
                            timestamp => Timestamp,
                            type => Type,
                            user_id => UserId
                        }
                    };
                {error, _} ->
                    ct:fail({"Failed to decode the message", Payload})
            end
    after 5_000 ->
        ct:fail("Did not receive message within 5 second")
    end.
