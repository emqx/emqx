%%--------------------------------------------------------------------
%% Copyright (c) 2024-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_rabbitmq_testlib).

-export([
    connector_config/1,
    action_config/1,
    source_config/1,

    ssl_options/1,
    connect_client/1,
    connect_and_setup_exchange_and_queue/1,
    cleanup_client_and_queue/1,

    receive_message/1,
    receive_message/3,
    publish_message/2
]).

-include_lib("amqp_client/include/amqp_client.hrl").

-define(CONNECTOR_TYPE_BIN, <<"rabbitmq">>).
-define(ACTION_TYPE_BIN, <<"rabbitmq">>).
-define(SOURCE_TYPE_BIN, <<"rabbitmq">>).

-define(USER, <<"guest">>).
-define(PASSWORD, <<"guest">>).
-define(EXCHANGE, <<"messages">>).
-define(QUEUE, <<"test_queue">>).
-define(ROUTING_KEY, <<"test_routing_key">>).

connector_config(Overrides) ->
    Defaults = #{
        <<"enable">> => true,
        <<"description">> => <<"my connector">>,
        <<"tags">> => [<<"some">>, <<"tags">>],
        <<"server">> => <<"rabbitmq">>,
        <<"port">> => 5672,
        <<"username">> => ?USER,
        <<"password">> => ?PASSWORD,
        <<"pool_size">> => 8,
        <<"timeout">> => <<"5s">>,
        <<"virtual_host">> => <<"/">>,
        <<"heartbeat">> => <<"30s">>,
        <<"resource_opts">> =>
            emqx_bridge_v2_testlib:common_connector_resource_opts()
    },
    InnerConfigMap = emqx_utils_maps:deep_merge(Defaults, Overrides),
    emqx_bridge_v2_testlib:parse_and_check_connector(?CONNECTOR_TYPE_BIN, <<"x">>, InnerConfigMap).

action_config(Overrides) ->
    Defaults = #{
        <<"enable">> => true,
        <<"description">> => <<"my action">>,
        <<"tags">> => [<<"some">>, <<"tags">>],
        <<"parameters">> => #{
            <<"exchange">> => ?EXCHANGE,
            <<"payload_template">> => <<"${.payload}">>,
            <<"routing_key">> => ?ROUTING_KEY,
            <<"delivery_mode">> => <<"non_persistent">>,
            <<"publish_confirmation_timeout">> => <<"30s">>,
            <<"wait_for_publish_confirmations">> => true
        },
        <<"resource_opts">> =>
            emqx_bridge_v2_testlib:common_action_resource_opts()
    },
    InnerConfigMap = emqx_utils_maps:deep_merge(Defaults, Overrides),
    emqx_bridge_v2_testlib:parse_and_check(action, ?ACTION_TYPE_BIN, <<"x">>, InnerConfigMap).

source_config(Overrides) ->
    Defaults = #{
        <<"enable">> => true,
        <<"description">> => <<"my action">>,
        <<"tags">> => [<<"some">>, <<"tags">>],
        <<"parameters">> => #{
            <<"no_ack">> => true,
            <<"queue">> => <<"test_queue">>,
            <<"wait_for_publish_confirmations">> => true
        },
        <<"resource_opts">> =>
            emqx_bridge_v2_testlib:common_source_resource_opts()
    },
    InnerConfigMap = emqx_utils_maps:deep_merge(Defaults, Overrides),
    emqx_bridge_v2_testlib:parse_and_check(source, ?SOURCE_TYPE_BIN, <<"x">>, InnerConfigMap).

connect_and_setup_exchange_and_queue(ClientOpts) ->
    #{
        exchange := Exchange,
        queue := Queue,
        routing_key := RoutingKey
    } = ClientOpts,
    {Connection, Channel} = connect_client(ClientOpts),
    %% Create an exchange
    #'exchange.declare_ok'{} =
        amqp_channel:call(
            Channel,
            #'exchange.declare'{
                exchange = Exchange,
                type = <<"topic">>
            }
        ),
    %% Create a queue
    #'queue.declare_ok'{} =
        amqp_channel:call(
            Channel,
            #'queue.declare'{queue = Queue}
        ),
    %% Bind the queue to the exchange
    #'queue.bind_ok'{} =
        amqp_channel:call(
            Channel,
            #'queue.bind'{
                queue = Queue,
                exchange = Exchange,
                routing_key = RoutingKey
            }
        ),
    amqp_channel:close(Channel),
    amqp_connection:close(Connection),
    ok.

connect_client(Opts) ->
    #{
        host := Host,
        port := Port,
        use_tls := UseTLS
    } = Opts,
    SSLOptions =
        case UseTLS of
            false -> none;
            true -> emqx_tls_lib:to_client_opts(ssl_options(UseTLS))
        end,
    %% Create an exchange and a queue
    {ok, Connection} =
        amqp_connection:start(#amqp_params_network{
            host = str(Host),
            port = Port,
            ssl_options = SSLOptions
        }),
    {ok, Channel} = amqp_connection:open_channel(Connection),
    {Connection, Channel}.

cleanup_client_and_queue(ClientOpts) ->
    #{queue := Queue} = ClientOpts,
    {Connection, Channel} = connect_client(ClientOpts),
    amqp_channel:call(Channel, #'queue.purge'{queue = Queue}),
    amqp_channel:close(Channel),
    amqp_connection:close(Connection),
    ok.

publish_message(Payload, ClientOpts) ->
    {Connection, Channel} = connect_client(ClientOpts),
    #{
        exchange := Exchange,
        routing_key := RoutingKey
    } = ClientOpts,
    MessageProperties = #'P_basic'{
        headers = [],
        delivery_mode = 1
    },
    Method = #'basic.publish'{
        exchange = Exchange,
        routing_key = RoutingKey
    },
    amqp_channel:call(
        Channel,
        Method,
        #amqp_msg{
            payload = Payload,
            props = MessageProperties
        }
    ),
    amqp_channel:close(Channel),
    amqp_connection:close(Connection),
    ok.

receive_message(ClientOpts) ->
    {Connection, Channel} = connect_client(ClientOpts),
    Res = receive_message(Connection, Channel, ClientOpts),
    amqp_channel:close(Channel),
    amqp_connection:close(Connection),
    Res.

receive_message(_Connection, Channel, ClientOpts) ->
    #{queue := Queue} = ClientOpts,
    #'basic.consume_ok'{consumer_tag = ConsumerTag} =
        amqp_channel:call(
            Channel,
            #'basic.consume'{queue = Queue}
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
                payload = Payload0
            } = Content,
            Payload =
                case emqx_utils_json:safe_decode(Payload0) of
                    {ok, DecodedPayload} -> DecodedPayload;
                    {error, _} -> Payload0
                end,
            #{
                payload => Payload,
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
            }
    after 5_000 ->
        ct:fail("Did not receive message within 5 second")
    end.

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

str(X) -> emqx_utils_conv:str(X).
