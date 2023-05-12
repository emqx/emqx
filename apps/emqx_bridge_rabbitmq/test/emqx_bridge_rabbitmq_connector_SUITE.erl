%%--------------------------------------------------------------------
%% Copyright (c) 2022-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_rabbitmq_connector_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include("emqx_connector.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("stdlib/include/assert.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

%% This test SUITE requires a running RabbitMQ instance. If you don't want to
%% bring up the whole CI infrastuctucture with the `scripts/ct/run.sh` script
%% you can create a clickhouse instance with the following command.
%% 5672 is the default port for AMQP 0-9-1 and 15672 is the default port for
%% the HTTP managament interface.
%%
%% docker run -it --rm --name rabbitmq -p 127.0.0.1:5672:5672 -p 127.0.0.1:15672:15672 rabbitmq:3.11-management

rabbit_mq_host() ->
    <<"rabbitmq">>.

rabbit_mq_port() ->
    5672.

rabbit_mq_exchange() ->
    <<"test_exchange">>.

rabbit_mq_queue() ->
    <<"test_queue">>.

rabbit_mq_routing_key() ->
    <<"test_routing_key">>.

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    case
        emqx_common_test_helpers:is_tcp_server_available(
            erlang:binary_to_list(rabbit_mq_host()), rabbit_mq_port()
        )
    of
        true ->
            ok = emqx_common_test_helpers:start_apps([emqx_conf]),
            ok = emqx_connector_test_helpers:start_apps([emqx_resource]),
            {ok, _} = application:ensure_all_started(emqx_connector),
            {ok, _} = application:ensure_all_started(emqx_ee_connector),
            {ok, _} = application:ensure_all_started(amqp_client),
            ChannelConnection = setup_rabbit_mq_exchange_and_queue(),
            [{channel_connection, ChannelConnection} | Config];
        false ->
            case os:getenv("IS_CI") of
                "yes" ->
                    throw(no_rabbitmq);
                _ ->
                    {skip, no_rabbitmq}
            end
    end.

setup_rabbit_mq_exchange_and_queue() ->
    %% Create an exachange and a queue
    {ok, Connection} =
        amqp_connection:start(#amqp_params_network{
            host = erlang:binary_to_list(rabbit_mq_host()),
            port = rabbit_mq_port()
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

get_channel_connection(Config) ->
    proplists:get_value(channel_connection, Config).

end_per_suite(Config) ->
    #{
        connection := Connection,
        channel := Channel
    } = get_channel_connection(Config),
    ok = emqx_common_test_helpers:stop_apps([emqx_conf]),
    ok = emqx_connector_test_helpers:stop_apps([emqx_resource]),
    _ = application:stop(emqx_connector),
    %% Close the channel
    ok = amqp_channel:close(Channel),
    %% Close the connection
    ok = amqp_connection:close(Connection).

% %%------------------------------------------------------------------------------
% %% Testcases
% %%------------------------------------------------------------------------------

t_lifecycle(Config) ->
    perform_lifecycle_check(
        erlang:atom_to_binary(?MODULE),
        rabbitmq_config(),
        Config
    ).

perform_lifecycle_check(ResourceID, InitialConfig, TestConfig) ->
    #{
        channel := Channel
    } = get_channel_connection(TestConfig),
    {ok, #{config := CheckedConfig}} =
        emqx_resource:check_config(emqx_bridge_rabbitmq_connector, InitialConfig),
    {ok, #{
        state := #{poolname := PoolName} = State,
        status := InitialStatus
    }} =
        emqx_resource:create_local(
            ResourceID,
            ?CONNECTOR_RESOURCE_GROUP,
            emqx_bridge_rabbitmq_connector,
            CheckedConfig,
            #{}
        ),
    ?assertEqual(InitialStatus, connected),
    %% Instance should match the state and status of the just started resource
    {ok, ?CONNECTOR_RESOURCE_GROUP, #{
        state := State,
        status := InitialStatus
    }} =
        emqx_resource:get_instance(ResourceID),
    ?assertEqual({ok, connected}, emqx_resource:health_check(ResourceID)),
    %% Perform query as further check that the resource is working as expected
    perform_query(ResourceID, Channel),
    ?assertEqual(ok, emqx_resource:stop(ResourceID)),
    %% Resource will be listed still, but state will be changed and healthcheck will fail
    %% as the worker no longer exists.
    {ok, ?CONNECTOR_RESOURCE_GROUP, #{
        state := State,
        status := StoppedStatus
    }} = emqx_resource:get_instance(ResourceID),
    ?assertEqual(stopped, StoppedStatus),
    ?assertEqual({error, resource_is_stopped}, emqx_resource:health_check(ResourceID)),
    % Resource healthcheck shortcuts things by checking ets. Go deeper by checking pool itself.
    ?assertEqual({error, not_found}, ecpool:stop_sup_pool(PoolName)),
    % Can call stop/1 again on an already stopped instance
    ?assertEqual(ok, emqx_resource:stop(ResourceID)),
    % Make sure it can be restarted and the healthchecks and queries work properly
    ?assertEqual(ok, emqx_resource:restart(ResourceID)),
    % async restart, need to wait resource
    timer:sleep(500),
    {ok, ?CONNECTOR_RESOURCE_GROUP, #{status := InitialStatus}} =
        emqx_resource:get_instance(ResourceID),
    ?assertEqual({ok, connected}, emqx_resource:health_check(ResourceID)),
    %% Check that everything is working again by performing a query
    perform_query(ResourceID, Channel),
    % Stop and remove the resource in one go.
    ?assertEqual(ok, emqx_resource:remove_local(ResourceID)),
    ?assertEqual({error, not_found}, ecpool:stop_sup_pool(PoolName)),
    % Should not even be able to get the resource data out of ets now unlike just stopping.
    ?assertEqual({error, not_found}, emqx_resource:get_instance(ResourceID)).

% %%------------------------------------------------------------------------------
% %% Helpers
% %%------------------------------------------------------------------------------

perform_query(PoolName, Channel) ->
    %% Send message to queue:
    ok = emqx_resource:query(PoolName, {query, test_data()}),
    %% Get the message from queue:
    ok = receive_simple_test_message(Channel).

receive_simple_test_message(Channel) ->
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
            Expected = test_data(),
            ?assertEqual(Expected, emqx_utils_json:decode(Content#amqp_msg.payload)),
            %% Ack the message
            amqp_channel:cast(Channel, #'basic.ack'{delivery_tag = DeliveryTag}),
            %% Cancel the consumer
            #'basic.cancel_ok'{consumer_tag = ConsumerTag} =
                amqp_channel:call(Channel, #'basic.cancel'{consumer_tag = ConsumerTag}),
            ok
    end.

rabbitmq_config() ->
    Config =
        #{
            server => rabbit_mq_host(),
            port => 5672,
            username => <<"guest">>,
            password => <<"guest">>,
            exchange => rabbit_mq_exchange(),
            routing_key => rabbit_mq_routing_key()
        },
    #{<<"config">> => Config}.

test_data() ->
    #{<<"msg_field">> => <<"Hello">>}.
