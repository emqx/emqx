%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_rabbitmq_connector_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include("../../emqx_connector/include/emqx_connector.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("stdlib/include/assert.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-import(emqx_bridge_rabbitmq_test_utils, [
    rabbit_mq_exchange/0,
    rabbit_mq_routing_key/0,
    rabbit_mq_queue/0,
    rabbit_mq_host/0,
    rabbit_mq_port/0,
    get_rabbitmq/1,
    ssl_options/1,
    get_channel_connection/1,
    parse_and_check/4,
    receive_message_from_rabbitmq/1
]).

%% This test SUITE requires a running RabbitMQ instance. If you don't want to
%% bring up the whole CI infrastructure with the `scripts/ct/run.sh` script
%% you can create a clickhouse instance with the following command.
%% 5672 is the default port for AMQP 0-9-1 and 15672 is the default port for
%% the HTTP management interface.
%%
%% docker run -it --rm --name rabbitmq -p 127.0.0.1:5672:5672 -p 127.0.0.1:15672:15672 rabbitmq:3.11-management

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

% %%------------------------------------------------------------------------------
% %% Testcases
% %%------------------------------------------------------------------------------

t_lifecycle(Config) ->
    perform_lifecycle_check(
        erlang:atom_to_binary(?FUNCTION_NAME),
        rabbitmq_config(),
        Config
    ).

t_start_passfile(Config) ->
    ResourceID = atom_to_binary(?FUNCTION_NAME),
    PasswordFilename = filename:join(?config(priv_dir, Config), "passfile"),
    ok = file:write_file(PasswordFilename, rabbit_mq_password()),
    InitialConfig = rabbitmq_config(#{
        password => iolist_to_binary(["file://", PasswordFilename])
    }),
    ?assertMatch(
        #{status := connected},
        create_local_resource(ResourceID, check_config(InitialConfig))
    ),
    ?assertEqual(
        ok,
        emqx_resource:remove_local(ResourceID)
    ).

perform_lifecycle_check(ResourceID, InitialConfig, TestConfig) ->
    CheckedConfig = check_config(InitialConfig),
    #{
        id := PoolName,
        state := State,
        status := InitialStatus
    } = create_local_resource(ResourceID, CheckedConfig),
    ?assertEqual(InitialStatus, connected),
    %% Instance should match the state and status of the just started resource
    {ok, ?CONNECTOR_RESOURCE_GROUP, #{
        state := State,
        status := InitialStatus
    }} =
        emqx_resource:get_instance(ResourceID),
    ?assertEqual({ok, connected}, emqx_resource:health_check(ResourceID)),
    %% Perform query as further check that the resource is working as expected
    perform_query(ResourceID, TestConfig),
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
    perform_query(ResourceID, TestConfig),
    % Stop and remove the resource in one go.
    ?assertEqual(ok, emqx_resource:remove_local(ResourceID)),
    ?assertEqual({error, not_found}, ecpool:stop_sup_pool(PoolName)),
    % Should not even be able to get the resource data out of ets now unlike just stopping.
    ?assertEqual({error, not_found}, emqx_resource:get_instance(ResourceID)).

% %%------------------------------------------------------------------------------
% %% Helpers
% %%------------------------------------------------------------------------------

check_config(Config) ->
    {ok, #{config := CheckedConfig}} =
        emqx_resource:check_config(emqx_bridge_rabbitmq_connector, Config),
    CheckedConfig.

create_local_resource(ResourceID, CheckedConfig) ->
    {ok, Bridge} = emqx_resource:create_local(
        ResourceID,
        ?CONNECTOR_RESOURCE_GROUP,
        emqx_bridge_rabbitmq_connector,
        CheckedConfig,
        #{}
    ),
    Bridge.

perform_query(PoolName, Channel) ->
    %% Send message to queue:
    ActionConfig = rabbitmq_action_config(),
    ChannelId = <<"test_channel">>,
    ?assertEqual(ok, emqx_resource_manager:add_channel(PoolName, ChannelId, ActionConfig)),
    ok = emqx_resource:query(PoolName, {ChannelId, payload()}),
    %% Get the message from queue:
    SendData = test_data(),
    RecvData = receive_message_from_rabbitmq(Channel),
    ?assertMatch(SendData, RecvData),
    ?assertEqual(ok, emqx_resource_manager:remove_channel(PoolName, ChannelId)),
    ok.

rabbitmq_config() ->
    rabbitmq_config(#{}).

rabbitmq_config(Overrides) ->
    Config =
        #{
            server => rabbit_mq_host(),
            port => 5672,
            username => <<"guest">>,
            password => rabbit_mq_password(),
            exchange => rabbit_mq_exchange(),
            routing_key => rabbit_mq_routing_key()
        },
    #{<<"config">> => maps:merge(Config, Overrides)}.

payload() ->
    #{<<"payload">> => test_data()}.

test_data() ->
    #{<<"Hello">> => <<"World">>}.

rabbitmq_action_config() ->
    #{
        config_root => actions,
        parameters => #{
            delivery_mode => non_persistent,
            exchange => rabbit_mq_exchange(),
            payload_template => <<"${.payload}">>,
            publish_confirmation_timeout => 30000,
            routing_key => rabbit_mq_routing_key(),
            wait_for_publish_confirmations => true
        }
    }.

rabbit_mq_password() ->
    <<"guest">>.
