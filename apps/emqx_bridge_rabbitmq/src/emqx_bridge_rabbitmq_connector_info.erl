%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_rabbitmq_connector_info).

-behaviour(emqx_connector_info).

-export([
    type_name/0,
    bridge_types/0,
    resource_callback_module/0,
    config_transform_module/0,
    config_schema/0,
    schema_module/0,
    api_schema/1
]).

type_name() ->
    rabbitmq.

bridge_types() ->
    [rabbitmq].

resource_callback_module() ->
    emqx_bridge_rabbitmq_connector.

config_transform_module() ->
    emqx_bridge_rabbitmq_connector.

config_schema() ->
    {rabbitmq,
        hoconsc:mk(
            hoconsc:map(
                name, hoconsc:ref(emqx_bridge_rabbitmq_connector_schema, "config_connector")
            ),
            #{
                desc => <<"RabbitMQ Connector Config">>,
                required => false
            }
        )}.

schema_module() ->
    emqx_bridge_rabbitmq_connector_schema.

api_schema(Method) ->
    emqx_connector_schema:api_ref(
        emqx_bridge_rabbitmq_connector_schema, <<"rabbitmq">>, Method
    ).
