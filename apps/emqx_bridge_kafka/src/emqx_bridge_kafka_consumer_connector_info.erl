%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_kafka_consumer_connector_info).

-behaviour(emqx_connector_info).

-export([
    type_name/0,
    bridge_types/0,
    resource_callback_module/0,
    config_schema/0,
    schema_module/0,
    api_schema/1
]).

type_name() ->
    kafka_consumer.

bridge_types() ->
    [kafka_consumer].

resource_callback_module() ->
    emqx_bridge_kafka_impl_consumer.

config_schema() ->
    {kafka_consumer,
        hoconsc:mk(
            hoconsc:map(name, hoconsc:ref(emqx_bridge_kafka_consumer_schema, "config_connector")),
            #{
                desc => <<"Kafka Consumer Connector Config">>,
                required => false
            }
        )}.

schema_module() ->
    emqx_bridge_kafka_consumer_schema.

api_schema(Method) ->
    emqx_connector_schema:api_ref(
        emqx_bridge_kafka_consumer_schema, <<"kafka_consumer">>, Method ++ "_connector"
    ).
