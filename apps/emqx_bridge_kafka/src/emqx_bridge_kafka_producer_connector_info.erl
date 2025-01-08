%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_kafka_producer_connector_info).

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
    kafka_producer.

bridge_types() ->
    [kafka, kafka_producer].

resource_callback_module() ->
    emqx_bridge_kafka_impl_producer.

config_schema() ->
    {kafka_producer,
        hoconsc:mk(
            hoconsc:map(name, hoconsc:ref(emqx_bridge_kafka, "config_connector")),
            #{
                desc => <<"Kafka Producer Connector Config">>,
                required => false
            }
        )}.

schema_module() ->
    emqx_bridge_kafka.

api_schema(Method) ->
    emqx_connector_schema:api_ref(
        emqx_bridge_kafka, <<"kafka_producer">>, Method ++ "_connector"
    ).
