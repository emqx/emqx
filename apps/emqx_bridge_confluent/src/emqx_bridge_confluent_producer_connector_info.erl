%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_confluent_producer_connector_info).

-behaviour(emqx_connector_info).

%% API exports.
-export([
    type_name/0,
    bridge_types/0,
    resource_callback_module/0,
    config_transform_module/0,
    config_schema/0,
    schema_module/0,
    api_schema/1
]).

%%--------------------------------------------------------------------
%% API Functions
%%--------------------------------------------------------------------

type_name() ->
    confluent_producer.

bridge_types() ->
    [confluent_producer].

resource_callback_module() ->
    emqx_bridge_kafka_impl_producer.

config_transform_module() ->
    emqx_bridge_confluent_producer.

config_schema() ->
    {confluent_producer,
        hoconsc:mk(
            hoconsc:map(name, hoconsc:ref(emqx_bridge_confluent_producer, "config_connector")),
            #{
                desc => <<"Confluent Connector Config">>,
                required => false
            }
        )}.

schema_module() ->
    emqx_bridge_confluent_producer.

api_schema(Method) ->
    emqx_connector_schema:api_ref(
        emqx_bridge_confluent_producer, <<"confluent_producer">>, Method ++ "_connector"
    ).
