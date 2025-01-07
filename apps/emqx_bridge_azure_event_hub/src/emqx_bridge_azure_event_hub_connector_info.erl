%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%%--------------------------------------------------------------------
%% @doc
%% Implementation of emqx_connector_info for Azure Event Hub connector.
%% This module provides connector-specific information and configurations
%% required by the emqx_connector application.
%%--------------------------------------------------------------------
-module(emqx_bridge_azure_event_hub_connector_info).

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
    azure_event_hub_producer.

bridge_types() ->
    [azure_event_hub_producer].

resource_callback_module() ->
    emqx_bridge_kafka_impl_producer.

config_transform_module() ->
    emqx_bridge_azure_event_hub.

config_schema() ->
    {azure_event_hub_producer,
        hoconsc:mk(
            hoconsc:map(name, hoconsc:ref(emqx_bridge_azure_event_hub, "config_connector")),
            #{
                desc => <<"Azure Event Hub Connector Config">>,
                required => false
            }
        )}.

schema_module() ->
    emqx_bridge_azure_event_hub.

api_schema(Method) ->
    emqx_connector_schema:api_ref(
        emqx_bridge_azure_event_hub, <<"azure_event_hub_producer">>, Method ++ "_connector"
    ).
