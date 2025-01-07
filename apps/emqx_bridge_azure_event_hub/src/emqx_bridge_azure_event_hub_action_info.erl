%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_azure_event_hub_action_info).

-behaviour(emqx_action_info).

-export([
    bridge_v1_type_name/0,
    action_type_name/0,
    connector_type_name/0,
    schema_module/0,
    connector_action_config_to_bridge_v1_config/2,
    bridge_v1_config_to_action_config/2
]).

bridge_v1_type_name() -> azure_event_hub_producer.

action_type_name() -> azure_event_hub_producer.

connector_type_name() -> azure_event_hub_producer.

schema_module() -> emqx_bridge_azure_event_hub.

connector_action_config_to_bridge_v1_config(ConnectorConfig, ActionConfig) ->
    emqx_bridge_kafka_producer_action_info:connector_action_config_to_bridge_v1_config(
        ConnectorConfig, ActionConfig
    ).

bridge_v1_config_to_action_config(BridgeV1Conf, ConnectorName) ->
    emqx_bridge_kafka_producer_action_info:bridge_v1_config_to_action_config(
        BridgeV1Conf, ConnectorName
    ).
