%%--------------------------------------------------------------------
%% Copyright (c) 2022-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_azure_event_hub_action_info).

-behaviour(emqx_action_info).

-export([
    bridge_v1_type_name/0,
    action_type_name/0,
    connector_type_name/0,
    schema_module/0,
    action_to_bridge_v1_fixup/1,
    bridge_v1_to_action_fixup/1
]).

bridge_v1_type_name() -> azure_event_hub_producer.

action_type_name() -> azure_event_hub_producer.

connector_type_name() -> azure_event_hub_producer.

schema_module() -> emqx_bridge_azure_event_hub.

action_to_bridge_v1_fixup(Config) ->
    emqx_bridge_kafka_action_info:action_to_bridge_v1_fixup(Config).

bridge_v1_to_action_fixup(Config) ->
    emqx_bridge_kafka_action_info:bridge_v1_to_action_fixup(Config).
