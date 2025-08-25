%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_azure_event_hub_action_info).

-behaviour(emqx_action_info).

-export([
    action_type_name/0,
    connector_type_name/0,
    schema_module/0
]).

action_type_name() -> azure_event_hub_producer.

connector_type_name() -> azure_event_hub_producer.

schema_module() -> emqx_bridge_azure_event_hub.
