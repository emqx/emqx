%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_timescale_action_info).

-behaviour(emqx_action_info).

-export([
    bridge_v1_type_name/0,
    action_type_name/0,
    connector_type_name/0,
    schema_module/0,
    connector_action_config_to_bridge_v1_config/2
]).

bridge_v1_type_name() -> timescale.

action_type_name() -> timescale.

connector_type_name() -> timescale.

schema_module() -> emqx_bridge_timescale.

connector_action_config_to_bridge_v1_config(ConnectorConfig, ActionConfig) ->
    emqx_bridge_pgsql_action_info:connector_action_config_to_bridge_v1_config(
        ConnectorConfig,
        ActionConfig
    ).
