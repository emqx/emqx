%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_pgsql_action_info).

-behaviour(emqx_action_info).

-export([
    bridge_v1_type_name/0,
    action_type_name/0,
    connector_type_name/0,
    schema_module/0,
    connector_action_config_to_bridge_v1_config/2
]).

bridge_v1_type_name() -> pgsql.

action_type_name() -> pgsql.

connector_type_name() -> pgsql.

schema_module() -> emqx_bridge_pgsql.

connector_action_config_to_bridge_v1_config(ConnectorConfig, ActionConfig) ->
    Config0 = emqx_action_info:connector_action_config_to_bridge_v1_config(
        ConnectorConfig,
        ActionConfig
    ),
    maps:with(bridge_v1_fields(), Config0).

%%------------------------------------------------------------------------------------------
%% Internal helper functions
%%------------------------------------------------------------------------------------------

bridge_v1_fields() ->
    [
        emqx_utils_conv:bin(K)
     || {K, _V} <- emqx_bridge_pgsql:fields("config")
    ].
