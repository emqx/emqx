%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_mysql_action_info).

-behaviour(emqx_action_info).

%% behaviour callbacks
-export([
    action_type_name/0,
    bridge_v1_config_to_action_config/2,
    bridge_v1_config_to_connector_config/1,
    bridge_v1_type_name/0,
    connector_action_config_to_bridge_v1_config/2,
    connector_type_name/0,
    schema_module/0
]).

-import(emqx_utils_conv, [bin/1]).

-define(MYSQL_TYPE, mysql).
-define(SCHEMA_MODULE, emqx_bridge_mysql).

action_type_name() -> ?MYSQL_TYPE.
bridge_v1_type_name() -> ?MYSQL_TYPE.
connector_type_name() -> ?MYSQL_TYPE.

schema_module() -> ?SCHEMA_MODULE.

connector_action_config_to_bridge_v1_config(ConnectorConfig, ActionConfig) ->
    MergedConfig =
        emqx_utils_maps:deep_merge(
            maps:without(
                [<<"connector">>],
                emqx_utils_maps:unindent(<<"parameters">>, ActionConfig)
            ),
            ConnectorConfig
        ),
    BridgeV1Keys = schema_keys("config"),
    maps:with(BridgeV1Keys, MergedConfig).

bridge_v1_config_to_action_config(BridgeV1Config, ConnectorName) ->
    ActionTopLevelKeys = schema_keys(mysql_action),
    ActionParametersKeys = schema_keys(action_parameters),
    ActionKeys = ActionTopLevelKeys ++ ActionParametersKeys,
    ActionConfig = make_config_map(ActionKeys, ActionParametersKeys, BridgeV1Config),
    emqx_utils_maps:update_if_present(
        <<"resource_opts">>,
        fun emqx_bridge_v2_schema:project_to_actions_resource_opts/1,
        ActionConfig#{<<"connector">> => ConnectorName}
    ).

bridge_v1_config_to_connector_config(BridgeV1Config) ->
    ConnectorKeys = schema_keys("config_connector"),
    emqx_utils_maps:update_if_present(
        <<"resource_opts">>,
        fun emqx_connector_schema:project_to_connector_resource_opts/1,
        maps:with(ConnectorKeys, BridgeV1Config)
    ).

make_config_map(PickKeys, IndentKeys, Config) ->
    Conf0 = maps:with(PickKeys, Config),
    emqx_utils_maps:indent(<<"parameters">>, IndentKeys, Conf0).

schema_keys(Name) ->
    [bin(Key) || Key <- proplists:get_keys(?SCHEMA_MODULE:fields(Name))].
