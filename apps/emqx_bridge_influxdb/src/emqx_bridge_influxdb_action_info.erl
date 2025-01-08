%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_influxdb_action_info).

-behaviour(emqx_action_info).

-export([
    bridge_v1_config_to_action_config/2,
    bridge_v1_config_to_connector_config/1,
    connector_action_config_to_bridge_v1_config/2,
    bridge_v1_type_name/0,
    action_type_name/0,
    connector_type_name/0,
    schema_module/0
]).

%% dynamic callback
-export([
    bridge_v1_type_name_fun/1
]).

-import(emqx_utils_conv, [bin/1]).

-define(SCHEMA_MODULE, emqx_bridge_influxdb).

bridge_v1_config_to_action_config(BridgeV1Config, ConnectorName) ->
    ActionTopLevelKeys = schema_keys(influxdb_action),
    ActionParametersKeys = schema_keys(action_parameters),
    ActionKeys = ActionTopLevelKeys ++ ActionParametersKeys,
    ActionConfig = make_config_map(ActionKeys, ActionParametersKeys, BridgeV1Config),
    emqx_utils_maps:update_if_present(
        <<"resource_opts">>,
        fun emqx_bridge_v2_schema:project_to_actions_resource_opts/1,
        ActionConfig#{<<"connector">> => ConnectorName}
    ).

bridge_v1_config_to_connector_config(BridgeV1Config) ->
    ActionTopLevelKeys = schema_keys(influxdb_action),
    ActionParametersKeys = schema_keys(action_parameters),
    ActionKeys = ActionTopLevelKeys ++ ActionParametersKeys,
    ConnectorTopLevelKeys = schema_keys("config_connector"),
    ConnectorKeys = maps:keys(BridgeV1Config) -- (ActionKeys -- ConnectorTopLevelKeys),
    ConnectorParametersKeys = ConnectorKeys -- ConnectorTopLevelKeys,
    ConnConfig0 = make_config_map(ConnectorKeys, ConnectorParametersKeys, BridgeV1Config),
    emqx_utils_maps:update_if_present(
        <<"resource_opts">>,
        fun emqx_connector_schema:project_to_connector_resource_opts/1,
        ConnConfig0
    ).

connector_action_config_to_bridge_v1_config(ConnectorRawConf, ActionRawConf) ->
    RawConf = emqx_action_info:connector_action_config_to_bridge_v1_config(
        ConnectorRawConf, ActionRawConf
    ),
    maps:without([<<"influxdb_type">>], RawConf).

bridge_v1_type_name() ->
    {fun ?MODULE:bridge_v1_type_name_fun/1, bridge_v1_type_names()}.

action_type_name() -> influxdb.

connector_type_name() -> influxdb.

schema_module() -> ?SCHEMA_MODULE.

bridge_v1_type_name_fun({#{<<"parameters">> := #{<<"database">> := _}}, _}) ->
    influxdb_api_v1;
bridge_v1_type_name_fun({#{<<"parameters">> := #{<<"bucket">> := _}}, _}) ->
    influxdb_api_v2.

make_config_map(PickKeys, IndentKeys, Config) ->
    Conf0 = maps:with(PickKeys, Config),
    emqx_utils_maps:indent(<<"parameters">>, IndentKeys, Conf0).

schema_keys(Name) ->
    [bin(Key) || Key <- proplists:get_keys(?SCHEMA_MODULE:fields(Name))].

bridge_v1_type_names() -> [influxdb, influxdb_api_v1, influxdb_api_v2].
