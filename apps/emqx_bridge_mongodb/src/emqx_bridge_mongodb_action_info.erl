%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_mongodb_action_info).

-behaviour(emqx_action_info).

%% behaviour callbacks
-export([
    bridge_v1_config_to_action_config/2,
    bridge_v1_config_to_connector_config/1,
    connector_action_config_to_bridge_v1_config/2,
    action_type_name/0,
    bridge_v1_type_name/0,
    connector_type_name/0,
    schema_module/0
]).

%% dynamic callback
-export([
    bridge_v1_type_name_fun/1
]).

-import(emqx_utils_conv, [bin/1]).

-define(SCHEMA_MODULE, emqx_bridge_mongodb).

bridge_v1_config_to_action_config(BridgeV1Config, ConnectorName) ->
    ActionTopLevelKeys = schema_keys(mongodb_action),
    ActionParametersKeys = schema_keys(action_parameters),
    ActionKeys = ActionTopLevelKeys ++ ActionParametersKeys,
    ActionConfig = make_config_map(ActionKeys, ActionParametersKeys, BridgeV1Config),
    emqx_utils_maps:update_if_present(
        <<"resource_opts">>,
        fun emqx_bridge_v2_schema:project_to_actions_resource_opts/1,
        ActionConfig#{<<"connector">> => ConnectorName}
    ).

bridge_v1_config_to_connector_config(BridgeV1Config) ->
    ActionTopLevelKeys = schema_keys(mongodb_action),
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

connector_action_config_to_bridge_v1_config(ConnectorConfig, ActionConfig) ->
    V1Config = emqx_action_info:connector_action_config_to_bridge_v1_config(
        ConnectorConfig,
        ActionConfig
    ),
    maps:remove(<<"local_topic">>, V1Config).

make_config_map(PickKeys, IndentKeys, Config) ->
    Conf0 = maps:with(PickKeys, Config),
    emqx_utils_maps:indent(<<"parameters">>, IndentKeys, Conf0).

bridge_v1_type_name() ->
    {fun ?MODULE:bridge_v1_type_name_fun/1, bridge_v1_type_names()}.

action_type_name() -> mongodb.

connector_type_name() -> mongodb.

schema_module() -> ?SCHEMA_MODULE.

bridge_v1_type_names() -> [mongodb_rs, mongodb_sharded, mongodb_single].

bridge_v1_type_name_fun({#{<<"parameters">> := #{<<"mongo_type">> := MongoType}}, _}) ->
    v1_type(MongoType).

v1_type(<<"rs">>) -> mongodb_rs;
v1_type(<<"sharded">>) -> mongodb_sharded;
v1_type(<<"single">>) -> mongodb_single.

schema_keys(Name) ->
    [bin(Key) || Key <- proplists:get_keys(?SCHEMA_MODULE:fields(Name))].
