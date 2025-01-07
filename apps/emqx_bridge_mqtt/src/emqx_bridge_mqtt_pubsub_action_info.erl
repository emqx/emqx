%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(emqx_bridge_mqtt_pubsub_action_info).

-behaviour(emqx_action_info).

-export([
    bridge_v1_type_name/0,
    action_type_name/0,
    connector_type_name/0,
    schema_module/0,
    bridge_v1_config_to_connector_config/1,
    bridge_v1_config_to_action_config/2,
    connector_action_config_to_bridge_v1_config/2,
    is_source/0
]).

bridge_v1_type_name() -> mqtt.

action_type_name() -> mqtt.

connector_type_name() -> mqtt.

schema_module() -> emqx_bridge_mqtt_pubsub_schema.

is_source() -> true.

bridge_v1_config_to_connector_config(Config) ->
    %% Transform the egress part to mqtt_publisher connector config
    SimplifiedConfig = check_and_simplify_bridge_v1_config(Config),
    ConnectorConfigMap = make_connector_config_from_bridge_v1_config(SimplifiedConfig),
    {mqtt, ConnectorConfigMap}.

make_connector_config_from_bridge_v1_config(Config) ->
    ConnectorConfigSchema = emqx_bridge_mqtt_connector_schema:fields("config_connector"),
    ConnectorTopFields = [
        erlang:atom_to_binary(FieldName, utf8)
     || {FieldName, _} <- ConnectorConfigSchema
    ],
    ConnectorConfigMap = maps:with(ConnectorTopFields, Config),
    ResourceOptsMap = maps:get(<<"resource_opts">>, ConnectorConfigMap, #{}),
    ResourceOptsMap2 = emqx_connector_schema:project_to_connector_resource_opts(ResourceOptsMap),
    ConnectorConfigMap2 = maps:put(<<"resource_opts">>, ResourceOptsMap2, ConnectorConfigMap),
    IngressMap0 = maps:get(<<"ingress">>, Config, #{}),
    EgressMap = maps:get(<<"egress">>, Config, #{}),
    %% Move pool_size to the top level
    PoolSizeIngress = maps:get(<<"pool_size">>, IngressMap0, undefined),
    PoolSize =
        case PoolSizeIngress of
            undefined ->
                DefaultPoolSize = emqx_connector_schema_lib:pool_size(default),
                maps:get(<<"pool_size">>, EgressMap, DefaultPoolSize);
            _ ->
                PoolSizeIngress
        end,
    %% Remove ingress part from the config
    ConnectorConfigMap3 = maps:remove(<<"ingress">>, ConnectorConfigMap2),
    %% Remove egress part from the config
    ConnectorConfigMap4 = maps:remove(<<"egress">>, ConnectorConfigMap3),
    ConnectorConfigMap5 = maps:put(<<"pool_size">>, PoolSize, ConnectorConfigMap4),
    ConnectorConfigMap5.

bridge_v1_config_to_action_config(BridgeV1Config, ConnectorName) ->
    SimplifiedConfig = check_and_simplify_bridge_v1_config(BridgeV1Config),
    bridge_v1_config_to_action_config_helper(
        SimplifiedConfig, ConnectorName
    ).

bridge_v1_config_to_action_config_helper(
    #{
        <<"egress">> := EgressMap0
    } = Config,
    ConnectorName
) ->
    %% Transform the egress part to mqtt_publisher connector config
    SchemaFields = emqx_bridge_mqtt_pubsub_schema:fields("mqtt_publisher_action"),
    ResourceOptsSchemaFields = emqx_bridge_mqtt_pubsub_schema:fields(action_resource_opts),
    ConfigMap1 = general_action_conf_map_from_bridge_v1_config(
        Config, ConnectorName, SchemaFields, ResourceOptsSchemaFields
    ),
    LocalTopicMap = maps:get(<<"local">>, EgressMap0, #{}),
    LocalTopic = maps:get(<<"topic">>, LocalTopicMap, undefined),
    EgressMap1 = maps:without([<<"local">>, <<"pool_size">>], EgressMap0),
    LocalParams = maps:get(<<"local">>, EgressMap0, #{}),
    EgressMap2 = emqx_utils_maps:unindent(<<"remote">>, EgressMap1),
    EgressMap = maps:put(<<"local">>, LocalParams, EgressMap2),
    %% Add parameters field (Egress map) to the action config
    ConfigMap2 = maps:put(<<"parameters">>, EgressMap, ConfigMap1),
    ConfigMap3 =
        case LocalTopic of
            undefined ->
                ConfigMap2;
            _ ->
                maps:put(<<"local_topic">>, LocalTopic, ConfigMap2)
        end,
    {action, mqtt, ConfigMap3};
bridge_v1_config_to_action_config_helper(
    #{
        <<"ingress">> := IngressMap0
    } = Config,
    ConnectorName
) ->
    %% Transform the egress part to mqtt_publisher connector config
    SchemaFields = emqx_bridge_mqtt_pubsub_schema:fields("mqtt_subscriber_source"),
    ResourceOptsSchemaFields = emqx_bridge_mqtt_pubsub_schema:fields(source_resource_opts),
    ConfigMap1 = general_action_conf_map_from_bridge_v1_config(
        Config, ConnectorName, SchemaFields, ResourceOptsSchemaFields
    ),
    IngressMap1 = maps:without([<<"pool_size">>, <<"local">>], IngressMap0),
    LocalParams = maps:get(<<"local">>, IngressMap0, #{}),
    IngressMap2 = emqx_utils_maps:unindent(<<"remote">>, IngressMap1),
    IngressMap = maps:put(<<"local">>, LocalParams, IngressMap2),
    %% Add parameters field (Egress map) to the action config
    ConfigMap2 = maps:put(<<"parameters">>, IngressMap, ConfigMap1),
    {source, mqtt, ConfigMap2};
bridge_v1_config_to_action_config_helper(
    _Config,
    _ConnectorName
) ->
    error({incompatible_bridge_v1, no_matching_action_or_source}).

general_action_conf_map_from_bridge_v1_config(
    Config, ConnectorName, SchemaFields, ResourceOptsSchemaFields
) ->
    ShemaFieldsNames = [
        erlang:atom_to_binary(FieldName, utf8)
     || {FieldName, _} <- SchemaFields
    ],
    ActionConfig0 = maps:with(ShemaFieldsNames, Config),
    ResourceOptsSchemaFieldsNames = [
        erlang:atom_to_binary(FieldName, utf8)
     || {FieldName, _} <- ResourceOptsSchemaFields
    ],
    ResourceOptsMap = maps:get(<<"resource_opts">>, ActionConfig0, #{}),
    ResourceOptsMap2 = maps:with(ResourceOptsSchemaFieldsNames, ResourceOptsMap),
    %% Only put resource_opts if the original config has it
    ActionConfig1 =
        case maps:is_key(<<"resource_opts">>, ActionConfig0) of
            true ->
                maps:put(<<"resource_opts">>, ResourceOptsMap2, ActionConfig0);
            false ->
                ActionConfig0
        end,
    ActionConfig2 = maps:put(<<"connector">>, ConnectorName, ActionConfig1),
    ActionConfig2.

check_and_simplify_bridge_v1_config(
    #{
        <<"egress">> := EgressMap
    } = Config
) when map_size(EgressMap) =:= 0 ->
    check_and_simplify_bridge_v1_config(maps:remove(<<"egress">>, Config));
check_and_simplify_bridge_v1_config(
    #{
        <<"ingress">> := IngressMap
    } = Config
) when map_size(IngressMap) =:= 0 ->
    check_and_simplify_bridge_v1_config(maps:remove(<<"ingress">>, Config));
check_and_simplify_bridge_v1_config(#{
    <<"egress">> := _EGressMap,
    <<"ingress">> := _InGressMap
}) ->
    %% We should crash beacuse we don't support upgrading when ingress and egress exist at the same time
    error(
        {unsupported_config, <<
            "Upgrade not supported when ingress and egress exist in the "
            "same MQTT bridge. Please divide the egress and ingress part "
            "to separate bridges in the configuration."
        >>}
    );
check_and_simplify_bridge_v1_config(SimplifiedConfig) ->
    SimplifiedConfig.

connector_action_config_to_bridge_v1_config(
    ConnectorConfig, ActionConfig
) ->
    Params0 = maps:get(<<"parameters">>, ActionConfig, #{}),
    ResourceOptsConnector = maps:get(<<"resource_opts">>, ConnectorConfig, #{}),
    ResourceOptsAction = maps:get(<<"resource_opts">>, ActionConfig, #{}),
    ResourceOpts0 = maps:merge(ResourceOptsConnector, ResourceOptsAction),
    V1ResourceOptsFields =
        lists:map(
            fun({Field, _}) -> atom_to_binary(Field) end,
            emqx_bridge_mqtt_schema:fields("creation_opts")
        ),
    ResourceOpts = maps:with(V1ResourceOptsFields, ResourceOpts0),
    %% Check the direction of the action
    Direction =
        case is_map_key(<<"retain">>, Params0) of
            %% Only source has retain
            true ->
                <<"publisher">>;
            false ->
                <<"subscriber">>
        end,
    Params1 = maps:remove(<<"direction">>, Params0),
    Params = maps:remove(<<"local">>, Params1),
    %% hidden; for backwards compatibility
    LocalParams = maps:get(<<"local">>, Params1, #{}),
    DefaultPoolSize = emqx_connector_schema_lib:pool_size(default),
    PoolSize = maps:get(<<"pool_size">>, ConnectorConfig, DefaultPoolSize),
    ConnectorConfig2 = maps:remove(<<"pool_size">>, ConnectorConfig),
    LocalTopic = maps:get(<<"local_topic">>, ActionConfig, undefined),
    BridgeV1Conf0 =
        case {Direction, LocalTopic} of
            {<<"publisher">>, undefined} ->
                #{
                    <<"egress">> =>
                        #{
                            <<"pool_size">> => PoolSize,
                            <<"remote">> => Params,
                            <<"local">> => LocalParams
                        }
                };
            {<<"publisher">>, LocalT} ->
                #{
                    <<"egress">> =>
                        #{
                            <<"pool_size">> => PoolSize,
                            <<"remote">> => Params,
                            <<"local">> =>
                                maps:merge(
                                    LocalParams,
                                    #{<<"topic">> => LocalT}
                                )
                        }
                };
            {<<"subscriber">>, _} ->
                #{
                    <<"ingress">> =>
                        #{
                            <<"pool_size">> => PoolSize,
                            <<"remote">> => project_to(Params, "ingress_remote"),
                            <<"local">> => LocalParams
                        }
                }
        end,
    BridgeV1Conf1 = maps:merge(BridgeV1Conf0, ConnectorConfig2),
    BridgeV1Conf2 = BridgeV1Conf1#{
        <<"resource_opts">> => ResourceOpts
    },
    BridgeV1Conf2.

project_to(Params, ConnectorSchemaRef) ->
    Fields0 = proplists:get_keys(emqx_bridge_mqtt_connector_schema:fields(ConnectorSchemaRef)),
    Fields = lists:map(fun emqx_utils_conv:bin/1, Fields0),
    maps:with(Fields, Params).
