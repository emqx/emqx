%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_hstreamdb_action_info).

-behaviour(emqx_action_info).

-export([
    bridge_v1_type_name/0,
    action_type_name/0,
    connector_type_name/0,
    schema_module/0,
    bridge_v1_config_to_connector_config/1,
    bridge_v1_config_to_action_config/2,
    connector_action_config_to_bridge_v1_config/2
]).

bridge_v1_type_name() -> hstreamdb.

action_type_name() -> hstreamdb.

connector_type_name() -> hstreamdb.

schema_module() -> emqx_bridge_hstreamdb.

bridge_v1_config_to_connector_config(BridgeV1Conf) ->
    ConnectorSchema = emqx_bridge_hstreamdb:fields(connector_fields),
    ConnectorAtomKeys = lists:foldl(fun({K, _}, Acc) -> [K | Acc] end, [], ConnectorSchema),
    ConnectorBinKeys = [atom_to_binary(K) || K <- ConnectorAtomKeys] ++ [<<"resource_opts">>],
    ConnectorConfig0 = maps:with(ConnectorBinKeys, BridgeV1Conf),
    emqx_utils_maps:update_if_present(
        <<"resource_opts">>,
        fun emqx_connector_schema:project_to_connector_resource_opts/1,
        ConnectorConfig0
    ).

bridge_v1_config_to_action_config(BridgeV1Conf, ConnectorName) ->
    Config0 = emqx_action_info:transform_bridge_v1_config_to_action_config(
        BridgeV1Conf, ConnectorName, emqx_bridge_hstreamdb, "config_connector"
    ),
    %% Remove fields no longer relevant for the action
    Config1 = lists:foldl(
        fun(Field, Acc) ->
            emqx_utils_maps:deep_remove(Field, Acc)
        end,
        Config0,
        [
            [<<"parameters">>, <<"pool_size">>],
            [<<"parameters">>, <<"direction">>]
        ]
    ),
    %% Move pool_size to aggregation_pool_size and writer_pool_size
    PoolSize = maps:get(<<"pool_size">>, BridgeV1Conf, 8),
    Config2 = emqx_utils_maps:deep_put(
        [<<"parameters">>, <<"aggregation_pool_size">>],
        Config1,
        PoolSize
    ),
    Config3 = emqx_utils_maps:deep_put(
        [<<"parameters">>, <<"writer_pool_size">>],
        Config2,
        PoolSize
    ),
    Config3.

connector_action_config_to_bridge_v1_config(ConnectorConfig, ActionConfig) ->
    BridgeV1Config1 = maps:without([<<"connector">>, <<"last_modified_at">>], ActionConfig),
    BridgeV1Config2 = emqx_utils_maps:deep_merge(ConnectorConfig, BridgeV1Config1),
    BridgeV1Config3 = maps:remove(<<"parameters">>, BridgeV1Config2),
    %% Pick out pool_size from aggregation_pool_size
    PoolSize = emqx_utils_maps:deep_get(
        [<<"parameters">>, <<"aggregation_pool_size">>], ActionConfig, 8
    ),
    BridgeV1Config4 = maps:put(<<"pool_size">>, PoolSize, BridgeV1Config3),

    %% Move the fields stream, partition_key and record_template from
    %% parameters in ActionConfig to the top level in BridgeV1Config
    lists:foldl(
        fun(Field, Acc) ->
            emqx_utils_maps:deep_put(
                [Field],
                Acc,
                emqx_utils_maps:deep_get([<<"parameters">>, Field], ActionConfig, <<>>)
            )
        end,
        BridgeV1Config4,
        [<<"stream">>, <<"partition_key">>, <<"record_template">>]
    ).
