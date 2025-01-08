%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_pulsar_action_info).

-behaviour(emqx_action_info).

-export([
    bridge_v1_type_name/0,
    action_type_name/0,
    connector_type_name/0,
    schema_module/0,
    is_action/1,
    connector_action_config_to_bridge_v1_config/2
]).

is_action(_) -> true.

bridge_v1_type_name() -> pulsar_producer.

action_type_name() -> pulsar.

connector_type_name() -> pulsar.

schema_module() -> emqx_bridge_pulsar_pubsub_schema.

connector_action_config_to_bridge_v1_config(ConnectorConfig, ActionConfig) ->
    BridgeV1Config1 = emqx_action_info:connector_action_config_to_bridge_v1_config(
        ConnectorConfig, ActionConfig
    ),
    BridgeV1Config = maps:with(v1_fields(pulsar_producer), BridgeV1Config1),
    emqx_utils_maps:update_if_present(
        <<"resource_opts">>,
        fun(RO) -> maps:with(v1_fields(producer_resource_opts), RO) end,
        BridgeV1Config
    ).

%%------------------------------------------------------------------------------------------
%% Internal helper functions
%%------------------------------------------------------------------------------------------

v1_fields(Struct) ->
    [
        to_bin(K)
     || {K, _} <- emqx_bridge_pulsar:fields(Struct)
    ].

to_bin(B) when is_binary(B) -> B;
to_bin(L) when is_list(L) -> list_to_binary(L);
to_bin(A) when is_atom(A) -> atom_to_binary(A, utf8).
