%%--------------------------------------------------------------------
%% Copyright (c) 2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_gcp_pubsub_consumer_action_info).

-behaviour(emqx_action_info).

-export([
    is_source/0,
    is_action/0,
    bridge_v1_type_name/0,
    action_type_name/0,
    connector_type_name/0,
    schema_module/0,
    bridge_v1_config_to_action_config/2,
    connector_action_config_to_bridge_v1_config/2
]).

is_source() -> true.

is_action() -> false.

bridge_v1_type_name() -> gcp_pubsub_consumer.

action_type_name() -> gcp_pubsub_consumer.

connector_type_name() -> gcp_pubsub_consumer.

schema_module() -> emqx_bridge_gcp_pubsub_consumer_schema.

bridge_v1_config_to_action_config(BridgeV1Config, ConnectorName) ->
    CommonSourceKeys = emqx_bridge_v2_schema:top_level_common_source_keys(),
    ParamsKeys = source_action_parameters_field_keys(),
    Config1 = maps:with(CommonSourceKeys, BridgeV1Config),
    ConsumerCfg = maps:get(<<"consumer">>, BridgeV1Config, #{}),
    Params = maps:with(ParamsKeys, ConsumerCfg),
    {source, gcp_pubsub_consumer,
        emqx_utils_maps:update_if_present(
            <<"resource_opts">>,
            fun(Cfg) ->
                emqx_bridge_v2_schema:project_to_sources_resource_opts(
                    Cfg,
                    resource_opts_fields()
                )
            end,
            Config1#{
                <<"connector">> => ConnectorName,
                <<"parameters">> => Params
            }
        )}.

connector_action_config_to_bridge_v1_config(ConnectorConfig, SourceConfig) ->
    BridgeV1Config1 = maps:remove(<<"connector">>, SourceConfig),
    BridgeV1Config2 = emqx_utils_maps:deep_merge(ConnectorConfig, BridgeV1Config1),
    BridgeV1Config3 =
        emqx_utils_maps:update_if_present(
            <<"resource_opts">>,
            fun(RO) -> maps:with(bridge_v1_resource_opts_fields(), RO) end,
            BridgeV1Config2
        ),
    emqx_utils_maps:rename(<<"parameters">>, <<"consumer">>, BridgeV1Config3).

%%------------------------------------------------------------------------------------------
%% Internal helper fns
%%------------------------------------------------------------------------------------------

resource_opts_fields() ->
    [
        to_bin(K)
     || {K, _} <- emqx_bridge_gcp_pubsub_consumer_schema:fields(source_resource_opts)
    ].

bridge_v1_resource_opts_fields() ->
    [
        to_bin(K)
     || {K, _} <- emqx_bridge_gcp_pubsub:fields("consumer_resource_opts")
    ].

source_action_parameters_field_keys() ->
    [
        to_bin(K)
     || {K, _} <- emqx_bridge_gcp_pubsub_consumer_schema:fields(source_parameters)
    ].

to_bin(L) when is_list(L) -> list_to_binary(L);
to_bin(A) when is_atom(A) -> atom_to_binary(A, utf8).
