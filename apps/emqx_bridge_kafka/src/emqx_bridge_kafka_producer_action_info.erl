%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_kafka_producer_action_info).

-behaviour(emqx_action_info).

-export([
    bridge_v1_type_name/0,
    action_type_name/0,
    connector_type_name/0,
    schema_module/0,
    connector_action_config_to_bridge_v1_config/2,
    bridge_v1_config_to_action_config/2
]).

bridge_v1_type_name() -> kafka.

action_type_name() -> kafka_producer.

connector_type_name() -> kafka_producer.

schema_module() -> emqx_bridge_kafka.

connector_action_config_to_bridge_v1_config(ConnectorConfig, ActionConfig) ->
    BridgeV1Config1 = maps:without([<<"connector">>, <<"last_modified_at">>], ActionConfig),
    BridgeV1Config2 = emqx_utils_maps:deep_merge(ConnectorConfig, BridgeV1Config1),
    BridgeV1Config = emqx_utils_maps:rename(<<"parameters">>, <<"kafka">>, BridgeV1Config2),
    maps:update_with(
        <<"kafka">>,
        fun(Params) -> maps:with(v1_parameters(), Params) end,
        BridgeV1Config
    ).

bridge_v1_config_to_action_config(BridgeV1Conf0 = #{<<"producer">> := _}, ConnectorName) ->
    %% Ancient v1 config, when `kafka' key was wrapped by `producer'
    BridgeV1Conf1 = emqx_utils_maps:unindent(<<"producer">>, BridgeV1Conf0),
    BridgeV1Conf =
        case maps:take(<<"mqtt">>, BridgeV1Conf1) of
            {#{<<"topic">> := Topic}, BridgeV1Conf2} when is_binary(Topic) ->
                BridgeV1Conf2#{<<"local_topic">> => Topic};
            _ ->
                maps:remove(<<"mqtt">>, BridgeV1Conf1)
        end,
    bridge_v1_config_to_action_config(BridgeV1Conf, ConnectorName);
bridge_v1_config_to_action_config(BridgeV1Conf, ConnectorName) ->
    Config0 = emqx_action_info:transform_bridge_v1_config_to_action_config(
        BridgeV1Conf, ConnectorName, schema_module(), kafka_producer
    ),
    KafkaMap = maps:get(<<"kafka">>, BridgeV1Conf, #{}),
    Config2 = emqx_utils_maps:deep_merge(Config0, #{<<"parameters">> => KafkaMap}),
    maps:with(producer_action_field_keys(), Config2).

%%------------------------------------------------------------------------------------------
%% Internal helper functions
%%------------------------------------------------------------------------------------------

v1_parameters() ->
    [
        to_bin(K)
     || {K, _} <- emqx_bridge_kafka:fields(v1_producer_kafka_opts)
    ].

producer_action_field_keys() ->
    [
        to_bin(K)
     || {K, _} <- emqx_bridge_kafka:fields(kafka_producer_action)
    ].

to_bin(B) when is_binary(B) -> B;
to_bin(L) when is_list(L) -> list_to_binary(L);
to_bin(A) when is_atom(A) -> atom_to_binary(A, utf8).
