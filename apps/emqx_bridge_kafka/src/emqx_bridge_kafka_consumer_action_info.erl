%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_kafka_consumer_action_info).

-behaviour(emqx_action_info).

-export([
    is_source/0,
    is_action/0,
    bridge_v1_type_name/0,
    action_type_name/0,
    connector_type_name/0,
    schema_module/0,
    connector_action_config_to_bridge_v1_config/2,
    bridge_v1_config_to_action_config/2
]).

is_source() -> true.

is_action() -> false.

bridge_v1_type_name() -> kafka_consumer.

action_type_name() -> kafka_consumer.

connector_type_name() -> kafka_consumer.

schema_module() -> emqx_bridge_kafka_consumer_schema.

connector_action_config_to_bridge_v1_config(ConnectorConfig, ActionConfig) ->
    V1Config1 = maps:without([<<"connector">>, <<"last_modified_at">>], ActionConfig),
    V1Config2 = emqx_utils_maps:deep_merge(ConnectorConfig, V1Config1),
    V1Config3 = maybe_fabricate_topic_mapping(V1Config2),
    {Params1, V1Config4} = maps:take(<<"parameters">>, V1Config3),
    TopLevelCfgKeys = [to_bin(K) || {K, _} <- emqx_bridge_kafka:fields(consumer_opts), K =/= kafka],
    TopLevelCfg = maps:with(TopLevelCfgKeys, Params1),
    Params = maps:with(v1_source_parameters(), Params1),
    V1Config5 = emqx_utils_maps:deep_merge(V1Config4, TopLevelCfg),
    V1Config = emqx_utils_maps:update_if_present(
        <<"resource_opts">>,
        %% Slightly different from default source resource opts...
        fun(RO) -> maps:with(v1_fields(connector_resource_opts), RO) end,
        V1Config5
    ),
    maps:put(<<"kafka">>, Params, V1Config).

bridge_v1_config_to_action_config(BridgeV1Conf, ConnectorName) ->
    Config0 = emqx_action_info:transform_bridge_v1_config_to_action_config(
        BridgeV1Conf, ConnectorName, schema_module(), source_parameters
    ),
    TopicMapping = maps:get(<<"topic_mapping">>, BridgeV1Conf, []),
    Params0 = maps:get(<<"kafka">>, BridgeV1Conf, #{}),
    Params1 = maps:with(source_parameters_field_keys(), Params0),
    Params2 = emqx_utils_maps:put_if(
        Params1, <<"topic_mapping">>, TopicMapping, TopicMapping =/= []
    ),
    Params = maybe_set_kafka_topic(Params2),
    {source, action_type_name(), maps:put(<<"parameters">>, Params, Config0)}.

%%------------------------------------------------------------------------------------------
%% Internal helper functions
%%------------------------------------------------------------------------------------------

v1_source_parameters() ->
    [
        <<"max_batch_bytes">>,
        <<"max_rejoin_attempts">>,
        <<"offset_commit_interval_seconds">>,
        <<"offset_reset_policy">>
    ].

%% The new schema has a single kafka topic, so we take it from topic mapping when
%% converting from v1.
maybe_set_kafka_topic(#{<<"topic_mapping">> := [#{<<"kafka_topic">> := Topic} | _]} = Params) ->
    Params#{<<"topic">> => Topic};
maybe_set_kafka_topic(Params) ->
    Params.

%% The old schema requires `topic_mapping', which is now hidden.
maybe_fabricate_topic_mapping(#{<<"parameters">> := Params0} = BridgeV1Config0) ->
    #{<<"topic">> := Topic} = Params0,
    case maps:get(<<"topic_mapping">>, Params0, undefined) of
        [_ | _] ->
            BridgeV1Config0;
        _ ->
            %% Have to fabricate an MQTT topic, unfortunately...  QoS and payload already
            %% have defaults.
            FakeTopicMapping = #{
                <<"kafka_topic">> => Topic,
                <<"mqtt_topic">> => <<>>
            },
            Params = Params0#{<<"topic_mapping">> => [FakeTopicMapping]},
            BridgeV1Config0#{<<"parameters">> := Params}
    end.

v1_fields(StructName) ->
    [
        to_bin(K)
     || {K, _} <- emqx_bridge_kafka:fields(StructName)
    ].

source_parameters_field_keys() ->
    [
        to_bin(K)
     || {K, _} <- emqx_bridge_kafka_consumer_schema:fields(source_parameters)
    ].

to_bin(B) when is_binary(B) -> B;
to_bin(L) when is_list(L) -> list_to_binary(L);
to_bin(A) when is_atom(A) -> atom_to_binary(A, utf8).
