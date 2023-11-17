%%--------------------------------------------------------------------
%% Copyright (c) 2022-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_kafka_action_info).

-behaviour(emqx_action_info).

-export([
    bridge_v1_type_name/0,
    action_type_name/0,
    connector_type_name/0,
    schema_module/0,
    action_to_bridge_v1_fixup/1,
    bridge_v1_to_action_fixup/1
]).

bridge_v1_type_name() -> kafka.

action_type_name() -> kafka_producer.

connector_type_name() -> kafka_producer.

schema_module() -> emqx_bridge_kafka.

action_to_bridge_v1_fixup(Config) ->
    rename(<<"parameters">>, <<"kafka">>, Config).

rename(OldKey, NewKey, Map) ->
    case maps:find(OldKey, Map) of
        {ok, Value} ->
            maps:remove(OldKey, maps:put(NewKey, Value, Map));
        error ->
            Map
    end.

bridge_v1_to_action_fixup(Config) ->
    KafkaField = emqx_utils_maps:deep_get([<<"parameters">>, <<"kafka">>], Config, #{}),
    Config1 = emqx_utils_maps:deep_remove([<<"parameters">>, <<"kafka">>], Config),
    emqx_utils_maps:deep_merge(Config1, #{<<"parameters">> => KafkaField}).
