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
    emqx_utils_maps:rename(<<"parameters">>, <<"kafka">>, Config).

bridge_v1_to_action_fixup(Config0) ->
    Config = emqx_utils_maps:rename(<<"kafka">>, <<"parameters">>, Config0),
    maps:with(producer_action_field_keys(), Config).

%%------------------------------------------------------------------------------------------
%% Internal helper fns
%%------------------------------------------------------------------------------------------

producer_action_field_keys() ->
    [
        to_bin(K)
     || {K, _} <- emqx_bridge_kafka:fields(kafka_producer_action)
    ].

to_bin(B) when is_binary(B) -> B;
to_bin(L) when is_list(L) -> list_to_binary(L);
to_bin(A) when is_atom(A) -> atom_to_binary(A, utf8).
