%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_gcp_pubsub_producer_action_info).

-behaviour(emqx_action_info).

-export([
    bridge_v1_type_name/0,
    action_type_name/0,
    connector_type_name/0,
    schema_module/0,
    bridge_v1_config_to_action_config/2
]).

bridge_v1_type_name() -> gcp_pubsub.

action_type_name() -> gcp_pubsub_producer.

connector_type_name() -> gcp_pubsub_producer.

schema_module() -> emqx_bridge_gcp_pubsub_producer_schema.

bridge_v1_config_to_action_config(BridgeV1Config, ConnectorName) ->
    CommonActionKeys = emqx_bridge_v2_schema:top_level_common_action_keys(),
    ParamsKeys = producer_action_parameters_field_keys(),
    Config1 = maps:with(CommonActionKeys, BridgeV1Config),
    Params = maps:with(ParamsKeys, BridgeV1Config),
    emqx_utils_maps:update_if_present(
        <<"resource_opts">>,
        fun emqx_bridge_v2_schema:project_to_actions_resource_opts/1,
        Config1#{
            <<"connector">> => ConnectorName,
            <<"parameters">> => Params
        }
    ).

%%------------------------------------------------------------------------------------------
%% Internal helper fns
%%------------------------------------------------------------------------------------------

producer_action_parameters_field_keys() ->
    [
        to_bin(K)
     || {K, _} <- emqx_bridge_gcp_pubsub_producer_schema:fields(action_parameters)
    ].

to_bin(L) when is_list(L) -> list_to_binary(L);
to_bin(A) when is_atom(A) -> atom_to_binary(A, utf8).
