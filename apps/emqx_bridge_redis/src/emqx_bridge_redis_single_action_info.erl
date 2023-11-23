%%--------------------------------------------------------------------
%% Copyright (c) 2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_redis_single_action_info).

-behaviour(emqx_action_info).

-export([
    bridge_v1_type_name/0,
    action_type_name/0,
    connector_type_name/0,
    schema_module/0,
    bridge_v1_config_to_action_config/2
]).

-import(hoconsc, [mk/2, enum/1, ref/1, ref/2]).

bridge_v1_type_name() -> redis_single.

action_type_name() -> redis_single_producer.

connector_type_name() -> redis_single_producer.

schema_module() -> emqx_bridge_redis_single_schema.

bridge_v1_config_to_action_config(BridgeV1Config, ConnectorName) ->
    CommonActionKeys = emqx_bridge_v2_schema:top_level_common_action_keys(),
    ParamsKeys = emqx_bridge_redis:producer_action_parameters_keys(),
    Config1 = maps:with(CommonActionKeys, BridgeV1Config),
    Params = maps:with(ParamsKeys, BridgeV1Config),
    Config1#{
        <<"connector">> => ConnectorName,
        <<"parameters">> => Params
    }.

%%------------------------------------------------------------------------------------------
%% Internal helper fns
%%------------------------------------------------------------------------------------------
