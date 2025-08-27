%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_redis_action_info).

-behaviour(emqx_action_info).

-export([
    action_type_name/0,
    connector_type_name/0,
    schema_module/0,
    action_convert_from_connector/2
]).

-define(SCHEMA_MODULE, emqx_bridge_redis_schema).

action_type_name() -> redis.

connector_type_name() -> redis.

schema_module() -> ?SCHEMA_MODULE.

action_convert_from_connector(ConnectorConfig, ActionConfig) ->
    case ConnectorConfig of
        #{<<"parameters">> := #{<<"redis_type">> := <<"redis_cluster">>}} ->
            emqx_utils_maps:update_if_present(
                <<"resource_opts">>,
                fun(Opts) ->
                    Opts#{
                        <<"batch_size">> => 1,
                        <<"batch_time">> => <<"0ms">>
                    }
                end,
                ActionConfig
            );
        _ ->
            ActionConfig
    end.
