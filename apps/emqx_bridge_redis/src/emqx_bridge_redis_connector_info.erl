%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_redis_connector_info).

-behaviour(emqx_connector_info).

-export([
    type_name/0,
    bridge_types/0,
    resource_callback_module/0,
    config_schema/0,
    schema_module/0,
    api_schema/1
]).

type_name() ->
    redis.

bridge_types() ->
    [redis, redis_single, redis_sentinel, redis_cluster].

resource_callback_module() ->
    emqx_bridge_redis_connector.

config_schema() ->
    {redis,
        hoconsc:mk(
            hoconsc:map(name, hoconsc:ref(emqx_bridge_redis_schema, "config_connector")),
            #{
                desc => <<"Redis Connector Config">>,
                required => false
            }
        )}.

schema_module() ->
    emqx_bridge_redis_schema.

api_schema(Method) ->
    emqx_connector_schema:api_ref(
        emqx_bridge_redis_schema, <<"redis">>, Method ++ "_connector"
    ).
