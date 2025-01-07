%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_rocketmq_connector_info).

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
    rocketmq.

bridge_types() ->
    [rocketmq].

resource_callback_module() ->
    emqx_bridge_rocketmq_connector.

config_schema() ->
    {rocketmq,
        hoconsc:mk(
            hoconsc:map(name, hoconsc:ref(emqx_bridge_rocketmq, "config_connector")),
            #{
                desc => <<"RocketMQ Connector Config">>,
                required => false
            }
        )}.

schema_module() ->
    emqx_bridge_rocketmq.

api_schema(Method) ->
    emqx_connector_schema:api_ref(
        emqx_bridge_rocketmq, <<"rocketmq">>, Method ++ "_connector"
    ).
