%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_kinesis_connector_info).

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
    kinesis.

bridge_types() ->
    [kinesis, kinesis_producer].

resource_callback_module() ->
    emqx_bridge_kinesis_impl_producer.

config_schema() ->
    {kinesis,
        hoconsc:mk(
            hoconsc:map(name, hoconsc:ref(emqx_bridge_kinesis, "config_connector")),
            #{
                desc => <<"Kinesis Connector Config">>,
                required => false
            }
        )}.

schema_module() ->
    emqx_bridge_kinesis.

api_schema(Method) ->
    emqx_connector_schema:api_ref(
        emqx_bridge_kinesis, <<"kinesis">>, Method ++ "_connector"
    ).
