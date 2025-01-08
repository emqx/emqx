%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_influxdb_connector_info).

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
    influxdb.

bridge_types() ->
    [influxdb, influxdb_api_v1, influxdb_api_v2].

resource_callback_module() ->
    emqx_bridge_influxdb_connector.

config_schema() ->
    {influxdb,
        hoconsc:mk(
            hoconsc:map(name, hoconsc:ref(emqx_bridge_influxdb, "config_connector")),
            #{
                desc => <<"InfluxDB Connector Config">>,
                required => false
            }
        )}.

schema_module() ->
    emqx_bridge_influxdb.

api_schema(Method) ->
    emqx_connector_schema:api_ref(
        emqx_bridge_influxdb, <<"influxdb">>, Method ++ "_connector"
    ).
