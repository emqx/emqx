%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_tablestore_connector_info).

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
    tablestore.

bridge_types() ->
    [tablestore, tablestore_timeseries].

resource_callback_module() ->
    emqx_bridge_tablestore_connector.

config_schema() ->
    {tablestore,
        hoconsc:mk(
            hoconsc:map(name, hoconsc:ref(emqx_bridge_tablestore, "config_connector")),
            #{
                desc => <<"Tablestore Connector Config">>,
                required => false
            }
        )}.

schema_module() ->
    emqx_bridge_tablestore.

api_schema(Method) ->
    emqx_connector_schema:api_ref(
        emqx_bridge_tablestore, <<"tablestore">>, Method ++ "_connector"
    ).
