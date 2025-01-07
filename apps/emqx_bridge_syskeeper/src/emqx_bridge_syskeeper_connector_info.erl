%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_syskeeper_connector_info).

-behaviour(emqx_connector_info).

-export([
    type_name/0,
    bridge_types/0,
    resource_callback_module/0,
    config_transform_module/0,
    config_schema/0,
    schema_module/0,
    api_schema/1
]).

type_name() ->
    syskeeper_forwarder.

bridge_types() ->
    [syskeeper_forwarder].

resource_callback_module() ->
    emqx_bridge_syskeeper_connector.

config_transform_module() ->
    emqx_bridge_syskeeper_connector.

config_schema() ->
    {syskeeper_forwarder,
        hoconsc:mk(
            hoconsc:map(name, hoconsc:ref(emqx_bridge_syskeeper_connector, config)),
            #{
                desc => <<"Syskeeper Connector Config">>,
                required => false
            }
        )}.

schema_module() ->
    emqx_bridge_syskeeper_connector.

api_schema(Method) ->
    emqx_connector_schema:api_ref(
        emqx_bridge_syskeeper_connector, <<"syskeeper_forwarder">>, Method
    ).
