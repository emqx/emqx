%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_datalayers_connector_info).

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
    datalayers.

bridge_types() ->
    [datalayers].

resource_callback_module() ->
    emqx_bridge_datalayers_connector.

config_schema() ->
    {datalayers,
        hoconsc:mk(
            hoconsc:map(name, hoconsc:ref(emqx_bridge_datalayers, "config_connector")),
            #{
                desc => <<"Datalayers Connector Config">>,
                required => false
            }
        )}.

schema_module() ->
    emqx_bridge_datalayers.

api_schema(Method) ->
    emqx_connector_schema:api_ref(
        emqx_bridge_datalayers, <<"datalayers">>, Method ++ "_connector"
    ).
