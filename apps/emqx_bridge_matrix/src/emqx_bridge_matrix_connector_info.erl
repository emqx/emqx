%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_matrix_connector_info).

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
    matrix.

bridge_types() ->
    [matrix].

resource_callback_module() ->
    emqx_postgresql.

config_schema() ->
    {matrix,
        hoconsc:mk(
            hoconsc:map(name, hoconsc:ref(emqx_bridge_matrix, "config_connector")),
            #{
                desc => <<"Matrix Connector Config">>,
                required => false
            }
        )}.

schema_module() ->
    emqx_bridge_matrix.

api_schema(Method) ->
    emqx_connector_schema:api_ref(
        emqx_bridge_matrix, <<"matrix">>, Method ++ "_connector"
    ).
