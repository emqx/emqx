%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_timescale_connector_info).

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
    timescale.

bridge_types() ->
    [timescale].

resource_callback_module() ->
    emqx_postgresql.

config_schema() ->
    {timescale,
        hoconsc:mk(
            hoconsc:map(name, hoconsc:ref(emqx_bridge_timescale, "config_connector")),
            #{
                desc => <<"Timescale Connector Config">>,
                required => false
            }
        )}.

schema_module() ->
    emqx_bridge_timescale.

api_schema(Method) ->
    emqx_connector_schema:api_ref(
        emqx_bridge_timescale, <<"timescale">>, Method ++ "_connector"
    ).
