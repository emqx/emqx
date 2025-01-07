%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_hstreamdb_connector_info).

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
    hstreamdb.

bridge_types() ->
    [hstreamdb].

resource_callback_module() ->
    emqx_bridge_hstreamdb_connector.

config_schema() ->
    {hstreamdb,
        hoconsc:mk(
            hoconsc:map(name, hoconsc:ref(emqx_bridge_hstreamdb, "config_connector")),
            #{
                desc => <<"HStreamDB Connector Config">>,
                required => false
            }
        )}.

schema_module() ->
    emqx_bridge_hstreamdb.

api_schema(Method) ->
    emqx_connector_schema:api_ref(
        emqx_bridge_hstreamdb, <<"hstreamdb">>, Method ++ "_connector"
    ).
