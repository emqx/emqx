%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_http_connector_info).

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
    http.

bridge_types() ->
    [webhook, http].

resource_callback_module() ->
    emqx_bridge_http_connector.

config_schema() ->
    {http,
        hoconsc:mk(
            hoconsc:map(name, hoconsc:ref(emqx_bridge_http_schema, "config_connector")),
            #{
                desc => <<"HTTP Connector Config">>,
                required => false
            }
        )}.

schema_module() ->
    emqx_bridge_http_schema.

api_schema(Method) ->
    emqx_connector_schema:api_ref(
        emqx_bridge_http_schema, <<"http">>, Method ++ "_connector"
    ).
