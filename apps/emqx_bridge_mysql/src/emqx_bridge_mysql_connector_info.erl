%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_mysql_connector_info).

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
    mysql.

bridge_types() ->
    [mysql].

resource_callback_module() ->
    emqx_bridge_mysql_connector.

config_schema() ->
    {mysql,
        hoconsc:mk(
            hoconsc:map(name, hoconsc:ref(emqx_bridge_mysql, "config_connector")),
            #{
                desc => <<"MySQL Connector Config">>,
                required => false
            }
        )}.

schema_module() ->
    emqx_bridge_mysql.

api_schema(Method) ->
    emqx_connector_schema:api_ref(
        emqx_bridge_mysql, <<"mysql">>, Method ++ "_connector"
    ).
