%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_oracle_connector_info).

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
    oracle.

bridge_types() ->
    [oracle].

resource_callback_module() ->
    emqx_oracle.

config_schema() ->
    {oracle,
        hoconsc:mk(
            hoconsc:map(name, hoconsc:ref(emqx_bridge_oracle, "config_connector")),
            #{
                desc => <<"Oracle Connector Config">>,
                required => false,
                validator => fun emqx_bridge_oracle:config_validator/1
            }
        )}.

schema_module() ->
    emqx_bridge_oracle.

api_schema(Method) ->
    emqx_connector_schema:api_ref(
        emqx_bridge_oracle, <<"oracle">>, Method ++ "_connector"
    ).
