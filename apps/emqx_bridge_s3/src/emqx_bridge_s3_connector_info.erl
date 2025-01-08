%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_s3_connector_info).

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
    s3.

bridge_types() ->
    [s3, s3_aggregated_upload].

resource_callback_module() ->
    emqx_bridge_s3_connector.

config_transform_module() ->
    emqx_bridge_s3.

config_schema() ->
    {s3,
        hoconsc:mk(
            hoconsc:map(name, hoconsc:ref(emqx_bridge_s3, "config_connector")),
            #{
                desc => <<"S3 Connector Config">>,
                required => false
            }
        )}.

schema_module() ->
    emqx_bridge_s3.

api_schema(Method) ->
    emqx_connector_schema:api_ref(
        emqx_bridge_s3, <<"s3">>, Method ++ "_connector"
    ).
