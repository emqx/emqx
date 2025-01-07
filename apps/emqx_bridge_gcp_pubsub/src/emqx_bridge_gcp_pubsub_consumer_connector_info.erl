%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_gcp_pubsub_consumer_connector_info).

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
    gcp_pubsub_consumer.

bridge_types() ->
    [gcp_pubsub_consumer].

resource_callback_module() ->
    emqx_bridge_gcp_pubsub_impl_consumer.

config_schema() ->
    {gcp_pubsub_consumer,
        hoconsc:mk(
            hoconsc:map(
                name, hoconsc:ref(emqx_bridge_gcp_pubsub_consumer_schema, "config_connector")
            ),
            #{
                desc => <<"GCP PubSub Consumer Connector Config">>,
                required => false
            }
        )}.

schema_module() ->
    emqx_bridge_gcp_pubsub_consumer_schema.

api_schema(Method) ->
    emqx_connector_schema:api_ref(
        emqx_bridge_gcp_pubsub_consumer_schema, <<"gcp_pubsub_consumer">>, Method ++ "_connector"
    ).
