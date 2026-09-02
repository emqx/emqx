-module(emqx_bridge_nats_connector_info).
-behaviour(emqx_connector_info).
-include("emqx_bridge_nats.hrl").
-export([
    type_name/0,
    bridge_types/0,
    resource_callback_module/0,
    config_transform_module/0,
    config_schema/0,
    schema_module/0,
    api_schema/1
]).

type_name() -> ?CONNECTOR_TYPE.
bridge_types() -> [?ACTION_TYPE].
resource_callback_module() -> emqx_bridge_nats_connector.
config_transform_module() -> emqx_bridge_nats_connector.
config_schema() ->
    {?CONNECTOR_TYPE,
        hoconsc:mk(
            hoconsc:map(name, hoconsc:ref(emqx_bridge_nats_connector_schema, "config_connector")),
            #{desc => <<"NATS Connector Config">>, required => false}
        )}.
schema_module() -> emqx_bridge_nats_connector_schema.
api_schema(Method) ->
    emqx_connector_schema:api_ref(
        emqx_bridge_nats_connector_schema, ?CONNECTOR_TYPE_BIN, Method ++ "_connector"
    ).
