-module(emqx_bridge_schema).

-export([roots/0, fields/1]).

%%======================================================================================
%% Hocon Schema Definitions

roots() -> ["bridges"].

fields("bridges") ->
    [{mqtt, hoconsc:mk(hoconsc:map(name, hoconsc:ref(?MODULE, "mqtt_bridge")))}];

fields("mqtt_bridge") ->
    emqx_connector_mqtt:fields("config").
