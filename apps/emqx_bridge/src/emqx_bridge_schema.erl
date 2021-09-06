-module(emqx_bridge_schema).

-export([roots/0, fields/1]).

%%======================================================================================
%% Hocon Schema Definitions

roots() -> ["bridges"].

fields("bridges") ->
    [{mqtt, hoconsc:ref("mqtt")}];

fields("mqtt") ->
    [{"?name"}, hoconsc:ref("mqtt_briage")];

fields("mqtt_briage") ->
    emqx_connector_mqtt:fields("config").
