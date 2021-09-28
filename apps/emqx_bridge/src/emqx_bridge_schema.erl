-module(emqx_bridge_schema).

-include_lib("typerefl/include/types.hrl").

-export([roots/0, fields/1]).

%%======================================================================================
%% Hocon Schema Definitions

roots() -> [bridges].

fields(bridges) ->
    [ {mqtt, hoconsc:mk(hoconsc:map(name, hoconsc:ref(?MODULE, "mqtt_bridge")))}
    , {http, hoconsc:mk(hoconsc:map(name, hoconsc:ref(?MODULE, "http_bridge")))}
    ];

fields("mqtt_bridge") ->
    emqx_connector_mqtt:fields("config");

fields("http_bridge") ->
    emqx_connector_http:fields(config) ++ http_channels().

http_channels() ->
    [{egress_channels, hoconsc:mk(hoconsc:map(id,
        hoconsc:ref(emqx_connector_http, "http_request")))}].
