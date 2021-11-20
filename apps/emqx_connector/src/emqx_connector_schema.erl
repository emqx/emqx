-module(emqx_connector_schema).

-behaviour(hocon_schema).

-include_lib("typerefl/include/types.hrl").

-export([roots/0, fields/1]).

%%======================================================================================
%% Hocon Schema Definitions

roots() -> ["connectors"].

fields("connectors") ->
    [ {mqtt,
       sc(hoconsc:map(name,
            hoconsc:union([ ref("mqtt_connector")
                          ])),
          #{ desc => "MQTT bridges"
          })}
    ];

fields("mqtt_connector") ->
    emqx_connector_mqtt_schema:fields("connector");

fields("mqtt_connector_info") ->
    [{id, sc(binary(), #{desc => "The connector Id"})}]
    ++ fields("mqtt_connector");

fields("mqtt_connector_test_info") ->
    [{bridge_type, sc(mqtt, #{desc => "The Bridge Type"})}]
    ++ fields("mqtt_connector").

sc(Type, Meta) -> hoconsc:mk(Type, Meta).

ref(Field) -> hoconsc:ref(?MODULE, Field).
