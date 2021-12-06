-module(emqx_connector_schema).

-behaviour(hocon_schema).

-include_lib("typerefl/include/types.hrl").

-import(hoconsc, [mk/2, ref/2]).

-export([roots/0, fields/1]).

-export([ get_response/0
        , put_request/0
        , post_request/0
        ]).

-define(CONN_TYPES, [mqtt]).

%%======================================================================================
%% For HTTP APIs

get_response() ->
    http_schema("get").

put_request() ->
    http_schema("put").

post_request() ->
    http_schema("post").

http_schema(Method) ->
    Schemas = [ref(schema_mod(Type), Method) || Type <- ?CONN_TYPES],
    hoconsc:union(Schemas).

%%======================================================================================
%% Hocon Schema Definitions

roots() -> ["connectors"].

fields(connectors) -> fields("connectors");
fields("connectors") ->
    [ {mqtt,
       mk(hoconsc:map(name,
            hoconsc:union([ ref(emqx_connector_mqtt_schema, "connector")
                          ])),
          #{ desc => "MQTT bridges"
          })}
    ].

schema_mod(Type) ->
    list_to_atom(lists:concat(["emqx_connector_", Type])).
