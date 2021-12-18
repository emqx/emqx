-module(emqx_bridge_schema).

-include_lib("typerefl/include/types.hrl").

-import(hoconsc, [mk/2, ref/2]).

-export([roots/0, fields/1]).

-export([ get_response/0
        , put_request/0
        , post_request/0
        ]).

-export([ common_bridge_fields/0
        , direction_field/2
        ]).

%%======================================================================================
%% Hocon Schema Definitions

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
    Schemas = lists:flatmap(fun(Type) ->
            [ref(schema_mod(Type), Method ++ "_ingress"),
             ref(schema_mod(Type), Method ++ "_egress")]
        end, ?CONN_TYPES),
    hoconsc:union([ref(emqx_bridge_http_schema, Method)
                   | Schemas]).

common_bridge_fields() ->
    [ {enable,
        mk(boolean(),
           #{ desc =>"Enable or disable this bridge"
            , default => true
            })}
    , {connector,
        mk(binary(),
           #{ nullable => false
            , example => <<"mqtt:my_mqtt_connector">>
            , desc =>"""
The connector Id to be used for this bridge. Connector Ids must be of format: '{type}:{name}'.<br>
In config files, you can find the corresponding config entry for a connector by such path: 'connectors.{type}.{name}'.<br>
"""
            })}
    ].

direction_field(Dir, Desc) ->
    {direction, mk(Dir,
        #{ nullable => false
         , desc => "The direction of the bridge. Can be one of 'ingress' or 'egress'.<br>"
            ++ Desc
         })}.

%%======================================================================================
%% For config files
roots() -> [bridges].

fields(bridges) ->
    [{http, mk(hoconsc:map(name, ref(emqx_bridge_http_schema, "bridge")), #{})}]
    ++ [{T, mk(hoconsc:map(name, hoconsc:union([
            ref(schema_mod(T), "ingress"),
            ref(schema_mod(T), "egress")
        ])), #{})} || T <- ?CONN_TYPES].

schema_mod(Type) ->
    list_to_atom(lists:concat(["emqx_bridge_", Type, "_schema"])).
