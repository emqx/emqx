-module(emqx_bridge_schema).

-include_lib("typerefl/include/types.hrl").

-import(hoconsc, [mk/2, ref/2]).

-export([roots/0, fields/1]).

-export([ get_response/0
        , put_request/0
        , post_request/0
        ]).

-export([ common_bridge_fields/0
        , metrics_status_fields/0
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

metrics_status_fields() ->
    [ {"metrics", mk(ref(?MODULE, "metrics"), #{desc => "The metrics of the bridge"})}
    , {"node_metrics", mk(hoconsc:array(ref(?MODULE, "node_metrics")),
        #{ desc => "The metrics of the bridge for each node"
         })}
    , {"status", mk(ref(?MODULE, "status"), #{desc => "The status of the bridge"})}
    , {"node_status", mk(hoconsc:array(ref(?MODULE, "node_status")),
        #{ desc => "The status of the bridge for each node"
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
        ])), #{})} || T <- ?CONN_TYPES];

fields("metrics") ->
    [ {"matched", mk(integer(), #{desc => "Count of this bridge is queried"})}
    , {"success", mk(integer(), #{desc => "Count of query success"})}
    , {"failed", mk(integer(), #{desc => "Count of query failed"})}
    , {"rate", mk(float(), #{desc => "The rate of matched, times/second"})}
    , {"rate_max", mk(float(), #{desc => "The max rate of matched, times/second"})}
    , {"rate_last5m", mk(float(),
        #{desc => "The average rate of matched in last 5 mins, times/second"})}
    ];

fields("node_metrics") ->
    [ node_name()
    , {"metrics", mk(ref(?MODULE, "metrics"), #{})}
    ];

fields("status") ->
    [ {"matched", mk(integer(), #{desc => "Count of this bridge is queried"})}
    , {"success", mk(integer(), #{desc => "Count of query success"})}
    , {"failed", mk(integer(), #{desc => "Count of query failed"})}
    , {"rate", mk(float(), #{desc => "The rate of matched, times/second"})}
    , {"rate_max", mk(float(), #{desc => "The max rate of matched, times/second"})}
    , {"rate_last5m", mk(float(),
        #{desc => "The average rate of matched in last 5 mins, times/second"})}
    ];

fields("node_status") ->
    [ node_name()
    , {"status", mk(ref(?MODULE, "status"), #{})}
    ].

node_name() ->
    {"node", mk(binary(), #{desc => "The node name", example => "emqx@127.0.0.1"})}.

schema_mod(Type) ->
    list_to_atom(lists:concat(["emqx_bridge_", Type, "_schema"])).
