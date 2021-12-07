-module(emqx_bridge_schema).

-include_lib("typerefl/include/types.hrl").

-import(hoconsc, [mk/2, ref/2]).

-export([roots/0, fields/1]).

-export([ get_response/0
        , put_request/0
        , post_request/0
        ]).

-export([ connector_name/0
        ]).

%%======================================================================================
%% Hocon Schema Definitions

-define(CONN_TYPES, [mqtt]).

%%======================================================================================
%% For HTTP APIs
get_response() ->
    http_schema("get").

connector_name() ->
    {connector,
        mk(binary(),
           #{ nullable => false
            , desc =>"""
The connector name to be used for this bridge.
Connectors are configured as 'connectors.{type}.{name}',
for example 'connectors.http.mybridge'.
"""
            })}.

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
