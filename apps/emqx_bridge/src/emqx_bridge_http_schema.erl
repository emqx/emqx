-module(emqx_bridge_http_schema).

-include_lib("typerefl/include/types.hrl").

-import(hoconsc, [mk/2, enum/1]).

-export([roots/0, fields/1]).

%%======================================================================================
%% Hocon Schema Definitions
roots() -> [].

fields("bridge") ->
    basic_config() ++
    [ {url, mk(binary(),
          #{ nullable => false
           , desc =>"""
The URL of the HTTP Bridge.<br>
Template with variables is allowed in the path, but variables cannot be used in the scheme, host,
or port part.<br>
For example, <code> http://localhost:9901/${topic} </code> is allowed, but
<code> http://${host}:9901/message </code> or <code> http://localhost:${port}/message </code>
is not allowed.
"""
           })}
    , {from_local_topic, mk(binary(),
          #{ desc =>"""
The MQTT topic filter to be forwarded to the HTTP server. All MQTT PUBLISH messages which topic
match the from_local_topic will be forwarded.<br>
NOTE: if this bridge is used as the output of a rule (emqx rule engine), and also from_local_topic is configured, then both the data got from the rule and the MQTT messages that matches
from_local_topic will be forwarded.
"""
           })}
    , {method, mk(method(),
          #{ default => post
           , desc =>"""
The method of the HTTP request. All the available methods are: post, put, get, delete.<br>
Template with variables is allowed.<br>
"""
           })}
    , {headers, mk(map(),
          #{ default => #{
                <<"accept">> => <<"application/json">>,
                <<"cache-control">> => <<"no-cache">>,
                <<"connection">> => <<"keep-alive">>,
                <<"content-type">> => <<"application/json">>,
                <<"keep-alive">> => <<"timeout=5">>}
           , desc =>"""
The headers of the HTTP request.<br>
Template with variables is allowed.
"""
           })
      }
    , {body, mk(binary(),
          #{ default => <<"${payload}">>
           , desc =>"""
The body of the HTTP request.<br>
Template with variables is allowed.
"""
           })}
    , {request_timeout, mk(emqx_schema:duration_ms(),
          #{ default => <<"30s">>
           , desc =>"""
How long will the HTTP request timeout.
"""
           })}
    ];

fields("post") ->
    [ type_field()
    , name_field()
    ] ++ fields("bridge");

fields("put") ->
    fields("bridge");

fields("get") ->
    [ id_field()
    ] ++ fields("post").

basic_config() ->
    proplists:delete(base_url, emqx_connector_http:fields(config)).

%%======================================================================================
id_field() ->
    {id, mk(binary(), #{desc => "The Bridge Id", example => "http:my_http_bridge"})}.

type_field() ->
    {type, mk(http, #{desc => "The Bridge Type"})}.

name_field() ->
    {name, mk(binary(), #{desc => "The Bridge Name"})}.

method() ->
    enum([post, put, get, delete]).
