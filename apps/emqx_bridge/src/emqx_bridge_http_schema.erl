-module(emqx_bridge_http_schema).

-include_lib("typerefl/include/types.hrl").

-import(hoconsc, [mk/2, enum/1]).

-export([roots/0, fields/1, namespace/0]).

%%======================================================================================
%% Hocon Schema Definitions
namespace() -> "bridge".

roots() -> [].

fields("config") ->
    basic_config() ++
    [ {url, mk(binary(),
          #{ required => true
           , desc =>"""
The URL of the HTTP Bridge.<br>
Template with variables is allowed in the path, but variables cannot be used in the scheme, host,
or port part.<br>
For example, <code> http://localhost:9901/${topic} </code> is allowed, but
<code> http://${host}:9901/message </code> or <code> http://localhost:${port}/message </code>
is not allowed.
"""
           })}
    , {local_topic, mk(binary(),
          #{ desc =>"""
The MQTT topic filter to be forwarded to the HTTP server. All MQTT 'PUBLISH' messages with the topic
matching the local_topic will be forwarded.<br/>
NOTE: if this bridge is used as the output of a rule (EMQX rule engine), and also local_topic is
configured, then both the data got from the rule and the MQTT messages that match local_topic
will be forwarded.
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
          #{ default => <<"15s">>
           , desc =>"""
How long will the HTTP request timeout.
"""
           })}
    ];

fields("post") ->
    [ type_field()
    , name_field()
    ] ++ fields("config");

fields("put") ->
    fields("config");

fields("get") ->
    emqx_bridge_schema:metrics_status_fields() ++ fields("post").

basic_config() ->
    [ {enable,
        mk(boolean(),
           #{ desc => "Enable or disable this bridge"
            , default => true
            })}
    , {direction,
        mk(egress,
           #{ desc => "The direction of this bridge, MUST be 'egress'"
            , default => egress
            })}
    ]
    ++ proplists:delete(base_url, emqx_connector_http:fields(config)).

%%======================================================================================

type_field() ->
    {type, mk(http,
        #{ required => true
         , desc => "The Bridge Type"
         })}.

name_field() ->
    {name, mk(binary(),
        #{ required => true
         , desc => "Bridge name, used as a human-readable description of the bridge."
         })}.

method() ->
    enum([post, put, get, delete]).
