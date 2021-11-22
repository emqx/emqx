-module(emqx_bridge_schema).

-include_lib("typerefl/include/types.hrl").

-export([roots/0, fields/1]).

%%======================================================================================
%% Hocon Schema Definitions

roots() -> [bridges].

fields(bridges) ->
    [ {mqtt,
       sc(hoconsc:map(name, hoconsc:union([ ref("ingress_mqtt_bridge")
                                          , ref("egress_mqtt_bridge")
                                          ])),
          #{ desc => "MQTT bridges"
          })}
    , {http,
       sc(hoconsc:map(name, ref("http_bridge")),
          #{ desc => "HTTP bridges"
          })}
    ];

fields("ingress_mqtt_bridge") ->
    [ direction(ingress, emqx_connector_mqtt_schema:ingress_desc())
    , connector_name()
    ] ++ proplists:delete(hookpoint, emqx_connector_mqtt_schema:fields("ingress"));

fields("egress_mqtt_bridge") ->
    [ direction(egress, emqx_connector_mqtt_schema:egress_desc())
    , connector_name()
    ] ++ emqx_connector_mqtt_schema:fields("egress");

fields("http_bridge") ->
    basic_config_http() ++
    [ {url,
       sc(binary(),
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
    , {from_local_topic,
       sc(binary(),
          #{ desc =>"""
The MQTT topic filter to be forwarded to the HTTP server. All MQTT PUBLISH messages which topic
match the from_local_topic will be forwarded.<br>
NOTE: if this bridge is used as the output of a rule (emqx rule engine), and also from_local_topic is configured, then both the data got from the rule and the MQTT messages that matches
from_local_topic will be forwarded.
"""
           })}
    , {method,
       sc(method(),
          #{ default => post
           , desc =>"""
The method of the HTTP request. All the available methods are: post, put, get, delete.<br>
Template with variables is allowed.<br>
"""
           })}
    , {headers,
       sc(map(),
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
    , {body,
       sc(binary(),
          #{ default => <<"${payload}">>
           , desc =>"""
The body of the HTTP request.<br>
Template with variables is allowed.
"""
           })}
    , {request_timeout,
       sc(emqx_schema:duration_ms(),
          #{ default => <<"30s">>
           , desc =>"""
How long will the HTTP request timeout.
"""
           })}
    ].

direction(Dir, Desc) ->
    {direction,
        sc(Dir,
           #{ nullable => false
            , desc => "The direction of the bridge. Can be one of 'ingress' or 'egress'.<br>" ++
                      Desc
            })}.

connector_name() ->
    {connector,
        sc(binary(),
           #{ nullable => false
            , desc =>"""
The connector name to be used for this bridge.
Connectors are configured by 'connectors.<type>.<name>
"""
            })}.

basic_config_http() ->
    proplists:delete(base_url, emqx_connector_http:fields(config)).

method() ->
    hoconsc:enum([post, put, get, delete]).

sc(Type, Meta) -> hoconsc:mk(Type, Meta).

ref(Field) -> hoconsc:ref(?MODULE, Field).
