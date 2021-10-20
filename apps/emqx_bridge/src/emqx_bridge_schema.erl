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
    basic_config_http() ++
    [ {url, hoconsc:mk(binary())}
    , {from_local_topic, hoconsc:mk(binary())}
    , {method, hoconsc:mk(method(), #{default => post})}
    , {headers, hoconsc:mk(map(),
        #{default => #{
            <<"accept">> => <<"application/json">>,
            <<"cache-control">> => <<"no-cache">>,
            <<"connection">> => <<"keep-alive">>,
            <<"content-type">> => <<"application/json">>,
            <<"keep-alive">> => <<"timeout=5">>}})
      }
    , {body, hoconsc:mk(binary(), #{default => <<"${payload}">>})}
    , {request_timeout, hoconsc:mk(emqx_schema:duration_ms(), #{default => <<"30s">>})}
    ].

basic_config_http() ->
    proplists:delete(base_url, emqx_connector_http:fields(config)).

method() ->
    hoconsc:enum([post, put, get, delete]).
