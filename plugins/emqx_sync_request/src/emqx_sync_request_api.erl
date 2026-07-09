%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_sync_request_api).

-behaviour(minirest_api).

-include("emqx_sync_request.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include_lib("emqx_utils/include/emqx_api_key_scopes.hrl").

-export([
    api_spec/0,
    paths/0,
    schema/1,
    fields/1,
    namespace/0
]).

-export([scopes/0]).

-export([request/2]).

namespace() -> undefined.

api_spec() ->
    emqx_dashboard_swagger:spec(?MODULE, #{check_schema => false, translate_body => true}).

scopes() -> ?SCOPE_PUBLISH.

paths() ->
    ["/plugin_api/emqx_sync_request/request"].

schema("/plugin_api/emqx_sync_request/request") ->
    #{
        'operationId' => request,
        post => #{
            summary => <<"Send a synchronous MQTT request">>,
            description =>
                <<"Publish one MQTT request and wait for the first matching response.">>,
            tags => [<<"Plugin">>],
            'requestBody' => hoconsc:mk(hoconsc:ref(?MODULE, sync_request), #{}),
            responses => #{
                200 => hoconsc:mk(hoconsc:ref(?MODULE, ok_response), #{}),
                400 => hoconsc:mk(hoconsc:ref(?MODULE, error_response), #{}),
                404 => hoconsc:mk(hoconsc:ref(?MODULE, error_response), #{}),
                409 => hoconsc:mk(hoconsc:ref(?MODULE, error_response), #{}),
                429 => hoconsc:mk(hoconsc:ref(?MODULE, error_response), #{}),
                503 => hoconsc:mk(hoconsc:ref(?MODULE, error_response), #{}),
                504 => hoconsc:mk(hoconsc:ref(?MODULE, error_response), #{}),
                500 => hoconsc:mk(hoconsc:ref(?MODULE, error_response), #{})
            },
            log_meta => emqx_dashboard_audit:importance(low)
        }
    }.

fields(sync_request) ->
    [
        {timeout,
            hoconsc:mk(binary(), #{
                required => false,
                default => ?DEFAULT_TIMEOUT,
                desc => <<"HTTP wait timeout, for example 10s.">>
            })},
        {request,
            hoconsc:mk(hoconsc:ref(?MODULE, request), #{
                required => true,
                desc => <<"MQTT request parameters.">>
            })}
    ];
fields(request) ->
    [
        {topic,
            hoconsc:mk(binary(), #{
                required => true,
                desc => <<"MQTT request topic.">>
            })},
        {response_topic,
            hoconsc:mk(binary(), #{
                required => true,
                desc => <<"MQTT response topic.">>
            })},
        {request_id,
            hoconsc:mk(binary(), #{
                required => true,
                desc => <<"Plain MQTT 5 Correlation Data, up to 128 bytes.">>
            })},
        {qos,
            hoconsc:mk(emqx_schema:qos(), #{
                required => false,
                default => 0,
                desc => <<"MQTT QoS.">>
            })},
        {payload_encoding,
            hoconsc:mk(hoconsc:enum([plain, base64]), #{
                required => false,
                default => plain,
                desc => <<"Request payload encoding.">>
            })},
        {payload,
            hoconsc:mk(binary(), #{
                required => true,
                desc => <<"MQTT request payload.">>
            })},
        {content_type,
            hoconsc:mk(binary(), #{
                required => false,
                desc => <<"MQTT 5 Content Type for the request.">>
            })}
    ];
fields(ok_response) ->
    [
        {code,
            hoconsc:mk(binary(), #{
                desc => <<"OK">>,
                example => ?CODE_OK
            })},
        {message,
            hoconsc:mk(binary(), #{
                desc => <<"OK">>,
                example => ?CODE_OK
            })},
        {response,
            hoconsc:mk(hoconsc:ref(?MODULE, response), #{
                required => true,
                desc => <<"MQTT response message.">>
            })}
    ];
fields(response) ->
    [
        {topic,
            hoconsc:mk(binary(), #{
                desc => <<"MQTT response topic.">>
            })},
        {request_id,
            hoconsc:mk(binary(), #{
                desc => <<"The request_id from the HTTP request.">>
            })},
        {payload_encoding,
            hoconsc:mk(hoconsc:enum([base64]), #{
                desc => <<"Response payload encoding.">>
            })},
        {payload,
            hoconsc:mk(binary(), #{
                desc => <<"Base64 encoded MQTT response payload.">>
            })},
        {content_type,
            hoconsc:mk(binary(), #{
                required => false,
                desc => <<"MQTT 5 Content Type from the response.">>
            })}
    ];
fields(error_response) ->
    [
        {code,
            hoconsc:mk(binary(), #{
                desc =>
                    <<"BAD_REQUEST, NO_SUBSCRIBERS, CONFLICT, TOO_MANY_REQUESTS, SERVICE_UNAVAILABLE, INTERNAL_ERROR, or TIMEOUT.">>
            })},
        {message,
            hoconsc:mk(binary(), #{
                desc => <<"Human-readable error message.">>
            })}
    ].

request(post, #{body := Body}) ->
    emqx_sync_request:request(Body).
