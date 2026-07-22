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

-export([install_dispatch/0, uninstall_dispatch/0, request/2, handle/3]).

namespace() -> undefined.

api_spec() ->
    emqx_dashboard_swagger:spec(?MODULE, #{check_schema => true, translate_body => true}).

scopes() -> ?SCOPE_PUBLISH.

paths() ->
    case whereis(?SERVICE) of
        undefined -> [];
        _Pid -> ["/plugin_api/emqx_sync_request/request"]
    end.

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

install_dispatch() ->
    update_dispatch(add).

uninstall_dispatch() ->
    update_dispatch(remove).

request(post, Params) ->
    to_minirest_result(
        emqx_sync_request_app:on_handle_api_call(post, [<<"request">>], Params, #{})
    ).

handle(post, [<<"request">>], #{body := Body}) ->
    case emqx_sync_request:on_health_check() of
        ok ->
            to_api_result(emqx_sync_request:request(Body));
        {error, _} ->
            {error, not_found}
    end;
handle(_Method, _Path, _Request) ->
    {error, not_found}.

to_minirest_result({ok, Status, _Headers, Body}) ->
    {Status, Body};
to_minirest_result({error, Status, _Headers, Body}) ->
    {Status, Body};
to_minirest_result({error, not_found}) ->
    {404, #{code => <<"NOT_FOUND">>, message => <<"Not Found">>}}.

to_api_result({ok, Response}) ->
    {ok, 200, #{}, #{code => ?CODE_OK, message => ?CODE_OK, response => Response}};
to_api_result({error, {bad_request, Reason}}) ->
    error_response(400, ?CODE_BAD_REQUEST, Reason);
to_api_result({error, no_subscribers}) ->
    error_response(404, ?CODE_NO_SUBSCRIBERS, no_subscribers);
to_api_result({error, multiple_subscribers}) ->
    error_response(409, ?CODE_CONFLICT, multiple_subscribers);
to_api_result({error, too_many_inflight_requests}) ->
    error_response(429, ?CODE_TOO_MANY_REQUESTS, too_many_inflight_requests);
to_api_result({error, failed_to_dispatch_request}) ->
    error_response(503, ?CODE_SERVICE_UNAVAILABLE, failed_to_dispatch_request);
to_api_result({error, service_unavailable}) ->
    error_response(503, ?CODE_SERVICE_UNAVAILABLE, service_unavailable);
to_api_result({error, timeout}) ->
    error_response(504, ?CODE_TIMEOUT, timeout);
to_api_result({error, Reason}) ->
    error_response(500, ?CODE_INTERNAL_ERROR, Reason).

error_response(Status, Code, Reason) ->
    {error, Status, #{}, #{code => Code, message => reason_hint(Reason)}}.

update_dispatch(Action) ->
    try
        #{started := Listeners} = emqx_dashboard:listeners_status(),
        lists:foreach(fun(Name) -> update_listener_modules(Name, Action) end, Listeners),
        update_listener_dispatches(Action, Listeners),
        ok
    catch
        Class:Reason:Stacktrace ->
            logger:warning(#{
                msg => "sync_request_update_api_dispatch_failed",
                action => Action,
                exception => Class,
                reason => Reason,
                stacktrace => Stacktrace
            }),
            ok
    end.

update_listener_modules(Name, Action) ->
    [Name, Transport, SocketOpts, Protocol, ProtoOpts0] = ranch_server:get_listener_start_args(
        Name
    ),
    #{env := Env0 = #{options := Options0}} = ProtoOpts0,
    Modules0 = maps:get(modules, Options0, []),
    Modules = update_modules(Action, Modules0),
    Options = Options0#{modules => Modules},
    ProtoOpts = ProtoOpts0#{env := Env0#{options := Options}},
    StartArgs = [Name, Transport, SocketOpts, Protocol, ProtoOpts],
    true = ets:insert(ranch_server, {{listener_start_args, Name}, StartArgs}).

update_listener_dispatches(add, Listeners) ->
    lists:foreach(fun minirest:update_dispatch/1, Listeners);
update_listener_dispatches(remove, Listeners) ->
    lists:foreach(fun update_listener_dispatch/1, Listeners).

update_listener_dispatch(Name) ->
    [Name, _Transport, _SocketOpts, _Protocol, #{env := #{options := Options}}] =
        ranch_server:get_listener_start_args(Name),
    #{dispatch := Dispatch} = minirest:generate_dispatch(Options),
    persistent_term:put(Name, Dispatch).

update_modules(add, Modules) ->
    [?MODULE | [Module || Module <- Modules, Module =/= ?MODULE]];
update_modules(remove, Modules) ->
    [Module || Module <- Modules, Module =/= ?MODULE].

reason_hint(<<"invalid_request_body">>) ->
    <<"Request body must be a JSON object.">>;
reason_hint(<<"request_required">>) ->
    <<"request object is required.">>;
reason_hint(<<"topic_required">>) ->
    <<"request.topic is required.">>;
reason_hint(<<"response_topic_required">>) ->
    <<"request.response_topic is required.">>;
reason_hint(<<"payload_required">>) ->
    <<"request.payload is required.">>;
reason_hint(<<"request_id_required">>) ->
    <<"request.request_id is required.">>;
reason_hint(<<"invalid_topic">>) ->
    <<"Topic must be a valid MQTT topic name without wildcards.">>;
reason_hint(<<"invalid_qos">>) ->
    <<"request.qos must be 0, 1, or 2.">>;
reason_hint(<<"invalid_payload_encoding">>) ->
    <<"request.payload_encoding must be plain or base64.">>;
reason_hint(<<"invalid_base64_payload">>) ->
    <<"request.payload must be valid base64 when payload_encoding is base64.">>;
reason_hint(<<"invalid_duration">>) ->
    <<"timeout must be a valid duration.">>;
reason_hint(<<"invalid_timeout">>) ->
    <<"timeout must be greater than 0 and no more than max_timeout.">>;
reason_hint(<<"request_id_too_large">>) ->
    <<"request.request_id must be no longer than 128 bytes.">>;
reason_hint(<<"request_payload_too_large">>) ->
    <<"request.payload exceeds max_payload_size.">>;
reason_hint(<<"response_payload_too_large">>) ->
    <<"MQTT response payload exceeds max_payload_size.">>;
reason_hint(no_subscribers) ->
    <<"No exact subscriber is online for the request topic.">>;
reason_hint(multiple_subscribers) ->
    <<"The request topic has a shared subscription or more than one exact subscriber.">>;
reason_hint(too_many_inflight_requests) ->
    <<"Too many sync requests are waiting for responses.">>;
reason_hint(failed_to_dispatch_request) ->
    <<"Failed to dispatch the request to the subscriber node.">>;
reason_hint(service_unavailable) ->
    <<"Sync request service is stopping.">>;
reason_hint(timeout) ->
    <<"Timed out waiting for a matching MQTT response.">>;
reason_hint(Reason) when is_binary(Reason) ->
    Reason;
reason_hint(Reason) ->
    iolist_to_binary(io_lib:format("~0p", [Reason])).
