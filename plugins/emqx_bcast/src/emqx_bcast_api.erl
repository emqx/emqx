%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bcast_api).

-export([
    handle/3,
    success_response/2,
    error_response/3
]).

-include("emqx_bcast.hrl").

handle(get, [<<"metrics">>], _Request) ->
    Body = emqx_bcast_metrics:collect(),
    {ok, 200, #{<<"content-type">> => <<"text/plain; version=0.0.4">>}, Body};
handle(Method, [<<"messages">> | _] = Path, Request) ->
    emqx_bcast_mgmt_api:handle(Method, Path, Request);
handle(Method, [<<"deliveries">> | _] = Path, Request) ->
    emqx_bcast_mgmt_api:handle(Method, Path, Request);
handle(post, [<<"pub">>], Request) ->
    Body = maps:get(body, Request, #{}),
    RequestId = emqx_bcast_utils:gen_api_uuid(),
    case maps:get(<<"Action">>, Body, undefined) of
        <<"PubBroadcast">> ->
            emqx_bcast_pub_broadcast:handle(Body, RequestId);
        <<"BatchPub">> ->
            emqx_bcast_batch_pub:handle(Body, RequestId);
        <<"RegisterMessage">> ->
            emqx_bcast_register_message:handle(Body, RequestId);
        undefined ->
            {error, 400, #{},
                error_response(RequestId, <<"MissingAction">>, <<"Action field is required">>)};
        _ ->
            {error, 400, #{},
                error_response(RequestId, <<"UnknownAction">>, <<"Unknown Action value">>)}
    end;
handle(_Method, _Path, _Request) ->
    {error, not_found}.

error_response(RequestId, Code, Message) ->
    #{
        <<"Success">> => false,
        <<"RequestId">> => RequestId,
        <<"Code">> => Code,
        <<"ErrorMessage">> => Message
    }.

success_response(RequestId, MessageId) ->
    #{
        <<"Success">> => true,
        <<"RequestId">> => RequestId,
        <<"MessageId">> => MessageId
    }.
