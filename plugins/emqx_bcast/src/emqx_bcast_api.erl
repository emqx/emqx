%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bcast_api).

-export([
    handle/3,
    handle_local/3,
    success_response/2,
    error_response/3,
    error_response/4
]).

-include("emqx_bcast.hrl").
-include_lib("emqx/include/logger.hrl").

handle(Method, Path, Request) ->
    case {Method, Path} of
        {get, [<<"metrics">>]} ->
            %% Metrics are node-local counters (delivered/acked live on the
            %% node that served the device). Serve them locally instead of
            %% forwarding to a random core: a forwarded scrape would report a
            %% random core's counters and make replicant-local counters
            %% invisible. Scrapers must aggregate all nodes.
            handle_local(get, [<<"metrics">>], Request);
        _ ->
            case emqx_bcast:is_core() of
                true ->
                    handle_local(Method, Path, Request);
                false ->
                    Core = emqx_bcast:random_core(),
                    case
                        emqx_rpc:call(
                            ?MODULE,
                            Core,
                            ?MODULE,
                            handle_local,
                            [Method, Path, Request],
                            ?BCAST_API_RPC_TIMEOUT_MS
                        )
                    of
                        {badrpc, Reason} ->
                            %% Keep internal RPC terms (including node names
                            %% and mnesia reasons) in the server log, not in
                            %% the HTTP response.
                            ?SLOG(error, #{
                                msg => "bcast_core_api_rpc_failed",
                                reason => Reason
                            }),
                            {error, 500, #{},
                                error_response(
                                    emqx_bcast_utils:gen_api_uuid(),
                                    <<"InternalError">>,
                                    <<"Internal error">>
                                )};
                        Result ->
                            Result
                    end
            end
    end.

handle_local(get, [<<"metrics">>], _Request) ->
    Body = emqx_bcast_metrics:collect(),
    {ok, 200, #{<<"content-type">> => <<"text/plain; version=0.0.4">>}, Body};
handle_local(Method, [<<"messages">> | _] = Path, Request) ->
    emqx_bcast_mgmt_api:handle(Method, Path, Request);
handle_local(Method, [<<"deliveries">> | _] = Path, Request) ->
    emqx_bcast_mgmt_api:handle(Method, Path, Request);
handle_local(post, [<<"pub">>], Request) ->
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
handle_local(_Method, _Path, _Request) ->
    {error, not_found}.

error_response(RequestId, Code, Message) ->
    error_response(RequestId, Code, Message, #{}).

error_response(RequestId, Code, Message, Extra) ->
    maps:merge(
        #{
            <<"Success">> => false,
            <<"RequestId">> => RequestId,
            <<"Code">> => Code,
            <<"ErrorMessage">> => Message
        },
        Extra
    ).

success_response(RequestId, MessageId) ->
    #{
        <<"Success">> => true,
        <<"RequestId">> => RequestId,
        <<"MessageId">> => MessageId
    }.
