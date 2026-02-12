%%--------------------------------------------------------------------
%% Copyright (c) 2017-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_coap_api).

-behaviour(minirest_api).

-include("emqx_coap.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include_lib("typerefl/include/types.hrl").
-include_lib("emqx/include/logger.hrl").

%% API
-export([api_spec/0, paths/0, schema/1, namespace/0]).

-export([request/2]).

-define(PREFIX, "/gateways/coap/clients/:clientid").
-define(TAGS, [<<"CoAP Gateways">>]).

-import(hoconsc, [mk/2, enum/1]).
-import(emqx_dashboard_swagger, [error_codes/2]).

-elvis([{elvis_style, atom_naming_convention, disable}]).

%%--------------------------------------------------------------------
%%  API
%%--------------------------------------------------------------------

namespace() -> "gateway_coap".

api_spec() ->
    emqx_dashboard_swagger:spec(?MODULE, #{check_schema => true, translate_body => true}).

paths() ->
    [?PREFIX ++ "/request"].

schema(?PREFIX ++ "/request") ->
    #{
        operationId => request,
        post => #{
            tags => ?TAGS,
            desc => ?DESC(send_coap_request),
            parameters => request_parameters(),
            requestBody => request_body(),
            responses => #{
                200 => coap_message(),
                400 => error_codes(['BAD_REQUEST'], <<"Missing required parameter">>),
                404 => error_codes(['CLIENT_NOT_FOUND'], ?DESC("client_not_found")),
                502 => error_codes(
                    ['CLIENT_BAD_RESPONSE'], <<"Client returned an invalid CoAP reply">>
                ),
                504 => error_codes(
                    ['CLIENT_NOT_RESPONSE'], ?DESC("client_not_response_timeout")
                )
            }
        }
    }.

request(post, #{body := Body, bindings := Bindings}) ->
    ClientId = maps:get(clientid, Bindings, undefined),
    Method = maps:get(<<"method">>, Body, get),
    AtomCT = maps:get(<<"content_type">>, Body, undefined),
    case AtomCT of
        undefined ->
            {400, #{code => 'BAD_REQUEST', message => <<"missing content_type">>}};
        _ ->
            Token = maps:get(<<"token">>, Body, <<>>),
            Payload = maps:get(<<"payload">>, Body, <<>>),
            WaitTime = maps:get(<<"timeout">>, Body),
            CT = erlang:atom_to_binary(AtomCT),
            Payload2 = parse_payload(CT, Payload),

            Msg = emqx_coap_message:request(
                con,
                Method,
                Payload2,
                #{content_format => CT}
            ),

            Msg2 = Msg#coap_message{token = Token},

            case call_client(ClientId, Msg2, WaitTime) of
                timeout ->
                    {504, #{code => 'CLIENT_NOT_RESPONSE'}};
                not_found ->
                    {404, #{code => 'CLIENT_NOT_FOUND'}};
                {error, internal} ->
                    {502, #{code => 'CLIENT_BAD_RESPONSE'}};
                Response = #coap_message{} ->
                    {200, format_to_response(CT, Response)};
                Other ->
                    ?SLOG(warning, #{
                        msg => "coap_client_bad_response",
                        clientid => ClientId,
                        response => Other
                    }),
                    {502, #{code => 'CLIENT_BAD_RESPONSE'}}
            end
    end.

%%--------------------------------------------------------------------
%%  Internal functions
%%--------------------------------------------------------------------
request_parameters() ->
    [{clientid, mk(binary(), #{in => path, required => true})}].

request_body() ->
    [
        {token, mk(binary(), #{desc => ?DESC(token)})},
        {method, mk(enum([get, put, post, delete]), #{desc => ?DESC(method)})},
        {timeout, mk(emqx_schema:timeout_duration_ms(), #{desc => ?DESC(timeout)})},
        {content_type,
            mk(
                enum(['text/plain', 'application/json', 'application/octet-stream']),
                #{desc => ?DESC(content_type)}
            )},
        {payload, mk(binary(), #{desc => ?DESC(payload)})}
    ].

coap_message() ->
    [
        {id, mk(integer(), #{desc => ?DESC(message_id)})},
        {token, mk(string(), #{desc => ?DESC(token)})},
        {method, mk(string(), #{desc => ?DESC(response_code)})},
        {payload, mk(string(), #{desc => ?DESC(payload)})}
    ].

format_to_response(ContentType, #coap_message{
    id = Id,
    token = Token,
    method = Method,
    payload = Payload
}) ->
    #{
        id => Id,
        token => Token,
        method => format_to_binary(Method),
        payload => format_payload(ContentType, Payload)
    }.

format_to_binary(Obj) ->
    erlang:list_to_binary(io_lib:format("~p", [Obj])).

format_payload(<<"application/octet-stream">>, Payload) ->
    base64:encode(Payload);
format_payload(_, Payload) ->
    Payload.

parse_payload(<<"application/octet-stream">>, Body) ->
    base64:decode(Body);
parse_payload(_, Body) ->
    Body.

call_client(ClientId, Msg, Timeout) ->
    try
        case emqx_gateway_cm_registry:lookup_channels(coap, ClientId) of
            [Channel | _] ->
                case do_send_request(Channel, Msg, Timeout) of
                    timeout ->
                        timeout;
                    Reply = #coap_message{} ->
                        maybe_collect_block2(Channel, Msg, Reply, Timeout);
                    Reply ->
                        Reply
                end;
            _ ->
                not_found
        end
    catch
        _:Error:Trace ->
            ?SLOG(warning, #{
                msg => "coap_client_call_exception",
                clientid => ClientId,
                error => Error,
                stacktrace => Trace
            }),
            {error, internal}
    end.

do_send_request(Channel, Msg, Timeout) ->
    RequestId = emqx_coap_channel:send_request(Channel, Msg),
    case gen_server:wait_response(RequestId, Timeout) of
        {reply, Reply} ->
            Reply;
        _ ->
            timeout
    end.

maybe_collect_block2(Channel, Req, Resp, Timeout) ->
    State0 = emqx_coap_blockwise:new(emqx_coap_blockwise:default_opts(coap)),
    Ctx = #{request => Req},
    collect_block2(Channel, Ctx, Resp, Timeout, State0, 0).

collect_block2(Channel, Ctx, Resp, Timeout, State0, N) ->
    case validate_block2_reply(Resp, N) of
        ok ->
            collect_block2_reply(Channel, Ctx, Resp, Timeout, State0, N);
        {error, _} = Error ->
            Error
    end.

collect_block2_reply(Channel, Ctx, Resp, Timeout, State0, N) ->
    case emqx_coap_blockwise:client_in_response(Ctx, Resp, State0) of
        {deliver, FullResp, _State} ->
            FullResp;
        {send_next, NextReq, State1} ->
            maybe_send_next_block2(Channel, Ctx, Resp, NextReq, Timeout, State0, State1, N)
    end.

maybe_send_next_block2(Channel, Ctx, Resp, NextReq, Timeout, State0, State1, N) ->
    case block2_exceeds_max_body(Resp, State0, N) of
        true ->
            block2_too_large_reply(Ctx, Resp);
        false ->
            send_next_block2(Channel, Ctx, NextReq, Timeout, State1, N)
    end.

send_next_block2(Channel, Ctx, NextReq, Timeout, State1, N) ->
    case do_send_request(Channel, NextReq, Timeout) of
        timeout ->
            timeout;
        NextResp = #coap_message{} ->
            collect_block2(Channel, Ctx, NextResp, Timeout, State1, N + 1);
        Other ->
            Other
    end.

validate_block2_reply(Resp, N) ->
    case emqx_coap_message:get_option(block2, Resp, undefined) of
        undefined when N =:= 0 ->
            ok;
        {Num, _More, Size} when is_integer(Num), Num >= 0, is_integer(Size), Size > 0 ->
            case Num =:= N of
                true -> ok;
                false -> {error, invalid_block2_sequence}
            end;
        _ ->
            {error, invalid_block2_option}
    end.

block2_exceeds_max_body(Resp, State, N) ->
    case emqx_coap_message:get_option(block2, Resp, undefined) of
        {N, true, Size} when is_integer(Size), Size > 0 ->
            MaxBlocks = max_block2_blocks(State, Size),
            (N + 1) >= MaxBlocks;
        _ ->
            false
    end.

max_block2_blocks(State, Size) ->
    MaxBody = emqx_coap_blockwise:max_body_size(State),
    (MaxBody + Size - 1) div Size.

block2_too_large_reply(#{request := Req0}, _Resp) when is_record(Req0, coap_message) ->
    emqx_coap_message:piggyback({error, request_entity_too_large}, Req0).
