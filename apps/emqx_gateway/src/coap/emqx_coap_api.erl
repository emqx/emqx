%%--------------------------------------------------------------------
%% Copyright (c) 2017-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(emqx_coap_api).

-behaviour(minirest_api).

-include_lib("hocon/include/hoconsc.hrl").
-include_lib("typerefl/include/types.hrl").
-include_lib("emqx/include/logger.hrl").
-include("src/coap/include/emqx_coap.hrl").

%% API
-export([api_spec/0, paths/0, schema/1, namespace/0]).

-export([request/2]).

-define(PREFIX, "/gateway/coap/clients/:clientid").

-import(hoconsc, [mk/2, enum/1]).
-import(emqx_dashboard_swagger, [error_codes/2]).

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
            tags => [<<"gateway|coap">>],
            desc => ?DESC(send_coap_request),
            parameters => request_parameters(),
            requestBody => request_body(),
            responses => #{
                200 => coap_message(),
                404 => error_codes(['CLIENT_NOT_FOUND'], <<"Client not found error">>),
                504 => error_codes(
                    ['CLIENT_NOT_RESPONSE'], <<"Waiting for client response timeout">>
                )
            }
        }
    }.

request(post, #{body := Body, bindings := Bindings}) ->
    ClientId = maps:get(clientid, Bindings, undefined),
    Method = maps:get(<<"method">>, Body, get),
    AtomCT = maps:get(<<"content_type">>, Body),
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
        Response ->
            {200, format_to_response(CT, Response)}
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
        {timeout, mk(emqx_schema:duration_ms(), #{desc => ?DESC(timeout)})},
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
                RequestId = emqx_coap_channel:send_request(Channel, Msg),
                case gen_server:wait_response(RequestId, Timeout) of
                    {reply, Reply} ->
                        Reply;
                    _ ->
                        timeout
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
            not_found
    end.
