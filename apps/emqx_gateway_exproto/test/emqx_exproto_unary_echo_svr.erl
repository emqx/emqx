%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_exproto_unary_echo_svr).

-behavior(emqx_exproto_v_1_connection_unary_handler_bhvr).

-import(
    emqx_exproto_echo_svr,
    [
        handle_in/3,
        handle_out/2,
        handle_out/3
    ]
).

-export([
    on_socket_created/2,
    on_received_bytes/2,
    on_socket_closed/2,
    on_timer_timeout/2,
    on_received_messages/2
]).

-define(LOG(Fmt, Args), ct:pal(Fmt, Args)).

-define(CLIENT, emqx_exproto_v_1_connection_adapter_client).

-define(close(Req), ?CLIENT:close(Req, #{channel => ct_test_channel})).

-define(TYPE_CONNECT, 1).
-define(TYPE_CONNACK, 2).
-define(TYPE_PUBLISH, 3).
-define(TYPE_PUBACK, 4).
-define(TYPE_SUBSCRIBE, 5).
-define(TYPE_SUBACK, 6).
-define(TYPE_UNSUBSCRIBE, 7).
-define(TYPE_UNSUBACK, 8).
-define(TYPE_DISCONNECT, 9).
-define(TYPE_RAW_PUBLISH, 10).

%%--------------------------------------------------------------------
%% callbacks
%%--------------------------------------------------------------------

-spec on_socket_created(emqx_exproto_pb:socket_created_request(), grpc:metadata()) ->
    {ok, emqx_exproto_pb:empty_success(), grpc:metadata()}
    | {error, grpc_stream:error_response()}.
on_socket_created(_Req, _Md) ->
    {ok, #{}, _Md}.

-spec on_socket_closed(emqx_exproto_pb:socket_closed_request(), grpc:metadata()) ->
    {ok, emqx_exproto_pb:empty_success(), grpc:metadata()}
    | {error, grpc_stream:error_response()}.
on_socket_closed(_Req, _Md) ->
    {ok, #{}, _Md}.

-spec on_received_bytes(emqx_exproto_pb:received_bytes_request(), grpc:metadata()) ->
    {ok, emqx_exproto_pb:empty_success(), grpc:metadata()}
    | {error, grpc_stream:error_response()}.
on_received_bytes(#{conn := Conn, bytes := Bytes}, _Md) ->
    #{<<"type">> := Type} = Params = emqx_utils_json:decode(Bytes, [return_maps]),
    _ = handle_in(Conn, Type, Params),
    {ok, #{}, _Md}.

-spec on_timer_timeout(emqx_exproto_pb:timer_timeout_request(), grpc:metadata()) ->
    {ok, emqx_exproto_pb:empty_success(), grpc:metadata()}
    | {error, grpc_stream:error_response()}.
on_timer_timeout(#{conn := Conn, type := 'KEEPALIVE'}, _Md) ->
    ?LOG("Close this connection ~p due to keepalive timeout", [Conn]),
    handle_out(Conn, ?TYPE_DISCONNECT),
    ?close(#{conn => Conn}),
    {ok, #{}, _Md}.

-spec on_received_messages(emqx_exproto_pb:received_messages_request(), grpc:metadata()) ->
    {ok, emqx_exproto_pb:empty_success(), grpc:metadata()}
    | {error, grpc_stream:error_response()}.
on_received_messages(#{conn := Conn, messages := Messages}, _Md) ->
    lists:foreach(
        fun(Message) ->
            handle_out(Conn, ?TYPE_PUBLISH, Message)
        end,
        Messages
    ),
    {ok, #{}, _Md}.
