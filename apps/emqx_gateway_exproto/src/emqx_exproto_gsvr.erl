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

%% The gRPC server for ConnectionAdapter
-module(emqx_exproto_gsvr).

% -behaviour(emqx_exproto_v_1_connection_adapter_bhvr).

-include("emqx_exproto.hrl").
-include_lib("emqx/include/logger.hrl").

-define(IS_QOS(X), (X =:= 0 orelse X =:= 1 orelse X =:= 2)).

-define(DEFAULT_CALL_TIMEOUT, 5000).

%% gRPC server callbacks
-export([
    send/2,
    close/2,
    authenticate/2,
    start_timer/2,
    publish/2,
    raw_publish/2,
    subscribe/2,
    unsubscribe/2
]).

%% TODO:
%% The spec should be :: grpc_cowboy_h:error_response()
%% But there is no such module as grpc_cowboy_h
-type error_response() :: term().

%%--------------------------------------------------------------------
%% gRPC ConnectionAdapter service
%%--------------------------------------------------------------------

-spec send(emqx_exproto_pb:send_bytes_request(), grpc:metadata()) ->
    {ok, emqx_exproto_pb:code_response(), grpc:metadata()}
    | {error, error_response()}.
send(Req = #{conn := Conn, bytes := Bytes}, Md) ->
    ?SLOG(debug, #{
        msg => "recv_grpc_function_call",
        function => ?FUNCTION_NAME,
        request => Req
    }),
    {ok, response(call(Conn, {send, Bytes})), Md}.

-spec close(emqx_exproto_pb:close_socket_request(), grpc:metadata()) ->
    {ok, emqx_exproto_pb:code_response(), grpc:metadata()}
    | {error, error_response()}.
close(Req = #{conn := Conn}, Md) ->
    ?SLOG(debug, #{
        msg => "recv_grpc_function_call",
        function => ?FUNCTION_NAME,
        request => Req
    }),
    {ok, response(call(Conn, close)), Md}.

-spec authenticate(emqx_exproto_pb:authenticate_request(), grpc:metadata()) ->
    {ok, emqx_exproto_pb:code_response(), grpc:metadata()}
    | {error, error_response()}.
authenticate(
    Req = #{
        conn := Conn,
        password := Password,
        clientinfo := ClientInfo
    },
    Md
) ->
    ?SLOG(debug, #{
        msg => "recv_grpc_function_call",
        function => ?FUNCTION_NAME,
        request => Req
    }),
    case validate(clientinfo, ClientInfo) of
        false ->
            {ok, response({error, ?RESP_REQUIRED_PARAMS_MISSED}), Md};
        _ ->
            {ok, response(call(Conn, {auth, ClientInfo, Password})), Md}
    end.

-spec start_timer(emqx_exproto_pb:timer_request(), grpc:metadata()) ->
    {ok, emqx_exproto_pb:code_response(), grpc:metadata()}
    | {error, error_response()}.
start_timer(Req = #{conn := Conn, type := Type, interval := Interval}, Md) when
    Type =:= 'KEEPALIVE' andalso Interval > 0
->
    ?SLOG(debug, #{
        msg => "recv_grpc_function_call",
        function => ?FUNCTION_NAME,
        request => Req
    }),

    {ok, response(call(Conn, {start_timer, keepalive, Interval})), Md};
start_timer(Req, Md) ->
    ?SLOG(debug, #{
        msg => "recv_grpc_function_call",
        function => ?FUNCTION_NAME,
        request => Req
    }),

    {ok, response({error, ?RESP_PARAMS_TYPE_ERROR}), Md}.

-spec publish(emqx_exproto_pb:publish_request(), grpc:metadata()) ->
    {ok, emqx_exproto_pb:code_response(), grpc:metadata()}
    | {error, error_response()}.
publish(Req = #{conn := Conn, topic := Topic, qos := Qos, payload := Payload}, Md) when
    ?IS_QOS(Qos)
->
    ?SLOG(debug, #{
        msg => "recv_grpc_function_call",
        function => ?FUNCTION_NAME,
        request => Req
    }),

    {ok, response(call(Conn, {publish, Topic, Qos, Payload})), Md};
publish(Req, Md) ->
    ?SLOG(debug, #{
        msg => "recv_grpc_function_call",
        function => ?FUNCTION_NAME,
        request => Req
    }),
    {ok, response({error, ?RESP_PARAMS_TYPE_ERROR}), Md}.

-spec raw_publish(emqx_exproto_pb:raw_publish_request(), grpc:metadata()) ->
    {ok, emqx_exproto_pb:code_response(), grpc:metadata()}
    | {error, error_response()}.
raw_publish(Req = #{topic := Topic, qos := Qos, payload := Payload}, Md) ->
    ?SLOG(debug, #{
        msg => "recv_grpc_function_call",
        function => ?FUNCTION_NAME,
        request => Req
    }),
    Msg = emqx_message:make(exproto, Qos, Topic, Payload),
    _ = emqx_broker:safe_publish(Msg),
    {ok, response(ok), Md}.

-spec subscribe(emqx_exproto_pb:subscribe_request(), grpc:metadata()) ->
    {ok, emqx_exproto_pb:code_response(), grpc:metadata()}
    | {error, error_response()}.
subscribe(Req = #{conn := Conn, topic := Topic, qos := Qos}, Md) when
    ?IS_QOS(Qos)
->
    ?SLOG(debug, #{
        msg => "recv_grpc_function_call",
        function => ?FUNCTION_NAME,
        request => Req
    }),
    {ok, response(call(Conn, {subscribe_from_client, Topic, Qos})), Md};
subscribe(Req, Md) ->
    ?SLOG(debug, #{
        msg => "recv_grpc_function_call",
        function => ?FUNCTION_NAME,
        request => Req
    }),
    {ok, response({error, ?RESP_PARAMS_TYPE_ERROR}), Md}.

-spec unsubscribe(emqx_exproto_pb:unsubscribe_request(), grpc:metadata()) ->
    {ok, emqx_exproto_pb:code_response(), grpc:metadata()}
    | {error, error_response()}.
unsubscribe(Req = #{conn := Conn, topic := Topic}, Md) ->
    ?SLOG(debug, #{
        msg => "recv_grpc_function_call",
        function => ?FUNCTION_NAME,
        request => Req
    }),
    {ok, response(call(Conn, {unsubscribe_from_client, Topic})), Md}.

%%--------------------------------------------------------------------
%% Internal funcs
%%--------------------------------------------------------------------

to_pid(ConnStr) ->
    binary_to_term(base64:decode(ConnStr)).

call(ConnStr, Req) ->
    try
        Pid = to_pid(ConnStr),
        emqx_gateway_conn:call(Pid, Req, ?DEFAULT_CALL_TIMEOUT)
    catch
        exit:badarg ->
            {error, ?RESP_PARAMS_TYPE_ERROR, <<"The conn type error">>};
        exit:noproc ->
            {error, ?RESP_CONN_PROCESS_NOT_ALIVE, <<"Connection process is not alive">>};
        exit:{noproc, _} ->
            {error, ?RESP_CONN_PROCESS_NOT_ALIVE, <<"Connection process is not alive">>};
        exit:timeout ->
            {error, ?RESP_UNKNOWN, <<"Connection is not answered">>};
        Class:Reason:Stk ->
            ?SLOG(error, #{
                msg => "call_conn_process_crashed",
                request => Req,
                conn_str => ConnStr,
                reason => {Class, Reason},
                stacktrace => Stk
            }),
            {error, ?RESP_UNKNOWN, <<"Unknown crashes">>}
    end.

%%--------------------------------------------------------------------
%% Data types

stringfy(Reason) ->
    unicode:characters_to_binary((io_lib:format("~0p", [Reason]))).

validate(clientinfo, M) ->
    Required = [proto_name, proto_ver, clientid],
    lists:all(fun(K) -> maps:is_key(K, M) end, Required).

response(ok) ->
    #{code => ?RESP_SUCCESS};
response({error, Code, Reason}) when
    ?IS_GRPC_RESULT_CODE(Code)
->
    #{code => Code, message => stringfy(Reason)};
response({error, Code}) when
    ?IS_GRPC_RESULT_CODE(Code)
->
    #{code => Code};
response(Other) ->
    #{code => ?RESP_UNKNOWN, message => stringfy(Other)}.
