%%--------------------------------------------------------------------
%% Copyright (c) 2020-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% the gRPC client worker for ConnectionHandler service
-module(emqx_exproto_gcli).

-include_lib("emqx/include/logger.hrl").

-logger_header("[ExProtoReq]").

%% APIs
-export([ new/2
        , async_call/3
        ]).

-define(CONN_HANDLER_MOD, emqx_exproto_v_1_connection_handler_client).
-define(CONN_UNARY_HANDLER_MOD, emqx_exproto_v_1_connection_unary_handler_client).

-type service_name() :: 'ConnectionUnaryHandler' | 'ConnectionHandler'.
-type client_mod() :: ?CONN_UNARY_HANDLER_MOD | ?CONN_HANDLER_MOD.

-type state() :: #{service_name := service_name(),
                   client_opts := map(),
                   streams => map(),
                   middleman => pid()
                  }.

%%--------------------------------------------------------------------
%% APIs
%%--------------------------------------------------------------------

-spec new(service_name(), map()) -> state().
new(ServiceName, Options) ->
    #{service_name => ServiceName, client_opts => Options}.

-spec async_call(atom(), map(), state()) -> {ok, state()} | {error, term()}.
async_call(FunName, Req,
           State = #{service_name := 'ConnectionHandler'}) ->
    streaming(FunName, Req, State);
async_call(FunName, Req,
           State = #{service_name := 'ConnectionUnaryHandler',
                     client_opts := Options}) ->
    case request(FunName, Req, Options) of
        ok -> {ok, State};
        {error, Reason} -> {error, Reason}
    end.

%%--------------------------------------------------------------------
%% Internal funcs
%%--------------------------------------------------------------------

%%--------------------------------------------------------------------
%% streaming

streaming(FunName, Req, State = #{client_opts := Options}) ->
    Streams = maps:get(streams, State, #{}),
    case ensure_stream_opened(FunName, Options, Streams) of
        {error, Reason} ->
            ?LOG(error, "CALL ~0p(~0p) failed, reason: ~0p",
                    [FunName, Options, Reason]),
            {error, Reason};
        {ok, Stream} ->
            case catch grpc_client:send(Stream, Req) of
                ok ->
                    ?LOG(debug, "Send to ~s method successfully, request: ~0p", [FunName, Req]),
                    {ok, State#{streams => Streams#{FunName => Stream}}};
                {'EXIT', {not_found, _Stk}} ->
                    %% Not found the stream, reopen it
                    ?LOG(info, "Can not find the old stream ref for ~s; "
                               "re-try with a new stream!", [FunName]),
                    streaming(FunName, Req, State#{streams => maps:remove(FunName, Streams)});
                {'EXIT', {timeout, _Stk}} ->
                    ?LOG(error, "Send to ~s method timeout, request: ~0p", [FunName, Req]),
                    {error, timeout};
                {'EXIT', {Reason1, _Stk}} ->
                    ?LOG(error, "Send to ~s method failure, request: ~0p, reason: ~p, "
                                "stacktrace: ~0p", [FunName, Req, Reason1, _Stk]),
                    {error, Reason1}
            end
    end.

ensure_stream_opened(FunName, Options, Streams) ->
    case maps:get(FunName, Streams, undefined) of
        undefined ->
            case apply(?CONN_HANDLER_MOD, FunName, [Options]) of
                {ok, Stream} -> {ok, Stream};
                {error, Reason} -> {error, Reason}
            end;
        Stream -> {ok, Stream}
    end.

%%--------------------------------------------------------------------
%% unary

request(FunName, Req, Options) ->
    case apply(?CONN_UNARY_HANDLER_MOD, FunName, [Req, Options]) of
        {ok, _EmptySucc, _Md} ->
            ok;
        {error, Reason} ->
            {error, Reason}
    end.
