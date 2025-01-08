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

%% the gRPC client agent for ConnectionHandler service
-module(emqx_exproto_gcli).

-include_lib("emqx/include/logger.hrl").

-logger_header("[ExProtoGCli]").

-export([
    init/2,
    maybe_shoot/1,
    maybe_shoot/3,
    ack/2,
    is_empty/1
]).

-export_type([grpc_client_state/0]).

-define(CONN_HANDLER_MOD, emqx_exproto_v_1_connection_handler_client).
-define(CONN_UNARY_HANDLER_MOD, emqx_exproto_v_1_connection_unary_handler_client).

-type service_name() :: 'ConnectionUnaryHandler' | 'ConnectionHandler'.

-type grpc_client_state() ::
    #{
        owner := pid(),
        service_name := service_name(),
        client_opts := options(),
        queue := queue:queue(),
        inflight := atom() | undefined,
        streams => map(),
        middleman => pid() | undefined
    }.

-type options() ::
    #{channel := term()}.

%%--------------------------------------------------------------------
%% APIs
%%--------------------------------------------------------------------

-spec init(service_name(), options()) -> grpc_client_state().
init(ServiceName, Options) ->
    #{
        owner => self(),
        service_name => ServiceName,
        client_opts => Options,
        queue => queue:new(),
        inflight => undefined
    }.

-spec maybe_shoot(atom(), map(), grpc_client_state()) -> grpc_client_state().
maybe_shoot(FunName, Req, GState = #{inflight := undefined}) ->
    shoot(FunName, Req, GState);
maybe_shoot(FunName, Req, GState) ->
    enqueue(FunName, Req, GState).

-spec maybe_shoot(grpc_client_state()) -> grpc_client_state().
maybe_shoot(GState = #{inflight := undefined, queue := Q}) ->
    case queue:is_empty(Q) of
        true ->
            GState;
        false ->
            {{value, {FunName, Req}}, Q1} = queue:out(Q),
            shoot(FunName, Req, GState#{queue => Q1})
    end.

-spec ack(atom(), grpc_client_state()) -> grpc_client_state().
ack(FunName, GState = #{inflight := FunName}) ->
    GState#{inflight => undefined, middleman => undefined};
ack(_, _) ->
    error(badarg).

-spec is_empty(grpc_client_state()) -> boolean().
is_empty(#{queue := Q, inflight := Inflight}) ->
    Inflight == undefined andalso queue:is_empty(Q).

%%--------------------------------------------------------------------
%% Internal funcs
%%--------------------------------------------------------------------

enqueue(FunName, Req, GState = #{queue := Q}) ->
    GState#{queue => queue:in({FunName, Req}, Q)}.

shoot(FunName, Req, GState) ->
    ServiceName = maps:get(service_name, GState),
    shoot(ServiceName, FunName, Req, GState).

shoot(
    'ConnectionUnaryHandler',
    FunName,
    Req,
    GState = #{owner := Owner, client_opts := Options}
) ->
    Pid =
        spawn(
            fun() ->
                try
                    Result = request(FunName, Req, Options),
                    hreply(Owner, Result, FunName)
                catch
                    T:R:Stk ->
                        hreply(Owner, {error, {{T, R}, Stk}}, FunName)
                end
            end
        ),
    GState#{inflight => FunName, middleman => Pid};
shoot(
    'ConnectionHandler',
    FunName,
    Req,
    GState
) ->
    GState1 = streaming(FunName, Req, GState),
    GState1#{inflight => FunName}.

%%--------------------------------------------------------------------
%% streaming

streaming(
    FunName,
    Req,
    GState = #{owner := Owner, client_opts := Options}
) ->
    Streams = maps:get(streams, GState, #{}),
    case ensure_stream_opened(FunName, Options, Streams) of
        {error, Reason} ->
            ?SLOG(error, #{
                msg => "request_grpc_server_failed",
                function => {FunName, Options},
                reason => Reason
            }),
            hreply(Owner, {error, Reason}, FunName),
            {ok, GState};
        {ok, Stream} ->
            case catch grpc_client:send(Stream, Req) of
                ok ->
                    ?SLOG(debug, #{
                        msg => "send_grpc_request_succeed",
                        function => FunName,
                        request => Req
                    }),
                    hreply(Owner, ok, FunName),
                    GState#{streams => Streams#{FunName => Stream}};
                {'EXIT', {not_found, _Stk}} ->
                    %% Not found the stream, reopen it
                    ?SLOG(info, #{
                        msg => "cannt_find_old_stream_ref",
                        function => FunName
                    }),
                    streaming(FunName, Req, GState#{streams => maps:remove(FunName, Streams)});
                {'EXIT', {timeout, _Stk}} ->
                    ?SLOG(error, #{
                        msg => "send_grpc_request_timeout",
                        function => FunName,
                        request => Req
                    }),
                    hreply(Owner, {error, timeout}, FunName),
                    GState;
                {'EXIT', {Reason1, Stk}} ->
                    ?SLOG(error, #{
                        msg => "send_grpc_request_failed",
                        function => FunName,
                        request => Req,
                        error => Reason1,
                        stacktrace => Stk
                    }),
                    hreply(Owner, {error, Reason1}, FunName),
                    GState
            end
    end.

ensure_stream_opened(FunName, Options, Streams) ->
    case maps:get(FunName, Streams, undefined) of
        undefined ->
            case apply(?CONN_HANDLER_MOD, FunName, [Options]) of
                {ok, Stream} -> {ok, Stream};
                {error, Reason} -> {error, Reason}
            end;
        Stream ->
            {ok, Stream}
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

hreply(Owner, Result, FunName) ->
    Owner ! {hreply, FunName, Result},
    ok.
