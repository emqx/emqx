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
-export([ new/1
        , async_call/3
        ]).

-define(CONN_HANDLER_MOD, emqx_exproto_v_1_connection_handler_client).

-type state() :: #{client_opts := map(),
                   streams := map()
                  }.

%%--------------------------------------------------------------------
%% APIs
%%--------------------------------------------------------------------

-spec new(map()) -> state().
new(Options) ->
    #{client_opts => Options, streams => #{}}.

-spec async_call(atom(), map(), state()) -> {ok, state()} | {error, term()}.
async_call(FunName, Req,
           State = #{client_opts := Options, streams := Streams}) ->
    case ensure_stream_opened(FunName, Options, Streams) of
        {error, Reason} ->
            ?LOG(error, "CALL ~0p:~0p(~0p) failed, reason: ~0p",
                    [?CONN_HANDLER_MOD, FunName, Options, Reason]),
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
                    async_call(FunName, Req, State#{streams => maps:remove(FunName, Streams)});
                {'EXIT', {timeout, _Stk}} ->
                    ?LOG(error, "Send to ~s method timeout, request: ~0p", [FunName, Req]),
                    {error, timeout};
                {'EXIT', {Reason1, _Stk}} ->
                    ?LOG(error, "Send to ~s method failure, request: ~0p, reason: ~p, "
                                "stacktrace: ~0p", [FunName, Req, Reason1, _Stk]),
                    {error, Reason1}
            end
    end.

%%--------------------------------------------------------------------
%% Internal funcs
%%--------------------------------------------------------------------

ensure_stream_opened(FunName, Options, Streams) ->
    case maps:get(FunName, Streams, undefined) of
        undefined ->
            case apply(?CONN_HANDLER_MOD, FunName, [Options]) of
                {ok, Stream} -> {ok, Stream};
                {error, Reason} -> {error, Reason}
            end;
        Stream -> {ok, Stream}
    end.
