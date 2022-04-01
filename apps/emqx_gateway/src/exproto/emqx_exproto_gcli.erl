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

-behaviour(gen_server).

-include_lib("emqx/include/logger.hrl").

%% APIs
-export([async_call/3]).

-export([start_link/2]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-record(state, {
    pool,
    id,
    streams
}).

-define(CONN_ADAPTER_MOD, emqx_exproto_v_1_connection_handler_client).

%%--------------------------------------------------------------------
%% APIs
%%--------------------------------------------------------------------

start_link(Pool, Id) ->
    gen_server:start_link(
        {local, emqx_misc:proc_name(?MODULE, Id)},
        ?MODULE,
        [Pool, Id],
        []
    ).

async_call(
    FunName,
    Req = #{conn := Conn},
    Options = #{pool_name := PoolName}
) ->
    cast(pick(PoolName, Conn), {rpc, FunName, Req, Options, self()}).

%%--------------------------------------------------------------------
%% cast, pick
%%--------------------------------------------------------------------

-compile({inline, [cast/2, pick/2]}).

cast(Deliver, Msg) ->
    gen_server:cast(Deliver, Msg).

pick(PoolName, Conn) ->
    gproc_pool:pick_worker(PoolName, Conn).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([Pool, Id]) ->
    true = gproc_pool:connect_worker(Pool, {Pool, Id}),
    {ok, #state{pool = Pool, id = Id, streams = #{}}}.

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast({rpc, Fun, Req, Options, From}, State = #state{streams = Streams}) ->
    case ensure_stream_opened(Fun, Options, Streams) of
        {error, Reason} ->
            ?SLOG(error, #{
                msg => "request_grpc_server_failed",
                function => {?CONN_ADAPTER_MOD, Fun, Options},
                reason => Reason
            }),
            reply(From, Fun, {error, Reason}),
            {noreply, State#state{streams = Streams#{Fun => undefined}}};
        {ok, Stream} ->
            case catch grpc_client:send(Stream, Req) of
                ok ->
                    ?SLOG(debug, #{
                        msg => "send_grpc_request_succeed",
                        function => {?CONN_ADAPTER_MOD, Fun},
                        request => Req
                    }),
                    reply(From, Fun, ok),
                    {noreply, State#state{streams = Streams#{Fun => Stream}}};
                {'EXIT', {not_found, _Stk}} ->
                    %% Not found the stream, reopen it
                    ?SLOG(info, #{
                        msg => "cannt_find_old_stream_ref",
                        function => {?CONN_ADAPTER_MOD, Fun}
                    }),
                    handle_cast(
                        {rpc, Fun, Req, Options, From},
                        State#state{streams = maps:remove(Fun, Streams)}
                    );
                {'EXIT', {timeout, _Stk}} ->
                    ?SLOG(error, #{
                        msg => "send_grpc_request_timeout",
                        function => {?CONN_ADAPTER_MOD, Fun},
                        request => Req
                    }),
                    reply(From, Fun, {error, timeout}),
                    {noreply, State#state{streams = Streams#{Fun => Stream}}};
                {'EXIT', {Reason1, Stk}} ->
                    ?SLOG(error, #{
                        msg => "send_grpc_request_failed",
                        function => {?CONN_ADAPTER_MOD, Fun},
                        request => Req,
                        error => Reason1,
                        stacktrace => Stk
                    }),
                    reply(From, Fun, {error, Reason1}),
                    {noreply, State#state{streams = Streams#{Fun => undefined}}}
            end
    end.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% Internal funcs
%%--------------------------------------------------------------------

reply(Pid, Fun, Result) ->
    Pid ! {hreply, Fun, Result},
    ok.

ensure_stream_opened(Fun, Options, Streams) ->
    case maps:get(Fun, Streams, undefined) of
        undefined ->
            case apply(?CONN_ADAPTER_MOD, Fun, [Options]) of
                {ok, Stream} -> {ok, Stream};
                {error, Reason} -> {error, Reason}
            end;
        Stream ->
            {ok, Stream}
    end.
