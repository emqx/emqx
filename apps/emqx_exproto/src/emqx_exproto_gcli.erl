%%--------------------------------------------------------------------
%% Copyright (c) 2020-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-logger_header("[ExProto gClient]").

%% APIs
-export([async_call/3]).

-export([start_link/2]).

%% gen_server callbacks
-export([ init/1
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , terminate/2
        , code_change/3
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
    gen_server:start_link({local, emqx_misc:proc_name(?MODULE, Id)},
                          ?MODULE, [Pool, Id], []).

async_call(FunName, Req = #{conn := Conn}, Options) ->
    case pick(Conn) of
        false ->
            ?LOG(error, "No available grpc client for ~s: ~p",
                 [FunName, Req]);
        Pid when is_pid(Pid) ->
            cast(Pid, {rpc, FunName, Req, Options, self()})
    end.

%%--------------------------------------------------------------------
%% cast, pick
%%--------------------------------------------------------------------

-compile({inline, [cast/2, pick/1]}).

cast(Deliver, Msg) ->
    gen_server:cast(Deliver, Msg).

-spec pick(term()) -> pid() | false.
pick(Conn) ->
    gproc_pool:pick_worker(exproto_gcli_pool, Conn).

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
            ?LOG(error, "CALL ~0p:~0p(~0p) failed, reason: ~0p",
                    [?CONN_ADAPTER_MOD, Fun, Options, Reason]),
            reply(From, Fun, {error, Reason}),
            {noreply, State#state{streams = Streams#{Fun => undefined}}};
        {ok, Stream} ->
            case catch grpc_client:send(Stream, Req) of
                ok ->
                    ?LOG(debug, "Send to ~s method successfully, request: ~0p", [Fun, Req]),
                    reply(From, Fun, ok),
                    {noreply, State#state{streams = Streams#{Fun => Stream}}};
                {'EXIT', {not_found, _Stk}} ->
                    %% Not found the stream, reopen it
                    ?LOG(info, "Can not find the old stream ref for ~s; "
                               "re-try with a new stream!", [Fun]),
                    handle_cast({rpc, Fun, Req, Options, From},
                                State#state{streams = maps:remove(Fun, Streams)});
                {'EXIT', {timeout, _Stk}} ->
                    ?LOG(error, "Send to ~s method timeout, request: ~0p", [Fun, Req]),
                    reply(From, Fun, {error, timeout}),
                    {noreply, State#state{streams = Streams#{Fun => Stream}}};
                {'EXIT', {Reason1, _Stk}} ->
                    ?LOG(error, "Send to ~s method failure, request: ~0p, reason: ~p, "
                                "stacktrace: ~0p", [Fun, Req, Reason1, _Stk]),
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
        Stream -> {ok, Stream}
    end.
