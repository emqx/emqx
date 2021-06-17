%%--------------------------------------------------------------------
%% Copyright (c) 2020-2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_stomp_connection).

-behaviour(gen_server).

-include("src/stomp/include/emqx_stomp.hrl").
-include_lib("emqx/include/logger.hrl").

-logger_header("[Stomp-Conn]").

-export([ start_link/3
        , info/1
        ]).

%% gen_server Function Exports
-export([ init/1
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , code_change/3
        , terminate/2
        ]).

%% for protocol
-export([send/4, heartbeat/2]).

-record(state, {transport, socket, peername, conn_name, conn_state,
                await_recv, rate_limit, parser, pstate,
                proto_env, heartbeat}).

-define(INFO_KEYS, [peername, await_recv, conn_state]).
-define(SOCK_STATS, [recv_oct, recv_cnt, send_oct, send_cnt]).

start_link(Transport, Sock, ProtoEnv) ->
    {ok, proc_lib:spawn_link(?MODULE, init, [[Transport, Sock, ProtoEnv]])}.

info(CPid) ->
    gen_server:call(CPid, info, infinity).

init([Transport, Sock, ProtoEnv]) ->
    process_flag(trap_exit, true),
    case Transport:wait(Sock) of
        {ok, NewSock} ->
            {ok, Peername} = Transport:ensure_ok_or_exit(peername, [NewSock]),
            ConnName = esockd:format(Peername),
            SendFun = {fun ?MODULE:send/4, [Transport, Sock, self()]},
            HrtBtFun = {fun ?MODULE:heartbeat/2, [Transport, Sock]},
            Parser = emqx_stomp_frame:init_parer_state(maps:get(frame, ProtoEnv)),
            PState = emqx_stomp_protocol:init(#{peername => Peername,
                                                sendfun => SendFun,
                                                heartfun => HrtBtFun}, ProtoEnv),
            RateLimit = init_rate_limit(maps:get(rate_limit, ProtoEnv, undefined)),
            State = run_socket(#state{transport   = Transport,
                                      socket      = NewSock,
                                      peername    = Peername,
                                      conn_name   = ConnName,
                                      conn_state  = running,
                                      await_recv  = false,
                                      rate_limit  = RateLimit,
                                      parser      = Parser,
                                      proto_env   = ProtoEnv,
                                      pstate      = PState}),
            emqx_logger:set_metadata_peername(esockd:format(Peername)),
            gen_server:enter_loop(?MODULE, [{hibernate_after, 5000}], State, 20000);
        {error, Reason} ->
            {stop, Reason}
    end.

init_rate_limit(undefined) ->
    undefined;
init_rate_limit({Rate, Burst}) ->
    esockd_rate_limit:new(Rate, Burst).

send(Data, Transport, Sock, ConnPid) ->
    try Transport:async_send(Sock, Data) of
        ok -> ok;
        {error, Reason} -> ConnPid ! {shutdown, Reason}
    catch
        error:Error -> ConnPid ! {shutdown, Error}
    end.

heartbeat(Transport, Sock) ->
    Transport:send(Sock, <<$\n>>).

handle_call(info, _From, State = #state{transport   = Transport,
                                        socket      = Sock,
                                        peername    = Peername,
                                        await_recv  = AwaitRecv,
                                        conn_state  = ConnState,
                                        pstate      = PState}) ->
    ClientInfo = [{peername,  Peername}, {await_recv, AwaitRecv},
                  {conn_state, ConnState}],
    ProtoInfo  = emqx_stomp_protocol:info(PState),
    case Transport:getstat(Sock, ?SOCK_STATS) of
        {ok, SockStats} ->
            {reply, lists:append([ClientInfo, ProtoInfo, SockStats]), State};
        {error, Reason} ->
            {stop, Reason, lists:append([ClientInfo, ProtoInfo]), State}
    end;

handle_call(Req, _From, State) ->
    ?LOG(error, "unexpected request: ~p", [Req]),
    {reply, ignored, State}.

handle_cast(Msg, State) ->
    ?LOG(error, "unexpected msg: ~p", [Msg]),
    noreply(State).

handle_info(timeout, State) ->
    shutdown(idle_timeout, State);

handle_info({shutdown, Reason}, State) ->
    shutdown(Reason, State);

handle_info({timeout, TRef, TMsg}, State) when TMsg =:= incoming;
                                               TMsg =:= outgoing ->

    Stat = case TMsg of
               incoming -> recv_oct;
               _ -> send_oct
           end,
    case getstat(Stat, State) of
        {ok, Val} ->
            with_proto(timeout, [TRef, {TMsg, Val}], State);
        {error, Reason} ->
            shutdown({sock_error, Reason}, State)
    end;

handle_info({timeout, TRef, TMsg}, State) ->
    with_proto(timeout, [TRef, TMsg], State);

handle_info({'EXIT', HbProc, Error}, State = #state{heartbeat = HbProc}) ->
    stop(Error, State);

handle_info(activate_sock, State) ->
    noreply(run_socket(State#state{conn_state = running}));

handle_info({inet_async, _Sock, _Ref, {ok, Bytes}}, State) ->
    ?LOG(debug, "RECV ~p", [Bytes]),
    received(Bytes, rate_limit(size(Bytes), State#state{await_recv = false}));

handle_info({inet_async, _Sock, _Ref, {error, Reason}}, State) ->
    shutdown(Reason, State);

handle_info({inet_reply, _Ref, ok}, State) ->
    noreply(State);

handle_info({inet_reply, _Sock, {error, Reason}}, State) ->
    shutdown(Reason, State);

handle_info({deliver, _Topic, Msg}, State = #state{pstate = PState}) ->
    noreply(State#state{pstate = case emqx_stomp_protocol:send(Msg, PState) of
                                     {ok, PState1} ->
                                         PState1;
                                     {error, dropped, PState1} ->
                                         PState1
                                 end});

handle_info(Info, State) ->
    ?LOG(error, "Unexpected info: ~p", [Info]),
    noreply(State).

terminate(Reason, #state{transport = Transport,
                         socket    = Sock,
                         pstate    = PState}) ->
    ?LOG(info, "terminated for ~p", [Reason]),
    Transport:fast_close(Sock),
    case {PState, Reason} of
        {undefined, _} -> ok;
        {_, {shutdown, Error}} ->
            emqx_stomp_protocol:shutdown(Error, PState);
        {_,  Reason} ->
            emqx_stomp_protocol:shutdown(Reason, PState)
    end.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% Receive and Parse data
%%--------------------------------------------------------------------

with_proto(Fun, Args, State = #state{pstate = PState}) ->
    case erlang:apply(emqx_stomp_protocol, Fun, Args ++ [PState]) of
        {ok, NPState} ->
            noreply(State#state{pstate = NPState});
        {F, Reason, NPState} when F == stop;
                                  F == error;
                                  F == shutdown ->
            shutdown(Reason, State#state{pstate = NPState})
    end.

received(<<>>, State) ->
    noreply(State);

received(Bytes, State = #state{parser   = Parser,
                               pstate = PState}) ->
    try emqx_stomp_frame:parse(Bytes, Parser) of
        {more, NewParser} ->
            noreply(State#state{parser = NewParser});
        {ok, Frame, Rest} ->
            ?LOG(info, "RECV Frame: ~s", [emqx_stomp_frame:format(Frame)]),
            case emqx_stomp_protocol:received(Frame, PState) of
                {ok, PState1}           ->
                    received(Rest, reset_parser(State#state{pstate = PState1}));
                {error, Error, PState1} ->
                    shutdown(Error, State#state{pstate = PState1});
                {stop, Reason, PState1} ->
                    stop(Reason, State#state{pstate = PState1})
            end;
        {error, Error} ->
            ?LOG(error, "Framing error - ~s", [Error]),
            ?LOG(error, "Bytes: ~p", [Bytes]),
            shutdown(frame_error, State)
    catch
        _Error:Reason ->
            ?LOG(error, "Parser failed for ~p", [Reason]),
            ?LOG(error, "Error data: ~p", [Bytes]),
            shutdown(parse_error, State)
    end.

reset_parser(State = #state{proto_env = ProtoEnv}) ->
    Parser = emqx_stomp_frame:init_parer_state(maps:get(frame, ProtoEnv)),
    State#state{parser = Parser}.

rate_limit(_Size, State = #state{rate_limit = undefined}) ->
    run_socket(State);
rate_limit(Size, State = #state{rate_limit = Rl}) ->
    case esockd_rate_limit:check(Size, Rl) of
        {0, Rl1} ->
            run_socket(State#state{conn_state = running, rate_limit = Rl1});
        {Pause, Rl1} ->
            ?LOG(error, "Rate limiter pause for ~p", [Pause]),
            erlang:send_after(Pause, self(), activate_sock),
            State#state{conn_state = blocked, rate_limit = Rl1}
    end.

run_socket(State = #state{conn_state = blocked}) ->
    State;
run_socket(State = #state{await_recv = true}) ->
    State;
run_socket(State = #state{transport = Transport, socket = Sock}) ->
    Transport:async_recv(Sock, 0, infinity),
    State#state{await_recv = true}.

getstat(Stat, #state{transport = Transport, socket = Sock}) ->
    case Transport:getstat(Sock, [Stat]) of
        {ok, [{Stat, Val}]} -> {ok, Val};
        {error, Error}      -> {error, Error}
    end.

noreply(State) ->
    {noreply, State}.

stop(Reason, State) ->
    {stop, Reason, State}.

shutdown(Reason, State) ->
    stop({shutdown, Reason}, State).

