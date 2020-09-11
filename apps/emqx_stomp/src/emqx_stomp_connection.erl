%%--------------------------------------------------------------------
%% Copyright (c) 2020 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-include("emqx_stomp.hrl").

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

-record(stomp_client, {transport, socket, peername, conn_name, conn_state,
                       await_recv, rate_limit, parse_fun, proto_state,
                       proto_env, heartbeat}).

-define(INFO_KEYS, [peername, await_recv, conn_state]).
-define(SOCK_STATS, [recv_oct, recv_cnt, send_oct, send_cnt]).

-define(LOG(Level, Format, Args, State),
        emqx_logger:Level("Stomp(~s): " ++ Format, [State#stomp_client.conn_name | Args])).

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
            SendFun = send_fun(Transport, Sock),
            ParseFun = emqx_stomp_frame:parser(ProtoEnv),
            ProtoState = emqx_stomp_protocol:init(Peername, SendFun, ProtoEnv),
            RateLimit = init_rate_limit(proplists:get_value(rate_limit, ProtoEnv)),
            State = run_socket(#stomp_client{transport   = Transport,
                                             socket      = NewSock,
                                             peername    = Peername,
                                             conn_name   = ConnName,
                                             conn_state  = running,
                                             await_recv  = false,
                                             rate_limit  = RateLimit,
                                             parse_fun   = ParseFun,
                                             proto_env   = ProtoEnv,
                                             proto_state = ProtoState}),
            gen_server:enter_loop(?MODULE, [{hibernate_after, 5000}], State, 20000);
        {error, Reason} ->
            {stop, Reason}
    end.

init_rate_limit(undefined) ->
    undefined;
init_rate_limit({Rate, Burst}) ->
    esockd_rate_limit:new(Rate, Burst).

send_fun(Transport, Sock) ->
    Self = self(),
    fun(Data) ->
        try Transport:async_send(Sock, Data) of
            ok -> ok;
            {error, Reason} -> Self ! {shutdown, Reason}
        catch
            error:Error -> Self ! {shutdown, Error}
        end
    end.

handle_call(info, _From, State = #stomp_client{transport   = Transport,
                                               socket      = Sock,
                                               peername    = Peername,
                                               await_recv  = AwaitRecv,
                                               conn_state  = ConnState,
                                               proto_state = ProtoState}) ->
    ClientInfo = [{peername,  Peername}, {await_recv, AwaitRecv},
                  {conn_state, ConnState}],
    ProtoInfo  = emqx_stomp_protocol:info(ProtoState),
    case Transport:getstat(Sock, ?SOCK_STATS) of
        {ok, SockStats} ->
            {reply, lists:append([ClientInfo, ProtoInfo, SockStats]), State};
        {error, Reason} ->
            {stop, Reason, lists:append([ClientInfo, ProtoInfo]), State}
    end;

handle_call(Req, _From, State) ->
    ?LOG(error, "unexpected request: ~p", [Req], State),
    {reply, ignored, State}.

handle_cast(Msg, State) ->
    ?LOG(error, "unexpected msg: ~p", [Msg], State),
    noreply(State).

handle_info(timeout, State) ->
    shutdown(idle_timeout, State);

handle_info({shutdown, Reason}, State) ->
    shutdown(Reason, State);

handle_info({transaction, {timeout, Id}}, State) ->
    emqx_stomp_transaction:timeout(Id),
    noreply(State);

handle_info({heartbeat, start, {Cx, Cy}},
            State = #stomp_client{transport = Transport, socket = Sock}) ->
    Self = self(),
    Incomming = {Cx, statfun(recv_oct, State), fun() -> Self ! {heartbeat, timeout} end},
    Outgoing  = {Cy, statfun(send_oct, State), fun() -> Transport:send(Sock, <<$\n>>) end},
    {ok, HbProc} = emqx_stomp_heartbeat:start_link(Incomming, Outgoing),
    noreply(State#stomp_client{heartbeat = HbProc});

handle_info({heartbeat, timeout}, State) ->
    stop({shutdown, heartbeat_timeout}, State);

handle_info({'EXIT', HbProc, Error}, State = #stomp_client{heartbeat = HbProc}) ->
    stop(Error, State);

handle_info(activate_sock, State) ->
    noreply(run_socket(State#stomp_client{conn_state = running}));

handle_info({inet_async, _Sock, _Ref, {ok, Bytes}}, State) ->
    ?LOG(debug, "RECV ~p", [Bytes], State),
    received(Bytes, rate_limit(size(Bytes), State#stomp_client{await_recv = false}));

handle_info({inet_async, _Sock, _Ref, {error, Reason}}, State) ->
    shutdown(Reason, State);

handle_info({inet_reply, _Ref, ok}, State) ->
    noreply(State);

handle_info({inet_reply, _Sock, {error, Reason}}, State) ->
    shutdown(Reason, State);

handle_info({deliver, _Topic, Msg}, State = #stomp_client{proto_state = ProtoState}) ->
    noreply(State#stomp_client{proto_state = case emqx_stomp_protocol:send(Msg, ProtoState) of 
                                                 {ok, ProtoState1} ->
                                                     ProtoState1;
                                                 {error, dropped, ProtoState1} ->
                                                     ProtoState1
                                             end});

handle_info(Info, State) ->
    ?LOG(error, "Unexpected info: ~p", [Info], State),
    noreply(State).

terminate(Reason, State = #stomp_client{transport   = Transport,
                                        socket      = Sock,
                                        proto_state = ProtoState}) ->
    ?LOG(info, "terminated for ~p", [Reason], State),
    Transport:fast_close(Sock),
    case {ProtoState, Reason} of
        {undefined, _} -> ok;
        {_, {shutdown, Error}} ->
            emqx_stomp_protocol:shutdown(Error, ProtoState);
        {_,  Reason} ->
            emqx_stomp_protocol:shutdown(Reason, ProtoState)
    end.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% Receive and Parse data
%%--------------------------------------------------------------------

received(<<>>, State) ->
    noreply(State);

received(Bytes, State = #stomp_client{parse_fun   = ParseFun,
                                      proto_state = ProtoState}) ->
    try ParseFun(Bytes) of
        {more, NewParseFun} ->
            noreply(State#stomp_client{parse_fun = NewParseFun});
        {ok, Frame, Rest} ->
            ?LOG(info, "RECV Frame: ~s", [emqx_stomp_frame:format(Frame)], State),
            case emqx_stomp_protocol:received(Frame, ProtoState) of
                {ok, ProtoState1}           ->
                    received(Rest, reset_parser(State#stomp_client{proto_state = ProtoState1}));
                {error, Error, ProtoState1} ->
                    shutdown(Error, State#stomp_client{proto_state = ProtoState1});
                {stop, Reason, ProtoState1} ->
                    stop(Reason, State#stomp_client{proto_state = ProtoState1})
            end;
        {error, Error} ->
            ?LOG(error, "Framing error - ~s", [Error], State),
            ?LOG(error, "Bytes: ~p", [Bytes], State),
            shutdown(frame_error, State)
    catch
        _Error:Reason ->
            ?LOG(error, "Parser failed for ~p", [Reason], State),
            ?LOG(error, "Error data: ~p", [Bytes], State),
            shutdown(parse_error, State)
    end.

reset_parser(State = #stomp_client{proto_env = ProtoEnv}) ->
    State#stomp_client{parse_fun = emqx_stomp_frame:parser(ProtoEnv)}.

rate_limit(_Size, State = #stomp_client{rate_limit = undefined}) ->
    run_socket(State);
rate_limit(Size, State = #stomp_client{rate_limit = Rl}) ->
    case esockd_rate_limit:check(Size, Rl) of
        {0, Rl1} ->
            run_socket(State#stomp_client{conn_state = running, rate_limit = Rl1});
        {Pause, Rl1} ->
            ?LOG(error, "Rate limiter pause for ~p", [Pause], State),
            erlang:send_after(Pause, self(), activate_sock),
            State#stomp_client{conn_state = blocked, rate_limit = Rl1}
    end.

run_socket(State = #stomp_client{conn_state = blocked}) ->
    State;
run_socket(State = #stomp_client{await_recv = true}) ->
    State;
run_socket(State = #stomp_client{transport = Transport, socket = Sock}) ->
    Transport:async_recv(Sock, 0, infinity),
    State#stomp_client{await_recv = true}.

statfun(Stat, #stomp_client{transport = Transport, socket = Sock}) ->
    fun() ->
        case Transport:getstat(Sock, [Stat]) of
            {ok, [{Stat, Val}]} -> {ok, Val};
            {error, Error}      -> {error, Error}
        end
    end.

noreply(State) ->
    {noreply, State}.

stop(Reason, State) ->
    {stop, Reason, State}.

shutdown(Reason, State) ->
    stop({shutdown, Reason}, State).

