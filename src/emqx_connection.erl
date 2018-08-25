%% Copyright (c) 2018 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_connection).

-behaviour(gen_server).

-include("emqx.hrl").
-include("emqx_mqtt.hrl").

-export([start_link/3]).
-export([info/1, stats/1, kick/1]).
-export([session/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-record(state, {
          transport,
          socket,
          peername,
          sockname,
          conn_state,
          await_recv,
          proto_state,
          parser_state,
          keepalive,
          enable_stats,
          stats_timer,
          incoming,
          rate_limit,
          publish_limit,
          limit_timer,
          idle_timeout
         }).

-define(SOCK_STATS, [recv_oct, recv_cnt, send_oct, send_cnt, send_pend]).

-define(LOG(Level, Format, Args, State),
        emqx_logger:Level("Client(~s): " ++ Format,
                          [esockd_net:format(State#state.peername) | Args])).

start_link(Transport, Socket, Options) ->
    {ok, proc_lib:spawn_link(?MODULE, init, [[Transport, Socket, Options]])}.

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

info(CPid) ->
    call(CPid, info).

stats(CPid) ->
    call(CPid, stats).

kick(CPid) ->
    call(CPid, kick).

session(CPid) ->
    call(CPid, session).

call(CPid, Req) ->
    gen_server:call(CPid, Req, infinity).

%%------------------------------------------------------------------------------
%% gen_server callbacks
%%------------------------------------------------------------------------------

init([Transport, RawSocket, Options]) ->
    case Transport:wait(RawSocket) of
        {ok, Socket} ->
            Zone = proplists:get_value(zone, Options),
            {ok, Peername} = Transport:ensure_ok_or_exit(peername, [Socket]),
            {ok, Sockname} = Transport:ensure_ok_or_exit(sockname, [Socket]),
            Peercert = Transport:ensure_ok_or_exit(peercert, [Socket]),
            RateLimit = init_limiter(proplists:get_value(rate_limit, Options)),
            PubLimit = init_limiter(emqx_zone:get_env(Zone, publish_limit)),
            EnableStats = emqx_zone:get_env(Zone, enable_stats, true),
            IdleTimout = emqx_zone:get_env(Zone, idle_timeout, 30000),
            SendFun = send_fun(Transport, Socket, Peername),
            ProtoState = emqx_protocol:init(#{peername => Peername,
                                              sockname => Sockname,
                                              peercert => Peercert,
                                              sendfun  => SendFun}, Options),
            ParserState = emqx_protocol:parser(ProtoState),
            State = run_socket(#state{transport     = Transport,
                                       socket        = Socket,
                                       peername      = Peername,
                                       await_recv    = false,
                                       conn_state    = running,
                                       rate_limit    = RateLimit,
                                       publish_limit = PubLimit,
                                       proto_state   = ProtoState,
                                       parser_state  = ParserState,
                                       enable_stats  = EnableStats,
                                       idle_timeout  = IdleTimout}),
            gen_server:enter_loop(?MODULE, [{hibernate_after, IdleTimout}],
                                  State, self(), IdleTimout);
        {error, Reason} ->
            {stop, Reason}
    end.

init_limiter(undefined) ->
    undefined;
init_limiter({Rate, Burst}) ->
    esockd_rate_limit:new(Rate, Burst).

send_fun(Transport, Socket, Peername) ->
    fun(Data) ->
        try Transport:async_send(Socket, Data) of
            ok ->
                ?LOG(debug, "SEND ~p", [iolist_to_binary(Data)], #state{peername = Peername}),
                emqx_metrics:inc('bytes/sent', iolist_size(Data)),
                ok;
            Error -> Error
        catch
            error:Error ->
                {error, Error}
        end
    end.

handle_call(info, _From, State = #state{transport     = Transport,
                                        socket        = Socket,
                                        peername      = Peername,
                                        sockname      = Sockname,
                                        conn_state    = ConnState,
                                        await_recv    = AwaitRecv,
                                        rate_limit    = RateLimit,
                                        publish_limit = PubLimit,
                                        proto_state   = ProtoState}) ->
    ConnInfo = [{socktype, Transport:type(Socket)},
                {peername, Peername},
                {sockname, Sockname},
                {conn_state, ConnState},
                {await_recv, AwaitRecv},
                {rate_limit, esockd_rate_limit:info(RateLimit)},
                {publish_limit, esockd_rate_limit:info(PubLimit)}],
    ProtoInfo = emqx_protocol:info(ProtoState),
    {reply, lists:usort(lists:append([ConnInfo, ProtoInfo])), State};

handle_call(stats, _From, State = #state{transport   = Transport,
                                          socket      = Socket,
                                          proto_state = ProtoState}) ->
    ProcStats = emqx_misc:proc_stats(),
    ProtoStats = emqx_protocol:stats(ProtoState),
    SockStats = case Transport:getstat(Socket, ?SOCK_STATS) of
                    {ok, Ss}   -> Ss;
                    {error, _} -> []
                end,
    {reply, lists:append([ProcStats, ProtoStats, SockStats]), State};

handle_call(kick, _From, State) ->
    {stop, {shutdown, kick}, ok, State};

handle_call(session, _From, State = #state{proto_state = ProtoState}) ->
    {reply, emqx_protocol:session(ProtoState), State};

handle_call(Req, _From, State) ->
    ?LOG(error, "unexpected call: ~p", [Req], State),
    {reply, ignored, State}.

handle_cast(Msg, State) ->
    ?LOG(error, "unexpected cast: ~p", [Msg], State),
    {noreply, State}.

handle_info({deliver, PubOrAck}, State = #state{proto_state = ProtoState}) ->
    case emqx_protocol:deliver(PubOrAck, ProtoState) of
        {ok, ProtoState1} ->
            {noreply, maybe_gc(ensure_stats_timer(State#state{proto_state = ProtoState1}))};
        {error, Reason} ->
            shutdown(Reason, State);
        {error, Reason, ProtoState1} ->
            shutdown(Reason, State#state{proto_state = ProtoState1})
    end;

handle_info(emit_stats, State = #state{proto_state = ProtoState}) ->
    Stats = element(2, handle_call(stats, undefined, State)),
    emqx_cm:set_client_stats(emqx_protocol:client_id(ProtoState), Stats),
    {noreply, State#state{stats_timer = undefined}, hibernate};

handle_info(timeout, State) ->
    shutdown(idle_timeout, State);

handle_info({shutdown, Error}, State) ->
    shutdown(Error, State);

handle_info({shutdown, conflict, {ClientId, NewPid}}, State) ->
    ?LOG(warning, "clientid '~s' conflict with ~p", [ClientId, NewPid], State),
    shutdown(conflict, State);

handle_info(activate_sock, State) ->
    {noreply, run_socket(State#state{conn_state = running, limit_timer = undefined})};

handle_info({inet_async, _Sock, _Ref, {ok, Data}}, State) ->
    ?LOG(debug, "RECV ~p", [Data], State),
    Size = iolist_size(Data),
    emqx_metrics:inc('bytes/received', Size),
    Incoming = #{bytes => Size, packets => 0},
    handle_packet(Data, State#state{await_recv = false, incoming = Incoming});

handle_info({inet_async, _Sock, _Ref, {error, Reason}}, State) ->
    shutdown(Reason, State);

handle_info({inet_reply, _Sock, ok}, State) ->
    {noreply, State};

handle_info({inet_reply, _Sock, {error, Reason}}, State) ->
    shutdown(Reason, State);

handle_info({keepalive, start, Interval}, State = #state{transport = Transport, socket = Sock}) ->
    ?LOG(debug, "Keepalive at the interval of ~p", [Interval], State),
    StatFun = fun() ->
                case Transport:getstat(Sock, [recv_oct]) of
                    {ok, [{recv_oct, RecvOct}]} -> {ok, RecvOct};
                    Error                       -> Error
                end
             end,
    case emqx_keepalive:start(StatFun, Interval, {keepalive, check}) of
        {ok, KeepAlive} ->
            {noreply, State#state{keepalive = KeepAlive}};
        {error, Error} ->
            shutdown(Error, State)
    end;

handle_info({keepalive, check}, State = #state{keepalive = KeepAlive}) ->
    case emqx_keepalive:check(KeepAlive) of
        {ok, KeepAlive1} ->
            {noreply, State#state{keepalive = KeepAlive1}};
        {error, timeout} ->
            shutdown(keepalive_timeout, State);
        {error, Error} ->
            shutdown(Error, State)
    end;

handle_info(Info, State) ->
    ?LOG(error, "unexpected info: ~p", [Info], State),
    {noreply, State}.

terminate(Reason, State = #state{transport   = Transport,
                                 socket      = Sock,
                                 keepalive   = KeepAlive,
                                 proto_state = ProtoState}) ->
    ?LOG(debug, "Terminated for ~p", [Reason], State),
    Transport:fast_close(Sock),
    emqx_keepalive:cancel(KeepAlive),
    case {ProtoState, Reason} of
        {undefined, _} -> ok;
        {_, {shutdown, Error}} ->
            emqx_protocol:shutdown(Error, ProtoState);
        {_, Reason} ->
            emqx_protocol:shutdown(Reason, ProtoState)
    end.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%------------------------------------------------------------------------------
%% Parse and handle packets
%%------------------------------------------------------------------------------

%% Receive and parse data
handle_packet(<<>>, State) ->
    {noreply, maybe_gc(ensure_stats_timer(ensure_rate_limit(State)))};

handle_packet(Data, State = #state{proto_state  = ProtoState,
                                   parser_state = ParserState,
                                   idle_timeout = IdleTimeout}) ->
    case catch emqx_frame:parse(Data, ParserState) of
        {more, NewParserState} ->
            {noreply, State#state{parser_state = NewParserState}, IdleTimeout};
        {ok, Packet = ?PACKET(Type), Rest} ->
            emqx_metrics:received(Packet),
            case emqx_protocol:received(Packet, ProtoState) of
                {ok, ProtoState1} ->
                    NewState = State#state{proto_state = ProtoState1},
                    handle_packet(Rest, inc_publish_cnt(Type, reset_parser(NewState)));
                {error, Error} ->
                    ?LOG(error, "Protocol error - ~p", [Error], State),
                    shutdown(Error, State);
                {error, Error, ProtoState1} ->
                    shutdown(Error, State#state{proto_state = ProtoState1});
                {stop, Reason, ProtoState1} ->
                    stop(Reason, State#state{proto_state = ProtoState1})
            end;
        {error, Error} ->
            ?LOG(error, "Framing error - ~p", [Error], State),
            shutdown(Error, State);
        {'EXIT', Reason} ->
            ?LOG(error, "Parse failed for ~p~nError data:~p", [Reason, Data], State),
            shutdown(parse_error, State)
    end.

reset_parser(State = #state{proto_state = ProtoState}) ->
    State#state{parser_state = emqx_protocol:parser(ProtoState)}.

inc_publish_cnt(Type, State = #state{incoming = Incoming = #{packets := Cnt}})
    when Type == ?PUBLISH; Type == ?SUBSCRIBE ->
    State#state{incoming = Incoming#{packets := Cnt + 1}};
inc_publish_cnt(_Type, State) ->
    State.

%%------------------------------------------------------------------------------
%% Ensure rate limit
%%------------------------------------------------------------------------------

ensure_rate_limit(State = #state{rate_limit = Rl, publish_limit = Pl,
                                 incoming = #{packets := Packets, bytes := Bytes}}) ->
    ensure_rate_limit([{Pl, #state.publish_limit, Packets},
                       {Rl, #state.rate_limit, Bytes}], State).

ensure_rate_limit([], State) ->
    run_socket(State);
ensure_rate_limit([{undefined, _Pos, _Num}|Limiters], State) ->
    ensure_rate_limit(Limiters, State);
ensure_rate_limit([{Rl, Pos, Num}|Limiters], State) ->
   case esockd_rate_limit:check(Num, Rl) of
       {0, Rl1} ->
           ensure_rate_limit(Limiters, setelement(Pos, State, Rl1));
       {Pause, Rl1} ->
           TRef = erlang:send_after(Pause, self(), activate_sock),
           setelement(Pos, State#state{conn_state = blocked, limit_timer = TRef}, Rl1)
   end.

run_socket(State = #state{conn_state = blocked}) ->
    State;
run_socket(State = #state{await_recv = true}) ->
    State;
run_socket(State = #state{transport = Transport, socket = Sock}) ->
    Transport:async_recv(Sock, 0, infinity),
    State#state{await_recv = true}.

%%------------------------------------------------------------------------------
%% Ensure stats timer
%%------------------------------------------------------------------------------

ensure_stats_timer(State = #state{enable_stats = true,
                                   stats_timer  = undefined,
                                   idle_timeout = IdleTimeout}) ->
    State#state{stats_timer = erlang:send_after(IdleTimeout, self(), emit_stats)};
ensure_stats_timer(State) -> State.

shutdown(Reason, State) ->
    stop({shutdown, Reason}, State).

stop(Reason, State) ->
    {stop, Reason, State}.

maybe_gc(State) ->
    %% TODO: gc and shutdown policy
    State.

