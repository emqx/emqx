%% Copyright (c) 2013-2019 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-include("logger.hrl").

-export([start_link/3]).
-export([info/1, attrs/1, stats/1]).
-export([kick/1]).
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
          active_n,
          proto_state,
          parser_state,
          gc_state,
          keepalive,
          enable_stats,
          stats_timer,
          rate_limit,
          pub_limit,
          limit_timer,
          idle_timeout
         }).

-define(DEFAULT_ACTIVE_N, 100).
-define(SOCK_STATS, [recv_oct, recv_cnt, send_oct, send_cnt, send_pend]).

start_link(Transport, Socket, Options) ->
    {ok, proc_lib:spawn_link(?MODULE, init, [[Transport, Socket, Options]])}.

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

%% for debug
info(CPid) when is_pid(CPid) ->
    call(CPid, info);

info(#state{transport   = Transport,
            socket      = Socket,
            peername    = Peername,
            sockname    = Sockname,
            conn_state  = ConnState,
            active_n    = ActiveN,
            rate_limit  = RateLimit,
            pub_limit   = PubLimit,
            proto_state = ProtoState}) ->
    ConnInfo = [{socktype, Transport:type(Socket)},
                {peername, Peername},
                {sockname, Sockname},
                {conn_state, ConnState},
                {active_n, ActiveN},
                {rate_limit, rate_limit_info(RateLimit)},
                {pub_limit, rate_limit_info(PubLimit)}],
    ProtoInfo = emqx_protocol:info(ProtoState),
    lists:usort(lists:append(ConnInfo, ProtoInfo)).

rate_limit_info(undefined) -> #{};
rate_limit_info(Limit) -> esockd_rate_limit:info(Limit).

%% for dashboard
attrs(CPid) when is_pid(CPid) ->
    call(CPid, attrs);

attrs(#state{peername = Peername,
             sockname = Sockname,
             proto_state = ProtoState}) ->
    SockAttrs = [{peername, Peername},
                 {sockname, Sockname}],
    ProtoAttrs = emqx_protocol:attrs(ProtoState),
    lists:usort(lists:append(SockAttrs, ProtoAttrs)).

%% Conn stats
stats(CPid) when is_pid(CPid) ->
    call(CPid, stats);

stats(#state{transport   = Transport,
             socket      = Socket,
             proto_state = ProtoState}) ->
    lists:append([emqx_misc:proc_stats(),
                  emqx_protocol:stats(ProtoState),
                  case Transport:getstat(Socket, ?SOCK_STATS) of
                      {ok, Ss}   -> Ss;
                      {error, _} -> []
                  end]).

kick(CPid) -> call(CPid, kick).

session(CPid) -> call(CPid, session).

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
            ActiveN = proplists:get_value(active_n, Options, ?DEFAULT_ACTIVE_N),
            EnableStats = emqx_zone:get_env(Zone, enable_stats, true),
            IdleTimout = emqx_zone:get_env(Zone, idle_timeout, 30000),
            SendFun = send_fun(Transport, Socket),
            ProtoState = emqx_protocol:init(#{peername => Peername,
                                              sockname => Sockname,
                                              peercert => Peercert,
                                              sendfun  => SendFun}, Options),
            ParserState = emqx_protocol:parser(ProtoState),
            GcPolicy = emqx_zone:get_env(Zone, force_gc_policy, false),
            GcState = emqx_gc:init(GcPolicy),
            State = run_socket(#state{transport    = Transport,
                                      socket       = Socket,
                                      peername     = Peername,
                                      conn_state   = running,
                                      active_n     = ActiveN,
                                      rate_limit   = RateLimit,
                                      pub_limit    = PubLimit,
                                      proto_state  = ProtoState,
                                      parser_state = ParserState,
                                      gc_state     = GcState,
                                      enable_stats = EnableStats,
                                      idle_timeout = IdleTimout
                                     }),
            ok = emqx_misc:init_proc_mng_policy(Zone),
            emqx_logger:set_metadata_peername(esockd_net:format(Peername)),
            gen_server:enter_loop(?MODULE, [{hibernate_after, IdleTimout}],
                                  State, self(), IdleTimout);
        {error, Reason} ->
            {stop, Reason}
    end.

init_limiter(undefined) ->
    undefined;
init_limiter({Rate, Burst}) ->
    esockd_rate_limit:new(Rate, Burst).

send_fun(Transport, Socket) ->
    fun(Packet, Options) ->
        Data = emqx_frame:serialize(Packet, Options),
        try Transport:async_send(Socket, Data) of
            ok ->
                emqx_metrics:trans(inc, 'bytes/sent', iolist_size(Data)),
                ok;
            Error -> Error
        catch
            error:Error ->
                {error, Error}
        end
    end.

handle_call(info, _From, State) ->
    {reply, info(State), State};

handle_call(attrs, _From, State) ->
    {reply, attrs(State), State};

handle_call(stats, _From, State) ->
    {reply, stats(State), State};

handle_call(kick, _From, State) ->
    {stop, {shutdown, kicked}, ok, State};

handle_call(session, _From, State = #state{proto_state = ProtoState}) ->
    {reply, emqx_protocol:session(ProtoState), State};

handle_call(Req, _From, State) ->
    ?LOG(error, "unexpected call: ~p", [Req]),
    {reply, ignored, State}.

handle_cast(Msg, State) ->
    ?LOG(error, "unexpected cast: ~p", [Msg]),
    {noreply, State}.

handle_info({deliver, PubOrAck}, State = #state{proto_state = ProtoState}) ->
    case emqx_protocol:deliver(PubOrAck, ProtoState) of
        {ok, ProtoState1} ->
            State1 = State#state{proto_state = ProtoState1},
            {noreply, maybe_gc(PubOrAck, ensure_stats_timer(State1))};
        {error, Reason} ->
            shutdown(Reason, State)
    end;

handle_info({timeout, Timer, emit_stats},
            State = #state{stats_timer = Timer,
                           proto_state = ProtoState,
                           gc_state = GcState}) ->
    emqx_metrics:commit(),
    emqx_cm:set_conn_stats(emqx_protocol:client_id(ProtoState), stats(State)),
    NewState = State#state{stats_timer = undefined},
    Limits = erlang:get(force_shutdown_policy),
    case emqx_misc:conn_proc_mng_policy(Limits) of
        continue ->
            {noreply, NewState};
        hibernate ->
            %% going to hibernate, reset gc stats
            GcState1 = emqx_gc:reset(GcState),
            {noreply, NewState#state{gc_state = GcState1}, hibernate};
        {shutdown, Reason} ->
            ?LOG(warning, "shutdown due to ~p", [Reason]),
            shutdown(Reason, NewState)
    end;

handle_info(timeout, State) ->
    shutdown(idle_timeout, State);

handle_info({shutdown, Reason}, State) ->
    shutdown(Reason, State);

handle_info({shutdown, discard, {ClientId, ByPid}}, State) ->
    ?LOG(warning, "discarded by ~s:~p", [ClientId, ByPid]),
    shutdown(discard, State);

handle_info({shutdown, conflict, {ClientId, NewPid}}, State) ->
    ?LOG(warning, "clientid '~s' conflict with ~p", [ClientId, NewPid]),
    shutdown(conflict, State);

handle_info({TcpOrSsL, _Sock, Data}, State) when TcpOrSsL =:= tcp; TcpOrSsL =:= ssl ->
    process_incoming(Data, State);

%% Rate limit here, cool:)
handle_info({tcp_passive, _Sock}, State) ->
    {noreply, run_socket(ensure_rate_limit(State))};
%% FIXME Later
handle_info({ssl_passive, _Sock}, State) ->
    {noreply, run_socket(ensure_rate_limit(State))};

handle_info({Err, _Sock, Reason}, State) when Err =:= tcp_error; Err =:= ssl_error ->
    shutdown(Reason, State);

handle_info({Closed, _Sock}, State) when Closed =:= tcp_closed; Closed =:= ssl_closed ->
    shutdown(closed, State);

%% Rate limit timer
handle_info(activate_sock, State) ->
    {noreply, run_socket(State#state{conn_state = running, limit_timer = undefined})};

handle_info({inet_reply, _Sock, ok}, State) ->
    {noreply, State};

handle_info({inet_reply, _Sock, {error, Reason}}, State) ->
    shutdown(Reason, State);

handle_info({keepalive, start, Interval}, State = #state{transport = Transport, socket = Socket}) ->
    ?LOG(debug, "Keepalive at the interval of ~p", [Interval]),
    StatFun = fun() ->
                case Transport:getstat(Socket, [recv_oct]) of
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
    ?LOG(error, "unexpected info: ~p", [Info]),
    {noreply, State}.

terminate(Reason, #state{transport   = Transport,
                         socket      = Socket,
                         keepalive   = KeepAlive,
                         proto_state = ProtoState}) ->
    ?LOG(debug, "Terminated for ~p", [Reason]),
    Transport:fast_close(Socket),
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
%% Internals: process incoming, parse and handle packets
%%------------------------------------------------------------------------------

process_incoming(Data, State) ->
    Oct = iolist_size(Data),
    ?LOG(debug, "RECV ~p", [Data]),
    emqx_pd:update_counter(incoming_bytes, Oct),
    emqx_metrics:trans(inc, 'bytes/received', Oct),
    case handle_packet(Data, State) of
        {noreply, State1} ->
            State2 = maybe_gc({1, Oct}, State1),
            {noreply, ensure_stats_timer(State2)};
        Shutdown -> Shutdown
    end.

%% Parse and handle packets
handle_packet(<<>>, State) ->
    {noreply, State};

handle_packet(Data, State = #state{proto_state  = ProtoState,
                                   parser_state = ParserState,
                                   idle_timeout = IdleTimeout}) ->
    try emqx_frame:parse(Data, ParserState) of
        {more, ParserState1} ->
            {noreply, State#state{parser_state = ParserState1}, IdleTimeout};
        {ok, Packet = ?PACKET(Type), Rest} ->
            emqx_metrics:received(Packet),
            (Type == ?PUBLISH) andalso emqx_pd:update_counter(incoming_pubs, 1),
            case emqx_protocol:received(Packet, ProtoState) of
                {ok, ProtoState1} ->
                    handle_packet(Rest, reset_parser(State#state{proto_state = ProtoState1}));
                {error, Reason} ->
                    ?LOG(error, "Process packet error - ~p", [Reason]),
                    shutdown(Reason, State);
                {error, Reason, ProtoState1} ->
                    shutdown(Reason, State#state{proto_state = ProtoState1});
                {stop, Error, ProtoState1} ->
                    stop(Error, State#state{proto_state = ProtoState1})
            end;
        {error, Reason} ->
            ?LOG(error, "Parse frame error - ~p", [Reason]),
            shutdown(Reason, State)
    catch
        _:Error ->
            ?LOG(error, "Parse failed for ~p~nError data:~p", [Error, Data]),
            shutdown(parse_error, State)
    end.

reset_parser(State = #state{proto_state = ProtoState}) ->
    State#state{parser_state = emqx_protocol:parser(ProtoState)}.

%%------------------------------------------------------------------------------
%% Ensure rate limit

ensure_rate_limit(State = #state{rate_limit = Rl, pub_limit = Pl}) ->
    Limiters = [{Pl, #state.pub_limit, emqx_pd:reset_counter(incoming_pubs)},
                {Rl, #state.rate_limit, emqx_pd:reset_counter(incoming_bytes)}],
    ensure_rate_limit(Limiters, State).

ensure_rate_limit([], State) ->
    State;
ensure_rate_limit([{undefined, _Pos, _Cnt}|Limiters], State) ->
    ensure_rate_limit(Limiters, State);
ensure_rate_limit([{Rl, Pos, Cnt}|Limiters], State) ->
   case esockd_rate_limit:check(Cnt, Rl) of
       {0, Rl1} ->
           ensure_rate_limit(Limiters, setelement(Pos, State, Rl1));
       {Pause, Rl1} ->
           TRef = erlang:send_after(Pause, self(), activate_sock),
           setelement(Pos, State#state{conn_state = blocked, limit_timer = TRef}, Rl1)
   end.

%%------------------------------------------------------------------------------
%% Activate socket

run_socket(State = #state{conn_state = blocked}) ->
    State;

run_socket(State = #state{transport = Transport, socket = Socket, active_n = N}) ->
    TrueOrN = case Transport:is_ssl(Socket) of
                  true  -> true; %% Cannot set '{active, N}' for SSL:(
                  false -> N
              end,
    ensure_ok_or_exit(Transport:setopts(Socket, [{active, TrueOrN}])),
    State.

ensure_ok_or_exit(ok) -> ok;
ensure_ok_or_exit({error, Reason}) ->
    self() ! {shutdown, Reason}.

%%------------------------------------------------------------------------------
%% Ensure stats timer

ensure_stats_timer(State = #state{enable_stats = true,
                                  stats_timer = undefined,
                                  idle_timeout = IdleTimeout}) ->
    State#state{stats_timer = emqx_misc:start_timer(IdleTimeout, emit_stats)};
ensure_stats_timer(State) -> State.

%%------------------------------------------------------------------------------
%% Maybe GC

maybe_gc(_, State = #state{gc_state = undefined}) ->
    State;
maybe_gc({publish, _PacketId, #message{payload = Payload}}, State) ->
    Oct = iolist_size(Payload),
    maybe_gc({1, Oct}, State);
maybe_gc({Cnt, Oct}, State = #state{gc_state = GCSt}) ->
    {_, GCSt1} = emqx_gc:run(Cnt, Oct, GCSt),
    State#state{gc_state = GCSt1};
maybe_gc(_, State) ->
    State.

%%------------------------------------------------------------------------------
%% Shutdown or stop

shutdown(Reason, State) ->
    stop({shutdown, Reason}, State).

stop(Reason, State) ->
    {stop, Reason, State}.
