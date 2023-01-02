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

-module(emqx_stomp_connection).

-behaviour(gen_server).

-include("emqx_stomp.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("emqx/include/types.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-logger_header("[Stomp-Conn]").

-import(emqx_misc,
        [ start_timer/2
        ]).

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
-export([send/4, heartbeat/2, statfun/3]).

%% for mgmt
-export([call/2, call/3]).

-record(state, {
          %% TCP/TLS Transport
          transport :: esockd:transport(),
          %% TCP/TLS Socket
          socket :: esockd:socket(),
          %% Peername of the connection
          peername :: emqx_types:peername(),
          %% Sockname of the connection
          sockname :: emqx_types:peername(),
          %% Sock State
          sockstate :: emqx_types:sockstate(),
          %% The {active, N} option
          active_n :: pos_integer(),
          %% Limiter
          limiter :: maybe(emqx_limiter:limiter()),
          %% Limit Timer
          limit_timer :: maybe(reference()),
          %% GC State
          gc_state :: maybe(emqx_gc:gc_state()),
          %% Stats Timer
          stats_timer :: disabled | maybe(reference()),
          %% Parser State
          parser :: emqx_stomp_frame:parser(),
          %% Protocol State
          pstate :: emqx_stomp_protocol:pstate(),
          %% XXX: some common confs
          proto_env :: list()
         }).

-type(state() :: #state{}).

-define(DEFAULT_GC_POLICY, #{bytes => 16777216, count => 16000}).
-define(DEFAULT_OOM_POLICY, #{ max_heap_size => 8388608,
                               message_queue_len => 10000}).

-define(ACTIVE_N, 100).
-define(IDLE_TIMEOUT, 30000).
-define(INFO_KEYS,  [socktype, peername, sockname, sockstate, active_n]).
-define(CONN_STATS, [recv_pkt, recv_msg, send_pkt, send_msg]).
-define(SOCK_STATS, [recv_oct, recv_cnt, send_oct, send_cnt, send_pend]).

-define(ENABLED(X), (X =/= undefined)).

start_link(Transport, Sock, ProtoEnv) ->
    {ok, proc_lib:spawn_link(?MODULE, init, [[Transport, Sock, ProtoEnv]])}.

-spec info(pid() | state()) -> emqx_types:infos().
info(CPid) when is_pid(CPid) ->
    call(CPid, info);
info(State = #state{pstate = PState}) ->
    ChanInfo = emqx_stomp_protocol:info(PState),
    SockInfo = maps:from_list(
                 info(?INFO_KEYS, State)),
    ChanInfo#{sockinfo => SockInfo}.

info(Keys, State) when is_list(Keys) ->
    [{Key, info(Key, State)} || Key <- Keys];
info(socktype, #state{transport = Transport, socket = Socket}) ->
    Transport:type(Socket);
info(peername, #state{peername = Peername}) ->
    Peername;
info(sockname, #state{sockname = Sockname}) ->
    Sockname;
info(sockstate, #state{sockstate = SockSt}) ->
    SockSt;
info(active_n, #state{active_n = ActiveN}) ->
    ActiveN.

-spec stats(pid() | state()) -> emqx_types:stats().
stats(CPid) when is_pid(CPid) ->
    call(CPid, stats);
stats(#state{transport = Transport,
             socket    = Socket,
             pstate    = PState}) ->
    SockStats = case Transport:getstat(Socket, ?SOCK_STATS) of
                    {ok, Ss}   -> Ss;
                    {error, _} -> []
                end,
    ConnStats = emqx_pd:get_counters(?CONN_STATS),
    ChanStats = emqx_stomp_protocol:stats(PState),
    ProcStats = emqx_misc:proc_stats(),
    lists:append([SockStats, ConnStats, ChanStats, ProcStats]).

call(Pid, Req) ->
    call(Pid, Req, infinity).
call(Pid, Req, Timeout) ->
    gen_server:call(Pid, Req, Timeout).

-spec init([term()]) -> no_return().
init([Transport, RawSocket, ProtoEnv]) ->
    case Transport:wait(RawSocket) of
        {ok, Socket} ->
            init_state(Transport, Socket, ProtoEnv);
        {error, Reason} ->
            ok = Transport:fast_close(RawSocket),
            exit_on_sock_error(Reason)
    end.

-spec init_state(module(), port(), [proplists:property()]) -> no_return().
init_state(Transport, Socket, ProtoEnv) ->
    {ok, Peername} = Transport:ensure_ok_or_exit(peername, [Socket]),
    {ok, Sockname} = Transport:ensure_ok_or_exit(sockname, [Socket]),

    SendFun = {fun ?MODULE:send/4, [Transport, Socket, self()]},
    StatFun = {fun ?MODULE:statfun/3, [Transport, Socket]},
    HrtBtFun = {fun ?MODULE:heartbeat/2, [Transport, Socket]},
    Parser = emqx_stomp_frame:init_parer_state(ProtoEnv),

    ActiveN = proplists:get_value(active_n, ProtoEnv, ?ACTIVE_N),
    GcState = emqx_gc:init(?DEFAULT_GC_POLICY),

    Peercert = Transport:ensure_ok_or_exit(peercert, [Socket]),
    ConnInfo = #{socktype => Transport:type(Socket),
                 peername => Peername,
                 sockname => Sockname,
                 peercert => Peercert,
                 statfun  => StatFun,
                 sendfun  => SendFun,
                 heartfun => HrtBtFun,
                 conn_mod => ?MODULE
                },
    PState = emqx_stomp_protocol:init(ConnInfo, ProtoEnv),
    State = #state{transport = Transport,
                   socket    = Socket,
                   peername  = Peername,
                   sockname  = Sockname,
                   sockstate = idle,
                   active_n  = ActiveN,
                   limiter   = undefined,
                   parser    = Parser,
                   proto_env = ProtoEnv,
                   gc_state  = GcState,
                   pstate    = PState},
    case activate_socket(State) of
        {ok, NState} ->
            emqx_logger:set_metadata_peername(
              esockd:format(Peername)),
            gen_server:enter_loop(
              ?MODULE, [{hibernate_after, 5000}], NState, 20000);
        {error, Reason} ->
            ok = Transport:fast_close(Socket),
            exit_on_sock_error(Reason)
    end.

-spec exit_on_sock_error(any()) -> no_return().
exit_on_sock_error(Reason) when Reason =:= einval;
                                Reason =:= enotconn;
                                Reason =:= closed ->
    erlang:exit(normal);
exit_on_sock_error(timeout) ->
    erlang:exit({shutdown, ssl_upgrade_timeout});
exit_on_sock_error(Reason) ->
    erlang:exit({shutdown, Reason}).

send(Frame, Transport, Sock, ConnPid) ->
    ?LOG(info, "SEND Frame: ~s", [emqx_stomp_frame:format(Frame)]),
    ok = inc_outgoing_stats(Frame),
    Data = emqx_stomp_frame:serialize(Frame),
    ?LOG(debug, "SEND ~p", [Data]),
    try Transport:async_send(Sock, Data) of
        ok -> ok;
        {error, Reason} -> ConnPid ! {shutdown, Reason}
    catch
        error:Error -> ConnPid ! {shutdown, Error}
    end.

heartbeat(Transport, Sock) ->
    ?LOG(debug, "SEND heartbeat: \\n"),
    Transport:send(Sock, <<$\n>>).

statfun(Stat, Transport, Sock) ->
    case Transport:getstat(Sock, [Stat]) of
        {ok, [{Stat, Val}]} -> {ok, Val};
        {error, Error}      -> {error, Error}
    end.

handle_call(info, _From, State) ->
    {reply, info(State), State};

handle_call(stats, _From, State) ->
    {reply, stats(State), State};

handle_call(discard, _From, State) ->
    %% TODO: send the DISCONNECT packet?
    shutdown_and_reply(discared, ok, State);

handle_call(kick, _From, State) ->
    shutdown_and_reply(kicked, ok, State);

handle_call(Req, _From, State) ->
    ?LOG(error, "unexpected request: ~p", [Req]),
    {reply, ignored, State}.

handle_cast(Msg, State) ->
    ?LOG(error, "unexpected msg: ~p", [Msg]),
    noreply(State).

handle_info({event, Name}, State = #state{pstate = PState})
  when Name == connected;
       Name == updated ->
    ClientId = emqx_stomp_protocol:info(clientid, PState),
    emqx_cm:insert_channel_info(ClientId, info(State), stats(State)),
    noreply(State);

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

handle_info({timeout, _TRef, limit_timeout}, State) ->
    NState = State#state{sockstate   = idle,
                         limit_timer = undefined
                        },
    handle_info(activate_socket, NState);

handle_info({timeout, _TRef, emit_stats},
            State = #state{pstate = PState}) ->
    ClientId = emqx_stomp_protocol:info(clientid, PState),
    emqx_cm:set_chan_stats(ClientId, stats(State)),
    noreply(State#state{stats_timer = undefined});

handle_info({timeout, TRef, TMsg}, State) ->
    with_proto(timeout, [TRef, TMsg], State);

handle_info(activate_socket, State) ->
    case activate_socket(State) of
        {ok, NState} ->
            noreply(NState);
        {error, Reason} ->
            handle_info({sock_error, Reason}, State)
    end;

handle_info({inet_reply, _Ref, ok}, State) ->
    noreply(State);

handle_info({Inet, _Sock, Data}, State) when Inet == tcp; Inet == ssl ->
    ?LOG(debug, "RECV ~0p", [Data]),
    Oct = iolist_size(Data),
    inc_counter(incoming_bytes, Oct),
    ok = emqx_metrics:inc('bytes.received', Oct),
    received(Data, ensure_stats_timer(?IDLE_TIMEOUT, State));

handle_info({Passive, _Sock}, State)
  when Passive == tcp_passive; Passive == ssl_passive ->
    %% In Stats
    Pubs = emqx_pd:reset_counter(incoming_pubs),
    Bytes = emqx_pd:reset_counter(incoming_bytes),
    InStats = #{cnt => Pubs, oct => Bytes},
    %% Ensure Rate Limit
    NState = ensure_rate_limit(InStats, State),
    %% Run GC and Check OOM
    NState1 = check_oom(run_gc(InStats, NState)),
    handle_info(activate_socket, NState1);

handle_info({Error, _Sock, Reason}, State)
  when Error == tcp_error; Error == ssl_error ->
    handle_info({sock_error, Reason}, State);

handle_info({Closed, _Sock}, State)
  when Closed == tcp_closed; Closed == ssl_closed ->
    handle_info({sock_closed, Closed}, close_socket(State));

handle_info({inet_reply, _Sock, {error, Reason}}, State) ->
    handle_info({sock_error, Reason}, State);

handle_info({sock_error, Reason}, State) ->
    case Reason =/= closed andalso Reason =/= einval of
        true -> ?LOG(warning, "socket_error: ~p", [Reason]);
        false -> ok
    end,
    handle_info({sock_closed, Reason}, close_socket(State));

handle_info({sock_closed, Reason}, State) ->
    shutdown(Reason, State);

handle_info({deliver, _Topic, Msg}, State = #state{pstate = PState}) ->
    noreply(State#state{pstate = case emqx_stomp_protocol:send(Msg, PState) of
                                     {ok, PState1} ->
                                         PState1;
                                     {error, dropped, PState1} ->
                                         PState1
                                 end});

handle_info(Info, State) ->
    with_proto(handle_info, [Info], State).

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
        ok ->
            noreply(State);
        {ok, NPState} ->
            noreply(State#state{pstate = NPState});
        {F, Reason, NPState} when F == stop;
                                  F == error;
                                  F == shutdown ->
            shutdown(Reason, State#state{pstate = NPState})
    end.

received(<<>>, State) ->
    noreply(State);

received(Bytes, State = #state{parser = Parser,
                               pstate = PState}) ->
    try emqx_stomp_frame:parse(Bytes, Parser) of
        {more, NewParser} ->
            noreply(State#state{parser = NewParser});
        {ok, Frame, Rest} ->
            ?LOG(info, "RECV Frame: ~s", [emqx_stomp_frame:format(Frame)]),
            ok = inc_incoming_stats(Frame),
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
    State#state{parser = emqx_stomp_frame:init_parer_state(ProtoEnv)}.

activate_socket(State = #state{sockstate = closed}) ->
    {ok, State};
activate_socket(State = #state{sockstate = blocked}) ->
    {ok, State};
activate_socket(State = #state{transport = Transport,
                               socket    = Socket,
                               active_n  = N}) ->
    case Transport:setopts(Socket, [{active, N}]) of
        ok -> {ok, State#state{sockstate = running}};
        Error -> Error
    end.

close_socket(State = #state{sockstate = closed}) -> State;
close_socket(State = #state{transport = Transport, socket = Socket}) ->
    ok = Transport:fast_close(Socket),
    State#state{sockstate = closed}.

%%--------------------------------------------------------------------
%% Inc incoming/outgoing stats

inc_incoming_stats(#stomp_frame{command = Cmd}) ->
    inc_counter(recv_pkt, 1),
    case Cmd of
        <<"SEND">> ->
            inc_counter(recv_msg, 1),
            inc_counter(incoming_pubs, 1),
            emqx_metrics:inc('messages.received'),
            emqx_metrics:inc('messages.qos1.received');
        _ ->
            ok
    end,
    emqx_metrics:inc('packets.received').

inc_outgoing_stats(#stomp_frame{command = Cmd}) ->
    inc_counter(send_pkt, 1),
    case Cmd of
        <<"MESSAGE">> ->
            inc_counter(send_msg, 1),
            inc_counter(outgoing_pubs, 1),
            emqx_metrics:inc('messages.sent'),
            emqx_metrics:inc('messages.qos1.sent');
        _ ->
            ok
    end,
    emqx_metrics:inc('packets.sent').

%%--------------------------------------------------------------------
%% Ensure rate limit

ensure_rate_limit(Stats, State = #state{limiter = Limiter}) ->
    case ?ENABLED(Limiter) andalso emqx_limiter:check(Stats, Limiter) of
        false -> State;
        {ok, Limiter1} ->
            State#state{limiter = Limiter1};
        {pause, Time, Limiter1} ->
            ?LOG(notice, "Pause ~pms due to rate limit", [Time]),
            TRef = start_timer(Time, limit_timeout),
            State#state{sockstate   = blocked,
                        limiter     = Limiter1,
                        limit_timer = TRef
                       }
    end.

%%--------------------------------------------------------------------
%% Run GC and Check OOM

run_gc(Stats, State = #state{gc_state = GcSt}) ->
    case ?ENABLED(GcSt) andalso emqx_gc:run(Stats, GcSt) of
        false -> State;
        {_IsGC, GcSt1} ->
            State#state{gc_state = GcSt1}
    end.

check_oom(State) ->
    OomPolicy = ?DEFAULT_OOM_POLICY,
    ?tp(debug, check_oom, #{policy => OomPolicy}),
    case ?ENABLED(OomPolicy) andalso emqx_misc:check_oom(OomPolicy) of
        {shutdown, Reason} ->
            %% triggers terminate/2 callback immediately
            erlang:exit({shutdown, Reason});
        _Other ->
            ok
    end,
    State.

%%--------------------------------------------------------------------
%% Ensure/cancel stats timer

ensure_stats_timer(Timeout, State = #state{stats_timer = undefined}) ->
    State#state{stats_timer = start_timer(Timeout, emit_stats)};
ensure_stats_timer(_Timeout, State) -> State.

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

shutdown_and_reply(Reason, Reply, State) ->
    {stop, {shutdown, Reason}, Reply, State}.

inc_counter(Key, Inc) ->
    _ = emqx_pd:inc_counter(Key, Inc),
    ok.
