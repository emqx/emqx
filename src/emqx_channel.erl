%%--------------------------------------------------------------------
%% Copyright (c) 2019 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% MQTT TCP/SSL Channel
-module(emqx_channel).

-behaviour(gen_statem).

-include("emqx.hrl").
-include("emqx_mqtt.hrl").
-include("logger.hrl").
-include("types.hrl").

-logger_header("[Channel]").

-export([start_link/3]).

%% APIs
-export([ info/1
        , attrs/1
        , stats/1
        ]).

%% gen_statem callbacks
-export([ idle/3
        , connected/3
        , disconnected/3
        ]).

-export([ init/1
        , callback_mode/0
        , code_change/4
        , terminate/3
        ]).

-record(state, {
          transport    :: esockd:transport(),
          socket       :: esockd:sock(),
          peername     :: {inet:ip_address(), inet:port_number()},
          sockname     :: {inet:ip_address(), inet:port_number()},
          conn_state   :: running | blocked,
          active_n     :: pos_integer(),
          rate_limit   :: maybe(esockd_rate_limit:bucket()),
          pub_limit    :: maybe(esockd_rate_limit:bucket()),
          limit_timer  :: maybe(reference()),
          serializer   :: emqx_frame:serializer(), %% TODO: remove it later.
          parse_state  :: emqx_frame:parse_state(),
          proto_state  :: emqx_protocol:protocol(),
          gc_state     :: emqx_gc:gc_state(),
          keepalive    :: maybe(reference()),
          enable_stats :: boolean(),
          stats_timer  :: maybe(reference()),
          idle_timeout :: timeout()
         }).

-define(ACTIVE_N, 100).
-define(HANDLE(T, C, D), handle((T), (C), (D))).
-define(CHAN_STATS, [recv_pkt, recv_msg, send_pkt, send_msg]).
-define(SOCK_STATS, [recv_oct, recv_cnt, send_oct, send_cnt, send_pend]).

-spec(start_link(esockd:transport(), esockd:sock(), proplists:proplist())
      -> {ok, pid()}).
start_link(Transport, Socket, Options) ->
    {ok, proc_lib:spawn_link(?MODULE, init, [{Transport, Socket, Options}])}.

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

%% For debug
-spec(info(pid() | #state{}) -> map()).
info(CPid) when is_pid(CPid) ->
    call(CPid, info);

info(#state{transport = Transport,
            socket = Socket,
            peername = Peername,
            sockname = Sockname,
            conn_state = ConnState,
            active_n = ActiveN,
            rate_limit = RateLimit,
            pub_limit = PubLimit,
            proto_state = ProtoState}) ->
    ConnInfo = #{socktype => Transport:type(Socket),
                 peername => Peername,
                 sockname => Sockname,
                 conn_state => ConnState,
                 active_n => ActiveN,
                 rate_limit => rate_limit_info(RateLimit),
                 pub_limit => rate_limit_info(PubLimit)
                },
    ProtoInfo = emqx_protocol:info(ProtoState),
    maps:merge(ConnInfo, ProtoInfo).

rate_limit_info(undefined) ->
    #{};
rate_limit_info(Limit) ->
    esockd_rate_limit:info(Limit).

%% For dashboard
attrs(CPid) when is_pid(CPid) ->
    call(CPid, attrs);

attrs(#state{peername = Peername,
             sockname = Sockname,
             proto_state = ProtoState}) ->
    SockAttrs = #{peername => Peername,
                  sockname => Sockname},
    ProtoAttrs = emqx_protocol:attrs(ProtoState),
    maps:merge(SockAttrs, ProtoAttrs).

%% Conn stats
stats(CPid) when is_pid(CPid) ->
    call(CPid, stats);

stats(#state{transport = Transport, socket = Socket}) ->
    SockStats = case Transport:getstat(Socket, ?SOCK_STATS) of
                    {ok, Ss}   -> Ss;
                    {error, _} -> []
                end,
    ChanStats = [{Name, emqx_pd:get_counter(Name)} || Name <- ?CHAN_STATS],
    lists:append([SockStats, ChanStats, emqx_misc:proc_stats()]).

call(CPid, Req) ->
    gen_statem:call(CPid, Req, infinity).

%%--------------------------------------------------------------------
%% gen_statem callbacks
%%--------------------------------------------------------------------

init({Transport, RawSocket, Options}) ->
    {ok, Socket} = Transport:wait(RawSocket),
    {ok, Peername} = Transport:ensure_ok_or_exit(peername, [Socket]),
    {ok, Sockname} = Transport:ensure_ok_or_exit(sockname, [Socket]),
    Peercert = Transport:ensure_ok_or_exit(peercert, [Socket]),
    emqx_logger:set_metadata_peername(esockd_net:format(Peername)),
    Zone = proplists:get_value(zone, Options),
    RateLimit = init_limiter(proplists:get_value(rate_limit, Options)),
    PubLimit = init_limiter(emqx_zone:get_env(Zone, publish_limit)),
    ActiveN = proplists:get_value(active_n, Options, ?ACTIVE_N),
    MaxSize = emqx_zone:get_env(Zone, max_packet_size, ?MAX_PACKET_SIZE),
    ParseState = emqx_frame:initial_parse_state(#{max_size => MaxSize}),
    ProtoState = emqx_protocol:init(#{peername => Peername,
                                      sockname => Sockname,
                                      peercert => Peercert,
                                      conn_mod => ?MODULE}, Options),
    GcPolicy = emqx_zone:get_env(Zone, force_gc_policy, false),
    GcState = emqx_gc:init(GcPolicy),
    ok = emqx_misc:init_proc_mng_policy(Zone),
    EnableStats = emqx_zone:get_env(Zone, enable_stats, true),
    IdleTimout = emqx_zone:get_env(Zone, idle_timeout, 30000),
    State = #state{transport    = Transport,
                   socket       = Socket,
                   peername     = Peername,
                   conn_state   = running,
                   active_n     = ActiveN,
                   rate_limit   = RateLimit,
                   pub_limit    = PubLimit,
                   parse_state  = ParseState,
                   proto_state  = ProtoState,
                   gc_state     = GcState,
                   enable_stats = EnableStats,
                   idle_timeout = IdleTimout
                  },
    gen_statem:enter_loop(?MODULE, [{hibernate_after, 2 * IdleTimout}],
                          idle, State, self(), [IdleTimout]).

init_limiter(undefined) ->
    undefined;
init_limiter({Rate, Burst}) ->
    esockd_rate_limit:new(Rate, Burst).

callback_mode() ->
    [state_functions, state_enter].

%%--------------------------------------------------------------------
%% Idle State

idle(enter, _, State) ->
    ok = activate_socket(State),
    keep_state_and_data;

idle(timeout, _Timeout, State) ->
    stop(idle_timeout, State);

idle(cast, {incoming, Packet = ?CONNECT_PACKET(ConnVar)}, State) ->
    #mqtt_packet_connect{proto_ver = ProtoVer} = ConnVar,
    Serializer = emqx_frame:init_serializer(#{version => ProtoVer}),
    NState = State#state{serializer = Serializer},
    handle_incoming(Packet, fun(St) -> {next_state, connected, St} end, NState);

idle(cast, {incoming, Packet}, State) ->
    ?LOG(warning, "Unexpected incoming: ~p", [Packet]),
    shutdown(unexpected_incoming_packet, State);

idle(EventType, Content, State) ->
    ?HANDLE(EventType, Content, State).

%%--------------------------------------------------------------------
%% Connected State

connected(enter, _, _State) ->
    %% What to do?
    keep_state_and_data;

connected(cast, {incoming, Packet = ?PACKET(?CONNECT)}, State) ->
    ?LOG(warning, "Unexpected connect: ~p", [Packet]),
    shutdown(unexpected_incoming_connect, State);

connected(cast, {incoming, Packet = ?PACKET(Type)}, State) ->
    ok = emqx_metrics:inc_recv(Packet),
    (Type == ?PUBLISH) andalso emqx_pd:update_counter(incoming_pubs, 1),
    handle_incoming(Packet, fun(St) -> {keep_state, St} end, State);

%% Handle delivery
connected(info, Devliery = {deliver, _Topic, Msg}, State = #state{proto_state = ProtoState}) ->
    case emqx_protocol:handle_out(Devliery, ProtoState) of
        {ok, NProtoState} ->
            {keep_state, State#state{proto_state = NProtoState}};
        {ok, Packet, NProtoState} ->
            NState = State#state{proto_state = NProtoState},
            handle_outgoing(Packet, fun(St) -> {keep_state, St} end, NState);
        {error, Reason} ->
            shutdown(Reason, State)
    end;

%% Start Keepalive
connected(info, {keepalive, start, Interval},
          State = #state{transport = Transport, socket = Socket}) ->
    StatFun = fun() ->
                  case Transport:getstat(Socket, [recv_oct]) of
                      {ok, [{recv_oct, RecvOct}]} -> {ok, RecvOct};
                      Error -> Error
                  end
              end,
    case emqx_keepalive:start(StatFun, Interval, {keepalive, check}) of
        {ok, KeepAlive} ->
            {keep_state, State#state{keepalive = KeepAlive}};
        {error, Error} ->
            shutdown(Error, State)
    end;

%% Keepalive timer
connected(info, {keepalive, check}, State = #state{keepalive = KeepAlive}) ->
    case emqx_keepalive:check(KeepAlive) of
        {ok, KeepAlive1} ->
            {keep_state, State#state{keepalive = KeepAlive1}};
        {error, timeout} ->
            shutdown(keepalive_timeout, State);
        {error, Error} ->
            shutdown(Error, State)
    end;

connected(EventType, Content, State) ->
    ?HANDLE(EventType, Content, State).

%%--------------------------------------------------------------------
%% Disconnected State

disconnected(enter, _, _State) ->
    %% TODO: What to do?
    keep_state_and_data;

disconnected(EventType, Content, State) ->
    ?HANDLE(EventType, Content, State).

%% Handle call
handle({call, From}, info, State) ->
    reply(From, info(State), State);

handle({call, From}, attrs, State) ->
    reply(From, attrs(State), State);

handle({call, From}, stats, State) ->
    reply(From, stats(State), State);

handle({call, From}, kick, State) ->
    ok = gen_statem:reply(From, ok),
    shutdown(kicked, State);

handle({call, From}, discard, State) ->
    ok = gen_statem:reply(From, ok),
    shutdown(discard, State);

handle({call, From}, Req, State) ->
    ?LOG(error, "Unexpected call: ~p", [Req]),
    reply(From, ignored, State);

%% Handle cast
handle(cast, Msg, State) ->
    ?LOG(error, "Unexpected cast: ~p", [Msg]),
    {keep_state, State};

%% Handle Incoming
handle(info, {Inet, _Sock, Data}, State) when Inet == tcp;
                                              Inet == ssl ->
    Oct = iolist_size(Data),
    ?LOG(debug, "RECV ~p", [Data]),
    emqx_pd:update_counter(incoming_bytes, Oct),
    ok = emqx_metrics:inc('bytes.received', Oct),
    NState = ensure_stats_timer(maybe_gc({1, Oct}, State)),
    process_incoming(Data, [], NState);

handle(info, {Error, _Sock, Reason}, State)
  when Error == tcp_error; Error == ssl_error ->
    shutdown(Reason, State);

handle(info, {Closed, _Sock}, State)
  when Closed == tcp_closed; Closed == ssl_closed ->
    shutdown(closed, State);

handle(info, {Passive, _Sock}, State) when Passive == tcp_passive;
                                           Passive == ssl_passive ->
    %% Rate limit here:)
    NState = ensure_rate_limit(State),
    ok = activate_socket(NState),
    {keep_state, NState};

handle(info, activate_socket, State) ->
    %% Rate limit timer expired.
    ok = activate_socket(State#state{conn_state = running}),
    {keep_state, State#state{conn_state = running, limit_timer = undefined}};

handle(info, {inet_reply, _Sock, ok}, State) ->
    %% something sent
    {keep_state, ensure_stats_timer(State)};

handle(info, {inet_reply, _Sock, {error, Reason}}, State) ->
    shutdown(Reason, State);

handle(info, {timeout, Timer, emit_stats},
       State = #state{stats_timer = Timer,
                      proto_state = ProtoState,
                      gc_state    = GcState}) ->
    ClientId = emqx_protocol:client_id(ProtoState),
    emqx_cm:set_conn_stats(ClientId, stats(State)),
    NState = State#state{stats_timer = undefined},
    Limits = erlang:get(force_shutdown_policy),
    case emqx_misc:conn_proc_mng_policy(Limits) of
        continue ->
            {keep_state, NState};
        hibernate ->
            %% going to hibernate, reset gc stats
            GcState1 = emqx_gc:reset(GcState),
            {keep_state, NState#state{gc_state = GcState1}, hibernate};
        {shutdown, Reason} ->
            ?LOG(error, "Shutdown exceptionally due to ~p", [Reason]),
            shutdown(Reason, NState)
    end;

handle(info, {shutdown, discard, {ClientId, ByPid}}, State) ->
    ?LOG(error, "Discarded by ~s:~p", [ClientId, ByPid]),
    shutdown(discard, State);

handle(info, {shutdown, conflict, {ClientId, NewPid}}, State) ->
    ?LOG(warning, "Clientid '~s' conflict with ~p", [ClientId, NewPid]),
    shutdown(conflict, State);

handle(info, {shutdown, Reason}, State) ->
    shutdown(Reason, State);

handle(info, Info, State) ->
    ?LOG(error, "Unexpected info: ~p", [Info]),
    {keep_state, State}.

code_change(_Vsn, State, Data, _Extra) ->
    {ok, State, Data}.

terminate(Reason, _StateName, #state{transport = Transport,
                                     socket = Socket,
                                     keepalive = KeepAlive,
                                     proto_state = ProtoState}) ->
    ?LOG(debug, "Terminated for ~p", [Reason]),
    ok = Transport:fast_close(Socket),
    ok = emqx_keepalive:cancel(KeepAlive),
    emqx_protocol:terminate(Reason, ProtoState).

%%--------------------------------------------------------------------
%% Process incoming data

process_incoming(<<>>, Packets, State) ->
    {keep_state, State, next_events(Packets)};

process_incoming(Data, Packets, State = #state{parse_state = ParseState}) ->
    try emqx_frame:parse(Data, ParseState) of
        {ok, NParseState} ->
            NState = State#state{parse_state = NParseState},
            {keep_state, NState, next_events(Packets)};
        {ok, Packet, Rest, NParseState} ->
            NState = State#state{parse_state = NParseState},
            process_incoming(Rest, [Packet|Packets], NState);
        {error, Reason} ->
            shutdown(Reason, State)
    catch
        error:Reason:Stk ->
            ?LOG(error, "Parse failed for ~p~n\
                 Stacktrace:~p~nError data:~p", [Reason, Stk, Data]),
            shutdown(parse_error, State)
    end.

next_events(Packets) when is_list(Packets) ->
    [next_events(Packet) || Packet <- lists:reverse(Packets)];
next_events(Packet) ->
    {next_event, cast, {incoming, Packet}}.

%%--------------------------------------------------------------------
%% Handle incoming packet

handle_incoming(Packet = ?PACKET(Type), SuccFun,
                State = #state{proto_state = ProtoState}) ->
    _ = inc_incoming_stats(Type),
    ?LOG(debug, "RECV ~s", [emqx_packet:format(Packet)]),
    case emqx_protocol:handle_in(Packet, ProtoState) of
        {ok, NProtoState} ->
            SuccFun(State#state{proto_state = NProtoState});
        {ok, OutPacket, NProtoState} ->
            handle_outgoing(OutPacket, SuccFun,
                            State#state{proto_state = NProtoState});
        {error, Reason} ->
            shutdown(Reason, State);
        {error, Reason, NProtoState} ->
            shutdown(Reason, State#state{proto_state = NProtoState});
        {stop, Error, NProtoState} ->
            stop(Error, State#state{proto_state = NProtoState})
    end.

%%--------------------------------------------------------------------
%% Handle outgoing packet

handle_outgoing(Packet = ?PACKET(Type), SuccFun,
                State = #state{transport = Transport,
                               socket = Socket,
                               serializer = Serializer}) ->
    _ = inc_outgoing_stats(Type),
    ?LOG(debug, "SEND ~s", [emqx_packet:format(Packet)]),
    Data = Serializer(Packet),
    case Transport:async_send(Socket, Data) of
        ok -> SuccFun(State);
        {error, Reason} ->
            shutdown(Reason, State)
    end.

%%--------------------------------------------------------------------
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
           ?LOG(debug, "Rate limit pause connection ~pms", [Pause]),
           TRef = erlang:send_after(Pause, self(), activate_socket),
           setelement(Pos, State#state{conn_state = blocked, limit_timer = TRef}, Rl1)
   end.

%% start_keepalive(0, _PState) ->
%%     ignore;
%% start_keepalive(Secs, #pstate{zone = Zone}) when Secs > 0 ->
%%     Backoff = emqx_zone:get_env(Zone, keepalive_backoff, 0.75),
%%     self() ! {keepalive, start, round(Secs * Backoff)}.

%%--------------------------------------------------------------------
%% Activate socket

activate_socket(#state{conn_state = blocked}) ->
    ok;

activate_socket(#state{transport = Transport, socket = Socket, active_n = N}) ->
    case Transport:setopts(Socket, [{active, N}]) of
        ok -> ok;
        {error, Reason} ->
            self() ! {shutdown, Reason},
            ok
    end.

%%--------------------------------------------------------------------
%% Inc incoming/outgoing stats

inc_incoming_stats(Type) ->
    emqx_pd:update_counter(recv_pkt, 1),
    Type =:= ?PUBLISH andalso emqx_pd:update_counter(recv_msg, 1).

inc_outgoing_stats(Type) ->
    emqx_pd:update_counter(send_pkt, 1),
    Type =:= ?PUBLISH andalso emqx_pd:update_counter(send_msg, 1).

%%--------------------------------------------------------------------
%% Ensure stats timer

ensure_stats_timer(State = #state{enable_stats = true,
                                  stats_timer  = undefined,
                                  idle_timeout = IdleTimeout}) ->
    State#state{stats_timer = emqx_misc:start_timer(IdleTimeout, emit_stats)};
ensure_stats_timer(State) -> State.

%%--------------------------------------------------------------------
%% Maybe GC

maybe_gc(_, State = #state{gc_state = undefined}) ->
    State;
maybe_gc({publish, _, #message{payload = Payload}}, State) ->
    Oct = iolist_size(Payload),
    maybe_gc({1, Oct}, State);
maybe_gc(Packets, State) when is_list(Packets) ->
    {Cnt, Oct} =
    lists:unzip([{1, iolist_size(Payload)}
                 || {publish, _, #message{payload = Payload}} <- Packets]),
    maybe_gc({lists:sum(Cnt), lists:sum(Oct)}, State);
maybe_gc({Cnt, Oct}, State = #state{gc_state = GCSt}) ->
    {_, GCSt1} = emqx_gc:run(Cnt, Oct, GCSt),
    State#state{gc_state = GCSt1};
maybe_gc(_, State) -> State.

%%--------------------------------------------------------------------
%% Helper functions

reply(From, Reply, State) ->
    {keep_state, State, [{reply, From, Reply}]}.

shutdown(Reason, State) ->
    stop({shutdown, Reason}, State).

stop(Reason, State) ->
    {stop, Reason, State}.

