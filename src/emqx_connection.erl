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

%% MQTT TCP/SSL Connection
-module(emqx_connection).

-behaviour(gen_statem).

-include("emqx.hrl").
-include("emqx_mqtt.hrl").
-include("logger.hrl").
-include("types.hrl").

-logger_header("[Conn]").

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
          socket       :: esockd:socket(),
          peername     :: emqx_types:peername(),
          sockname     :: emqx_types:peername(),
          conn_state   :: running | blocked,
          active_n     :: pos_integer(),
          rate_limit   :: maybe(esockd_rate_limit:bucket()),
          pub_limit    :: maybe(esockd_rate_limit:bucket()),
          limit_timer  :: maybe(reference()),
          parse_state  :: emqx_frame:parse_state(),
          chan_state   :: emqx_channel:channel(),
          gc_state     :: emqx_gc:gc_state(),
          keepalive    :: maybe(emqx_keepalive:keepalive()),
          stats_timer  :: disabled | maybe(reference()),
          idle_timeout :: timeout()
         }).

-define(ACTIVE_N, 100).
-define(HANDLE(T, C, D), handle((T), (C), (D))).
-define(CHAN_STATS, [recv_pkt, recv_msg, send_pkt, send_msg]).
-define(SOCK_STATS, [recv_oct, recv_cnt, send_oct, send_cnt, send_pend]).

-spec(start_link(esockd:transport(), esockd:socket(), proplists:proplist())
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
            chan_state = ChanState}) ->
    ConnInfo = #{socktype => Transport:type(Socket),
                 peername => Peername,
                 sockname => Sockname,
                 conn_state => ConnState,
                 active_n => ActiveN,
                 rate_limit => rate_limit_info(RateLimit),
                 pub_limit => rate_limit_info(PubLimit)
                },
    ChanInfo = emqx_channel:info(ChanState),
    maps:merge(ConnInfo, ChanInfo).

rate_limit_info(undefined) ->
    undefined;
rate_limit_info(Limit) ->
    esockd_rate_limit:info(Limit).

%% For dashboard
attrs(CPid) when is_pid(CPid) ->
    call(CPid, attrs);

attrs(#state{peername = Peername,
             sockname = Sockname,
             conn_state = ConnState,
             chan_state = ChanState}) ->
    SockAttrs = #{peername => Peername,
                  sockname => Sockname,
                  conn_state => ConnState
                 },
    ChanAttrs = emqx_channel:attrs(ChanState),
    maps:merge(SockAttrs, ChanAttrs).

%% @doc Get connection stats
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
    ChanState = emqx_channel:init(#{peername => Peername,
                                    sockname => Sockname,
                                    peercert => Peercert,
                                    conn_mod => ?MODULE}, Options),
    GcPolicy = emqx_zone:get_env(Zone, force_gc_policy, false),
    GcState = emqx_gc:init(GcPolicy),
    EnableStats = emqx_zone:get_env(Zone, enable_stats, true),
    StatsTimer = if EnableStats -> undefined; ?Otherwise-> disabled end,
    IdleTimout = emqx_zone:get_env(Zone, idle_timeout, 30000),
    ok = emqx_misc:init_proc_mng_policy(Zone),
    State = #state{transport    = Transport,
                   socket       = Socket,
                   peername     = Peername,
                   conn_state   = running,
                   active_n     = ActiveN,
                   rate_limit   = RateLimit,
                   pub_limit    = PubLimit,
                   parse_state  = ParseState,
                   chan_state   = ChanState,
                   gc_state     = GcState,
                   stats_timer  = StatsTimer,
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
    case activate_socket(State) of
        ok -> keep_state_and_data;
        {error, Reason} ->
            shutdown(Reason, State)
    end;

idle(timeout, _Timeout, State) ->
    stop(idle_timeout, State);

idle(cast, {incoming, Packet = ?CONNECT_PACKET(ConnVar)}, State) ->
    handle_incoming(Packet,
                    fun(St = #state{chan_state = ChanState}) ->
                            %% Ensure keepalive after connected successfully.
                            Interval = emqx_channel:keepalive(ChanState),
                            NextEvent = {next_event, info, {keepalive, start, Interval}},
                            {next_state, connected, St, NextEvent}
                    end, State);

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
    handle_incoming(Packet, fun keep_state/1, State);

connected(info, Deliver = {deliver, _Topic, _Msg},
          State = #state{chan_state = ChanState}) ->
    Delivers = emqx_misc:drain_deliver([Deliver]),
    case emqx_channel:handle_out(Delivers, ChanState) of
        {ok, NChanState} ->
            keep_state(State#state{chan_state = NChanState});
        {ok, Packets, NChanState} ->
            NState = State#state{chan_state = NChanState},
            handle_outgoing(Packets, fun keep_state/1, NState);
        {error, Reason} ->
            shutdown(Reason, State)
    end;

%% Start Keepalive
connected(info, {keepalive, start, Interval}, State) ->
    case ensure_keepalive(Interval, State) of
        ignore -> keep_state(State);
        {ok, KeepAlive} ->
            keep_state(State#state{keepalive = KeepAlive});
        {error, Reason} ->
            shutdown(Reason, State)
    end;

%% Keepalive timer
connected(info, {keepalive, check}, State = #state{keepalive = KeepAlive}) ->
    case emqx_keepalive:check(KeepAlive) of
        {ok, KeepAlive1} ->
            keep_state(State#state{keepalive = KeepAlive1});
        {error, timeout} ->
            shutdown(keepalive_timeout, State);
        {error, Reason} ->
            shutdown(Reason, State)
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

%%handle({call, From}, kick, State) ->
%%    ok = gen_statem:reply(From, ok),
%%    shutdown(kicked, State);

%%handle({call, From}, discard, State) ->
%%    ok = gen_statem:reply(From, ok),
%%    shutdown(discard, State);

handle({call, From}, Req, State) ->
    ?LOG(error, "Unexpected call: ~p", [Req]),
    reply(From, ignored, State);

%% Handle cast
handle(cast, Msg, State) ->
    ?LOG(error, "Unexpected cast: ~p", [Msg]),
    keep_state(State);

%% Handle Incoming
handle(info, {Inet, _Sock, Data}, State) when Inet == tcp;
                                              Inet == ssl ->
    ?LOG(debug, "RECV ~p", [Data]),
    Oct = iolist_size(Data),
    emqx_pd:update_counter(incoming_bytes, Oct),
    ok = emqx_metrics:inc('bytes.received', Oct),
    NState = ensure_stats_timer(maybe_gc(1, Oct, State)),
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
    case activate_socket(NState) of
        ok -> keep_state(NState);
        {error, Reason} ->
            shutdown(Reason, NState)
    end;

handle(info, activate_socket, State) ->
    %% Rate limit timer expired.
    NState = State#state{conn_state = running},
    case activate_socket(NState) of
        ok ->
            keep_state(NState#state{limit_timer = undefined});
        {error, Reason} ->
            shutdown(Reason, NState)
    end;

handle(info, {inet_reply, _Sock, ok}, State) ->
    %% something sent
    keep_state(ensure_stats_timer(State));

handle(info, {inet_reply, _Sock, {error, Reason}}, State) ->
    shutdown(Reason, State);

handle(info, {timeout, Timer, emit_stats},
       State = #state{stats_timer = Timer,
                      chan_state = ChanState,
                      gc_state = GcState}) ->
    ClientId = emqx_channel:client_id(ChanState),
    ok = emqx_cm:set_conn_stats(ClientId, stats(State)),
    NState = State#state{stats_timer = undefined},
    Limits = erlang:get(force_shutdown_policy),
    case emqx_misc:conn_proc_mng_policy(Limits) of
        continue ->
            keep_state(NState);
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
    keep_state(State).

code_change(_Vsn, State, Data, _Extra) ->
    {ok, State, Data}.

terminate(Reason, _StateName, #state{transport = Transport,
                                     socket = Socket,
                                     keepalive = KeepAlive,
                                     chan_state = ChanState}) ->
    ?LOG(debug, "Terminated for ~p", [Reason]),
    ok = Transport:fast_close(Socket),
    ok = emqx_keepalive:cancel(KeepAlive),
    emqx_channel:terminate(Reason, ChanState).

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
                State = #state{chan_state = ChanState}) ->
    _ = inc_incoming_stats(Type),
    ok = emqx_metrics:inc_recv(Packet),
    ?LOG(debug, "RECV ~s", [emqx_packet:format(Packet)]),
    case emqx_channel:handle_in(Packet, ChanState) of
        {ok, NChanState} ->
            SuccFun(State#state{chan_state = NChanState});
        {ok, OutPacket, NChanState} ->
            handle_outgoing(OutPacket, SuccFun,
                            State#state{chan_state = NChanState});
        {error, Reason, NChanState} ->
            shutdown(Reason, State#state{chan_state = NChanState});
        {stop, Error, NChanState} ->
            stop(Error, State#state{chan_state = NChanState})
    end.

%%--------------------------------------------------------------------
%% Handle outgoing packets

handle_outgoing(Packets, SuccFun, State = #state{chan_state = ChanState})
  when is_list(Packets) ->
    ProtoVer = emqx_channel:proto_ver(ChanState),
    IoData = lists:foldl(
               fun(Packet = ?PACKET(Type), Acc) ->
                       ?LOG(debug, "SEND ~s", [emqx_packet:format(Packet)]),
                       _ = inc_outgoing_stats(Type),
                       [emqx_frame:serialize(Packet, ProtoVer)|Acc]
               end, [], Packets),
    send(lists:reverse(IoData), SuccFun, State);

handle_outgoing(Packet = ?PACKET(Type), SuccFun, State = #state{chan_state = ChanState}) ->
    ?LOG(debug, "SEND ~s", [emqx_packet:format(Packet)]),
    _ = inc_outgoing_stats(Type),
    ProtoVer = emqx_channel:proto_ver(ChanState),
    IoData = emqx_frame:serialize(Packet, ProtoVer),
    send(IoData, SuccFun, State).

%%--------------------------------------------------------------------
%% Send data

send(IoData, SuccFun, State = #state{transport = Transport, socket = Socket}) ->
    Oct = iolist_size(IoData),
    ok = emqx_metrics:inc('bytes.sent', Oct),
    case Transport:async_send(Socket, IoData) of
        ok -> SuccFun(maybe_gc(1, Oct, State));
        {error, Reason} ->
            shutdown(Reason, State)
    end.

%%--------------------------------------------------------------------
%% Ensure keepalive

ensure_keepalive(0, State) ->
    ignore;
ensure_keepalive(Interval, State = #state{transport = Transport,
                                          socket = Socket,
                                          chan_state = ChanState}) ->
    StatFun = fun() ->
                  case Transport:getstat(Socket, [recv_oct]) of
                      {ok, [{recv_oct, RecvOct}]} ->
                          {ok, RecvOct};
                      Error -> Error
                  end
              end,
    Backoff = emqx_zone:get_env(emqx_channel:zone(ChanState),
                                keepalive_backoff, 0.75),
    emqx_keepalive:start(StatFun, round(Interval * Backoff), {keepalive, check}).

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

%%--------------------------------------------------------------------
%% Activate Socket

activate_socket(#state{conn_state = blocked}) ->
    ok;
activate_socket(#state{transport = Transport,
                       socket = Socket,
                       active_n = N}) ->
    Transport:setopts(Socket, [{active, N}]).

%%--------------------------------------------------------------------
%% Inc incoming/outgoing stats

inc_incoming_stats(Type) ->
    emqx_pd:update_counter(recv_pkt, 1),
    case Type == ?PUBLISH of
        true ->
            emqx_pd:update_counter(recv_msg, 1),
            emqx_pd:update_counter(incoming_pubs, 1);
        false -> ok
    end.

inc_outgoing_stats(Type) ->
    emqx_pd:update_counter(send_pkt, 1),
    (Type == ?PUBLISH)
        andalso emqx_pd:update_counter(send_msg, 1).

%%--------------------------------------------------------------------
%% Ensure stats timer

ensure_stats_timer(State = #state{stats_timer = undefined,
                                  idle_timeout = IdleTimeout}) ->
    State#state{stats_timer = emqx_misc:start_timer(IdleTimeout, emit_stats)};
%% disabled or timer existed
ensure_stats_timer(State) -> State.

%%--------------------------------------------------------------------
%% Maybe GC

maybe_gc(_Cnt, _Oct, State = #state{gc_state = undefined}) ->
    State;
maybe_gc(Cnt, Oct, State = #state{gc_state = GCSt}) ->
    {_, GCSt1} = emqx_gc:run(Cnt, Oct, GCSt),
    %% TODO: gc metric?
    State#state{gc_state = GCSt1}.

%%--------------------------------------------------------------------
%% Helper functions

-compile({inline, [reply/3]}).
reply(From, Reply, State) ->
    {keep_state, State, [{reply, From, Reply}]}.

-compile({inline, [keep_state/1]}).
keep_state(State) ->
    {keep_state, State}.

-compile({inline, [shutdown/2]}).
shutdown(Reason, State) ->
    stop({shutdown, Reason}, State).

-compile({inline, [stop/2]}).
stop(Reason, State) ->
    {stop, Reason, State}.

