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

%% for Debug
-export([state/1]).

%% state callbacks
-export([ idle/3
        , connected/3
        , disconnected/3
        ]).

%% gen_statem callbacks
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
          serialize    :: fun((emqx_types:packet()) -> iodata()),
          parse_state  :: emqx_frame:parse_state(),
          proto_state  :: emqx_protocol:proto_state(),
          gc_state     :: emqx_gc:gc_state(),
          keepalive    :: maybe(emqx_keepalive:keepalive()),
          stats_timer  :: disabled | maybe(reference()),
          idle_timeout :: timeout(),
          connected    :: boolean(),
          connected_at :: erlang:timestamp()
        }).

-type(state() :: #state{}).

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

%% @doc Get infos of the channel.
-spec(info(pid() | state()) -> emqx_types:infos()).
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
            proto_state = ProtoState,
            gc_state = GCState,
            stats_timer = StatsTimer,
            idle_timeout = IdleTimeout,
            connected = Connected,
            connected_at = ConnectedAt}) ->
    ChanInfo = #{socktype => Transport:type(Socket),
                 peername => Peername,
                 sockname => Sockname,
                 conn_state => ConnState,
                 active_n => ActiveN,
                 rate_limit => limit_info(RateLimit),
                 pub_limit => limit_info(PubLimit),
                 gc_state => emqx_gc:info(GCState),
                 enable_stats => case StatsTimer of
                                     disabled   -> false;
                                     _Otherwise -> true
                                 end,
                 idle_timeout => IdleTimeout,
                 connected => Connected,
                 connected_at => ConnectedAt
                },
    maps:merge(ChanInfo, emqx_protocol:info(ProtoState)).

limit_info(undefined) ->
    undefined;
limit_info(Limit) ->
    esockd_rate_limit:info(Limit).

%% @doc Get attrs of the channel.
-spec(attrs(pid() | state()) -> emqx_types:attrs()).
attrs(CPid) when is_pid(CPid) ->
    call(CPid, attrs);
attrs(#state{transport = Transport,
             socket = Socket,
             peername = Peername,
             sockname = Sockname,
             proto_state = ProtoState,
             connected = Connected,
             connected_at = ConnectedAt}) ->
    ConnAttrs = #{socktype => Transport:type(Socket),
                  peername => Peername,
                  sockname => Sockname,
                  connected => Connected,
                  connected_at => ConnectedAt},
    maps:merge(ConnAttrs, emqx_protocol:attrs(ProtoState)).

%% @doc Get stats of the channel.
-spec(stats(pid() | state()) -> emqx_types:stats()).
stats(CPid) when is_pid(CPid) ->
    call(CPid, stats);
stats(#state{transport = Transport,
             socket = Socket,
             proto_state = ProtoState}) ->
    SockStats = case Transport:getstat(Socket, ?SOCK_STATS) of
                    {ok, Ss}   -> Ss;
                    {error, _} -> []
                end,
    ChanStats = [{Name, emqx_pd:get_counter(Name)} || Name <- ?CHAN_STATS],
    SessStats = emqx_session:stats(emqx_protocol:info(session, ProtoState)),
    lists:append([SockStats, ChanStats, SessStats, emqx_misc:proc_stats()]).

state(CPid) -> call(CPid, get_state).

%% @private
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
    EnableStats = emqx_zone:get_env(Zone, enable_stats, true),
    StatsTimer = if EnableStats -> undefined; ?Otherwise -> disabled end,
    IdleTimout = emqx_zone:get_env(Zone, idle_timeout, 30000),
    ok = emqx_misc:init_proc_mng_policy(Zone),
    State = #state{transport    = Transport,
                   socket       = Socket,
                   peername     = Peername,
                   sockname     = Sockname,
                   conn_state   = running,
                   active_n     = ActiveN,
                   rate_limit   = RateLimit,
                   pub_limit    = PubLimit,
                   parse_state  = ParseState,
                   proto_state  = ProtoState,
                   gc_state     = GcState,
                   stats_timer  = StatsTimer,
                   idle_timeout = IdleTimout,
                   connected    = false
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

idle(cast, {incoming, Packet = ?CONNECT_PACKET(
                                  #mqtt_packet_connect{
                                     proto_ver = ProtoVer}
                                 )}, State) ->
    State1 = State#state{serialize = serialize_fun(ProtoVer)},
    handle_incoming(Packet, fun(NewSt) ->
                                    {next_state, connected, NewSt}
                            end, State1);

idle(cast, {incoming, Packet}, State) ->
    ?LOG(warning, "Unexpected incoming: ~p", [Packet]),
    shutdown(unexpected_incoming_packet, State);

idle(EventType, Content, State) ->
    ?HANDLE(EventType, Content, State).

%%--------------------------------------------------------------------
%% Connected State

connected(enter, _PrevSt, State = #state{proto_state = ProtoState}) ->
    NState = State#state{connected = true,
                         connected_at = os:timestamp()},
    ClientId = emqx_protocol:info(client_id, ProtoState),
    ok = emqx_cm:register_channel(ClientId),
    ok = emqx_cm:set_chan_attrs(ClientId, info(NState)),
    %% Ensure keepalive after connected successfully.
    Interval = emqx_protocol:info(keepalive, ProtoState),
    case ensure_keepalive(Interval, NState) of
        ignore -> keep_state(NState);
        {ok, KeepAlive} ->
            keep_state(NState#state{keepalive = KeepAlive});
        {error, Reason} ->
            shutdown(Reason, NState)
    end;

connected(cast, {incoming, ?PACKET(?CONNECT)}, State) ->
    Shutdown = fun(NewSt) -> shutdown(?RC_PROTOCOL_ERROR, NewSt) end,
    handle_outgoing(?DISCONNECT_PACKET(?RC_PROTOCOL_ERROR), Shutdown, State);

connected(cast, {incoming, Packet}, State) when is_record(Packet, mqtt_packet) ->
    handle_incoming(Packet, fun keep_state/1, State);

connected(info, Deliver = {deliver, _Topic, _Msg},
          State = #state{proto_state = ProtoState}) ->
    Delivers = emqx_misc:drain_deliver([Deliver]),
    case emqx_protocol:handle_deliver(Delivers, ProtoState) of
        {ok, NProtoState} ->
            keep_state(State#state{proto_state = NProtoState});
        {ok, Packets, NProtoState} ->
            NState = State#state{proto_state = NProtoState},
            handle_outgoing(Packets, fun keep_state/1, NState);
        {error, Reason} ->
            shutdown(Reason, State);
        {error, Reason, NProtoState} ->
            shutdown(Reason, State#state{proto_state = NProtoState})
    end;

%% TODO: Improve later.
connected(info, {subscribe, TopicFilters}, State) ->
    handle_request({subscribe, TopicFilters}, State);

connected(info, {unsubscribe, TopicFilters}, State) ->
    handle_request({unsubscribe, TopicFilters}, State);

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
    %% CleanStart is true
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

handle({call, From}, get_state, State) ->
    reply(From, State, State);

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

%% Handle incoming data
handle(info, {Inet, _Sock, Data}, State) when Inet == tcp;
                                              Inet == ssl ->
    Oct = iolist_size(Data),
    ?LOG(debug, "RECV ~p", [Data]),
    emqx_pd:update_counter(incoming_bytes, Oct),
    ok = emqx_metrics:inc('bytes.received', Oct),
    NState = maybe_gc(1, Oct, State),
    process_incoming(Data, ensure_stats_timer(NState));

handle(info, {Error, _Sock, Reason}, State) when Error == tcp_error;
                                                 Error == ssl_error ->
    shutdown(Reason, State);

handle(info, {Closed, _Sock}, State) when Closed == tcp_closed;
                                          Closed == ssl_closed ->
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
                      proto_state = ProtoState,
                      gc_state    = GcState}) ->
    ClientId = emqx_protocol:info(client_id, ProtoState),
    ok = emqx_cm:set_chan_stats(ClientId, stats(State)),
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

handle(info, {timeout, Timer, Msg},
       State = #state{proto_state = ProtoState}) ->
    case emqx_protocol:handle_timeout(Timer, Msg, ProtoState) of
        {ok, NProtoState} ->
            keep_state(State#state{proto_state = NProtoState});
        {ok, Packets, NProtoState} ->
            handle_outgoing(Packets, fun keep_state/1,
                            State#state{proto_state = NProtoState});
        {error, Reason} ->
            shutdown(Reason, State);
        {error, Reason, NProtoState} ->
            shutdown(Reason, State#state{proto_state = NProtoState})
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

terminate(Reason, _StateName, #state{transport   = Transport,
                                     socket      = Socket,
                                     keepalive   = KeepAlive,
                                     proto_state = ProtoState}) ->
    ?LOG(debug, "Terminated for ~p", [Reason]),
    ok = Transport:fast_close(Socket),
    ok = emqx_keepalive:cancel(KeepAlive),
    emqx_protocol:terminate(Reason, ProtoState).

%%--------------------------------------------------------------------
%% Handle internal request

handle_request(Req, State = #state{proto_state = ProtoState}) ->
    case emqx_protocol:handle_req(Req, ProtoState) of
        {ok, _Result, NProtoState} -> %% TODO:: how to handle the result?
            keep_state(State#state{proto_state = NProtoState});
        {error, Reason, NProtoState} ->
            shutdown(Reason, State#state{proto_state = NProtoState})
    end.

%%--------------------------------------------------------------------
%% Process incoming data

-compile({inline, [process_incoming/2]}).
process_incoming(Data, State) ->
    process_incoming(Data, [], State).

process_incoming(<<>>, Packets, State) ->
    {keep_state, State, next_incoming_events(Packets)};

process_incoming(Data, Packets, State = #state{parse_state = ParseState}) ->
    try emqx_frame:parse(Data, ParseState) of
        {ok, NParseState} ->
            NState = State#state{parse_state = NParseState},
            {keep_state, NState, next_incoming_events(Packets)};
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

next_incoming_events(Packets) when is_list(Packets) ->
    [next_event(cast, {incoming, Packet})
     || Packet <- lists:reverse(Packets)].

%%--------------------------------------------------------------------
%% Handle incoming packet

handle_incoming(Packet = ?PACKET(Type), SuccFun,
                State = #state{proto_state = ProtoState}) ->
    _ = inc_incoming_stats(Type),
    ok = emqx_metrics:inc_recv(Packet),
    ?LOG(debug, "RECV ~s", [emqx_packet:format(Packet)]),
    case emqx_protocol:handle_in(Packet, ProtoState) of
        {ok, NProtoState} ->
            SuccFun(State#state{proto_state = NProtoState});
        {ok, OutPackets, NProtoState} ->
            handle_outgoing(OutPackets, SuccFun,
                            State#state{proto_state = NProtoState});
        {error, Reason, NProtoState} ->
            shutdown(Reason, State#state{proto_state = NProtoState});
        {error, Reason, OutPacket, NProtoState} ->
            Shutdown = fun(NewSt) -> shutdown(Reason, NewSt) end,
            handle_outgoing(OutPacket, Shutdown, State#state{proto_state = NProtoState});
        {stop, Error, NProtoState} ->
            stop(Error, State#state{proto_state = NProtoState})
    end.

%%--------------------------------------------------------------------
%% Handle outgoing packets

handle_outgoing(Packets, SuccFun, State = #state{serialize = Serialize})
  when is_list(Packets) ->
    send(lists:map(Serialize, Packets), SuccFun, State);

handle_outgoing(Packet, SuccFun, State = #state{serialize = Serialize}) ->
    send(Serialize(Packet), SuccFun, State).

%%--------------------------------------------------------------------
%% Serialize fun

serialize_fun(ProtoVer) ->
    fun(Packet = ?PACKET(Type)) ->
        ?LOG(debug, "SEND ~s", [emqx_packet:format(Packet)]),
        _ = inc_outgoing_stats(Type),
        emqx_frame:serialize(Packet, ProtoVer)
    end.

%%--------------------------------------------------------------------
%% Send data

send(IoData, SuccFun, State = #state{transport = Transport,
                                     socket = Socket}) ->
    Oct = iolist_size(IoData),
    ok = emqx_metrics:inc('bytes.sent', Oct),
    case Transport:async_send(Socket, IoData) of
        ok -> SuccFun(maybe_gc(1, Oct, State));
        {error, Reason} ->
            shutdown(Reason, State)
    end.

%%--------------------------------------------------------------------
%% Ensure keepalive

ensure_keepalive(0, _State) ->
    ignore;
ensure_keepalive(Interval, #state{transport   = Transport,
                                  socket      = Socket,
                                  proto_state = ProtoState}) ->
    StatFun = fun() ->
                  case Transport:getstat(Socket, [recv_oct]) of
                      {ok, [{recv_oct, RecvOct}]} ->
                          {ok, RecvOct};
                      Error -> Error
                  end
              end,
    Backoff = emqx_zone:get_env(emqx_protocol:info(zone, ProtoState),
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
            setelement(Pos, State#state{conn_state = blocked,
                                        limit_timer = TRef}, Rl1)
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

-compile({inline,
          [ inc_incoming_stats/1
          , inc_outgoing_stats/1
          ]}).

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

ensure_stats_timer(State = #state{stats_timer  = undefined,
                                  idle_timeout = IdleTimeout}) ->
    TRef = emqx_misc:start_timer(IdleTimeout, emit_stats),
    State#state{stats_timer = TRef};
%% disabled or timer existed
ensure_stats_timer(State) -> State.

%%--------------------------------------------------------------------
%% Maybe GC

maybe_gc(_Cnt, _Oct, State = #state{gc_state = undefined}) ->
    State;
maybe_gc(Cnt, Oct, State = #state{gc_state = GCSt}) ->
    {Ok, GCSt1} = emqx_gc:run(Cnt, Oct, GCSt),
    Ok andalso emqx_metrics:inc('channel.gc.cnt'),
    State#state{gc_state = GCSt1}.

%%--------------------------------------------------------------------
%% Helper functions

-compile({inline,
          [ reply/3
          , keep_state/1
          , next_event/2
          , shutdown/2
          , stop/2
          ]}).

reply(From, Reply, State) ->
    {keep_state, State, [{reply, From, Reply}]}.

keep_state(State) ->
    {keep_state, State}.

next_event(Type, Content) ->
    {next_event, Type, Content}.

shutdown(Reason, State) ->
    stop({shutdown, Reason}, State).

stop(Reason, State) ->
    {stop, Reason, State}.
