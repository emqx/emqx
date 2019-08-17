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

-logger_header("[Connection]").

-export([start_link/3]).

%% APIs
-export([ info/1
        , attrs/1
        , stats/1
        ]).

%% For Debug
-export([state/1]).

-export([ kick/1
        , discard/1
        , takeover/2
        ]).

%% state callbacks
-export([ idle/3
        , connected/3
        , disconnected/3
        , takeovering/3
        ]).

%% gen_statem callbacks
-export([ init/1
        , callback_mode/0
        , code_change/4
        , terminate/3
        ]).

-record(state, {
          transport   :: esockd:transport(),
          socket      :: esockd:socket(),
          peername    :: emqx_types:peername(),
          sockname    :: emqx_types:peername(),
          conn_state  :: running | blocked,
          active_n    :: pos_integer(),
          rate_limit  :: maybe(esockd_rate_limit:bucket()),
          pub_limit   :: maybe(esockd_rate_limit:bucket()),
          limit_timer :: maybe(reference()),
          parse_state :: emqx_frame:parse_state(),
          serialize   :: fun((emqx_types:packet()) -> iodata()),
          chan_state  :: emqx_channel:channel(),
          keepalive   :: maybe(emqx_keepalive:keepalive())
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
            chan_state = ChanState}) ->
    ConnInfo = #{socktype => Transport:type(Socket),
                 peername => Peername,
                 sockname => Sockname,
                 conn_state => ConnState,
                 active_n => ActiveN,
                 rate_limit => limit_info(RateLimit),
                 pub_limit => limit_info(PubLimit)
                },
    maps:merge(ConnInfo, emqx_channel:info(ChanState)).

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
             chan_state = ChanState}) ->
    ConnAttrs = #{socktype => Transport:type(Socket),
                  peername => Peername,
                  sockname => Sockname
                 },
    maps:merge(ConnAttrs, emqx_channel:attrs(ChanState)).

%% @doc Get stats of the channel.
-spec(stats(pid() | state()) -> emqx_types:stats()).
stats(CPid) when is_pid(CPid) ->
    call(CPid, stats);
stats(#state{transport = Transport,
             socket = Socket,
             chan_state = ChanState}) ->
    SockStats = case Transport:getstat(Socket, ?SOCK_STATS) of
                    {ok, Ss}   -> Ss;
                    {error, _} -> []
                end,
    ChanStats = [{Name, emqx_pd:get_counter(Name)} || Name <- ?CHAN_STATS],
    SessStats = emqx_session:stats(emqx_channel:info(session, ChanState)),
    lists:append([SockStats, ChanStats, SessStats, emqx_misc:proc_stats()]).

state(CPid) ->
    call(CPid, get_state).

-spec(kick(pid()) -> ok).
kick(CPid) ->
    call(CPid, kick).

-spec(discard(pid()) -> ok).
discard(CPid) ->
    gen_statem:cast(CPid, discard).

%% TODO:
-spec(takeover(pid(), 'begin'|'end') -> {ok, Result :: term()}).
takeover(CPid, Phase) ->
    gen_statem:call(CPid, {takeover, Phase}).

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
    ChanState = emqx_channel:init(#{peername => Peername,
                                    sockname => Sockname,
                                    peercert => Peercert,
                                    conn_mod => ?MODULE}, Options),
    IdleTimout = emqx_zone:get_env(Zone, idle_timeout, 30000),
    State = #state{transport    = Transport,
                   socket       = Socket,
                   peername     = Peername,
                   sockname     = Sockname,
                   conn_state   = running,
                   active_n     = ActiveN,
                   rate_limit   = RateLimit,
                   pub_limit    = PubLimit,
                   parse_state  = ParseState,
                   chan_state   = ChanState
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

connected(enter, _PrevSt, State = #state{chan_state = ChanState}) ->
    ClientId = emqx_channel:info(client_id, ChanState),
    ok = emqx_cm:register_channel(ClientId),
    ok = emqx_cm:set_chan_attrs(ClientId, info(State)),
    %% Ensure keepalive after connected successfully.
    Interval = emqx_channel:info(keepalive, ChanState),
    case ensure_keepalive(Interval, State) of
        ignore -> keep_state(State);
        {ok, KeepAlive} ->
            keep_state(State#state{keepalive = KeepAlive});
        {error, Reason} ->
            shutdown(Reason, State)
    end;

connected(cast, {incoming, Packet = ?PACKET(?CONNECT)}, State) ->
    ?LOG(warning, "Unexpected connect: ~p", [Packet]),
    shutdown(unexpected_incoming_connect, State);

connected(cast, {incoming, Packet}, State) when is_record(Packet, mqtt_packet) ->
    handle_incoming(Packet, fun keep_state/1, State);

connected(info, Deliver = {deliver, _Topic, _Msg},
          State = #state{chan_state = ChanState}) ->
    case emqx_channel:handle_out(Deliver, ChanState) of
        {ok, NChanState} ->
            keep_state(State#state{chan_state = NChanState});
        {ok, Packets, NChanState} ->
            NState = State#state{chan_state = NChanState},
            handle_outgoing(Packets, fun keep_state/1, NState);
        {stop, Reason, NChanState} ->
            stop(Reason, State#state{chan_state = NChanState})
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
    %% CleanStart is true
    keep_state_and_data;

disconnected(EventType, Content, State) ->
    ?HANDLE(EventType, Content, State).

%%--------------------------------------------------------------------
%% Takeovering State

takeovering(enter, _PreState, State) ->
    {keep_state, State};

takeovering(EventType, Content, State) ->
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

handle({call, From}, kick, State) ->
    ok = gen_statem:reply(From, ok),
    shutdown(kicked, State);

handle({call, From}, {takeover, 'begin'}, State = #state{chan_state = ChanState}) ->
    {ok, Session, NChanState} = emqx_channel:takeover('begin', ChanState),
    ok = gen_statem:reply(From, {ok, Session}),
    {next_state, takeovering, State#state{chan_state = NChanState}};

handle({call, From}, {takeover, 'end'}, State = #state{chan_state = ChanState}) ->
    {ok, Delivers, NChanState} = emqx_channel:takeover('end', ChanState),
    ok = gen_statem:reply(From, {ok, Delivers}),
    shutdown(takeovered, State#state{chan_state = NChanState});

handle({call, From}, Req, State = #state{chan_state = ChanState}) ->
    case emqx_channel:handle_call(Req, ChanState) of
        {ok, Reply, NChanState} ->
            reply(From, Reply, State#state{chan_state = NChanState});
        {stop, Reason, Reply, NChanState} ->
            ok = gen_statem:reply(From, Reply),
            stop(Reason, State#state{chan_state = NChanState})
    end;

%% Handle cast
handle(cast, Msg, State = #state{chan_state = ChanState}) ->
    case emqx_channel:handle_cast(Msg, ChanState) of
        {ok, NChanState} ->
            keep_state(State#state{chan_state = NChanState});
        {stop, Reason, NChanState} ->
            stop(Reason, State#state{chan_state = NChanState})
    end;

%% Handle incoming data
handle(info, {Inet, _Sock, Data}, State = #state{chan_state = ChanState})
  when Inet == tcp; Inet == ssl ->
    Oct = iolist_size(Data),
    ?LOG(debug, "RECV ~p", [Data]),
    emqx_pd:update_counter(incoming_bytes, Oct),
    ok = emqx_metrics:inc('bytes.received', Oct),
    NChanState = emqx_channel:ensure_timer(
                   emit_stats, emqx_channel:gc(1, Oct, ChanState)),
    process_incoming(Data, State#state{chan_state = NChanState});

handle(info, {Error, _Sock, Reason}, State)
  when Error == tcp_error; Error == ssl_error ->
    shutdown(Reason, State);

handle(info, {Closed, _Sock}, State = #state{chan_state = ChanState})
  when Closed == tcp_closed; Closed == ssl_closed ->
    case emqx_channel:info(session, ChanState) of
        undefined -> shutdown(closed, State);
        Session ->
            case emqx_session:info(clean_start, Session) of
                true -> shutdown(closed, State);
                false -> {next_state, disconnected, State}
            end
    end;

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

handle(info, {inet_reply, _Sock, ok}, State = #state{chan_state = ChanState}) ->
    %% something sent
    NChanState = emqx_channel:ensure_timer(emit_stats, ChanState),
    keep_state(State#state{chan_state = NChanState});

handle(info, {inet_reply, _Sock, {error, Reason}}, State) ->
    shutdown(Reason, State);

handle(info, {timeout, TRef, emit_stats}, State) when is_reference(TRef) ->
    handle_timeout(TRef, {emit_stats, stats(State)}, State);

handle(info, {timeout, TRef, Msg}, State) when is_reference(TRef) ->
    handle_timeout(TRef, Msg, State);

handle(info, {shutdown, conflict, {ClientId, NewPid}}, State) ->
    ?LOG(warning, "Clientid '~s' conflict with ~p", [ClientId, NewPid]),
    shutdown(conflict, State);

handle(info, {shutdown, Reason}, State) ->
    shutdown(Reason, State);

handle(info, Info, State = #state{chan_state = ChanState}) ->
    case emqx_channel:handle_info(Info, ChanState) of
        {ok, NChanState} ->
            keep_state(State#state{chan_state = NChanState});
        {stop, Reason, NChanState} ->
            stop(Reason, State#state{chan_state = NChanState})
    end.

code_change(_Vsn, State, Data, _Extra) ->
    {ok, State, Data}.

terminate(Reason, _StateName, #state{transport  = Transport,
                                     socket     = Socket,
                                     keepalive  = KeepAlive,
                                     chan_state = ChanState}) ->
    ?LOG(debug, "Terminated for ~p", [Reason]),
    ok = Transport:fast_close(Socket),
    KeepAlive =/= undefined
        andalso emqx_keepalive:cancel(KeepAlive),
    emqx_channel:terminate(Reason, ChanState).

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
    [next_event(cast, {incoming, Packet}) || Packet <- Packets].

%%--------------------------------------------------------------------
%% Handle incoming packet

handle_incoming(Packet = ?PACKET(Type), SuccFun,
                State = #state{chan_state = ChanState}) ->
    _ = inc_incoming_stats(Type),
    ok = emqx_metrics:inc_recv(Packet),
    ?LOG(debug, "RECV ~s", [emqx_packet:format(Packet)]),
    case emqx_channel:handle_in(Packet, ChanState) of
        {ok, NChanState} ->
            SuccFun(State#state{chan_state= NChanState});
        {ok, OutPackets, NChanState} ->
            handle_outgoing(OutPackets, SuccFun, State#state{chan_state = NChanState});
        {stop, Reason, NChanState} ->
            stop(Reason, State#state{chan_state = NChanState});
        {stop, Reason, OutPacket, NChanState} ->
            Shutdown = fun(NewSt) -> shutdown(Reason, NewSt) end,
            handle_outgoing(OutPacket, Shutdown, State#state{chan_state = NChanState})
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
        ok -> SuccFun(State);
        {error, Reason} ->
            shutdown(Reason, State)
    end.

%%--------------------------------------------------------------------
%% Handle timeout

handle_timeout(TRef, Msg, State = #state{chan_state = ChanState}) ->
    case emqx_channel:timeout(TRef, Msg, ChanState) of
        {ok, NChanState} ->
            keep_state(State#state{chan_state = NChanState});
        {ok, Packets, NChanState} ->
            handle_outgoing(Packets, fun keep_state/1,
                            State#state{chan_state = NChanState});
        {stop, Reason, NChanState} ->
            stop(Reason, State#state{chan_state = NChanState})
    end.

%%--------------------------------------------------------------------
%% Ensure keepalive

ensure_keepalive(0, _State) ->
    ignore;
ensure_keepalive(Interval, #state{transport  = Transport,
                                  socket     = Socket,
                                  chan_state = ChanState}) ->
    StatFun = fun() ->
                  case Transport:getstat(Socket, [recv_oct]) of
                      {ok, [{recv_oct, RecvOct}]} ->
                          {ok, RecvOct};
                      Error -> Error
                  end
              end,
    Backoff = emqx_zone:get_env(emqx_channel:info(zone, ChanState),
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

