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
        , state/1
        ]).

-export([call/2]).

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

-record(connection, {
          %% TCP/TLS Transport
          transport :: esockd:transport(),
          %% TCP/TLS Socket
          socket :: esockd:socket(),
          %% Peername of the connection
          peername :: emqx_types:peername(),
          %% Sockname of the connection
          sockname :: emqx_types:peername(),
          %% The {active, N} option
          active_n :: pos_integer(),
          %% The active state
          active_state :: running | blocked,
          %% Rate Limit
          rate_limit :: maybe(esockd_rate_limit:bucket()),
          %% Publish Limit
          pub_limit :: maybe(esockd_rate_limit:bucket()),
          %% Limit Timer
          limit_timer :: maybe(reference()),
          %% Parser State
          parse_state :: emqx_frame:parse_state(),
          %% Serialize function
          serialize :: fun((emqx_types:packet()) -> iodata()),
          %% Channel State
          chan_state :: emqx_channel:channel()
        }).

-type(connection() :: #connection{}).

-define(ACTIVE_N, 100).
-define(HANDLE(T, C, D), handle((T), (C), (D))).
-define(ATTR_KEYS, [socktype, peername, sockname]).
-define(INFO_KEYS, [socktype, peername, sockname, active_n, active_state,
                    rate_limit, pub_limit]).
-define(CONN_STATS, [recv_pkt, recv_msg, send_pkt, send_msg]).
-define(SOCK_STATS, [recv_oct, recv_cnt, send_oct, send_cnt, send_pend]).

%% @doc Start the connection.
-spec(start_link(esockd:transport(), esockd:socket(), proplists:proplist())
      -> {ok, pid()}).
start_link(Transport, Socket, Options) ->
    {ok, proc_lib:spawn_link(?MODULE, init, [{Transport, Socket, Options}])}.

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

%% @doc Get infos of the connection.
-spec(info(pid()|connection()) -> emqx_types:infos()).
info(CPid) when is_pid(CPid) ->
    call(CPid, info);
info(Conn = #connection{chan_state = ChanState}) ->
    ConnInfo = info(?INFO_KEYS, Conn),
    ChanInfo = emqx_channel:info(ChanState),
    maps:merge(ChanInfo, #{connection => maps:from_list(ConnInfo)}).

info(Keys, Conn) when is_list(Keys) ->
    [{Key, info(Key, Conn)} || Key <- Keys];
info(socktype, #connection{transport = Transport, socket = Socket}) ->
    Transport:type(Socket);
info(peername, #connection{peername = Peername}) ->
    Peername;
info(sockname, #connection{sockname = Sockname}) ->
    Sockname;
info(active_n, #connection{active_n = ActiveN}) ->
    ActiveN;
info(active_state, #connection{active_state = ActiveSt}) ->
    ActiveSt;
info(rate_limit, #connection{rate_limit = RateLimit}) ->
    limit_info(RateLimit);
info(pub_limit, #connection{pub_limit = PubLimit}) ->
    limit_info(PubLimit);
info(chan_state, #connection{chan_state = ChanState}) ->
    emqx_channel:info(ChanState).

limit_info(Limit) ->
    emqx_misc:maybe_apply(fun esockd_rate_limit:info/1, Limit).

%% @doc Get attrs of the connection.
-spec(attrs(pid()|connection()) -> emqx_types:attrs()).
attrs(CPid) when is_pid(CPid) ->
    call(CPid, attrs);
attrs(Conn = #connection{chan_state = ChanState}) ->
    ConnAttrs = info(?ATTR_KEYS, Conn),
    ChanAttrs = emqx_channel:attrs(ChanState),
    maps:merge(ChanAttrs, #{connection => maps:from_list(ConnAttrs)}).

%% @doc Get stats of the channel.
-spec(stats(pid()|connection()) -> emqx_types:stats()).
stats(CPid) when is_pid(CPid) ->
    call(CPid, stats);
stats(#connection{transport  = Transport,
                  socket     = Socket,
                  chan_state = ChanState}) ->
    ProcStats = emqx_misc:proc_stats(),
    SockStats = case Transport:getstat(Socket, ?SOCK_STATS) of
                    {ok, Ss}   -> Ss;
                    {error, _} -> []
                end,
    ConnStats = [{Name, emqx_pd:get_counter(Name)} || Name <- ?CONN_STATS],
    ChanStats = emqx_channel:stats(ChanState),
    lists:append([ProcStats, SockStats, ConnStats, ChanStats]).

%% For debug
-spec(state(pid()) -> connection()).
state(CPid) -> call(CPid, state).

%% kick|discard|takeover
-spec(call(pid(), Req :: term()) -> Reply :: term()).
call(CPid, Req) -> gen_statem:call(CPid, Req).

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
    State = #connection{transport    = Transport,
                        socket       = Socket,
                        peername     = Peername,
                        sockname     = Sockname,
                        active_n     = ActiveN,
                        active_state = running,
                        rate_limit   = RateLimit,
                        pub_limit    = PubLimit,
                        parse_state  = ParseState,
                        chan_state   = ChanState
                       },
    gen_statem:enter_loop(?MODULE, [{hibernate_after, 2 * IdleTimout}],
                          idle, State, self(), [IdleTimout]).

init_limiter(undefined) -> undefined;
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
    shutdown(idle_timeout, State);

idle(cast, {incoming, Packet = ?CONNECT_PACKET(ConnPkt)}, State) ->
    #mqtt_packet_connect{proto_ver = ProtoVer} = ConnPkt,
    NState = State#connection{serialize = serialize_fun(ProtoVer)},
    SuccFun = fun(NewSt) -> {next_state, connected, NewSt} end,
    handle_incoming(Packet, SuccFun, NState);

idle(cast, {incoming, Packet}, State) ->
    ?LOG(warning, "Unexpected incoming: ~p", [Packet]),
    shutdown(unexpected_incoming_packet, State);

idle(EventType, Content, State) ->
    ?HANDLE(EventType, Content, State).

%%--------------------------------------------------------------------
%% Connected State

connected(enter, _PrevSt, State) ->
    ok = register_self(State),
    keep_state_and_data;

connected(cast, {incoming, Packet}, State) when is_record(Packet, mqtt_packet) ->
    handle_incoming(Packet, fun keep_state/1, State);

connected(info, Deliver = {deliver, _Topic, _Msg}, State) ->
    handle_deliver(emqx_misc:drain_deliver([Deliver]), State);

connected(EventType, Content, State) ->
    ?HANDLE(EventType, Content, State).

%%--------------------------------------------------------------------
%% Disconnected State

disconnected(enter, _, State = #connection{chan_state = ChanState}) ->
    case emqx_channel:handle_info(disconnected, ChanState) of
        {ok, NChanState} ->
            ok = register_self(State#connection{chan_state = NChanState}),
            keep_state(State#connection{chan_state = NChanState});
        {stop, Reason, NChanState} ->
            stop(Reason, State#connection{chan_state = NChanState})
    end;

disconnected(info, Deliver = {deliver, _Topic, _Msg}, State) ->
    handle_deliver([Deliver], State);

disconnected(EventType, Content, State) ->
    ?HANDLE(EventType, Content, State).

%%--------------------------------------------------------------------
%% Handle call

handle({call, From}, info, State) ->
    reply(From, info(State), State);

handle({call, From}, attrs, State) ->
    reply(From, attrs(State), State);

handle({call, From}, stats, State) ->
    reply(From, stats(State), State);

handle({call, From}, state, State) ->
    reply(From, State, State);

handle({call, From}, Req, State = #connection{chan_state = ChanState}) ->
    case emqx_channel:handle_call(Req, ChanState) of
        {ok, Reply, NChanState} ->
            reply(From, Reply, State#connection{chan_state = NChanState});
        {stop, Reason, Reply, NChanState} ->
            ok = gen_statem:reply(From, Reply),
            stop(Reason, State#connection{chan_state = NChanState})
    end;

%%--------------------------------------------------------------------
%% Handle cast

handle(cast, Msg, State = #connection{chan_state = ChanState}) ->
    case emqx_channel:handle_cast(Msg, ChanState) of
        {ok, NChanState} ->
            keep_state(State#connection{chan_state = NChanState});
        {stop, Reason, NChanState} ->
            stop(Reason, State#connection{chan_state = NChanState})
    end;

%%--------------------------------------------------------------------
%% Handle info

%% Handle incoming data
handle(info, {Inet, _Sock, Data}, State = #connection{chan_state = ChanState})
  when Inet == tcp; Inet == ssl ->
    ?LOG(debug, "RECV ~p", [Data]),
    Oct = iolist_size(Data),
    emqx_pd:update_counter(incoming_bytes, Oct),
    ok = emqx_metrics:inc('bytes.received', Oct),
    NChanState = emqx_channel:received(Oct, ChanState),
    NState = State#connection{chan_state = NChanState},
    process_incoming(Data, NState);

handle(info, {Error, _Sock, Reason}, State)
  when Error == tcp_error; Error == ssl_error ->
    shutdown(Reason, State);

handle(info, {Closed, _Sock}, State)
  when Closed == tcp_closed; Closed == ssl_closed ->
    {next_state, disconnected, State};

handle(info, {Passive, _Sock}, State)
  when Passive == tcp_passive; Passive == ssl_passive ->
    %% Rate limit here:)
    NState = ensure_rate_limit(State),
    case activate_socket(NState) of
        ok -> keep_state(NState);
        {error, Reason} ->
            shutdown(Reason, NState)
    end;

handle(info, activate_socket, State) ->
    %% Rate limit timer expired.
    NState = State#connection{active_state = running,
                              limit_timer  = undefined
                             },
    case activate_socket(NState) of
        ok -> keep_state(NState);
        {error, Reason} ->
            shutdown(Reason, NState)
    end;

handle(info, {inet_reply, _Sock, ok}, _State) ->
    %% something sent
    keep_state_and_data;

handle(info, {inet_reply, _Sock, {error, Reason}}, State) ->
    shutdown(Reason, State);

handle(info, {timeout, TRef, keepalive},
       State = #connection{transport = Transport, socket = Socket}) ->
    case Transport:getstat(Socket, [recv_oct]) of
        {ok, [{recv_oct, RecvOct}]} ->
            handle_timeout(TRef, {keepalive, RecvOct}, State);
        {error, Reason} ->
            shutdown(Reason, State)
    end;

handle(info, {timeout, TRef, emit_stats}, State) ->
    handle_timeout(TRef, {emit_stats, stats(State)}, State);

handle(info, {timeout, TRef, Msg}, State) ->
    handle_timeout(TRef, Msg, State);

handle(info, {shutdown, Reason}, State) ->
    shutdown(Reason, State);

handle(info, Info, State = #connection{chan_state = ChanState}) ->
    case emqx_channel:handle_info(Info, ChanState) of
        {ok, NChanState} ->
            keep_state(State#connection{chan_state = NChanState});
        {stop, Reason, NChanState} ->
            stop(Reason, State#connection{chan_state = NChanState})
    end.

code_change(_Vsn, State, Data, _Extra) ->
    {ok, State, Data}.

terminate(Reason, _StateName, State) ->
    #connection{transport  = Transport,
                socket     = Socket,
                chan_state = ChanState} = State,
    ?LOG(debug, "Terminated for ~p", [Reason]),
    ok = Transport:fast_close(Socket),
    emqx_channel:terminate(Reason, ChanState).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

register_self(State = #connection{chan_state = ChanState}) ->
    emqx_channel:handle_cast({register, attrs(State), stats(State)}, ChanState).

%%--------------------------------------------------------------------
%% Process incoming data

-compile({inline, [process_incoming/2]}).
process_incoming(Data, State) ->
    process_incoming(Data, [], State).

process_incoming(<<>>, Packets, State) ->
    {keep_state, State, next_incoming_events(Packets)};

process_incoming(Data, Packets, State = #connection{parse_state = ParseState, chan_state = ChanState}) ->
    try emqx_frame:parse(Data, ParseState) of
        {ok, NParseState} ->
            NState = State#connection{parse_state = NParseState},
            {keep_state, NState, next_incoming_events(Packets)};
        {ok, Packet, Rest, NParseState} ->
            NState = State#connection{parse_state = NParseState},
            process_incoming(Rest, [Packet|Packets], NState);
        {error, Reason} ->
            shutdown(Reason, State)
    catch
        error:Reason:Stk ->
            ?LOG(error, "Parse failed for ~p~n\
                 Stacktrace:~p~nError data:~p", [Reason, Stk, Data]),
            case emqx_channel:handle_out({disconnect, emqx_reason_codes:mqtt_frame_error(Reason)}, ChanState) of
                {stop, Reason0, OutPackets, NChanState} ->
                    Shutdown = fun(NewSt) -> stop(Reason0, NewSt) end,
                    NState = State#connection{chan_state = NChanState},
                    handle_outgoing(OutPackets, Shutdown, NState);
                {stop, Reason0, NChanState} ->
                    stop(Reason0, State#connection{chan_state = NChanState})
            end
    end.

-compile({inline, [next_incoming_events/1]}).
next_incoming_events(Packets) ->
    [next_event(cast, {incoming, Packet}) || Packet <- Packets].

%%--------------------------------------------------------------------
%% Handle incoming packet

handle_incoming(Packet = ?PACKET(Type), SuccFun,
                State = #connection{chan_state = ChanState}) ->
    _ = inc_incoming_stats(Type),
    ok = emqx_metrics:inc_recv(Packet),
    ?LOG(debug, "RECV ~s", [emqx_packet:format(Packet)]),
    case emqx_channel:handle_in(Packet, ChanState) of
        {ok, NChanState} ->
            SuccFun(State#connection{chan_state= NChanState});
        {ok, OutPackets, NChanState} ->
            handle_outgoing(OutPackets, SuccFun,
                            State#connection{chan_state = NChanState});
        {stop, Reason, NChanState} ->
            stop(Reason, State#connection{chan_state = NChanState});
        {stop, Reason, OutPackets, NChanState} ->
            Shutdown = fun(NewSt) -> stop(Reason, NewSt) end,
            NState = State#connection{chan_state = NChanState},
            handle_outgoing(OutPackets, Shutdown, NState)
    end.

%%-------------------------------------------------------------------
%% Handle deliver

handle_deliver(Delivers, State = #connection{chan_state = ChanState}) ->
    case emqx_channel:handle_out({deliver, Delivers}, ChanState) of
        {ok, NChanState} ->
            keep_state(State#connection{chan_state = NChanState});
        {ok, Packets, NChanState} ->
            NState = State#connection{chan_state = NChanState},
            handle_outgoing(Packets, fun keep_state/1, NState);
        {stop, Reason, NChanState} ->
            stop(Reason, State#connection{chan_state = NChanState})
    end.

%%--------------------------------------------------------------------
%% Handle outgoing packets

handle_outgoing(Packets, SuccFun, State = #connection{serialize = Serialize})
  when is_list(Packets) ->
    send(lists:map(Serialize, Packets), SuccFun, State);

handle_outgoing(Packet, SuccFun, State = #connection{serialize = Serialize}) ->
    send(Serialize(Packet), SuccFun, State).

%%--------------------------------------------------------------------
%% Serialize fun

serialize_fun(ProtoVer) ->
    fun(Packet = ?PACKET(Type)) ->
        ?LOG(debug, "SEND ~s", [emqx_packet:format(Packet)]),
        _ = inc_outgoing_stats(Type),
        _ = emqx_metrics:inc_sent(Packet),
        emqx_frame:serialize(Packet, ProtoVer)
    end.

%%--------------------------------------------------------------------
%% Send data

send(IoData, SuccFun, State = #connection{transport  = Transport,
                                          socket     = Socket,
                                          chan_state = ChanState}) ->
    Oct = iolist_size(IoData),
    ok = emqx_metrics:inc('bytes.sent', Oct),
    case Transport:async_send(Socket, IoData) of
        ok -> NChanState = emqx_channel:sent(Oct, ChanState),
              SuccFun(State#connection{chan_state = NChanState});
        {error, Reason} ->
            shutdown(Reason, State)
    end.

%%--------------------------------------------------------------------
%% Handle timeout

handle_timeout(TRef, Msg, State = #connection{chan_state = ChanState}) ->
    case emqx_channel:timeout(TRef, Msg, ChanState) of
        {ok, NChanState} ->
            keep_state(State#connection{chan_state = NChanState});
        {ok, Packets, NChanState} ->
            handle_outgoing(Packets, fun keep_state/1,
                            State#connection{chan_state = NChanState});
        {stop, Reason, NChanState} ->
            stop(Reason, State#connection{chan_state = NChanState})
    end.

%%--------------------------------------------------------------------
%% Ensure rate limit

-define(ENABLED(Rl), (Rl =/= undefined)).

ensure_rate_limit(State = #connection{rate_limit = Rl, pub_limit = Pl}) ->
    Pubs = emqx_pd:reset_counter(incoming_pubs),
    Bytes = emqx_pd:reset_counter(incoming_bytes),
    Limiters = [{Pl, #connection.pub_limit, Pubs} || ?ENABLED(Pl)] ++
               [{Rl, #connection.rate_limit, Bytes} || ?ENABLED(Rl)],
    ensure_rate_limit(Limiters, State).

ensure_rate_limit([], State) ->
    State;
ensure_rate_limit([{Rl, Pos, Cnt}|Limiters], State) ->
    case esockd_rate_limit:check(Cnt, Rl) of
        {0, Rl1} ->
            ensure_rate_limit(Limiters, setelement(Pos, State, Rl1));
        {Pause, Rl1} ->
            ?LOG(debug, "Pause ~pms due to rate limit", [Pause]),
            TRef = erlang:send_after(Pause, self(), activate_socket),
            NState = State#connection{active_state = blocked,
                                      limit_timer  = TRef},
            setelement(Pos, NState, Rl1)
    end.

%%--------------------------------------------------------------------
%% Activate Socket

activate_socket(#connection{active_state = blocked}) ->
    ok;
activate_socket(#connection{transport = Transport,
                            socket    = Socket,
                            active_n  = N}) ->
    Transport:setopts(Socket, [{active, N}]).

%%--------------------------------------------------------------------
%% Inc incoming/outgoing stats

-compile({inline,
          [ inc_incoming_stats/1
          , inc_outgoing_stats/1
          ]}).

inc_incoming_stats(Type) ->
    emqx_pd:update_counter(recv_pkt, 1),
    if
        Type == ?PUBLISH ->
            emqx_pd:update_counter(recv_msg, 1),
            emqx_pd:update_counter(incoming_pubs, 1);
        true -> ok
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

