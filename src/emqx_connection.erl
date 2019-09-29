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
        , stats/1
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

-record(state, {
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
          %% Publish Limit
          pub_limit :: maybe(esockd_rate_limit:bucket()),
          %% Rate Limit
          rate_limit :: maybe(esockd_rate_limit:bucket()),
          %% Limit Timer
          limit_timer :: maybe(reference()),
          %% Parser State
          parse_state :: emqx_frame:parse_state(),
          %% Serialize function
          serialize :: emqx_frame:serialize_fun(),
          %% Channel State
          chan_state :: emqx_channel:channel()
        }).

-type(state() :: #state{}).

-define(ACTIVE_N, 100).
-define(HANDLE(T, C, D), handle((T), (C), (D))).
-define(INFO_KEYS, [socktype, peername, sockname, active_n, active_state,
                    pub_limit, rate_limit]).
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
-spec(info(pid()|state()) -> emqx_types:infos()).
info(CPid) when is_pid(CPid) ->
    call(CPid, info);
info(Conn = #state{chan_state = ChanState}) ->
    ChanInfo = emqx_channel:info(ChanState),
    SockInfo = maps:from_list(info(?INFO_KEYS, Conn)),
    maps:merge(ChanInfo, #{sockinfo => SockInfo}).

info(Keys, Conn) when is_list(Keys) ->
    [{Key, info(Key, Conn)} || Key <- Keys];
info(socktype, #state{transport = Transport, socket = Socket}) ->
    Transport:type(Socket);
info(peername, #state{peername = Peername}) ->
    Peername;
info(sockname, #state{sockname = Sockname}) ->
    Sockname;
info(active_n, #state{active_n = ActiveN}) ->
    ActiveN;
info(active_state, #state{active_state = ActiveSt}) ->
    ActiveSt;
info(pub_limit, #state{pub_limit = PubLimit}) ->
    limit_info(PubLimit);
info(rate_limit, #state{rate_limit = RateLimit}) ->
    limit_info(RateLimit);
info(chan_state, #state{chan_state = ChanState}) ->
    emqx_channel:info(ChanState).

limit_info(Limit) ->
    emqx_misc:maybe_apply(fun esockd_rate_limit:info/1, Limit).

%% @doc Get stats of the channel.
-spec(stats(pid()|state()) -> emqx_types:stats()).
stats(CPid) when is_pid(CPid) ->
    call(CPid, stats);
stats(#state{transport  = Transport,
             socket     = Socket,
             chan_state = ChanState}) ->
    SockStats = case Transport:getstat(Socket, ?SOCK_STATS) of
                    {ok, Ss}   -> Ss;
                    {error, _} -> []
                end,
    ConnStats = emqx_pd:get_counters(?CONN_STATS),
    ChanStats = emqx_channel:stats(ChanState),
    ProcStats = emqx_misc:proc_stats(),
    [{sock_stats, SockStats}, {conn_stats, ConnStats},
     {chan_stats, ChanStats}, {proc_stats, ProcStats}].

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
    ConnInfo = #{socktype => Transport:type(Socket),
                 peername => Peername,
                 sockname => Sockname,
                 peercert => Peercert,
                 conn_mod => ?MODULE
                },
    emqx_logger:set_metadata_peername(esockd_net:format(Peername)),
    Zone = proplists:get_value(zone, Options),
    ActiveN = proplists:get_value(active_n, Options, ?ACTIVE_N),
    PubLimit = init_limiter(emqx_zone:get_env(Zone, publish_limit)),
    RateLimit = init_limiter(proplists:get_value(rate_limit, Options)),
    FrameOpts = emqx_zone:frame_options(Zone),
    ParseState = emqx_frame:initial_parse_state(FrameOpts),
    Serialize = emqx_frame:serialize_fun(),
    ChanState = emqx_channel:init(ConnInfo, Options),
    State = #state{transport    = Transport,
                   socket       = Socket,
                   peername     = Peername,
                   sockname     = Sockname,
                   active_n     = ActiveN,
                   active_state = running,
                   pub_limit    = PubLimit,
                   rate_limit   = RateLimit,
                   parse_state  = ParseState,
                   serialize    = Serialize,
                   chan_state   = ChanState
                  },
    IdleTimout = emqx_zone:get_env(Zone, idle_timeout, 30000),
    gen_statem:enter_loop(?MODULE, [{hibernate_after, 2 * IdleTimout}],
                          idle, State, self(), [IdleTimout]).

-compile({inline, [init_limiter/1]}).
init_limiter(undefined) -> undefined;
init_limiter({Rate, Burst}) ->
    esockd_rate_limit:new(Rate, Burst).

-compile({inline, [callback_mode/0]}).
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
    SuccFun = fun(NewSt) -> {next_state, connected, NewSt} end,
    Serialize = emqx_frame:serialize_fun(ConnPkt),
    NState = State#state{serialize = Serialize},
    handle_incoming(Packet, SuccFun, NState);

idle(cast, {incoming, Packet}, State) when is_record(Packet, mqtt_packet) ->
    SuccFun = fun(NewSt) -> {next_state, connected, NewSt} end,
    handle_incoming(Packet, SuccFun, State);

idle(cast, {incoming, FrameError = {frame_error, _Reason}}, State) ->
    handle_incoming(FrameError, State);

idle(EventType, Content, State) ->
    ?HANDLE(EventType, Content, State).

%%--------------------------------------------------------------------
%% Connected State

connected(enter, _PrevSt, State) ->
    ok = register_self(State),
    keep_state_and_data;

connected(cast, {incoming, Packet}, State) when is_record(Packet, mqtt_packet) ->
    handle_incoming(Packet, fun keep_state/1, State);

connected(cast, {incoming, FrameError = {frame_error, _Reason}}, State) ->
    handle_incoming(FrameError, State);

connected(info, Deliver = {deliver, _Topic, _Msg}, State) ->
    handle_deliver(emqx_misc:drain_deliver([Deliver]), State);

connected(EventType, Content, State) ->
    ?HANDLE(EventType, Content, State).

%%--------------------------------------------------------------------
%% Disconnected State

disconnected(enter, _, State = #state{chan_state = ChanState}) ->
    case emqx_channel:handle_info(disconnected, ChanState) of
        {ok, NChanState} ->
            ok = register_self(State#state{chan_state = NChanState}),
            keep_state(State#state{chan_state = NChanState});
        {stop, Reason, NChanState} ->
            stop(Reason, State#state{chan_state = NChanState})
    end;

disconnected(info, Deliver = {deliver, _Topic, _Msg}, State) ->
    handle_deliver([Deliver], State);

disconnected(EventType, Content, State) ->
    ?HANDLE(EventType, Content, State).

%%--------------------------------------------------------------------
%% Handle call

handle({call, From}, info, State) ->
    reply(From, info(State), State);

handle({call, From}, stats, State) ->
    reply(From, stats(State), State);

handle({call, From}, state, State) ->
    reply(From, State, State);

handle({call, From}, Req, State = #state{chan_state = ChanState}) ->
    case emqx_channel:handle_call(Req, ChanState) of
        {ok, Reply, NChanState} ->
            reply(From, Reply, State#state{chan_state = NChanState});
        {stop, Reason, Reply, NChanState} ->
            ok = gen_statem:reply(From, Reply),
            stop(Reason, State#state{chan_state = NChanState});
        {stop, Reason, Packet, Reply, NChanState} ->
            handle_outgoing(Packet, State#state{chan_state = NChanState}),
            ok = gen_statem:reply(From, Reply),
            stop(Reason, State#state{chan_state = NChanState})
    end;

%%--------------------------------------------------------------------
%% Handle cast

handle(cast, Msg, State = #state{chan_state = ChanState}) ->
    case emqx_channel:handle_info(Msg, ChanState) of
        ok -> {ok, State};
        {ok, NChanState} ->
            keep_state(State#state{chan_state = NChanState});
        {stop, Reason, NChanState} ->
            stop(Reason, State#state{chan_state = NChanState})
    end;

%%--------------------------------------------------------------------
%% Handle info

%% Handle incoming data
handle(info, {Inet, _Sock, Data}, State = #state{chan_state = ChanState})
  when Inet == tcp; Inet == ssl ->
    ?LOG(debug, "RECV ~p", [Data]),
    Oct = iolist_size(Data),
    emqx_pd:update_counter(incoming_bytes, Oct),
    ok = emqx_metrics:inc('bytes.received', Oct),
    NChanState = emqx_channel:received(Oct, ChanState),
    NState = State#state{chan_state = NChanState},
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
    NState = State#state{active_state = running,
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
       State = #state{transport = Transport, socket = Socket}) ->
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
                                     chan_state = ChanState
                                    }) ->
    ?LOG(debug, "Terminated for ~p", [Reason]),
    ok = Transport:fast_close(Socket),
    emqx_channel:terminate(Reason, ChanState).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

register_self(State = #state{active_n = ActiveN,
                             active_state = ActiveSt,
                             chan_state = ChanState
                            }) ->
    ChanAttrs = emqx_channel:attrs(ChanState),
    SockAttrs = #{active_n => ActiveN,
                  active_state => ActiveSt
                 },
    Attrs = maps:merge(ChanAttrs, #{sockinfo => SockAttrs}),
    emqx_channel:handle_info({register, Attrs, stats(State)}, ChanState).

%%--------------------------------------------------------------------
%% Process incoming data

-compile({inline, [process_incoming/2]}).
process_incoming(Data, State) ->
    process_incoming(Data, [], State).

process_incoming(<<>>, Packets, State) ->
    keep_state(State, next_incoming_events(Packets));

process_incoming(Data, Packets, State = #state{parse_state = ParseState}) ->
    try emqx_frame:parse(Data, ParseState) of
        {more, NParseState} ->
            NState = State#state{parse_state = NParseState},
            keep_state(NState, next_incoming_events(Packets));
        {ok, Packet, Rest, NParseState} ->
            NState = State#state{parse_state = NParseState},
            process_incoming(Rest, [Packet|Packets], NState)
    catch
        error:Reason:Stk ->
            ?LOG(error, "~nParse failed for ~p~nStacktrace: ~p~nFrame data:~p",
                 [Reason, Stk, Data]),
            keep_state(State, next_incoming_events(Packets++[{frame_error, Reason}]))
    end.

-compile({inline, [next_incoming_events/1]}).
next_incoming_events([]) -> [];
next_incoming_events(Packets) ->
    [next_event(cast, {incoming, Packet}) || Packet <- Packets].

%%--------------------------------------------------------------------
%% Handle incoming packet

handle_incoming(Packet = ?PACKET(Type), SuccFun, State = #state{chan_state = ChanState}) ->
    _ = inc_incoming_stats(Type),
    _ = emqx_metrics:inc_recv(Packet),
    ?LOG(debug, "RECV ~s", [emqx_packet:format(Packet)]),
    case emqx_channel:handle_in(Packet, ChanState) of
        {ok, NChanState} ->
            SuccFun(State#state{chan_state= NChanState});
        {ok, OutPackets, NChanState} ->
            NState = State#state{chan_state = NChanState},
            handle_outgoing(OutPackets, SuccFun, NState);
        {stop, Reason, NChanState} ->
            stop(Reason, State#state{chan_state = NChanState});
        {stop, Reason, OutPackets, NChanState} ->
            NState = State#state{chan_state= NChanState},
            stop(Reason, handle_outgoing(OutPackets, fun(NewSt) -> NewSt end, NState))
    end.

handle_incoming(FrameError = {frame_error, _Reason}, State = #state{chan_state = ChanState}) ->
    case emqx_channel:handle_in(FrameError, ChanState) of
        {close, Reason, NChanState} ->
            close(Reason, State#state{chan_state = NChanState});
        {close, Reason, OutPackets, NChanState} ->
            NState = State#state{chan_state= NChanState},
            close(Reason, handle_outgoing(OutPackets, fun(NewSt) -> NewSt end, NState));
        {stop, Reason, NChanState} ->
            stop(Reason, State#state{chan_state = NChanState});
        {stop, Reason, OutPackets, NChanState} ->
            NState = State#state{chan_state= NChanState},
            stop(Reason, handle_outgoing(OutPackets, fun(NewSt) -> NewSt end, NState))
    end.

%%-------------------------------------------------------------------
%% Handle deliver

handle_deliver(Delivers, State = #state{chan_state = ChanState}) ->
    case emqx_channel:handle_out({deliver, Delivers}, ChanState) of
        {ok, NChanState} ->
            keep_state(State#state{chan_state = NChanState});
        {ok, Packets, NChanState} ->
            handle_outgoing(Packets, fun keep_state/1, State#state{chan_state = NChanState})
    end.

%%--------------------------------------------------------------------
%% Handle outgoing packets

handle_outgoing(Packet, State) ->
    handle_outgoing(Packet, fun (_) -> ok end, State).

handle_outgoing(Packets, SuccFun, State) when is_list(Packets) ->
    send(lists:map(serialize_and_inc_stats_fun(State), Packets), SuccFun, State);

handle_outgoing(Packet, SuccFun, State) ->
    send((serialize_and_inc_stats_fun(State))(Packet), SuccFun, State).

serialize_and_inc_stats_fun(#state{serialize = Serialize}) ->
    fun(Packet = ?PACKET(Type)) ->
        case Serialize(Packet) of
            <<>> -> ?LOG(warning, "~s is discarded due to the frame is too large!",
                         [emqx_packet:format(Packet)]),
                    <<>>;
            Data -> _ = inc_outgoing_stats(Type),
                    _ = emqx_metrics:inc_sent(Packet),
                    ?LOG(debug, "SEND ~s", [emqx_packet:format(Packet)]),
                    Data
        end
    end.

%%--------------------------------------------------------------------
%% Send data

send(IoData, SuccFun, State = #state{transport  = Transport,
                                     socket     = Socket,
                                     chan_state = ChanState}) ->
    Oct = iolist_size(IoData),
    ok = emqx_metrics:inc('bytes.sent', Oct),
    case Transport:async_send(Socket, IoData) of
        ok -> NChanState = emqx_channel:sent(Oct, ChanState),
              SuccFun(State#state{chan_state = NChanState});
        {error, Reason} ->
            shutdown(Reason, State)
    end.

%%--------------------------------------------------------------------
%% Handle timeout

handle_timeout(TRef, Msg, State = #state{chan_state = ChanState}) ->
    case emqx_channel:handle_timeout(TRef, Msg, ChanState) of
        {ok, NChanState} ->
            keep_state(State#state{chan_state = NChanState});
        {ok, Packets, NChanState} ->
            handle_outgoing(Packets, fun keep_state/1, State#state{chan_state = NChanState});
        {close, Reason, NChanState} ->
            close(Reason, State#state{chan_state = NChanState});
        {close, Reason, OutPackets, NChanState} ->
            NState = State#state{chan_state= NChanState},
            close(Reason, handle_outgoing(OutPackets, fun(NewSt) -> NewSt end, NState));
        {stop, Reason, NChanState} ->
            stop(Reason, State#state{chan_state = NChanState});
        {stop, Reason, OutPackets, NChanState} ->
            NState = State#state{chan_state= NChanState},
            stop(Reason, handle_outgoing(OutPackets, fun(NewSt) -> NewSt end, NState))
    end.

%%--------------------------------------------------------------------
%% Ensure rate limit

-define(ENABLED(Rl), (Rl =/= undefined)).

ensure_rate_limit(State = #state{rate_limit = Rl, pub_limit = Pl}) ->
    Pubs = emqx_pd:reset_counter(incoming_pubs),
    Bytes = emqx_pd:reset_counter(incoming_bytes),
    Limiters = [{Pl, #state.pub_limit, Pubs} || ?ENABLED(Pl)] ++
               [{Rl, #state.rate_limit, Bytes} || ?ENABLED(Rl)],
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
            NState = State#state{active_state = blocked,
                                 limit_timer  = TRef
                                },
            setelement(Pos, NState, Rl1)
    end.

%%--------------------------------------------------------------------
%% Activate Socket

-compile({inline, [activate_socket/1]}).
activate_socket(#state{active_state = blocked}) -> ok;
activate_socket(#state{transport = Transport,
                       socket    = Socket,
                       active_n  = N}) ->
    Transport:setopts(Socket, [{active, N}]).

%%--------------------------------------------------------------------
%% Inc incoming/outgoing stats

-compile({inline, [inc_incoming_stats/1]}).
inc_incoming_stats(Type) when is_integer(Type) ->
    emqx_pd:update_counter(recv_pkt, 1),
    if
        Type == ?PUBLISH ->
            emqx_pd:update_counter(recv_msg, 1),
            emqx_pd:update_counter(incoming_pubs, 1);
        true -> ok
    end.

-compile({inline, [inc_outgoing_stats/1]}).
inc_outgoing_stats(Type) ->
    emqx_pd:update_counter(send_pkt, 1),
    (Type == ?PUBLISH)
        andalso emqx_pd:update_counter(send_msg, 1).

%%--------------------------------------------------------------------
%% Helper functions

-compile({inline,
          [ reply/3
          , keep_state/1
          , keep_state/2
          , next_event/2
          , shutdown/2
          , stop/2
          ]}).

reply(From, Reply, State) ->
    {keep_state, State, [{reply, From, Reply}]}.

keep_state(State) ->
    {keep_state, State}.

keep_state(State, Events) ->
    {keep_state, State, Events}.

next_event(Type, Content) ->
    {next_event, Type, Content}.

close(Reason, State = #state{transport = Transport, socket = Socket}) ->
    ?LOG(warning, "Closed for ~p", [Reason]),
    ok = Transport:fast_close(Socket),
    {next_state, disconnected, State}.

shutdown(Reason, State) ->
    stop({shutdown, Reason}, State).

stop(Reason, State) ->
    {stop, Reason, State}.

