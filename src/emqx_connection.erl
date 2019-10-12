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

%% MQTT/TCP Connection
-module(emqx_connection).

-include("emqx.hrl").
-include("emqx_mqtt.hrl").
-include("logger.hrl").
-include("types.hrl").

-logger_header("[MQTT]").

%% API
-export([ start_link/3
        , stop/1
        ]).

-export([ info/1
        , stats/1
        ]).

-export([call/2]).

%% callback
-export([init/4]).

%% Sys callbacks
-export([ system_continue/3
        , system_terminate/4
        , system_code_change/4
        , system_get_state/1
        ]).

%% Internal callbacks
-export([wakeup_from_hib/2]).

-record(state, {
          %% Parent
          parent :: pid(),
          %% TCP/TLS Transport
          transport :: esockd:transport(),
          %% TCP/TLS Socket
          socket :: esockd:socket(),
          %% Peername of the connection
          peername :: emqx_types:peername(),
          %% Sockname of the connection
          sockname :: emqx_types:peername(),
          %% Sock state
          sockstate :: emqx_types:sockstate(),
          %% The {active, N} option
          active_n :: pos_integer(),
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
          channel :: emqx_channel:channel(),
          %% Idle timer
          idle_timer :: reference()
        }).

-type(state() :: #state{}).

-define(ACTIVE_N, 100).
-define(INFO_KEYS, [socktype, peername, sockname, sockstate, active_n,
                    pub_limit, rate_limit]).
-define(CONN_STATS, [recv_pkt, recv_msg, send_pkt, send_msg]).
-define(SOCK_STATS, [recv_oct, recv_cnt, send_oct, send_cnt, send_pend]).

-spec(start_link(esockd:transport(), esockd:socket(), proplists:proplist())
      -> {ok, pid()}).
start_link(Transport, Socket, Options) ->
    CPid = proc_lib:spawn_link(?MODULE, init, [self(), Transport, Socket, Options]),
    {ok, CPid}.

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

%% @doc Get infos of the connection/channel.
-spec(info(pid()|state()) -> emqx_types:infos()).
info(CPid) when is_pid(CPid) ->
    call(CPid, info);
info(State = #state{channel = Channel}) ->
    ChanInfo = emqx_channel:info(Channel),
    SockInfo = maps:from_list(info(?INFO_KEYS, State)),
    maps:merge(ChanInfo, #{sockinfo => SockInfo}).

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
    ActiveN;
info(pub_limit, #state{pub_limit = PubLimit}) ->
    limit_info(PubLimit);
info(rate_limit, #state{rate_limit = RateLimit}) ->
    limit_info(RateLimit);
info(channel, #state{channel = Channel}) ->
    emqx_channel:info(Channel).

limit_info(Limit) ->
    emqx_misc:maybe_apply(fun esockd_rate_limit:info/1, Limit).

%% @doc Get stats of the connection/channel.
-spec(stats(pid()|state()) -> emqx_types:stats()).
stats(CPid) when is_pid(CPid) ->
    call(CPid, stats);
stats(#state{transport = Transport,
             socket    = Socket,
             channel   = Channel}) ->
    SockStats = case Transport:getstat(Socket, ?SOCK_STATS) of
                    {ok, Ss}   -> Ss;
                    {error, _} -> []
                end,
    ConnStats = emqx_pd:get_counters(?CONN_STATS),
    ChanStats = emqx_channel:stats(Channel),
    ProcStats = emqx_misc:proc_stats(),
    lists:append([SockStats, ConnStats, ChanStats, ProcStats]).

call(Pid, Req) ->
    gen_server:call(Pid, Req, infinity).

stop(Pid) ->
    gen_server:stop(Pid).

%%--------------------------------------------------------------------
%% callbacks
%%--------------------------------------------------------------------

init(Parent, Transport, RawSocket, Options) ->
    case Transport:wait(RawSocket) of
        {ok, Socket} ->
            do_init(Parent, Transport, Socket, Options);
        {error, Reason} when Reason =:= enotconn;
                             Reason =:= einval;
                             Reason =:= closed ->
            Transport:fast_close(RawSocket),
            exit(normal);
        {error, timeout} ->
            Transport:fast_close(RawSocket),
            exit({shutdown, ssl_upgrade_timeout});
        {error, Reason} ->
            Transport:fast_close(RawSocket),
            exit(Reason)
    end.

do_init(Parent, Transport, Socket, Options) ->
    {ok, Peername} = Transport:ensure_ok_or_exit(peername, [Socket]),
    {ok, Sockname} = Transport:ensure_ok_or_exit(sockname, [Socket]),
    emqx_logger:set_metadata_peername(esockd_net:format(Peername)),
    Peercert = Transport:ensure_ok_or_exit(peercert, [Socket]),
    ConnInfo = #{socktype => Transport:type(Socket),
                 peername => Peername,
                 sockname => Sockname,
                 peercert => Peercert,
                 conn_mod => ?MODULE
                },
    Zone = proplists:get_value(zone, Options),
    ActiveN = proplists:get_value(active_n, Options, ?ACTIVE_N),
    PubLimit = init_limiter(emqx_zone:get_env(Zone, publish_limit)),
    RateLimit = init_limiter(proplists:get_value(rate_limit, Options)),
    FrameOpts = emqx_zone:frame_options(Zone),
    ParseState = emqx_frame:initial_parse_state(FrameOpts),
    Serialize = emqx_frame:serialize_fun(),
    Channel = emqx_channel:init(ConnInfo, Options),
    IdleTimout = emqx_zone:get_env(Zone, idle_timeout, 30000),
    IdleTimer = emqx_misc:start_timer(IdleTimout, idle_timeout),
    HibAfterTimeout = emqx_zone:get_env(Zone, hibernate_after, IdleTimout*2),
    State = #state{parent      = Parent,
                   transport   = Transport,
                   socket      = Socket,
                   peername    = Peername,
                   sockname    = Sockname,
                   sockstate   = idle,
                   active_n    = ActiveN,
                   pub_limit   = PubLimit,
                   rate_limit  = RateLimit,
                   parse_state = ParseState,
                   serialize   = Serialize,
                   channel     = Channel,
                   idle_timer  = IdleTimer
                  },
    case activate_socket(State) of
        {ok, NState} ->
            recvloop(NState, #{hibernate_after => HibAfterTimeout});
        {error, Reason} when Reason =:= einval;
                             Reason =:= enotconn;
                             Reason =:= closed ->
            Transport:fast_close(Socket),
            exit(normal);
        {error, Reason} ->
            Transport:fast_close(Socket),
            erlang:exit({shutdown, Reason})
    end.

-compile({inline, [init_limiter/1]}).
init_limiter(undefined) -> undefined;
init_limiter({Rate, Burst}) ->
    esockd_rate_limit:new(Rate, Burst).

%%--------------------------------------------------------------------
%% Recv Loop

recvloop(State = #state{parent = Parent},
         Options = #{hibernate_after := HibAfterTimeout}) ->
    receive
        {system, From, Request} ->
            sys:handle_system_msg(Request, From, Parent,
                                  ?MODULE, [], {State, Options});
        {'EXIT', Parent, Reason} ->
            terminate(Reason, State);
        Msg ->
            process_msg([Msg], State, Options)
    after
        HibAfterTimeout ->
            hibernate(State, Options)
    end.

hibernate(State, Options) ->
    proc_lib:hibernate(?MODULE, wakeup_from_hib, [State, Options]).

wakeup_from_hib(State, Options) ->
    %% Maybe do something later here.
    recvloop(State, Options).

%%--------------------------------------------------------------------
%% Process next Msg

process_msg([], State, Options) ->
    recvloop(State, Options);

process_msg([Msg|More], State, Options) ->
    case catch handle_msg(Msg, State) of
        ok ->
            process_msg(More, State, Options);
        {ok, NState} ->
            process_msg(More, NState, Options);
        {ok, NextMsgs, NState} ->
            process_msg(append_msg(NextMsgs, More), NState, Options);
        {stop, Reason} ->
            terminate(Reason, State);
        {stop, Reason, NState} ->
            terminate(Reason, NState);
        {'EXIT', Reason} ->
            terminate(Reason, State)
    end.

%%--------------------------------------------------------------------
%% Handle a Msg

handle_msg({'$gen_call', From, Req}, State) ->
    case handle_call(From, Req, State) of
        {reply, Reply, NState} ->
            gen_server:reply(From, Reply),
            {ok, NState};
        {stop, Reason, Reply, NState} ->
            gen_server:reply(From, Reply),
            stop(Reason, NState)
    end;

handle_msg({Inet, _Sock, Data}, State = #state{channel = Channel})
  when Inet == tcp; Inet == ssl ->
    ?LOG(debug, "RECV ~p", [Data]),
    Oct = iolist_size(Data),
    emqx_pd:update_counter(incoming_bytes, Oct),
    ok = emqx_metrics:inc('bytes.received', Oct),
    {ok, NChannel} = emqx_channel:handle_in(Oct, Channel),
    process_incoming(Data, State#state{channel = NChannel});

handle_msg({incoming, Packet = ?CONNECT_PACKET(ConnPkt)},
           State = #state{idle_timer = IdleTimer}) ->
    ok = emqx_misc:cancel_timer(IdleTimer),
    Serialize = emqx_frame:serialize_fun(ConnPkt),
    NState = State#state{serialize  = Serialize,
                         idle_timer = undefined
                        },
    handle_incoming(Packet, NState);

handle_msg({incoming, Packet}, State) ->
    handle_incoming(Packet, State);

handle_msg({Error, _Sock, Reason}, State)
  when Error == tcp_error; Error == ssl_error ->
    handle_info({sock_error, Reason}, State);

handle_msg({Closed, _Sock}, State)
  when Closed == tcp_closed; Closed == ssl_closed ->
    handle_info(sock_closed, State);

handle_msg({Passive, _Sock}, State)
  when Passive == tcp_passive; Passive == ssl_passive ->
    %% Rate limit and activate socket here.
    NState = ensure_rate_limit(State),
    case activate_socket(NState) of
        {ok, NState} -> {ok, NState};
        {error, Reason} ->
            {ok, {sock_error, Reason}, NState}
    end;

%% Rate limit timer expired.
handle_msg(activate_socket, State) ->
    NState = State#state{sockstate   = idle,
                         limit_timer = undefined
                        },
    case activate_socket(NState) of
        {ok, NState} -> {ok, NState};
        {error, Reason} ->
            {ok, {sock_error, Reason}, State}
    end;

handle_msg(Deliver = {deliver, _Topic, _Msg},
           State = #state{channel = Channel}) ->
    Delivers = emqx_misc:drain_deliver([Deliver]),
    Result = emqx_channel:handle_out(Delivers, Channel),
    handle_return(Result, State);

handle_msg({outgoing, Packets}, State) ->
    {ok, handle_outgoing(Packets, State)};

%% something sent
handle_msg({inet_reply, _Sock, ok}, _State) ->
    ok;

handle_msg({inet_reply, _Sock, {error, Reason}}, State) ->
    handle_info({sock_error, Reason}, State);

handle_msg({timeout, TRef, TMsg}, State) ->
    handle_timeout(TRef, TMsg, State);

handle_msg(Shutdown = {shutdown, _Reason}, State) ->
    stop(Shutdown, State);

handle_msg(Msg, State) -> handle_info(Msg, State).

%%--------------------------------------------------------------------
%% Terminate

terminate(Reason, #state{transport = Transport,
                         socket    = Socket,
                         sockstate = SockSt,
                         channel   = Channel}) ->
    ?LOG(debug, "Terminated for ~p", [Reason]),
    SockSt =:= closed orelse Transport:fast_close(Socket),
    emqx_channel:terminate(Reason, Channel),
    exit(Reason).

%%--------------------------------------------------------------------
%% Sys callbacks

system_continue(_Parent, _Deb, {State, Options}) ->
    recvloop(State, Options).

system_terminate(Reason, _Parent, _Deb, {State, _}) ->
    terminate(Reason, State).

system_code_change(Misc, _, _, _) ->
    {ok, Misc}.

system_get_state({State, _Options}) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% Handle call

handle_call(_From, info, State) ->
    {reply, info(State), State};

handle_call(_From, stats, State) ->
    {reply, stats(State), State};

handle_call(_From, Req, State = #state{channel = Channel}) ->
    case emqx_channel:handle_call(Req, Channel) of
        {reply, Reply, NChannel} ->
            {reply, Reply, State#state{channel = NChannel}};
        {stop, Reason, Reply, NChannel} ->
            {stop, Reason, Reply, State#state{channel = NChannel}};
        {stop, Reason, Reply, OutPacket, NChannel} ->
            NState = State#state{channel = NChannel},
            NState1 = handle_outgoing(OutPacket, NState),
            {stop, Reason, Reply, NState1}
    end.

%%--------------------------------------------------------------------
%% Handle timeout

handle_timeout(TRef, idle_timeout, State = #state{idle_timer = TRef}) ->
    stop(idle_timeout, State);

handle_timeout(TRef, emit_stats, State) ->
    handle_timeout(TRef, {emit_stats, stats(State)}, State);

handle_timeout(TRef, keepalive, State = #state{transport = Transport,
                                               socket    = Socket}) ->
    case Transport:getstat(Socket, [recv_oct]) of
        {ok, [{recv_oct, RecvOct}]} ->
            handle_timeout(TRef, {keepalive, RecvOct}, State);
        {error, Reason} ->
            handle_info({sockerr, Reason}, State)
    end;

handle_timeout(TRef, Msg, State = #state{channel = Channel}) ->
    handle_return(emqx_channel:handle_timeout(TRef, Msg, Channel), State).

%%--------------------------------------------------------------------
%% Process/Parse incoming data.

-compile({inline, [process_incoming/2]}).
process_incoming(Data, State) ->
    {Packets, NState} = parse_incoming(Data, State),
    {ok, next_incoming_msgs(Packets), NState}.

-compile({inline, [parse_incoming/2]}).
parse_incoming(Data, State) ->
    parse_incoming(Data, [], State).

parse_incoming(<<>>, Packets, State) ->
    {Packets, State};

parse_incoming(Data, Packets, State = #state{parse_state = ParseState}) ->
    try emqx_frame:parse(Data, ParseState) of
        {more, NParseState} ->
            {Packets, State#state{parse_state = NParseState}};
        {ok, Packet, Rest, NParseState} ->
            NState = State#state{parse_state = NParseState},
            parse_incoming(Rest, [Packet|Packets], NState)
    catch
        error:Reason:Stk ->
            ?LOG(error, "~nParse failed for ~p~nStacktrace: ~p~nFrame data:~p",
                 [Reason, Stk, Data]),
            {[{frame_error, Reason}|Packets], State}
    end.

next_incoming_msgs([Packet]) ->
    {incoming, Packet};
next_incoming_msgs(Packets) ->
    [{incoming, Packet} || Packet <- lists:reverse(Packets)].

%%--------------------------------------------------------------------
%% Handle incoming packet

handle_incoming(Packet = ?PACKET(Type), State = #state{channel = Channel}) ->
    _ = inc_incoming_stats(Type),
    ok = emqx_metrics:inc_recv(Packet),
    ?LOG(debug, "RECV ~s", [emqx_packet:format(Packet)]),
    handle_return(emqx_channel:handle_in(Packet, Channel), State);

handle_incoming(FrameError, State = #state{channel = Channel}) ->
    handle_return(emqx_channel:handle_in(FrameError, Channel), State).

%%--------------------------------------------------------------------
%% Handle channel return

handle_return(ok, State) ->
    {ok, State};
handle_return({ok, NChannel}, State) ->
    {ok, State#state{channel = NChannel}};
handle_return({ok, Replies, NChannel}, State) ->
    {ok, next_msgs(Replies), State#state{channel = NChannel}};
handle_return({stop, Reason, NChannel}, State) ->
    stop(Reason, State#state{channel = NChannel});
handle_return({stop, Reason, OutPacket, NChannel}, State) ->
    NState = State#state{channel = NChannel},
    NState1 = handle_outgoing(OutPacket, NState),
    stop(Reason, NState1).

%%--------------------------------------------------------------------
%% Handle outgoing packets

handle_outgoing(Packets, State) when is_list(Packets) ->
    send(lists:map(serialize_and_inc_stats_fun(State), Packets), State);

handle_outgoing(Packet, State) ->
    send((serialize_and_inc_stats_fun(State))(Packet), State).

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

send(IoData, State = #state{transport = Transport,
                            socket    = Socket,
                            channel   = Channel}) ->
    Oct = iolist_size(IoData),
    ok = emqx_metrics:inc('bytes.sent', Oct),
    case Transport:async_send(Socket, IoData) of
        ok ->
            {ok, NChannel} = emqx_channel:handle_out(Oct, Channel),
            State#state{channel = NChannel};
        Error = {error, _Reason} ->
            %% Simulate an inet_reply to postpone handling the error
            self() ! {inet_reply, Socket, Error}, State
    end.

%%--------------------------------------------------------------------
%% Handle Info

handle_info({enter, _}, State = #state{active_n  = ActiveN,
                                       sockstate = SockSt,
                                       channel   = Channel}) ->
    ChanAttrs = emqx_channel:attrs(Channel),
    SockAttrs = #{active_n  => ActiveN,
                  sockstate => SockSt
                 },
    Attrs = maps:merge(ChanAttrs, #{sockinfo => SockAttrs}),
    handle_info({register, Attrs, stats(State)}, State);

handle_info({sockerr, _Reason}, #state{sockstate = closed}) -> ok;
handle_info({sockerr, Reason}, State) ->
    ?LOG(debug, "Socket error: ~p", [Reason]),
    handle_info({sock_closed, Reason}, close_socket(State));

handle_info(sock_closed, #state{sockstate = closed}) -> ok;
handle_info(sock_closed, State) ->
    ?LOG(debug, "Socket closed"),
    handle_info({sock_closed, closed}, close_socket(State));

handle_info({close, Reason}, State) ->
    ?LOG(debug, "Force close due to : ~p", [Reason]),
    {ok, close_socket(State)};

handle_info(Info, State = #state{channel = Channel}) ->
    handle_return(emqx_channel:handle_info(Info, Channel), State).

%%--------------------------------------------------------------------
%% Activate Socket

-compile({inline, [activate_socket/1]}).
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

%%--------------------------------------------------------------------
%% Close Socket

close_socket(State = #state{transport = Transport, socket = Socket}) ->
    ok = Transport:fast_close(Socket),
    State#state{sockstate = closed}.

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
            NState = State#state{sockstate = blocked, limit_timer = TRef},
            setelement(Pos, NState, Rl1)
    end.

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
    (Type == ?PUBLISH) andalso emqx_pd:update_counter(send_msg, 1).

%%--------------------------------------------------------------------
%% Helper functions

-compile({inline, [append_msg/2]}).
append_msg(Msgs, Q) when is_list(Msgs) ->
    lists:append(Msgs, Q);
append_msg(Msg, Q) -> [Msg|Q].

-compile({inline, [next_msgs/1]}).
next_msgs(Packet) when is_record(Packet, mqtt_packet) ->
    {outgoing, Packet};
next_msgs(Action) when is_tuple(Action) ->
    Action;
next_msgs(Actions) when is_list(Actions) ->
    Actions.

-compile({inline, [stop/2]}).
stop(Reason, State) ->
    {stop, Reason, State}.

