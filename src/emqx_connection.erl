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

%% MQTT/TCP|TLS Connection
-module(emqx_connection).

-include("emqx.hrl").
-include("emqx_mqtt.hrl").
-include("logger.hrl").
-include("types.hrl").

-logger_header("[MQTT]").

-ifdef(TEST).
-compile(export_all).
-compile(nowarn_export_all).
-endif.

%% API
-export([ start_link/3
        , stop/1
        ]).

-export([ info/1
        , stats/1
        ]).

-export([call/2]).

%% Callback
-export([init/4]).

%% Sys callbacks
-export([ system_continue/3
        , system_terminate/4
        , system_code_change/4
        , system_get_state/1
        ]).

%% Internal callback
-export([wakeup_from_hib/3]).

-import(emqx_misc,
        [ maybe_apply/2
        , start_timer/2
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
          %% Sock State
          sockstate :: emqx_types:sockstate(),
          %% The {active, N} option
          active_n :: pos_integer(),
          %% Limiter
          limiter :: maybe(emqx_limiter:limiter()),
          %% Limit Timer
          limit_timer :: maybe(reference()),
          %% Parse State
          parse_state :: emqx_frame:parse_state(),
          %% Serialize function
          serialize :: emqx_frame:serialize_fun(),
          %% Channel State
          channel :: emqx_channel:channel(),
          %% GC State
          gc_state :: maybe(emqx_gc:gc_state()),
          %% Stats Timer
          stats_timer :: disabled | maybe(reference()),
          %% Idle Timer
          idle_timer :: maybe(reference())
        }).

-type(state() :: #state{}).

-define(ACTIVE_N, 100).
-define(INFO_KEYS, [socktype, peername, sockname, sockstate, active_n, limiter]).
-define(CONN_STATS, [recv_pkt, recv_msg, send_pkt, send_msg]).
-define(SOCK_STATS, [recv_oct, recv_cnt, send_oct, send_cnt, send_pend]).

-define(ENABLED(X), (X =/= undefined)).

-spec(start_link(esockd:transport(), esockd:socket(), proplists:proplist())
      -> {ok, pid()}).
start_link(Transport, Socket, Options) ->
    Args = [self(), Transport, Socket, Options],
    CPid = proc_lib:spawn_link(?MODULE, init, Args),
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
info(limiter, #state{limiter = Limiter}) ->
    maybe_apply(fun emqx_limiter:info/1, Limiter).

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

attrs(#state{active_n = ActiveN, sockstate = SockSt, channel = Channel}) ->
    SockAttrs = #{active_n  => ActiveN,
                  sockstate => SockSt
                 },
    ChanAttrs = emqx_channel:attrs(Channel),
    maps:merge(ChanAttrs, #{sockinfo => SockAttrs}).

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
        {error, Reason} ->
            ok = Transport:fast_close(RawSocket),
            exit_on_sock_error(Reason)
    end.

do_init(Parent, Transport, Socket, Options) ->
    {ok, Peername} = Transport:ensure_ok_or_exit(peername, [Socket]),
    {ok, Sockname} = Transport:ensure_ok_or_exit(sockname, [Socket]),
    Peercert = Transport:ensure_ok_or_exit(peercert, [Socket]),
    ConnInfo = #{socktype => Transport:type(Socket),
                 peername => Peername,
                 sockname => Sockname,
                 peercert => Peercert,
                 conn_mod => ?MODULE
                },
    Zone = proplists:get_value(zone, Options),
    ActiveN = proplists:get_value(active_n, Options, ?ACTIVE_N),
    Limiter = emqx_limiter:init(Options),
    FrameOpts = emqx_zone:mqtt_frame_options(Zone),
    ParseState = emqx_frame:initial_parse_state(FrameOpts),
    Serialize = emqx_frame:serialize_fun(),
    Channel = emqx_channel:init(ConnInfo, Options),
    GcState = emqx_zone:init_gc_state(Zone),
    StatsTimer = emqx_zone:stats_timer(Zone),
    IdleTimeout = emqx_zone:idle_timeout(Zone),
    IdleTimer = start_timer(IdleTimeout, idle_timeout),
    emqx_misc:tune_heap_size(emqx_zone:oom_policy(Zone)),
    emqx_logger:set_metadata_peername(esockd:format(Peername)),
    State = #state{transport   = Transport,
                   socket      = Socket,
                   peername    = Peername,
                   sockname    = Sockname,
                   sockstate   = idle,
                   active_n    = ActiveN,
                   limiter     = Limiter,
                   parse_state = ParseState,
                   serialize   = Serialize,
                   channel     = Channel,
                   gc_state    = GcState,
                   stats_timer = StatsTimer,
                   idle_timer  = IdleTimer
                  },
    case activate_socket(State) of
        {ok, NState} ->
            hibernate(Parent, NState, #{idle_timeout => IdleTimeout});
        {error, Reason} ->
            ok = Transport:fast_close(Socket),
            exit_on_sock_error(Reason)
    end.

exit_on_sock_error(Reason) when Reason =:= einval;
                                Reason =:= enotconn;
                                Reason =:= closed ->
    erlang:exit(normal);
exit_on_sock_error(timeout) ->
    erlang:exit({shutdown, ssl_upgrade_timeout});
exit_on_sock_error(Reason) ->
    erlang:exit({shutdown, Reason}).

%%--------------------------------------------------------------------
%% Recv Loop

recvloop(Parent, State, Options = #{idle_timeout := IdleTimeout}) ->
    receive
        {system, From, Request} ->
            sys:handle_system_msg(Request, From, Parent,
                                  ?MODULE, [], {State, Options});
        {'EXIT', Parent, Reason} ->
            terminate(Reason, State);
        Msg ->
            NState = ensure_stats_timer(IdleTimeout, State),
            process_msg([Msg], Parent, NState, Options)
    after
        IdleTimeout ->
            NState = cancel_stats_timer(State),
            hibernate(Parent, NState, Options)
    end.

hibernate(Parent, State, Options) ->
    proc_lib:hibernate(?MODULE, wakeup_from_hib, [Parent, State, Options]).

wakeup_from_hib(Parent, State, Options) ->
    %% Maybe do something later here.
    recvloop(Parent, State, Options).

%%--------------------------------------------------------------------
%% Ensure/cancel stats timer

-compile({inline, [ensure_stats_timer/2]}).
ensure_stats_timer(Timeout, State = #state{stats_timer = undefined}) ->
    State#state{stats_timer = start_timer(Timeout, emit_stats)};
ensure_stats_timer(_Timeout, State) -> State.

-compile({inline, [cancel_stats_timer/1]}).
cancel_stats_timer(State = #state{stats_timer = TRef}) when is_reference(TRef) ->
    ok = emqx_misc:cancel_timer(TRef),
    State#state{stats_timer = undefined};
cancel_stats_timer(State) -> State.

%%--------------------------------------------------------------------
%% Process next Msg

process_msg([], Parent, State, Options) ->
    recvloop(Parent, State, Options);

process_msg([Msg|More], Parent, State, Options) ->
    case catch handle_msg(Msg, State) of
        ok ->
            process_msg(More, Parent, State, Options);
        {ok, NState} ->
            process_msg(More, Parent, NState, Options);
        {ok, Msgs, NState} ->
            process_msg(append_msg(Msgs, More), Parent, NState, Options);
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

handle_msg({Inet, _Sock, Data}, State) when Inet == tcp; Inet == ssl ->
    ?LOG(debug, "RECV ~p", [Data]),
    Oct = iolist_size(Data),
    emqx_pd:inc_counter(incoming_bytes, Oct),
    ok = emqx_metrics:inc('bytes.received', Oct),
    parse_incoming(Data, State);

handle_msg({incoming, Packet = ?CONNECT_PACKET(ConnPkt)},
           State = #state{idle_timer = IdleTimer}) ->
    ok = emqx_misc:cancel_timer(IdleTimer),
    Serialize = emqx_frame:serialize_fun(ConnPkt),
    NState = State#state{serialize  = Serialize,
                         idle_timer = undefined
                        },
    handle_incoming(Packet, NState);

handle_msg({incoming, ?PACKET(?PINGREQ)}, State) ->
    handle_outgoing(?PACKET(?PINGRESP), State);

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
    InStats = #{cnt => emqx_pd:reset_counter(incoming_pubs),
                oct => emqx_pd:reset_counter(incoming_bytes)
               },
    %% Ensure Rate Limit
    NState = ensure_rate_limit(InStats, State),
    %% Run GC and Check OOM
    NState1 = check_oom(run_gc(InStats, NState)),
    handle_info(activate_socket, NState1);

handle_msg(Deliver = {deliver, _Topic, _Msg},
           State = #state{channel = Channel}) ->
    Delivers = [Deliver|emqx_misc:drain_deliver()],
    Ret = emqx_channel:handle_out(Delivers, Channel),
    handle_chan_return(Ret, State);

handle_msg({outgoing, Packets}, State) ->
    handle_outgoing(Packets, State);

%% Something sent
handle_msg({inet_reply, _Sock, ok}, State = #state{active_n = ActiveN}) ->
    case emqx_pd:get_counter(outgoing_pubs) > ActiveN of
        true ->
            OutStats = #{cnt => emqx_pd:reset_counter(outgoing_pubs),
                         oct => emqx_pd:reset_counter(outgoing_bytes)
                        },
            {ok, check_oom(run_gc(OutStats, State))};
        false -> ok
    end;

handle_msg({inet_reply, _Sock, {error, Reason}}, State) ->
    handle_info({sock_error, Reason}, State);

handle_msg({timeout, TRef, TMsg}, State) ->
    handle_timeout(TRef, TMsg, State);

handle_msg(Shutdown = {shutdown, _Reason}, State) ->
    stop(Shutdown, State);

handle_msg(Msg, State) ->
    handle_info(Msg, State).

%%--------------------------------------------------------------------
%% Terminate

terminate(Reason, State = #state{channel = Channel}) ->
    ?LOG(debug, "Terminated due to ~p", [Reason]),
    emqx_channel:terminate(Reason, Channel),
    close_socket(State),
    exit(Reason).

%%--------------------------------------------------------------------
%% Sys callbacks

system_continue(Parent, _Deb, {State, Options}) ->
    recvloop(Parent, State, Options).

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
        {shutdown, Reason, Reply, NChannel} ->
            shutdown(Reason, Reply, State#state{channel = NChannel});
        {shutdown, Reason, Reply, OutPacket, NChannel} ->
            NState = State#state{channel = NChannel},
            ok = handle_outgoing(OutPacket, NState),
            shutdown(Reason, Reply, NState)
    end.

%%--------------------------------------------------------------------
%% Handle timeout

handle_timeout(TRef, idle_timeout, State = #state{idle_timer = TRef}) ->
    shutdown(idle_timeout, State);

handle_timeout(TRef, limit_timeout, State = #state{limit_timer = TRef}) ->
    NState = State#state{sockstate   = idle,
                         limit_timer = undefined
                        },
    handle_info(activate_socket, NState);

handle_timeout(TRef, emit_stats, State = #state{stats_timer = TRef,
                                                channel = Channel}) ->
    #{clientid := ClientId} = emqx_channel:info(clientinfo, Channel),
    (ClientId =/= undefined) andalso
        emqx_cm:set_chan_stats(ClientId, stats(State)),
    {ok, State#state{stats_timer = undefined}};

handle_timeout(TRef, keepalive, State = #state{transport = Transport,
                                               socket    = Socket}) ->
    case Transport:getstat(Socket, [recv_oct]) of
        {ok, [{recv_oct, RecvOct}]} ->
            handle_timeout(TRef, {keepalive, RecvOct}, State);
        {error, Reason} ->
            handle_info({sock_error, Reason}, State)
    end;

handle_timeout(TRef, Msg, State = #state{channel = Channel}) ->
    Ret = emqx_channel:handle_timeout(TRef, Msg, Channel),
    handle_chan_return(Ret, State).

%%--------------------------------------------------------------------
%% Parse incoming data

-compile({inline, [parse_incoming/2]}).
parse_incoming(Data, State) ->
    {Packets, NState} = parse_incoming(Data, [], State),
    {ok, next_incoming_msgs(Packets), NState}.

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

handle_incoming(Packet, State = #state{channel = Channel})
  when is_record(Packet, mqtt_packet) ->
    ok = inc_incoming_stats(Packet),
    ?LOG(debug, "RECV ~s", [emqx_packet:format(Packet)]),
    handle_chan_return(emqx_channel:handle_in(Packet, Channel), State);

handle_incoming(FrameError, State = #state{channel = Channel}) ->
    handle_chan_return(emqx_channel:handle_in(FrameError, Channel), State).

%%--------------------------------------------------------------------
%% Handle channel return

handle_chan_return(ok, State) ->
    {ok, State};
handle_chan_return({ok, NChannel}, State) ->
    {ok, State#state{channel = NChannel}};
handle_chan_return({ok, Replies, NChannel}, State) ->
    {ok, next_msgs(Replies), State#state{channel = NChannel}};
handle_chan_return({shutdown, Reason, NChannel}, State) ->
    shutdown(Reason, State#state{channel = NChannel});
handle_chan_return({shutdown, Reason, OutPacket, NChannel}, State) ->
    NState = State#state{channel = NChannel},
    ok = handle_outgoing(OutPacket, NState),
    shutdown(Reason, NState).

%%--------------------------------------------------------------------
%% Handle outgoing packets

handle_outgoing(Packets, State) when is_list(Packets) ->
    send(lists:map(serialize_and_inc_stats_fun(State), Packets), State);

handle_outgoing(Packet, State) ->
    send((serialize_and_inc_stats_fun(State))(Packet), State).

serialize_and_inc_stats_fun(#state{serialize = Serialize}) ->
    fun(Packet) ->
        case Serialize(Packet) of
            <<>> -> ?LOG(warning, "~s is discarded due to the frame is too large!",
                         [emqx_packet:format(Packet)]),
                    <<>>;
            Data -> ?LOG(debug, "SEND ~s", [emqx_packet:format(Packet)]),
                    ok = inc_outgoing_stats(Packet),
                    Data
        end
    end.

%%--------------------------------------------------------------------
%% Send data

-spec(send(iodata(), state()) -> ok).
send(IoData, #state{transport = Transport, socket = Socket}) ->
    Oct = iolist_size(IoData),
    ok = emqx_metrics:inc('bytes.sent', Oct),
    emqx_pd:inc_counter(outgoing_bytes, Oct),
    case Transport:async_send(Socket, IoData) of
        ok -> ok;
        Error = {error, _Reason} ->
            %% Send an inet_reply to postpone handling the error
            self() ! {inet_reply, Socket, Error},
            ok
    end.

%%--------------------------------------------------------------------
%% Handle Info

handle_info({connack, ConnAck}, State = #state{channel = Channel}) ->
    #{clientid := ClientId} = emqx_channel:info(clientinfo, Channel),
    ok = emqx_cm:register_channel(ClientId),
    ok = emqx_cm:set_chan_attrs(ClientId, attrs(State)),
    ok = emqx_cm:set_chan_stats(ClientId, stats(State)),
    ok = handle_outgoing(ConnAck, State);

handle_info({enter, disconnected}, State = #state{channel = Channel}) ->
    #{clientid := ClientId} = emqx_channel:info(clientinfo, Channel),
    emqx_cm:set_chan_attrs(ClientId, attrs(State)),
    emqx_cm:set_chan_stats(ClientId, stats(State));

handle_info(activate_socket, State = #state{sockstate = OldSst}) ->
    case activate_socket(State) of
        {ok, NState = #state{sockstate = NewSst}} ->
            if OldSst =/= NewSst ->
                   {ok, {event, sockstate_changed}, NState};
               true -> {ok, NState}
            end;
        {error, Reason} ->
            handle_info({sock_error, Reason}, State)
    end;

handle_info({event, sockstate_changed}, State = #state{channel = Channel}) ->
    #{clientid := ClientId} = emqx_channel:info(clientinfo, Channel),
    ClientId =/= undefined andalso emqx_cm:set_chan_attrs(ClientId, attrs(State));

%%TODO: this is not right
handle_info({sock_error, _Reason}, #state{sockstate = closed}) ->
    ok;
handle_info({sock_error, Reason}, State) ->
    ?LOG(debug, "Socket error: ~p", [Reason]),
    handle_info({sock_closed, Reason}, close_socket(State));

handle_info(sock_closed, #state{sockstate = closed}) -> ok;
handle_info(sock_closed, State) ->
    ?LOG(debug, "Socket closed"),
    handle_info({sock_closed, closed}, close_socket(State));

handle_info({close, Reason}, State) ->
    ?LOG(debug, "Force close due to : ~p", [Reason]),
    handle_info({sock_closed, Reason}, close_socket(State));

handle_info(Info, State = #state{channel = Channel}) ->
    handle_chan_return(emqx_channel:handle_info(Info, Channel), State).

%%--------------------------------------------------------------------
%% Ensure rate limit

ensure_rate_limit(Stats, State = #state{limiter = Limiter}) ->
    case ?ENABLED(limiter) andalso emqx_limiter:check(Stats, Limiter) of
        false -> State;
        {ok, Limiter1} ->
            State#state{limiter = Limiter1};
        {pause, Time, Limiter1} ->
            ?LOG(debug, "Pause ~pms due to rate limit", [Time]),
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
        {IsGC, GcSt1} ->
            IsGC andalso emqx_metrics:inc('channel.gc.cnt'),
            State#state{gc_state = GcSt1}
    end.

check_oom(State = #state{channel = Channel}) ->
    #{zone := Zone} = emqx_channel:info(clientinfo, Channel),
    OomPolicy = emqx_zone:oom_policy(Zone),
    case ?ENABLED(OomPolicy) andalso emqx_misc:check_oom(OomPolicy) of
        Shutdown = {shutdown, _Reason} ->
            erlang:send(self(), Shutdown);
        _Other -> ok
    end,
    State.

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

close_socket(State = #state{sockstate = closed}) ->
    State;
close_socket(State = #state{transport = Transport, socket = Socket}) ->
    ok = Transport:fast_close(Socket),
    State#state{sockstate = closed}.

%%--------------------------------------------------------------------
%% Inc incoming/outgoing stats

-compile({inline, [inc_incoming_stats/1]}).
inc_incoming_stats(Packet = ?PACKET(Type)) ->
    emqx_pd:inc_counter(recv_pkt, 1),
    if
        Type == ?PUBLISH ->
            emqx_pd:inc_counter(recv_msg, 1),
            emqx_pd:inc_counter(incoming_pubs, 1);
        true -> ok
    end,
    emqx_metrics:inc_recv(Packet).

-compile({inline, [inc_outgoing_stats/1]}).
inc_outgoing_stats(Packet = ?PACKET(Type)) ->
    emqx_pd:inc_counter(send_pkt, 1),
    if
        Type == ?PUBLISH ->
            emqx_pd:inc_counter(send_msg, 1),
            emqx_pd:inc_counter(outgoing_pubs, 1);
        true -> ok
    end,
    emqx_metrics:inc_sent(Packet).

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

-compile({inline, [shutdown/2, shutdown/3]}).
shutdown(Reason, State) ->
    stop({shutdown, Reason}, State).

shutdown(Reason, Reply, State) ->
    stop({shutdown, Reason}, Reply, State).

-compile({inline, [stop/2, stop/3]}).
stop(Reason, State) ->
    {stop, Reason, State}.

stop(Reason, Reply, State) ->
    {stop, Reason, Reply, State}.

