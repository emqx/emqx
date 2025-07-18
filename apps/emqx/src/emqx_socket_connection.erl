%%--------------------------------------------------------------------
%% Copyright (c) 2018-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% This module interacts with the transport layer of MQTT
%% Transport: esockd_socket.
%%
%% NOTE
%% When changing this module, please make an effort to port changes to
%% `emqx_connection` module if they make sense there, and vice versa.
-module(emqx_socket_connection).

-include("emqx.hrl").
-include("emqx_mqtt.hrl").
-include("logger.hrl").
-include("types.hrl").
-include("emqx_external_trace.hrl").
-include("emqx_instr.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-ifdef(TEST).
-compile(export_all).
-compile(nowarn_export_all).
-endif.

-elvis([{elvis_style, used_ignored_variable, disable}]).

%% API
-export([
    start_link/3,
    stop/1
]).

-export([
    info/1,
    info/2,
    stats/1
]).

%% `emqx_congestion` callbacks:
-export([
    sockstats/2,
    sockopts/2
]).

-export([
    async_set_keepalive/4,
    async_set_socket_options/2
]).

-export([
    call/2,
    call/3,
    cast/2
]).

%% Callback
-export([init/4]).

%% Sys callbacks
-export([
    system_continue/3,
    system_terminate/4,
    system_code_change/4,
    system_get_state/1
]).

%% Internal callback
-export([wakeup_from_hib/2, recvloop/2, get_state/1]).

%% Export for CT
-export([set_field/3]).

-export_type([
    state/0,
    parser/0
]).

-record(state, {
    %% TCP/TLS Socket
    socket :: socket:socket(),
    %% Sock State
    sockstate :: idle | closed | congested(),
    %% Packet parser / serializer
    parser :: parser(),
    serialize :: emqx_frame:serialize_opts(),
    %% Channel State
    channel :: emqx_channel:channel(),
    %% GC State
    gc_state :: option(emqx_gc:gc_state()),
    %% Stats Timer
    %% When `disabled` stats are never reported.
    %% Until complete CONNECT packet received acts as idle timer, which shuts
    %% the connection down once triggered.
    stats_timer :: disabled | option(reference()) | {idle, reference()},
    %% ActiveN + GC tracker
    gc_tracker :: gc_tracker(),
    %% Hibernate connection process if inactive for
    hibernate_after :: integer() | infinity,
    %% Zone name
    zone :: atom(),
    %% Listener Type and Name
    listener :: {Type :: atom(), Name :: atom()},

    %% Extra field for future hot-upgrade support
    extra = []
}).

-record(congested, {
    handle :: reference(),
    deadline :: _TimestampMs :: integer(),
    sendq :: [erlang:iodata()]
}).

-type congested() :: #congested{}.

-type gc_tracker() ::
    {
        ActiveN :: non_neg_integer(),
        {PktsIn :: non_neg_integer(), BytesIn :: non_neg_integer()},
        {PktsOut :: non_neg_integer(), BytesOut :: non_neg_integer()}
    }.

-type parser() ::
    %% Bytestream parser.
    _Stream :: emqx_frame:parse_state().

-opaque state() :: #state{}.

-define(INFO_KEYS, [
    socktype,
    peername,
    sockname,
    sockstate
]).

-define(SOCK_STATS, [
    recv_oct,
    recv_cnt,
    send_oct,
    send_cnt,
    send_pend
]).

-define(ENABLED(X), (X =/= undefined)).

-define(LOG(Level, Data), ?SLOG(Level, (Data)#{tag => "MQTT"})).

-spec start_link(esockd_socket, socket:socket(), emqx_channel:opts()) -> {ok, pid()}.
start_link(Transport, Socket, Options) ->
    Args = [self(), Transport, Socket, Options],
    CPid = proc_lib:spawn_link(?MODULE, init, Args),
    {ok, CPid}.

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

%% @doc Get infos of the connection/channel.
-spec info(pid() | state()) -> emqx_types:infos().
info(CPid) when is_pid(CPid) ->
    call(CPid, info);
info(State = #state{channel = Channel}) ->
    ChanInfo = emqx_channel:info(Channel),
    SockInfo = maps:from_list(info(?INFO_KEYS, State)),
    ChanInfo#{sockinfo => SockInfo}.

-spec info([atom()] | atom() | tuple(), pid() | state()) -> term().
info(Keys, State) when is_list(Keys) ->
    [{Key, info(Key, State)} || Key <- Keys];
info(socktype, #state{}) ->
    tcp;
info(peername, #state{channel = Channel}) ->
    emqx_channel:info(peername, Channel);
info(sockname, #state{channel = Channel}) ->
    emqx_channel:info(sockname, Channel);
info(sockstate, #state{sockstate = SockSt}) ->
    SockSt;
info(stats_timer, #state{stats_timer = StatsTimer}) ->
    StatsTimer;
info({channel, Info}, #state{channel = Channel}) ->
    emqx_channel:info(Info, Channel).

%% @doc Get stats of the connection/channel.
-spec stats(pid() | state()) -> emqx_types:stats().
stats(CPid) when is_pid(CPid) ->
    call(CPid, stats);
stats(State = #state{channel = Channel}) ->
    SockStats = sockstats(?SOCK_STATS, State),
    ChanStats = emqx_channel:stats(Channel),
    ProcStats = emqx_utils:proc_stats(),
    lists:append([SockStats, ChanStats, ProcStats]).

%% @doc Gather socket statistics, for `emqx_congestion` alarms.
-spec sockstats([atom()], state()) -> emqx_types:stats().
sockstats(Keys, #state{socket = Socket, sockstate = SS}) ->
    #{counters := Counters} = socket:info(Socket),
    lists:map(
        fun
            (S = recv_oct) -> {S, maps:get(read_byte, Counters, 0)};
            (S = recv_cnt) -> {S, maps:get(read_pkg, Counters, 0)};
            (S = send_oct) -> {S, maps:get(write_byte, Counters, 0)};
            (S = send_cnt) -> {S, maps:get(write_pkg, Counters, 0)};
            (S = send_pend) -> {S, sendq_bytesize(SS)}
        end,
        Keys
    ).

%% @doc Gather socket options, for `emqx_congestion` alarms.
-spec sockopts([atom()], state()) -> emqx_types:stats().
sockopts(Names, #state{socket = Socket}) ->
    emqx_utils:flattermap(
        fun
            (buffer = N) -> sockopt_val(N, socket:getopt(Socket, {otp, rcvbuf}));
            (recbuf = N) -> sockopt_val(N, socket:getopt(Socket, {socket, rcvbuf}));
            (sndbuf = N) -> sockopt_val(N, socket:getopt(Socket, {socket, sndbuf}));
            (high_watermark) -> _NA = [];
            (high_msgq_watermark) -> _NA = []
        end,
        Names
    ).

sockopt_val(Name, {ok, V}) -> {Name, V};
sockopt_val(_, {error, _}) -> [].

%% @doc Set TCP keepalive socket options to override system defaults.
%% Idle: The number of seconds a connection needs to be idle before
%%       TCP begins sending out keep-alive probes (Linux default 7200).
%% Interval: The number of seconds between TCP keep-alive probes
%%           (Linux default 75).
%% Probes: The maximum number of TCP keep-alive probes to send before
%%         giving up and killing the connection if no response is
%%         obtained from the other end (Linux default 9).
%%
%% NOTE: This API sets TCP socket options, which has nothing to do with
%%       the MQTT layer's keepalive (PINGREQ and PINGRESP).
async_set_keepalive(Pid, Idle, Interval, Probes) ->
    async_set_socket_options(Pid, [
        {{socket, keepalive}, true},
        {{tcp, keepcnt}, Probes},
        {{tcp, keepidle}, Idle},
        {{tcp, keepintvl}, Interval}
    ]).

%% @doc Set custom socket options.
%% This API is made async because the call might be originated from
%% a hookpoint callback (otherwise deadlock).
%% If failed to set, the error message is logged.
async_set_socket_options(Pid, Options) ->
    cast(Pid, {async_set_socket_options, Options}).

cast(Pid, Req) ->
    gen_server:cast(Pid, Req).

call(Pid, Req) ->
    call(Pid, Req, infinity).
call(Pid, Req, Timeout) ->
    gen_server:call(Pid, Req, Timeout).

stop(Pid) ->
    gen_server:stop(Pid).

%%--------------------------------------------------------------------
%% callbacks
%%--------------------------------------------------------------------

init(Parent, esockd_socket, RawSocket, Options) ->
    case esockd_socket:wait(RawSocket) of
        {ok, Socket} ->
            ?tp(connection_started, #{
                socket => Socket,
                listener => maps:get(listener, Options),
                connmod => ?MODULE
            }),
            run_loop(Parent, init_state(Socket, Options));
        {error, Reason} ->
            ok = esockd_socket:fast_close(RawSocket),
            exit_on_sock_error(Reason)
    end.

init_state(
    Socket,
    #{zone := Zone, listener := {Type, Listener}} = Opts
) ->
    SockType = ensure_ok_or_exit(esockd_socket:type(Socket), Socket),
    {ok, Peername} = ensure_ok_or_exit(esockd_socket:peername(Socket), Socket),
    {ok, Sockname} = ensure_ok_or_exit(esockd_socket:sockname(Socket), Socket),
    Peercert = ensure_ok_or_exit(esockd_socket:peercert(Socket), Socket),
    PeerSNI = ensure_ok_or_exit(esockd_socket:peersni(Socket), Socket),
    ConnInfo = #{
        socktype => SockType,
        peername => Peername,
        sockname => Sockname,
        peercert => Peercert,
        peersni => PeerSNI,
        conn_mod => ?MODULE,
        sock => Socket
    },

    ActiveN = get_active_n(Type, Listener),
    FrameOpts = #{
        strict_mode => emqx_config:get_zone_conf(Zone, [mqtt, strict_mode]),
        max_size => emqx_config:get_zone_conf(Zone, [mqtt, max_packet_size])
    },
    Parser = init_parser(FrameOpts),
    Serialize = emqx_frame:initial_serialize_opts(FrameOpts),
    %% Init Channel
    Channel = emqx_channel:init(ConnInfo, Opts),
    GcState =
        case emqx_config:get_zone_conf(Zone, [force_gc]) of
            #{enable := false} -> undefined;
            GcPolicy -> emqx_gc:init(GcPolicy)
        end,

    #state{
        socket = Socket,
        sockstate = idle,
        parser = Parser,
        serialize = Serialize,
        channel = Channel,
        gc_state = GcState,
        gc_tracker = init_gc_tracker(ActiveN),
        hibernate_after = maps:get(hibernate_after, Opts, get_zone_idle_timeout(Zone)),
        zone = Zone,
        listener = {Type, Listener},
        extra = []
    }.

ensure_ok_or_exit(Result, Sock) ->
    case Result of
        {error, Reason} when Reason =:= enotconn; Reason =:= closed ->
            exit(normal);
        {error, Reason} ->
            esockd_socket:fast_close(Sock),
            exit({shutdown, Reason});
        Ok ->
            Ok
    end.

init_gc_tracker(ActiveN) ->
    {ActiveN, {0, 0}, {0, 0}}.

run_loop(
    Parent,
    State = #state{
        socket = Socket,
        channel = Channel,
        listener = Listener,
        zone = Zone
    }
) ->
    emqx_logger:set_proc_metadata(#{
        peername => esockd:format(emqx_channel:info(peername, Channel)),
        connmod => ?MODULE
    }),
    ShutdownPolicy = emqx_config:get_zone_conf(Zone, [force_shutdown]),
    _ = emqx_utils:tune_heap_size(ShutdownPolicy),
    _ = set_tcp_keepalive(Listener),
    case sock_async_recv(Socket, 0) of
        {ok, Data} ->
            NState = start_idle_timer(State),
            handle_recv({recv_more, Data}, Parent, NState);
        {select, _SelectInfo} ->
            NState = start_idle_timer(State),
            hibernate(Parent, NState);
        {error, {Reason, _}} ->
            _ = Reason == closed orelse esockd_socket:fast_close(Socket),
            exit_on_sock_error(Reason);
        {error, Reason} ->
            _ = Reason == closed orelse esockd_socket:fast_close(Socket),
            exit_on_sock_error(Reason)
    end.

-spec exit_on_sock_error(any()) -> no_return().
exit_on_sock_error(Reason) when
    Reason =:= einval;
    Reason =:= enotconn;
    Reason =:= closed
->
    erlang:exit(normal);
exit_on_sock_error(Reason) ->
    erlang:exit({shutdown, Reason}).

%%--------------------------------------------------------------------
%% Recv Loop

recvloop(
    Parent,
    State = #state{
        hibernate_after = HibernateTimeout,
        zone = Zone
    }
) ->
    receive
        Msg ->
            handle_recv(Msg, Parent, State)
    after HibernateTimeout ->
        case emqx_olp:backoff_hibernation(Zone) of
            true ->
                recvloop(Parent, State);
            false ->
                _ = try_set_chan_stats(State),
                hibernate(Parent, cancel_stats_timer(State))
        end
    end.

handle_recv({system, From, Request}, Parent, State) ->
    sys:handle_system_msg(Request, From, Parent, ?MODULE, [], State);
handle_recv({'EXIT', Parent, Reason}, Parent, State) ->
    %% FIXME: it's not trapping exit, should never receive an EXIT
    terminate(Reason, State);
handle_recv(Msg, Parent, State) ->
    case process_msg(Msg, ensure_stats_timer(State)) of
        {ok, NewState} ->
            ?MODULE:recvloop(Parent, NewState);
        {stop, Reason, NewSate} ->
            terminate(Reason, NewSate)
    end.

hibernate(Parent, State) ->
    proc_lib:hibernate(?MODULE, wakeup_from_hib, [Parent, State]).

%% Maybe do something here later.
wakeup_from_hib(Parent, State) ->
    ?MODULE:recvloop(Parent, State).

%%--------------------------------------------------------------------

-compile({inline, [sock_async_recv/2]}).

sock_async_recv(Socket, Len) ->
    socket:recv(Socket, Len, [], nowait).

sock_setopts(Socket, [{Opt, Value} | Rest]) ->
    case socket:setopt(Socket, Opt, Value) of
        ok -> sock_setopts(Socket, Rest);
        Error -> Error
    end;
sock_setopts(_Sock, []) ->
    ok.

%%--------------------------------------------------------------------
%% Ensure/cancel stats timer

init_stats_timer(#state{zone = Zone}) ->
    case emqx_config:get_zone_conf(Zone, [stats, enable]) of
        true -> undefined;
        false -> disabled
    end.

-compile({inline, [ensure_stats_timer/1]}).
ensure_stats_timer(State = #state{stats_timer = undefined}) ->
    Timeout = get_zone_idle_timeout(State#state.zone),
    State#state{stats_timer = start_timer(Timeout, emit_stats)};
ensure_stats_timer(State) ->
    %% Either already active, disabled, or paused.
    State.

-compile({inline, [cancel_stats_timer/1]}).
cancel_stats_timer(State = #state{stats_timer = TRef}) when is_reference(TRef) ->
    ?tp(debug, cancel_stats_timer, #{}),
    ok = emqx_utils:cancel_timer(TRef),
    State#state{stats_timer = undefined};
cancel_stats_timer(State) ->
    State.

start_idle_timer(State = #state{zone = Zone}) ->
    IdleTimeout = get_zone_idle_timeout(Zone),
    TimerRef = start_timer(IdleTimeout, idle_timeout),
    State#state{stats_timer = {idle, TimerRef}}.

cancel_idle_timer(#state{stats_timer = {idle, TRef}}) ->
    emqx_utils:cancel_timer(TRef);
cancel_idle_timer(_State) ->
    ok.

-compile({inline, [get_zone_idle_timeout/1]}).
get_zone_idle_timeout(Zone) ->
    emqx_channel:get_mqtt_conf(Zone, idle_timeout).

%%--------------------------------------------------------------------
%% Process next Msg

process_msgs([], State) ->
    {ok, State};
process_msgs([Msgs | More], State) when is_list(Msgs) ->
    case process_msgs(Msgs, State) of
        {ok, NState} ->
            process_msgs(More, NState);
        Stop ->
            Stop
    end;
process_msgs([Msg | More], State) ->
    case process_msg(Msg, State) of
        {ok, NState} ->
            process_msgs(More, NState);
        Stop ->
            Stop
    end.

process_msg(Msg, State) ->
    try handle_msg(Msg, State) of
        ok ->
            {ok, State};
        {ok, NState} ->
            {ok, NState};
        {ok, NextMsgs, NState} when is_list(NextMsgs) ->
            process_msgs(NextMsgs, NState);
        {ok, NextMsg, NState} ->
            process_msg(NextMsg, NState);
        {stop, Reason, NState} ->
            {stop, Reason, NState};
        {stop, Reason} ->
            {stop, Reason, State}
    catch
        exit:normal ->
            {stop, normal, State};
        exit:shutdown ->
            {stop, shutdown, State};
        exit:{shutdown, _} = Shutdown ->
            {stop, Shutdown, State};
        Exception:Context:Stack ->
            {stop,
                #{
                    exception => Exception,
                    context => Context,
                    stacktrace => Stack
                },
                State}
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
handle_msg({'$gen_cast', Req}, State) ->
    NewState = handle_cast(Req, State),
    {ok, NewState};
handle_msg({'$socket', Socket, select, Handle}, State = #state{sockstate = SS}) ->
    case SS of
        idle ->
            handle_data_ready(Socket, State);
        #congested{handle = Handle} ->
            handle_send_ready(Socket, SS, State);
        _ ->
            handle_data_ready(Socket, State)
    end;
handle_msg({'$socket', _Socket, abort, {_Handle, Reason}}, State = #state{sockstate = SS}) ->
    case SS =/= closed of
        true ->
            handle_info({sock_error, Reason}, State);
        false ->
            %% In case there were more than 1 outstanding select:
            {ok, State}
    end;
handle_msg({recv, Data}, State) ->
    handle_data(Data, false, State);
handle_msg({recv_more, Data}, State) ->
    handle_data(Data, true, State);
handle_msg({incoming, Packet}, State) ->
    ?TRACE("MQTT", "mqtt_packet_received", #{packet => Packet}),
    handle_incoming(Packet, State);
handle_msg({outgoing, Packets}, State) ->
    handle_outgoing(Packets, State);
handle_msg(
    Deliver = {deliver, _Topic, _Msg},
    #state{gc_tracker = {ActiveN, _, _}} = State
) ->
    ?BROKER_INSTR_SETMARK(t0_deliver, {_Msg#message.extra, ?BROKER_INSTR_TS()}),
    Delivers = [Deliver | emqx_utils:drain_deliver(ActiveN)],
    with_channel(handle_deliver, [Delivers], State);
handle_msg({connack, ConnAck}, State) ->
    handle_outgoing(ConnAck, State);
handle_msg({close, Reason}, State) ->
    %% @FIXME here it could be close due to appl error.
    ?TRACE("SOCKET", "socket_force_closed", #{reason => Reason}),
    handle_info({sock_closed, Reason}, close_socket(State));
handle_msg({event, connected}, State = #state{channel = Channel}) ->
    ClientId = emqx_channel:info(clientid, Channel),
    emqx_cm:insert_channel_info(ClientId, info(State), stats(State)),
    {ok, ensure_stats_timer(State)};
handle_msg({event, disconnected}, State = #state{channel = Channel}) ->
    ClientId = emqx_channel:info(clientid, Channel),
    emqx_cm:set_chan_info(ClientId, info(State)),
    {ok, State};
handle_msg({event, _Other}, State = #state{channel = Channel}) ->
    case emqx_channel:info(clientid, Channel) of
        %% ClientId is yet unknown (i.e. connect packet is not received yet)
        undefined ->
            ok;
        ClientId ->
            emqx_cm:set_chan_info(ClientId, info(State)),
            emqx_cm:set_chan_stats(ClientId, stats(State))
    end,
    {ok, State};
handle_msg({timeout, TRef, TMsg}, State) ->
    handle_timeout(TRef, TMsg, State);
handle_msg(Shutdown = {shutdown, _Reason}, State) ->
    stop(Shutdown, State);
handle_msg(Msg, State) ->
    handle_info(Msg, State).

%%--------------------------------------------------------------------
%% Terminate

-spec terminate(any(), state()) -> no_return().
terminate(
    Reason,
    State = #state{channel = Channel}
) ->
    try
        Channel1 = emqx_channel:set_conn_state(disconnected, Channel),
        emqx_congestion:cancel_alarms(?MODULE, State),
        emqx_channel:terminate(Reason, Channel1),
        close_socket_ok(State),
        ?TRACE("SOCKET", "emqx_connection_terminated", #{reason => Reason})
    catch
        E:C:S ->
            ?tp(warning, unclean_terminate, #{exception => E, context => C, stacktrace => S})
    end,
    ?tp(info, terminate, #{reason => Reason}),
    maybe_raise_exception(Reason).

%% close socket, discard new state, always return ok.
close_socket_ok(State) ->
    _ = close_socket(State),
    ok.

%% tell truth about the original exception
-spec maybe_raise_exception(any()) -> no_return().
maybe_raise_exception(#{
    exception := Exception,
    context := Context,
    stacktrace := Stacktrace
}) ->
    erlang:raise(Exception, Context, Stacktrace);
maybe_raise_exception({shutdown, normal}) ->
    ok;
maybe_raise_exception(normal) ->
    ok;
maybe_raise_exception(shutdown) ->
    ok;
maybe_raise_exception(Reason) ->
    exit(Reason).

%%--------------------------------------------------------------------
%% Sys callbacks

system_continue(Parent, _Debug, State) ->
    ?MODULE:recvloop(Parent, State).

system_terminate(Reason, _Parent, _Debug, State) ->
    terminate(Reason, State).

system_code_change(State, _Mod, _OldVsn, _Extra) ->
    {ok, State}.

system_get_state(State) -> {ok, State}.

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
            {ok, NState2} = handle_outgoing(OutPacket, NState),
            NState3 = graceful_shutdown_transport(Reason, NState2),
            shutdown(Reason, Reply, NState3)
    end.

%%--------------------------------------------------------------------
%% Handle timeout

handle_timeout(_TRef, idle_timeout, State) ->
    shutdown(idle_timeout, State);
handle_timeout(
    _TRef,
    emit_stats,
    State = #state{
        channel = Channel,
        sockstate = SS
    }
) ->
    ClientId = emqx_channel:info(clientid, Channel),
    emqx_cm:set_chan_stats(ClientId, stats(State)),
    emqx_congestion:maybe_alarm_conn_congestion(?MODULE, State),
    check_send_timeout(SS, State#state{stats_timer = undefined});
handle_timeout(
    TRef,
    keepalive,
    State = #state{
        channel = Channel
    }
) ->
    case emqx_channel:info(conn_state, Channel) of
        disconnected ->
            {ok, State};
        _ ->
            with_channel(handle_timeout, [TRef, keepalive], State)
    end;
handle_timeout(TRef, Msg, State) ->
    with_channel(handle_timeout, [TRef, Msg], State).

try_set_chan_stats(State = #state{channel = Channel}) ->
    case emqx_channel:info(clientid, Channel) of
        %% ClientID is not yet known, nothing to report.
        undefined -> false;
        ClientId -> emqx_cm:set_chan_stats(ClientId, stats(State))
    end.

%%--------------------------------------------------------------------
%% Parse incoming data

handle_data(
    Data,
    RequestMore,
    State0 = #state{
        socket = Socket,
        sockstate = SS,
        gc_tracker = {ActiveN, {Pubs, Bytes}, Out}
    }
) ->
    Oct = iolist_size(Data),
    emqx_metrics:inc('bytes.received', Oct),
    ?LOG(debug, #{
        msg => "raw_bin_received",
        size => Oct,
        bin => binary_to_list(binary:encode_hex(Data)),
        type => "hex"
    }),
    {More, N, Packets, State1} = parse_incoming(Data, State0),
    State = State1#state{gc_tracker = {ActiveN, {Pubs + N, Bytes + Oct}, Out}},
    Msgs = next_incoming_msgs(Packets),
    case RequestMore of
        false ->
            {ok, Msgs, State};
        true when SS =/= closed ->
            request_more_data(Socket, More, Msgs, State);
        _ ->
            {ok, Msgs, State}
    end.

-compile({inline, [request_more_data/4]}).
request_more_data(Socket, More, Acc, State) ->
    %% TODO: `{otp, select_read}`.
    case sock_async_recv(Socket, More) of
        {ok, DataMore} ->
            {ok, [Acc, {recv_more, DataMore}], State};
        {select, {_Info, DataMore}} ->
            {ok, [Acc, {recv, DataMore}], State};
        {select, _Info} ->
            {ok, Acc, State};
        {error, {closed, DataMore}} ->
            NState = socket_closed(State),
            {ok, [Acc, {recv, DataMore}, {sock_closed, tcp_closed}], NState};
        {error, closed} ->
            NState = socket_closed(State),
            {ok, [Acc, {sock_closed, tcp_closed}], NState};
        {error, {Reason, DataMore}} ->
            {ok, [Acc, {recv, DataMore}, {sock_error, Reason}], State};
        {error, Reason} ->
            {ok, [Acc, {sock_error, Reason}], State}
    end.

-compile({inline, [handle_data_ready/2]}).
handle_data_ready(Socket, State) ->
    case sock_async_recv(Socket, 0) of
        {ok, Data} ->
            handle_data(Data, true, State);
        {error, {closed, Data}} ->
            {ok, [{recv, Data}, {sock_closed, tcp_closed}], socket_closed(State)};
        {error, closed} ->
            handle_info({sock_closed, tcp_closed}, socket_closed(State));
        {error, {Reason, Data}} ->
            {ok, [{recv, Data}, {sock_error, Reason}], State};
        {error, Reason} ->
            handle_info({sock_error, Reason}, State)
    end.

%% @doc: return a reversed Msg list
-compile({inline, [next_incoming_msgs/1]}).
next_incoming_msgs([Packet]) ->
    {incoming, Packet};
next_incoming_msgs(Packets) ->
    Fun = fun(Packet, Acc) -> [{incoming, Packet} | Acc] end,
    lists:foldl(Fun, [], Packets).

parse_incoming(Data, State = #state{parser = Parser}) ->
    try
        run_parser(Data, Parser, State)
    catch
        throw:{?FRAME_PARSE_ERROR, Reason} ->
            ?LOG(info, #{
                msg => "frame_parse_error",
                reason => Reason,
                at_state => describe_parser_state(Parser),
                input_bytes => Data
            }),
            NState = update_state_on_parse_error(Reason, State),
            {0, 0, [{frame_error, Reason}], NState};
        error:Reason:Stacktrace ->
            ?LOG(error, #{
                msg => "frame_parse_failed",
                at_state => describe_parser_state(Parser),
                input_bytes => Data,
                reason => Reason,
                stacktrace => Stacktrace
            }),
            {0, 0, [{frame_error, Reason}], State}
    end.

init_parser(FrameOpts) ->
    %% Go with regular streaming parser.
    emqx_frame:initial_parse_state(FrameOpts).

update_state_on_parse_error(#{proto_ver := ProtoVer, parse_state := ParseState}, State) ->
    Serialize = emqx_frame:serialize_opts(ProtoVer, ?MAX_PACKET_SIZE),
    State#state{serialize = Serialize, parser = ParseState};
update_state_on_parse_error(_, State) ->
    State.

run_parser(Data, ParseState, State) ->
    run_stream_parser(Data, [], 0, ParseState, State).

-compile({inline, [run_stream_parser/5]}).
run_stream_parser(<<>>, Acc, N, NParseState, State) ->
    {0, N, Acc, State#state{parser = NParseState}};
run_stream_parser(Data, Acc, N, ParseState, State) ->
    case emqx_frame:parse(Data, ParseState) of
        {Packet, Rest, NParseState} ->
            run_stream_parser(Rest, [Packet | Acc], N + 1, NParseState, State);
        {More, NParseState} ->
            {More, N, Acc, State#state{parser = NParseState}}
    end.

describe_parser_state(ParseState) ->
    emqx_frame:describe_state(ParseState).

%%--------------------------------------------------------------------
%% Handle incoming packet

handle_incoming(Packet = ?PACKET(Type), State) ->
    inc_incoming_stats(Packet),
    case Type of
        ?CONNECT ->
            %% CONNECT packet is fully received, time to cancel idle timer.
            ok = cancel_idle_timer(State),
            NState = State#state{
                serialize = emqx_frame:serialize_opts(Packet#mqtt_packet.variable),
                stats_timer = init_stats_timer(State)
            },
            with_channel(handle_in, [Packet], NState);
        _ ->
            with_channel(handle_in, [Packet], State)
    end;
handle_incoming(FrameError, State) ->
    with_channel(handle_in, [FrameError], State).

%%--------------------------------------------------------------------
%% With Channel

with_channel(Fun, Args, State = #state{channel = Channel}) ->
    case erlang:apply(emqx_channel, Fun, Args ++ [Channel]) of
        ok ->
            {ok, State};
        {ok, NChannel} ->
            {ok, State#state{channel = NChannel}};
        {ok, Replies, NChannel} ->
            {ok, next_msgs(Replies), State#state{channel = NChannel}};
        {continue, Replies, NChannel} ->
            %% NOTE: Will later go back to `emqx_channel:handle_info/2`.
            {ok, [next_msgs(Replies), continue], State#state{channel = NChannel}};
        {shutdown, Reason, NChannel} ->
            shutdown(Reason, State#state{channel = NChannel});
        {shutdown, Reason, Packet, NChannel} ->
            NState = State#state{channel = NChannel},
            {ok, NState2} = handle_outgoing(Packet, NState),
            shutdown(Reason, NState2)
    end.

%%--------------------------------------------------------------------
%% Handle outgoing packets

handle_outgoing(Packets, State = #state{channel = _Channel}) ->
    Res = do_handle_outgoing(Packets, State),
    _ = ?EXT_TRACE_OUTGOING_STOP(
        emqx_external_trace:basic_attrs(_Channel),
        Packets
    ),
    Res.

do_handle_outgoing(Packets, State) when is_list(Packets) ->
    N = length(Packets),
    send(N, [serialize_and_inc_stats(State, Packet) || Packet <- Packets], State);
do_handle_outgoing(Packet, State) ->
    send(1, serialize_and_inc_stats(State, Packet), State).

serialize_and_inc_stats(#state{serialize = Serialize}, Packet) ->
    try emqx_frame:serialize_pkt(Packet, Serialize) of
        <<>> ->
            ?LOG(warning, #{
                msg => "packet_is_discarded",
                reason => "frame_is_too_large",
                packet => emqx_packet:format(Packet, hidden)
            }),
            emqx_metrics:inc('delivery.dropped.too_large'),
            emqx_metrics:inc('delivery.dropped'),
            inc_dropped_stats(),
            <<>>;
        Data ->
            ?TRACE("MQTT", "mqtt_packet_sent", #{packet => Packet}),
            emqx_metrics:inc_sent(Packet),
            inc_outgoing_stats(Packet),
            Data
    catch
        %% Maybe Never happen.
        throw:{?FRAME_SERIALIZE_ERROR, Reason} ->
            ?LOG(info, #{
                reason => Reason,
                input_packet => Packet
            }),
            erlang:error({?FRAME_SERIALIZE_ERROR, Reason});
        error:Reason:Stacktrace ->
            ?LOG(error, #{
                input_packet => Packet,
                exception => Reason,
                stacktrace => Stacktrace
            }),
            erlang:error(?FRAME_SERIALIZE_ERROR)
    end.

%%--------------------------------------------------------------------
%% Send data

-spec send(non_neg_integer(), iodata(), state()) ->
    {ok, state()} | {ok, {sock_error, _Reason}, state()}.
send(Num, IoData, #state{socket = Socket, sockstate = idle} = State) ->
    Oct = iolist_size(IoData),
    Handle = make_ref(),
    case socket:send(Socket, IoData, [], Handle) of
        ok ->
            sent(Num, Oct, State);
        {select, {_Info, Rest}} ->
            sent(Num, Oct, queue_send(Handle, Rest, State));
        {select, _Info} ->
            sent(Num, Oct, queue_send(Handle, IoData, State));
        {error, {Reason, _Rest}} ->
            %% Defer error handling:
            {ok, {sock_error, Reason}, State};
        {error, Reason} ->
            {ok, {sock_error, Reason}, State}
    end;
send(Num, IoData, #state{sockstate = SS = #congested{sendq = SQ, deadline = Deadline}} = State) ->
    case erlang:monotonic_time(millisecond) of
        BeforeDeadline when BeforeDeadline < Deadline ->
            NState = State#state{sockstate = SS#congested{sendq = [IoData | SQ]}},
            sent(Num, iolist_size(IoData), NState);
        _PastDeadline ->
            {ok, {sock_error, send_timeout}, State}
    end;
send(_Num, _IoVec, #state{sockstate = closed} = State) ->
    {ok, State}.

-compile({inline, [handle_send_ready/3]}).
handle_send_ready(Socket, SS = #congested{sendq = SQ}, State) ->
    IoData = sendq_to_iodata(SQ, []),
    Handle = make_ref(),
    case socket:send(Socket, IoData, [], Handle) of
        ok ->
            NState = State#state{sockstate = idle},
            {ok, NState};
        {select, {_Info, Rest}} ->
            %% Partially accepted, renew deadline.
            {ok, queue_send(Handle, Rest, State)};
        {select, _Info} ->
            %% Totally congested, keep the deadline.
            NSS = SS#congested{handle = Handle, sendq = IoData},
            NState = State#state{sockstate = NSS},
            {ok, NState};
        {error, {Reason, _Rest}} ->
            %% Defer error handling:
            {ok, {sock_error, Reason}, State};
        {error, Reason} ->
            {ok, {sock_error, Reason}, State}
    end.

queue_send(Handle, IoData, State = #state{listener = {Type, Name}}) ->
    Timeout = emqx_config:get_listener_conf(Type, Name, [tcp_options, send_timeout], 15_000),
    Deadline = erlang:monotonic_time(millisecond) + Timeout,
    SockState = #congested{handle = Handle, deadline = Deadline, sendq = [IoData]},
    State#state{sockstate = SockState}.

check_send_timeout(#congested{deadline = Deadline}, State) ->
    case erlang:monotonic_time(millisecond) of
        BeforeDeadline when BeforeDeadline < Deadline ->
            {ok, State};
        _PastDeadline ->
            {ok, {sock_error, send_timeout}, State}
    end;
check_send_timeout(_, State) ->
    {ok, State}.

sendq_to_iodata([IoData | Rest], Acc) ->
    sendq_to_iodata(Rest, [IoData | Acc]);
sendq_to_iodata([], Acc) ->
    Acc.

sendq_bytesize(#congested{sendq = SQ}) ->
    erlang:iolist_size(SQ);
sendq_bytesize(_) ->
    0.

%% Some bytes sent
sent(
    Num,
    Oct,
    State = #state{gc_tracker = {ActiveN, In = {PktsIn, BytesIn}, {PktsOut, BytesOut}}}
) ->
    %% TODO: Not actually "sent", as is `emqx_metrics:inc_sent/1`.
    emqx_metrics:inc('bytes.sent', Oct),
    NPktsOut = PktsOut + Num,
    NBytesOut = BytesOut + Oct,
    if
        PktsIn > ActiveN ->
            %% Run GC and check OOM after certain amount of messages or bytes received.
            NState = trigger_gc(PktsIn, BytesIn, ActiveN, State);
        NPktsOut > ActiveN ->
            %% Run GC and check OOM after certain amount of messages or bytes sent.
            NState = trigger_gc(NPktsOut, NBytesOut, ActiveN, State);
        true ->
            NState = State#state{gc_tracker = {ActiveN, In, {NPktsOut, NBytesOut}}}
    end,
    ?BROKER_INSTR_WMARK(t0_deliver, {T0, TDeliver} when is_integer(T0), begin
        TSent = ?BROKER_INSTR_TS(),
        ?BROKER_INSTR_OBSERVE_HIST(connection, deliver_delay_us, ?US(TDeliver - T0)),
        ?BROKER_INSTR_OBSERVE_HIST(connection, deliver_total_lat_us, ?US(TSent - T0))
    end),
    {ok, NState}.

-compile({inline, [trigger_gc/4]}).
trigger_gc(NPkts, NBytes, ActiveN, State) ->
    NState = State#state{gc_tracker = init_gc_tracker(ActiveN)},
    check_oom(NPkts, NBytes, run_gc(NPkts, NBytes, NState)).

%%--------------------------------------------------------------------
%% Handle Info

handle_info({sock_error, Reason}, State) ->
    case Reason =/= closed andalso Reason =/= einval of
        true -> ?SLOG(warning, #{msg => "socket_error", reason => Reason});
        false -> ok
    end,
    handle_info({sock_closed, Reason}, ensure_close_socket(Reason, State));
%% handle QUIC control stream events
handle_info({quic, Event, Handle, Prop}, State) when is_atom(Event) ->
    case emqx_quic_stream:Event(Handle, Prop, State) of
        {{continue, Msgs}, NewState} ->
            {ok, Msgs, NewState};
        Other ->
            Other
    end;
handle_info(Info, State) ->
    with_channel(handle_info, [Info], State).

%%--------------------------------------------------------------------
%% Handle Info

handle_cast(
    {async_set_socket_options, SockOpts},
    State = #state{socket = Socket}
) ->
    case sock_setopts(Socket, SockOpts) of
        ok ->
            ?tp(debug, "custom_socket_options_successfully", #{opts => SockOpts});
        {error, {invalid, {socket_option, SockOpt}}} ->
            ?tp(warning, "unsupported_socket_keepalive", #{option => SockOpt});
        {error, Reason} when Reason == closed; Reason == einval ->
            %% socket is already closed, ignore this error
            ?tp(debug, "socket already closed", #{reason => socket_already_closed}),
            ok;
        Err ->
            %% other errors
            ?tp(error, "failed_to_set_custom_socket_option", #{reason => Err})
    end,
    State;
handle_cast(Req, State) ->
    ?tp(error, "received_unknown_cast", #{cast => Req}),
    State.

%%--------------------------------------------------------------------
%% Run GC and Check OOM

run_gc(Pubs, Bytes, State = #state{gc_state = GcSt, zone = Zone}) ->
    case
        ?ENABLED(GcSt) andalso not emqx_olp:backoff_gc(Zone) andalso
            emqx_gc:run(Pubs, Bytes, GcSt)
    of
        false -> State;
        {_IsGC, GcSt1} -> State#state{gc_state = GcSt1}
    end.

check_oom(Pubs, Bytes, State = #state{zone = Zone}) ->
    ShutdownPolicy = emqx_config:get_zone_conf(Zone, [force_shutdown]),
    case emqx_utils:check_oom(ShutdownPolicy) of
        {shutdown, Reason} ->
            %% triggers terminate/2 callback immediately
            ?tp(warning, check_oom_shutdown, #{
                policy => ShutdownPolicy,
                incoming_pubs => Pubs,
                incoming_bytes => Bytes,
                shutdown => Reason
            }),
            erlang:exit({shutdown, Reason});
        Result ->
            ?tp(debug, check_oom_ok, #{
                policy => ShutdownPolicy,
                incoming_pubs => Pubs,
                incoming_bytes => Bytes,
                result => Result
            }),
            ok
    end,
    State.

%%--------------------------------------------------------------------
%% Close Socket

ensure_close_socket(closed, State) ->
    socket_closed(State);
ensure_close_socket(_Reason, State) ->
    close_socket(State).

close_socket(State = #state{sockstate = closed}) ->
    State;
close_socket(State = #state{socket = Socket}) ->
    ok = esockd_socket:fast_close(Socket),
    State#state{sockstate = closed}.

socket_closed(State) ->
    State#state{sockstate = closed}.

%%--------------------------------------------------------------------
%% Inc incoming/outgoing stats

-compile({inline, [inc_incoming_stats/1]}).
inc_incoming_stats(Packet = ?PACKET(Type)) ->
    inc_counter(recv_pkt, 1),
    case Type of
        ?PUBLISH ->
            inc_counter(recv_msg, 1),
            inc_qos_stats(recv_msg, Packet);
        _ ->
            ok
    end,
    emqx_metrics:inc_recv(Packet).

-compile({inline, [inc_dropped_stats/0]}).
inc_dropped_stats() ->
    inc_counter('send_msg.dropped', 1),
    inc_counter('send_msg.dropped.too_large', 1).

-compile({inline, [inc_outgoing_stats/1]}).
inc_outgoing_stats(Packet = ?PACKET(Type)) ->
    inc_counter(send_pkt, 1),
    case Type of
        ?PUBLISH ->
            inc_counter(send_msg, 1),
            inc_qos_stats(send_msg, Packet);
        _ ->
            ok
    end.

-compile({inline, [inc_qos_stats/2]}).
inc_qos_stats(Type, Packet) ->
    case emqx_packet:qos(Packet) of
        ?QOS_0 when Type =:= send_msg -> inc_counter('send_msg.qos0', 1);
        ?QOS_1 when Type =:= send_msg -> inc_counter('send_msg.qos1', 1);
        ?QOS_2 when Type =:= send_msg -> inc_counter('send_msg.qos2', 1);
        ?QOS_0 when Type =:= recv_msg -> inc_counter('recv_msg.qos0', 1);
        ?QOS_1 when Type =:= recv_msg -> inc_counter('recv_msg.qos1', 1);
        ?QOS_2 when Type =:= recv_msg -> inc_counter('recv_msg.qos2', 1);
        %% for bad qos
        _ -> ok
    end.

%%--------------------------------------------------------------------
%% Helper functions

-compile({inline, [next_msgs/1]}).
next_msgs(Packet) when is_record(Packet, mqtt_packet) ->
    {outgoing, Packet};
next_msgs(Event) when is_tuple(Event) ->
    Event;
next_msgs(More) when is_list(More) ->
    More.

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

inc_counter(Key, Inc) ->
    _ = emqx_pd:inc_counter(Key, Inc),
    ok.

set_tcp_keepalive({tcp, Id}) ->
    Conf = emqx_config:get_listener_conf(tcp, Id, [tcp_options, keepalive], "none"),
    case Conf of
        "none" ->
            ok;
        Value ->
            {Idle, Interval, Probes} = emqx_schema:parse_tcp_keepalive(Value),
            async_set_keepalive(self(), Idle, Interval, Probes)
    end.

-spec graceful_shutdown_transport(atom(), state()) -> state().
graceful_shutdown_transport(_Reason, S = #state{socket = Socket}) ->
    _ = socket:shutdown(Socket, read_write),
    S#state{sockstate = closed}.

start_timer(Time, Msg) ->
    emqx_utils:start_timer(Time, Msg).

%%--------------------------------------------------------------------
%% For CT tests
%%--------------------------------------------------------------------

set_field(Name, Value, State) ->
    Pos = emqx_utils:index_of(Name, record_info(fields, state)),
    setelement(Pos + 1, State, Value).

get_state(Pid) ->
    State = sys:get_state(Pid),
    maps:from_list(
        lists:zip(
            record_info(fields, state),
            tl(tuple_to_list(State))
        )
    ).

get_active_n(Type, Listener) ->
    emqx_config:get_listener_conf(Type, Listener, [tcp_options, active_n]).
