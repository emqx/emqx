%%--------------------------------------------------------------------
%% Copyright (c) 2018-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% MQTT/WS|WSS Connection
-module(emqx_ws_connection).

-include("emqx_mqtt.hrl").
-include("logger.hrl").
-include("types.hrl").
-include("emqx_external_trace.hrl").

-ifdef(TEST).
-compile(export_all).
-compile(nowarn_export_all).
-endif.

-elvis([{elvis_style, used_ignored_variable, disable}]).

%% API
-export([
    info/1,
    info/2,
    stats/1
]).

-export([
    call/2,
    call/3
]).

%% WebSocket callbacks
-export([
    init/2,
    websocket_init/1,
    websocket_handle/2,
    websocket_info/2,
    websocket_close/2,
    terminate/3
]).

%% Export for CT
-export([set_field/3]).

-record(state, {
    %% Peername of the ws connection
    peername :: emqx_types:peername(),
    %% Sockname of the ws connection
    sockname :: emqx_types:peername(),
    %% Sock state
    sockstate :: running | shutdown,
    %% MQTT Piggyback
    mqtt_piggyback :: single | multiple,
    %% Parse State
    parse_state :: emqx_frame:parse_state(),
    %% Serialize options
    serialize :: emqx_frame:serialize_opts(),
    %% Channel
    channel :: emqx_channel:channel(),
    %% GC State
    gc_state :: option(emqx_gc:gc_state()),
    %% Stats Timer
    stats_timer :: paused | disabled | option(reference()),
    %% Idle Timer
    idle_timer :: option(reference()),
    %% Zone name
    zone :: atom(),
    %% Listener Type and Name
    listener :: {Type :: atom(), Name :: atom()},

    %% Extra field for future hot-upgrade support
    extra = []
}).

-type state() :: #state{}.

-define(INFO_KEYS, [socktype, peername, sockname, sockstate]).
-define(SOCK_STATS, [recv_oct, recv_cnt, send_oct, send_cnt]).

-define(ENABLED(X), (X =/= undefined)).

-define(LOG(Level, Data),
    ?SLOG(Level, (begin
        Data
    end)#{
        tag => "MQTT"
    })
).

%%--------------------------------------------------------------------
%% Info, Stats
%%--------------------------------------------------------------------

-type info() :: atom() | {channel, _Info}.

-spec info(pid() | state()) -> emqx_types:infos().
info(WsPid) when is_pid(WsPid) ->
    call(WsPid, info);
info(State = #state{channel = Channel}) ->
    ChanInfo = emqx_channel:info(Channel),
    SockInfo = maps:from_list(
        info(?INFO_KEYS, State)
    ),
    ChanInfo#{sockinfo => SockInfo}.

-spec info
    (info(), state()) -> _Value;
    (info(), state()) -> [{atom(), _Value}].
info(Keys, State) when is_list(Keys) ->
    [{Key, info(Key, State)} || Key <- Keys];
info(socktype, _State) ->
    ws;
info(peername, #state{peername = Peername}) ->
    Peername;
info(sockname, #state{sockname = Sockname}) ->
    Sockname;
info(sockstate, #state{sockstate = SockSt}) ->
    SockSt;
info(channel, #state{channel = Channel}) ->
    emqx_channel:info(Channel);
info(gc_state, #state{gc_state = GcSt}) ->
    emqx_maybe:apply(fun emqx_gc:info/1, GcSt);
info(stats_timer, #state{stats_timer = TRef}) ->
    TRef;
info(idle_timer, #state{idle_timer = TRef}) ->
    TRef;
info({channel, Info}, #state{channel = Channel}) ->
    emqx_channel:info(Info, Channel).

-spec stats(pid() | state()) -> emqx_types:stats().
stats(WsPid) when is_pid(WsPid) ->
    call(WsPid, stats);
stats(#state{channel = Channel}) ->
    SockStats = emqx_pd:get_counters(?SOCK_STATS),
    ChanStats = emqx_channel:stats(Channel),
    ProcStats = emqx_utils:proc_stats(),
    lists:append([SockStats, ChanStats, ProcStats]).

%% kick|discard|takeover
-spec call(pid(), Req :: term()) -> Reply :: term().
call(WsPid, Req) ->
    call(WsPid, Req, 5000).

call(WsPid, Req, Timeout) when is_pid(WsPid) ->
    Mref = erlang:monitor(process, WsPid),
    WsPid ! {call, {self(), Mref}, Req},
    receive
        {Mref, Reply} ->
            ok = emqx_pmon:demonitor(Mref),
            Reply;
        {'DOWN', Mref, _, _, Reason} ->
            exit(Reason)
    after Timeout ->
        ok = emqx_pmon:demonitor(Mref),
        exit(timeout)
    end.

%%--------------------------------------------------------------------
%% WebSocket callbacks
%%--------------------------------------------------------------------

init(Req, #{listener := {Type, Listener}} = Opts) ->
    WsOpts = get_ws_opts(Type, Listener),
    case check_origin_header(Req, WsOpts) of
        ok ->
            check_max_connections(Type, Listener, Req, Opts, WsOpts);
        {error, Reason} ->
            ?SLOG(error, #{msg => "invalid_origin_header", reason => Reason}),
            {ok, cowboy_req:reply(403, Req), #{}}
    end.

check_max_connections(Type, Listener, Req, Opts, WsOpts) ->
    case emqx_config:get_listener_conf(Type, Listener, [max_connections]) of
        infinity ->
            check_sec_websocket_protocol(Type, Listener, Req, Opts, WsOpts);
        Max ->
            case get_current_connections(Req) of
                N when N < Max ->
                    check_sec_websocket_protocol(Type, Listener, Req, Opts, WsOpts);
                N ->
                    Reason = #{
                        msg => "websocket_max_connections_limited",
                        current => N,
                        max => Max
                    },
                    ?SLOG(warning, Reason),
                    {ok, cowboy_req:reply(429, Req), #{}}
            end
    end.

check_sec_websocket_protocol(Type, Listener, Req, Opts, WsOpts) ->
    Header = <<"sec-websocket-protocol">>,
    #{
        fail_if_no_subprotocol := FailIfNoSubprotocol,
        supported_subprotocols := SupportedSubprotocols
    } = WsOpts,
    case cowboy_req:parse_header(Header, Req) of
        undefined when not FailIfNoSubprotocol ->
            upgrade(Type, Listener, Req, Opts, WsOpts);
        undefined ->
            {ok, cowboy_req:reply(400, Req), #{}};
        Subprotocols ->
            case pick_subprotocol(Subprotocols, SupportedSubprotocols) of
                {ok, Subprotocol} ->
                    NReq = cowboy_req:set_resp_header(Header, Subprotocol, Req),
                    upgrade(Type, Listener, NReq, Opts, WsOpts);
                {error, no_supported_subprotocol} ->
                    {ok, cowboy_req:reply(400, Req), #{}}
            end
    end.

upgrade(Type, Listener, Req, Opts, WsOpts) ->
    {Peername, PeerCert, PeerSNI} = get_peer_info(Type, Listener, Req, Opts),
    Sockname = cowboy_req:sock(Req),
    WsCookie = get_ws_cookie(Req),
    ConnInfo = #{
        socktype => ws,
        peername => Peername,
        sockname => Sockname,
        peercert => PeerCert,
        peersni => PeerSNI,
        ws_cookie => WsCookie,
        conn_mod => ?MODULE
    },
    CowboyWsOpts = #{
        active_n => get_active_n(Type, Listener),
        compress => maps:get(compress, WsOpts),
        deflate_opts => maps:get(deflate_opts, WsOpts),
        max_frame_size => maps:get(max_frame_size, WsOpts),
        idle_timeout => maps:get(idle_timeout, WsOpts),
        validate_utf8 => maps:get(validate_utf8, WsOpts)
    },
    {cowboy_websocket_linger, Req, [ConnInfo, Opts], CowboyWsOpts}.

pick_subprotocol([], _SupportedSubprotocols) ->
    {error, no_supported_subprotocol};
pick_subprotocol([Subprotocol | Rest], SupportedSubprotocols) ->
    case lists:member(Subprotocol, SupportedSubprotocols) of
        true ->
            {ok, Subprotocol};
        false ->
            pick_subprotocol(Rest, SupportedSubprotocols)
    end.

parse_header_fun_origin(Req, #{
    allow_origin_absence := AllowOriginAbsence,
    check_origins := CheckOrigins
}) ->
    case cowboy_req:header(<<"origin">>, Req) of
        undefined when AllowOriginAbsence ->
            ok;
        undefined ->
            {error, origin_header_cannot_be_absent};
        Value ->
            case lists:member(Value, CheckOrigins) of
                true -> ok;
                false -> {error, #{bad_origin => Value}}
            end
    end.

check_origin_header(Req, #{check_origin_enable := true} = WsOpts) ->
    parse_header_fun_origin(Req, WsOpts);
check_origin_header(_Req, #{check_origin_enable := false}) ->
    ok.

websocket_init([ConnInfo, Opts]) ->
    init_connection(ConnInfo, Opts).

get_current_connections(Req) ->
    %% NOTE: Involves a gen:call to the connections supervisor.
    RanchRef = maps:get(ref, Req),
    RanchConnsSup = ranch_server:get_connections_sup(RanchRef),
    proplists:get_value(active, supervisor:count_children(RanchConnsSup), 0).

init_connection(
    ConnInfo = #{peername := Peername, sockname := Sockname},
    Opts = #{listener := {Type, Listener}, zone := Zone}
) ->
    MQTTPiggyback = get_ws_opt(Type, Listener, mqtt_piggyback),
    FrameOpts = #{
        strict_mode => emqx_config:get_zone_conf(Zone, [mqtt, strict_mode]),
        max_size => emqx_config:get_zone_conf(Zone, [mqtt, max_packet_size])
    },
    ParseState = emqx_frame:initial_parse_state(FrameOpts),
    Serialize = emqx_frame:initial_serialize_opts(FrameOpts),
    Channel = emqx_channel:init(ConnInfo, Opts),
    GcState = get_force_gc(Zone),
    StatsTimer = get_stats_enable(Zone),
    %% MQTT Idle Timeout
    IdleTimeout = emqx_channel:get_mqtt_conf(Zone, idle_timeout),
    IdleTimer = emqx_utils:start_timer(IdleTimeout, idle_timeout),
    _ = tune_heap_size(Channel),
    emqx_logger:set_metadata_peername(esockd:format(Peername)),
    State = #state{
        peername = Peername,
        sockname = Sockname,
        sockstate = running,
        mqtt_piggyback = MQTTPiggyback,
        parse_state = ParseState,
        serialize = Serialize,
        channel = Channel,
        gc_state = GcState,
        stats_timer = StatsTimer,
        idle_timer = IdleTimer,
        zone = Zone,
        listener = {Type, Listener},
        extra = []
    },
    init_gc_metrics(),
    {ok, State, hibernate}.

tune_heap_size(Channel) ->
    case
        emqx_config:get_zone_conf(
            emqx_channel:info(zone, Channel),
            [force_shutdown]
        )
    of
        #{enable := false} -> ok;
        ShutdownPolicy -> emqx_utils:tune_heap_size(ShutdownPolicy)
    end.

get_stats_enable(Zone) ->
    case emqx_config:get_zone_conf(Zone, [stats, enable]) of
        true -> paused;
        false -> disabled
    end.

get_force_gc(Zone) ->
    case emqx_config:get_zone_conf(Zone, [force_gc]) of
        #{enable := false} -> undefined;
        GcPolicy -> emqx_gc:init(GcPolicy)
    end.

get_ws_cookie(Req) ->
    try
        cowboy_req:parse_cookies(Req)
    catch
        error:badarg ->
            ?SLOG(error, #{msg => "bad_cookie"}),
            undefined;
        Error:Reason ->
            ?SLOG(error, #{
                msg => "failed_to_parse_cookie",
                exception => Error,
                reason => Reason
            }),
            undefined
    end.

get_peer_info(Type, Listener, Req, Opts) ->
    Host = maps:get(host, Req, undefined),
    case
        emqx_config:get_listener_conf(Type, Listener, [proxy_protocol]) andalso
            maps:get(proxy_header, Req)
    of
        #{src_address := SrcAddr, src_port := SrcPort, ssl := SSL} = ProxyInfo ->
            SourceName = {SrcAddr, SrcPort},
            %% Notice: CN is only available in Proxy Protocol V2 additional info.
            %% `CN` is unsupported in Proxy Protocol V1
            %% `pp2_ssl_cn` is required by config `peer_cert_as_username` or `peer_cert_as_clientid`.
            %% It will be parsed by esockd.
            %% See also `emqx_channel:set_peercert_infos/3` and `esockd_peercert:common_name/1`
            SourceSSL =
                case maps:get(cn, SSL, undefined) of
                    undefined -> undefined;
                    CN -> [{pp2_ssl_cn, CN}]
                end,
            PeerSNI = maps:get(authority, ProxyInfo, Host),
            {SourceName, SourceSSL, PeerSNI};
        #{src_address := SrcAddr, src_port := SrcPort} = ProxyInfo ->
            PeerSNI = maps:get(authority, ProxyInfo, Host),
            SourceName = {SrcAddr, SrcPort},
            {SourceName, nossl, PeerSNI};
        _ ->
            {get_peer(Req, Opts), cowboy_req:cert(Req), Host}
    end.

websocket_handle({binary, Data}, State) ->
    on_frame_in(Data, State);
%% Pings should be replied with pongs, cowboy does it automatically
%% Pongs can be safely ignored. Clause here simply prevents crash.
websocket_handle(Frame, State) when Frame =:= ping; Frame =:= pong ->
    {ok, State};
websocket_handle({Frame, _}, State) when Frame =:= ping; Frame =:= pong ->
    {ok, State};
websocket_handle({Frame, _}, State) ->
    %% TODO: should not close the ws connection
    ?LOG(error, #{msg => "unexpected_frame", frame => Frame}),
    {[{shutdown, unexpected_ws_frame}], State}.

websocket_info({call, From, Req}, State) ->
    handle_call(From, Req, State);
websocket_info({gc, Which}, State = #state{listener = {Type, Listener}}) ->
    Metrics = rotate_gc_metrics(Which, get_active_n(Type, Listener)),
    check_oom(run_gc(Metrics, State));
websocket_info(
    Deliver = {deliver, _Topic, _Msg},
    State = #state{listener = {Type, Listener}}
) ->
    ActiveN = get_active_n(Type, Listener),
    Delivers = [Deliver | emqx_utils:drain_deliver(ActiveN)],
    commands(with_channel(handle_deliver, [Delivers], {[], State}));
websocket_info({timeout, TRef, Msg}, State) when is_reference(TRef) ->
    handle_timeout(TRef, Msg, State);
websocket_info({stop, Reason}, State) ->
    {[{shutdown, Reason}], State};
websocket_info(Info, State) ->
    handle_info(Info, State).

websocket_close({_, ReasonCode, _Payload}, State) when is_integer(ReasonCode) ->
    websocket_close(ReasonCode, State);
websocket_close(Reason, State) ->
    ?TRACE("SOCKET", "websocket_closed", #{reason => Reason}),
    handle_info({sock_closed, Reason}, State).

terminate(Reason, _Req, #state{channel = Channel}) ->
    ?TRACE("SOCKET", "websocket_terminated", #{reason => Reason}),
    emqx_channel:terminate(Reason, Channel);
terminate(_Reason, _Req, _UnExpectedState) ->
    ok.

-compile({inline, [commands/1]}).
commands({FrameAcc, State}) ->
    case lists:flatten(lists:reverse(FrameAcc)) of
        [] -> {ok, State};
        Commands -> {Commands, State}
    end.

%%--------------------------------------------------------------------
%% Handle call
%%--------------------------------------------------------------------

handle_call(From, info, State) ->
    gen_server:reply(From, info(State)),
    {ok, State};
handle_call(From, stats, State) ->
    gen_server:reply(From, stats(State)),
    {ok, State};
handle_call(From, Req, State = #state{channel = Channel}) ->
    case emqx_channel:handle_call(Req, Channel) of
        {reply, Reply, NChannel} ->
            gen_server:reply(From, Reply),
            {ok, State#state{channel = NChannel}};
        {shutdown, Reason, Reply, NChannel} ->
            gen_server:reply(From, Reply),
            NState = State#state{channel = NChannel},
            commands(order_shutdown(Reason, [], NState));
        {shutdown, Reason, Reply, Packet, NChannel} ->
            gen_server:reply(From, Reply),
            NState = State#state{channel = NChannel},
            Frames = handle_outgoing(Packet, NState),
            commands(order_shutdown(Reason, [Frames], NState))
    end.

%%--------------------------------------------------------------------
%% Handle Info
%%--------------------------------------------------------------------

handle_info(Info, State) ->
    commands(with_channel(handle_info, [Info], {[], State})).

handle_event({event, connected}, State = #state{channel = Channel}) ->
    ClientId = emqx_channel:info(clientid, Channel),
    emqx_cm:insert_channel_info(ClientId, info(State), stats(State)),
    resume_stats_timer(State);
handle_event({event, disconnected}, State = #state{channel = Channel}) ->
    ClientId = emqx_channel:info(clientid, Channel),
    emqx_cm:set_chan_info(ClientId, info(State)),
    State;
handle_event({event, _Other}, State = #state{channel = Channel}) ->
    case emqx_channel:info(clientid, Channel) of
        %% ClientId is yet unknown (i.e. connect packet is not received yet)
        undefined ->
            ok;
        ClientId ->
            emqx_cm:set_chan_info(ClientId, info(State)),
            emqx_cm:set_chan_stats(ClientId, stats(State))
    end,
    State.

%%--------------------------------------------------------------------
%% Handle timeout
%%--------------------------------------------------------------------

handle_timeout(TRef, idle_timeout, State = #state{idle_timer = TRef}) ->
    {[{shutdown, idle_timeout}], State};
handle_timeout(
    TRef,
    emit_stats,
    State = #state{
        channel = Channel,
        stats_timer = TRef
    }
) ->
    ClientId = emqx_channel:info(clientid, Channel),
    emqx_cm:set_chan_stats(ClientId, stats(State)),
    {ok, State#state{stats_timer = undefined}};
handle_timeout(TRef, TMsg, State) ->
    commands(with_channel(handle_timeout, [TRef, TMsg], {[], State})).

%%--------------------------------------------------------------------
%% Run GC, Check OOM
%%--------------------------------------------------------------------

init_gc_metrics() ->
    erlang:put(last_recv_cnt, 0),
    erlang:put(last_recv_oct, 0),
    erlang:put(last_send_cnt, 0),
    erlang:put(last_send_oct, 0),
    erlang:put(gc_recv_cnt, 0),
    erlang:put(gc_send_cnt, 0).

rotate_gc_metrics(incoming, ActiveN) ->
    RecvCnt = emqx_pd:get_counter(recv_cnt),
    RecvOct = emqx_pd:get_counter(recv_oct),
    _ = erlang:put(gc_recv_cnt, RecvCnt + ActiveN),
    RecvCntLast = erlang:put(last_recv_cnt, RecvCnt),
    RecvOctLast = erlang:put(last_recv_oct, RecvOct),
    Cnt = RecvCnt - RecvCntLast,
    Oct = RecvOct - RecvOctLast,
    {Cnt, Oct};
rotate_gc_metrics(outgoing, ActiveN) ->
    SendCnt = emqx_pd:get_counter(send_cnt),
    SendOct = emqx_pd:get_counter(send_oct),
    _ = erlang:put(gc_send_cnt, SendCnt + ActiveN),
    SendCntLast = erlang:put(last_send_cnt, SendCnt),
    SendOctLast = erlang:put(last_send_oct, SendOct),
    Cnt = SendCnt - SendCntLast,
    Oct = SendOct - SendOctLast,
    {Cnt, Oct}.

trigger_gc_incoming() ->
    case emqx_pd:get_counter(recv_cnt) > erlang:get(gc_recv_cnt) of
        true ->
            erlang:erase(gc_recv_cnt),
            self() ! {gc, incoming};
        false ->
            false
    end.

trigger_gc_outgoing() ->
    case emqx_pd:get_counter(send_cnt) > erlang:get(gc_send_cnt) of
        true ->
            erlang:erase(gc_send_cnt),
            self() ! {gc, outgoing};
        false ->
            false
    end.

run_gc({Cnt, Oct}, State = #state{gc_state = GcSt}) ->
    case ?ENABLED(GcSt) andalso emqx_gc:run(Cnt, Oct, GcSt) of
        false -> State;
        {_IsGC, GcSt1} -> State#state{gc_state = GcSt1}
    end.

check_oom(State = #state{zone = Zone}) ->
    ShutdownPolicy = emqx_config:get_zone_conf(Zone, [force_shutdown]),
    case ShutdownPolicy of
        #{enable := false} ->
            {ok, State};
        #{enable := true} ->
            case emqx_utils:check_oom(ShutdownPolicy) of
                Shutdown = {shutdown, _Reason} ->
                    {[Shutdown], State};
                _Other ->
                    {ok, State}
            end
    end.

%%--------------------------------------------------------------------
%% Parse incoming data
%%--------------------------------------------------------------------

on_frame_in(Data, State) when is_list(Data) ->
    on_frame_in(iolist_to_binary(Data), State);
on_frame_in(Data, State) ->
    ?LOG(debug, #{
        msg => "raw_bin_received",
        size => byte_size(Data),
        bin => binary_to_list(binary:encode_hex(Data)),
        type => "hex"
    }),
    State2 = ensure_stats_timer(State),
    {Packets, State3} = parse_incoming(Data, [], State2),
    inc_recv_stats(length(Packets), byte_size(Data)),
    handle_incoming(Packets, State3).

parse_incoming(<<>>, Packets, State) ->
    {lists:reverse(Packets), State};
parse_incoming(Data, Packets, State = #state{parse_state = ParseState}) ->
    try emqx_frame:parse(Data, ParseState) of
        {_More, NParseState} ->
            {Packets, State#state{parse_state = NParseState}};
        {Packet, Rest, NParseState} ->
            NState = State#state{parse_state = NParseState},
            parse_incoming(Rest, [Packet | Packets], NState)
    catch
        throw:{?FRAME_PARSE_ERROR, Reason} ->
            ?LOG(info, #{
                msg => "frame_parse_error",
                reason => Reason,
                at_state => emqx_frame:describe_state(ParseState),
                input_bytes => Data
            }),
            NState = update_state_on_parse_error(Reason, State),
            {[{frame_error, Reason} | Packets], NState};
        error:Reason:Stacktrace ->
            ?LOG(error, #{
                msg => "frame_parse_failed",
                at_state => emqx_frame:describe_state(ParseState),
                input_bytes => Data,
                exception => Reason,
                stacktrace => Stacktrace
            }),
            {[{frame_error, Reason} | Packets], State}
    end.

update_state_on_parse_error(#{proto_ver := ProtoVer, parse_state := NParseState}, State) ->
    Serialize = emqx_frame:serialize_opts(ProtoVer, ?MAX_PACKET_SIZE),
    State#state{parse_state = NParseState, serialize = Serialize};
update_state_on_parse_error(_, State) ->
    State.

%%--------------------------------------------------------------------
%% Handle incoming packet
%%--------------------------------------------------------------------

handle_incoming(Packets = [?CONNECT_PACKET(ConnPkt) | _], State) ->
    Serialize = emqx_frame:serialize_opts(ConnPkt),
    NState = cancel_idle_timer(State#state{serialize = Serialize}),
    do_handle_incoming(Packets, NState);
handle_incoming(Packets, State) ->
    do_handle_incoming(Packets, State).

-compile({inline, [do_handle_incoming/2]}).
do_handle_incoming(Packets, State) ->
    Result = handle_incoming_packets(Packets, {[], State}),
    _ = trigger_gc_incoming(),
    commands(Result).

handle_incoming_packets([Packet = #mqtt_packet{} | Packets], ResAcc) ->
    ?TRACE("WS-MQTT", "mqtt_packet_received", #{packet => Packet}),
    ok = inc_incoming_stats(Packet),
    handle_incoming_packets(Packets, with_channel(handle_in, [Packet], ResAcc));
handle_incoming_packets([FrameError], ResAcc) ->
    %% NOTE: If there was a frame parsing error, it always goes last in the list.
    with_channel(handle_in, [FrameError], ResAcc);
handle_incoming_packets([], ResAcc) ->
    ResAcc.

%%--------------------------------------------------------------------
%% With Channel
%%--------------------------------------------------------------------

with_channel(Fun, Args, {FrameAcc, State = #state{channel = Channel, sockstate = running}}) ->
    case erlang:apply(emqx_channel, Fun, Args ++ [Channel]) of
        ok ->
            {FrameAcc, State};
        {ok, NChannel} ->
            {FrameAcc, State#state{channel = NChannel}};
        {ok, Replies, NChannel} ->
            handle_replies(Replies, FrameAcc, State#state{channel = NChannel});
        {continue, Replies, NChannel} ->
            self() ! continue,
            handle_replies(Replies, FrameAcc, State#state{channel = NChannel});
        {shutdown, Reason, NChannel} ->
            order_shutdown(Reason, FrameAcc, State#state{channel = NChannel});
        {shutdown, Reason, Packet, NChannel} ->
            NState = State#state{channel = NChannel},
            Frames = handle_outgoing(Packet, NState),
            order_shutdown(Reason, [Frames | FrameAcc], NState)
    end;
with_channel(_Fun, _Args, {FrameAcc, State = #state{sockstate = _NotRunning}}) ->
    {FrameAcc, State}.

handle_replies([{connack, Packet} | Rest], FrameAcc, State) ->
    Frames = handle_outgoing(Packet, State),
    handle_replies(Rest, [Frames | FrameAcc], State);
handle_replies([{outgoing, Packets} | Rest], FrameAcc, State) ->
    Frames = handle_outgoing(Packets, State),
    handle_replies(Rest, [Frames | FrameAcc], State);
handle_replies([Packet = #mqtt_packet{} | Rest], FrameAcc, State) ->
    Frames = handle_outgoing(Packet, State),
    handle_replies(Rest, [Frames | FrameAcc], State);
handle_replies([{close, Reason} | Rest], FrameAcc, State) ->
    ?TRACE("SOCKET", "socket_force_closed", #{reason => Reason}),
    Frame = handle_close(Reason),
    handle_replies(Rest, [Frame | FrameAcc], State);
handle_replies([Event | Rest], FrameAcc, State) ->
    handle_replies(Rest, FrameAcc, handle_event(Event, State));
handle_replies([], FrameAcc, State) ->
    {FrameAcc, State};
handle_replies(Event, FrameAcc, State) ->
    handle_replies([Event], FrameAcc, State).

order_shutdown(Reason, FrameAcc, State) ->
    {[{shutdown, Reason} | FrameAcc], State#state{sockstate = shutdown}}.

handle_close(#{cause := Cause}) when is_atom(Cause) ->
    {close, iodata(Cause)};
handle_close(Reason) ->
    {close, iodata(Reason)}.

iodata(X) when is_atom(X) ->
    atom_to_binary(X);
iodata(X) when is_list(X) ->
    X;
iodata(X) when is_binary(X) ->
    X.

%%--------------------------------------------------------------------
%% Handle outgoing packets
%%--------------------------------------------------------------------

handle_outgoing(Packets, State = #state{channel = _Channel}) ->
    Frames = do_handle_outgoing(Packets, State),
    _ = ?EXT_TRACE_OUTGOING_STOP(
        emqx_external_trace:basic_attrs(_Channel),
        Packets
    ),
    Frames.

do_handle_outgoing(Packets, State) ->
    Frames = serialize_and_inc_stats(Packets, State),
    case is_list(Packets) of
        true -> Cnt = length(Packets);
        _ -> Cnt = 1
    end,
    ok = inc_sent_stats(Cnt, framelist_bytesize(Frames, 0)),
    _ = trigger_gc_outgoing(),
    Frames.

serialize_and_inc_stats(Packets, #state{serialize = Serialize, mqtt_piggyback = Piggyback}) ->
    try
        serialize_and_inc_stats(Packets, Serialize, Piggyback)
    catch
        %% Maybe Never happen.
        throw:{?FRAME_SERIALIZE_ERROR, Reason} ->
            erlang:error({?FRAME_SERIALIZE_ERROR, Reason});
        error:Reason:Stacktrace ->
            ?LOG(error, #{
                packets => Packets,
                exception => Reason,
                stacktrace => Stacktrace
            }),
            erlang:error(?FRAME_SERIALIZE_ERROR)
    end.

serialize_and_inc_stats(Packet, Serialize, _) when is_tuple(Packet) ->
    {binary, serialize_packet_and_inc_stats(Packet, Serialize)};
serialize_and_inc_stats(Packets, Serialize, single) ->
    {binary, [serialize_packet_and_inc_stats(P, Serialize) || P <- Packets]};
serialize_and_inc_stats(Packets, Serialize, multiple) ->
    [{binary, serialize_packet_and_inc_stats(P, Serialize)} || P <- Packets].

serialize_packet_and_inc_stats(Packet, Serialize) ->
    case emqx_frame:serialize_pkt(Packet, Serialize) of
        <<>> ->
            ?LOG(warning, #{
                msg => "packet_discarded",
                reason => "frame_too_large",
                packet => Packet
            }),
            ok = emqx_metrics:inc('delivery.dropped.too_large'),
            ok = emqx_metrics:inc('delivery.dropped'),
            ok = inc_outgoing_stats({error, message_too_large}),
            <<>>;
        Data ->
            ?TRACE("WS-MQTT", "mqtt_packet_sent", #{packet => Packet}),
            ok = inc_outgoing_stats(Packet),
            Data
    end.

framelist_bytesize([{binary, Data} | Rest], Oct) ->
    framelist_bytesize(Rest, Oct + iolist_size(Data));
framelist_bytesize([], Oct) ->
    Oct;
framelist_bytesize({binary, Data}, Oct) ->
    Oct + iolist_size(Data).

%%--------------------------------------------------------------------
%% Inc incoming/outgoing stats
%%--------------------------------------------------------------------

-compile(
    {inline, [
        inc_recv_stats/2,
        inc_incoming_stats/1,
        inc_outgoing_stats/1,
        inc_sent_stats/2,
        inc_qos_stats/2
    ]}
).

inc_recv_stats(Cnt, Oct) ->
    _ = emqx_pd:inc_counter(recv_cnt, Cnt),
    _ = emqx_pd:inc_counter(recv_oct, Oct),
    emqx_metrics:inc('bytes.received', Oct).

inc_incoming_stats(Packet = ?PACKET(Type)) ->
    _ = emqx_pd:inc_counter(recv_pkt, 1),
    _ =
        case Type of
            ?PUBLISH ->
                _ = emqx_pd:inc_counter(recv_msg, 1),
                inc_qos_stats(recv_msg, Packet);
            _ ->
                ok
        end,
    emqx_metrics:inc_recv(Packet).

inc_outgoing_stats({error, message_too_large}) ->
    _ = emqx_pd:inc_counter('send_msg.dropped', 1),
    _ = emqx_pd:inc_counter('send_msg.dropped.too_large', 1);
inc_outgoing_stats(Packet = ?PACKET(Type)) ->
    _ = emqx_pd:inc_counter(send_pkt, 1),
    _ =
        case Type of
            ?PUBLISH ->
                _ = emqx_pd:inc_counter(send_msg, 1),
                inc_qos_stats(send_msg, Packet);
            _ ->
                ok
        end,
    emqx_metrics:inc_sent(Packet).

inc_sent_stats(Cnt, Oct) ->
    _ = emqx_pd:inc_counter(send_cnt, Cnt),
    _ = emqx_pd:inc_counter(send_oct, Oct),
    emqx_metrics:inc('bytes.sent', Oct).

inc_qos_stats(Type, Packet) ->
    case inc_qos_stats_key(Type, emqx_packet:qos(Packet)) of
        undefined ->
            ignore;
        Key ->
            emqx_pd:inc_counter(Key, 1)
    end.

inc_qos_stats_key(send_msg, ?QOS_0) -> 'send_msg.qos0';
inc_qos_stats_key(send_msg, ?QOS_1) -> 'send_msg.qos1';
inc_qos_stats_key(send_msg, ?QOS_2) -> 'send_msg.qos2';
inc_qos_stats_key(recv_msg, ?QOS_0) -> 'recv_msg.qos0';
inc_qos_stats_key(recv_msg, ?QOS_1) -> 'recv_msg.qos1';
inc_qos_stats_key(recv_msg, ?QOS_2) -> 'recv_msg.qos2';
%% for bad qos
inc_qos_stats_key(_, _) -> undefined.

%%--------------------------------------------------------------------
%% Helper functions
%%--------------------------------------------------------------------

-compile({inline, [cancel_idle_timer/1, ensure_stats_timer/1]}).

%%--------------------------------------------------------------------
%% Cancel idle timer

cancel_idle_timer(State = #state{idle_timer = IdleTimer}) ->
    ok = emqx_utils:cancel_timer(IdleTimer),
    State#state{idle_timer = undefined}.

%%--------------------------------------------------------------------
%% Ensure stats timer

ensure_stats_timer(
    State = #state{
        zone = Zone,
        stats_timer = undefined
    }
) ->
    Timeout = emqx_channel:get_mqtt_conf(Zone, idle_timeout),
    State#state{stats_timer = emqx_utils:start_timer(Timeout, emit_stats)};
ensure_stats_timer(State) ->
    %% Either already active, disabled or paused.
    State.

resume_stats_timer(State = #state{stats_timer = paused}) ->
    State#state{stats_timer = undefined};
resume_stats_timer(State = #state{stats_timer = disabled}) ->
    State.

get_peer(Req, #{listener := {Type, Listener}}) ->
    {PeerAddr, PeerPort} = cowboy_req:peer(Req),
    AddrHeaderName = get_ws_header_opt(Type, Listener, proxy_address_header),
    AddrHeader = cowboy_req:header(AddrHeaderName, Req, <<>>),
    ClientAddr =
        case string:tokens(binary_to_list(AddrHeader), ", ") of
            [] ->
                undefined;
            AddrList ->
                hd(AddrList)
        end,
    Addr =
        case inet:parse_address(ClientAddr) of
            {ok, A} ->
                A;
            _ ->
                PeerAddr
        end,
    PortHeaderName = get_ws_header_opt(Type, Listener, proxy_port_header),
    PortHeader = cowboy_req:header(PortHeaderName, Req, <<>>),
    ClientPort =
        case string:tokens(binary_to_list(PortHeader), ", ") of
            [] ->
                undefined;
            PortList ->
                hd(PortList)
        end,
    try
        {Addr, list_to_integer(ClientPort)}
    catch
        _:_ -> {Addr, PeerPort}
    end.

%%--------------------------------------------------------------------
%% For CT tests
%%--------------------------------------------------------------------

set_field(Name, Value, State) ->
    Pos = emqx_utils:index_of(Name, record_info(fields, state)),
    setelement(Pos + 1, State, Value).

%% ensure lowercase letters in headers
get_ws_header_opt(Type, Listener, Key) ->
    iolist_to_binary(string:lowercase(get_ws_opt(Type, Listener, Key))).

get_ws_opts(Type, Listener) ->
    emqx_config:get_listener_conf(Type, Listener, [websocket]).

get_ws_opt(Type, Listener, Key) ->
    emqx_config:get_listener_conf(Type, Listener, [websocket, Key]).

get_active_n(Type, Listener) ->
    emqx_config:get_listener_conf(Type, Listener, [tcp_options, active_n]).
