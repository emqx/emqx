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

%% MQTT WebSocket Channel
-module(emqx_ws_channel).

-include("emqx.hrl").
-include("emqx_mqtt.hrl").
-include("logger.hrl").
-include("types.hrl").

-logger_header("[WsChannel]").

-export([ info/1
        , attrs/1
        , stats/1
        ]).

%% WebSocket callbacks
-export([ init/2
        , websocket_init/1
        , websocket_handle/2
        , websocket_info/2
        , terminate/3
        ]).

-record(state, {
          peername     :: emqx_types:peername(),
          sockname     :: emqx_types:peername(),
          fsm_state    :: idle | connected | disconnected,
          serialize    :: fun((emqx_types:packet()) -> iodata()),
          parse_state  :: emqx_frame:parse_state(),
          proto_state  :: emqx_protocol:proto_state(),
          gc_state     :: emqx_gc:gc_state(),
          keepalive    :: maybe(emqx_keepalive:keepalive()),
          pendings     :: list(),
          stats_timer  :: disabled | maybe(reference()),
          idle_timeout :: timeout(),
          connected    :: boolean(),
          connected_at :: erlang:timestamp(),
          reason       :: term()
        }).

-type(state() :: #state{}).

-define(SOCK_STATS, [recv_oct, recv_cnt, send_oct, send_cnt]).
-define(CHAN_STATS, [recv_pkt, recv_msg, send_pkt, send_msg]).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec(info(pid() | state()) -> emqx_types:infos()).
info(WSPid) when is_pid(WSPid) ->
    call(WSPid, info);
info(#state{peername = Peername,
            sockname = Sockname,
            proto_state = ProtoState,
            gc_state = GCState,
            stats_timer = StatsTimer,
            idle_timeout = IdleTimeout,
            connected = Connected,
            connected_at = ConnectedAt}) ->
    ChanInfo = #{socktype => websocket,
                 peername => Peername,
                 sockname => Sockname,
                 conn_state => running,
                 gc_state => emqx_gc:info(GCState),
                 enable_stats => enable_stats(StatsTimer),
                 idle_timeout => IdleTimeout,
                 connected => Connected,
                 connected_at => ConnectedAt
                },
    maps:merge(ChanInfo, emqx_protocol:info(ProtoState)).

enable_stats(disabled)  -> false;
enable_stats(_MaybeRef) -> true.

-spec(attrs(pid() | state()) -> emqx_types:attrs()).
attrs(WSPid) when is_pid(WSPid) ->
    call(WSPid, attrs);
attrs(#state{peername = Peername,
             sockname = Sockname,
             proto_state = ProtoState,
             connected = Connected,
             connected_at = ConnectedAt}) ->
    ConnAttrs = #{socktype => websocket,
                  peername => Peername,
                  sockname => Sockname,
                  connected => Connected,
                  connected_at => ConnectedAt
                 },
    maps:merge(ConnAttrs, emqx_protocol:attrs(ProtoState)).

-spec(stats(pid() | state()) -> emqx_types:stats()).
stats(WSPid) when is_pid(WSPid) ->
    call(WSPid, stats);
stats(#state{proto_state = ProtoState}) ->
    ProcStats = emqx_misc:proc_stats(),
    SessStats = emqx_session:stats(emqx_protocol:info(session, ProtoState)),
    lists:append([ProcStats, SessStats, chan_stats(), wsock_stats()]).

%% @private
call(WSPid, Req) when is_pid(WSPid) ->
    Mref = erlang:monitor(process, WSPid),
    WSPid ! {call, {self(), Mref}, Req},
    receive
        {Mref, Reply} ->
            erlang:demonitor(Mref, [flush]),
            Reply;
        {'DOWN', Mref, _, _, Reason} ->
            exit(Reason)
    after 5000 ->
        erlang:demonitor(Mref, [flush]),
        exit(timeout)
    end.

%%--------------------------------------------------------------------
%% WebSocket callbacks
%%--------------------------------------------------------------------

init(Req, Opts) ->
    IdleTimeout = proplists:get_value(idle_timeout, Opts, 7200000),
    DeflateOptions = maps:from_list(proplists:get_value(deflate_options, Opts, [])),
    MaxFrameSize = case proplists:get_value(max_frame_size, Opts, 0) of
                       0 -> infinity;
                       I -> I
                   end,
    Compress = proplists:get_value(compress, Opts, false),
    WsOpts = #{compress => Compress,
               deflate_opts => DeflateOptions,
               max_frame_size => MaxFrameSize,
               idle_timeout => IdleTimeout
              },
    case cowboy_req:parse_header(<<"sec-websocket-protocol">>, Req) of
        undefined ->
            %% TODO: why not reply 500???
            {cowboy_websocket, Req, [Req, Opts], WsOpts};
        [<<"mqtt", Vsn/binary>>] ->
            Resp = cowboy_req:set_resp_header(
                     <<"sec-websocket-protocol">>, <<"mqtt", Vsn/binary>>, Req),
            {cowboy_websocket, Resp, [Req, Opts], WsOpts};
        _ ->
            {ok, cowboy_req:reply(400, Req), #state{}}
    end.

websocket_init([Req, Opts]) ->
    Peername = cowboy_req:peer(Req),
    Sockname = cowboy_req:sock(Req),
    Peercert = cowboy_req:cert(Req),
    WsCookie = try cowboy_req:parse_cookies(Req)
               catch
                   error:badarg ->
                       ?LOG(error, "Illegal cookie"),
                       undefined;
                   Error:Reason ->
                       ?LOG(error, "Cookie is parsed failed, Error: ~p, Reason ~p",
                            [Error, Reason]),
                       undefined
               end,
    ProtoState = emqx_protocol:init(#{peername => Peername,
                                      sockname => Sockname,
                                      peercert => Peercert,
                                      ws_cookie => WsCookie,
                                      conn_mod => ?MODULE}, Opts),
    Zone = proplists:get_value(zone, Opts),
    MaxSize = emqx_zone:get_env(Zone, max_packet_size, ?MAX_PACKET_SIZE),
    ParseState = emqx_frame:initial_parse_state(#{max_size => MaxSize}),
    GcPolicy = emqx_zone:get_env(Zone, force_gc_policy, false),
    GcState = emqx_gc:init(GcPolicy),
    EnableStats = emqx_zone:get_env(Zone, enable_stats, true),
    StatsTimer = if EnableStats -> undefined; ?Otherwise-> disabled end,
    IdleTimout = emqx_zone:get_env(Zone, idle_timeout, 30000),
    emqx_logger:set_metadata_peername(esockd_net:format(Peername)),
    ok = emqx_misc:init_proc_mng_policy(Zone),
    {ok, #state{peername     = Peername,
                sockname     = Sockname,
                fsm_state    = idle,
                parse_state  = ParseState,
                proto_state  = ProtoState,
                gc_state     = GcState,
                pendings     = [],
                stats_timer  = StatsTimer,
                idle_timeout = IdleTimout,
                connected    = false
               }}.

stat_fun() ->
    fun() -> {ok, emqx_pd:get_counter(recv_oct)} end.

websocket_handle({binary, Data}, State) when is_list(Data) ->
    websocket_handle({binary, iolist_to_binary(Data)}, State);

websocket_handle({binary, Data}, State) when is_binary(Data) ->
    ?LOG(debug, "RECV ~p", [Data]),
    Oct = iolist_size(Data),
    emqx_pd:update_counter(recv_cnt, 1),
    emqx_pd:update_counter(recv_oct, Oct),
    ok = emqx_metrics:inc('bytes.received', Oct),
    NState = maybe_gc(1, Oct, State),
    process_incoming(Data, ensure_stats_timer(NState));

%% Pings should be replied with pongs, cowboy does it automatically
%% Pongs can be safely ignored. Clause here simply prevents crash.
websocket_handle(Frame, State)
  when Frame =:= ping; Frame =:= pong ->
    {ok, State};
websocket_handle({FrameType, _}, State)
  when FrameType =:= ping; FrameType =:= pong ->
    {ok, State};
%% According to mqtt spec[https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901285]
websocket_handle({FrameType, _}, State) ->
    ?LOG(error, "Frame error: unexpected frame - ~p", [FrameType]),
    stop(unexpected_ws_frame, State).

websocket_info({call, From, info}, State) ->
    gen_server:reply(From, info(State)),
    {ok, State};

websocket_info({call, From, attrs}, State) ->
    gen_server:reply(From, attrs(State)),
    {ok, State};

websocket_info({call, From, stats}, State) ->
    gen_server:reply(From, stats(State)),
    {ok, State};

websocket_info({call, From, kick}, State) ->
    gen_server:reply(From, ok),
    stop(kick, State);

websocket_info({incoming, Packet = ?CONNECT_PACKET(
                                      #mqtt_packet_connect{
                                         proto_ver = ProtoVer}
                                     )},
               State = #state{fsm_state = idle}) ->
    handle_incoming(Packet, fun connected/1,
                    State#state{serialize = serialize_fun(ProtoVer)});

websocket_info({incoming, Packet}, State = #state{fsm_state = idle}) ->
    ?LOG(warning, "Unexpected incoming: ~p", [Packet]),
    stop(unexpected_incoming_packet, State);

websocket_info({incoming, Packet = ?PACKET(?CONNECT)},
               State = #state{fsm_state = connected}) ->
    ?LOG(warning, "Unexpected connect: ~p", [Packet]),
    stop(unexpected_incoming_connect, State);

websocket_info({incoming, Packet}, State = #state{fsm_state = connected})
  when is_record(Packet, mqtt_packet) ->
    handle_incoming(Packet, fun reply/1, State);

websocket_info(Deliver = {deliver, _Topic, _Msg},
               State = #state{proto_state = ProtoState}) ->
    Delivers = emqx_misc:drain_deliver([Deliver]),
    case emqx_protocol:handle_deliver(Delivers, ProtoState) of
        {ok, NProtoState} ->
            reply(State#state{proto_state = NProtoState});
        {ok, Packets, NProtoState} ->
            reply(enqueue(Packets, State#state{proto_state = NProtoState}));
        {error, Reason} ->
            stop(Reason, State);
        {error, Reason, NProtoState} ->
            stop(Reason, State#state{proto_state = NProtoState})
    end;

websocket_info({keepalive, check}, State = #state{keepalive = KeepAlive}) ->
    case emqx_keepalive:check(KeepAlive) of
        {ok, KeepAlive1} ->
            {ok, State#state{keepalive = KeepAlive1}};
        {error, timeout} ->
            stop(keepalive_timeout, State);
        {error, Error} ->
            ?LOG(error, "Keepalive error: ~p", [Error]),
            stop(keepalive_error, State)
    end;

websocket_info({timeout, Timer, emit_stats},
       State = #state{stats_timer = Timer,
                      proto_state = ProtoState,
                      gc_state    = GcState}) ->
    ClientId = emqx_protocol:info(client_id, ProtoState),
    ok = emqx_cm:set_chan_stats(ClientId, stats(State)),
    NState = State#state{stats_timer = undefined},
    Limits = erlang:get(force_shutdown_policy),
    case emqx_misc:conn_proc_mng_policy(Limits) of
        continue ->
            {ok, NState};
        hibernate ->
            %% going to hibernate, reset gc stats
            GcState1 = emqx_gc:reset(GcState),
            {ok, NState#state{gc_state = GcState1}, hibernate};
        {shutdown, Reason} ->
            ?LOG(error, "Shutdown exceptionally due to ~p", [Reason]),
            stop(Reason, NState)
    end;

websocket_info({timeout, Timer, Msg},
               State = #state{proto_state = ProtoState}) ->
    case emqx_protocol:handle_timeout(Timer, Msg, ProtoState) of
        {ok, NProtoState} ->
            {ok, State#state{proto_state = NProtoState}};
        {ok, Packets, NProtoState} ->
            reply(enqueue(Packets, State#state{proto_state = NProtoState}));
        {error, Reason} ->
            stop(Reason, State);
        {error, Reason, NProtoState} ->
            stop(Reason, State#state{proto_state = NProtoState})
    end;

websocket_info({subscribe, TopicFilters}, State) ->
    handle_request({subscribe, TopicFilters}, State);

websocket_info({unsubscribe, TopicFilters}, State) ->
    handle_request({unsubscribe, TopicFilters}, State);

websocket_info({shutdown, discard, {ClientId, ByPid}}, State) ->
    ?LOG(warning, "Discarded by ~s:~p", [ClientId, ByPid]),
    stop(discard, State);

websocket_info({shutdown, conflict, {ClientId, NewPid}}, State) ->
    ?LOG(warning, "Clientid '~s' conflict with ~p", [ClientId, NewPid]),
    stop(conflict, State);

%% websocket_info({binary, Data}, State) ->
%%    {reply, {binary, Data}, State};

websocket_info({shutdown, Reason}, State) ->
    stop(Reason, State);

websocket_info({stop, Reason}, State) ->
    stop(Reason, State);

websocket_info(Info, State) ->
    ?LOG(error, "Unexpected info: ~p", [Info]),
    {ok, State}.

terminate(SockError, _Req, #state{keepalive   = Keepalive,
                                  proto_state = ProtoState,
                                  reason      = Reason}) ->
    ?LOG(debug, "Terminated for ~p, sockerror: ~p",
         [Reason, SockError]),
    emqx_keepalive:cancel(Keepalive),
    emqx_protocol:terminate(Reason, ProtoState).

%%--------------------------------------------------------------------
%% Connected callback

connected(State = #state{proto_state = ProtoState}) ->
    NState = State#state{fsm_state = connected,
                         connected = true,
                         connected_at = os:timestamp()
                        },
    ClientId = emqx_protocol:info(client_id, ProtoState),
    ok = emqx_cm:register_channel(ClientId),
    ok = emqx_cm:set_chan_attrs(ClientId, info(NState)),
    %% Ensure keepalive after connected successfully.
    Interval = emqx_protocol:info(keepalive, ProtoState),
    case ensure_keepalive(Interval, NState) of
        ignore -> reply(NState);
        {ok, KeepAlive} ->
            reply(NState#state{keepalive = KeepAlive});
        {error, Reason} ->
            stop(Reason, NState)
    end.

%%--------------------------------------------------------------------
%% Ensure keepalive

ensure_keepalive(0, _State) ->
    ignore;
ensure_keepalive(Interval, #state{proto_state = ProtoState}) ->
    Backoff = emqx_zone:get_env(emqx_protocol:info(zone, ProtoState),
                                keepalive_backoff, 0.75),
    emqx_keepalive:start(stat_fun(), round(Interval * Backoff), {keepalive, check}).

%%--------------------------------------------------------------------
%% Handle internal request

handle_request(Req, State = #state{proto_state = ProtoState}) ->
    case emqx_protocol:handle_req(Req, ProtoState) of
        {ok, _Result, NProtoState} -> %% TODO:: how to handle the result?
            {ok, State#state{proto_state = NProtoState}};
        {error, Reason, NProtoState} ->
            stop(Reason, State#state{proto_state = NProtoState})
    end.

%%--------------------------------------------------------------------
%% Process incoming data

process_incoming(<<>>, State) ->
    {ok, State};

process_incoming(Data, State = #state{parse_state = ParseState}) ->
    try emqx_frame:parse(Data, ParseState) of
        {ok, NParseState} ->
            {ok, State#state{parse_state = NParseState}};
        {ok, Packet, Rest, NParseState} ->
            self() ! {incoming, Packet},
            process_incoming(Rest, State#state{parse_state = NParseState});
        {error, Reason} ->
            ?LOG(error, "Frame error: ~p", [Reason]),
            stop(Reason, State)
    catch
        error:Reason:Stk ->
            ?LOG(error, "Parse failed for ~p~n\
                 Stacktrace:~p~nFrame data: ~p", [Reason, Stk, Data]),
            stop(parse_error, State)
    end.

%%--------------------------------------------------------------------
%% Handle incoming packets

handle_incoming(Packet = ?PACKET(Type), SuccFun,
                State = #state{proto_state = ProtoState}) ->
    _ = inc_incoming_stats(Type),
    ok = emqx_metrics:inc_recv(Packet),
    ?LOG(debug, "RECV ~s", [emqx_packet:format(Packet)]),
    case emqx_protocol:handle_in(Packet, ProtoState) of
        {ok, NProtoState} ->
            SuccFun(State#state{proto_state = NProtoState});
        {ok, OutPackets, NProtoState} ->
            SuccFun(enqueue(OutPackets, State#state{proto_state = NProtoState}));
        {error, Reason, NProtoState} ->
            stop(Reason, State#state{proto_state = NProtoState});
        {error, Reason, OutPacket, NProtoState} ->
            stop(Reason, enqueue(OutPacket, State#state{proto_state = NProtoState}));
        {stop, Error, NProtoState} ->
            stop(Error, State#state{proto_state = NProtoState})
    end.

%%--------------------------------------------------------------------
%% Handle outgoing packets

handle_outgoing(Packets, #state{serialize = Serialize}) ->
    Data = lists:map(Serialize, Packets),
    emqx_pd:update_counter(send_oct, iolist_size(Data)),
    {binary, Data}.

%%--------------------------------------------------------------------
%% Serialize fun

serialize_fun(ProtoVer) ->
    fun(Packet = ?PACKET(Type)) ->
        ?LOG(debug, "SEND ~s", [emqx_packet:format(Packet)]),
        _ = inc_outgoing_stats(Type),
        emqx_frame:serialize(Packet, ProtoVer)
    end.

%%--------------------------------------------------------------------
%% Inc incoming/outgoing stats

inc_incoming_stats(Type) ->
    emqx_pd:update_counter(recv_pkt, 1),
    (Type == ?PUBLISH)
        andalso emqx_pd:update_counter(recv_msg, 1).

inc_outgoing_stats(Type) ->
    emqx_pd:update_counter(send_cnt, 1),
    emqx_pd:update_counter(send_pkt, 1),
    (Type == ?PUBLISH)
        andalso emqx_pd:update_counter(send_msg, 1).

%%--------------------------------------------------------------------
%% Reply or Stop

reply(State = #state{pendings = []}) ->
    {ok, State};
reply(State = #state{pendings = Pendings}) ->
    Reply = handle_outgoing(Pendings, State),
    {reply, Reply, State#state{pendings = []}}.

stop(Reason, State = #state{pendings = []}) ->
    {stop, State#state{reason = Reason}};
stop(Reason, State = #state{pendings = Pendings}) ->
    Reply = handle_outgoing(Pendings, State),
    {reply, [Reply, close],
     State#state{pendings = [], reason = Reason}}.

enqueue(Packet, State) when is_record(Packet, mqtt_packet) ->
    enqueue([Packet], State);
enqueue(Packets, State = #state{pendings = Pendings}) ->
    State#state{pendings = lists:append(Pendings, Packets)}.

%%--------------------------------------------------------------------
%% Ensure stats timer

ensure_stats_timer(State = #state{stats_timer = undefined,
                                  idle_timeout = IdleTimeout}) ->
    TRef = emqx_misc:start_timer(IdleTimeout, emit_stats),
    State#state{stats_timer = TRef};
%% disabled or timer existed
ensure_stats_timer(State) -> State.

wsock_stats() ->
    [{Key, emqx_pd:get_counter(Key)} || Key <- ?SOCK_STATS].

chan_stats() ->
    [{Name, emqx_pd:get_counter(Name)} || Name <- ?CHAN_STATS].

%%--------------------------------------------------------------------
%% Maybe GC

maybe_gc(_Cnt, _Oct, State = #state{gc_state = undefined}) ->
    State;
maybe_gc(Cnt, Oct, State = #state{gc_state = GCSt}) ->
    {Ok, GCSt1} = emqx_gc:run(Cnt, Oct, GCSt),
    Ok andalso emqx_metrics:inc('channel.gc.cnt'),
    State#state{gc_state = GCSt1}.

