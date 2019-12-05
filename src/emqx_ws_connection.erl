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

%% MQTT/WS|WSS Connection
-module(emqx_ws_connection).

-include("emqx.hrl").
-include("emqx_mqtt.hrl").
-include("logger.hrl").
-include("types.hrl").

-logger_header("[MQTT/WS]").

-ifdef(TEST).
-compile(export_all).
-compile(nowarn_export_all).
-endif.

%% API
-export([ info/1
        , stats/1
        ]).

-export([call/2]).

%% WebSocket callbacks
-export([ init/2
        , websocket_init/1
        , websocket_handle/2
        , websocket_info/2
        , websocket_close/2
        , terminate/3
        ]).

-import(emqx_misc,
        [ maybe_apply/2
        , start_timer/2
        ]).

-record(state, {
          %% Peername of the ws connection.
          peername :: emqx_types:peername(),
          %% Sockname of the ws connection
          sockname :: emqx_types:peername(),
          %% Sock state
          sockstate :: emqx_types:sockstate(),
          %% Simulate the active_n opt
          active_n :: pos_integer(),
          %% Limiter
          limiter :: emqx_limiter:limiter(),
          %% Limit Timer
          limit_timer :: maybe(reference()),
          %% Parse State
          parse_state :: emqx_frame:parse_state(),
          %% Serialize function
          serialize :: emqx_frame:serialize_fun(),
          %% Channel
          channel :: emqx_channel:channel(),
          %% GC State
          gc_state :: maybe(emqx_gc:gc_state()),
          %% Out Pending Packets
          pendings :: list(emqx_types:packet()),
          %% Stats Timer
          stats_timer :: disabled | maybe(reference()),
          %% Idle Timeout
          idle_timeout :: timeout(),
          %% Idle Timer
          idle_timer :: reference(),
          %% The stop reason
          stop_reason :: term()
        }).

-type(state() :: #state{}).

-define(ACTIVE_N, 100).
-define(INFO_KEYS, [socktype, peername, sockname, sockstate, active_n, limiter]).
-define(SOCK_STATS, [recv_oct, recv_cnt, send_oct, send_cnt]).
-define(CONN_STATS, [recv_pkt, recv_msg, send_pkt, send_msg]).

-define(ENABLED(X), (X =/= undefined)).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec(info(pid()|state()) -> emqx_types:infos()).
info(WsPid) when is_pid(WsPid) ->
    call(WsPid, info);
info(WsConn = #state{channel = Channel}) ->
    ChanInfo = emqx_channel:info(Channel),
    SockInfo = maps:from_list(info(?INFO_KEYS, WsConn)),
    maps:merge(ChanInfo, #{sockinfo => SockInfo}).

info(Keys, WsConn) when is_list(Keys) ->
    [{Key, info(Key, WsConn)} || Key <- Keys];
info(socktype, _State) ->
    ws;
info(peername, #state{peername = Peername}) ->
    Peername;
info(sockname, #state{sockname = Sockname}) ->
    Sockname;
info(sockstate, #state{sockstate = SockSt}) ->
    SockSt;
info(active_n, #state{active_n = ActiveN}) ->
    ActiveN;
info(limiter, #state{limiter = Limiter}) ->
    maybe_apply(fun emqx_limiter:info/1, Limiter);
info(channel, #state{channel = Channel}) ->
    emqx_channel:info(Channel);
info(stop_reason, #state{stop_reason = Reason}) ->
    Reason.

attrs(State = #state{channel = Channel}) ->
    ChanAttrs = emqx_channel:attrs(Channel),
    SockAttrs = maps:from_list(info(?INFO_KEYS, State)),
    maps:merge(ChanAttrs, #{sockinfo => SockAttrs}).

-spec(stats(pid()|state()) -> emqx_types:stats()).
stats(WsPid) when is_pid(WsPid) ->
    call(WsPid, stats);
stats(#state{channel = Channel}) ->
    SockStats = emqx_pd:get_counters(?SOCK_STATS),
    ConnStats = emqx_pd:get_counters(?CONN_STATS),
    ChanStats = emqx_channel:stats(Channel),
    ProcStats = emqx_misc:proc_stats(),
    lists:append([SockStats, ConnStats, ChanStats, ProcStats]).

%% kick|discard|takeover
-spec(call(pid(), Req :: term()) -> Reply :: term()).
call(WsPid, Req) when is_pid(WsPid) ->
    Mref = erlang:monitor(process, WsPid),
    WsPid ! {call, {self(), Mref}, Req},
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
    %% WS Transport Idle Timeout
    IdleTimeout = proplists:get_value(idle_timeout, Opts, 7200000),
    DeflateOptions = maps:from_list(proplists:get_value(deflate_options, Opts, [])),
    MaxFrameSize = case proplists:get_value(max_frame_size, Opts, 0) of
                       0 -> infinity;
                       I -> I
                   end,
    Compress = proplists:get_value(compress, Opts, false),
    WsOpts = #{compress       => Compress,
               deflate_opts   => DeflateOptions,
               max_frame_size => MaxFrameSize,
               idle_timeout   => IdleTimeout
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
            {ok, cowboy_req:reply(400, Req), WsOpts}
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
                       ?LOG(error, "Failed to parse cookie, Error: ~p, Reason ~p",
                            [Error, Reason]),
                       undefined
               end,
    ConnInfo = #{socktype  => ws,
                 peername  => Peername,
                 sockname  => Sockname,
                 peercert  => Peercert,
                 ws_cookie => WsCookie,
                 conn_mod  => ?MODULE
                },
    Zone = proplists:get_value(zone, Opts),
    ActiveN = proplists:get_value(active_n, Opts, ?ACTIVE_N),
    Limiter = emqx_limiter:init(Opts),
    FrameOpts = emqx_zone:mqtt_frame_options(Zone),
    ParseState = emqx_frame:initial_parse_state(FrameOpts),
    Serialize = emqx_frame:serialize_fun(),
    Channel = emqx_channel:init(ConnInfo, Opts),
    GcState = emqx_zone:init_gc_state(Zone),
    StatsTimer = emqx_zone:stats_timer(Zone),
    %% MQTT Idle Timeout
    IdleTimeout = emqx_zone:idle_timeout(Zone),
    IdleTimer = start_timer(IdleTimeout, idle_timeout),
    emqx_misc:tune_heap_size(emqx_zone:oom_policy(Zone)),
    emqx_logger:set_metadata_peername(esockd:format(Peername)),
    {ok, #state{peername     = Peername,
                sockname     = Sockname,
                sockstate    = running,
                active_n     = ActiveN,
                limiter      = Limiter,
                parse_state  = ParseState,
                serialize    = Serialize,
                channel      = Channel,
                gc_state     = GcState,
                pendings     = [],
                stats_timer  = StatsTimer,
                idle_timeout = IdleTimeout,
                idle_timer   = IdleTimer
               }, hibernate}.

websocket_handle({binary, Data}, State) when is_list(Data) ->
    websocket_handle({binary, iolist_to_binary(Data)}, State);

websocket_handle({binary, Data}, State) ->
    ?LOG(debug, "RECV ~p", [Data]),
    ok = inc_recv_stats(1, iolist_size(Data)),
    parse_incoming(Data, ensure_stats_timer(State));

%% Pings should be replied with pongs, cowboy does it automatically
%% Pongs can be safely ignored. Clause here simply prevents crash.
websocket_handle(Frame, State) when Frame =:= ping; Frame =:= pong ->
    {ok, State};

websocket_handle({FrameType, _}, State) when FrameType =:= ping;
                                             FrameType =:= pong ->
    {ok, State};

websocket_handle({FrameType, _}, State) ->
    ?LOG(error, "Unexpected frame - ~p", [FrameType]),
    stop({shutdown, unexpected_ws_frame}, State).

websocket_info({call, From, Req}, State) ->
    handle_call(From, Req, State);

websocket_info({cast, Msg}, State = #state{channel = Channel}) ->
    handle_chan_return(emqx_channel:handle_info(Msg, Channel), State);

websocket_info({incoming, Packet = ?CONNECT_PACKET(ConnPkt)},
               State = #state{idle_timer = IdleTimer}) ->
    ok = emqx_misc:cancel_timer(IdleTimer),
    Serialize = emqx_frame:serialize_fun(ConnPkt),
    NState = State#state{serialize  = Serialize,
                         idle_timer = undefined
                        },
    handle_incoming(Packet, NState);

websocket_info({incoming, ?PACKET(?PINGREQ)}, State) ->
    reply(?PACKET(?PINGRESP), State);

websocket_info({incoming, Packet}, State) ->
    handle_incoming(Packet, State);

websocket_info(rate_limit, State) ->
    InStats = #{cnt => emqx_pd:reset_counter(incoming_pubs),
                oct => emqx_pd:reset_counter(incoming_bytes)
               },
    erlang:send(self(), {check_gc, InStats}),
    ensure_rate_limit(InStats, State);

websocket_info({check_gc, Stats}, State) ->
    {ok, check_oom(run_gc(Stats, State))};

websocket_info(Deliver = {deliver, _Topic, _Msg},
               State = #state{active_n = ActiveN, channel = Channel}) ->
    Delivers = [Deliver|emqx_misc:drain_deliver(ActiveN)],
    Ret = emqx_channel:handle_deliver(Delivers, Channel),
    handle_chan_return(Ret, State);

websocket_info({timeout, TRef, limit_timeout}, State = #state{limit_timer = TRef}) ->
    NState = State#state{sockstate   = running,
                         limit_timer = undefined
                        },
    {reply, [{active, true}], NState};

websocket_info({timeout, TRef, Msg}, State) when is_reference(TRef) ->
    handle_timeout(TRef, Msg, State);

websocket_info({close, Reason}, State) ->
    stop({shutdown, Reason}, State);

websocket_info({shutdown, Reason}, State) ->
    stop({shutdown, Reason}, State);

websocket_info({stop, Reason}, State) ->
    stop(Reason, State);

websocket_info(Info, State) ->
    handle_info(Info, State).

websocket_close(Reason, State) ->
    ?LOG(debug, "WebSocket closed due to ~p~n", [Reason]),
    handle_info({sock_closed, Reason}, State).

terminate(SockError, _Req, #state{channel = Channel,
                                  stop_reason = Reason}) ->
    ?LOG(debug, "Terminated for ~p, sockerror: ~p", [Reason, SockError]),
    emqx_channel:terminate(Reason, Channel).

%%--------------------------------------------------------------------
%% Handle call

handle_call(From, info, State) ->
    gen_server:reply(From, info(State)),
    {ok, State};

handle_call(From, stats, State) ->
    gen_server:reply(From, stats(State)),
    {ok, State};

handle_call(From, Req, State = #state{channel = Channel}) ->
    case emqx_channel:handle_call(Req, Channel) of
        {reply, Reply, NChannel} ->
            _ = gen_server:reply(From, Reply),
            {ok, State#state{channel = NChannel}};
        {stop, Reason, Reply, NChannel} ->
            _ = gen_server:reply(From, Reply),
            stop(Reason, State#state{channel = NChannel});
        {stop, Reason, Reply, OutPacket, NChannel} ->
            gen_server:reply(From, Reply),
            NState = State#state{channel = NChannel},
            stop(Reason, enqueue(OutPacket, NState))
    end.

%%--------------------------------------------------------------------
%% Handle Info

handle_info({connack, ConnAck}, State = #state{channel = Channel}) ->
    #{clientid := ClientId} = emqx_channel:info(clientinfo, Channel),
    ok = emqx_cm:register_channel(ClientId),
    ok = emqx_cm:set_chan_attrs(ClientId, attrs(State)),
    ok = emqx_cm:set_chan_stats(ClientId, stats(State)),
    reply(enqueue(ConnAck, State));

handle_info({enter, disconnected}, State = #state{channel = Channel}) ->
    #{clientid := ClientId} = emqx_channel:info(clientinfo, Channel),
    emqx_cm:set_chan_attrs(ClientId, attrs(State)),
    emqx_cm:set_chan_stats(ClientId, stats(State)),
    reply(State);

handle_info(Info, State = #state{channel = Channel}) ->
    Ret = emqx_channel:handle_info(Info, Channel),
    handle_chan_return(Ret, State).

%%--------------------------------------------------------------------
%% Handle timeout

handle_timeout(TRef, idle_timeout, State = #state{idle_timer = TRef}) ->
    shutdown(idle_timeout, State);

handle_timeout(TRef, keepalive, State) when is_reference(TRef) ->
    RecvOct = emqx_pd:get_counter(recv_oct),
    handle_timeout(TRef, {keepalive, RecvOct}, State);

handle_timeout(TRef, emit_stats, State = #state{channel = Channel,
                                                stats_timer = TRef}) ->
    #{clientid := ClientId} = emqx_channel:info(clientinfo, Channel),
    (ClientId =/= undefined) andalso emqx_cm:set_chan_stats(ClientId, stats(State)),
    reply(State#state{stats_timer = undefined});

handle_timeout(TRef, TMsg, State = #state{channel = Channel}) ->
    Ret = emqx_channel:handle_timeout(TRef, TMsg, Channel),
    handle_chan_return(Ret, State).

%%--------------------------------------------------------------------
%% Ensure stats timer

-compile({inline, [ensure_stats_timer/1]}).
ensure_stats_timer(State = #state{idle_timeout = Timeout,
                                  stats_timer  = undefined}) ->
    State#state{stats_timer = start_timer(Timeout, emit_stats)};
ensure_stats_timer(State) -> State.

%%--------------------------------------------------------------------
%% Ensure rate limit

ensure_rate_limit(Stats, State = #state{limiter = Limiter}) ->
    case ?ENABLED(Limiter) andalso emqx_limiter:check(Stats, Limiter) of
        false -> {ok, State};
        {ok, Limiter1} ->
            {ok, State#state{limiter = Limiter1}};
        {pause, Time, Limiter1} ->
            ?LOG(debug, "Pause ~pms due to rate limit", [Time]),
            TRef = start_timer(Time, limit_timeout),
            NState = State#state{sockstate   = blocked,
                                 limiter     = Limiter1,
                                 limit_timer = TRef
                                },
            {reply, [{active, false}], NState}
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
%% Parse incoming data

parse_incoming(<<>>, State) ->
    {ok, State};

parse_incoming(Data, State = #state{parse_state = ParseState}) ->
    try emqx_frame:parse(Data, ParseState) of
        {more, NParseState} ->
            {ok, State#state{parse_state = NParseState}};
        {ok, Packet, Rest, NParseState} ->
            erlang:send(self(), {incoming, Packet}),
            parse_incoming(Rest, State#state{parse_state = NParseState})
    catch
        error:Reason:Stk ->
            ?LOG(error, "~nParse failed for ~p~nStacktrace: ~p~nFrame data: ~p",
                 [Reason, Stk, Data]),
            self() ! {incoming, {frame_error, Reason}},
            {ok, State}
    end.

%%--------------------------------------------------------------------
%% Handle incoming packet

handle_incoming(Packet, State = #state{active_n = ActiveN, channel = Channel})
  when is_record(Packet, mqtt_packet) ->
    ?LOG(debug, "RECV ~s", [emqx_packet:format(Packet)]),
    ok = inc_incoming_stats(Packet),
    (emqx_pd:get_counter(incoming_pubs) > ActiveN)
        andalso erlang:send(self(), rate_limit),
    Ret = emqx_channel:handle_in(Packet, Channel),
    handle_chan_return(Ret, State);

handle_incoming(FrameError, State = #state{channel = Channel}) ->
    handle_chan_return(emqx_channel:handle_in(FrameError, Channel), State).

%%--------------------------------------------------------------------
%% Handle channel return

handle_chan_return(ok, State) ->
    reply(State);
handle_chan_return({ok, NChannel}, State) ->
    reply(State#state{channel= NChannel});
handle_chan_return({ok, Replies, NChannel}, State) ->
    reply(Replies, State#state{channel= NChannel});
handle_chan_return({shutdown, Reason, NChannel}, State) ->
    stop(Reason, State#state{channel = NChannel});
handle_chan_return({shutdown, Reason, OutPacket, NChannel}, State) ->
    NState = State#state{channel = NChannel},
    stop(Reason, enqueue(OutPacket, NState)).

%%--------------------------------------------------------------------
%% Handle outgoing packets

handle_outgoing(Packets, State = #state{active_n = ActiveN}) ->
    IoData = lists:map(serialize_and_inc_stats_fun(State), Packets),
    Oct = iolist_size(IoData),
    ok = inc_sent_stats(length(Packets), Oct),
    case emqx_pd:get_counter(outgoing_pubs) > ActiveN of
        true ->
            OutStats = #{cnt => emqx_pd:reset_counter(outgoing_pubs),
                         oct => emqx_pd:reset_counter(outgoing_bytes)
                        },
            erlang:send(self(), {check_gc, OutStats});
        false -> ok
    end,
    {{binary, IoData}, ensure_stats_timer(State)}.

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
%% Inc incoming/outgoing stats

-compile({inline,
          [ inc_recv_stats/2
          , inc_incoming_stats/1
          , inc_outgoing_stats/1
          , inc_sent_stats/2
          ]}).

inc_recv_stats(Cnt, Oct) ->
    emqx_pd:inc_counter(incoming_bytes, Oct),
    emqx_pd:inc_counter(recv_cnt, Cnt),
    emqx_pd:inc_counter(recv_oct, Oct),
    emqx_metrics:inc('bytes.received', Oct).

inc_incoming_stats(Packet = ?PACKET(Type)) ->
    emqx_pd:inc_counter(recv_pkt, 1),
    if Type == ?PUBLISH ->
           emqx_pd:inc_counter(recv_msg, 1),
           emqx_pd:inc_counter(incoming_pubs, 1);
       true -> ok
    end,
    emqx_metrics:inc_recv(Packet).

inc_outgoing_stats(Packet = ?PACKET(Type)) ->
    emqx_pd:inc_counter(send_pkt, 1),
    if Type == ?PUBLISH ->
           emqx_pd:inc_counter(send_msg, 1),
           emqx_pd:inc_counter(outgoing_pubs, 1);
       true -> ok
    end,
    emqx_metrics:inc_sent(Packet).

inc_sent_stats(Cnt, Oct) ->
    emqx_pd:inc_counter(outgoing_bytes, Oct),
    emqx_pd:inc_counter(send_cnt, Cnt),
    emqx_pd:inc_counter(send_oct, Oct),
    emqx_metrics:inc('bytes.sent', Oct).

%%--------------------------------------------------------------------
%% Reply or Stop

reply(Packet, State) when is_record(Packet, mqtt_packet) ->
    reply(enqueue(Packet, State));
reply({outgoing, Packets}, State) ->
    reply(enqueue(Packets, State));
reply(Close = {close, _Reason}, State) ->
    self() ! Close,
    reply(State);

reply([], State) ->
    reply(State);
reply([Packet|More], State) when is_record(Packet, mqtt_packet) ->
    reply(More, enqueue(Packet, State));
reply([{outgoing, Packets}|More], State) ->
    reply(More, enqueue(Packets, State));
reply([Other|More], State) ->
    self() ! Other,
    reply(More, State).

-compile({inline, [reply/1, enqueue/2]}).

reply(State = #state{pendings = []}) ->
    {ok, State};
reply(State = #state{pendings = Pendings}) ->
    {Reply, NState} = handle_outgoing(Pendings, State),
    {reply, Reply, NState#state{pendings = []}}.

enqueue(Packet, State) when is_record(Packet, mqtt_packet) ->
    enqueue([Packet], State);
enqueue(Packets, State = #state{pendings = Pendings}) ->
    State#state{pendings = lists:append(Pendings, Packets)}.

shutdown(Reason, State) ->
    stop({shutdown, Reason}, State).

stop(Reason, State = #state{pendings = []}) ->
    {stop, State#state{stop_reason = Reason}};
stop(Reason, State = #state{pendings = Pendings}) ->
    {Reply, State1} = handle_outgoing(Pendings, State),
    State2 = State1#state{pendings = [], stop_reason = Reason},
    {reply, [Reply, close], State2}.

