%%--------------------------------------------------------------------
%% Copyright (c) 2020 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% Export for CT
-export([set_field/3]).

-import(emqx_misc,
        [ maybe_apply/2
        , start_timer/2
        ]).

-record(state, {
          %% Peername of the ws connection
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
          %% Serialize Fun
          serialize :: emqx_frame:serialize_fun(),
          %% Channel
          channel :: emqx_channel:channel(),
          %% GC State
          gc_state :: maybe(emqx_gc:gc_state()),
          %% Postponed Packets|Cmds|Events
          postponed :: list(emqx_types:packet()|ws_cmd()|tuple()),
          %% Stats Timer
          stats_timer :: disabled | maybe(reference()),
          %% Idle Timeout
          idle_timeout :: timeout(),
          %% Idle Timer
          idle_timer :: reference()
        }).

-type(state() :: #state{}).

-type(ws_cmd() :: {active, boolean()}|close).

-define(ACTIVE_N, 100).
-define(INFO_KEYS, [socktype, peername, sockname, sockstate, active_n]).
-define(SOCK_STATS, [recv_oct, recv_cnt, send_oct, send_cnt]).
-define(CONN_STATS, [recv_pkt, recv_msg, send_pkt, send_msg]).

-define(ENABLED(X), (X =/= undefined)).

%%--------------------------------------------------------------------
%% Info, Stats
%%--------------------------------------------------------------------

-spec(info(pid()|state()) -> emqx_types:infos()).
info(WsPid) when is_pid(WsPid) ->
    call(WsPid, info);
info(State = #state{channel = Channel}) ->
    ChanInfo = emqx_channel:info(Channel),
    SockInfo = maps:from_list(
                 info(?INFO_KEYS, State)),
    ChanInfo#{sockinfo => SockInfo}.

info(Keys, State) when is_list(Keys) ->
    [{Key, info(Key, State)} || Key <- Keys];
info(socktype, _State) -> ws;
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
info(gc_state, #state{gc_state = GcSt}) ->
    maybe_apply(fun emqx_gc:info/1, GcSt);
info(postponed, #state{postponed = Postponed}) ->
    Postponed;
info(stats_timer, #state{stats_timer = TRef}) ->
    TRef;
info(idle_timeout, #state{idle_timeout = Timeout}) ->
    Timeout;
info(idle_timer, #state{idle_timer = TRef}) ->
    TRef.

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
                       ?LOG(error, "Failed to parse cookie, Error: ~0p, Reason ~0p",
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
    PubLimit = emqx_zone:publish_limit(Zone),
    Limiter = emqx_limiter:init([{pub_limit, PubLimit}|Opts]),
    ActiveN = proplists:get_value(active_n, Opts, ?ACTIVE_N),
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
                postponed    = [],
                stats_timer  = StatsTimer,
                idle_timeout = IdleTimeout,
                idle_timer   = IdleTimer
               }, hibernate}.

websocket_handle({binary, Data}, State) when is_list(Data) ->
    websocket_handle({binary, iolist_to_binary(Data)}, State);

websocket_handle({binary, Data}, State) ->
    ?LOG(debug, "RECV ~0p", [Data]),
    ok = inc_recv_stats(1, iolist_size(Data)),
    NState = ensure_stats_timer(State),
    return(parse_incoming(Data, NState));

%% Pings should be replied with pongs, cowboy does it automatically
%% Pongs can be safely ignored. Clause here simply prevents crash.
websocket_handle(Frame, State) when Frame =:= ping; Frame =:= pong ->
    return(State);

websocket_handle({Frame, _}, State) when Frame =:= ping; Frame =:= pong ->
    return(State);

websocket_handle({Frame, _}, State) ->
    %% TODO: should not close the ws connection
    ?LOG(error, "Unexpected frame - ~p", [Frame]),
    shutdown(unexpected_ws_frame, State).

websocket_info({call, From, Req}, State) ->
    handle_call(From, Req, State);

websocket_info({cast, rate_limit}, State) ->
    Stats = #{cnt => emqx_pd:reset_counter(incoming_pubs),
              oct => emqx_pd:reset_counter(incoming_bytes)
             },
    NState = postpone({check_gc, Stats}, State),
    return(ensure_rate_limit(Stats, NState));

websocket_info({cast, Msg}, State) ->
    handle_info(Msg, State);

websocket_info({incoming, Packet = ?CONNECT_PACKET(ConnPkt)}, State) ->
    Serialize = emqx_frame:serialize_fun(ConnPkt),
    NState = State#state{serialize = Serialize},
    handle_incoming(Packet, cancel_idle_timer(NState));

websocket_info({incoming, ?PACKET(?PINGREQ)}, State) ->
    return(enqueue(?PACKET(?PINGRESP), State));

websocket_info({incoming, Packet}, State) ->
    handle_incoming(Packet, State);

websocket_info({outgoing, Packets}, State) ->
    return(enqueue(Packets, State));

websocket_info({check_gc, Stats}, State) ->
    return(check_oom(run_gc(Stats, State)));

websocket_info(Deliver = {deliver, _Topic, _Msg},
               State = #state{active_n = ActiveN}) ->
    Delivers = [Deliver|emqx_misc:drain_deliver(ActiveN)],
    with_channel(handle_deliver, [Delivers], State);

websocket_info({timeout, TRef, limit_timeout},
               State = #state{limit_timer = TRef}) ->
    NState = State#state{sockstate   = running,
                         limit_timer = undefined
                        },
    return(enqueue({active, true}, NState));

websocket_info({timeout, TRef, Msg}, State) when is_reference(TRef) ->
    handle_timeout(TRef, Msg, State);

websocket_info({shutdown, Reason}, State) ->
    shutdown(Reason, State);

websocket_info({stop, Reason}, State) ->
    shutdown(Reason, State);

websocket_info(Info, State) ->
    handle_info(Info, State).

websocket_close({_, ReasonCode, _Payload}, State) when is_integer(ReasonCode) ->
    websocket_close(ReasonCode, State);
websocket_close(Reason, State) ->
    ?LOG(debug, "Websocket closed due to ~p~n", [Reason]),
    handle_info({sock_closed, Reason}, State).

terminate(Reason, _Req, #state{channel = Channel}) ->
    ?LOG(debug, "Terminated due to ~p", [Reason]),
    emqx_channel:terminate(Reason, Channel).

%%--------------------------------------------------------------------
%% Handle call
%%--------------------------------------------------------------------

handle_call(From, info, State) ->
    gen_server:reply(From, info(State)),
    return(State);

handle_call(From, stats, State) ->
    gen_server:reply(From, stats(State)),
    return(State);

handle_call(From, Req, State = #state{channel = Channel}) ->
    case emqx_channel:handle_call(Req, Channel) of
        {reply, Reply, NChannel} ->
            gen_server:reply(From, Reply),
            return(State#state{channel = NChannel});
        {shutdown, Reason, Reply, NChannel} ->
            gen_server:reply(From, Reply),
            shutdown(Reason, State#state{channel = NChannel});
        {shutdown, Reason, Reply, Packet, NChannel} ->
            gen_server:reply(From, Reply),
            NState = State#state{channel = NChannel},
            shutdown(Reason, enqueue(Packet, NState))
    end.

%%--------------------------------------------------------------------
%% Handle Info
%%--------------------------------------------------------------------

handle_info({connack, ConnAck}, State) ->
    return(enqueue(ConnAck, State));

handle_info({close, Reason}, State) ->
    ?LOG(debug, "Force to close the socket due to ~p", [Reason]),
    return(enqueue({close, Reason}, State));

handle_info({event, connected}, State = #state{channel = Channel}) ->
    ClientId = emqx_channel:info(clientid, Channel),
    ok = emqx_cm:register_channel(ClientId, info(State), stats(State)),
    return(State);

handle_info({event, disconnected}, State = #state{channel = Channel}) ->
    ClientId = emqx_channel:info(clientid, Channel),
    emqx_cm:set_chan_info(ClientId, info(State)),
    emqx_cm:connection_closed(ClientId),
    return(State);

handle_info({event, _Other}, State = #state{channel = Channel}) ->
    ClientId = emqx_channel:info(clientid, Channel),
    emqx_cm:set_chan_info(ClientId, info(State)),
    emqx_cm:set_chan_stats(ClientId, stats(State)),
    return(State);

handle_info(Info, State) ->
    with_channel(handle_info, [Info], State).

%%--------------------------------------------------------------------
%% Handle timeout
%%--------------------------------------------------------------------

handle_timeout(TRef, idle_timeout, State = #state{idle_timer = TRef}) ->
    shutdown(idle_timeout, State);

handle_timeout(TRef, keepalive, State) when is_reference(TRef) ->
    RecvOct = emqx_pd:get_counter(recv_oct),
    handle_timeout(TRef, {keepalive, RecvOct}, State);

handle_timeout(TRef, emit_stats, State = #state{channel = Channel,
                                                stats_timer = TRef}) ->
    ClientId = emqx_channel:info(clientid, Channel),
    emqx_cm:set_chan_stats(ClientId, stats(State)),
    return(State#state{stats_timer = undefined});

handle_timeout(TRef, TMsg, State) ->
    with_channel(handle_timeout, [TRef, TMsg], State).

%%--------------------------------------------------------------------
%% Ensure rate limit
%%--------------------------------------------------------------------

ensure_rate_limit(Stats, State = #state{limiter = Limiter}) ->
    case ?ENABLED(Limiter) andalso emqx_limiter:check(Stats, Limiter) of
        false -> State;
        {ok, Limiter1} ->
            State#state{limiter = Limiter1};
        {pause, Time, Limiter1} ->
            ?LOG(warning, "Pause ~pms due to rate limit", [Time]),
            TRef = start_timer(Time, limit_timeout),
            NState = State#state{sockstate   = blocked,
                                 limiter     = Limiter1,
                                 limit_timer = TRef
                                },
            enqueue({active, false}, NState)
    end.

%%--------------------------------------------------------------------
%% Run GC, Check OOM
%%--------------------------------------------------------------------

run_gc(Stats, State = #state{gc_state = GcSt}) ->
    case ?ENABLED(GcSt) andalso emqx_gc:run(Stats, GcSt) of
        false -> State;
        {_IsGC, GcSt1} ->
            State#state{gc_state = GcSt1}
    end.

check_oom(State = #state{channel = Channel}) ->
    OomPolicy = emqx_zone:oom_policy(emqx_channel:info(zone, Channel)),
    case ?ENABLED(OomPolicy) andalso emqx_misc:check_oom(OomPolicy) of
        Shutdown = {shutdown, _Reason} ->
            postpone(Shutdown, State);
        _Other -> State
    end.

%%--------------------------------------------------------------------
%% Parse incoming data
%%--------------------------------------------------------------------

parse_incoming(<<>>, State) ->
    State;

parse_incoming(Data, State = #state{parse_state = ParseState}) ->
    try emqx_frame:parse(Data, ParseState) of
        {more, NParseState} ->
            State#state{parse_state = NParseState};
        {ok, Packet, Rest, NParseState} ->
            NState = State#state{parse_state = NParseState},
            parse_incoming(Rest, postpone({incoming, Packet}, NState))
    catch
        error:Reason:Stk ->
            ?LOG(error, "~nParse failed for ~0p~n~0p~nFrame data: ~0p",
                 [Reason, Stk, Data]),
            FrameError = {frame_error, Reason},
            postpone({incoming, FrameError}, State)
    end.

%%--------------------------------------------------------------------
%% Handle incoming packet
%%--------------------------------------------------------------------

handle_incoming(Packet, State = #state{active_n = ActiveN})
  when is_record(Packet, mqtt_packet) ->
    ?LOG(debug, "RECV ~s", [emqx_packet:format(Packet)]),
    ok = inc_incoming_stats(Packet),
    NState = case emqx_pd:get_counter(incoming_pubs) > ActiveN of
                 true  -> postpone({cast, rate_limit}, State);
                 false -> State
             end,
    with_channel(handle_in, [Packet], NState);

handle_incoming(FrameError, State) ->
    with_channel(handle_in, [FrameError], State).

%%--------------------------------------------------------------------
%% With Channel
%%--------------------------------------------------------------------

with_channel(Fun, Args, State = #state{channel = Channel}) ->
    case erlang:apply(emqx_channel, Fun, Args ++ [Channel]) of
        ok -> return(State);
        {ok, NChannel} ->
            return(State#state{channel = NChannel});
        {ok, Replies, NChannel} ->
            return(postpone(Replies, State#state{channel= NChannel}));
        {shutdown, Reason, NChannel} ->
            shutdown(Reason, State#state{channel = NChannel});
        {shutdown, Reason, Packet, NChannel} ->
            NState = State#state{channel = NChannel},
            shutdown(Reason, postpone(Packet, NState))
    end.

%%--------------------------------------------------------------------
%% Handle outgoing packets
%%--------------------------------------------------------------------

handle_outgoing(Packets, State = #state{active_n = ActiveN}) ->
    IoData = lists:map(serialize_and_inc_stats_fun(State), Packets),
    Oct = iolist_size(IoData),
    ok = inc_sent_stats(length(Packets), Oct),
    NState = case emqx_pd:get_counter(outgoing_pubs) > ActiveN of
                 true ->
                     Stats = #{cnt => emqx_pd:reset_counter(outgoing_pubs),
                               oct => emqx_pd:reset_counter(outgoing_bytes)
                              },
                     postpone({check_gc, Stats}, State);
                 false -> State
             end,
    {{binary, IoData}, ensure_stats_timer(NState)}.

serialize_and_inc_stats_fun(#state{serialize = Serialize}) ->
    fun(Packet) ->
        case Serialize(Packet) of
            <<>> -> ?LOG(warning, "~s is discarded due to the frame is too large.",
                         [emqx_packet:format(Packet)]),
                    ok = emqx_metrics:inc('delivery.dropped.too_large'),
                    ok = emqx_metrics:inc('delivery.dropped'),
                    <<>>;
            Data -> ?LOG(debug, "SEND ~s", [emqx_packet:format(Packet)]),
                    ok = inc_outgoing_stats(Packet),
                    Data
        end
    end.

%%--------------------------------------------------------------------
%% Inc incoming/outgoing stats
%%--------------------------------------------------------------------

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
%% Helper functions
%%--------------------------------------------------------------------

-compile({inline, [cancel_idle_timer/1, ensure_stats_timer/1]}).

%%--------------------------------------------------------------------
%% Cancel idle timer

cancel_idle_timer(State = #state{idle_timer = IdleTimer}) ->
    ok = emqx_misc:cancel_timer(IdleTimer),
    State#state{idle_timer = undefined}.

%%--------------------------------------------------------------------
%% Ensure stats timer

ensure_stats_timer(State = #state{idle_timeout = Timeout,
                                  stats_timer  = undefined}) ->
    State#state{stats_timer = start_timer(Timeout, emit_stats)};
ensure_stats_timer(State) -> State.

-compile({inline, [postpone/2, enqueue/2, return/1, shutdown/2]}).

%%--------------------------------------------------------------------
%% Postpone the packet, cmd or event

postpone(Packet, State) when is_record(Packet, mqtt_packet) ->
    enqueue(Packet, State);
postpone(Event, State) when is_tuple(Event) ->
    enqueue(Event, State);
postpone(More, State) when is_list(More) ->
    lists:foldl(fun postpone/2, State, More).

enqueue([Packet], State = #state{postponed = Postponed}) ->
    State#state{postponed = [Packet|Postponed]};
enqueue(Packets, State = #state{postponed = Postponed})
  when is_list(Packets) ->
    State#state{postponed = lists:reverse(Packets) ++ Postponed};
enqueue(Other, State = #state{postponed = Postponed}) ->
    State#state{postponed = [Other|Postponed]}.

shutdown(Reason, State = #state{postponed = Postponed}) ->
    return(State#state{postponed = [{shutdown, Reason}|Postponed]}).

return(State = #state{postponed = []}) ->
    {ok, State};
return(State = #state{postponed = Postponed}) ->
    {Packets, Cmds, Events} = classify(Postponed, [], [], []),
    ok = lists:foreach(fun trigger/1, Events),
    State1 = State#state{postponed = []},
    case {Packets, Cmds} of
        {[], []}   -> {ok, State1};
        {[], Cmds} -> {Cmds, State1};
        {Packets, Cmds} ->
            {Frame, State2} = handle_outgoing(Packets, State1),
            {[Frame|Cmds], State2}
    end.

classify([], Packets, Cmds, Events) ->
    {Packets, Cmds, Events};
classify([Packet|More], Packets, Cmds, Events)
  when is_record(Packet, mqtt_packet) ->
    classify(More, [Packet|Packets], Cmds, Events);
classify([Cmd = {active, _}|More], Packets, Cmds, Events) ->
    classify(More, Packets, [Cmd|Cmds], Events);
classify([Cmd = {shutdown, _Reason}|More], Packets, Cmds, Events) ->
    classify(More, Packets, [Cmd|Cmds], Events);
classify([Cmd = close|More], Packets, Cmds, Events) ->
    classify(More, Packets, [Cmd|Cmds], Events);
classify([Cmd = {close, _Reason}|More], Packets, Cmds, Events) ->
    classify(More, Packets, [Cmd|Cmds], Events);
classify([Event|More], Packets, Cmds, Events) ->
    classify(More, Packets, Cmds, [Event|Events]).

trigger(Event) -> erlang:send(self(), Event).

%%--------------------------------------------------------------------
%% For CT tests
%%--------------------------------------------------------------------

set_field(Name, Value, State) ->
    Pos = emqx_misc:index_of(Name, record_info(fields, state)),
    setelement(Pos+1, State, Value).

