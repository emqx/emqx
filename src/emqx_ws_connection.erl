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

%% MQTT WebSocket Connection
-module(emqx_ws_connection).

-include("emqx.hrl").
-include("emqx_mqtt.hrl").
-include("logger.hrl").
-include("types.hrl").

-logger_header("[WsConnection]").

-export([ info/1
        , attrs/1
        , stats/1
        , state/1
        ]).

-export([call/2]).

%% WebSocket callbacks
-export([ init/2
        , websocket_init/1
        , websocket_handle/2
        , websocket_info/2
        , terminate/3
        ]).

-record(ws_connection, {
          %% Peername of the ws connection.
          peername :: emqx_types:peername(),
          %% Sockname of the ws connection
          sockname :: emqx_types:peername(),
          %% FSM state
          fsm_state :: idle | connected | disconnected,
          %% Parser State
          parse_state :: emqx_frame:parse_state(),
          %% Serialize function
          serialize :: fun((emqx_types:packet()) -> iodata()),
          %% Channel State
          chan_state :: emqx_channel:channel(),
          %% Out Pending Packets
          pendings :: list(emqx_types:packet()),
          %% The stop reason
          stop_reason :: term()
        }).

-type(ws_connection() :: #ws_connection{}).

-define(INFO_KEYS, [socktype, peername, sockname, active_state]).
-define(ATTR_KEYS, [socktype, peername, sockname]).
-define(SOCK_STATS, [recv_oct, recv_cnt, send_oct, send_cnt]).
-define(CONN_STATS, [recv_pkt, recv_msg, send_pkt, send_msg]).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec(info(pid()|ws_connection()) -> emqx_types:infos()).
info(WsPid) when is_pid(WsPid) ->
    call(WsPid, info);
info(WsConn = #ws_connection{chan_state = ChanState}) ->
    ConnInfo = info(?INFO_KEYS, WsConn),
    ChanInfo = emqx_channel:info(ChanState),
    maps:merge(ChanInfo, #{connection => maps:from_list(ConnInfo)}).

info(Keys, WsConn) when is_list(Keys) ->
    [{Key, info(Key, WsConn)} || Key <- Keys];
info(socktype, #ws_connection{}) ->
    websocket;
info(peername, #ws_connection{peername = Peername}) ->
    Peername;
info(sockname, #ws_connection{sockname = Sockname}) ->
    Sockname;
info(active_state, #ws_connection{}) ->
    running;
info(chan_state, #ws_connection{chan_state = ChanState}) ->
    emqx_channel:info(ChanState).

-spec(attrs(pid()|ws_connection()) -> emqx_types:attrs()).
attrs(WsPid) when is_pid(WsPid) ->
    call(WsPid, attrs);
attrs(WsConn = #ws_connection{chan_state = ChanState}) ->
    ConnAttrs = info(?ATTR_KEYS, WsConn),
    ChanAttrs = emqx_channel:attrs(ChanState),
    maps:merge(ChanAttrs, #{conninfo => maps:from_list(ConnAttrs)}).

-spec(stats(pid()|ws_connection()) -> emqx_types:stats()).
stats(WsPid) when is_pid(WsPid) ->
    call(WsPid, stats);
stats(#ws_connection{chan_state = ChanState}) ->
    ProcStats = emqx_misc:proc_stats(),
    SockStats = wsock_stats(),
    ConnStats = conn_stats(),
    ChanStats = emqx_channel:stats(ChanState),
    lists:append([ProcStats, SockStats, ConnStats, ChanStats]).

wsock_stats() ->
    [{Key, emqx_pd:get_counter(Key)} || Key <- ?SOCK_STATS].

conn_stats() ->
    [{Name, emqx_pd:get_counter(Name)} || Name <- ?CONN_STATS].

-spec(state(pid()) -> ws_connection()).
state(WsPid) -> call(WsPid, state).

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
    ChanState = emqx_channel:init(#{peername  => Peername,
                                    sockname  => Sockname,
                                    peercert  => Peercert,
                                    ws_cookie => WsCookie,
                                    conn_mod  => ?MODULE
                                   }, Opts),
    Zone = proplists:get_value(zone, Opts),
    MaxSize = emqx_zone:get_env(Zone, max_packet_size, ?MAX_PACKET_SIZE),
    ParseState = emqx_frame:initial_parse_state(#{max_size => MaxSize}),
    emqx_logger:set_metadata_peername(esockd_net:format(Peername)),
    {ok, #ws_connection{peername    = Peername,
                        sockname    = Sockname,
                        fsm_state   = idle,
                        parse_state = ParseState,
                        chan_state  = ChanState,
                        pendings    = [],
                        serialize   = serialize_fun(?MQTT_PROTO_V5, undefined)}}.

websocket_handle({binary, Data}, State) when is_list(Data) ->
    websocket_handle({binary, iolist_to_binary(Data)}, State);

websocket_handle({binary, Data}, State = #ws_connection{chan_state = ChanState}) ->
    ?LOG(debug, "RECV ~p", [Data]),
    Oct = iolist_size(Data),
    ok = inc_recv_stats(1, Oct),
    NChanState = emqx_channel:received(Oct, ChanState),
    NState = State#ws_connection{chan_state = NChanState},
    process_incoming(Data, NState);

%% Pings should be replied with pongs, cowboy does it automatically
%% Pongs can be safely ignored. Clause here simply prevents crash.
websocket_handle(Frame, State)
  when Frame =:= ping; Frame =:= pong ->
    {ok, State};

websocket_handle({FrameType, _}, State)
  when FrameType =:= ping; FrameType =:= pong ->
    {ok, State};

websocket_handle({FrameType, _}, State) ->
    ?LOG(error, "Unexpected frame - ~p", [FrameType]),
    stop({shutdown, unexpected_ws_frame}, State).

websocket_info({call, From, info}, State) ->
    gen_server:reply(From, info(State)),
    {ok, State};

websocket_info({call, From, attrs}, State) ->
    gen_server:reply(From, attrs(State)),
    {ok, State};

websocket_info({call, From, stats}, State) ->
    gen_server:reply(From, stats(State)),
    {ok, State};

websocket_info({call, From, state}, State) ->
    gen_server:reply(From, State),
    {ok, State};

websocket_info({call, From, Req}, State = #ws_connection{chan_state = ChanState}) ->
    case emqx_channel:handle_call(Req, ChanState) of
        {ok, Reply, NChanState} ->
            _ = gen_server:reply(From, Reply),
            {ok, State#ws_connection{chan_state = NChanState}};
        {stop, Reason, Reply, NChanState} ->
            _ = gen_server:reply(From, Reply),
            stop(Reason, State#ws_connection{chan_state = NChanState})
    end;

websocket_info({cast, Msg}, State = #ws_connection{chan_state = ChanState}) ->
    case emqx_channel:handle_cast(Msg, ChanState) of
        {ok, NChanState} ->
            {ok, State#ws_connection{chan_state = NChanState}};
        {stop, Reason, NChanState} ->
            stop(Reason, State#ws_connection{chan_state = NChanState})
    end;

websocket_info({incoming, Packet = ?CONNECT_PACKET(ConnPkt)},
                State = #ws_connection{fsm_state = idle}) ->
    #mqtt_packet_connect{proto_ver = ProtoVer, properties = Properties} = ConnPkt,
    MaxPacketSize = emqx_mqtt_props:get_property('Maximum-Packet-Size', Properties, undefined),
    NState = State#ws_connection{serialize = serialize_fun(ProtoVer, MaxPacketSize)},
    handle_incoming(Packet, fun connected/1, NState);

websocket_info({incoming, Packet}, State = #ws_connection{fsm_state = idle}) ->
    ?LOG(warning, "Unexpected incoming: ~p", [Packet]),
    stop(unexpected_incoming_packet, State);

websocket_info({incoming, Packet}, State = #ws_connection{fsm_state = connected})
  when is_record(Packet, mqtt_packet) ->
    handle_incoming(Packet, fun reply/1, State);

websocket_info(Deliver = {deliver, _Topic, _Msg},
               State = #ws_connection{chan_state = ChanState}) ->
    Delivers = emqx_misc:drain_deliver([Deliver]),
    case emqx_channel:handle_out({deliver, Delivers}, ChanState) of
        {ok, NChanState} ->
            reply(State#ws_connection{chan_state = NChanState});
        {ok, Packets, NChanState} ->
            reply(enqueue(Packets, State#ws_connection{chan_state = NChanState}));
        {stop, Reason, NChanState} ->
            stop(Reason, State#ws_connection{chan_state = NChanState})
    end;

websocket_info({timeout, TRef, keepalive}, State) when is_reference(TRef) ->
    RecvOct = emqx_pd:get_counter(recv_oct),
    handle_timeout(TRef, {keepalive, RecvOct}, State);

websocket_info({timeout, TRef, emit_stats}, State) when is_reference(TRef) ->
    handle_timeout(TRef, {emit_stats, stats(State)}, State);

websocket_info({timeout, TRef, Msg}, State) when is_reference(TRef) ->
    handle_timeout(TRef, Msg, State);

websocket_info({shutdown, Reason}, State) ->
    stop({shutdown, Reason}, State);

websocket_info({stop, Reason}, State) ->
    stop(Reason, State);

websocket_info(Info, State = #ws_connection{chan_state = ChanState}) ->
    case emqx_channel:handle_info(Info, ChanState) of
        {ok, NChanState} ->
            {ok, State#ws_connection{chan_state = NChanState}};
        {stop, Reason, NChanState} ->
            stop(Reason, State#ws_connection{chan_state = NChanState})
    end.

terminate(SockError, _Req, #ws_connection{chan_state  = ChanState,
                                          stop_reason = Reason}) ->
    ?LOG(debug, "Terminated for ~p, sockerror: ~p",
         [Reason, SockError]),
    emqx_channel:terminate(Reason, ChanState).

%%--------------------------------------------------------------------
%% Connected callback

connected(State = #ws_connection{chan_state = ChanState}) ->
    ok = emqx_channel:handle_cast({register, attrs(State), stats(State)}, ChanState),
    reply(State#ws_connection{fsm_state = connected}).

%%--------------------------------------------------------------------
%% Handle timeout

handle_timeout(TRef, Msg, State = #ws_connection{chan_state = ChanState}) ->
    case emqx_channel:timeout(TRef, Msg, ChanState) of
        {ok, NChanState} ->
            {ok, State#ws_connection{chan_state = NChanState}};
        {ok, Packets, NChanState} ->
            NState = State#ws_connection{chan_state = NChanState},
            reply(enqueue(Packets, NState));
        {stop, Reason, NChanState} ->
            stop(Reason, State#ws_connection{chan_state = NChanState})
    end.

%%--------------------------------------------------------------------
%% Process incoming data

process_incoming(<<>>, State) ->
    {ok, State};

process_incoming(Data, State = #ws_connection{parse_state = ParseState,
                                              chan_state  = ChanState}) ->
    try emqx_frame:parse(Data, ParseState) of
        {more, NParseState} ->
            {ok, State#ws_connection{parse_state = NParseState}};
        {ok, Packet, Rest, NParseState} ->
            self() ! {incoming, Packet},
            process_incoming(Rest, State#ws_connection{parse_state = NParseState});
        {error, Reason} ->
            ?LOG(error, "Frame error: ~p", [Reason]),
            stop(Reason, State)
    catch
        error:Reason:Stk ->
            ?LOG(error, "Parse failed for ~p~nStacktrace:~p~nFrame data: ~p", [Reason, Stk, Data]),
            Result = 
                case emqx_channel:info(connected, ChanState) of
                    undefined ->
                        emqx_channel:handle_out({connack, emqx_reason_codes:mqtt_frame_error(Reason)}, ChanState);
                    true ->
                        emqx_channel:handle_out({disconnect, emqx_reason_codes:mqtt_frame_error(Reason)}, ChanState);
                    _ ->
                        ignore
                end,
            case Result of
                {stop, Reason0, OutPackets, NChanState} ->
                    NState = State#ws_connection{chan_state = NChanState},
                    stop(Reason0, enqueue(OutPackets, NState));
                {stop, Reason0, NChanState} ->
                    stop(Reason0, State#ws_connection{chan_state = NChanState});
                ignore ->
                    {ok, State}
            end
    end.

%%--------------------------------------------------------------------
%% Handle incoming packets

handle_incoming(Packet = ?PACKET(Type), SuccFun,
                State = #ws_connection{chan_state = ChanState}) ->
    _ = inc_incoming_stats(Type),
    ok = emqx_metrics:inc_recv(Packet),
    ?LOG(debug, "RECV ~s", [emqx_packet:format(Packet)]),
    case emqx_channel:handle_in(Packet, ChanState) of
        {ok, NChanState} ->
            SuccFun(State#ws_connection{chan_state= NChanState});
        {ok, OutPackets, NChanState} ->
            NState = State#ws_connection{chan_state= NChanState},
            SuccFun(enqueue(OutPackets, NState));
        {stop, Reason, NChanState} ->
            stop(Reason, State#ws_connection{chan_state= NChanState});
        {stop, Reason, OutPacket, NChanState} ->
            NState = State#ws_connection{chan_state= NChanState},
            stop(Reason, enqueue(OutPacket, NState))
    end.

%%--------------------------------------------------------------------
%% Handle outgoing packets

handle_outgoing(Packets, State = #ws_connection{serialize  = Serialize,
                                                chan_state = ChanState}) ->
    Data = lists:map(Serialize, Packets),
    Oct = iolist_size(Data),
    ok = inc_sent_stats(length(Packets), Oct),
    NChanState = emqx_channel:sent(Oct, ChanState),
    {{binary, Data}, State#ws_connection{chan_state = NChanState}}.

%%--------------------------------------------------------------------
%% Serialize fun

serialize_fun(ProtoVer, MaxPacketSize) ->
    fun(Packet = ?PACKET(Type)) ->
        IoData = emqx_frame:serialize(Packet, ProtoVer),
        case Type =/= ?PUBLISH orelse MaxPacketSize =:= undefined orelse iolist_size(IoData) =< MaxPacketSize of
            true ->
                ?LOG(debug, "SEND ~s", [emqx_packet:format(Packet)]),
                _ = inc_outgoing_stats(Type),
                _ = emqx_metrics:inc_sent(Packet),
                IoData;
            false ->
                ?LOG(warning, "DROP ~s due to oversize packet size", [emqx_packet:format(Packet)]),
                <<"">>
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
    emqx_pd:update_counter(recv_cnt, Cnt),
    emqx_pd:update_counter(recv_oct, Oct),
    emqx_metrics:inc('bytes.received', Oct).

inc_incoming_stats(Type) ->
    emqx_pd:update_counter(recv_pkt, 1),
    (Type == ?PUBLISH)
        andalso emqx_pd:update_counter(recv_msg, 1).

inc_outgoing_stats(Type) ->
    emqx_pd:update_counter(send_pkt, 1),
    (Type == ?PUBLISH)
        andalso emqx_pd:update_counter(send_msg, 1).

inc_sent_stats(Cnt, Oct) ->
    emqx_pd:update_counter(send_cnt, Cnt),
    emqx_pd:update_counter(send_oct, Oct),
    emqx_metrics:inc('bytes.sent', Oct).

%%--------------------------------------------------------------------
%% Reply or Stop

-compile({inline, [reply/1]}).

reply(State = #ws_connection{pendings = []}) ->
    {ok, State};
reply(State = #ws_connection{pendings = Pendings}) ->
    {Reply, NState} = handle_outgoing(Pendings, State),
    {reply, Reply, NState#ws_connection{pendings = []}}.

stop(Reason, State = #ws_connection{pendings = []}) ->
    {stop, State#ws_connection{stop_reason = Reason}};
stop(Reason, State = #ws_connection{pendings = Pendings}) ->
    {Reply, State1} = handle_outgoing(Pendings, State),
    State2 = State1#ws_connection{pendings = [], stop_reason = Reason},
    {reply, [Reply, close], State2}.

enqueue(Packet, State) when is_record(Packet, mqtt_packet) ->
    enqueue([Packet], State);
enqueue(Packets, State = #ws_connection{pendings = Pendings}) ->
    State#ws_connection{pendings = lists:append(Pendings, Packets)}.

