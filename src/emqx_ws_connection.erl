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
        ]).

-export([ kick/1
        , discard/1
        , takeover/2
        ]).

%% WebSocket callbacks
-export([ init/2
        , websocket_init/1
        , websocket_handle/2
        , websocket_info/2
        , terminate/3
        ]).

-record(state, {
          peername    :: emqx_types:peername(),
          sockname    :: emqx_types:peername(),
          fsm_state   :: idle | connected | disconnected,
          serialize   :: fun((emqx_types:packet()) -> iodata()),
          parse_state :: emqx_frame:parse_state(),
          chan_state  :: emqx_channel:channel(),
          pendings    :: list(),
          stop_reason :: term()
        }).

-type(state() :: #state{}).

-define(SOCK_STATS, [recv_oct, recv_cnt, send_oct, send_cnt]).
-define(CONN_STATS, [recv_pkt, recv_msg, send_pkt, send_msg]).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec(info(pid() | state()) -> emqx_types:infos()).
info(WSPid) when is_pid(WSPid) ->
    call(WSPid, info);
info(#state{peername   = Peername,
            sockname   = Sockname,
            chan_state = ChanState}) ->
    ConnInfo = #{socktype   => websocket,
                 peername   => Peername,
                 sockname   => Sockname,
                 conn_state => running
                },
    ChanInfo = emqx_channel:info(ChanState),
    maps:merge(ConnInfo, ChanInfo).

-spec(attrs(pid() | state()) -> emqx_types:attrs()).
attrs(WSPid) when is_pid(WSPid) ->
    call(WSPid, attrs);
attrs(#state{peername   = Peername,
             sockname   = Sockname,
             chan_state = ChanState}) ->
    ConnAttrs = #{socktype => websocket,
                  peername => Peername,
                  sockname => Sockname
                 },
    ChanAttrs = emqx_channel:attrs(ChanState),
    maps:merge(ConnAttrs, ChanAttrs).

-spec(stats(pid() | state()) -> emqx_types:stats()).
stats(WSPid) when is_pid(WSPid) ->
    call(WSPid, stats);
stats(#state{chan_state = ChanState}) ->
    ProcStats = emqx_misc:proc_stats(),
    ChanStats = emqx_channel:stats(ChanState),
    lists:append([ProcStats, wsock_stats(), conn_stats(), ChanStats]).

wsock_stats() ->
    [{Key, emqx_pd:get_counter(Key)} || Key <- ?SOCK_STATS].

conn_stats() ->
    [{Name, emqx_pd:get_counter(Name)} || Name <- ?CONN_STATS].

-spec(kick(pid()) -> ok).
kick(CPid) ->
    call(CPid, kick).

-spec(discard(pid()) -> ok).
discard(WSPid) ->
    WSPid ! {cast, discard}, ok.

-spec(takeover(pid(), 'begin'|'end') -> Result :: term()).
takeover(CPid, Phase) ->
    call(CPid, {takeover, Phase}).

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
    ChanState = emqx_channel:init(#{peername => Peername,
                                    sockname => Sockname,
                                    peercert => Peercert,
                                    ws_cookie => WsCookie,
                                    conn_mod => ?MODULE
                                   }, Opts),
    Zone = proplists:get_value(zone, Opts),
    MaxSize = emqx_zone:get_env(Zone, max_packet_size, ?MAX_PACKET_SIZE),
    ParseState = emqx_frame:initial_parse_state(#{max_size => MaxSize}),
    emqx_logger:set_metadata_peername(esockd_net:format(Peername)),
    {ok, #state{peername    = Peername,
                sockname    = Sockname,
                fsm_state   = idle,
                parse_state = ParseState,
                chan_state  = ChanState,
                pendings    = []
               }}.

websocket_handle({binary, Data}, State) when is_list(Data) ->
    websocket_handle({binary, iolist_to_binary(Data)}, State);

websocket_handle({binary, Data}, State = #state{chan_state = ChanState})
  when is_binary(Data) ->
    ?LOG(debug, "RECV ~p", [Data]),
    Oct = iolist_size(Data),
    emqx_pd:update_counter(recv_cnt, 1),
    emqx_pd:update_counter(recv_oct, Oct),
    ok = emqx_metrics:inc('bytes.received', Oct),
    NChanState = emqx_channel:ensure_timer(
                   stats_timer, emqx_channel:gc(1, Oct, ChanState)),
    process_incoming(Data, State#state{chan_state = NChanState});

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
    stop(kicked, State);

websocket_info({call, From, Req}, State = #state{chan_state = ChanState}) ->
    case emqx_channel:handle_call(Req, ChanState) of
        {ok, Reply, NChanState} ->
            _ = gen_server:reply(From, Reply),
            {ok, State#state{chan_state = NChanState}};
        {stop, Reason, Reply, NChanState} ->
            _ = gen_server:reply(From, Reply),
            stop(Reason, State#state{chan_state = NChanState})
    end;

websocket_info({cast, Msg}, State = #state{chan_state = ChanState}) ->
    case emqx_channel:handle_cast(Msg, ChanState) of
        {ok, NChanState} ->
            {ok, State#state{chan_state = NChanState}};
        {stop, Reason, NChanState} ->
            stop(Reason, State#state{chan_state = NChanState})
    end;

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
               State = #state{chan_state = ChanState}) ->
    Delivers = emqx_misc:drain_deliver([Deliver]),
    case emqx_channel:handle_out({deliver, Delivers}, ChanState) of
        {ok, NChanState} ->
            reply(State#state{chan_state = NChanState});
        {ok, Packets, NChanState} ->
            reply(enqueue(Packets, State#state{chan_state = NChanState}));
        {stop, Reason, NChanState} ->
            stop(Reason, State#state{chan_state = NChanState})
    end;

websocket_info({timeout, TRef, keepalive}, State) when is_reference(TRef) ->
    RecvOct = emqx_pd:get_counter(recv_oct),
    handle_timeout(TRef, {keepalive, RecvOct}, State);

websocket_info({timeout, TRef, emit_stats}, State) when is_reference(TRef) ->
    handle_timeout(TRef, {emit_stats, stats(State)}, State);

websocket_info({timeout, TRef, Msg}, State) when is_reference(TRef) ->
    handle_timeout(TRef, Msg, State);

websocket_info({shutdown, discard, {ClientId, ByPid}}, State) ->
    ?LOG(warning, "Discarded by ~s:~p", [ClientId, ByPid]),
    stop(discard, State);

websocket_info({shutdown, conflict, {ClientId, NewPid}}, State) ->
    ?LOG(warning, "Clientid '~s' conflict with ~p", [ClientId, NewPid]),
    stop(conflict, State);

websocket_info({shutdown, Reason}, State) ->
    stop(Reason, State);

websocket_info({stop, Reason}, State) ->
    stop(Reason, State);

websocket_info(Info, State = #state{chan_state = ChanState}) ->
    case emqx_channel:handle_info(Info, ChanState) of
        {ok, NChanState} ->
            {ok, State#state{chan_state = NChanState}};
        {stop, Reason, NChanState} ->
            stop(Reason, State#state{chan_state = NChanState})
    end.

terminate(SockError, _Req, #state{chan_state  = ChanState,
                                  stop_reason = Reason}) ->
    ?LOG(debug, "Terminated for ~p, sockerror: ~p",
         [Reason, SockError]),
    emqx_channel:terminate(Reason, ChanState).

%%--------------------------------------------------------------------
%% Connected callback

connected(State = #state{chan_state = ChanState}) ->
    NState = State#state{fsm_state = connected},
    #{client_id := ClientId} = emqx_channel:info(client, ChanState),
    ok = emqx_cm:register_channel(ClientId),
    ok = emqx_cm:set_chan_attrs(ClientId, attrs(NState)),
    reply(NState).

%%--------------------------------------------------------------------
%% Handle timeout

handle_timeout(TRef, Msg, State = #state{chan_state = ChanState}) ->
    case emqx_channel:timeout(TRef, Msg, ChanState) of
        {ok, NChanState} ->
            {ok, State#state{chan_state = NChanState}};
        {ok, Packets, NChanState} ->
            reply(enqueue(Packets, State#state{chan_state = NChanState}));
        {stop, Reason, NChanState} ->
            stop(Reason, State#state{chan_state = NChanState})
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

handle_incoming(Packet = ?PACKET(Type), SuccFun, State = #state{chan_state = ChanState}) ->
    _ = inc_incoming_stats(Type),
    ok = emqx_metrics:inc_recv(Packet),
    ?LOG(debug, "RECV ~s", [emqx_packet:format(Packet)]),
    case emqx_channel:handle_in(Packet, ChanState) of
        {ok, NChanState} ->
            SuccFun(State#state{chan_state= NChanState});
        {ok, OutPackets, NChanState} ->
            SuccFun(enqueue(OutPackets, State#state{chan_state= NChanState}));
        {stop, Reason, NChanState} ->
            stop(Reason, State#state{chan_state= NChanState});
        {stop, Reason, OutPacket, NChanState} ->
            stop(Reason, enqueue(OutPacket, State#state{chan_state= NChanState}))
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
reply(State = #state{chan_state = ChanState, pendings = Pendings}) ->
    Reply = handle_outgoing(Pendings, State),
    NChanState = emqx_channel:ensure_timer(stats_timer, ChanState),
    {reply, Reply, State#state{chan_state = NChanState, pendings = []}}.

stop(Reason, State = #state{pendings = []}) ->
    {stop, State#state{stop_reason = Reason}};
stop(Reason, State = #state{pendings = Pendings}) ->
    Reply = handle_outgoing(Pendings, State),
    {reply, [Reply, close],
     State#state{pendings = [], stop_reason = Reason}}.

enqueue(Packet, State) when is_record(Packet, mqtt_packet) ->
    enqueue([Packet], State);
enqueue(Packets, State = #state{pendings = Pendings}) ->
    State#state{pendings = lists:append(Pendings, Packets)}.

