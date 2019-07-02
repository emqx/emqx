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

-logger_header("[WS Conn]").

-export([ info/1
        , attrs/1
        , stats/1
        ]).

%% websocket callbacks
-export([ init/2
        , websocket_init/1
        , websocket_handle/2
        , websocket_info/2
        , terminate/3
        ]).

-record(state, {
          request,
          options,
          peername     :: {inet:ip_address(), inet:port_number()},
          sockname     :: {inet:ip_address(), inet:port_number()},
          parse_state  :: emqx_frame:parse_state(),
          packets      :: list(emqx_mqtt:packet()),
          chan_state   :: emqx_channel:channel(),
          keepalive    :: maybe(emqx_keepalive:keepalive()),
          stats_timer  :: disabled | maybe(reference()),
          idle_timeout :: timeout(),
          shutdown
         }).

-define(SOCK_STATS, [recv_oct, recv_cnt, send_oct, send_cnt]).
-define(CHAN_STATS, [recv_pkt, recv_msg, send_pkt, send_msg]).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

%% for debug
info(WSPid) when is_pid(WSPid) ->
    call(WSPid, info);

info(#state{peername = Peername,
            sockname = Sockname,
            chan_state = ChanState}) ->
    ConnInfo = #{socktype => websocket,
                 conn_state => running,
                 peername => Peername,
                 sockname => Sockname
                },
    ChanInfo = emqx_channel:info(ChanState),
    maps:merge(ConnInfo, ChanInfo).

%% for dashboard
attrs(WSPid) when is_pid(WSPid) ->
    call(WSPid, attrs);

attrs(#state{peername = Peername,
             sockname = Sockname,
             chan_state = ChanState}) ->
    SockAttrs = #{peername => Peername,
                  sockname => Sockname},
    ChanAttrs = emqx_channel:attrs(ChanState),
    maps:merge(SockAttrs, ChanAttrs).

stats(WSPid) when is_pid(WSPid) ->
    call(WSPid, stats);

stats(#state{}) ->
    lists:append([chan_stats(), wsock_stats(), emqx_misc:proc_stats()]).

%%kick(WSPid) when is_pid(WSPid) ->
%%    call(WSPid, kick).

%%session(WSPid) when is_pid(WSPid) ->
%%    call(WSPid, session).

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
                       MFS -> MFS
                   end,
    Compress = proplists:get_value(compress, Opts, false),
    Options = #{compress => Compress,
                deflate_opts => DeflateOptions,
                max_frame_size => MaxFrameSize,
                idle_timeout => IdleTimeout},
    case cowboy_req:parse_header(<<"sec-websocket-protocol">>, Req) of
        undefined ->
            {cowboy_websocket, Req, #state{}, Options};
        [<<"mqtt", Vsn/binary>>] ->
            Resp = cowboy_req:set_resp_header(<<"sec-websocket-protocol">>, <<"mqtt", Vsn/binary>>, Req),
            {cowboy_websocket, Resp, #state{request = Req, options = Opts}, Options};
        _ ->
            {ok, cowboy_req:reply(400, Req), #state{}}
    end.

websocket_init(#state{request = Req, options = Options}) ->
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
                                    conn_mod => ?MODULE}, Options),
    Zone = proplists:get_value(zone, Options),
    MaxSize = emqx_zone:get_env(Zone, max_packet_size, ?MAX_PACKET_SIZE),
    ParseState = emqx_frame:initial_parse_state(#{max_size => MaxSize}),
    EnableStats = emqx_zone:get_env(Zone, enable_stats, true),
    StatsTimer = if EnableStats -> undefined; ?Otherwise-> disabled end,
    IdleTimout = emqx_zone:get_env(Zone, idle_timeout, 30000),
    emqx_logger:set_metadata_peername(esockd_net:format(Peername)),
    ok = emqx_misc:init_proc_mng_policy(Zone),
    {ok, #state{peername     = Peername,
                sockname     = Sockname,
                parse_state  = ParseState,
                chan_state   = ChanState,
                stats_timer  = StatsTimer,
                idle_timeout = IdleTimout
               }}.

send_fun(WsPid) ->
    fun(Packet, Options) ->
        Data = emqx_frame:serialize(Packet, Options),
        BinSize = iolist_size(Data),
        emqx_pd:update_counter(send_cnt, 1),
        emqx_pd:update_counter(send_oct, BinSize),
        WsPid ! {binary, iolist_to_binary(Data)},
        {ok, Data}
    end.

stat_fun() ->
    fun() -> {ok, emqx_pd:get_counter(recv_oct)} end.

websocket_handle({binary, <<>>}, State) ->
    {ok, ensure_stats_timer(State)};
websocket_handle({binary, [<<>>]}, State) ->
    {ok, ensure_stats_timer(State)};
websocket_handle({binary, Data}, State = #state{parse_state = ParseState}) ->
    ?LOG(debug, "RECV ~p", [Data]),
    BinSize = iolist_size(Data),
    emqx_pd:update_counter(recv_oct, BinSize),
    ok = emqx_metrics:inc('bytes.received', BinSize),
    try emqx_frame:parse(iolist_to_binary(Data), ParseState) of
        {ok, NParseState} ->
            {ok, State#state{parse_state = NParseState}};
        {ok, Packet, Rest, NParseState} ->
            ok = emqx_metrics:inc_recv(Packet),
            emqx_pd:update_counter(recv_cnt, 1),
            handle_incoming(Packet, fun(NState) ->
                                            websocket_handle({binary, Rest}, NState)
                                    end,
                            State#state{parse_state = NParseState});
        {error, Reason} ->
            ?LOG(error, "Frame error: ~p", [Reason]),
            shutdown(Reason, State)
    catch
        error:Reason:Stk ->
            ?LOG(error, "Parse failed for ~p~n\
                 Stacktrace:~p~nFrame data: ~p", [Reason, Stk, Data]),
            shutdown(parse_error, State)
    end;
%% Pings should be replied with pongs, cowboy does it automatically
%% Pongs can be safely ignored. Clause here simply prevents crash.
websocket_handle(Frame, State)
  when Frame =:= ping; Frame =:= pong ->
    {ok, ensure_stats_timer(State)};
websocket_handle({FrameType, _}, State)
  when FrameType =:= ping; FrameType =:= pong ->
    {ok, ensure_stats_timer(State)};
%% According to mqtt spec[https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901285]
websocket_handle({_OtherFrameType, _}, State) ->
    ?LOG(error, "Frame error: Other type of data frame"),
    shutdown(other_frame_type, State).

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
    shutdown(kick, State);

websocket_info(Delivery, State = #state{chan_state = ChanState})
  when element(1, Delivery) =:= deliver ->
    case emqx_channel:handle_out(Delivery, ChanState) of
        {ok, NChanState} ->
            {ok, State#state{chan_state = NChanState}};
        {ok, Packet, NChanState} ->
            handle_outgoing(Packet, State#state{chan_state = NChanState});
        {error, Reason} ->
            shutdown(Reason, State)
    end;

websocket_info({timeout, Timer, emit_stats},
               State = #state{stats_timer = Timer, chan_state = ChanState}) ->
    ClientId = emqx_channel:client_id(ChanState),
    ok = emqx_cm:set_conn_stats(ClientId, stats(State)),
    {ok, State#state{stats_timer = undefined}, hibernate};

websocket_info({keepalive, start, Interval}, State) ->
    ?LOG(debug, "Keepalive at the interval of ~p", [Interval]),
    case emqx_keepalive:start(stat_fun(), Interval, {keepalive, check}) of
        {ok, KeepAlive} ->
            {ok, State#state{keepalive = KeepAlive}};
        {error, Error} ->
            ?LOG(warning, "Keepalive error: ~p", [Error]),
            shutdown(Error, State)
    end;

websocket_info({keepalive, check}, State = #state{keepalive = KeepAlive}) ->
    case emqx_keepalive:check(KeepAlive) of
        {ok, KeepAlive1} ->
            {ok, State#state{keepalive = KeepAlive1}};
        {error, timeout} ->
            ?LOG(debug, "Keepalive Timeout!"),
            shutdown(keepalive_timeout, State);
        {error, Error} ->
            ?LOG(error, "Keepalive error: ~p", [Error]),
            shutdown(keepalive_error, State)
    end;

websocket_info({shutdown, discard, {ClientId, ByPid}}, State) ->
    ?LOG(warning, "Discarded by ~s:~p", [ClientId, ByPid]),
    shutdown(discard, State);

websocket_info({shutdown, conflict, {ClientId, NewPid}}, State) ->
    ?LOG(warning, "Clientid '~s' conflict with ~p", [ClientId, NewPid]),
    shutdown(conflict, State);

%% websocket_info({binary, Data}, State) ->
%%    {reply, {binary, Data}, State};

websocket_info({shutdown, Reason}, State) ->
    shutdown(Reason, State);

websocket_info({stop, Reason}, State) ->
    {stop, State#state{shutdown = Reason}};

websocket_info(Info, State) ->
    ?LOG(error, "Unexpected info: ~p", [Info]),
    {ok, State}.

terminate(SockError, _Req, #state{keepalive  = Keepalive,
                                  chan_state = ChanState,
                                  shutdown   = Shutdown}) ->
    ?LOG(debug, "Terminated for ~p, sockerror: ~p",
         [Shutdown, SockError]),
    emqx_keepalive:cancel(Keepalive),
    case {ChanState, Shutdown} of
        {undefined, _} -> ok;
        {_, {shutdown, Reason}} ->
            emqx_channel:terminate(Reason, ChanState);
        {_, Error} ->
            emqx_channel:terminate(Error, ChanState)
    end.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

handle_incoming(Packet = ?PACKET(Type), SuccFun,
                State = #state{chan_state = ChanState}) ->
    _ = inc_incoming_stats(Type),
    case emqx_channel:handle_in(Packet, ChanState) of
        {ok, NChanState} ->
            SuccFun(State#state{chan_state = NChanState});
        {ok, OutPacket, NChanState} ->
            %% TODO: SuccFun,
            handle_outgoing(OutPacket, State#state{chan_state = NChanState});
        {error, Reason, NChanState} ->
            shutdown(Reason, State#state{chan_state = NChanState});
        {stop, Error, NChanState} ->
            shutdown(Error, State#state{chan_state = NChanState})
    end.

handle_outgoing(Packet = ?PACKET(Type), State = #state{chan_state = ChanState}) ->
    ProtoVer = emqx_channel:info(proto_ver, ChanState),
    Data = emqx_frame:serialize(Packet, ProtoVer),
    BinSize = iolist_size(Data),
    _ = inc_outgoing_stats(Type, BinSize),
    {reply, {binary, Data}, ensure_stats_timer(State)}.

inc_incoming_stats(Type) ->
    emqx_pd:update_counter(recv_pkt, 1),
    (Type == ?PUBLISH)
        andalso emqx_pd:update_counter(recv_msg, 1).

inc_outgoing_stats(Type, BinSize) ->
    emqx_pd:update_counter(send_cnt, 1),
    emqx_pd:update_counter(send_oct, BinSize),
    emqx_pd:update_counter(send_pkt, 1),
    (Type == ?PUBLISH)
        andalso emqx_pd:update_counter(send_msg, 1).

ensure_stats_timer(State = #state{stats_timer = undefined,
                                  idle_timeout = IdleTimeout}) ->
    TRef = emqx_misc:start_timer(IdleTimeout, emit_stats),
    State#state{stats_timer = TRef};
%% disabled or timer existed
ensure_stats_timer(State) -> State.

-compile({inline, [shutdown/2]}).
shutdown(Reason, State) ->
    %% Fix the issue#2591(https://github.com/emqx/emqx/issues/2591#issuecomment-500278696)
    %% self() ! {stop, Reason},
    {stop, State#state{shutdown = Reason}}.

wsock_stats() ->
    [{Key, emqx_pd:get_counter(Key)} || Key <- ?SOCK_STATS].

chan_stats() ->
    [{Name, emqx_pd:get_counter(Name)} || Name <- ?CHAN_STATS].

