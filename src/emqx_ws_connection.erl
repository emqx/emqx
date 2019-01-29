%% Copyright (c) 2013-2019 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_ws_connection).

-include("emqx.hrl").
-include("emqx_mqtt.hrl").
-include("logger.hrl").

-export([info/1, attrs/1]).
-export([stats/1]).
-export([kick/1]).
-export([session/1]).

%% websocket callbacks
-export([init/2]).
-export([websocket_init/1]).
-export([websocket_handle/2]).
-export([websocket_info/2]).
-export([terminate/3]).

-record(state, {
          request,
          options,
          peername,
          sockname,
          idle_timeout,
          proto_state,
          parser_state,
          keepalive,
          enable_stats,
          stats_timer,
          shutdown
         }).

-define(SOCK_STATS, [recv_oct, recv_cnt, send_oct, send_cnt]).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

%% for debug
info(WSPid) when is_pid(WSPid) ->
    call(WSPid, info);

info(#state{peername    = Peername,
            sockname    = Sockname,
            proto_state = ProtoState}) ->
    ProtoInfo = emqx_protocol:info(ProtoState),
    ConnInfo = [{socktype, websocket},
                {conn_state, running},
                {peername, Peername},
                {sockname, Sockname}],
    lists:append([ConnInfo, ProtoInfo]).

%% for dashboard
attrs(WSPid) when is_pid(WSPid) ->
    call(WSPid, attrs);

attrs(#state{peername    = Peername,
             sockname    = Sockname,
             proto_state = ProtoState}) ->
    SockAttrs = [{peername, Peername},
                 {sockname, Sockname}],
    ProtoAttrs = emqx_protocol:attrs(ProtoState),
    lists:usort(lists:append(SockAttrs, ProtoAttrs)).

stats(WSPid) when is_pid(WSPid) ->
    call(WSPid, stats);

stats(#state{proto_state = ProtoState}) ->
    lists:append([wsock_stats(),
                  emqx_misc:proc_stats(),
                  emqx_protocol:stats(ProtoState)
                 ]).

kick(WSPid) when is_pid(WSPid) ->
    call(WSPid, kick).

session(WSPid) when is_pid(WSPid) ->
    call(WSPid, session).

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

%%------------------------------------------------------------------------------
%% WebSocket callbacks
%%------------------------------------------------------------------------------

init(Req, Opts) ->
    case cowboy_req:parse_header(<<"sec-websocket-protocol">>, Req) of
        undefined ->
            {cowboy_websocket, Req, #state{}};
        [<<"mqtt", Vsn/binary>>] ->
            Resp = cowboy_req:set_resp_header(<<"sec-websocket-protocol">>, <<"mqtt", Vsn/binary>>, Req),
            {cowboy_websocket, Resp, #state{request = Req, options = Opts}, #{idle_timeout => 86400000}};
        _ ->
            {ok, cowboy_req:reply(400, Req), #state{}}
    end.

websocket_init(#state{request = Req, options = Options}) ->
    Peername = cowboy_req:peer(Req),
    Sockname = cowboy_req:sock(Req),
    Peercert = cowboy_req:cert(Req),
    ProtoState = emqx_protocol:init(#{peername => Peername,
                                      sockname => Sockname,
                                      peercert => Peercert,
                                      sendfun  => send_fun(self())}, Options),
    ParserState = emqx_protocol:parser(ProtoState),
    Zone = proplists:get_value(zone, Options),
    EnableStats = emqx_zone:get_env(Zone, enable_stats, true),
    IdleTimout = emqx_zone:get_env(Zone, idle_timeout, 30000),

    emqx_logger:set_metadata_peername(esockd_net:format(Peername)),
    {ok, #state{peername     = Peername,
                sockname     = Sockname,
                parser_state = ParserState,
                proto_state  = ProtoState,
                enable_stats = EnableStats,
                idle_timeout = IdleTimout}}.

send_fun(WsPid) ->
    fun(Packet, Options) ->
        Data = emqx_frame:serialize(Packet, Options),
        BinSize = iolist_size(Data),
        emqx_metrics:trans(inc, 'bytes/sent', BinSize),
        emqx_pd:update_counter(send_cnt, 1),
        emqx_pd:update_counter(send_oct, BinSize),
        WsPid ! {binary, iolist_to_binary(Data)},
        ok
    end.

stat_fun() ->
    fun() -> {ok, emqx_pd:get_counter(recv_oct)} end.

websocket_handle({binary, <<>>}, State) ->
    {ok, ensure_stats_timer(State)};
websocket_handle({binary, [<<>>]}, State) ->
    {ok, ensure_stats_timer(State)};
websocket_handle({binary, Data}, State = #state{parser_state = ParserState,
                                                proto_state  = ProtoState}) ->
    ?LOG(debug, "RECV ~p", [Data]),
    BinSize = iolist_size(Data),
    emqx_pd:update_counter(recv_oct, BinSize),
    emqx_metrics:trans(inc, 'bytes/received', BinSize),
    try emqx_frame:parse(iolist_to_binary(Data), ParserState) of
        {more, ParserState1} ->
            {ok, State#state{parser_state = ParserState1}};
        {ok, Packet, Rest} ->
            emqx_metrics:received(Packet),
            emqx_pd:update_counter(recv_cnt, 1),
            case emqx_protocol:received(Packet, ProtoState) of
                {ok, ProtoState1} ->
                    websocket_handle({binary, Rest}, reset_parser(State#state{proto_state = ProtoState1}));
                {error, Error} ->
                    ?LOG(error, "Protocol error - ~p", [Error]),
                    shutdown(Error, State);
                {error, Reason, ProtoState1} ->
                    shutdown(Reason, State#state{proto_state = ProtoState1});
                {stop, Error, ProtoState1} ->
                    shutdown(Error, State#state{proto_state = ProtoState1})
            end;
        {error, Error} ->
            ?LOG(error, "Frame error: ~p", [Error]),
            shutdown(Error, State)
    catch
        _:Error ->
            ?LOG(error, "Frame error:~p~nFrame data: ~p", [Error, Data]),
            shutdown(parse_error, State)
    end.

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

websocket_info({call, From, session}, State = #state{proto_state = ProtoState}) ->
    gen_server:reply(From, emqx_protocol:session(ProtoState)),
    {ok, State};

websocket_info({deliver, PubOrAck}, State = #state{proto_state = ProtoState}) ->
    case emqx_protocol:deliver(PubOrAck, ProtoState) of
        {ok, ProtoState1} ->
            {ok, ensure_stats_timer(State#state{proto_state = ProtoState1})};
        {error, Reason} ->
            shutdown(Reason, State)
    end;

websocket_info({timeout, Timer, emit_stats},
               State = #state{stats_timer = Timer, proto_state = ProtoState}) ->
    emqx_metrics:commit(),
    emqx_cm:set_conn_stats(emqx_protocol:client_id(ProtoState), stats(State)),
    {ok, State#state{stats_timer = undefined}, hibernate};

websocket_info({keepalive, start, Interval}, State) ->
    ?LOG(debug, "Keepalive at the interval of ~p", [Interval]),
    case emqx_keepalive:start(stat_fun(), Interval, {keepalive, check}) of
        {ok, KeepAlive} ->
            {ok, State#state{keepalive = KeepAlive}};
        {error, Error} ->
            ?LOG(warning, "Keepalive error - ~p", [Error]),
            shutdown(Error, State)
    end;

websocket_info({keepalive, check}, State = #state{keepalive = KeepAlive}) ->
    case emqx_keepalive:check(KeepAlive) of
        {ok, KeepAlive1} ->
            {ok, State#state{keepalive = KeepAlive1}};
        {error, timeout} ->
            ?LOG(debug, "Keepalive Timeout!", []),
            shutdown(keepalive_timeout, State);
        {error, Error} ->
            ?LOG(warning, "Keepalive error - ~p", [Error]),
            shutdown(keepalive_error, State)
    end;

websocket_info({shutdown, discard, {ClientId, ByPid}}, State) ->
    ?LOG(warning, "discarded by ~s:~p", [ClientId, ByPid]),
    shutdown(discard, State);

websocket_info({shutdown, conflict, {ClientId, NewPid}}, State) ->
    ?LOG(warning, "clientid '~s' conflict with ~p", [ClientId, NewPid]),
    shutdown(conflict, State);

websocket_info({binary, Data}, State) ->
    {reply, {binary, Data}, State};

websocket_info({shutdown, Reason}, State) ->
    shutdown(Reason, State);

websocket_info(Info, State) ->
    ?LOG(error, "unexpected info: ~p", [Info]),
    {ok, State}.

terminate(SockError, _Req, #state{keepalive   = Keepalive,
                                  proto_state = ProtoState,
                                  shutdown    = Shutdown}) ->

    ?LOG(debug, "Terminated for ~p, sockerror: ~p",
           [Shutdown, SockError]),
    emqx_keepalive:cancel(Keepalive),
    case {ProtoState, Shutdown} of
        {undefined, _} -> ok;
        {_, {shutdown, Reason}} ->
            emqx_protocol:shutdown(Reason, ProtoState);
        {_, Error} ->
            emqx_protocol:shutdown(Error, ProtoState)
    end.

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------

reset_parser(State = #state{proto_state = ProtoState}) ->
    State#state{parser_state = emqx_protocol:parser(ProtoState)}.

ensure_stats_timer(State = #state{enable_stats = true,
                                  stats_timer  = undefined,
                                  idle_timeout = IdleTimeout}) ->
    State#state{stats_timer = emqx_misc:start_timer(IdleTimeout, emit_stats)};
ensure_stats_timer(State) ->
    State.

shutdown(Reason, State) ->
    {stop, State#state{shutdown = Reason}}.

wsock_stats() ->
    [{Key, emqx_pd:get_counter(Key)} || Key <- ?SOCK_STATS].

