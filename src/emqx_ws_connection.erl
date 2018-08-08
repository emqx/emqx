%% Copyright (c) 2018 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-behaviour(gen_server).

-include("emqx.hrl").

-include("emqx_mqtt.hrl").

-import(proplists, [get_value/2, get_value/3]).

%% API Exports
-export([start_link/3]).

%% Management and Monitor API
-export([info/1, stats/1, kick/1, clean_acl_cache/2]).

%% SUB/UNSUB Asynchronously
-export([subscribe/2, unsubscribe/2]).

%% Get the session proc?
-export([session/1]).

%% gen_server Function Exports
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%% WebSocket Client State
-record(wsclient_state, {ws_pid, peername, proto_state, keepalive,
                         enable_stats, force_gc_count}).

%% recv_oct
%% Number of bytes received by the socket.

%% recv_cnt
%% Number of packets received by the socket.

-define(SOCK_STATS, [recv_oct, recv_cnt, send_oct, send_cnt, send_pend]).

-define(WSLOG(Level, Format, Args, State),
              emqx_logger:Level("WsClient(~s): " ++ Format,
                                [esockd_net:format(State#wsclient_state.peername) | Args])).

%% @doc Start WebSocket Client.
start_link(Env, WsPid, Req) ->
    gen_server:start_link(?MODULE, [Env, WsPid, Req],
                          [[{hibernate_after, 10000}]]).

info(CPid) ->
    gen_server:call(CPid, info).

stats(CPid) ->
    gen_server:call(CPid, stats).

kick(CPid) ->
    gen_server:call(CPid, kick).

subscribe(CPid, TopicTable) ->
    CPid ! {subscribe, TopicTable}.

unsubscribe(CPid, Topics) ->
    CPid ! {unsubscribe, Topics}.

session(CPid) ->
    gen_server:call(CPid, session).

clean_acl_cache(CPid, Topic) ->
    gen_server:call(CPid, {clean_acl_cache, Topic}).

%%--------------------------------------------------------------------
%% gen_server Callbacks
%%--------------------------------------------------------------------

init([Options, WsPid, Req]) ->
    init_stas(),
    process_flag(trap_exit, true),
    true = link(WsPid),
    Peername = cowboy_req:peer(Req),
    Headers  = cowboy_req:headers(Req),
    Sockname = cowboy_req:sock(Req),
    Peercert = cowboy_req:cert(Req),
    Zone     = proplists:get_value(zone, Options),
    ProtoState = emqx_protocol:init(#{zone     => Zone,
                                      peername => Peername,
                                      sockname => Sockname,
                                      peercert => Peercert,
                                      sendfun  => send_fun(WsPid)},
                                    [{ws_initial_headers, Headers} | Options]),
    IdleTimeout = get_value(client_idle_timeout, Options, 30000),
    EnableStats = get_value(client_enable_stats, Options, false),
    ForceGcCount = emqx_gc:conn_max_gc_count(),
    {ok, #wsclient_state{ws_pid         = WsPid,
                         peername       = Peername,
                         proto_state    = ProtoState,
                         enable_stats   = EnableStats,
                         force_gc_count = ForceGcCount}, IdleTimeout}.

handle_call(info, From, State = #wsclient_state{peername    = Peername,
                                                proto_state = ProtoState}) ->
    Info = [{websocket, true}, {peername, Peername} | emqx_protocol:info(ProtoState)],
    {reply, Stats, _, _} = handle_call(stats, From, State),
    reply(lists:append(Info, Stats), State);

handle_call(stats, _From, State = #wsclient_state{proto_state = ProtoState}) ->
    reply(lists:append([emqx_misc:proc_stats(),
                        wsock_stats(),
                        emqx_protocol:stats(ProtoState)]), State);

handle_call(kick, _From, State) ->
    {stop, {shutdown, kick}, ok, State};

handle_call(session, _From, State = #wsclient_state{proto_state = ProtoState}) ->
    reply(emqx_protocol:session(ProtoState), State);

handle_call({clean_acl_cache, Topic}, _From, State) ->
    erase({acl, publish, Topic}),
    reply(ok, State);

handle_call(Req, _From, State) ->
    ?WSLOG(error, "Unexpected request: ~p", [Req], State),
    reply({error, unexpected_request}, State).

handle_cast({received, Packet, BinSize}, State = #wsclient_state{proto_state = ProtoState}) ->
    put(recv_oct, get(recv_oct) + BinSize),
    put(recv_cnt, get(recv_cnt) + 1),
    emqx_metrics:received(Packet),
    case emqx_protocol:received(Packet, ProtoState) of
        {ok, ProtoState1} ->
            {noreply, gc(State#wsclient_state{proto_state = ProtoState1}), hibernate};
        {error, Error} ->
            ?WSLOG(error, "Protocol error - ~p", [Error], State),
            shutdown(Error, State);
        {error, Error, ProtoState1} ->
            shutdown(Error, State#wsclient_state{proto_state = ProtoState1});
        {stop, Reason, ProtoState1} ->
            stop(Reason, State#wsclient_state{proto_state = ProtoState1})
    end;

handle_cast(Msg, State) ->
    ?WSLOG(error, "unexpected msg: ~p", [Msg], State),
    {noreply, State}.

handle_info(SubReq ={subscribe, _TopicTable}, State) ->
    with_proto(
      fun(ProtoState) ->
          emqx_protocol:process(SubReq, ProtoState)
      end, State);

handle_info(UnsubReq = {unsubscribe, _Topics}, State) ->
    with_proto(
      fun(ProtoState) ->
          emqx_protocol:process(UnsubReq, ProtoState)
      end, State);

handle_info({deliver, PubOrAck}, State) ->
    with_proto(
      fun(ProtoState) ->
          emqx_protocol:deliver(PubOrAck, ProtoState)
      end, gc(State));

handle_info(emit_stats, State) ->
    {noreply, emit_stats(State), hibernate};

handle_info(timeout, State) ->
    shutdown(idle_timeout, State);

handle_info({shutdown, conflict, {ClientId, NewPid}}, State) ->
    ?WSLOG(warning, "clientid '~s' conflict with ~p", [ClientId, NewPid], State),
    shutdown(conflict, State);

handle_info({shutdown, Reason}, State) ->
    shutdown(Reason, State);

handle_info({keepalive, start, Interval}, State) ->
    ?WSLOG(debug, "Keepalive at the interval of ~p", [Interval], State),
    case emqx_keepalive:start(stat_fun(), Interval, {keepalive, check}) of
        {ok, KeepAlive} ->
            {noreply, State#wsclient_state{keepalive = KeepAlive}, hibernate};
        {error, Error} ->
            ?WSLOG(warning, "Keepalive error - ~p", [Error], State),
            shutdown(Error, State)
    end;

handle_info({keepalive, check}, State = #wsclient_state{keepalive = KeepAlive}) ->
    case emqx_keepalive:check(KeepAlive) of
        {ok, KeepAlive1} ->
            {noreply, emit_stats(State#wsclient_state{keepalive = KeepAlive1}), hibernate};
        {error, timeout} ->
            ?WSLOG(debug, "Keepalive Timeout!", [], State),
            shutdown(keepalive_timeout, State);
        {error, Error} ->
            ?WSLOG(warning, "Keepalive error - ~p", [Error], State),
            shutdown(keepalive_error, State)
    end;

handle_info({'EXIT', WsPid, normal}, State = #wsclient_state{ws_pid = WsPid}) ->
    stop(normal, State);

handle_info({'EXIT', WsPid, Reason}, State = #wsclient_state{ws_pid = WsPid}) ->
    ?WSLOG(error, "shutdown: ~p",[Reason], State),
    shutdown(Reason, State);

%% The session process exited unexpectedly.
handle_info({'EXIT', Pid, Reason}, State = #wsclient_state{proto_state = ProtoState}) ->
    case emqx_protocol:session(ProtoState) of
        Pid -> stop(Reason, State);
        _   -> ?WSLOG(error, "Unexpected EXIT: ~p, Reason: ~p", [Pid, Reason], State),
               {noreply, State, hibernate}
    end;

handle_info(Info, State) ->
    ?WSLOG(error, "Unexpected Info: ~p", [Info], State),
    {noreply, State, hibernate}.

terminate(Reason, #wsclient_state{proto_state = ProtoState, keepalive = KeepAlive}) ->
    emqx_keepalive:cancel(KeepAlive),
    case Reason of
        {shutdown, Error} ->
            emqx_protocol:shutdown(Error, ProtoState);
        _ ->
            emqx_protocol:shutdown(Reason, ProtoState)
    end.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

send_fun(WsPid) ->
    fun(Data) ->
        BinSize = iolist_size(Data),
        emqx_metrics:inc('bytes/sent', BinSize),
        put(send_oct, get(send_oct) + BinSize),
        put(send_cnt, get(send_cnt) + 1),
        WsPid ! {binary, iolist_to_binary(Data)}
    end.

stat_fun() ->
    fun() ->
        {ok, get(recv_oct)}
    end.

emit_stats(State = #wsclient_state{proto_state = ProtoState}) ->
    emit_stats(emqx_protocol:clientid(ProtoState), State).

emit_stats(_ClientId, State = #wsclient_state{enable_stats = false}) ->
    State;
emit_stats(undefined, State) ->
    State;
emit_stats(ClientId, State) ->
    {reply, Stats, _, _} = handle_call(stats, undefined, State),
    emqx_cm:set_client_stats(ClientId, Stats),
    State.

wsock_stats() ->
    [{Key, get(Key)}|| Key <- ?SOCK_STATS].

with_proto(Fun, State = #wsclient_state{proto_state = ProtoState}) ->
    {ok, ProtoState1} = Fun(ProtoState),
    {noreply, State#wsclient_state{proto_state = ProtoState1}, hibernate}.

reply(Reply, State) ->
    {reply, Reply, State, hibernate}.

shutdown(Reason, State) ->
    stop({shutdown, Reason}, State).

stop(Reason, State) ->
    {stop, Reason, State}.

gc(State) ->
    Cb = fun() -> emit_stats(State) end,
    emqx_gc:maybe_force_gc(#wsclient_state.force_gc_count, State, Cb).

init_stas() ->
    put(recv_oct, 0),
    put(recv_cnt, 0),
    put(send_oct, 0),
    put(send_cnt, 0).

