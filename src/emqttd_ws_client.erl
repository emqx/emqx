%%--------------------------------------------------------------------
%% Copyright (c) 2013-2017 EMQ Enterprise, Inc. (http://emqtt.io)
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

%% @doc MQTT WebSocket Connection.

-module(emqttd_ws_client).

-behaviour(gen_server2).

-author("Feng Lee <feng@emqtt.io>").

-include("emqttd.hrl").

-include("emqttd_protocol.hrl").

-include("emqttd_internal.hrl").

-import(proplists, [get_value/3]).

%% API Exports
-export([start_link/4]).

%% Management and Monitor API
-export([info/1, stats/1, kick/1, clean_acl_cache/2]).

%% SUB/UNSUB Asynchronously
-export([subscribe/2, unsubscribe/2]).

%% Get the session proc?
-export([session/1]).

%% gen_server Function Exports
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%% gen_server2 Callbacks
-export([prioritise_call/4, prioritise_info/3, handle_pre_hibernate/1]).

%% WebSocket Client State
-record(wsclient_state, {ws_pid, peername, connection, proto_state, keepalive,
                         enable_stats, force_gc_count}).

-define(SOCK_STATS, [recv_oct, recv_cnt, send_oct, send_cnt, send_pend]).

-define(WSLOG(Level, Format, Args, State),
              lager:Level("WsClient(~s): " ++ Format,
                          [esockd_net:format(State#wsclient_state.peername) | Args])).

%% @doc Start WebSocket Client.
start_link(Env, WsPid, Req, ReplyChannel) ->
    gen_server2:start_link(?MODULE, [Env, WsPid, Req, ReplyChannel],
                           [{spawn_opt, ?FULLSWEEP_OPTS}]). %% Tune GC.

info(CPid) ->
    gen_server2:call(CPid, info).

stats(CPid) ->
    gen_server2:call(CPid, stats).

kick(CPid) ->
    gen_server2:call(CPid, kick).

subscribe(CPid, TopicTable) ->
    CPid ! {subscribe, TopicTable}.

unsubscribe(CPid, Topics) ->
    CPid ! {unsubscribe, Topics}.

session(CPid) ->
    gen_server2:call(CPid, session).

clean_acl_cache(CPid, Topic) ->
    gen_server2:call(CPid, {clean_acl_cache, Topic}).

%%--------------------------------------------------------------------
%% gen_server Callbacks
%%--------------------------------------------------------------------

init([Env, WsPid, Req, ReplyChannel]) ->
    process_flag(trap_exit, true),
    Conn = Req:get(connection),
    true = link(WsPid),
    case Req:get(peername) of
        {ok, Peername} ->
            Headers = mochiweb_headers:to_list(
                        mochiweb_request:get(headers, Req)),
            ProtoState = emqttd_protocol:init(Conn, Peername, send_fun(ReplyChannel),
                                              [{ws_initial_headers, Headers} | Env]),
            IdleTimeout = get_value(client_idle_timeout, Env, 30000),
            EnableStats = get_value(client_enable_stats, Env, false),
            ForceGcCount = emqttd_gc:conn_max_gc_count(),
            {ok, #wsclient_state{connection     = Conn,
                                 ws_pid         = WsPid,
                                 peername       = Peername,
                                 proto_state    = ProtoState,
                                 enable_stats   = EnableStats,
                                 force_gc_count = ForceGcCount},
             IdleTimeout, {backoff, 2000, 2000, 20000}, ?MODULE};
        {error, enotconn} -> Conn:fast_close(),
                             exit(WsPid, normal),
                             exit(normal);
        {error, Reason}   -> Conn:fast_close(),
                             exit(WsPid, normal),
                             exit({shutdown, Reason})
    end.

prioritise_call(Msg, _From, _Len, _State) ->
    case Msg of info -> 10; stats -> 10; state -> 10; _ -> 5 end.

prioritise_info(Msg, _Len, _State) ->
    case Msg of {redeliver, _} -> 5; _ -> 0 end.

handle_pre_hibernate(State = #wsclient_state{ws_pid = WsPid}) ->
    erlang:garbage_collect(WsPid),
    {hibernate, emqttd_gc:reset_conn_gc_count(#wsclient_state.force_gc_count, emit_stats(State))}.

handle_call(info, From, State = #wsclient_state{peername    = Peername,
                                                proto_state = ProtoState}) ->
    Info = [{websocket, true}, {peername, Peername} | emqttd_protocol:info(ProtoState)],
    {reply, Stats, _, _} = handle_call(stats, From, State),
    reply(lists:append(Info, Stats), State);

handle_call(stats, _From, State = #wsclient_state{proto_state = ProtoState}) ->
    reply(lists:append([emqttd_misc:proc_stats(),
                        wsock_stats(State),
                        emqttd_protocol:stats(ProtoState)]), State);

handle_call(kick, _From, State) ->
    {stop, {shutdown, kick}, ok, State};

handle_call(session, _From, State = #wsclient_state{proto_state = ProtoState}) ->
    reply(emqttd_protocol:session(ProtoState), State);

handle_call({clean_acl_cache, Topic}, _From, State) ->
    erase({acl, publish, Topic}),
    reply(ok, State);

handle_call(Req, _From, State) ->
    ?WSLOG(error, "Unexpected request: ~p", [Req], State),
    reply({error, unexpected_request}, State).

handle_cast({received, Packet}, State = #wsclient_state{proto_state = ProtoState}) ->
    emqttd_metrics:received(Packet),
    case emqttd_protocol:received(Packet, ProtoState) of
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
    ?WSLOG(error, "Unexpected Msg: ~p", [Msg], State),
    {noreply, State, hibernate}.

handle_info({subscribe, TopicTable}, State) ->
    with_proto(
      fun(ProtoState) ->
          emqttd_protocol:subscribe(TopicTable, ProtoState)
      end, State);

handle_info({unsubscribe, Topics}, State) ->
    with_proto(
      fun(ProtoState) ->
          emqttd_protocol:unsubscribe(Topics, ProtoState)
      end, State);

handle_info({suback, PacketId, GrantedQos}, State) ->
    with_proto(
      fun(ProtoState) ->
          Packet = ?SUBACK_PACKET(PacketId, GrantedQos),
          emqttd_protocol:send(Packet, ProtoState)
      end, State);

handle_info({deliver, Message}, State) ->
    with_proto(
      fun(ProtoState) ->
          emqttd_protocol:send(Message, ProtoState)
      end, gc(State));

handle_info({redeliver, {?PUBREL, PacketId}}, State) ->
    with_proto(
      fun(ProtoState) ->
          emqttd_protocol:pubrel(PacketId, ProtoState)
      end, State);

handle_info(emit_stats, State) ->
    {noreply, emit_stats(State), hibernate};

handle_info(timeout, State) ->
    shutdown(idle_timeout, State);

handle_info({shutdown, conflict, {ClientId, NewPid}}, State) ->
    ?WSLOG(warning, "clientid '~s' conflict with ~p", [ClientId, NewPid], State),
    shutdown(conflict, State);

handle_info({shutdown, Reason}, State) ->
    shutdown(Reason, State);

handle_info({keepalive, start, Interval}, State = #wsclient_state{connection = Conn}) ->
    ?WSLOG(debug, "Keepalive at the interval of ~p", [Interval], State),
    case emqttd_keepalive:start(stat_fun(Conn), Interval, {keepalive, check}) of
        {ok, KeepAlive} ->
            {noreply, State#wsclient_state{keepalive = KeepAlive}, hibernate};
        {error, Error} ->
            ?WSLOG(warning, "Keepalive error - ~p", [Error], State),
            shutdown(Error, State)
    end;

handle_info({keepalive, check}, State = #wsclient_state{keepalive = KeepAlive}) ->
    case emqttd_keepalive:check(KeepAlive) of
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
    case emqttd_protocol:session(ProtoState) of
        Pid -> stop(Reason, State);
        _   -> ?WSLOG(error, "Unexpected EXIT: ~p, Reason: ~p", [Pid, Reason], State),
               {noreply, State, hibernate}
    end;

handle_info(Info, State) ->
    ?WSLOG(error, "Unexpected Info: ~p", [Info], State),
    {noreply, State, hibernate}.

terminate(Reason, #wsclient_state{proto_state = ProtoState, keepalive = KeepAlive}) ->
    emqttd_keepalive:cancel(KeepAlive),
    case Reason of
        {shutdown, Error} ->
            emqttd_protocol:shutdown(Error, ProtoState);
        _ ->
            emqttd_protocol:shutdown(Reason, ProtoState)
    end.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

send_fun(ReplyChannel) ->
    Self = self(),
    fun(Packet) ->
        Data = emqttd_serializer:serialize(Packet),
        emqttd_metrics:inc('bytes/sent', iolist_size(Data)),
        case ReplyChannel({binary, Data}) of
            ok -> ok;
            {error, Reason} -> Self ! {shutdown, Reason}
        end
    end.

stat_fun(Conn) ->
    fun() ->
        case Conn:getstat([recv_oct]) of
            {ok, [{recv_oct, RecvOct}]} -> {ok, RecvOct};
            {error, Error}              -> {error, Error}
        end
    end.

emit_stats(State = #wsclient_state{proto_state = ProtoState}) ->
    emit_stats(emqttd_protocol:clientid(ProtoState), State).

emit_stats(_ClientId, State = #wsclient_state{enable_stats = false}) ->
    State;
emit_stats(undefined, State) ->
    State;
emit_stats(ClientId, State) ->
    {reply, Stats, _, _} = handle_call(stats, undefined, State),
    emqttd_stats:set_client_stats(ClientId, Stats),
    State.

wsock_stats(#wsclient_state{connection = Conn}) ->
    case Conn:getstat(?SOCK_STATS) of
        {ok,   Ss} -> Ss;
        {error, _} -> []
    end.

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
    emqttd_gc:maybe_force_gc(#wsclient_state.force_gc_count, State, Cb).

