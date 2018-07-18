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

-module(emqx_connection).

-behaviour(gen_server).

-include("emqx.hrl").

-include("emqx_mqtt.hrl").

-include("emqx_misc.hrl").

-import(proplists, [get_value/2, get_value/3]).

%% API Function Exports
-export([start_link/3]).

%% Management and Monitor API
-export([info/1, stats/1, kick/1, clean_acl_cache/2]).

-export([set_rate_limit/2, get_rate_limit/1]).

%% SUB/UNSUB Asynchronously. Called by plugins.
-export([subscribe/2, unsubscribe/2]).

%% Get the session proc?
-export([session/1]).

%% gen_server Function Exports
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, code_change/3,
         terminate/2]).

%% Unused fields: connname, peerhost, peerport
-record(state, {transport, socket, peername, conn_state, await_recv,
                rate_limit, max_packet_size, proto_state, parse_state,
                keepalive, enable_stats, idle_timeout, force_gc_count}).

-define(INFO_KEYS, [peername, conn_state, await_recv]).

-define(SOCK_STATS, [recv_oct, recv_cnt, send_oct, send_cnt, send_pend]).

-define(LOG(Level, Format, Args, State),
            emqx_logger:Level("Client(~s): " ++ Format,
                              [esockd_net:format(State#state.peername) | Args])).

start_link(Transport, Sock, Env) ->
    {ok, proc_lib:spawn_link(?MODULE, init, [[Transport, Sock, Env]])}.

info(CPid) ->
    gen_server:call(CPid, info).

stats(CPid) ->
    gen_server:call(CPid, stats).

kick(CPid) ->
    gen_server:call(CPid, kick).

set_rate_limit(CPid, Rl) ->
    gen_server:call(CPid, {set_rate_limit, Rl}).

get_rate_limit(CPid) ->
    gen_server:call(CPid, get_rate_limit).

subscribe(CPid, TopicTable) ->
    CPid ! {subscribe, TopicTable}.

unsubscribe(CPid, Topics) ->
    CPid ! {unsubscribe, Topics}.

session(CPid) ->
    gen_server:call(CPid, session, infinity).

clean_acl_cache(CPid, Topic) ->
    gen_server:call(CPid, {clean_acl_cache, Topic}).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([Transport, Sock, Env]) ->
    case Transport:wait(Sock) of
        {ok, NewSock} ->
            {ok, Peername} = Transport:ensure_ok_or_exit(peername, [NewSock]),
            do_init(Transport, Sock, Peername, Env);
        {error, Reason} ->
            {stop, Reason}
    end.

do_init(Transport, Sock, Peername, Env) ->
    RateLimit = get_value(rate_limit, Env),
    PacketSize = get_value(max_packet_size, Env, ?MAX_PACKET_SIZE),
    SendFun = send_fun(Transport, Sock, Peername),
    ProtoState = emqx_protocol:init(Transport, Sock, Peername, SendFun, Env),
    EnableStats = get_value(client_enable_stats, Env, false),
    IdleTimout = get_value(client_idle_timeout, Env, 30000),
    ForceGcCount = emqx_gc:conn_max_gc_count(),
    State = run_socket(#state{transport       = Transport,
                              socket          = Sock,
                              peername        = Peername,
                              await_recv      = false,
                              conn_state      = running,
                              rate_limit      = RateLimit,
                              max_packet_size = PacketSize,
                              proto_state     = ProtoState,
                              enable_stats    = EnableStats,
                              idle_timeout    = IdleTimout,
                              force_gc_count  = ForceGcCount}),
    gen_server:enter_loop(?MODULE, [{hibernate_after, IdleTimout}],
                          init_parse_state(State), self(), IdleTimout).

send_fun(Transport, Sock, Peername) ->
    Self = self(),
    fun(Packet) ->
        Data = emqx_frame:serialize(Packet),
        ?LOG(debug, "SEND ~p", [Data], #state{peername = Peername}),
        emqx_metrics:inc('bytes/sent', iolist_size(Data)),
        try Transport:async_send(Sock, Data) of
            ok -> ok;
            {error, Reason} -> Self ! {shutdown, Reason}
        catch
            error:Error -> Self ! {shutdown, Error}
        end
    end.

init_parse_state(State = #state{max_packet_size = Size, proto_state = ProtoState}) ->
    Version = emqx_protocol:get(proto_ver, ProtoState),
    State#state{parse_state = emqx_frame:initial_state(
                                #{max_packet_size => Size, version => Version})}.

handle_call(info, From, State = #state{proto_state = ProtoState}) ->
    ProtoInfo  = emqx_protocol:info(ProtoState),
    ClientInfo = ?record_to_proplist(state, State, ?INFO_KEYS),
    {reply, Stats, _, _} = handle_call(stats, From, State),
    reply(lists:append([ClientInfo, ProtoInfo, Stats]), State);

handle_call(stats, _From, State = #state{proto_state = ProtoState}) ->
    reply(lists:append([emqx_misc:proc_stats(),
                        emqx_protocol:stats(ProtoState),
                        sock_stats(State)]), State);

handle_call(kick, _From, State) ->
    {stop, {shutdown, kick}, ok, State};

handle_call({set_rate_limit, Rl}, _From, State) ->
    reply(ok, State#state{rate_limit = Rl});

handle_call(get_rate_limit, _From, State = #state{rate_limit = Rl}) ->
    reply(Rl, State);

handle_call(session, _From, State = #state{proto_state = ProtoState}) ->
    reply(emqx_protocol:session(ProtoState), State);

handle_call({clean_acl_cache, Topic}, _From, State) ->
    erase({acl, publish, Topic}),
    reply(ok, State);

handle_call(Req, _From, State) ->
    ?LOG(error, "Unexpected Call: ~p", [Req], State),
    {reply, ignore, State}.

handle_cast(Msg, State) ->
    ?LOG(error, "Unexpected Cast: ~p", [Msg], State),
    {noreply, State}.

handle_info({subscribe, TopicTable}, State) ->
    with_proto(
      fun(ProtoState) ->
          emqx_protocol:subscribe(TopicTable, ProtoState)
      end, State);

handle_info({unsubscribe, Topics}, State) ->
    with_proto(
      fun(ProtoState) ->
          emqx_protocol:unsubscribe(Topics, ProtoState)
      end, State);

%% Asynchronous SUBACK
handle_info({suback, PacketId, GrantedQos}, State) ->
    with_proto(
      fun(ProtoState) ->
          Packet = ?SUBACK_PACKET(PacketId, GrantedQos),
          emqx_protocol:send(Packet, ProtoState)
      end, State);

%% Fastlane
handle_info({dispatch, _Topic, Msg}, State) ->
    handle_info({deliver, emqx_message:set_flag(qos, ?QOS_0, Msg)}, State);

handle_info({deliver, Message}, State) ->
    with_proto(
      fun(ProtoState) ->
          emqx_protocol:send(Message, ProtoState)
      end, State);

handle_info({redeliver, {?PUBREL, PacketId}}, State) ->
    with_proto(
      fun(ProtoState) ->
          emqx_protocol:pubrel(PacketId, ProtoState)
      end, State);

handle_info(emit_stats, State) ->
    {noreply, emit_stats(State), hibernate};

handle_info(timeout, State) ->
    shutdown(idle_timeout, State);

%% Fix issue #535
handle_info({shutdown, Error}, State) ->
    shutdown(Error, State);

handle_info({shutdown, conflict, {ClientId, NewPid}}, State) ->
    ?LOG(warning, "clientid '~s' conflict with ~p", [ClientId, NewPid], State),
    shutdown(conflict, State);

handle_info(activate_sock, State) ->
    {noreply, run_socket(State#state{conn_state = running})};

handle_info({inet_async, _Sock, _Ref, {ok, Data}}, State) ->
    Size = iolist_size(Data),
    ?LOG(debug, "RECV ~p", [Data], State),
    emqx_metrics:inc('bytes/received', Size),
    received(Data, rate_limit(Size, State#state{await_recv = false}));

handle_info({inet_async, _Sock, _Ref, {error, Reason}}, State) ->
    shutdown(Reason, State);

handle_info({inet_reply, _Sock, ok}, State) ->
    {noreply, gc(State)}; %% Tune GC

handle_info({inet_reply, _Sock, {error, Reason}}, State) ->
    shutdown(Reason, State);

handle_info({keepalive, start, Interval},
            State = #state{transport = Transport, socket = Sock}) ->
    ?LOG(debug, "Keepalive at the interval of ~p", [Interval], State),
    StatFun = fun() ->
                case Transport:getstat(Sock, [recv_oct]) of
                    {ok, [{recv_oct, RecvOct}]} -> {ok, RecvOct};
                    Error                       -> Error
                end
             end,
    case emqx_keepalive:start(StatFun, Interval, {keepalive, check}) of
        {ok, KeepAlive} ->
            {noreply, State#state{keepalive = KeepAlive}};
        {error, Error} ->
            ?LOG(warning, "Keepalive error - ~p", [Error], State),
            shutdown(Error, State)
    end;

handle_info({keepalive, check}, State = #state{keepalive = KeepAlive}) ->
    case emqx_keepalive:check(KeepAlive) of
        {ok, KeepAlive1} ->
            {noreply, State#state{keepalive = KeepAlive1}};
        {error, timeout} ->
            ?LOG(debug, "Keepalive timeout", [], State),
            shutdown(keepalive_timeout, State);
        {error, Error} ->
            ?LOG(warning, "Keepalive error - ~p", [Error], State),
            shutdown(Error, State)
    end;

handle_info(Info, State) ->
    ?LOG(error, "Unexpected Info: ~p", [Info], State),
    {noreply, State}.

terminate(Reason, State = #state{transport   = Transport,
                                 socket      = Sock,
                                 keepalive   = KeepAlive,
                                 proto_state = ProtoState}) ->

    ?LOG(debug, "Terminated for ~p", [Reason], State),
    Transport:fast_close(Sock),
    emqx_keepalive:cancel(KeepAlive),
    case {ProtoState, Reason} of
        {undefined, _} ->
            ok;
        {_, {shutdown, Error}} ->
            emqx_protocol:shutdown(Error, ProtoState);
        {_, Reason} ->
            emqx_protocol:shutdown(Reason, ProtoState)
    end.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

%% Receive and Parse TCP Data
received(<<>>, State) ->
    {noreply, gc(State)};

received(Bytes, State = #state{parse_state  = ParseState,
                               proto_state  = ProtoState,
                               idle_timeout = IdleTimeout}) ->
    case catch emqx_frame:parse(Bytes, ParseState) of
        {more, NewParseState} ->
            {noreply, State#state{parse_state = NewParseState}, IdleTimeout};
        {ok, Packet, Rest} ->
            emqx_metrics:received(Packet),
            case emqx_protocol:received(Packet, ProtoState) of
                {ok, ProtoState1} ->
                    received(Rest, init_parse_state(State#state{proto_state = ProtoState1}));
                {error, Error} ->
                    ?LOG(error, "Protocol error - ~p", [Error], State),
                    shutdown(Error, State);
                {error, Error, ProtoState1} ->
                    shutdown(Error, State#state{proto_state = ProtoState1});
                {stop, Reason, ProtoState1} ->
                    stop(Reason, State#state{proto_state = ProtoState1})
            end;
        {error, Error} ->
            ?LOG(error, "Framing error - ~p", [Error], State),
            shutdown(Error, State);
        {'EXIT', Reason} ->
            ?LOG(error, "Parser failed for ~p", [Reason], State),
            ?LOG(error, "Error data: ~p", [Bytes], State),
            shutdown(parser_error, State)
    end.

rate_limit(_Size, State = #state{rate_limit = undefined}) ->
    run_socket(State);
rate_limit(Size, State = #state{rate_limit = Rl}) ->
    case Rl:check(Size) of
        {0, Rl1} ->
            run_socket(State#state{conn_state = running, rate_limit = Rl1});
        {Pause, Rl1} ->
            ?LOG(warning, "Rate limiter pause for ~p", [Pause], State),
            erlang:send_after(Pause, self(), activate_sock),
            State#state{conn_state = blocked, rate_limit = Rl1}
    end.

run_socket(State = #state{conn_state = blocked}) ->
    State;
run_socket(State = #state{await_recv = true}) ->
    State;
run_socket(State = #state{transport = Transport, socket = Sock}) ->
    Transport:async_recv(Sock, 0, infinity),
    State#state{await_recv = true}.

with_proto(Fun, State = #state{proto_state = ProtoState}) ->
    {ok, ProtoState1} = Fun(ProtoState),
    {noreply, State#state{proto_state = ProtoState1}}.

emit_stats(State = #state{proto_state = ProtoState}) ->
    emit_stats(emqx_protocol:clientid(ProtoState), State).

emit_stats(_ClientId, State = #state{enable_stats = false}) ->
    State;
emit_stats(undefined, State) ->
    State;
emit_stats(ClientId, State) ->
    {reply, Stats, _, _} = handle_call(stats, undefined, State),
    emqx_cm:set_client_stats(ClientId, Stats),
    State.

sock_stats(#state{transport = Transport, socket = Sock}) ->
    case Transport:getstat(Sock, ?SOCK_STATS) of
        {ok, Ss} -> Ss;
        _Error   -> []
    end.

reply(Reply, State) ->
    {reply, Reply, State, hibernate}.

shutdown(Reason, State) ->
    stop({shutdown, Reason}, State).

stop(Reason, State) ->
    {stop, Reason, State}.

gc(State = #state{transport = Transport, socket = Sock}) ->
    Cb = fun() -> Transport:gc(Sock), emit_stats(State) end,
    emqx_gc:maybe_force_gc(#state.force_gc_count, State, Cb).

