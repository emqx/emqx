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

%% @doc MQTT/TCP Connection.

-module(emqttd_client).

-behaviour(gen_server2).

-author("Feng Lee <feng@emqtt.io>").

-include("emqttd.hrl").

-include("emqttd_protocol.hrl").

-include("emqttd_internal.hrl").

-import(proplists, [get_value/2, get_value/3]).

%% API Function Exports
-export([start_link/2]).

%% Management and Monitor API
-export([info/1, stats/1, kick/1, clean_acl_cache/2]).

-export([set_rate_limit/2, get_rate_limit/1]).

%% SUB/UNSUB Asynchronously. Called by plugins.
-export([subscribe/2, unsubscribe/2]).

%% Get the session proc?
-export([session/1]).

%% gen_server Function Exports
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         code_change/3, terminate/2]).

%% gen_server2 Callbacks
-export([prioritise_call/4, prioritise_info/3, handle_pre_hibernate/1]).

%% Client State
%% Unused fields: connname, peerhost, peerport
-record(client_state, {connection, peername, conn_state, await_recv,
                       rate_limit, packet_size, parser, proto_state,
                       keepalive, enable_stats, idle_timeout, force_gc_count}).

-define(INFO_KEYS, [peername, conn_state, await_recv]).

-define(SOCK_STATS, [recv_oct, recv_cnt, send_oct, send_cnt, send_pend]).

-define(LOG(Level, Format, Args, State),
            lager:Level("Client(~s): " ++ Format,
                        [esockd_net:format(State#client_state.peername) | Args])).

start_link(Conn, Env) ->
    {ok, proc_lib:spawn_link(?MODULE, init, [[Conn, Env]])}.

info(CPid) ->
    gen_server2:call(CPid, info).

stats(CPid) ->
    gen_server2:call(CPid, stats).

kick(CPid) ->
    gen_server2:call(CPid, kick).

set_rate_limit(Cpid, Rl) ->
    gen_server2:call(Cpid, {set_rate_limit, Rl}).

get_rate_limit(Cpid) ->
    gen_server2:call(Cpid, get_rate_limit).

subscribe(CPid, TopicTable) ->
    CPid ! {subscribe, TopicTable}.

unsubscribe(CPid, Topics) ->
    CPid ! {unsubscribe, Topics}.

session(CPid) ->
    gen_server2:call(CPid, session, infinity).

clean_acl_cache(CPid, Topic) ->
    gen_server2:call(CPid, {clean_acl_cache, Topic}).

%%--------------------------------------------------------------------
%% gen_server Callbacks
%%--------------------------------------------------------------------

init([Conn0, Env]) ->
    {ok, Conn} = Conn0:wait(),
    case Conn:peername() of
        {ok, Peername}    -> do_init(Conn, Env, Peername);
        {error, enotconn} -> Conn:fast_close(),
                             exit(normal);
        {error, Reason}   -> Conn:fast_close(),
                             exit({shutdown, Reason})
    end.

do_init(Conn, Env, Peername) ->
    %% Send Fun
    SendFun = send_fun(Conn, Peername),
    RateLimit = get_value(rate_limit, Conn:opts()),
    PacketSize = get_value(max_packet_size, Env, ?MAX_PACKET_SIZE),
    Parser = emqttd_parser:initial_state(PacketSize),
    ProtoState = emqttd_protocol:init(Conn, Peername, SendFun, Env),
    EnableStats = get_value(client_enable_stats, Env, false),
    IdleTimout = get_value(client_idle_timeout, Env, 30000),
    ForceGcCount = emqttd_gc:conn_max_gc_count(),
    State = run_socket(#client_state{connection     = Conn,
                                     peername       = Peername,
                                     await_recv     = false,
                                     conn_state     = running,
                                     rate_limit     = RateLimit,
                                     packet_size    = PacketSize,
                                     parser         = Parser,
                                     proto_state    = ProtoState,
                                     enable_stats   = EnableStats,
                                     idle_timeout   = IdleTimout,
                                     force_gc_count = ForceGcCount}),
    gen_server2:enter_loop(?MODULE, [], State, self(), IdleTimout,
                           {backoff, 2000, 2000, 20000}).

send_fun(Conn, Peername) ->
    Self = self(),
    fun(Packet) ->
        Data = emqttd_serializer:serialize(Packet),
        ?LOG(debug, "SEND ~p", [Data], #client_state{peername = Peername}),
        emqttd_metrics:inc('bytes/sent', iolist_size(Data)),
        try Conn:async_send(Data) of
            ok -> ok;
            true -> ok; %% Compatible with esockd 4.x
            {error, Reason} -> Self ! {shutdown, Reason}
        catch
            error:Error -> Self ! {shutdown, Error}
        end
    end.

prioritise_call(Msg, _From, _Len, _State) ->
    case Msg of info -> 10; stats -> 10; state -> 10; _ -> 5 end.

prioritise_info(Msg, _Len, _State) ->
    case Msg of {redeliver, _} -> 5; _ -> 0 end.

handle_pre_hibernate(State) ->
    {hibernate, emqttd_gc:reset_conn_gc_count(#client_state.force_gc_count, emit_stats(State))}.

handle_call(info, From, State = #client_state{proto_state = ProtoState}) ->
    ProtoInfo  = emqttd_protocol:info(ProtoState),
    ClientInfo = ?record_to_proplist(client_state, State, ?INFO_KEYS),
    {reply, Stats, _, _} = handle_call(stats, From, State),
    reply(lists:append([ClientInfo, ProtoInfo, Stats]), State);

handle_call(stats, _From, State = #client_state{proto_state = ProtoState}) ->
    reply(lists:append([emqttd_misc:proc_stats(),
                        emqttd_protocol:stats(ProtoState),
                        sock_stats(State)]), State);

handle_call(kick, _From, State) ->
    {stop, {shutdown, kick}, ok, State};

handle_call({set_rate_limit, Rl}, _From, State) ->
    reply(ok, State#client_state{rate_limit = Rl});

handle_call(get_rate_limit, _From, State = #client_state{rate_limit = Rl}) ->
    reply(Rl, State);

handle_call(session, _From, State = #client_state{proto_state = ProtoState}) ->
    reply(emqttd_protocol:session(ProtoState), State);

handle_call({clean_acl_cache, Topic}, _From, State) ->
    erase({acl, publish, Topic}),
    reply(ok, State);

handle_call(Req, _From, State) ->
    ?UNEXPECTED_REQ(Req, State).

handle_cast(Msg, State) ->
    ?UNEXPECTED_MSG(Msg, State).

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

%% Asynchronous SUBACK
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
      end, State);

handle_info({redeliver, {?PUBREL, PacketId}}, State) ->
    with_proto(
      fun(ProtoState) ->
          emqttd_protocol:pubrel(PacketId, ProtoState)
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
    {noreply, run_socket(State#client_state{conn_state = running}), hibernate};

handle_info({inet_async, _Sock, _Ref, {ok, Data}}, State) ->
    Size = iolist_size(Data),
    ?LOG(debug, "RECV ~p", [Data], State),
    emqttd_metrics:inc('bytes/received', Size),
    received(Data, rate_limit(Size, State#client_state{await_recv = false}));

handle_info({inet_async, _Sock, _Ref, {error, Reason}}, State) ->
    shutdown(Reason, State);

handle_info({inet_reply, _Sock, ok}, State) ->
    {noreply, gc(State), hibernate}; %% Tune GC

handle_info({inet_reply, _Sock, {error, Reason}}, State) ->
    shutdown(Reason, State);

handle_info({keepalive, start, Interval}, State = #client_state{connection = Conn}) ->
    ?LOG(debug, "Keepalive at the interval of ~p", [Interval], State),
    StatFun = fun() ->
                case Conn:getstat([recv_oct]) of
                    {ok, [{recv_oct, RecvOct}]} -> {ok, RecvOct};
                    {error, Error}              -> {error, Error}
                end
             end,
    case emqttd_keepalive:start(StatFun, Interval, {keepalive, check}) of
        {ok, KeepAlive} ->
            {noreply, State#client_state{keepalive = KeepAlive}, hibernate};
        {error, Error} ->
            ?LOG(warning, "Keepalive error - ~p", [Error], State),
            shutdown(Error, State)
    end;

handle_info({keepalive, check}, State = #client_state{keepalive = KeepAlive}) ->
    case emqttd_keepalive:check(KeepAlive) of
        {ok, KeepAlive1} ->
            {noreply, State#client_state{keepalive = KeepAlive1}, hibernate};
        {error, timeout} ->
            ?LOG(debug, "Keepalive timeout", [], State),
            shutdown(keepalive_timeout, State);
        {error, Error} ->
            ?LOG(warning, "Keepalive error - ~p", [Error], State),
            shutdown(Error, State)
    end;

handle_info(Info, State) ->
    ?UNEXPECTED_INFO(Info, State).

terminate(Reason, State = #client_state{connection  = Conn,
                                        keepalive   = KeepAlive,
                                        proto_state = ProtoState}) ->

    ?LOG(debug, "Terminated for ~p", [Reason], State),
    Conn:fast_close(),
    emqttd_keepalive:cancel(KeepAlive),
    case {ProtoState, Reason} of
        {undefined, _} ->
            ok;
        {_, {shutdown, Error}} ->
            emqttd_protocol:shutdown(Error, ProtoState);
        {_, Reason} ->
            emqttd_protocol:shutdown(Reason, ProtoState)
    end.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

%% Receive and Parse TCP Data
received(<<>>, State) ->
    {noreply, gc(State), hibernate};

received(Bytes, State = #client_state{parser       = Parser,
                                      packet_size  = PacketSize,
                                      proto_state  = ProtoState,
                                      idle_timeout = IdleTimeout}) ->
    case catch emqttd_parser:parse(Bytes, Parser) of
        {more, NewParser} ->
            {noreply, run_socket(State#client_state{parser = NewParser}), IdleTimeout};
        {ok, Packet, Rest} ->
            emqttd_metrics:received(Packet),
            case emqttd_protocol:received(Packet, ProtoState) of
                {ok, ProtoState1} ->
                    received(Rest, State#client_state{parser = emqttd_parser:initial_state(PacketSize),
                                                      proto_state = ProtoState1});
                {error, Error} ->
                    ?LOG(error, "Protocol error - ~p", [Error], State),
                    shutdown(Error, State);
                {error, Error, ProtoState1} ->
                    shutdown(Error, State#client_state{proto_state = ProtoState1});
                {stop, Reason, ProtoState1} ->
                    stop(Reason, State#client_state{proto_state = ProtoState1})
            end;
        {error, Error} ->
            ?LOG(error, "Framing error - ~p", [Error], State),
            shutdown(Error, State);
        {'EXIT', Reason} ->
            ?LOG(error, "Parser failed for ~p", [Reason], State),
            ?LOG(error, "Error data: ~p", [Bytes], State),
            shutdown(parser_error, State)
    end.

rate_limit(_Size, State = #client_state{rate_limit = undefined}) ->
    run_socket(State);
rate_limit(Size, State = #client_state{rate_limit = Rl}) ->
    case Rl:check(Size) of
        {0, Rl1} ->
            run_socket(State#client_state{conn_state = running, rate_limit = Rl1});
        {Pause, Rl1} ->
            ?LOG(warning, "Rate limiter pause for ~p", [Pause], State),
            erlang:send_after(Pause, self(), activate_sock),
            State#client_state{conn_state = blocked, rate_limit = Rl1}
    end.

run_socket(State = #client_state{conn_state = blocked}) ->
    State;
run_socket(State = #client_state{await_recv = true}) ->
    State;
run_socket(State = #client_state{connection = Conn}) ->
    Conn:async_recv(0, infinity),
    State#client_state{await_recv = true}.

with_proto(Fun, State = #client_state{proto_state = ProtoState}) ->
    {ok, ProtoState1} = Fun(ProtoState),
    {noreply, State#client_state{proto_state = ProtoState1}, hibernate}.

emit_stats(State = #client_state{proto_state = ProtoState}) ->
    emit_stats(emqttd_protocol:clientid(ProtoState), State).

emit_stats(_ClientId, State = #client_state{enable_stats = false}) ->
    State;
emit_stats(undefined, State) ->
    State;
emit_stats(ClientId, State) ->
    {reply, Stats, _, _} = handle_call(stats, undefined, State),
    emqttd_stats:set_client_stats(ClientId, Stats),
    State.

sock_stats(#client_state{connection = Conn}) ->
    case Conn:getstat(?SOCK_STATS) of {ok, Ss} -> Ss; {error, _} -> [] end.

reply(Reply, State) ->
    {reply, Reply, State, hibernate}.

shutdown(Reason, State) ->
    stop({shutdown, Reason}, State).

stop(Reason, State) ->
    {stop, Reason, State}.

gc(State = #client_state{connection = Conn}) ->
    Cb = fun() -> Conn:gc(), emit_stats(State) end,
    emqttd_gc:maybe_force_gc(#client_state.force_gc_count, State, Cb).

