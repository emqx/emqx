%%%-----------------------------------------------------------------------------
%%% Copyright (c) 2012-2015 eMQTT.IO, All Rights Reserved.
%%%
%%% Permission is hereby granted, free of charge, to any person obtaining a copy
%%% of this software and associated documentation files (the "Software"), to deal
%%% in the Software without restriction, including without limitation the rights
%%% to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
%%% copies of the Software, and to permit persons to whom the Software is
%%% furnished to do so, subject to the following conditions:
%%%
%%% The above copyright notice and this permission notice shall be included in all
%%% copies or substantial portions of the Software.
%%%
%%% THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
%%% IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
%%% FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
%%% AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
%%% LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
%%% OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
%%% SOFTWARE.
%%%-----------------------------------------------------------------------------
%%% @doc
%%% MQTT Client
%%%
%%% @end
%%%-----------------------------------------------------------------------------

-module(emqttd_client).

-author("Feng Lee <feng@emqtt.io>").

-include("emqttd.hrl").

-include("emqttd_protocol.hrl").

%% API Function Exports
-export([start_link/2, session/1, info/1, kick/1]).

%% SUB/UNSUB Asynchronously
-export([subscribe/2, unsubscribe/2]).

-behaviour(gen_server).

%% gen_server Function Exports
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         code_change/3, terminate/2]).

%% Client State...
-record(state, {transport,
                socket,
                peername,
                conn_name,
                await_recv,
                conn_state,
                conserve,
                parser,
                proto_state,
                packet_opts,
                keepalive}).

start_link(SockArgs, MqttEnv) ->
    {ok, proc_lib:spawn_link(?MODULE, init, [[SockArgs, MqttEnv]])}.

session(CPid) ->
    gen_server:call(CPid, session, infinity).

info(CPid) ->
    gen_server:call(CPid, info, infinity).

kick(CPid) ->
    gen_server:call(CPid, kick).

subscribe(CPid, TopicTable) ->
    gen_server:cast(CPid, {subscribe, TopicTable}).

unsubscribe(CPid, Topics) ->
    gen_server:cast(CPid, {unsubscribe, Topics}).

init([SockArgs = {Transport, Sock, _SockFun}, MqttEnv]) ->
    % Transform if ssl.
    {ok, NewSock} = esockd_connection:accept(SockArgs),
    {ok, Peername} = emqttd_net:peername(Sock),
    {ok, ConnStr} = emqttd_net:connection_string(Sock, inbound),
    lager:info("Connect from ~s", [ConnStr]),
    SendFun = fun(Data) -> Transport:send(NewSock, Data) end,
    PktOpts = proplists:get_value(packet, MqttEnv),
    ProtoState = emqttd_protocol:init(Peername, SendFun, PktOpts),
    State = control_throttle(#state{transport    = Transport,
                                    socket       = NewSock,
                                    peername     = Peername,
                                    conn_name    = ConnStr,
                                    await_recv   = false,
                                    conn_state   = running,
                                    conserve     = false,
                                    packet_opts  = PktOpts,
                                    parser       = emqttd_parser:new(PktOpts),
                                    proto_state  = ProtoState}),
    ClientOpts = proplists:get_value(client, MqttEnv),
    IdleTimout = proplists:get_value(idle_timeout, ClientOpts, 10),
    gen_server:enter_loop(?MODULE, [], State, timer:seconds(IdleTimout)).

handle_call(session, _From, State = #state{proto_state = ProtoState}) ->
    {reply, emqttd_protocol:session(ProtoState), State};

handle_call(info, _From, State = #state{conn_name = ConnName,
                                        proto_state = ProtoState}) ->
    {reply, [{conn_name, ConnName} | emqttd_protocol:info(ProtoState)], State};

handle_call(kick, _From, State) ->
    {stop, {shutdown, kick}, ok, State};

handle_call(Req, _From, State = #state{peername = Peername}) ->
    lager:error("Client ~s: unexpected request - ~p", [emqttd_net:format(Peername), Req]),
    {reply, {error, unsupported_request}, State}.    

handle_cast({subscribe, TopicTable}, State) ->
    with_session(fun(SessPid) -> emqttd_session:subscribe(SessPid, TopicTable) end, State);

handle_cast({unsubscribe, Topics}, State) ->
    with_session(fun(SessPid) -> emqttd_session:unsubscribe(SessPid, Topics) end, State);

handle_cast(Msg, State = #state{peername = Peername}) ->
    lager:error("Client ~s: unexpected msg - ~p",[emqttd_net:format(Peername), Msg]),
    {noreply, State}.

handle_info(timeout, State) ->
    stop({shutdown, timeout}, State);
    
handle_info({stop, duplicate_id, _NewPid}, State=#state{proto_state = ProtoState,
                                                        conn_name   = ConnName}) ->
    lager:warning("Shutdown for duplicate clientid: ~s, conn:~s",
                  [emqttd_protocol:clientid(ProtoState), ConnName]),
    stop({shutdown, duplicate_id}, State);

handle_info({deliver, Message}, State = #state{proto_state = ProtoState}) ->
    {ok, ProtoState1} = emqttd_protocol:send(Message, ProtoState),
    noreply(State#state{proto_state = ProtoState1});

handle_info({redeliver, {?PUBREL, PacketId}},  State = #state{proto_state = ProtoState}) ->
    {ok, ProtoState1} = emqttd_protocol:redeliver({?PUBREL, PacketId}, ProtoState),
    noreply(State#state{proto_state = ProtoState1});

handle_info({inet_reply, _Ref, ok}, State) ->
    noreply(State);

handle_info({inet_async, Sock, _Ref, {ok, Data}}, State = #state{peername = Peername, socket = Sock}) ->
    lager:debug("RECV from ~s: ~p", [emqttd_net:format(Peername), Data]),
    emqttd_metrics:inc('bytes/received', size(Data)),
    received(Data, control_throttle(State #state{await_recv = false}));

handle_info({inet_async, _Sock, _Ref, {error, Reason}}, State) ->
    network_error(Reason, State);

handle_info({inet_reply, _Sock, {error, Reason}}, State = #state{peername = Peername}) ->
    lager:error("Client ~s: unexpected inet_reply '~p'", [emqttd_net:format(Peername), Reason]),
    {noreply, State};

handle_info({keepalive, start, TimeoutSec}, State = #state{transport = Transport, socket = Socket, peername = Peername}) ->
    lager:debug("Client ~s: Start KeepAlive with ~p seconds", [emqttd_net:format(Peername), TimeoutSec]),
    StatFun = fun() ->
            case Transport:getstat(Socket, [recv_oct]) of
                {ok, [{recv_oct, RecvOct}]} -> {ok, RecvOct};
                {error, Error} -> {error, Error}
            end
    end,
    KeepAlive = emqttd_keepalive:start(StatFun, TimeoutSec, {keepalive, check}),
    noreply(State#state{keepalive = KeepAlive});

handle_info({keepalive, check}, State = #state{peername = Peername, keepalive = KeepAlive}) ->
    case emqttd_keepalive:check(KeepAlive) of
    {ok, KeepAlive1} ->
        lager:debug("Client ~s: Keepalive Resumed", [emqttd_net:format(Peername)]),
        noreply(State#state{keepalive = KeepAlive1});
    {error, timeout} ->
        lager:debug("Client ~s: Keepalive Timeout!", [emqttd_net:format(Peername)]),
        stop({shutdown, keepalive_timeout}, State#state{keepalive = undefined});
    {error, Error} ->
        lager:debug("Client ~s: Keepalive Error: ~p!", [emqttd_net:format(Peername), Error]),
        stop({shutdown, keepalive_error}, State#state{keepalive = undefined})
    end;

handle_info(Info, State = #state{peername = Peername}) ->
    lager:error("Client ~s: unexpected info ~p",[emqttd_net:format(Peername), Info]),
    {noreply, State}.

terminate(Reason, #state{peername = Peername,
                         transport = Transport,
                         socket = Socket,
                         keepalive = KeepAlive,
                         proto_state = ProtoState}) ->
    lager:info("Client(~s) terminated, reason: ~p", [emqttd_net:format(Peername), Reason]),
    emqttd_keepalive:cancel(KeepAlive),
    if
        Reason == {shutdown, conn_closed} -> ok;
        true -> Transport:fast_close(Socket)
    end,
    case {ProtoState, Reason} of
        {undefined, _} -> ok;
        {_, {shutdown, Error}} -> 
            emqttd_protocol:shutdown(Error, ProtoState);
        {_,  Reason} ->
            emqttd_protocol:shutdown(Reason, ProtoState)
    end.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%=============================================================================
%%% Internal functions
%%%=============================================================================

noreply(State) ->
    {noreply, State, hibernate}.

stop(Reason, State) ->
    {stop, Reason, State}.

with_session(Fun, State = #state{proto_state = ProtoState}) ->
    Fun(emqttd_protocol:session(ProtoState)), noreply(State).

%% receive and parse tcp data
received(<<>>, State) ->
    {noreply, State, hibernate};

received(Bytes, State = #state{packet_opts = PacketOpts,
                               parser = Parser,
                               proto_state = ProtoState,
                               conn_name   = ConnStr}) ->
    case Parser(Bytes) of
    {more, NewParser} ->
        {noreply, control_throttle(State #state{parser = NewParser}), hibernate};
    {ok, Packet, Rest} ->
        received_stats(Packet),
        case emqttd_protocol:received(Packet, ProtoState) of
        {ok, ProtoState1} ->
            received(Rest, State#state{parser = emqttd_parser:new(PacketOpts),
                                       proto_state = ProtoState1});
        {error, Error} ->
            lager:error("MQTT protocol error ~p for connection ~p~n", [Error, ConnStr]),
            stop({shutdown, Error}, State);
        {error, Error, ProtoState1} ->
            stop({shutdown, Error}, State#state{proto_state = ProtoState1});
        {stop, Reason, ProtoState1} ->
            stop(Reason, State#state{proto_state = ProtoState1})
        end;
    {error, Error} ->
        lager:error("MQTT detected framing error ~p for connection ~p", [Error, ConnStr]),
        stop({shutdown, Error}, State)
    end.

network_error(Reason, State = #state{peername = Peername}) ->
    lager:warning("Client ~s: MQTT detected network error '~p'",
                    [emqttd_net:format(Peername), Reason]),
    stop({shutdown, conn_closed}, State).

run_socket(State = #state{conn_state = blocked}) ->
    State;
run_socket(State = #state{await_recv = true}) ->
    State;
run_socket(State = #state{transport = Transport, socket = Sock}) ->
    Transport:async_recv(Sock, 0, infinity),
    State#state{await_recv = true}.

control_throttle(State = #state{conn_state = Flow,
                                conserve   = Conserve}) ->
    case {Flow, Conserve} of
        {running,   true} -> State #state{conn_state = blocked};
        {blocked,  false} -> run_socket(State #state{conn_state = running});
        {_,            _} -> run_socket(State)
    end.

received_stats(?PACKET(Type)) ->
    emqttd_metrics:inc('packets/received'), inc(Type).
inc(?CONNECT) ->
    emqttd_metrics:inc('packets/connect');
inc(?PUBLISH) ->
    emqttd_metrics:inc('messages/received'),
    emqttd_metrics:inc('packets/publish/received');
inc(?SUBSCRIBE) ->
    emqttd_metrics:inc('packets/subscribe');
inc(?UNSUBSCRIBE) ->
    emqttd_metrics:inc('packets/unsubscribe');
inc(?PINGREQ) ->
    emqttd_metrics:inc('packets/pingreq');
inc(?DISCONNECT) ->
    emqttd_metrics:inc('packets/disconnect');
inc(_) ->
    ignore.

