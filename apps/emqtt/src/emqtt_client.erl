%%-----------------------------------------------------------------------------
%% Copyright (c) 2012-2015, Feng Lee <feng@emqtt.io>
%% 
%% Permission is hereby granted, free of charge, to any person obtaining a copy
%% of this software and associated documentation files (the "Software"), to deal
%% in the Software without restriction, including without limitation the rights
%% to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
%% copies of the Software, and to permit persons to whom the Software is
%% furnished to do so, subject to the following conditions:
%% 
%% The above copyright notice and this permission notice shall be included in all
%% copies or substantial portions of the Software.
%% 
%% THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
%% IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
%% FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
%% AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
%% LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
%% OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
%% SOFTWARE.
%%------------------------------------------------------------------------------
-module(emqtt_client).

-author('feng@emqtt.io').

-behaviour(gen_server).

-export([start_link/1, info/1]).

-export([init/1,
        handle_call/3,
        handle_cast/2,
        handle_info/2,
        code_change/3,
        terminate/2]).

-include("emqtt.hrl").

-include("emqtt_packet.hrl").

%%Client State...
-record(state, {transport,
                socket,
                peer_name,
                conn_name,
                await_recv,
                conn_state,
                conserve,
                parse_state,
                proto_state,
                keepalive}).

start_link(SockArgs) ->
    {ok, proc_lib:spawn_link(?MODULE, init, [SockArgs])}.

%%TODO: rename?
info(Pid) ->
    gen_server:call(Pid, info).

init(SockArgs = {Transport, Sock, _SockFun}) ->
    %transform if ssl.
    {ok, NewSock} = esockd_connection:accept(SockArgs),
    {ok, Peername} = emqtt_net:peer_string(Sock),
    {ok, ConnStr} = emqtt_net:connection_string(Sock, inbound),
    lager:info("Connect from ~s", [ConnStr]),
    State = control_throttle(#state{transport    = Transport,
                                    socket       = NewSock, 
                                    peer_name    = Peername,
                                    conn_name    = ConnStr, 
                                    await_recv   = false, 
                                    conn_state   = running, 
                                    conserve     = false, 
                                    parse_state  = emqtt_parser:init(), 
                                    proto_state  = emqtt_protocol:init(Transport, NewSock, Peername)}),
    gen_server:enter_loop(?MODULE, [], State, 10000).

%%TODO: Not enough...
handle_call(info, _From, State = #state{conn_name=ConnName,
                                        proto_state = ProtoState}) ->
    {reply, [{conn_name, ConnName} | emqtt_protocol:info(ProtoState)], State};

handle_call(Req, _From, State) ->
    {stop, {badreq, Req}, State}.

handle_cast(Msg, State) ->
    {stop, {badmsg, Msg}, State}.

handle_info(timeout, State) ->
    stop({shutdown, timeout}, State);
    
handle_info({stop, duplicate_id, _NewPid}, State=#state{proto_state = ProtoState,
                                                        conn_name=ConnName}) ->
    %% TODO: to...
    %% need transfer data???
    %% emqtt_client:transfer(NewPid, Data),
    lager:error("Shutdown for duplicate clientid: ~s, conn:~s", 
        [emqtt_protocol:client_id(ProtoState), ConnName]), 
    stop({shutdown, duplicate_id}, State);

%%TODO: ok??
handle_info({dispatch, {From, Message}}, #state{proto_state = ProtoState} = State) ->
    {ok, ProtoState1} = emqtt_protocol:send({From, Message}, ProtoState),
    {noreply, State#state{proto_state = ProtoState1}};

handle_info({redeliver, {?PUBREL, PacketId}}, #state{proto_state = ProtoState} = State) ->
    {ok, ProtoState1} = emqtt_protocol:redeliver({?PUBREL, PacketId}, ProtoState),
    {noreply, State#state{proto_state = ProtoState1}};

handle_info({inet_reply, _Ref, ok}, State) ->
    {noreply, State, hibernate};

handle_info({inet_async, Sock, _Ref, {ok, Data}}, State = #state{peer_name = PeerName, socket = Sock}) ->
    lager:debug("RECV from ~s: ~p", [PeerName, Data]),
    emqtt_metrics:inc('bytes/received', size(Data)),
    process_received_bytes(Data,
                           control_throttle(State #state{await_recv = false}));

handle_info({inet_async, _Sock, _Ref, {error, Reason}}, State) ->
    network_error(Reason, State);

handle_info({inet_reply, _Sock, {error, Reason}}, State = #state{peer_name = PeerName}) ->
    lager:critical("Client ~s: unexpected inet_reply '~p'", [PeerName, Reason]),
    {noreply, State};

handle_info({keepalive, start, TimeoutSec}, State = #state{transport = Transport, socket = Socket}) ->
    lager:info("Client ~s: Start KeepAlive with ~p seconds", [State#state.peer_name, TimeoutSec]),
    KeepAlive = emqtt_keepalive:new({Transport, Socket}, TimeoutSec, {keepalive, timeout}),
    {noreply, State#state{ keepalive = KeepAlive }};

handle_info({keepalive, timeout}, State = #state{keepalive = KeepAlive}) ->
    case emqtt_keepalive:resume(KeepAlive) of
    timeout ->
        lager:info("Client ~s: Keepalive Timeout!", [State#state.peer_name]),
        stop({shutdown, keepalive_timeout}, State#state{keepalive = undefined});
    {resumed, KeepAlive1} ->
        lager:info("Client ~s: Keepalive Resumed", [State#state.peer_name]),
        {noreply, State#state{keepalive = KeepAlive1}}
    end;

handle_info(Info, State = #state{peer_name = PeerName}) ->
    lager:critical("Client ~s: unexpected info ~p",[PeerName, Info]),
    {stop, {badinfo, Info}, State}.

terminate(Reason, #state{peer_name = PeerName, keepalive = KeepAlive, proto_state = ProtoState}) ->
    lager:info("Client ~s: ~p terminated, reason: ~p~n", [PeerName, self(), Reason]),
    emqtt_keepalive:cancel(KeepAlive),
    case {ProtoState, Reason} of
        {undefined, _} -> ok;
        {_, {shutdown, Error}} -> 
            emqtt_protocol:shutdown(Error, ProtoState);
        {_, _} -> 
            ok
    end,
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
    
%-------------------------------------------------------
% receive and parse tcp data
%-------------------------------------------------------
process_received_bytes(<<>>, State) ->
    {noreply, State, hibernate};

process_received_bytes(Bytes, State = #state{parse_state = ParseState,
                                             proto_state = ProtoState,
                                             conn_name   = ConnStr}) ->
    case emqtt_parser:parse(Bytes, ParseState) of
    {more, ParseState1} ->
        {noreply,
         control_throttle(State #state{parse_state = ParseState1}),
         hibernate};
    {ok, Packet, Rest} ->
        received_stats(Packet),
        case emqtt_protocol:received(Packet, ProtoState) of
        {ok, ProtoState1} ->
            process_received_bytes(Rest, State#state{parse_state = emqtt_parser:init(),
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
        lager:error("MQTT detected framing error ~p for connection ~p~n", [ConnStr, Error]),
        stop({shutdown, Error}, State)
    end.

%%----------------------------------------------------------------------------
network_error(Reason, State = #state{peer_name = PeerName}) ->
    lager:error("Client ~s: MQTT detected network error '~p'", [PeerName, Reason]),
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

stop(Reason, State ) ->
    {stop, Reason, State}.

received_stats(?PACKET(Type)) ->
    emqtt_metrics:inc('packets/received'), 
    inc(Type).
inc(?CONNECT) ->
    emqtt_metrics:inc('packets/connect');
inc(?PUBLISH) ->
    emqtt_metrics:inc('messages/received'),
    emqtt_metrics:inc('packets/publish/received');
inc(?SUBSCRIBE) ->
    emqtt_metrics:inc('packets/subscribe');
inc(?UNSUBSCRIBE) ->
    emqtt_metrics:inc('packets/unsubscribe');
inc(?PINGREQ) ->
    emqtt_metrics:inc('packets/pingreq');
inc(?DISCONNECT) ->
    emqtt_metrics:inc('packets/disconnect');
inc(_) ->
    ignore.
    
