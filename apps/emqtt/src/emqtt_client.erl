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

-export([start_link/1, info/1, go/2]).

-export([init/1,
        handle_call/3,
        handle_cast/2,
        handle_info/2,
        code_change/3,
        terminate/2]).

-include("emqtt.hrl").

%%Client State...
-record(state, {
    socket,
    peer_name,
    conn_name,
    await_recv,
    conn_state,
    conserve,
    parse_state,
    proto_state,
    keepalive
}).

start_link(Sock) ->
    gen_server:start_link(?MODULE, [Sock], []).

info(Pid) ->
    gen_server:call(Pid, info).

go(Pid, Sock) ->
    gen_server:call(Pid, {go, Sock}).

init([Sock]) ->
    {ok, #state{socket = Sock}, 1000}.

handle_call({go, Sock}, _From, #state{socket = Sock}) ->
    {ok, Peername} = emqtt_net:peer_string(Sock),
    {ok, ConnStr} = emqtt_net:connection_string(Sock, inbound),
    lager:info("Connect from ~s", [ConnStr]),
    {reply, ok, 
     control_throttle( 
       #state{ socket       = Sock, 
               peer_name    = Peername,
               conn_name    = ConnStr, 
               await_recv   = false, 
               conn_state   = running, 
               conserve     = false, 
               parse_state  = emqtt_packet:initial_state(), 
               proto_state  = emqtt_protocol:initial_state(Sock, Peername)}), 10000};

handle_call(info, _From, State = #state{
        conn_name=ConnName, proto_state = ProtoState}) ->
    {reply, [{conn_name, ConnName} | emqtt_protocol:info(ProtoState)], State};

handle_call(Req, _From, State) ->
    {stop, {badreq, Req}, State}.

handle_cast(Msg, State) ->
    {stop, {badmsg, Msg}, State}.

handle_info(timeout, State) ->
    stop({shutdown, timeout}, State);
    
handle_info({stop, duplicate_id}, State=#state{conn_name=ConnName}) ->
    %%TODO:
    %lager:error("Shutdown for duplicate clientid:~s, conn:~s", [ClientId, ConnName]), 
    stop({shutdown, duplicate_id}, State);

%%TODO: ok??
handle_info({dispatch, Message}, #state{proto_state = ProtoState} = State) ->
    {ok, ProtoState1} = emqtt_protocol:send_message(Message, ProtoState),
    {noreply, State#state{proto_state = ProtoState1}};

handle_info({inet_reply, _Ref, ok}, State) ->
    {noreply, State, hibernate};

handle_info({inet_async, Sock, _Ref, {ok, Data}}, #state{ socket = Sock}=State) ->
    process_received_bytes(
        Data, control_throttle(State #state{ await_recv = false }));

handle_info({inet_async, _Sock, _Ref, {error, Reason}}, State) ->
    network_error(Reason, State);

handle_info({inet_reply, _Sock, {error, Reason}}, State) ->
    lager:critical("unexpected inet_reply '~p'", [Reason]),
    {noreply, State};

handle_info({keepalive, start, TimeoutSec}, State = #state{socket = Socket}) ->
    lager:info("~s keepalive started: ~p", [State#state.peer_name, TimeoutSec]),
    KeepAlive = emqtt_keepalive:new(Socket, TimeoutSec, {keepalive, timeout}),
    {noreply, State#state{ keepalive = KeepAlive }};

handle_info({keepalive, timeout}, State = #state { keepalive = KeepAlive }) ->
    case emqtt_keepalive:resume(KeepAlive) of
    timeout ->
        lager:info("~s keepalive timeout!", [State#state.peer_name]),
        {stop, normal, State};
    {resumed, KeepAlive1} ->
        lager:info("~s keepalive resumed.", [State#state.peer_name]),
        {noreply, State#state{ keepalive = KeepAlive1 }}
    end;

handle_info(Info, State) ->
    lager:error("badinfo :~p",[Info]),
    {stop, {badinfo, Info}, State}.

terminate(Reason, #state{proto_state = unefined}) ->
    io:format("client terminated: ~p, reason: ~p~n", [self(), Reason]),
    %%TODO: fix keep_alive...
    %%emqtt_keep_alive:cancel(KeepAlive),
    %emqtt_protocol:client_terminated(ProtoState),
    ok;

terminate(_Reason, #state { keepalive = KeepAlive, proto_state = ProtoState }) ->
    %%TODO: fix keep_alive...
    emqtt_keepalive:cancel(KeepAlive),
    emqtt_protocol:client_terminated(ProtoState),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
    
async_recv(Sock, Length, infinity) when is_port(Sock) ->
    prim_inet:async_recv(Sock, Length, -1);

async_recv(Sock, Length, Timeout) when is_port(Sock) ->
    prim_inet:async_recv(Sock, Length, Timeout).

%-------------------------------------------------------
% receive and parse tcp data
%-------------------------------------------------------
process_received_bytes(<<>>, State) ->
    {noreply, State, hibernate};

process_received_bytes(Bytes,
                       State = #state{ parse_state = ParseState,
                                       proto_state = ProtoState,
                                       conn_name   = ConnStr }) ->
    case emqtt_packet:parse(Bytes, ParseState) of
    {more, ParseState1} ->
        {noreply,
         control_throttle( State #state{ parse_state = ParseState1 }),
         hibernate};
    {ok, Packet, Rest} ->
        case emqtt_protocol:handle_packet(Packet, ProtoState) of
        {ok, ProtoState1} ->
            process_received_bytes(
              Rest,
              State#state{ parse_state = emqtt_packet:initial_state(),
                           proto_state = ProtoState1 });
        {error, Error} ->
            lager:error("MQTT protocol error ~p for connection ~p~n", [Error, ConnStr]),
            stop({shutdown, Error}, State);
        {error, Error, ProtoState1} ->
            stop({shutdown, Error}, State#state{proto_state = ProtoState1});
        {stop, ProtoState1} ->
            stop(normal, State#state{proto_state = ProtoState1})
        end;
    {error, Error} ->
        lager:error("MQTT detected framing error ~p for connection ~p~n", [ConnStr, Error]),
        stop({shutdown, Error}, State)
    end.

%%----------------------------------------------------------------------------
network_error(Reason,
              State = #state{ conn_name  = ConnStr}) ->
    lager:error("MQTT detected network error '~p' for ~p", [Reason, ConnStr]),
    %%TODO: where to SEND WILL MSG??
    %%send_will_msg(State),
    % todo: flush channel after publish
    stop({shutdown, conn_closed}, State).

run_socket(State = #state{ conn_state = blocked }) ->
    State;
run_socket(State = #state{ await_recv = true }) ->
    State;
run_socket(State = #state{ socket = Sock }) ->
    async_recv(Sock, 0, infinity),
    State#state{ await_recv = true }.

control_throttle(State = #state{ conn_state = Flow,
                                 conserve         = Conserve }) ->
    case {Flow, Conserve} of
        {running,   true} -> State #state{ conn_state = blocked };
        {blocked,  false} -> run_socket(State #state{
                                                conn_state = running });
        {_,            _} -> run_socket(State)
    end.

stop(Reason, State ) ->
    {stop, Reason, State}.


