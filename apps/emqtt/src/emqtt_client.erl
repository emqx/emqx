%%-----------------------------------------------------------------------------
%% Copyright (c) 2014, Feng Lee <feng@slimchat.io>
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

-author('feng@slimchat.io').

-behaviour(gen_server).

-export([start_link/1, info/1, go/2]).

-export([init/1,
		handle_call/3,
		handle_cast/2,
		handle_info/2,
        code_change/3,
		terminate/2]).

-include("emqtt.hrl").

-include("emqtt_frame.hrl").

%%Client State...
-record(conn_state, {
	socket,
	conn_name,
	await_recv,
	connection_state,
	conserve,
	parse_state,
	proto_state,
	keep_alive
}).

start_link(Sock) ->
    gen_server:start_link(?MODULE, [Sock], []).

info(Pid) ->
	gen_server:call(Pid, info).

go(Pid, Sock) ->
	gen_server:call(Pid, {go, Sock}).

init([Sock]) ->
    {ok, #conn_state{socket = Sock}, hibernate}.

handle_call({go, Sock}, _From, State = #conn_state{socket = Sock}) ->
    {ok, ConnStr} = emqtt_net:connection_string(Sock, inbound),
     lager:debug("conn from ~s", [ConnStr]),
    {reply, ok, 
	 control_throttle(
	   #conn_state{ socket           = Sock,
               conn_name        = ConnStr,
               await_recv       = false,
               connection_state = running,
               conserve         = false,
               parse_state      = emqtt_frame:initial_state(),
			   proto_state		= emqtt_protocol:initial_state(Sock)})};

handle_call(info, _From, State = #conn_state{conn_name=ConnName, proto_state = ProtoState}) ->
	{reply, [{conn_name, ConnName} | emqtt_protocol:info(ProtoState)], State};

handle_call(Req, _From, State) ->
    {stop, {badreq, Req}, State}.

handle_cast(Msg, State) ->
	{stop, {badmsg, Msg}, State}.

handle_info(timeout, State) ->
	stop({shutdown, timeout}, State);
    
handle_info({stop, duplicate_id}, State=#conn_state{conn_name=ConnName}) ->
	%%TODO:
	%lager:error("Shutdown for duplicate clientid:~s, conn:~s", [ClientId, ConnName]), 
	stop({shutdown, duplicate_id}, State);

%%TODO: ok??
handle_info({dispatch, Msg}, #conn_state{proto_state = ProtoState} = State) ->
	{ok, ProtoState1} = emqtt_protocol:send_message(Msg, ProtoState),
	{noreply, State#conn_state{proto_state = ProtoState1}};

handle_info({inet_reply, _Ref, ok}, State) ->
    {noreply, State, hibernate};

handle_info({inet_async, Sock, _Ref, {ok, Data}}, #conn_state{ socket = Sock}=State) ->
    process_received_bytes(
      Data, control_throttle(State #conn_state{ await_recv = false }));

handle_info({inet_async, _Sock, _Ref, {error, Reason}}, State) ->
    network_error(Reason, State);

%%TODO: HOW TO HANDLE THIS??
handle_info({inet_reply, _Sock, {error, Reason}}, State) ->
	{noreply, State};

handle_info(keep_alive_timeout, #conn_state{keep_alive=KeepAlive}=State) ->
	case emqtt_keep_alive:state(KeepAlive) of
	idle ->
		lager:info("keep_alive timeout: ~p", [State#conn_state.conn_name]),
		{stop, normal, State};
	active ->
		KeepAlive1 = emqtt_keep_alive:reset(KeepAlive),
		{noreply, State#conn_state{keep_alive=KeepAlive1}}
	end;

handle_info(Info, State) ->
	lager:error("badinfo :~p",[Info]),
	{stop, {badinfo, Info}, State}.

terminate(_Reason, #conn_state{proto_state = unefined}) ->
	%%TODO: fix keep_alive...
	%%emqtt_keep_alive:cancel(KeepAlive),
	%emqtt_protocol:client_terminated(ProtoState),
	ok;

terminate(_Reason, #conn_state{proto_state = ProtoState}) ->
	%%TODO: fix keep_alive...
	%%emqtt_keep_alive:cancel(KeepAlive),
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
                       State = #conn_state{ parse_state = ParseState,
									   proto_state = ProtoState,
                                       conn_name   = ConnStr }) ->
    case emqtt_frame:parse(Bytes, ParseState) of
	{more, ParseState1} ->
		{noreply,
		 control_throttle( State #conn_state{ parse_state = ParseState1 }),
		 hibernate};
	{ok, Frame, Rest} ->
		case emqtt_protocol:handle_frame(Frame, ProtoState) of
		{ok, ProtoState1} ->
			process_received_bytes(
			  Rest,
			  State#conn_state{ parse_state = emqtt_frame:initial_state(),
						   proto_state = ProtoState1 });
		{error, Error} ->
			lager:error("MQTT protocol error ~p for connection ~p~n", [Error, ConnStr]),
			stop({shutdown, Error}, State);
		{error, Error, ProtoState1} ->
			stop({shutdown, Error}, State#conn_state{proto_state = ProtoState1});
		{stop, ProtoState1} ->
			stop(normal, State#conn_state{proto_state = ProtoState1})
		end;
	{error, Error} ->
		lager:error("MQTT detected framing error ~p for connection ~p~n", [ConnStr, Error]),
		stop({shutdown, Error}, State)
    end.

%%----------------------------------------------------------------------------
network_error(Reason,
              State = #conn_state{ conn_name  = ConnStr}) ->
    lager:error("MQTT detected network error '~p' for ~p", [Reason, ConnStr]),
	%%TODO: where to SEND WILL MSG??
    %%send_will_msg(State),
    % todo: flush channel after publish
    stop({shutdown, conn_closed}, State).

run_socket(State = #conn_state{ connection_state = blocked }) ->
    State;
run_socket(State = #conn_state{ await_recv = true }) ->
    State;
run_socket(State = #conn_state{ socket = Sock }) ->
    async_recv(Sock, 0, infinity),
    State#conn_state{ await_recv = true }.

control_throttle(State = #conn_state{ connection_state = Flow,
                                 conserve         = Conserve }) ->
    case {Flow, Conserve} of
        {running,   true} -> State #conn_state{ connection_state = blocked };
        {blocked,  false} -> run_socket(State #conn_state{
                                                connection_state = running });
        {_,            _} -> run_socket(State)
    end.

stop(Reason, State ) ->
    {stop, Reason, State}.

