%%%-------------------------------------------------------------------
%%% File    : emysql_recv.erl
%%% Author  : Fredrik Thulin <ft@it.su.se>
%%% Descrip.: Handles data being received on a MySQL socket. Decodes
%%%           per-row framing and sends each row to parent.
%%%
%%% Created :  4 Aug 2005 by Fredrik Thulin <ft@it.su.se>
%%%
%%% Note    : All MySQL code was written by Magnus Ahltorp, originally
%%%           in the file mysql.erl - I just moved it here.
%%%
%%% Copyright (c) 2001-2004 Kungliga Tekniska 
%%% See the file COPYING
%%%
%%%           Signals this receiver process can send to it's parent
%%%             (the parent is a mysql_conn connection handler) :
%%%
%%%             {mysql_recv, self(), data, Packet, Num}
%%%             {mysql_recv, self(), closed, {error, Reason}}
%%%             {mysql_recv, self(), closed, normal}
%%%
%%%           Internally (from inside init/4 to start_link/4) the
%%%           following signals may be sent to the parent process :
%%%
%%%             {mysql_recv, self(), init, {ok, Sock}}
%%%             {mysql_recv, self(), init, {error, E}}
%%%
%%%-------------------------------------------------------------------
-module(emysql_recv).

%%--------------------------------------------------------------------
%% External exports (should only be used by the 'mysql_conn' module)
%%--------------------------------------------------------------------
-export([start_link/2]).

%callback
-export([init/3]).

-record(state, {
		socket,
		parent,
		log_fun,
		data}).

-define(SECURE_CONNECTION, 32768).

-define(CONNECT_TIMEOUT, 10000).

%%--------------------------------------------------------------------
%% Function: start_link(Host, Port, Parent)
%%           Host = string()
%%           Port = integer()
%%           Parent = pid(), process that should get received frames
%% Descrip.: Start a process that connects to Host:Port and waits for
%%           data. When it has received a MySQL frame, it sends it to
%%           Parent and waits for the next frame.
%% Returns : {ok, RecvPid, Socket} |
%%           {error, Reason}
%%           RecvPid = pid(), receiver process pid
%%           Socket  = term(), gen_tcp socket
%%           Reason  = atom() | string()
%%--------------------------------------------------------------------
start_link(Host, Port) ->
	proc_lib:start_link(?MODULE, init, [self(), Host, Port]).

%%--------------------------------------------------------------------
%% Function: init((Host, Port, Parent)
%%           Host = string()
%%           Port = integer()
%%           Parent = pid(), process that should get received frames
%% Descrip.: Connect to Host:Port and then enter receive-loop.
%% Returns : error | never returns
%%--------------------------------------------------------------------
init(Parent, Host, Port) ->
    case gen_tcp:connect(Host, Port, [binary, {packet, 0}]) of
	{ok, Sock} ->
		proc_lib:init_ack(Parent, {ok, self(), Sock}),
		loop(#state{socket = Sock, parent = Parent, data = <<>>});
	{error, Reason} ->
		proc_lib:init_ack(Parent, {error, Reason})
	end.

%%--------------------------------------------------------------------
%% Function: loop(State)
%%           State = state record()
%% Descrip.: The main loop. Wait for data from our TCP socket and act
%%           on received data or signals that our socket was closed.
%% Returns : error | never returns
%%--------------------------------------------------------------------
loop(State) ->
    Sock = State#state.socket,
    receive
	{tcp, Sock, InData} ->
	    NewData = list_to_binary([State#state.data, InData]),
	    %% send data to parent if we have enough data
	    Rest = sendpacket(State#state.parent, NewData),
	    loop(State#state{data = Rest});
	{tcp_error, Sock, Reason} ->
	    State#state.parent ! {mysql_recv, self(), closed, {error, Reason}},
	    error;
	{tcp_closed, Sock} ->
	    State#state.parent ! {mysql_recv, self(), closed, normal},
	    error;
	_Other -> %maybe system message
		loop(State)
    end.

%%--------------------------------------------------------------------
%% Function: sendpacket(Parent, Data)
%%           Parent = pid()
%%           Data   = binary()
%% Descrip.: Check if we have received one or more complete frames by
%%           now, and if so - send them to Parent.
%% Returns : Rest = binary()
%%--------------------------------------------------------------------
%% send data to parent if we have enough data
sendpacket(Parent, Data) ->
    case Data of
	<<Length:24/little, Num:8, D/binary>> ->
	    if
		Length =< size(D) ->
		    {Packet, Rest} = split_binary(D, Length),
		    Parent ! {mysql_recv, self(), data, Packet, Num},
		    sendpacket(Parent, Rest);
		true ->
		    Data
	    end;
	_ ->
	    Data
    end.
