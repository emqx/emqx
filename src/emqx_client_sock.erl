%%%===================================================================
%%% Copyright (c) 2013-2018 EMQ Inc. All rights reserved.
%%%
%%% Licensed under the Apache License, Version 2.0 (the "License");
%%% you may not use this file except in compliance with the License.
%%% You may obtain a copy of the License at
%%%
%%%     http://www.apache.org/licenses/LICENSE-2.0
%%%
%%% Unless required by applicable law or agreed to in writing, software
%%% distributed under the License is distributed on an "AS IS" BASIS,
%%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%%% See the License for the specific language governing permissions and
%%% limitations under the License.
%%%===================================================================

-module(emqx_client_sock).

-include("emqx_client.hrl").

-export([connect/4, connect/5, send/2, close/1, stop/1]).

-export([sockname/1, setopts/2, getstat/2]).

%% Internal export
-export([receiver/2, receiver_loop/3]).

-record(ssl_socket, {tcp, ssl}).

-type(socket() :: inet:socket() | #ssl_socket{}).

-type(sockname() :: {inet:ip_address(), inet:port_number()}).

-type(option() :: gen_tcp:connect_option()
                | {ssl_options, [ssl:ssl_option()]}).

-export_type([socket/0, option/0]).

%%--------------------------------------------------------------------
%% Socket API
%%--------------------------------------------------------------------

-spec(connect(pid(), inet:ip_address() | inet:hostname(),
              inet:port_number(), [option()])
      -> {ok, socket()} | {error, term()}).
connect(ClientPid, Host, Port, SockOpts) ->
    connect(ClientPid, Host, Port, SockOpts, ?DEFAULT_CONNECT_TIMEOUT).

connect(ClientPid, Host, Port, SockOpts, Timeout) ->
    case do_connect(Host, Port, SockOpts, Timeout) of
        {ok, Sock} ->
            Receiver = spawn_link(?MODULE, receiver, [ClientPid, Sock]),
            ok = controlling_process(Sock, Receiver),
            {ok, Sock, Receiver};
        Error ->
            Error
    end.

do_connect(Host, Port, SockOpts, Timeout) ->
    TcpOpts = emqx_misc:merge_opts(?DEFAULT_TCP_OPTIONS,
                                   lists:keydelete(ssl_options, 1, SockOpts)),
    case gen_tcp:connect(Host, Port, TcpOpts, Timeout) of
        {ok, Sock} ->
            case lists:keyfind(ssl_options, 1, SockOpts) of
                {ssl_options, SslOpts} ->
                    ssl_upgrade(Sock, SslOpts, Timeout);
                false -> {ok, Sock}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

ssl_upgrade(Sock, SslOpts, Timeout) ->
    case ssl:connect(Sock, SslOpts, Timeout) of
        {ok, SslSock} ->
            {ok, #ssl_socket{tcp = Sock, ssl = SslSock}};
        {error, Reason} -> {error, Reason}
    end.

-spec(controlling_process(socket(), pid()) -> ok).
controlling_process(Sock, Pid) when is_port(Sock) ->
    gen_tcp:controlling_process(Sock, Pid);
controlling_process(#ssl_socket{ssl = SslSock}, Pid) ->
    ssl:controlling_process(SslSock, Pid).

-spec(send(socket(), iodata()) -> ok | {error, einval | closed}).
send(Sock, Data) when is_port(Sock) ->
    try erlang:port_command(Sock, Data) of
        true -> ok
    catch
        error:badarg ->
            {error, einval}
    end;
send(#ssl_socket{ssl = SslSock}, Data) ->
    ssl:send(SslSock, Data).

-spec(close(socket()) -> ok).
close(Sock) when is_port(Sock) ->
    gen_tcp:close(Sock);
close(#ssl_socket{ssl = SslSock}) ->
    ssl:close(SslSock).

-spec(stop(Receiver :: pid()) -> stop).
stop(Receiver) ->
    Receiver ! stop.

-spec(setopts(socket(), [gen_tcp:option() | ssl:socketoption()]) -> ok).
setopts(Sock, Opts) when is_port(Sock) ->
    inet:setopts(Sock, Opts);
setopts(#ssl_socket{ssl = SslSock}, Opts) ->
    ssl:setopts(SslSock, Opts).

-spec(getstat(socket(), [atom()])
      -> {ok, [{atom(), integer()}]} | {error, term()}).
getstat(Sock, Options) when is_port(Sock) ->
    inet:getstat(Sock, Options);
getstat(#ssl_socket{tcp = Sock}, Options) ->
    inet:getstat(Sock, Options).

-spec(sockname(socket()) -> {ok, sockname()} | {error, term()}).
sockname(Sock) when is_port(Sock) ->
    inet:sockname(Sock);
sockname(#ssl_socket{ssl = SslSock}) ->
    ssl:sockname(SslSock).

%%--------------------------------------------------------------------
%% Receiver
%%--------------------------------------------------------------------

receiver(ClientPid, Sock) ->
    receiver_activate(ClientPid, Sock, emqx_parser:initial_state()).

receiver_activate(ClientPid, Sock, ParseState) ->
    setopts(Sock, [{active, once}]),
    erlang:hibernate(?MODULE, receiver_loop, [ClientPid, Sock, ParseState]).

receiver_loop(ClientPid, Sock, ParseState) ->
    receive
        {TcpOrSsL, _Sock, Data} when TcpOrSsL =:= tcp;
                                     TcpOrSsL =:= ssl ->
            case parse_received_bytes(ClientPid, Data, ParseState) of
                {ok, NewParseState} ->
                    receiver_activate(ClientPid, Sock, NewParseState);
                {error, Error} ->
                    exit({frame_error, Error})
            end;
        {Error, _Sock, Reason} when Error =:= tcp_error;
                                    Error =:= ssl_error ->
            exit({Error, Reason});
        {Closed, _Sock} when Closed =:= tcp_closed;
                             Closed =:= ssl_closed ->
            exit(Closed);
        stop ->
            close(Sock)
    end.

parse_received_bytes(_ClientPid, <<>>, ParseState) ->
    {ok, ParseState};

parse_received_bytes(ClientPid, Data, ParseState) ->
    io:format("RECV Data: ~p~n", [Data]),
    case emqx_parser:parse(Data, ParseState) of
        {more, ParseState1} ->
            {ok, ParseState1};
        {ok, Packet, Rest} ->
            io:format("RECV Packet: ~p~n", [Packet]),
            gen_statem:cast(ClientPid, Packet),
            parse_received_bytes(ClientPid, Rest, emqx_parser:initial_state());
        {error, Error} ->
            {error, Error}
    end.

