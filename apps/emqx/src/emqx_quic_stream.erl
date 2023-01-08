%%--------------------------------------------------------------------
%% Copyright (c) 2021-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% MQTT/QUIC Stream
-module(emqx_quic_stream).

-behaviour(quicer_remote_stream).

%% emqx transport Callbacks
-export([
    type/1,
    wait/1,
    getstat/2,
    fast_close/1,
    ensure_ok_or_exit/2,
    async_send/3,
    setopts/2,
    getopts/2,
    peername/1,
    sockname/1,
    peercert/1
]).

-include("logger.hrl").
-ifndef(BUILD_WITHOUT_QUIC).
-include_lib("quicer/include/quicer.hrl").
-else.
%% STREAM SHUTDOWN FLAGS
-define(QUIC_STREAM_SHUTDOWN_FLAG_NONE, 0).
% Cleanly closes the send path.
-define(QUIC_STREAM_SHUTDOWN_FLAG_GRACEFUL, 1).
% Abruptly closes the send path.
-define(QUIC_STREAM_SHUTDOWN_FLAG_ABORT_SEND, 2).
% Abruptly closes the receive path.
-define(QUIC_STREAM_SHUTDOWN_FLAG_ABORT_RECEIVE, 4).
% Abruptly closes both send and receive paths.
-define(QUIC_STREAM_SHUTDOWN_FLAG_ABORT, 6).
-define(QUIC_STREAM_SHUTDOWN_FLAG_IMMEDIATE, 8).
-endif.

-type cb_ret() :: gen_statem:event_handler_result().
-type cb_data() :: emqtt_quic:cb_data().
-type connection_handle() :: quicer:connection_handle().
-type stream_handle() :: quicer:stream_handle().

-export([
    new_stream/3,
    send_complete/3,
    peer_send_shutdown/3,
    peer_send_aborted/3,
    peer_receive_aborted/3,
    send_shutdown_complete/3,
    stream_closed/3,
    passive/3
]).

-export_type([socket/0]).

-opaque socket() :: {quic, connection_handle(), stream_handle(), socket_info()}.

-type socket_info() :: #{
    is_orphan => boolean(),
    ctrl_stream_start_flags => quicer:stream_open_flags(),
    %% and quicer:new_conn_props()
    _ => _
}.

%% for accepting
-spec wait
    ({pid(), connection_handle(), socket_info()}) ->
        {ok, socket()} | {error, enotconn};
    %% For handover
    ({pid(), connection_handle(), stream_handle(), socket_info()}) ->
        {ok, socket()} | {error, any()}.

%%% For Accepting New Remote Stream
wait({ConnOwner, Conn, ConnInfo}) ->
    {ok, Conn} = quicer:async_accept_stream(Conn, []),
    ConnOwner ! {self(), stream_acceptor_ready},
    receive
        %% New incoming stream, this is a *control* stream
        {quic, new_stream, Stream, #{is_orphan := IsOrphan, flags := StartFlags}} ->
            SocketInfo = ConnInfo#{
                is_orphan => IsOrphan,
                ctrl_stream_start_flags => StartFlags
            },
            {ok, socket(Conn, Stream, SocketInfo)};
        %% connection closed event for stream acceptor
        {quic, closed, undefined, undefined} ->
            {error, enotconn};
        %% Connection owner process down
        {'EXIT', ConnOwner, _Reason} ->
            {error, enotconn}
    end.
%% UNUSED, for ownership handover,
%% wait({PrevOwner, Conn, Stream, SocketInfo}) ->
%%     case quicer:wait_for_handoff(PrevOwner, Stream) of
%%         ok ->
%%             {ok, socket(Conn, Stream, SocketInfo)};
%%         owner_down ->
%%             {error, owner_down}
%%     end.

type(_) ->
    quic.

peername({quic, Conn, _Stream, _Info}) ->
    quicer:peername(Conn).

sockname({quic, Conn, _Stream, _Info}) ->
    quicer:sockname(Conn).

peercert(_S) ->
    %% @todo but unsupported by msquic
    nossl.

getstat({quic, Conn, _Stream, _Info}, Stats) ->
    case quicer:getstat(Conn, Stats) of
        {error, _} -> {error, closed};
        Res -> Res
    end.

setopts({quic, _Conn, Stream, _Info}, Opts) ->
    lists:foreach(
        fun
            ({Opt, V}) when is_atom(Opt) ->
                quicer:setopt(Stream, Opt, V);
            (Opt) when is_atom(Opt) ->
                quicer:setopt(Stream, Opt, true)
        end,
        Opts
    ),
    ok.

getopts(_Socket, _Opts) ->
    %% @todo
    {ok, [
        {high_watermark, 0},
        {high_msgq_watermark, 0},
        {sndbuf, 0},
        {recbuf, 0},
        {buffer, 80000}
    ]}.

%% @TODO supply some App Error Code
fast_close({ConnOwner, Conn, _ConnInfo}) when is_pid(ConnOwner) ->
    %% handshake aborted.
    quicer:async_shutdown_connection(Conn, ?QUIC_CONNECTION_SHUTDOWN_FLAG_NONE, 0),
    ok;
fast_close({quic, _Conn, Stream, _Info}) ->
    %% Force flush
    quicer:async_shutdown_stream(Stream),
    %% @FIXME Since we shutdown the control stream, we shutdown the connection as well
    %% *BUT* Msquic does not flush the send buffer if we shutdown the connection after
    %% gracefully shutdown the stream.
    % quicer:async_shutdown_connection(Conn, ?QUIC_CONNECTION_SHUTDOWN_FLAG_NONE, 0),
    ok.

-spec ensure_ok_or_exit(atom(), list(term())) -> term().
ensure_ok_or_exit(Fun, Args = [Sock | _]) when is_atom(Fun), is_list(Args) ->
    case erlang:apply(?MODULE, Fun, Args) of
        {error, Reason} when Reason =:= enotconn; Reason =:= closed ->
            fast_close(Sock),
            exit(normal);
        {error, Reason} ->
            fast_close(Sock),
            exit({shutdown, Reason});
        Result ->
            Result
    end.

async_send({quic, _Conn, Stream, _Info}, Data, _Options) ->
    case quicer:async_send(Stream, Data, ?QUICER_SEND_FLAG_SYNC) of
        {ok, _Len} -> ok;
        Other -> Other
    end.

%%%
%%% quicer stream callbacks
%%%
-spec new_stream(stream_handle(), quicer:new_stream_props(), cb_data()) -> cb_ret().
new_stream(_Stream, #{flags := _Flags, is_orphan := _IsOrphan}, _Conn) ->
    {stop, unimpl}.

-spec peer_receive_aborted(stream_handle(), non_neg_integer(), cb_data()) -> cb_ret().
peer_receive_aborted(Stream, ErrorCode, S) ->
    quicer:async_shutdown_stream(Stream, ?QUIC_STREAM_SHUTDOWN_FLAG_ABORT, ErrorCode),
    {ok, S}.

-spec peer_send_aborted(stream_handle(), non_neg_integer(), cb_data()) -> cb_ret().
peer_send_aborted(Stream, ErrorCode, S) ->
    %% we abort receive with same reason
    quicer:async_shutdown_stream(Stream, ?QUIC_STREAM_SHUTDOWN_FLAG_ABORT, ErrorCode),
    {ok, S}.

-spec peer_send_shutdown(stream_handle(), undefined, cb_data()) -> cb_ret().
peer_send_shutdown(Stream, undefined, S) ->
    ok = quicer:async_shutdown_stream(Stream, ?QUIC_STREAM_SHUTDOWN_FLAG_GRACEFUL, 0),
    {ok, S}.

-spec send_complete(stream_handle(), boolean(), cb_data()) -> cb_ret().
send_complete(_Stream, false, S) ->
    {ok, S};
send_complete(_Stream, true = _IsCancelled, S) ->
    ?SLOG(error, #{message => "send cancelled"}),
    {ok, S}.

-spec send_shutdown_complete(stream_handle(), boolean(), cb_data()) -> cb_ret().
send_shutdown_complete(_Stream, _IsGraceful, S) ->
    {ok, S}.

%% Local stream, Unidir
%% -spec handle_stream_data(stream_handle(), binary(), quicer:recv_data_props(), cb_data())
%%                         -> cb_ret().
%% handle_stream_data(Stream, Bin, Flags, #{ is_local := true
%%                                         , parse_state := PS} = S) ->
%%     ?SLOG(debug, #{data => Bin}, Flags),
%%     case parse(Bin, PS, []) of
%%         {keep_state, NewPS, Packets} ->
%%             quicer:setopt(Stream, active, once),
%%             {keep_state, S#{parse_state := NewPS},
%%              [{next_event, cast, P } || P <- lists:reverse(Packets)]};
%%         {stop, _} = Stop ->
%%             Stop
%%     end;
%% %% Remote stream
%% handle_stream_data(_Stream, _Bin, _Flags,
%%                    #{is_local := false, is_unidir := true, conn := _Conn} = _S) ->
%%     {stop, unimpl}.

-spec passive(stream_handle(), undefined, cb_data()) -> cb_ret().
passive(Stream, undefined, S) ->
    quicer:setopt(Stream, active, 10),
    {ok, S}.

-spec stream_closed(stream_handle(), quicer:stream_closed_props(), cb_data()) -> cb_ret().
stream_closed(
    _Stream,
    #{
        is_conn_shutdown := IsConnShutdown,
        is_app_closing := IsAppClosing,
        is_shutdown_by_app := IsAppShutdown,
        is_closed_remotely := IsRemote,
        status := Status,
        error := Code
    },
    S
) when
    is_boolean(IsConnShutdown) andalso
        is_boolean(IsAppClosing) andalso
        is_boolean(IsAppShutdown) andalso
        is_boolean(IsRemote) andalso
        is_atom(Status) andalso
        is_integer(Code)
->
    %% @TODO for now we fake a sock_closed for
    %% emqx_connection:process_msg to append
    %% a msg to be processed
    {ok, {sock_closed, Status}, S}.

%%%
%%%  Internals
%%%
-spec socket(connection_handle(), stream_handle(), socket_info()) -> socket().
socket(Conn, CtrlStream, Info) when is_map(Info) ->
    {quic, Conn, CtrlStream, Info}.
