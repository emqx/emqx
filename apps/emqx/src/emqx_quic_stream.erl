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

-behaviour(quicer_stream).

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
    init_handoff/4,
    new_stream/3,
    start_completed/3,
    send_complete/3,
    peer_send_shutdown/3,
    peer_send_aborted/3,
    peer_receive_aborted/3,
    send_shutdown_complete/3,
    stream_closed/3,
    peer_accepted/3,
    passive/3,
    handle_call/4
]).

-export_type([socket/0]).

-opaque socket() :: {quic, connection_handle(), stream_handle(), socket_info()}.

-type socket_info() :: #{
    is_orphan => boolean(),
    ctrl_stream_start_flags => quicer:stream_open_flags(),
    %% quicer:new_conn_props
    _ => _
}.

-spec wait({pid(), quicer:connection_handle(), socket_info()}) ->
    {ok, socket()} | {error, enotconn}.
wait({ConnOwner, Conn, ConnInfo}) ->
    {ok, Conn} = quicer:async_accept_stream(Conn, []),
    ConnOwner ! {self(), stream_acceptor_ready},
    receive
        %% New incoming stream, this is a *ctrl* stream
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

setopts(Socket, Opts) ->
    lists:foreach(
        fun
            ({Opt, V}) when is_atom(Opt) ->
                quicer:setopt(Socket, Opt, V);
            (Opt) when is_atom(Opt) ->
                quicer:setopt(Socket, Opt, true)
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

fast_close({quic, _Conn, Stream, _Info}) ->
    %% Flush send buffer, gracefully shutdown
    quicer:async_shutdown_stream(Stream),
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
    case quicer:send(Stream, Data) of
        {ok, _Len} -> ok;
        Other -> Other
    end.

%%%
%%% quicer stream callbacks
%%%

-spec init_handoff(stream_handle(), #{}, quicer:connection_handle(), #{}) -> cb_ret().
init_handoff(_Stream, _StreamOpts, _Conn, _Flags) ->
    %% stream owner already set while starts.
    {stop, unimpl}.

-spec new_stream(stream_handle(), quicer:new_stream_props(), cb_data()) -> cb_ret().
new_stream(_Stream, #{flags := _Flags, is_orphan := _IsOrphan}, _Conn) ->
    {stop, unimpl}.

-spec peer_accepted(stream_handle(), undefined, cb_data()) -> cb_ret().
peer_accepted(_Stream, undefined, S) ->
    %% We just ignore it
    {ok, S}.

-spec peer_receive_aborted(stream_handle(), non_neg_integer(), cb_data()) -> cb_ret().
peer_receive_aborted(Stream, ErrorCode, #{is_unidir := false} = S) ->
    %% we abort send with same reason
    quicer:async_shutdown_stream(Stream, ?QUIC_STREAM_SHUTDOWN_FLAG_ABORT, ErrorCode),
    {ok, S};
peer_receive_aborted(Stream, ErrorCode, #{is_unidir := true, is_local := true} = S) ->
    quicer:async_shutdown_stream(Stream, ?QUIC_STREAM_SHUTDOWN_FLAG_ABORT, ErrorCode),
    {ok, S}.

-spec peer_send_aborted(stream_handle(), non_neg_integer(), cb_data()) -> cb_ret().
peer_send_aborted(Stream, ErrorCode, #{is_unidir := false} = S) ->
    %% we abort receive with same reason
    quicer:async_shutdown_stream(Stream, ?QUIC_STREAM_SHUTDOWN_FLAG_ABORT_RECEIVE, ErrorCode),
    {ok, S};
peer_send_aborted(Stream, ErrorCode, #{is_unidir := true, is_local := false} = S) ->
    quicer:async_shutdown_stream(Stream, ?QUIC_STREAM_SHUTDOWN_FLAG_ABORT_RECEIVE, ErrorCode),
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

-spec start_completed(stream_handle(), quicer:stream_start_completed_props(), cb_data()) ->
    cb_ret().
start_completed(_Stream, #{status := success, stream_id := StreamId} = Prop, S) ->
    ?SLOG(debug, Prop),
    {ok, S#{stream_id => StreamId}};
start_completed(_Stream, #{status := stream_limit_reached, stream_id := _StreamId} = Prop, _S) ->
    ?SLOG(error, #{message => start_completed}, Prop),
    {stop, stream_limit_reached};
start_completed(_Stream, #{status := Other} = Prop, S) ->
    ?SLOG(error, Prop),
    %% or we could retry?
    {stop, {start_fail, Other}, S}.

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
passive(_Stream, undefined, _S) ->
    {stop, unimpl}.

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

handle_call(_Stream, _Request, _Opts, S) ->
    {error, unimpl, S}.

%%%
%%%  Internals
%%%
-spec socket(connection_handle(), stream_handle(), socket_info()) -> socket().
socket(Conn, CtrlStream, Info) when is_map(Info) ->
    {quic, Conn, CtrlStream, Info}.
