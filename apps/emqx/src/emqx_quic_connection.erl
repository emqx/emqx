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

-module(emqx_quic_connection).

-include("logger.hrl").
-ifndef(BUILD_WITHOUT_QUIC).
-include_lib("quicer/include/quicer.hrl").
-else.
-define(QUIC_CONNECTION_SHUTDOWN_FLAG_NONE, 0).
-endif.

-behavior(quicer_connection).

-export([
    init/1,
    new_conn/3,
    connected/3,
    transport_shutdown/3,
    shutdown/3,
    closed/3,
    local_address_changed/3,
    peer_address_changed/3,
    streams_available/3,
    peer_needs_streams/3,
    resumed/3,
    nst_received/3,
    new_stream/3
]).

-type cb_state() :: #{
    ctrl_pid := undefined | pid(),
    conn := undefined | quicer:conneciton_hanlder(),
    stream_opts := map(),
    is_resumed => boolean(),
    _ => _
}.
-type cb_ret() :: quicer_lib:cb_ret().

-spec init(map() | list()) -> {ok, cb_state()}.
init(ConnOpts) when is_list(ConnOpts) ->
    init(maps:from_list(ConnOpts));
init(#{stream_opts := SOpts} = S) when is_list(SOpts) ->
    init(S#{stream_opts := maps:from_list(SOpts)});
init(ConnOpts) when is_map(ConnOpts) ->
    {ok, init_cb_state(ConnOpts)}.

-spec closed(quicer:conneciton_hanlder(), quicer:conn_closed_props(), cb_state()) ->
    {stop, normal, cb_state()}.
closed(_Conn, #{is_peer_acked := _} = Prop, S) ->
    ?SLOG(debug, Prop),
    {stop, normal, S}.

-spec new_conn(quicer:connection_handler(), quicer:new_conn_props(), cb_state()) ->
    {ok, cb_state()} | {error, any()}.
new_conn(
    Conn,
    #{version := _Vsn} = ConnInfo,
    #{zone := Zone, conn := undefined, ctrl_pid := undefined} = S
) ->
    process_flag(trap_exit, true),
    ?SLOG(debug, ConnInfo),
    case emqx_olp:is_overloaded() andalso is_zone_olp_enabled(Zone) of
        false ->
            {ok, Pid} = emqx_connection:start_link(
                emqx_quic_stream,
                {self(), Conn, maps:without([crypto_buffer], ConnInfo)},
                S
            ),
            receive
                {Pid, stream_acceptor_ready} ->
                    ok = quicer:async_handshake(Conn),
                    {ok, S#{conn := Conn, ctrl_pid := Pid}};
                {'EXIT', _Pid, _Reason} ->
                    {error, stream_accept_error}
            end;
        true ->
            emqx_metrics:inc('olp.new_conn'),
            {error, overloaded}
    end.

-spec connected(quicer:connection_handler(), quicer:connected_props(), cb_state()) ->
    {ok, cb_state()} | {error, any()}.
connected(Conn, Props, #{slow_start := false} = S) ->
    ?SLOG(debug, Props),
    {ok, _Pid} = emqx_connection:start_link(emqx_quic_stream, Conn, S),
    {ok, S};
connected(_Conn, Props, S) ->
    ?SLOG(debug, Props),
    {ok, S}.

-spec resumed(quicer:connection_handle(), SessionData :: binary() | false, cb_state()) -> cb_ret().
resumed(Conn, Data, #{resumed_callback := ResumeFun} = S) when
    is_function(ResumeFun)
->
    ResumeFun(Conn, Data, S);
resumed(_Conn, _Data, S) ->
    {ok, S#{is_resumed := true}}.

-spec nst_received(quicer:connection_handle(), TicketBin :: binary(), cb_state()) -> cb_ret().
nst_received(_Conn, _Data, S) ->
    %% As server we should not recv NST!
    {stop, no_nst_for_server, S}.

-spec new_stream(quicer:stream_handle(), quicer:new_stream_props(), cb_state()) -> cb_ret().
new_stream(
    Stream,
    #{is_orphan := true} = Props,
    #{
        conn := Conn,
        streams := Streams,
        stream_opts := SOpts
    } = CBState
) ->
    %% Spawn new stream
    case quicer_stream:start_link(emqx_quic_stream, Stream, Conn, SOpts, Props) of
        {ok, StreamOwner} ->
            quicer_connection:handoff_stream(Stream, StreamOwner),
            {ok, CBState#{streams := [{StreamOwner, Stream} | Streams]}};
        Other ->
            Other
    end.

-spec shutdown(quicer:connection_handle(), quicer:error_code(), cb_state()) -> cb_ret().
shutdown(Conn, _ErrorCode, S) ->
    %% @TODO check spec what to do with the ErrorCode?
    quicer:async_shutdown_connection(Conn, ?QUIC_CONNECTION_SHUTDOWN_FLAG_NONE, 0),
    {ok, S}.

-spec transport_shutdown(quicer:connection_handle(), quicer:transport_shutdown_props(), cb_state()) ->
    cb_ret().
transport_shutdown(_C, _DownInfo, S) ->
    %% @TODO some counter
    {ok, S}.

-spec peer_address_changed(quicer:connection_handle(), quicer:quicer_addr(), cb_state) -> cb_ret().
peer_address_changed(_C, _NewAddr, S) ->
    {ok, S}.

-spec local_address_changed(quicer:connection_handle(), quicer:quicer_addr(), cb_state()) ->
    cb_ret().
local_address_changed(_C, _NewAddr, S) ->
    {ok, S}.

-spec streams_available(
    quicer:connection_handle(),
    {BidirStreams :: non_neg_integer(), UnidirStreams :: non_neg_integer()},
    cb_state()
) -> cb_ret().
streams_available(_C, {BidirCnt, UnidirCnt}, S) ->
    {ok, S#{
        peer_bidi_stream_count => BidirCnt,
        peer_unidi_stream_count => UnidirCnt
    }}.

-spec peer_needs_streams(quicer:connection_handle(), undefined, cb_state()) -> cb_ret().
%% @TODO this is not going to get triggered.
%% for https://github.com/microsoft/msquic/issues/3120
peer_needs_streams(_C, undefined, S) ->
    {ok, S}.

%%%
%%%  Internals
%%%
-spec is_zone_olp_enabled(emqx_types:zone()) -> boolean().
is_zone_olp_enabled(Zone) ->
    case emqx_config:get_zone_conf(Zone, [overload_protection]) of
        #{enable := true} ->
            true;
        _ ->
            false
    end.

-spec init_cb_state(map()) -> cb_state().
init_cb_state(Map) ->
    Map#{
        ctrl_pid => undefined,
        conn => undefined
    }.
