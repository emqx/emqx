%%--------------------------------------------------------------------
%% Copyright (c) 2022-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%%
%% @doc QUIC data stream
%% Following the behaviour of emqx_connection:
%%  The MQTT packets and their side effects are handled *atomically*.
%%

-module(emqx_quic_data_stream).

-ifndef(BUILD_WITHOUT_QUIC).
-behaviour(quicer_remote_stream).

-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("quicer/include/quicer.hrl").
-include("emqx_mqtt.hrl").
-include("logger.hrl").

%% Connection Callbacks
-export([
    init_handoff/4,
    post_handoff/3,
    send_complete/3,
    peer_send_shutdown/3,
    peer_send_aborted/3,
    peer_receive_aborted/3,
    send_shutdown_complete/3,
    stream_closed/3,
    passive/3
]).

-export([handle_stream_data/4]).

%% gen_server API
-export([activate_data/2]).

-export([
    handle_call/3,
    handle_info/2,
    handle_continue/2
]).

-type cb_ret() :: quicer_stream:cb_ret().
-type cb_state() :: quicer_stream:cb_state().
-type error_code() :: quicer:error_code().
-type connection_handle() :: quicer:connection_handle().
-type stream_handle() :: quicer:stream_handle().
-type handoff_data() :: {
    emqx_frame:parse_state() | undefined,
    emqx_frame:serialize_opts() | undefined,
    emqx_channel:channel() | undefined
}.
%%
%% @doc Activate the data handling.
%%      Note, data handling is disabled before finishing the validation over control stream.
-spec activate_data(pid(), {
    emqx_frame:parse_state(), emqx_frame:serialize_opts(), emqx_channel:channel()
}) -> ok.
activate_data(StreamPid, {PS, Serialize, Channel}) ->
    gen_server:call(StreamPid, {activate, {PS, Serialize, Channel}}, infinity).

%%
%% @doc Handoff from previous owner, from the connection owner.
%%      Note, unlike control stream, there is no acceptor for data streams.
%%            The connection owner get new stream, spawn new proc and then handover to it.
%%
-spec init_handoff(stream_handle(), map(), connection_handle(), quicer:new_stream_props()) ->
    {ok, cb_state()}.
init_handoff(
    Stream,
    #{conn_shared_state := ConnSharedState} = _StreamOpts,
    Connection,
    #{is_orphan := true, flags := Flags}
) ->
    {ok, init_state(Stream, Connection, Flags, ConnSharedState)}.

%%
%% @doc Post handoff data stream
%%
-spec post_handoff(stream_handle(), handoff_data(), cb_state()) -> cb_ret().
post_handoff(_Stream, {undefined = _PS, undefined = _Serialize, undefined = _Channel}, S) ->
    %% When the channel isn't ready yet.
    %% Data stream should wait for activate call with ?MODULE:activate_data/2
    {ok, S};
post_handoff(Stream, {PS, Serialize, Channel}, S) ->
    ?tp(debug, ?FUNCTION_NAME, #{channel => Channel, serialize => Serialize}),
    _ = quicer:setopt(Stream, active, 10),
    {ok, S#{channel := Channel, serialize := Serialize, parse_state := PS}}.

-spec peer_receive_aborted(stream_handle(), error_code(), cb_state()) -> cb_ret().
peer_receive_aborted(Stream, ErrorCode, #{is_unidir := _} = S) ->
    %% we abort send with same reason
    _ = quicer:async_shutdown_stream(Stream, ?QUIC_STREAM_SHUTDOWN_FLAG_ABORT, ErrorCode),
    {ok, S}.

-spec peer_send_aborted(stream_handle(), error_code(), cb_state()) -> cb_ret().
peer_send_aborted(Stream, ErrorCode, #{is_unidir := _} = S) ->
    %% we abort receive with same reason
    _ = quicer:async_shutdown_stream(Stream, ?QUIC_STREAM_SHUTDOWN_FLAG_ABORT_RECEIVE, ErrorCode),
    {ok, S}.

-spec peer_send_shutdown(stream_handle(), undefined, cb_state()) -> cb_ret().
peer_send_shutdown(Stream, undefined, S) ->
    ok = quicer:async_shutdown_stream(Stream, ?QUIC_STREAM_SHUTDOWN_FLAG_GRACEFUL, 0),
    {ok, S}.

-spec send_complete(stream_handle(), IsCanceled :: boolean(), cb_state()) -> cb_ret().
send_complete(_Stream, false, S) ->
    {ok, S};
send_complete(_Stream, true = _IsCanceled, S) ->
    {ok, S}.

-spec send_shutdown_complete(stream_handle(), IsGraceful :: boolean(), cb_state()) -> cb_ret().
send_shutdown_complete(_Stream, _Flags, S) ->
    {ok, S}.

-spec handle_stream_data(stream_handle(), binary(), quicer:recv_data_props(), cb_state()) ->
    cb_ret().
handle_stream_data(
    _Stream,
    Bin,
    _Flags,
    #{
        is_unidir := false,
        channel := Channel,
        parse_state := PS,
        data_queue := QueuedData,
        task_queue := TQ
    } = State
) when
    %% assert get stream data only after channel is created
    Channel =/= undefined
->
    {MQTTPackets, NewPS} = parse_incoming(list_to_binary(lists:reverse([Bin | QueuedData])), PS),
    NewTQ = lists:foldl(
        fun(Item, Acc) ->
            queue:in(Item, Acc)
        end,
        TQ,
        [{incoming, P} || P <- lists:reverse(MQTTPackets)]
    ),
    {{continue, handle_appl_msg}, State#{parse_state := NewPS, task_queue := NewTQ}}.

-spec passive(stream_handle(), undefined, cb_state()) -> cb_ret().
passive(Stream, undefined, S) ->
    _ = quicer:setopt(Stream, active, 10),
    {ok, S}.

-spec stream_closed(stream_handle(), quicer:stream_closed_props(), cb_state()) -> cb_ret().
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
    {stop, normal, S}.

-spec handle_call(Request :: term(), From :: {pid(), term()}, cb_state()) -> cb_ret().
handle_call(Call, _From, S) ->
    do_handle_call(Call, S).

-spec handle_continue(Continue :: term(), cb_state()) -> cb_ret().
handle_continue(handle_appl_msg, #{task_queue := Q} = S) ->
    case queue:out(Q) of
        {{value, Item}, Q2} ->
            do_handle_appl_msg(Item, S#{task_queue := Q2});
        {empty, _Q} ->
            {ok, S}
    end.

%%% Internals
do_handle_appl_msg(
    {outgoing, Packets},
    #{
        channel := Channel,
        stream := _Stream,
        serialize := _Serialize
    } = S
) when
    Channel =/= undefined
->
    case handle_outgoing(Packets, S) of
        {ok, Size} ->
            ok = emqx_metrics:inc('bytes.sent', Size),
            {{continue, handle_appl_msg}, S};
        {error, E1, E2} ->
            {stop, {E1, E2}, S};
        {error, E} ->
            {stop, E, S}
    end;
do_handle_appl_msg(
    {incoming, #mqtt_packet{} = Packet},
    #{
        channel := Channel,
        conn_shared_state := #{cnts_ref := SharedCntsRef}
    } = S
) when
    Channel =/= undefined
->
    ok = inc_incoming_stats(Packet),
    _ = emqx_quic_connection:step_cnt(SharedCntsRef, control_packet, 1),
    with_channel(handle_in, [Packet], S);
do_handle_appl_msg({incoming, {frame_error, _} = FE}, #{channel := Channel} = S) when
    Channel =/= undefined
->
    with_channel(handle_in, [FE], S);
do_handle_appl_msg({close, Reason}, S) ->
    %% @TODO shall we abort shutdown or graceful shutdown here?
    with_channel(handle_info, [{sock_closed, Reason}], S);
do_handle_appl_msg({event, updated}, S) ->
    %% Data stream don't care about connection state changes.
    {{continue, handle_appl_msg}, S}.

handle_info(Deliver = {deliver, _, _}, S) ->
    Delivers = [Deliver],
    with_channel(handle_deliver, [Delivers], S);
handle_info({timeout, Ref, Msg}, S) ->
    with_channel(handle_timeout, [Ref, Msg], S);
handle_info(Info, State) ->
    with_channel(handle_info, [Info], State).

with_channel(Fun, Args, #{channel := Channel, task_queue := Q} = S) when
    Channel =/= undefined
->
    case apply(emqx_channel, Fun, Args ++ [Channel]) of
        ok ->
            {{continue, handle_appl_msg}, S};
        {ok, Msgs, NewChannel} when is_list(Msgs) ->
            {{continue, handle_appl_msg}, S#{
                task_queue := queue:join(Q, queue:from_list(Msgs)),
                channel := NewChannel
            }};
        {ok, Msg, NewChannel} when is_record(Msg, mqtt_packet) ->
            {{continue, handle_appl_msg}, S#{
                task_queue := queue:in({outgoing, Msg}, Q), channel := NewChannel
            }};
        %% @FIXME WTH?
        {ok, {outgoing, _} = Msg, NewChannel} ->
            {{continue, handle_appl_msg}, S#{task_queue := queue:in(Msg, Q), channel := NewChannel}};
        {ok, NewChannel} ->
            {{continue, handle_appl_msg}, S#{channel := NewChannel}};
        %% @TODO optimisation for shutdown wrap
        {shutdown, Reason, NewChannel} ->
            {stop, {shutdown, Reason}, S#{channel := NewChannel}};
        {shutdown, Reason, Msgs, NewChannel} when is_list(Msgs) ->
            %% @TODO handle outgoing?
            {stop, {shutdown, Reason}, S#{
                channel := NewChannel,
                task_queue := queue:join(Q, queue:from_list(Msgs))
            }};
        {shutdown, Reason, Msg, NewChannel} ->
            {stop, {shutdown, Reason}, S#{
                channel := NewChannel,
                task_queue := queue:in(Msg, Q)
            }}
    end.

handle_outgoing(#mqtt_packet{} = P, S) ->
    handle_outgoing([P], S);
handle_outgoing(Packets, #{serialize := Serialize, stream := Stream, is_unidir := false}) when
    is_list(Packets)
->
    OutBin = [serialize_packet(P, Serialize) || P <- filter_disallowed_out(Packets)],
    %% Send data async but still want send feedback via {quic, send_complete, ...}
    Res = quicer:async_send(Stream, OutBin, ?QUICER_SEND_FLAG_SYNC),
    ?TRACE("MQTT", "mqtt_packet_sent", #{packets => Packets}),
    [ok = inc_outgoing_stats(P) || P <- Packets],
    Res.

serialize_packet(Packet, Serialize) ->
    try emqx_frame:serialize_pkt(Packet, Serialize) of
        <<>> ->
            ?SLOG(warning, #{
                msg => "packet_is_discarded",
                reason => "frame_is_too_large",
                packet => emqx_packet:format(Packet, hidden)
            }),
            ok = emqx_metrics:inc('delivery.dropped.too_large'),
            ok = emqx_metrics:inc('delivery.dropped'),
            ok = inc_outgoing_stats({error, message_too_large}),
            <<>>;
        Data ->
            Data
    catch
        %% Maybe Never happen.
        throw:{?FRAME_SERIALIZE_ERROR, Reason} ->
            ?SLOG(info, #{
                reason => Reason,
                input_packet => Packet
            }),
            erlang:error({?FRAME_SERIALIZE_ERROR, Reason});
        error:Reason:Stacktrace ->
            ?SLOG(error, #{
                input_packet => Packet,
                exception => Reason,
                stacktrace => Stacktrace
            }),
            erlang:error(?FRAME_SERIALIZE_ERROR)
    end.

-spec init_state(
    quicer:stream_handle(),
    quicer:connection_handle(),
    non_neg_integer(),
    map()
) ->
    % @TODO
    map().
init_state(Stream, Connection, OpenFlags, ConnSharedState) ->
    init_state(Stream, Connection, OpenFlags, ConnSharedState, undefined).

init_state(Stream, Connection, OpenFlags, ConnSharedState, PS) ->
    %% quic stream handle
    #{
        stream => Stream,
        %% quic connection handle
        conn => Connection,
        %% if it is QUIC unidi stream
        is_unidir => quicer:is_unidirectional(OpenFlags),
        %% Frame Parse State
        parse_state => PS,
        %% Peer Stream handle in a pair for type unidir only
        peer_stream => undefined,
        %% if the stream is locally initiated.
        is_local => false,
        %% queue binary data when is NOT connected, in reversed order.
        data_queue => [],
        %% Channel from connection
        %% `undefined' means the connection is not connected.
        channel => undefined,
        %% serialize opts for connection
        serialize => undefined,
        %% Current working queue
        task_queue => queue:new(),
        %% Connection Shared State
        conn_shared_state => ConnSharedState
    }.

-spec do_handle_call(term(), cb_state()) -> cb_ret().
do_handle_call(
    {activate, {PS, Serialize, Channel}},
    #{
        channel := undefined,
        stream := Stream,
        serialize := undefined
    } = S
) ->
    NewS = S#{channel := Channel, serialize := Serialize, parse_state := PS},
    %% We use quic protocol for flow control, and we don't check return val
    case quicer:setopt(Stream, active, true) of
        ok ->
            {reply, ok, NewS};
        {error, E} ->
            ?SLOG(error, #{msg => "set_stream_active_failed", error => E}),
            {stop, E, NewS}
    end;
do_handle_call(_Call, _S) ->
    {error, unimpl}.

%% @doc return reserved order of Packets
parse_incoming(Data, PS) ->
    try
        do_parse_incoming(Data, [], PS)
    catch
        throw:{?FRAME_PARSE_ERROR, Reason} ->
            ?SLOG(info, #{
                msg => "frame_parse_error",
                reason => Reason,
                input_bytes => Data
            }),
            {[{frame_error, Reason}], PS};
        error:Reason:Stacktrace ->
            ?SLOG(error, #{
                msg => "frame_parse_failed",
                input_bytes => Data,
                reason => Reason,
                stacktrace => Stacktrace
            }),
            {[{frame_error, Reason}], PS}
    end.

do_parse_incoming(<<>>, Packets, ParseState) ->
    {Packets, ParseState};
do_parse_incoming(Data, Packets, ParseState) ->
    case emqx_frame:parse(Data, ParseState) of
        {more, NParseState} ->
            {Packets, NParseState};
        {Packet, Rest, NParseState} ->
            do_parse_incoming(Rest, [Packet | Packets], NParseState)
    end.

%% followings are copied from emqx_connection
-compile({inline, [inc_incoming_stats/1]}).
inc_incoming_stats(Packet = ?PACKET(Type)) ->
    inc_counter(recv_pkt, 1),
    case Type =:= ?PUBLISH of
        true ->
            inc_counter(recv_msg, 1),
            inc_qos_stats(recv_msg, Packet),
            inc_counter(incoming_pubs, 1);
        false ->
            ok
    end,
    emqx_metrics:inc_recv(Packet).

-compile({inline, [inc_outgoing_stats/1]}).
inc_outgoing_stats({error, message_too_large}) ->
    inc_counter('send_msg.dropped', 1),
    inc_counter('send_msg.dropped.too_large', 1);
inc_outgoing_stats(Packet = ?PACKET(Type)) ->
    inc_counter(send_pkt, 1),
    case Type of
        ?PUBLISH ->
            inc_counter(send_msg, 1),
            inc_counter(outgoing_pubs, 1),
            inc_qos_stats(send_msg, Packet);
        _ ->
            ok
    end,
    emqx_metrics:inc_sent(Packet).

inc_counter(Key, Inc) ->
    _ = emqx_pd:inc_counter(Key, Inc),
    ok.

inc_qos_stats(Type, Packet) ->
    case inc_qos_stats_key(Type, emqx_packet:qos(Packet)) of
        undefined ->
            ignore;
        Key ->
            inc_counter(Key, 1)
    end.

inc_qos_stats_key(send_msg, ?QOS_0) -> 'send_msg.qos0';
inc_qos_stats_key(send_msg, ?QOS_1) -> 'send_msg.qos1';
inc_qos_stats_key(send_msg, ?QOS_2) -> 'send_msg.qos2';
inc_qos_stats_key(recv_msg, ?QOS_0) -> 'recv_msg.qos0';
inc_qos_stats_key(recv_msg, ?QOS_1) -> 'recv_msg.qos1';
inc_qos_stats_key(recv_msg, ?QOS_2) -> 'recv_msg.qos2';
%% for bad qos
inc_qos_stats_key(_, _) -> undefined.

filter_disallowed_out(Packets) ->
    lists:filter(fun is_datastream_out_pkt/1, Packets).

is_datastream_out_pkt(#mqtt_packet{header = #mqtt_packet_header{type = Type}}) when
    Type > 2 andalso Type < 12
->
    true;
is_datastream_out_pkt(_) ->
    false.
%% BUILD_WITHOUT_QUIC
-else.
-endif.
