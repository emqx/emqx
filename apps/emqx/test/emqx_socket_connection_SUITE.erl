%%--------------------------------------------------------------------
%% Copyright (c) 2018-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_socket_connection_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("emqx/include/asserts.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("eunit/include/eunit.hrl").

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [
            {emqx,
                "listeners.tcp.default.tcp_backend = socket\n"
                "listeners.tcp.default.tcp_options.active_n = 10\n"
                "listeners.tcp.default.tcp_options.high_watermark = 5\n"
                "listeners.tcp.default.tcp_options.send_timeout = 2s\n"}
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    emqx_cth_suite:stop(proplists:get_value(apps, Config)).

init_per_testcase(_Case, Config) ->
    ok = snabbkaffe:start_trace(),
    Config.

end_per_testcase(_Case, _Config) ->
    snabbkaffe:stop().

t_send_congestion_times_out(_) ->
    Self = self(),
    ok = meck_esockd_socket([no_history]),
    ok = meck:new(socket, [unstick, passthrough, no_history]),
    ok = meck:expect(socket, sendv, fun(_Socket, IoData, _Handle) ->
        Data = iolist_to_binary(IoData),
        Self ! {socket, send, Data},
        {_, Ret} = erlang:process_info(Self, {dictionary, ?FUNCTION_NAME}),
        Ret
    end),
    try
        %% Init a minimal connection state:
        State0 = mk_connstate(),
        %% Simulate entering congested socket state:
        State1 = emqx_socket_connection:queue_send(make_ref(), [<<"aaaaa">>], 5, State0),
        {ok, State2} = emqx_socket_connection:send(1, [<<"b">>], State1),
        {ok, State3} = emqx_socket_connection:send(1, [<<"c">>], State2),
        ?assertNotEqual(idle, emqx_socket_connection:info(sockstate, State3)),
        %% Simulate partially decongested socket after 1.5 seconds:
        ok = timer:sleep(1500),
        erlang:put(?FUNCTION_NAME, {select, {info1, _Rest = [<<"c">>]}}),
        {ok, State4} = emqx_socket_connection:handle_send_ready(sock, State3),
        ?assertReceive({socket, send, <<"aaaaabc">>}),
        ?assertNotEqual(idle, emqx_socket_connection:info(sockstate, State4)),
        %% Put more packets in the send queue:
        {ok, State5} = emqx_socket_connection:send(3, [<<"ddd">>], State4),
        {ok, State6} = emqx_socket_connection:send(3, [<<"eee">>], State5),
        %% Simulate totally congested socket after 1.5 seconds:
        %% This still succeeds because partial decongestion reset the deadline.
        ok = timer:sleep(1500),
        erlang:put(?FUNCTION_NAME, {select, info2}),
        {ok, State7} = emqx_socket_connection:handle_send_ready(sock, State6),
        ?assertReceive({socket, send, <<"cdddeee">>}),
        ?assertNotEqual(idle, emqx_socket_connection:info(sockstate, State7)),
        %% Queue another packet:
        {ok, State8} = emqx_socket_connection:send(5, [<<"fffff">>], State7),
        %% Sending more packets after 1.5 seconds fails because of send timeout:
        ok = timer:sleep(1500),
        ?assertMatch(
            {ok, {sock_error, send_timeout}, _},
            emqx_socket_connection:send(1, [<<"last">>], State8)
        )
    after
        ok = meck:unload(socket),
        ok = meck:unload(esockd_socket)
    end.

t_repeated_send_congestion_preserves_send_order(_) ->
    Self = self(),
    ok = meck_esockd_socket([no_history]),
    ok = meck:new(socket, [unstick, passthrough, no_history]),
    ok = meck:expect(socket, sendv, fun(_Socket, IoData, _Handle) ->
        Self ! {socket, send, iolist_to_binary(IoData)},
        case get(?FUNCTION_NAME) of
            undefined ->
                put(?FUNCTION_NAME, selected_once),
                {select, for_test};
            selected_once ->
                ok
        end
    end),
    try
        %% Init a minimal connection state:
        State0 = mk_connstate(),
        %% Simulate entering congested socket state:
        State1 = emqx_socket_connection:queue_send(make_ref(), [<<"a">>], 1, State0),
        %% Queue one more packet:
        {ok, State2} = emqx_socket_connection:send(1, [<<"b">>], State1),
        %% Simulate socket getting ready, it goes back unready w/o sending anything:
        {ok, State3} = emqx_socket_connection:handle_send_ready(sock, State2),
        ?assertReceive({socket, send, <<"ab">>}),
        %% Simulate socket getting ready again:
        %% Both time we expect the same iodata fed into the socket.
        {ok, State4} = emqx_socket_connection:handle_send_ready(sock, State3),
        ?assertReceive({socket, send, <<"ab">>}),
        %% Verify it got decongested:
        ?assertEqual(idle, emqx_socket_connection:info(sockstate, State4))
    after
        erase(?FUNCTION_NAME),
        ok = meck:unload(socket),
        ok = meck:unload(esockd_socket)
    end.

t_data_ready_handles_rearmed_select(_) ->
    ok = meck_esockd_socket([no_history]),
    ok = meck:new(socket, [unstick, passthrough, no_history]),
    SelectInfo = {select_info, recv, make_ref()},
    ok = meck:expect(socket, recv, fun(sock, 0, [], nowait) ->
        {select, SelectInfo}
    end),
    try
        State0 = emqx_socket_connection:init_state(sock, #{
            zone => default,
            limiter => undefined,
            listener => {tcp, default}
        }),
        ?assertMatch(
            {ok, _},
            emqx_socket_connection:handle_msg({'$socket', sock, select, make_ref()}, State0)
        )
    after
        ok = meck:unload(socket),
        ok = meck:unload(esockd_socket)
    end.

-doc """
The channel info map that `info/1` returns, which the connection stores in the
channel info table, has no `conninfo.sock`. The channel keeps the socket.
""".
t_info_omits_sock(_) ->
    ok = meck_esockd_socket([no_history]),
    try
        State = mk_connstate(),
        #{conninfo := ConnInfo} = emqx_socket_connection:info(State),
        ?assertMatch(#{socktype := tcp, peername := {{127, 0, 0, 1}, 3456}}, ConnInfo),
        ?assertNot(maps:is_key(sock, ConnInfo)),
        ?assertMatch(#{sock := sock}, emqx_socket_connection:info({channel, conninfo}, State))
    after
        meck:unload(esockd_socket)
    end.

-doc """
Allocation policy `always` retains the configured 1 MiB receive buffer while the
connection waits for CONNECT.
""".
t_always_buffer_retained(_) ->
    with_buffer_alloc_policy(?FUNCTION_NAME, ~"1MB", always, fun(_Sock, Pid) ->
        ?retry(
            50,
            20,
            ?assertMatch(
                #{{'<', 1048576} := 0, {'<', 2097152} := 1, infinity := 0},
                drv_buffer_histogram(Pid)
            )
        )
    end).

-doc """
Allocation policy `packet` releases the configured 1 MiB receive buffer while the
connection waits for CONNECT.
""".
t_packet_buffer_released_before_connect(_) ->
    with_buffer_alloc_policy(?FUNCTION_NAME, ~"1MB", packet, fun(_Sock, Pid) ->
        ?retry(
            50,
            20,
            ?assertMatch(
                #{{'<', 1048576} := 0, {'<', 2097152} := 0, infinity := 0},
                drv_buffer_histogram(Pid)
            )
        )
    end).

-doc """
Allocation policy `packet` releases large receive buffers after PUBACK, before
the client sends another packet.
""".
t_packet_buffer_released_after_puback(_) ->
    with_buffer_alloc_policy(?FUNCTION_NAME, ~"1MB", packet, fun(Sock, Pid) ->
        Connect = ?CONNECT_PACKET(#mqtt_packet_connect{clientid = atom_to_binary(?FUNCTION_NAME)}),
        ok = gen_tcp:send(Sock, emqx_frame:serialize(Connect)),
        ?assertMatch(?CONNACK_PACKET(?CONNACK_ACCEPT), recv_packet(Sock, 4, 1000)),
        Payload = binary:copy(<<"x">>, 256 * 1024),
        Publish = ?PUBLISH_PACKET(?QOS_1, <<"t/buffer_alloc">>, 1, Payload),
        ok = gen_tcp:send(Sock, emqx_frame:serialize(Publish)),
        ?assertMatch(?PUBACK_PACKET(1), recv_packet(Sock, 4, 1000)),
        #{socket := ServerSocket} = emqx_socket_connection:get_state(Pid),
        true = erlang:garbage_collect(Pid),
        ct:pal("Receive buffer: ~p", [socket:getopt(ServerSocket, {otp, rcvbuf})]),
        ct:pal("Process binaries: ~p", [erlang:process_info(Pid, binary)]),
        ?retry(
            50,
            20,
            ?assertMatch(
                #{{'<', 1048576} := 0, {'<', 2097152} := 0, infinity := 0},
                drv_buffer_histogram(Pid)
            )
        )
    end).

-doc """
Allocation policy `packet` releases large receive buffers after PINGREQ,
while the MQTT connection remains open.
""".
t_packet_buffer_released_after_pingresp(_) ->
    with_buffer_alloc_policy(?FUNCTION_NAME, ~"1MB", packet, fun(Sock, Pid) ->
        Connect = ?CONNECT_PACKET(#mqtt_packet_connect{clientid = atom_to_binary(?FUNCTION_NAME)}),
        ok = gen_tcp:send(Sock, emqx_frame:serialize(Connect)),
        ?assertMatch(?CONNACK_PACKET(?CONNACK_ACCEPT), recv_packet(Sock, 4, 1000)),
        ok = gen_tcp:send(Sock, emqx_frame:serialize(?PACKET(?PINGREQ))),
        PingResp = iolist_to_binary(emqx_frame:serialize(?PACKET(?PINGRESP))),
        ?assertEqual({ok, PingResp}, gen_tcp:recv(Sock, byte_size(PingResp), 1000)),
        #{socket := ServerSocket} = emqx_socket_connection:get_state(Pid),
        true = erlang:garbage_collect(Pid),
        ct:pal("Receive buffer: ~p", [socket:getopt(ServerSocket, {otp, rcvbuf})]),
        ct:pal("Process binaries: ~p", [erlang:process_info(Pid, binary)]),
        ?retry(
            50,
            20,
            ?assertMatch(
                #{{'<', 1048576} := 0, {'<', 2097152} := 0, infinity := 0},
                drv_buffer_histogram(Pid)
            )
        )
    end).

with_buffer_alloc_policy(Name, Buffer, Policy, Check) ->
    Port = emqx_common_test_helpers:select_free_port(tcp),
    Path = [listeners, tcp, Name],
    {ok, _} = emqx:update_config(
        Path,
        {create, #{
            <<"bind">> => iolist_to_binary(["127.0.0.1:", integer_to_list(Port)]),
            <<"tcp_backend">> => <<"socket">>,
            <<"tcp_options">> => #{<<"buffer">> => Buffer},
            <<"socket_options">> => #{<<"buffer_alloc">> => atom_to_binary(Policy)}
        }}
    ),
    try
        {{ok, Sock}, {ok, #{?snk_meta := #{pid := Pid}}}} = ?wait_async_action(
            gen_tcp:connect("127.0.0.1", Port, [binary, {active, false}, {nodelay, true}]),
            #{?snk_kind := connection_started, listener := {tcp, Name}}
        ),
        _ = sys:get_state(Pid),
        Check(Sock, Pid)
    after
        {ok, _} = emqx:remove_config(Path)
    end.

drv_buffer_histogram(Pid) ->
    Width = 4,
    {ok, {Start, 0, Allocations}} = instrument:allocations(#{
        allocator_types => [binary_alloc],
        flags => [per_process],
        histogram_start => (1024 * 1024) div 2,
        histogram_width => Width
    }),
    maybe
        true ?= is_process_alive(Pid),
        Binaries = maps:with([binary, drv_binary], maps:get(Pid, Allocations, #{})),
        Boundaries =
            [{'<', Start bsl N} || N <- lists:seq(0, Width - 2)] ++ [infinity],
        Counts = lists:foldl(
            fun(Hist, Acc) -> lists:zipwith(fun erlang:'+'/2, tuple_to_list(Hist), Acc) end,
            lists:duplicate(Width, 0),
            maps:values(Binaries)
        ),
        maps:from_list(lists:zip(Boundaries, Counts))
    else
        false -> dead
    end.

-doc """
PUBLISH packets smaller than, equal to and larger than the 4 KiB receive buffer
arrive complete and in order. The publisher sends them one packet per write,
all in one write, and each packet split over several writes.
""".
t_recv_packet_sizes(_) ->
    Topic = <<"t/recv_packet_sizes">>,
    {ok, Sub} = emqtt:start_link([{clientid, <<"recv_packet_sizes_sub">>}]),
    {ok, _} = emqtt:connect(Sub),
    {ok, _, [?QOS_0]} = emqtt:subscribe(Sub, Topic, ?QOS_0),
    {ok, Sock} = gen_tcp:connect("127.0.0.1", 1883, [binary, {active, false}, {nodelay, true}]),
    Connect = ?CONNECT_PACKET(#mqtt_packet_connect{clientid = <<"recv_packet_sizes_pub">>}),
    ok = gen_tcp:send(Sock, emqx_frame:serialize(Connect)),
    ?assertMatch(?CONNACK_PACKET(?CONNACK_ACCEPT), recv_packet(Sock, 4, 5000)),
    Sizes = [1, 1000, 4090, 4096, 4100, 6144, 16384, 20480, 65536, 262144],
    Payloads = [crypto:strong_rand_bytes(Size) || Size <- Sizes],
    Packets = [
        iolist_to_binary(emqx_frame:serialize(?PUBLISH_PACKET(?QOS_0, Topic, undefined, P)))
     || P <- Payloads
    ],
    lists:foreach(fun(Packet) -> ok = gen_tcp:send(Sock, Packet) end, Packets),
    ok = gen_tcp:send(Sock, Packets),
    lists:foreach(fun(Packet) -> send_in_chunks(Sock, Packet, 1500) end, Packets),
    Expected = Payloads ++ Payloads ++ Payloads,
    Received = receive_payloads(length(Expected)),
    ?assertEqual([byte_size(P) || P <- Expected], [byte_size(P) || P <- Received]),
    ?assert(Expected =:= Received),
    ?assertNotReceive({publish, _}).

recv_packet(Sock, Size, Timeout) ->
    {ok, Data} = gen_tcp:recv(Sock, Size, Timeout),
    {Packet, <<>>, _ParseState} = emqx_frame:parse(Data),
    Packet.

receive_payloads(0) ->
    [];
receive_payloads(N) ->
    receive
        {publish, #{payload := Payload}} ->
            [Payload | receive_payloads(N - 1)]
    after 10000 ->
        []
    end.

send_in_chunks(Sock, Bin, Size) when byte_size(Bin) > Size ->
    <<Chunk:Size/binary, Rest/binary>> = Bin,
    ok = gen_tcp:send(Sock, Chunk),
    ok = timer:sleep(1),
    send_in_chunks(Sock, Rest, Size);
send_in_chunks(Sock, Bin, _Size) ->
    ok = gen_tcp:send(Sock, Bin).

t_parse_incoming_first_packet_hints(_) ->
    ok = meck_esockd_socket([no_history]),
    ok = meck:new(emqx_frame, [passthrough, no_history, no_link]),
    try
        ?assertMatch(
            {0, 0, [], _NState},
            emqx_socket_connection:parse_incoming(<<>>, mk_connstate(channel(idle)))
        ),
        %% SUBSCRIBE as the first packet: rejected before CONNECT, enriched with hints.
        ?assertMatch(
            {0, 0,
                [
                    {frame_error, #{
                        cause := unexpected_packet_before_connect,
                        header_type := 'SUBSCRIBE',
                        packet_type := 'SUBSCRIBE',
                        resemble_protocol := _
                    }}
                ],
                _NState},
            emqx_socket_connection:parse_incoming(<<16#82, 16#00>>, mk_connstate(channel(idle)))
        ),
        %% CONNECT with remaining_len=0 in idle state.
        ?assertMatch(
            {0, 0, [{frame_error, #{cause := zero_remaining_len}}], _NState},
            emqx_socket_connection:parse_incoming(<<16#10, 16#00>>, mk_connstate(channel(idle)))
        ),
        %% bad_subqos in connected state: no enrichment.
        ?assertMatch(
            {0, 0, [{frame_error, bad_subqos}], _NState},
            emqx_socket_connection:parse_incoming(
                <<?SUBSCRIBE:4, 2:4, 16#06, 16#00, 16#01, 16#00, 16#01, $t, 16#03>>,
                connstate_after_connect(channel(connected))
            )
        ),
        ok = meck:expect(emqx_frame, parse, fun(_, _) ->
            erlang:error(forced_parse_error)
        end),
        ?assertMatch(
            {0, 0, [{frame_error, forced_parse_error}], _NState},
            emqx_socket_connection:parse_incoming(
                <<"for_testing">>,
                mk_connstate(channel(connected))
            )
        )
    after
        ok = meck:unload(emqx_frame),
        ok = meck:unload(esockd_socket)
    end.

mk_connstate() ->
    emqx_socket_connection:init_state(sock, #{
        zone => default,
        limiter => undefined,
        listener => {tcp, default}
    }).

mk_connstate(Channel) ->
    emqx_socket_connection:set_field(channel, Channel, mk_connstate()).

%% Connection state whose parser has already accepted a CONNECT, so that the
%% packets after it are parsed as usual.
connstate_after_connect(Channel) ->
    Connect = iolist_to_binary(emqx_frame:serialize(?CONNECT_PACKET(#mqtt_packet_connect{}))),
    {0, 1, [_], State} =
        emqx_socket_connection:parse_incoming(Connect, mk_connstate(Channel)),
    State.

channel(ConnState) ->
    ConnInfo = #{
        socktype => tcp,
        peername => {{127, 0, 0, 1}, 3456},
        sockname => {{127, 0, 0, 1}, 1883},
        peercert => undefined,
        peersni => undefined,
        conn_mod => emqx_socket_connection,
        sock => sock
    },
    Channel0 = emqx_channel:init(ConnInfo, #{
        zone => default,
        limiter => undefined,
        listener => {tcp, default}
    }),
    emqx_channel:set_field(conn_state, ConnState, Channel0).

meck_esockd_socket(Opts) ->
    ok = meck:new(esockd_socket, [passthrough | Opts]),
    ok = meck:expect(esockd_socket, type, fun(_) -> tcp end),
    ok = meck:expect(esockd_socket, peername, fun(_) -> {ok, {{127, 0, 0, 1}, 3456}} end),
    ok = meck:expect(esockd_socket, sockname, fun(_) -> {ok, {{127, 0, 0, 1}, 1883}} end),
    ok = meck:expect(esockd_socket, peercert, fun(_) -> undefined end),
    ok = meck:expect(esockd_socket, peersni, fun(_) -> undefined end).
