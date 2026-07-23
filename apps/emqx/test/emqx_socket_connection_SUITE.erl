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

t_send_congestion_times_out(_) ->
    Self = self(),
    ok = meck_esockd_socket([no_history]),
    ok = meck:new(socket, [unstick, passthrough, no_history]),
    ok = meck:expect(socket, send, fun(_Socket, IoData, [], _Handle) ->
        Data = iolist_to_binary(IoData),
        Self ! {socket, send, Data},
        {_, Ret} = erlang:process_info(Self, {dictionary, ?FUNCTION_NAME}),
        Ret
    end),
    try
        %% Init a minimal connection state:
        State0 = mk_connstate(),
        %% Simulate entering congested socket state:
        State1 = emqx_socket_connection:queue_send(make_ref(), <<"aaaaa">>, 5, State0),
        {ok, State2} = emqx_socket_connection:send(1, <<"b">>, State1),
        {ok, State3} = emqx_socket_connection:send(1, <<"c">>, State2),
        ?assertNotEqual(idle, emqx_socket_connection:info(sockstate, State3)),
        %% Simulate partially decongested socket after 1.5 seconds:
        ok = timer:sleep(1500),
        erlang:put(?FUNCTION_NAME, {select, {info1, _Rest = <<"c">>}}),
        {ok, State4} = emqx_socket_connection:handle_send_ready(sock, State3),
        ?assertReceive({socket, send, <<"aaaaabc">>}),
        ?assertNotEqual(idle, emqx_socket_connection:info(sockstate, State4)),
        %% Put more packets in the send queue:
        {ok, State5} = emqx_socket_connection:send(3, <<"ddd">>, State4),
        {ok, State6} = emqx_socket_connection:send(3, <<"eee">>, State5),
        %% Simulate totally congested socket after 1.5 seconds:
        %% This still succeeds because partial decongestion reset the deadline.
        ok = timer:sleep(1500),
        erlang:put(?FUNCTION_NAME, {select, info2}),
        {ok, State7} = emqx_socket_connection:handle_send_ready(sock, State6),
        ?assertReceive({socket, send, <<"cdddeee">>}),
        ?assertNotEqual(idle, emqx_socket_connection:info(sockstate, State7)),
        %% Queue another packet:
        {ok, State8} = emqx_socket_connection:send(5, <<"fffff">>, State7),
        %% Sending more packets after 1.5 seconds fails because of send timeout:
        ok = timer:sleep(1500),
        ?assertMatch(
            {ok, {sock_error, send_timeout}, _},
            emqx_socket_connection:send(1, <<"last">>, State8)
        )
    after
        ok = meck:unload(socket),
        ok = meck:unload(esockd_socket)
    end.

t_repeated_send_congestion_preserves_send_order(_) ->
    Self = self(),
    ok = meck_esockd_socket([no_history]),
    ok = meck:new(socket, [unstick, passthrough, no_history]),
    ok = meck:expect(socket, send, fun(_Socket, IoData, [], _Handle) ->
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
        State1 = emqx_socket_connection:queue_send(make_ref(), <<"a">>, 1, State0),
        %% Queue one more packet:
        {ok, State2} = emqx_socket_connection:send(1, <<"b">>, State1),
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

t_parse_incoming_first_packet_hints(_) ->
    ok = meck_esockd_socket([no_history]),
    ok = meck:new(emqx_frame, [passthrough, no_history, no_link]),
    try
        ?assertMatch(
            {0, 0, [], _NState},
            emqx_socket_connection:parse_incoming(<<>>, mk_connstate(channel(idle)))
        ),
        %% SUBSCRIBE with remaining_len=0 in idle state: enriched with hints.
        ?assertMatch(
            {0, 0,
                [
                    {frame_error, #{
                        cause := zero_remaining_len,
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
                mk_connstate(channel(connected))
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
