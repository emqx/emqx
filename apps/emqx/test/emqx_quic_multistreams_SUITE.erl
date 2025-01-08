%%--------------------------------------------------------------------
%% Copyright (c) 2021-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(emqx_quic_multistreams_SUITE).

-ifndef(BUILD_WITHOUT_QUIC).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("quicer/include/quicer.hrl").
-include_lib("emqx/include/emqx_cm.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

suite() ->
    [{timetrap, {seconds, 30}}].

all() ->
    [
        {group, mstream},
        {group, shutdown},
        {group, misc},
        t_listener_with_lowlevel_settings,
        t_listener_inval_settings
    ].

groups() ->
    [
        {mstream, [], [{group, profiles}]},

        {profiles, [], [
            {group, profile_low_latency},
            {group, profile_max_throughput}
        ]},
        {profile_low_latency, [], [
            {group, pub_qos0},
            {group, pub_qos1},
            {group, pub_qos2}
        ]},
        {profile_max_throughput, [], [
            {group, pub_qos0},
            {group, pub_qos1},
            {group, pub_qos2}
        ]},
        {pub_qos0, [], [
            {group, sub_qos0},
            {group, sub_qos1},
            {group, sub_qos2}
        ]},
        {pub_qos1, [], [
            {group, sub_qos0},
            {group, sub_qos1},
            {group, sub_qos2}
        ]},
        {pub_qos2, [], [
            {group, sub_qos0},
            {group, sub_qos1},
            {group, sub_qos2}
        ]},
        {sub_qos0, [{group, qos}]},
        {sub_qos1, [{group, qos}]},
        {sub_qos2, [{group, qos}]},
        {qos, [
            t_multi_streams_sub,
            t_multi_streams_pub_5x100,
            t_multi_streams_pub_parallel,
            t_multi_streams_pub_parallel_no_blocking,
            t_multi_streams_sub_pub_async,
            t_multi_streams_sub_pub_sync,
            t_multi_streams_unsub,
            t_multi_streams_corr_topic,
            t_multi_streams_unsub_via_other,
            t_multi_streams_dup_sub,
            t_multi_streams_packet_boundary,
            t_multi_streams_packet_malform,
            t_multi_streams_kill_sub_stream,
            t_multi_streams_packet_too_large,
            t_multi_streams_sub_0_rtt,
            t_multi_streams_sub_0_rtt_large_payload,
            t_multi_streams_sub_0_rtt_stream_data_cont,
            t_conn_change_client_addr
        ]},

        {shutdown, [
            {group, graceful_shutdown},
            {group, abort_recv_shutdown},
            {group, abort_send_shutdown},
            {group, abort_send_recv_shutdown}
        ]},

        {graceful_shutdown, [
            {group, ctrl_stream_shutdown},
            {group, data_stream_shutdown}
        ]},
        {abort_recv_shutdown, [
            {group, ctrl_stream_shutdown},
            {group, data_stream_shutdown}
        ]},
        {abort_send_shutdown, [
            {group, ctrl_stream_shutdown},
            {group, data_stream_shutdown}
        ]},
        {abort_send_recv_shutdown, [
            {group, ctrl_stream_shutdown},
            {group, data_stream_shutdown}
        ]},

        {ctrl_stream_shutdown, [
            t_multi_streams_shutdown_ctrl_stream,
            t_multi_streams_shutdown_ctrl_stream_then_reconnect,
            t_multi_streams_remote_shutdown,
            t_multi_streams_emqx_ctrl_kill,
            t_multi_streams_emqx_ctrl_exit_normal,
            t_multi_streams_remote_shutdown_with_reconnect
        ]},

        {data_stream_shutdown, [
            t_multi_streams_shutdown_pub_data_stream,
            t_multi_streams_shutdown_sub_data_stream
        ]},
        {misc, [
            t_conn_silent_close,
            t_client_conn_bump_streams,
            t_olp_true,
            t_olp_reject,
            t_conn_resume,
            t_conn_without_ctrl_stream
        ]}
    ].

init_per_suite(Config) ->
    Apps = start_emqx(Config),
    [{port, 14567}, {pub_qos, 0}, {sub_qos, 0}, {apps, Apps} | Config].

end_per_suite(Config) ->
    emqx_cth_suite:stop(?config(apps, Config)).

start_emqx(Config) ->
    emqx_cth_suite:start(
        [mk_emqx_spec()],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ).

stop_emqx(Config) ->
    emqx_cth_suite:stop(?config(apps, Config)).

restart_emqx(Config) ->
    ok = stop_emqx(Config),
    emqx_cth_suite:start(
        [mk_emqx_spec()],
        #{work_dir => emqx_cth_suite:work_dir(Config), boot_type => restart}
    ).

mk_emqx_spec() ->
    {emqx,
        %% Turn off force_shutdown policy.
        "force_shutdown.enable = false"
        "\n listeners.quic.default {"
        "\n   enable = true, bind = 14567, acceptors = 16, idle_timeout_ms = 15000"
        "\n }"}.

init_per_group(pub_qos0, Config) ->
    [{pub_qos, 0} | Config];
init_per_group(sub_qos0, Config) ->
    [{sub_qos, 0} | Config];
init_per_group(pub_qos1, Config) ->
    [{pub_qos, 1} | Config];
init_per_group(sub_qos1, Config) ->
    [{sub_qos, 1} | Config];
init_per_group(pub_qos2, Config) ->
    [{pub_qos, 2} | Config];
init_per_group(sub_qos2, Config) ->
    [{sub_qos, 2} | Config];
init_per_group(abort_send_shutdown, Config) ->
    [{stream_shutdown_flag, ?QUIC_STREAM_SHUTDOWN_FLAG_ABORT_SEND} | Config];
init_per_group(abort_recv_shutdown, Config) ->
    [{stream_shutdown_flag, ?QUIC_STREAM_SHUTDOWN_FLAG_ABORT_RECEIVE} | Config];
init_per_group(abort_send_recv_shutdown, Config) ->
    [{stream_shutdown_flag, ?QUIC_STREAM_SHUTDOWN_FLAG_ABORT} | Config];
init_per_group(graceful_shutdown, Config) ->
    [{stream_shutdown_flag, ?QUIC_STREAM_SHUTDOWN_FLAG_GRACEFUL} | Config];
init_per_group(profile_max_throughput, Config) ->
    quicer:reg_open(quic_execution_profile_type_max_throughput),
    Config;
init_per_group(profile_low_latency, Config) ->
    quicer:reg_open(quic_execution_profile_low_latency),
    Config;
init_per_group(_, Config) ->
    Config.

end_per_group(_, Config) ->
    Config.

t_quic_sock(Config) ->
    Port = 4567,
    SslOpts = [
        {cert, certfile(Config)},
        {key, keyfile(Config)},
        {idle_timeout_ms, 10000},
        % QUIC_SERVER_RESUME_AND_ZERORTT
        {server_resumption_level, 2},
        {peer_bidi_stream_count, 10},
        {alpn, ["mqtt"]}
    ],
    Server = quic_server:start_link(Port, SslOpts),
    timer:sleep(500),
    {ok, Sock} = emqtt_quic:connect(
        "localhost",
        Port,
        [{alpn, ["mqtt"]}, {active, false}],
        3000
    ),
    send_and_recv_with(Sock),
    ok = emqtt_quic:close(Sock),
    quic_server:stop(Server).

t_quic_sock_fail(_Config) ->
    Port = 4567,
    Error1 =
        {error,
            {transport_down, #{
                error => 2,
                status => connection_refused
            }}},
    Error2 = {error, {transport_down, #{error => 1, status => unreachable}}},
    case
        emqtt_quic:connect(
            "localhost",
            Port,
            [{alpn, ["mqtt"]}, {active, false}],
            3000
        )
    of
        Error1 ->
            ok;
        Error2 ->
            ok;
        Other ->
            ct:fail("unexpected return ~p", [Other])
    end.

t_0_rtt(Config) ->
    Port = 4568,
    SslOpts = [
        {cert, certfile(Config)},
        {key, keyfile(Config)},
        {idle_timeout_ms, 10000},
        % QUIC_SERVER_RESUME_AND_ZERORTT
        {server_resumption_level, 2},
        {peer_bidi_stream_count, 10},
        {alpn, ["mqtt"]}
    ],
    Server = quic_server:start_link(Port, SslOpts),
    timer:sleep(500),
    {ok, {quic, Conn, _Stream} = Sock} = emqtt_quic:connect(
        "localhost",
        Port,
        [
            {alpn, ["mqtt"]},
            {active, false},
            {quic_event_mask, 1}
        ],
        3000
    ),
    send_and_recv_with(Sock),
    ok = emqtt_quic:close(Sock),
    NST =
        receive
            {quic, nst_received, Conn, Ticket} ->
                Ticket
        end,
    {ok, Sock2} = emqtt_quic:connect(
        "localhost",
        Port,
        [
            {alpn, ["mqtt"]},
            {active, false},
            {nst, NST}
        ],
        3000
    ),
    send_and_recv_with(Sock2),
    ok = emqtt_quic:close(Sock2),
    quic_server:stop(Server).

t_0_rtt_fail(Config) ->
    Port = 4569,
    SslOpts = [
        {cert, certfile(Config)},
        {key, keyfile(Config)},
        {idle_timeout_ms, 10000},
        % QUIC_SERVER_RESUME_AND_ZERORTT
        {server_resumption_level, 2},
        {peer_bidi_stream_count, 10},
        {alpn, ["mqtt"]}
    ],
    Server = quic_server:start_link(Port, SslOpts),
    timer:sleep(500),
    {ok, {quic, Conn, _Stream} = Sock} = emqtt_quic:connect(
        "localhost",
        Port,
        [
            {alpn, ["mqtt"]},
            {active, false},
            {quic_event_mask, 1}
        ],
        3000
    ),
    send_and_recv_with(Sock),
    ok = emqtt_quic:close(Sock),
    <<_Head:16, Left/binary>> =
        receive
            {quic, nst_received, Conn, Ticket} when is_binary(Ticket) ->
                Ticket
        end,

    Error = {error, {not_found, invalid_parameter}},
    Error = emqtt_quic:connect(
        "localhost",
        Port,
        [
            {alpn, ["mqtt"]},
            {active, false},
            {nst, Left}
        ],
        3000
    ),
    quic_server:stop(Server).

t_multi_streams_sub(Config) ->
    PubQos = ?config(pub_qos, Config),
    SubQos = ?config(sub_qos, Config),
    RecQos = calc_qos(PubQos, SubQos),
    Topic = atom_to_binary(?FUNCTION_NAME),
    {ok, C} = emqtt:start_link([{proto_ver, v5} | Config]),
    {ok, _} = emqtt:quic_connect(C),
    {ok, _, [SubQos]} = emqtt:subscribe_via(C, {new_data_stream, []}, #{}, [
        {Topic, [{qos, SubQos}]}
    ]),
    case emqtt:publish(C, Topic, <<"qos 2 1">>, PubQos) of
        ok when PubQos == 0 -> ok;
        {ok, _} -> ok
    end,
    receive
        {publish, #{
            client_pid := C,
            payload := <<"qos 2 1">>,
            qos := RecQos,
            topic := Topic
        }} ->
            ok;
        Other ->
            ct:fail("unexpected recv ~p", [Other])
    after 100 ->
        ct:fail("not received")
    end,
    ok = emqtt:disconnect(C).

t_multi_streams_pub_5x100(Config) ->
    PubQos = ?config(pub_qos, Config),
    SubQos = ?config(sub_qos, Config),
    Topic = atom_to_binary(?FUNCTION_NAME),
    {ok, C} = emqtt:start_link([{proto_ver, v5} | Config]),
    {ok, _} = emqtt:quic_connect(C),
    {ok, _, [SubQos]} = emqtt:subscribe_via(C, {new_data_stream, []}, #{}, [
        {Topic, [{qos, SubQos}]}
    ]),

    PubVias = lists:map(
        fun(_N) ->
            {ok, Via} = emqtt:start_data_stream(C, []),
            Via
        end,
        lists:seq(1, 5)
    ),
    CtrlVia = proplists:get_value(socket, emqtt:info(C)),
    [
        begin
            case emqtt:publish_via(C, PVia, Topic, #{}, <<"stream data ", N>>, [{qos, PubQos}]) of
                ok when PubQos == 0 -> ok;
                {ok, _} -> ok
            end,
            0 == (N rem 10) andalso timer:sleep(10)
        end
     || %% also publish on control stream
        N <- lists:seq(1, 100),
        PVia <- [CtrlVia | PubVias]
    ],
    ?assert(timeout =/= recv_pub(600)),
    ok = emqtt:disconnect(C).

t_multi_streams_pub_parallel(Config) ->
    PubQos = ?config(pub_qos, Config),
    SubQos = ?config(sub_qos, Config),
    RecQos = calc_qos(PubQos, SubQos),
    PktId1 = calc_pkt_id(RecQos, 1),
    PktId2 = calc_pkt_id(RecQos, 2),
    Topic = atom_to_binary(?FUNCTION_NAME),
    {ok, C} = emqtt:start_link([{proto_ver, v5} | Config]),
    {ok, _} = emqtt:quic_connect(C),
    {ok, _, [SubQos]} = emqtt:subscribe(C, #{}, [{Topic, [{qos, SubQos}]}]),
    ok = emqtt:publish_async(
        C,
        {new_data_stream, []},
        Topic,
        <<"stream data 1">>,
        [{qos, PubQos}],
        undefined
    ),
    ok = emqtt:publish_async(
        C,
        {new_data_stream, []},
        Topic,
        <<"stream data 2">>,
        [{qos, PubQos}],
        undefined
    ),
    PubRecvs = recv_pub(2),
    ?assertMatch(
        [
            {publish, #{
                client_pid := C,
                packet_id := PktId1,
                payload := <<"stream data", _/binary>>,
                qos := RecQos,
                topic := Topic
            }},
            {publish, #{
                client_pid := C,
                packet_id := PktId2,
                payload := <<"stream data", _/binary>>,
                qos := RecQos,
                topic := Topic
            }}
        ],
        PubRecvs
    ),
    Payloads = [P || {publish, #{payload := P}} <- PubRecvs],
    ?assert(
        [<<"stream data 1">>, <<"stream data 2">>] == Payloads orelse
            [<<"stream data 2">>, <<"stream data 1">>] == Payloads
    ),
    ok = emqtt:disconnect(C).

%% @doc test two pub streams, one send incomplete MQTT packet() can not block another.
t_multi_streams_pub_parallel_no_blocking(Config) ->
    PubQos = ?config(pub_qos, Config),
    SubQos = ?config(sub_qos, Config),
    RecQos = calc_qos(PubQos, SubQos),
    PktId2 = calc_pkt_id(RecQos, 1),
    Topic = atom_to_binary(?FUNCTION_NAME),
    {ok, C} = emqtt:start_link([{proto_ver, v5} | Config]),
    {ok, _} = emqtt:quic_connect(C),
    {ok, _, [SubQos]} = emqtt:subscribe(C, #{}, [{Topic, [{qos, SubQos}]}]),
    Drop = <<"stream data 1">>,
    meck:new(emqtt_quic, [passthrough, no_history]),
    meck:expect(emqtt_quic, send, fun(Sock, IoList) ->
        case lists:last(IoList) == Drop of
            true ->
                ct:pal("meck droping ~p", [Drop]),
                meck:passthrough([Sock, IoList -- [Drop]]);
            false ->
                meck:passthrough([Sock, IoList])
        end
    end),
    ok = emqtt:publish_async(
        C,
        {new_data_stream, []},
        Topic,
        Drop,
        [{qos, PubQos}],
        undefined
    ),
    ok = emqtt:publish_async(
        C,
        {new_data_stream, []},
        Topic,
        <<"stream data 2">>,
        [{qos, PubQos}],
        undefined
    ),
    PubRecvs = recv_pub(1),
    ?assertMatch(
        [
            {publish, #{
                client_pid := C,
                packet_id := PktId2,
                payload := <<"stream data 2">>,
                qos := RecQos,
                topic := Topic
            }}
        ],
        PubRecvs
    ),
    meck:unload(emqtt_quic),
    ?assertEqual(timeout, recv_pub(1)),
    ok = emqtt:disconnect(C).

t_multi_streams_packet_boundary(Config) ->
    PubQos = ?config(pub_qos, Config),
    SubQos = ?config(sub_qos, Config),
    RecQos = calc_qos(PubQos, SubQos),
    PktId1 = calc_pkt_id(RecQos, 1),
    PktId2 = calc_pkt_id(RecQos, 2),
    PktId3 = calc_pkt_id(RecQos, 3),
    Topic = atom_to_binary(?FUNCTION_NAME),

    %% make quicer to batch job
    quicer:reg_open(quic_execution_profile_type_max_throughput),

    {ok, C} = emqtt:start_link([{proto_ver, v5} | Config]),
    {ok, _} = emqtt:quic_connect(C),
    {ok, _, [SubQos]} = emqtt:subscribe(C, #{}, [{Topic, [{qos, SubQos}]}]),

    {ok, PubVia} = emqtt:start_data_stream(C, []),
    ok = emqtt:publish_async(
        C,
        PubVia,
        Topic,
        <<"stream data 1">>,
        [{qos, PubQos}],
        undefined
    ),
    ok = emqtt:publish_async(
        C,
        PubVia,
        Topic,
        <<"stream data 2">>,
        [{qos, PubQos}],
        undefined
    ),
    ThisFunB = atom_to_binary(?FUNCTION_NAME),
    LargePart3 = iolist_to_binary([
        <<N:64, ThisFunB/binary>>
     || N <- lists:seq(1, 20000)
    ]),
    ok = emqtt:publish_async(
        C,
        PubVia,
        Topic,
        LargePart3,
        [{qos, PubQos}],
        undefined
    ),
    PubRecvs = recv_pub(3, [], 1000),
    ?assertMatch(
        [
            {publish, #{
                client_pid := C,
                packet_id := PktId1,
                payload := <<"stream data 1">>,
                qos := RecQos,
                topic := Topic
            }},
            {publish, #{
                client_pid := C,
                packet_id := PktId2,
                payload := <<"stream data 2">>,
                qos := RecQos,
                topic := Topic
            }},
            {publish, #{
                client_pid := C,
                packet_id := PktId3,
                payload := _LargePart3_TO_BE_CHECKED,
                qos := RecQos,
                topic := Topic
            }}
        ],
        PubRecvs
    ),
    {publish, #{payload := LargePart3Recv}} = lists:last(PubRecvs),
    CommonLen = binary:longest_common_prefix([LargePart3Recv, LargePart3]),
    Size3 = byte_size(LargePart3),
    case Size3 - CommonLen of
        0 ->
            ok;
        Left ->
            ct:fail(
                "unmatched large payload: offset: ~p ~n send: ~p ~n recv ~p",
                [
                    CommonLen,
                    binary:part(LargePart3, {CommonLen, Left}),
                    binary:part(LargePart3Recv, {CommonLen, Left})
                ]
            )
    end,
    ok = emqtt:disconnect(C).

%% @doc test that one malformed stream will not close the entire connection
t_multi_streams_packet_malform(Config) ->
    PubQos = ?config(pub_qos, Config),
    SubQos = ?config(sub_qos, Config),
    RecQos = calc_qos(PubQos, SubQos),
    PktId1 = calc_pkt_id(RecQos, 1),
    PktId2 = calc_pkt_id(RecQos, 2),
    PktId3 = calc_pkt_id(RecQos, 3),
    Topic = atom_to_binary(?FUNCTION_NAME),

    %% make quicer to batch job
    quicer:reg_open(quic_execution_profile_type_max_throughput),

    {ok, C} = emqtt:start_link([{proto_ver, v5} | Config]),
    {ok, _} = emqtt:quic_connect(C),
    {ok, _, [SubQos]} = emqtt:subscribe(C, #{}, [{Topic, [{qos, SubQos}]}]),

    {ok, PubVia} = emqtt:start_data_stream(C, []),
    ok = emqtt:publish_async(
        C,
        PubVia,
        Topic,
        <<"stream data 1">>,
        [{qos, PubQos}],
        undefined
    ),

    {ok, {quic, _Conn, MalformStream}} = emqtt:start_data_stream(C, []),
    {ok, _} = quicer:send(MalformStream, <<0, 0, 0, 0, 0, 0, 0, 0, 0, 0>>),

    ok = emqtt:publish_async(
        C,
        PubVia,
        Topic,
        <<"stream data 2">>,
        [{qos, PubQos}],
        undefined
    ),
    LargePart3 = binary:copy(atom_to_binary(?FUNCTION_NAME), 2000),
    ok = emqtt:publish_async(
        C,
        PubVia,
        Topic,
        LargePart3,
        [{qos, PubQos}],
        undefined
    ),
    PubRecvs = recv_pub(3),
    ?assertMatch(
        [
            {publish, #{
                client_pid := C,
                packet_id := PktId1,
                payload := <<"stream data 1">>,
                qos := RecQos,
                topic := Topic
            }},
            {publish, #{
                client_pid := C,
                packet_id := PktId2,
                payload := <<"stream data 2">>,
                qos := RecQos,
                topic := Topic
            }},
            {publish, #{
                client_pid := C,
                packet_id := PktId3,
                payload := LargePart3,
                qos := RecQos,
                topic := Topic
            }}
        ],
        PubRecvs
    ),

    case quicer:send(MalformStream, <<0, 0, 0, 0, 0, 0, 0, 0, 0, 0>>) of
        {ok, 10} -> ok;
        {error, cancelled} -> ok;
        {error, stm_send_error, aborted} -> ok;
        {error, closed} -> ok
    end,

    ?assert(is_list(emqtt:info(C))),
    {error, closed} =
        snabbkaffe:retry(
            10000,
            10,
            fun() ->
                {error, closed} = quicer:send(
                    MalformStream, <<1, 2, 3, 4, 5, 6, 7, 8, 9, 0>>
                )
            end
        ),
    ?assert(is_list(emqtt:info(C))),

    ok = emqtt:disconnect(C).

t_multi_streams_packet_too_large(Config) ->
    PubQos = ?config(pub_qos, Config),
    SubQos = ?config(sub_qos, Config),
    Topic = atom_to_binary(?FUNCTION_NAME),
    RecQos = calc_qos(PubQos, SubQos),
    PktId1 = calc_pkt_id(RecQos, 1),
    PktId2 = calc_pkt_id(RecQos, 2),
    PktId3 = calc_pkt_id(RecQos, 3),

    OldMax = emqx_config:get_zone_conf(default, [mqtt, max_packet_size]),
    emqx_config:put_zone_conf(default, [mqtt, max_packet_size], 1000),

    {ok, C} = emqtt:start_link([{proto_ver, v5} | Config]),
    {ok, _} = emqtt:quic_connect(C),
    {ok, _, [SubQos]} = emqtt:subscribe(C, #{}, [{Topic, [{qos, SubQos}]}]),

    {ok, PubVia} = emqtt:start_data_stream(C, []),
    ok = emqtt:publish_async(
        C,
        PubVia,
        Topic,
        <<"stream data 1">>,
        [{qos, PubQos}],
        undefined
    ),

    ok = emqtt:publish_async(
        C,
        PubVia,
        Topic,
        <<"stream data 2">>,
        [{qos, PubQos}],
        undefined
    ),

    PubRecvs = recv_pub(2),
    ?assertMatch(
        [
            {publish, #{
                client_pid := C,
                packet_id := PktId1,
                payload := <<"stream data 1">>,
                qos := RecQos,
                topic := Topic
            }},
            {publish, #{
                client_pid := C,
                packet_id := PktId2,
                payload := <<"stream data 2">>,
                qos := RecQos,
                topic := Topic
            }}
        ],
        PubRecvs
    ),

    {ok, PubVia2} = emqtt:start_data_stream(C, []),
    ok = emqtt:publish_async(
        C,
        PubVia2,
        Topic,
        binary:copy(<<"too large">>, 200),
        [{qos, PubQos}],
        undefined
    ),
    ?assert(is_list(emqtt:info(C))),

    timeout = recv_pub(1),

    %% send large payload on stream 1
    ok = emqtt:publish_async(
        C,
        PubVia,
        Topic,
        binary:copy(<<"too large">>, 200),
        [{qos, PubQos}],
        undefined
    ),
    timeout = recv_pub(1),
    ?assert(is_list(emqtt:info(C))),

    %% Connection could be kept but data stream are closed!
    {error, closed} = quicer:send(via_stream(PubVia), <<1>>),
    {error, closed} = quicer:send(via_stream(PubVia2), <<1>>),
    %% We could send data over new stream
    {ok, PubVia3} = emqtt:start_data_stream(C, []),
    ok = emqtt:publish_async(
        C,
        PubVia3,
        Topic,
        <<"stream data 3">>,
        [{qos, PubQos}],
        undefined
    ),
    [
        {publish, #{
            client_pid := C,
            packet_id := PktId3,
            payload := <<"stream data 3">>,
            qos := RecQos,
            topic := Topic
        }}
    ] = recv_pub(1),

    ?assert(is_list(emqtt:info(C))),

    emqx_config:put_zone_conf(default, [mqtt, max_packet_size], OldMax),
    ok = emqtt:disconnect(C).

t_conn_change_client_addr(Config) ->
    PubQos = ?config(pub_qos, Config),
    SubQos = ?config(sub_qos, Config),
    RecQos = calc_qos(PubQos, SubQos),
    Topic = atom_to_binary(?FUNCTION_NAME),
    {ok, C} = emqtt:start_link([{proto_ver, v5} | Config]),
    {ok, _} = emqtt:quic_connect(C),
    {ok, _, [SubQos]} = emqtt:subscribe(C, #{}, [{Topic, [{qos, SubQos}]}]),

    {ok, {quic, Conn, _} = PubVia} = emqtt:start_data_stream(C, []),
    ok = emqtt:publish_async(
        C,
        PubVia,
        Topic,
        <<"stream data 1">>,
        [{qos, PubQos}],
        undefined
    ),

    ?assertMatch(
        [
            {publish, #{
                client_pid := C,
                packet_id := _PktId1,
                payload := <<"stream data 1">>,
                qos := RecQos
            }}
        ],
        recv_pub(1)
    ),
    NewPort = select_port(),
    {ok, OldAddr} = quicer:sockname(Conn),
    ?assertEqual(
        ok, quicer:setopt(Conn, local_address, "127.0.0.1:" ++ integer_to_list(NewPort))
    ),
    {ok, NewAddr} = quicer:sockname(Conn),
    ct:pal("NewAddr: ~p, Old Addr: ~p", [NewAddr, OldAddr]),
    ?assertNotEqual(OldAddr, NewAddr),
    ?assert(is_list(emqtt:info(C))),
    ok = emqtt:disconnect(C).

t_multi_streams_sub_pub_async(Config) ->
    Topic = atom_to_binary(?FUNCTION_NAME),
    PubQos = ?config(pub_qos, Config),
    SubQos = ?config(sub_qos, Config),
    RecQos = calc_qos(PubQos, SubQos),
    PktId1 = calc_pkt_id(RecQos, 1),
    Topic2 = <<Topic/binary, "_two">>,
    {ok, C} = emqtt:start_link([{proto_ver, v5} | Config]),
    {ok, _} = emqtt:quic_connect(C),
    {ok, _, [SubQos]} = emqtt:subscribe_via(C, {new_data_stream, []}, #{}, [
        {Topic, [{qos, SubQos}]}
    ]),
    {ok, _, [SubQos]} = emqtt:subscribe_via(C, {new_data_stream, []}, #{}, [
        {Topic2, [{qos, SubQos}]}
    ]),
    ok = emqtt:publish_async(
        C,
        {new_data_stream, []},
        Topic,
        <<"stream data 1">>,
        [{qos, PubQos}],
        undefined
    ),
    ok = emqtt:publish_async(
        C,
        {new_data_stream, []},
        Topic2,
        <<"stream data 2">>,
        [{qos, PubQos}],
        undefined
    ),
    PubRecvs = recv_pub(2),
    ?assertMatch(
        [
            {publish, #{
                client_pid := C,
                packet_id := PktId1,
                payload := <<"stream data", _/binary>>,
                qos := RecQos
            }},
            {publish, #{
                client_pid := C,
                packet_id := PktId1,
                payload := <<"stream data", _/binary>>,
                qos := RecQos
            }}
        ],
        PubRecvs
    ),
    Payloads = [P || {publish, #{payload := P}} <- PubRecvs],
    ?assert(
        [<<"stream data 1">>, <<"stream data 2">>] == Payloads orelse
            [<<"stream data 2">>, <<"stream data 1">>] == Payloads
    ),
    ok = emqtt:disconnect(C).

t_multi_streams_sub_pub_sync(Config) ->
    PubQos = ?config(pub_qos, Config),
    SubQos = ?config(sub_qos, Config),
    RecQos = calc_qos(PubQos, SubQos),
    PktId1 = calc_pkt_id(RecQos, 1),
    Topic = atom_to_binary(?FUNCTION_NAME),
    Topic2 = <<Topic/binary, "two">>,
    {ok, C} = emqtt:start_link([{proto_ver, v5} | Config]),
    {ok, _} = emqtt:quic_connect(C),
    {ok, #{via := SVia1}, [SubQos]} = emqtt:subscribe_via(C, {new_data_stream, []}, #{}, [
        {Topic, [{qos, SubQos}]}
    ]),
    {ok, #{via := SVia2}, [SubQos]} = emqtt:subscribe_via(C, {new_data_stream, []}, #{}, [
        {Topic2, [{qos, SubQos}]}
    ]),

    case
        emqtt:publish_via(C, {new_data_stream, []}, Topic, #{}, <<"stream data 3">>, [{qos, PubQos}])
    of
        ok when PubQos == 0 ->
            Via1 = undefined,
            ok;
        {ok, #{reason_code := 0, via := Via1}} ->
            ok
    end,
    case
        emqtt:publish_via(C, {new_data_stream, []}, Topic2, #{}, <<"stream data 4">>, [
            {qos, PubQos}
        ])
    of
        ok when PubQos == 0 -> ok;
        {ok, #{reason_code := 0, via := Via2}} ->
            ?assert(Via1 =/= Via2),
            ok
    end,
    ct:pal("SVia1: ~p, SVia2: ~p", [SVia1, SVia2]),
    PubRecvs = recv_pub(2),
    ?assertMatch(
        [
            {publish, #{
                client_pid := C,
                packet_id := PktId1,
                payload := <<"stream data 3">>,
                qos := RecQos,
                via := SVia1
            }},
            {publish, #{
                client_pid := C,
                packet_id := PktId1,
                payload := <<"stream data 4">>,
                qos := RecQos,
                via := SVia2
            }}
        ],
        lists:sort(PubRecvs)
    ),
    ok = emqtt:disconnect(C).

t_multi_streams_dup_sub(Config) ->
    PubQos = ?config(pub_qos, Config),
    SubQos = ?config(sub_qos, Config),
    RecQos = calc_qos(PubQos, SubQos),
    PktId1 = calc_pkt_id(RecQos, 1),
    Topic = atom_to_binary(?FUNCTION_NAME),
    {ok, C} = emqtt:start_link([{proto_ver, v5} | Config]),
    {ok, _} = emqtt:quic_connect(C),
    {ok, #{via := SVia1}, [SubQos]} = emqtt:subscribe_via(C, {new_data_stream, []}, #{}, [
        {Topic, [{qos, SubQos}]}
    ]),
    {ok, #{via := SVia2}, [SubQos]} = emqtt:subscribe_via(C, {new_data_stream, []}, #{}, [
        {Topic, [{qos, SubQos}]}
    ]),

    #{data_stream_socks := [{quic, _Conn, SubStream} | _]} = proplists:get_value(
        extra, emqtt:info(C)
    ),
    ?assertEqual(2, length(emqx_broker:subscribers(Topic))),

    case
        emqtt:publish_via(C, {new_data_stream, []}, Topic, #{}, <<"stream data 3">>, [{qos, PubQos}])
    of
        ok when PubQos == 0 ->
            ok;
        {ok, #{reason_code := 0, via := _Via1}} ->
            ok
    end,
    PubRecvs = recv_pub(2),
    ?assertMatch(
        [
            {publish, #{
                client_pid := C,
                packet_id := PktId1,
                payload := <<"stream data 3">>,
                qos := RecQos
            }},
            {publish, #{
                client_pid := C,
                packet_id := PktId1,
                payload := <<"stream data 3">>,
                qos := RecQos
            }}
        ],
        lists:sort(PubRecvs)
    ),

    RecvVias = [Via || {publish, #{via := Via}} <- PubRecvs],

    ct:pal("~p, ~p, ~n recv from: ~p~n", [SVia1, SVia2, PubRecvs]),
    %% Can recv in any order
    ?assert([SVia1, SVia2] == RecvVias orelse [SVia2, SVia1] == RecvVias),

    %% Shutdown one stream
    quicer:async_shutdown_stream(SubStream, ?QUIC_STREAM_SHUTDOWN_FLAG_GRACEFUL, 500),
    timer:sleep(100),

    ?assertEqual(1, length(emqx_broker:subscribers(Topic))),

    ok = emqtt:disconnect(C).

t_multi_streams_corr_topic(Config) ->
    PubQos = ?config(pub_qos, Config),
    SubQos = ?config(sub_qos, Config),
    RecQos = calc_qos(PubQos, SubQos),
    PktId1 = calc_pkt_id(RecQos, 1),
    PktId2 = calc_pkt_id(RecQos, 2),
    Topic = atom_to_binary(?FUNCTION_NAME),
    {ok, C} = emqtt:start_link([{proto_ver, v5} | Config]),
    {ok, _} = emqtt:quic_connect(C),
    {ok, #{via := SubVia}, [SubQos]} = emqtt:subscribe_via(C, {new_data_stream, []}, #{}, [
        {Topic, [{qos, SubQos}]}
    ]),

    case
        emqtt:publish_via(C, {new_data_stream, []}, Topic, #{}, <<1, 2, 3, 4, 5>>, [{qos, PubQos}])
    of
        ok when PubQos == 0 ->
            ok;
        {ok, #{reason_code := 0, via := _Via}} ->
            ok
    end,

    #{data_stream_socks := [PubVia | _]} = proplists:get_value(extra, emqtt:info(C)),
    ?assert(PubVia =/= SubVia),

    case emqtt:publish_via(C, PubVia, Topic, #{}, <<6, 7, 8, 9>>, [{qos, PubQos}]) of
        ok when PubQos == 0 -> ok;
        {ok, #{reason_code := 0, via := PubVia}} -> ok
    end,
    PubRecvs = recv_pub(2),
    ?assertMatch(
        [
            {publish, #{
                client_pid := C,
                packet_id := PktId1,
                payload := <<1, 2, 3, 4, 5>>,
                qos := RecQos
            }},
            {publish, #{
                client_pid := C,
                packet_id := PktId2,
                payload := <<6, 7, 8, 9>>,
                qos := RecQos
            }}
        ],
        PubRecvs
    ),
    ok = emqtt:disconnect(C).

t_multi_streams_unsub(Config) ->
    PubQos = ?config(pub_qos, Config),
    SubQos = ?config(sub_qos, Config),
    RecQos = calc_qos(PubQos, SubQos),
    PktId1 = calc_pkt_id(RecQos, 1),

    Topic = atom_to_binary(?FUNCTION_NAME),
    {ok, C} = emqtt:start_link([{proto_ver, v5} | Config]),
    {ok, _} = emqtt:quic_connect(C),
    {ok, #{via := SubVia}, [SubQos]} = emqtt:subscribe_via(C, {new_data_stream, []}, #{}, [
        {Topic, [{qos, SubQos}]}
    ]),
    case
        emqtt:publish_via(C, {new_data_stream, []}, Topic, #{}, <<1, 2, 3, 4, 5>>, [{qos, PubQos}])
    of
        ok when PubQos == 0 ->
            ok;
        {ok, #{reason_code := 0, via := _PVia}} ->
            ok
    end,

    #{data_stream_socks := [PubVia | _]} = proplists:get_value(extra, emqtt:info(C)),
    ?assert(PubVia =/= SubVia),
    PubRecvs = recv_pub(1),
    ?assertMatch(
        [
            {publish, #{
                client_pid := C,
                packet_id := PktId1,
                payload := <<1, 2, 3, 4, 5>>,
                qos := RecQos
            }}
        ],
        PubRecvs
    ),

    emqtt:unsubscribe_via(C, SubVia, Topic),
    ?retry(
        _Sleep2 = 100,
        _Attempts2 = 50,
        [] = emqx_router:lookup_routes(Topic)
    ),

    case emqtt:publish_via(C, PubVia, Topic, #{}, <<6, 7, 8, 9>>, [{qos, PubQos}]) of
        ok when PubQos == 0 ->
            ok;
        {ok, #{reason_code := 16, via := PubVia, reason_code_name := no_matching_subscribers}} ->
            ok
    end,

    timeout = recv_pub(1),
    ok = emqtt:disconnect(C).

t_multi_streams_kill_sub_stream(Config) ->
    PubQos = ?config(pub_qos, Config),
    SubQos = ?config(sub_qos, Config),
    RecQos = calc_qos(PubQos, SubQos),
    PktId1 = calc_pkt_id(RecQos, 1),

    Topic = atom_to_binary(?FUNCTION_NAME),
    Topic2 = <<Topic/binary, "two">>,
    {ok, C} = emqtt:start_link([{proto_ver, v5} | Config]),
    {ok, _} = emqtt:quic_connect(C),
    {ok, #{via := _SVia}, [SubQos]} = emqtt:subscribe_via(C, {new_data_stream, []}, #{}, [
        {Topic, [{qos, SubQos}]}
    ]),
    {ok, #{via := _SVia2}, [SubQos]} = emqtt:subscribe_via(C, {new_data_stream, []}, #{}, [
        {Topic2, [{qos, SubQos}]}
    ]),
    [TopicStreamOwner] = emqx_broker:subscribers(Topic),
    exit(TopicStreamOwner, kill),
    case
        emqtt:publish_via(C, {new_data_stream, []}, Topic, #{}, <<1, 2, 3, 4, 5>>, [{qos, PubQos}])
    of
        ok when PubQos == 0 ->
            ok;
        {ok, #{reason_code := Code, via := _PVia}} when Code == 0 orelse Code == 16 ->
            ok
    end,

    case
        emqtt:publish_via(C, {new_data_stream, []}, Topic2, #{}, <<1, 2, 3, 4, 5>>, [{qos, PubQos}])
    of
        ok when PubQos == 0 ->
            ok;
        {ok, #{reason_code := 0, via := _PVia2}} ->
            ok
    end,

    ?assertMatch(
        [
            {publish, #{
                client_pid := C,
                packet_id := PktId1,
                topic := Topic2,
                payload := <<1, 2, 3, 4, 5>>,
                qos := RecQos
            }}
        ],
        recv_pub(1)
    ),
    ?assertEqual(timeout, recv_pub(1)),
    ok.

t_multi_streams_unsub_via_other(Config) ->
    PubQos = ?config(pub_qos, Config),
    SubQos = ?config(sub_qos, Config),
    RecQos = calc_qos(PubQos, SubQos),
    PktId1 = calc_pkt_id(RecQos, 1),
    PktId2 = calc_pkt_id(RecQos, 2),

    Topic = atom_to_binary(?FUNCTION_NAME),
    Topic2 = <<Topic/binary, "two">>,
    {ok, C} = emqtt:start_link([{proto_ver, v5} | Config]),
    {ok, _} = emqtt:quic_connect(C),
    {ok, #{via := _SVia}, [SubQos]} = emqtt:subscribe_via(C, {new_data_stream, []}, #{}, [
        {Topic, [{qos, SubQos}]}
    ]),
    {ok, #{via := SVia2}, [SubQos]} = emqtt:subscribe_via(C, {new_data_stream, []}, #{}, [
        {Topic2, [{qos, SubQos}]}
    ]),

    case
        emqtt:publish_via(C, {new_data_stream, []}, Topic, #{}, <<1, 2, 3, 4, 5>>, [{qos, PubQos}])
    of
        ok when PubQos == 0 -> ok;
        {ok, #{reason_code := 0, via := _PVia}} -> ok
    end,

    PubRecvs = recv_pub(1),
    ?assertMatch(
        [
            {publish, #{
                client_pid := C,
                packet_id := PktId1,
                payload := <<1, 2, 3, 4, 5>>,
                qos := RecQos
            }}
        ],
        PubRecvs
    ),

    #{data_stream_socks := [PubVia | _]} = proplists:get_value(extra, emqtt:info(C)),

    %% Unsub topic1 via stream2 should fail with error code 17: "No subscription existed"
    {ok, #{via := SVia2}, [17]} = emqtt:unsubscribe_via(C, SVia2, Topic),

    case emqtt:publish_via(C, PubVia, Topic, #{}, <<6, 7, 8, 9>>, [{qos, PubQos}]) of
        ok when PubQos == 0 -> ok;
        {ok, #{reason_code := 0, via := _PVia2}} -> ok
    end,

    PubRecvs2 = recv_pub(1),
    ?assertMatch(
        [
            {publish, #{
                client_pid := C,
                packet_id := PktId2,
                payload := <<6, 7, 8, 9>>,
                qos := RecQos
            }}
        ],
        PubRecvs2
    ),
    ok = emqtt:disconnect(C).

t_multi_streams_shutdown_pub_data_stream(Config) ->
    PubQos = ?config(pub_qos, Config),
    SubQos = ?config(sub_qos, Config),
    RecQos = calc_qos(PubQos, SubQos),
    PktId1 = calc_pkt_id(RecQos, 1),

    Topic = atom_to_binary(?FUNCTION_NAME),
    Topic2 = <<Topic/binary, "two">>,
    {ok, C} = emqtt:start_link([{proto_ver, v5} | Config]),
    {ok, _} = emqtt:quic_connect(C),
    {ok, #{via := SVia}, [SubQos]} = emqtt:subscribe_via(C, {new_data_stream, []}, #{}, [
        {Topic, [{qos, SubQos}]}
    ]),
    {ok, #{via := SVia2}, [SubQos]} = emqtt:subscribe_via(C, {new_data_stream, []}, #{}, [
        {Topic2, [{qos, SubQos}]}
    ]),

    ?assert(SVia =/= SVia2),

    case
        emqtt:publish_via(C, {new_data_stream, []}, Topic, #{}, <<1, 2, 3, 4, 5>>, [{qos, PubQos}])
    of
        ok when PubQos == 0 -> ok;
        {ok, #{reason_code := 0, via := _PVia}} -> ok
    end,

    PubRecvs = recv_pub(1),
    #{data_stream_socks := [PubVia | _]} = proplists:get_value(extra, emqtt:info(C)),
    {quic, _Conn, DataStream} = PubVia,
    quicer:shutdown_stream(DataStream, ?config(stream_shutdown_flag, Config), 500, 100),
    timer:sleep(500),
    %% Still alive
    ?assert(is_list(emqtt:info(C))),
    ?assertMatch(
        [
            {publish, #{
                client_pid := C,
                packet_id := PktId1,
                payload := <<1, 2, 3, 4, 5>>,
                qos := RecQos
            }}
        ],
        PubRecvs
    ),
    emqtt:stop(C).

t_multi_streams_shutdown_sub_data_stream(Config) ->
    PubQos = ?config(pub_qos, Config),
    SubQos = ?config(sub_qos, Config),
    RecQos = calc_qos(PubQos, SubQos),
    PktId1 = calc_pkt_id(RecQos, 1),

    Topic = atom_to_binary(?FUNCTION_NAME),
    Topic2 = <<Topic/binary, "two">>,
    {ok, C} = emqtt:start_link([{proto_ver, v5} | Config]),
    {ok, _} = emqtt:quic_connect(C),
    {ok, #{via := SVia}, [SubQos]} = emqtt:subscribe_via(C, {new_data_stream, []}, #{}, [
        {Topic, [{qos, SubQos}]}
    ]),

    {ok, #{via := SVia2}, [SubQos]} = emqtt:subscribe_via(C, {new_data_stream, []}, #{}, [
        {Topic2, [{qos, SubQos}]}
    ]),

    ?assert(SVia =/= SVia2),
    {quic, _Conn, DataStream} = SVia2,
    quicer:shutdown_stream(DataStream, ?QUIC_STREAM_SHUTDOWN_FLAG_ABORT_RECEIVE, 500, 100),

    case
        emqtt:publish_via(C, {new_data_stream, []}, Topic, #{}, <<1, 2, 3, 4, 5>>, [{qos, PubQos}])
    of
        ok when PubQos == 0 -> ok;
        {ok, #{reason_code := 0, via := _PVia}} -> ok
    end,

    PubRecvs = recv_pub(1),
    ?assertMatch(
        [
            {publish, #{
                client_pid := C,
                packet_id := PktId1,
                payload := <<1, 2, 3, 4, 5>>,
                qos := RecQos
            }}
        ],
        PubRecvs
    ),

    #{data_stream_socks := [_PubVia | _]} = proplists:get_value(extra, emqtt:info(C)),
    timer:sleep(500),
    %% Still alive
    ?assert(is_list(emqtt:info(C))),
    emqtt:stop(C).

t_multi_streams_shutdown_ctrl_stream(Config) ->
    PubQos = ?config(pub_qos, Config),
    SubQos = ?config(sub_qos, Config),
    RecQos = calc_qos(PubQos, SubQos),
    PktId1 = calc_pkt_id(RecQos, 1),

    Topic = atom_to_binary(?FUNCTION_NAME),
    Topic2 = <<Topic/binary, "two">>,
    {ok, C} = emqtt:start_link([{proto_ver, v5} | Config]),
    unlink(C),
    {ok, _} = emqtt:quic_connect(C),
    {ok, #{via := _SVia}, [SubQos]} = emqtt:subscribe_via(C, {new_data_stream, []}, #{}, [
        {Topic, [{qos, SubQos}]}
    ]),
    {ok, #{via := _SVia2}, [SubQos]} = emqtt:subscribe_via(C, {new_data_stream, []}, #{}, [
        {Topic2, [{qos, SubQos}]}
    ]),

    case
        emqtt:publish_via(C, {new_data_stream, []}, Topic, #{}, <<1, 2, 3, 4, 5>>, [{qos, PubQos}])
    of
        ok when PubQos == 0 -> ok;
        {ok, #{reason_code := 0, via := _PVia}} -> ok
    end,

    PubRecvs = recv_pub(1),
    ?assertMatch(
        [
            {publish, #{
                client_pid := C,
                packet_id := PktId1,
                payload := <<1, 2, 3, 4, 5>>,
                qos := RecQos
            }}
        ],
        PubRecvs
    ),

    {quic, _Conn, Ctrlstream} = proplists:get_value(socket, emqtt:info(C)),
    Flag = ?config(stream_shutdown_flag, Config),
    AppErrorCode =
        case Flag of
            ?QUIC_STREAM_SHUTDOWN_FLAG_GRACEFUL -> 0;
            _ -> 500
        end,
    quicer:shutdown_stream(Ctrlstream, Flag, AppErrorCode, 1000),
    timer:sleep(500),
    %% Client should be closed
    ?assertMatch({'EXIT', {noproc, {gen_statem, call, [_, info, infinity]}}}, catch emqtt:info(C)).

t_multi_streams_shutdown_ctrl_stream_then_reconnect(Config) ->
    erlang:process_flag(trap_exit, true),
    PubQos = ?config(pub_qos, Config),
    SubQos = ?config(sub_qos, Config),
    RecQos = calc_qos(PubQos, SubQos),
    PktId1 = calc_pkt_id(RecQos, 1),

    Topic = atom_to_binary(?FUNCTION_NAME),
    Topic2 = <<Topic/binary, "two">>,
    {ok, C} = emqtt:start_link([
        {proto_ver, v5},
        {reconnect, true},
        {clean_start, false},
        {clientid, atom_to_binary(?FUNCTION_NAME)},
        %% speedup test
        {connect_timeout, 5}
        | Config
    ]),
    {ok, _} = emqtt:quic_connect(C),
    {ok, #{via := SVia}, [SubQos]} = emqtt:subscribe_via(C, {new_data_stream, []}, #{}, [
        {Topic, [{qos, SubQos}]}
    ]),
    {ok, #{via := SVia2}, [SubQos]} = emqtt:subscribe_via(C, {new_data_stream, []}, #{}, [
        {Topic2, [{qos, SubQos}]}
    ]),

    ?assert(SVia2 =/= SVia),

    case
        emqtt:publish_via(C, {new_data_stream, []}, Topic, #{}, <<1, 2, 3, 4, 5>>, [{qos, PubQos}])
    of
        ok when PubQos == 0 -> ok;
        {ok, #{reason_code := 0, via := _PVia}} -> ok
    end,

    PubRecvs = recv_pub(1),
    ?assertMatch(
        [
            {publish, #{
                client_pid := C,
                packet_id := PktId1,
                payload := <<1, 2, 3, 4, 5>>,
                qos := RecQos
            }}
        ],
        PubRecvs
    ),

    {quic, _Conn, Ctrlstream} = proplists:get_value(socket, emqtt:info(C)),
    quicer:shutdown_stream(Ctrlstream, ?config(stream_shutdown_flag, Config), 500, 100),
    timer:sleep(200),
    %% Client should not be closed
    ?assert(is_list(emqtt:info(C))),
    emqtt:stop(C).

t_multi_streams_emqx_ctrl_kill(Config) ->
    erlang:process_flag(trap_exit, true),
    PubQos = ?config(pub_qos, Config),
    SubQos = ?config(sub_qos, Config),
    RecQos = calc_qos(PubQos, SubQos),
    PktId1 = calc_pkt_id(RecQos, 1),

    Topic = atom_to_binary(?FUNCTION_NAME),
    Topic2 = <<Topic/binary, "two">>,
    {ok, C} = emqtt:start_link([
        {proto_ver, v5},
        {reconnect, false},
        %% speedup test
        {connect_timeout, 5}
        | Config
    ]),
    {ok, _} = emqtt:quic_connect(C),
    {ok, #{via := SVia}, [SubQos]} = emqtt:subscribe_via(C, {new_data_stream, []}, #{}, [
        {Topic, [{qos, SubQos}]}
    ]),
    {ok, #{via := SVia2}, [SubQos]} = emqtt:subscribe_via(C, {new_data_stream, []}, #{}, [
        {Topic2, [{qos, SubQos}]}
    ]),

    ?assert(SVia2 =/= SVia),

    case
        emqtt:publish_via(C, {new_data_stream, []}, Topic, #{}, <<1, 2, 3, 4, 5>>, [{qos, PubQos}])
    of
        ok when PubQos == 0 -> ok;
        {ok, #{reason_code := 0, via := _PVia}} -> ok
    end,

    PubRecvs = recv_pub(1),
    ?assertMatch(
        [
            {publish, #{
                client_pid := C,
                packet_id := PktId1,
                payload := <<1, 2, 3, 4, 5>>,
                qos := RecQos
            }}
        ],
        PubRecvs
    ),

    ClientId = proplists:get_value(clientid, emqtt:info(C)),
    [{ClientId, TransPid}] = ets:lookup(?CHAN_TAB, ClientId),
    exit(TransPid, kill),

    %% Client should be closed
    assert_client_die(C).

t_multi_streams_emqx_ctrl_exit_normal(Config) ->
    erlang:process_flag(trap_exit, true),
    PubQos = ?config(pub_qos, Config),
    SubQos = ?config(sub_qos, Config),
    RecQos = calc_qos(PubQos, SubQos),
    PktId1 = calc_pkt_id(RecQos, 1),

    Topic = atom_to_binary(?FUNCTION_NAME),
    Topic2 = <<Topic/binary, "two">>,
    {ok, C} = emqtt:start_link([
        {proto_ver, v5},
        {reconnect, false},
        %% speedup test
        {connect_timeout, 5}
        | Config
    ]),
    {ok, _} = emqtt:quic_connect(C),
    {ok, #{via := SVia}, [SubQos]} = emqtt:subscribe_via(C, {new_data_stream, []}, #{}, [
        {Topic, [{qos, SubQos}]}
    ]),
    {ok, #{via := SVia2}, [SubQos]} = emqtt:subscribe_via(C, {new_data_stream, []}, #{}, [
        {Topic2, [{qos, SubQos}]}
    ]),

    ?assert(SVia2 =/= SVia),

    case
        emqtt:publish_via(C, {new_data_stream, []}, Topic, #{}, <<1, 2, 3, 4, 5>>, [{qos, PubQos}])
    of
        ok when PubQos == 0 -> ok;
        {ok, #{reason_code := 0, via := _PVia}} -> ok
    end,

    PubRecvs = recv_pub(1),
    ?assertMatch(
        [
            {publish, #{
                client_pid := C,
                packet_id := PktId1,
                payload := <<1, 2, 3, 4, 5>>,
                qos := RecQos
            }}
        ],
        PubRecvs
    ),

    ClientId = proplists:get_value(clientid, emqtt:info(C)),
    [{ClientId, TransPid}] = ets:lookup(?CHAN_TAB, ClientId),

    emqx_connection:stop(TransPid),
    %% Client exit normal.
    assert_client_die(C).

t_multi_streams_remote_shutdown(Config) ->
    erlang:process_flag(trap_exit, true),
    PubQos = ?config(pub_qos, Config),
    SubQos = ?config(sub_qos, Config),
    RecQos = calc_qos(PubQos, SubQos),
    PktId1 = calc_pkt_id(RecQos, 1),

    Topic = atom_to_binary(?FUNCTION_NAME),
    Topic2 = <<Topic/binary, "two">>,
    {ok, C} = emqtt:start_link([
        {proto_ver, v5},
        {reconnect, false},
        {clientid, atom_to_binary(?FUNCTION_NAME)},
        %% speedup test
        {connect_timeout, 5}
        | Config
    ]),
    {ok, _} = emqtt:quic_connect(C),
    {ok, #{via := SVia}, [SubQos]} = emqtt:subscribe_via(C, {new_data_stream, []}, #{}, [
        {Topic, [{qos, SubQos}]}
    ]),
    {ok, #{via := SVia2}, [SubQos]} = emqtt:subscribe_via(C, {new_data_stream, []}, #{}, [
        {Topic2, [{qos, SubQos}]}
    ]),

    ?assert(SVia2 =/= SVia),

    case
        emqtt:publish_via(C, {new_data_stream, []}, Topic, #{}, <<1, 2, 3, 4, 5>>, [{qos, PubQos}])
    of
        ok when PubQos == 0 -> ok;
        {ok, #{reason_code := 0, via := _PVia}} -> ok
    end,

    PubRecvs = recv_pub(1),
    ?assertMatch(
        [
            {publish, #{
                client_pid := C,
                packet_id := PktId1,
                payload := <<1, 2, 3, 4, 5>>,
                qos := RecQos
            }}
        ],
        PubRecvs
    ),

    {quic, _Conn, _Ctrlstream} = proplists:get_value(socket, emqtt:info(C)),

    ok = stop_emqx(Config),
    try
        %% Client should be closed
        assert_client_die(C, 100, 200)
    after
        restart_emqx(Config)
    end.

t_multi_streams_remote_shutdown_with_reconnect(Config) ->
    erlang:process_flag(trap_exit, true),
    PubQos = ?config(pub_qos, Config),
    SubQos = ?config(sub_qos, Config),
    RecQos = calc_qos(PubQos, SubQos),
    PktId1 = calc_pkt_id(RecQos, 1),

    Topic = atom_to_binary(?FUNCTION_NAME),
    Topic2 = <<Topic/binary, "two">>,
    {ok, C} = emqtt:start_link([
        {proto_ver, v5},
        {reconnect, true},
        {clean_start, false},
        {clientid, atom_to_binary(?FUNCTION_NAME)},
        %% speedup test
        {connect_timeout, 5}
        | Config
    ]),
    {ok, _} = emqtt:quic_connect(C),
    {ok, #{via := SVia}, [SubQos]} = emqtt:subscribe_via(C, {new_data_stream, []}, #{}, [
        {Topic, [{qos, SubQos}]}
    ]),
    {ok, #{via := SVia2}, [SubQos]} = emqtt:subscribe_via(C, {new_data_stream, []}, #{}, [
        {Topic2, [{qos, SubQos}]}
    ]),

    ?assert(SVia2 =/= SVia),

    case
        emqtt:publish_via(C, {new_data_stream, []}, Topic, #{}, <<1, 2, 3, 4, 5>>, [{qos, PubQos}])
    of
        ok when PubQos == 0 -> ok;
        {ok, #{reason_code := 0, via := _PVia}} -> ok
    end,

    PubRecvs = recv_pub(1),
    ?assertMatch(
        [
            {publish, #{
                client_pid := C,
                packet_id := PktId1,
                payload := <<1, 2, 3, 4, 5>>,
                qos := RecQos
            }}
        ],
        PubRecvs
    ),

    {quic, _Conn, _Ctrlstream} = proplists:get_value(socket, emqtt:info(C)),

    _Apps = restart_emqx(Config),

    ?assert(is_list(emqtt:info(C))),
    emqtt:stop(C).

t_conn_silent_close(Config) ->
    erlang:process_flag(trap_exit, true),
    {ok, C} = emqtt:start_link([
        {proto_ver, v5},
        {connect_timeout, 5}
        | Config
    ]),
    {ok, _} = emqtt:quic_connect(C),
    %% quic idle timeout + 1s
    timer:sleep(16000),
    Topic = atom_to_binary(?FUNCTION_NAME),
    ?assertException(
        exit,
        noproc,
        emqtt:publish_via(C, {new_data_stream, []}, Topic, #{}, <<1, 2, 3, 4, 5>>, [{qos, 1}])
    ).

t_client_conn_bump_streams(Config) ->
    {ok, C} = emqtt:start_link([
        {proto_ver, v5},
        {connect_timeout, 5}
        | Config
    ]),
    {ok, _} = emqtt:quic_connect(C),
    {quic, Conn, _Stream} = proplists:get_value(socket, emqtt:info(C)),
    ok = quicer:setopt(Conn, settings, #{peer_unidi_stream_count => 20}).

t_olp_true(Config) ->
    meck:new(emqx_olp, [passthrough, no_history]),
    ok = meck:expect(emqx_olp, is_overloaded, fun() -> true end),
    {ok, C} = emqtt:start_link([
        {proto_ver, v5},
        {connect_timeout, 5}
        | Config
    ]),
    {ok, _} = emqtt:quic_connect(C),
    ok = meck:unload(emqx_olp).

t_olp_reject(Config) ->
    erlang:process_flag(trap_exit, true),
    emqx_config:put_zone_conf(default, [overload_protection, enable], true),
    meck:new(emqx_olp, [passthrough, no_history]),
    ok = meck:expect(emqx_olp, is_overloaded, fun() -> true end),
    {ok, C} = emqtt:start_link([
        {proto_ver, v5},
        {connect_timeout, 5}
        | Config
    ]),
    ?assertEqual(
        {error,
            {transport_down, #{
                error => 346,
                status =>
                    user_canceled
            }}},
        emqtt:quic_connect(C)
    ),
    ok = meck:unload(emqx_olp),
    emqx_config:put_zone_conf(default, [overload_protection, enable], false).

t_conn_resume(Config) ->
    erlang:process_flag(trap_exit, true),
    {ok, C0} = emqtt:start_link([
        {proto_ver, v5},
        {connect_timeout, 5}
        | Config
    ]),

    {ok, _} = emqtt:quic_connect(C0),
    #{nst := NST} = proplists:get_value(extra, emqtt:info(C0)),
    emqtt:disconnect(C0),
    {ok, C} = emqtt:start_link([
        {proto_ver, v5},
        {connect_timeout, 5},
        {nst, NST}
        | Config
    ]),
    {ok, _} = emqtt:quic_connect(C),
    Cid = proplists:get_value(clientid, emqtt:info(C)),
    ct:pal("~p~n", [emqx_cm:get_chan_info(Cid)]).

t_conn_without_ctrl_stream(Config) ->
    erlang:process_flag(trap_exit, true),
    {ok, Conn} = quicer:connect(
        {127, 0, 0, 1},
        ?config(port, Config),
        [{alpn, ["mqtt"]}, {verify, none}],
        3000
    ),
    receive
        {quic, transport_shutdown, Conn, _} -> ok
    end.

t_data_stream_race_ctrl_stream(Config) ->
    erlang:process_flag(trap_exit, true),
    {ok, C0} = emqtt:start_link([
        {proto_ver, v5},
        {connect_timeout, 5}
        | Config
    ]),
    {ok, _} = emqtt:quic_connect(C0),
    #{nst := NST} = proplists:get_value(extra, emqtt:info(C0)),
    emqtt:disconnect(C0),
    {ok, C} = emqtt:start_link([
        {proto_ver, v5},
        {connect_timeout, 5},
        {nst, NST}
        | Config
    ]),
    {ok, _} = emqtt:quic_connect(C),
    Cid = proplists:get_value(clientid, emqtt:info(C)),
    ct:pal("~p~n", [emqx_cm:get_chan_info(Cid)]).

t_multi_streams_sub_0_rtt(Config) ->
    PubQos = ?config(pub_qos, Config),
    SubQos = ?config(sub_qos, Config),
    RecQos = calc_qos(PubQos, SubQos),
    Topic = atom_to_binary(?FUNCTION_NAME),
    {ok, C0} = emqtt:start_link([{proto_ver, v5} | Config]),
    {ok, _} = emqtt:quic_connect(C0),
    {ok, _, [SubQos]} = emqtt:subscribe_via(C0, {new_data_stream, []}, #{}, [
        {Topic, [{qos, SubQos}]}
    ]),
    {ok, C} = emqtt:start_link([{proto_ver, v5} | Config]),
    ok = emqtt:open_quic_connection(C),
    ok = emqtt:quic_mqtt_connect(C),
    ok = emqtt:publish_async(
        C,
        {new_data_stream, []},
        Topic,
        #{},
        <<"qos 2 1">>,
        [{qos, PubQos}],
        infinity,
        fun(_) -> ok end
    ),
    {ok, _} = emqtt:quic_connect(C),
    receive
        {publish, #{
            client_pid := C0,
            payload := <<"qos 2 1">>,
            qos := RecQos,
            topic := Topic
        }} ->
            ok;
        Other ->
            ct:fail("unexpected recv ~p", [Other])
    after 100 ->
        ct:fail("not received")
    end,
    ok = emqtt:disconnect(C),
    ok = emqtt:disconnect(C0).

t_multi_streams_sub_0_rtt_large_payload(Config) ->
    PubQos = ?config(pub_qos, Config),
    SubQos = ?config(sub_qos, Config),
    RecQos = calc_qos(PubQos, SubQos),
    Topic = atom_to_binary(?FUNCTION_NAME),
    Payload = binary:copy(<<"qos 2 1">>, 1600),
    {ok, C0} = emqtt:start_link([{proto_ver, v5} | Config]),
    {ok, _} = emqtt:quic_connect(C0),
    {ok, _, [SubQos]} = emqtt:subscribe_via(C0, {new_data_stream, []}, #{}, [
        {Topic, [{qos, SubQos}]}
    ]),
    {ok, C} = emqtt:start_link([{proto_ver, v5} | Config]),
    ok = emqtt:open_quic_connection(C),
    ok = emqtt:quic_mqtt_connect(C),
    ok = emqtt:publish_async(
        C,
        {new_data_stream, []},
        Topic,
        #{},
        Payload,
        [{qos, PubQos}],
        infinity,
        fun(_) -> ok end
    ),
    {ok, _} = emqtt:quic_connect(C),
    receive
        {publish, #{
            client_pid := C0,
            payload := Payload,
            qos := RecQos,
            topic := Topic
        }} ->
            ok;
        Other ->
            ct:fail("unexpected recv ~p", [Other])
    after 100 ->
        ct:fail("not received")
    end,
    ok = emqtt:disconnect(C),
    ok = emqtt:disconnect(C0).

%% @doc verify data stream can continue after 0-RTT handshake
t_multi_streams_sub_0_rtt_stream_data_cont(Config) ->
    PubQos = ?config(pub_qos, Config),
    SubQos = ?config(sub_qos, Config),
    RecQos = calc_qos(PubQos, SubQos),
    Topic = atom_to_binary(?FUNCTION_NAME),
    Payload = binary:copy(<<"qos 2 1">>, 1600),
    {ok, C0} = emqtt:start_link([{proto_ver, v5} | Config]),
    {ok, _} = emqtt:quic_connect(C0),
    {ok, _, [SubQos]} = emqtt:subscribe_via(C0, {new_data_stream, []}, #{}, [
        {Topic, [{qos, SubQos}]}
    ]),
    {ok, C} = emqtt:start_link([{proto_ver, v5} | Config]),
    ok = emqtt:open_quic_connection(C),
    ok = emqtt:quic_mqtt_connect(C),
    {ok, PubVia} = emqtt:start_data_stream(C, []),
    ok = emqtt:publish_async(
        C,
        PubVia,
        Topic,
        #{},
        Payload,
        [{qos, PubQos}],
        infinity,
        fun(_) -> ok end
    ),
    {ok, _} = emqtt:quic_connect(C),
    receive
        {publish, #{
            client_pid := C0,
            payload := Payload,
            qos := RecQos,
            topic := Topic
        }} ->
            ok;
        Other ->
            ct:fail("unexpected recv ~p", [Other])
    after 100 ->
        ct:fail("not received")
    end,
    Payload2 = <<"2nd part", Payload/binary>>,
    ok = emqtt:publish_async(
        C,
        PubVia,
        Topic,
        #{},
        Payload2,
        [{qos, PubQos}],
        infinity,
        fun(_) -> ok end
    ),
    receive
        {publish, #{
            client_pid := C0,
            payload := Payload2,
            qos := RecQos,
            topic := Topic
        }} ->
            ok;
        Other2 ->
            ct:fail("unexpected recv ~p", [Other2])
    after 100 ->
        ct:fail("not received")
    end,
    ok = emqtt:disconnect(C),
    ok = emqtt:disconnect(C0).

t_listener_inval_settings(_Config) ->
    LPort = select_port(),
    %% too small
    LowLevelTunings = #{stream_recv_buffer_default => 1024},
    ?assertThrow(
        {error, {failed_to_start, _}},
        emqx_common_test_helpers:ensure_quic_listener(?FUNCTION_NAME, LPort, LowLevelTunings)
    ).

t_listener_with_lowlevel_settings(_Config) ->
    LPort = select_port(),
    LowLevelTunings = #{
        max_bytes_per_key => 274877906,
        %% In conf schema we use handshake_idle_timeout
        handshake_idle_timeout_ms => 2000,
        %% In conf schema we use idle_timeout
        idle_timeout_ms => 20000,
        %% not use since we are server
        %% tls_client_max_send_buffer,
        tls_server_max_send_buffer => 10240,
        stream_recv_window_default => 16384 * 2,
        %% there is one debug assertion: stream_recv_window_default > stream_recv_buffer_default
        stream_recv_buffer_default => 16384,
        conn_flow_control_window => 1024,
        max_stateless_operations => 16,
        initial_window_packets => 1300,
        send_idle_timeout_ms => 12000,
        initial_rtt_ms => 300,
        max_ack_delay_ms => 6000,
        disconnect_timeout_ms => 60000,
        %% In conf schema,  we use keep_alive_interval
        keep_alive_interval_ms => 12000,
        %% over written by conn opts
        peer_bidi_stream_count => 100,
        %% over written by conn opts
        peer_unidi_stream_count => 100,
        retry_memory_limit => 640,
        load_balancing_mode => 1,
        max_operations_per_drain => 32,
        send_buffering_enabled => 1,
        pacing_enabled => 0,
        migration_enabled => 0,
        datagram_receive_enabled => 1,
        server_resumption_level => 0,
        minimum_mtu => 1250,
        maximum_mtu => 1600,
        mtu_discovery_search_complete_timeout_us => 500000000,
        mtu_discovery_missing_probe_count => 6,
        max_binding_stateless_operations => 200,
        stateless_operation_expiration_ms => 200
    },
    ?assertEqual(
        ok, emqx_common_test_helpers:ensure_quic_listener(?FUNCTION_NAME, LPort, LowLevelTunings)
    ),
    timer:sleep(1000),
    {ok, C} = emqtt:start_link([{proto_ver, v5}, {port, LPort}]),
    {ok, _} = emqtt:quic_connect(C),
    {ok, _, _} = emqtt:subscribe(C, <<"test/1/2">>, qos2),
    {ok, _, [_SubQos]} = emqtt:subscribe_via(C, {new_data_stream, []}, #{}, [
        {<<"test/1/3">>, [{qos, 2}]}
    ]),
    ok = emqtt:disconnect(C).

t_keep_alive(Config) ->
    process_flag(trap_exit, true),

    Topic = atom_to_binary(?FUNCTION_NAME),
    PubQos = ?config(pub_qos, Config),
    SubQos = ?config(sub_qos, Config),
    RecQos = calc_qos(PubQos, SubQos),
    PktId1 = calc_pkt_id(RecQos, 1),
    Topic2 = <<Topic/binary, "_two">>,
    %% GIVEN: keepalive is 2s
    {ok, C} = emqtt:start_link([{proto_ver, v5}, {force_ping, false}, {keepalive, 2} | Config]),
    {ok, _} = emqtt:quic_connect(C),

    %% WHEN: we have active data on data stream only
    %% but keep client ctrl stream quiet with meck
    meck:new(emqtt, [no_link, passthrough, no_history]),
    meck:expect(emqtt, connected, fun
        (info, {timeout, _TRef, keepalive}, State) ->
            {keep_state, State};
        (Arg1, Arg2, Arg3) ->
            meck:passthrough([Arg1, Arg2, Arg3])
    end),
    {ok, _, [SubQos]} = emqtt:subscribe_via(C, {new_data_stream, []}, #{}, [
        {Topic, [{qos, SubQos}]}
    ]),
    {ok, _, [SubQos]} = emqtt:subscribe_via(C, {new_data_stream, []}, #{}, [
        {Topic2, [{qos, SubQos}]}
    ]),
    ok = emqtt:publish_async(
        C,
        {new_data_stream, []},
        Topic,
        <<"stream data 1">>,
        [{qos, PubQos}],
        undefined
    ),
    ok = emqtt:publish_async(
        C,
        {new_data_stream, []},
        Topic2,
        <<"stream data 2">>,
        [{qos, PubQos}],
        undefined
    ),
    PubRecvs = recv_pub(2),

    ?assertMatch(
        [
            {publish, #{
                client_pid := C,
                packet_id := PktId1,
                payload := <<"stream data", _/binary>>,
                qos := RecQos
            }},
            {publish, #{
                client_pid := C,
                packet_id := PktId1,
                payload := <<"stream data", _/binary>>,
                qos := RecQos
            }}
        ],
        PubRecvs
    ),
    Payloads = [P || {publish, #{payload := P}} <- PubRecvs],
    ?assert(
        [<<"stream data 1">>, <<"stream data 2">>] == Payloads orelse
            [<<"stream data 2">>, <<"stream data 1">>] == Payloads
    ),

    %% THEN: after 4s, idle timeout , client should get disconnected.
    receive
        {disconnected, ?RC_KEEP_ALIVE_TIMEOUT, _} ->
            meck:unload(emqtt),
            ok
    after 4000 ->
        meck:unload(emqtt),
        ct:fail("Didnt shutdown ~p", [process_info(self(), messages)])
    end.

t_keep_alive_idle_ctrl_stream(Config) ->
    process_flag(trap_exit, true),

    Topic = atom_to_binary(?FUNCTION_NAME),
    PubQos = ?config(pub_qos, Config),
    SubQos = ?config(sub_qos, Config),
    RecQos = calc_qos(PubQos, SubQos),
    PktId1 = calc_pkt_id(RecQos, 1),
    Topic2 = <<Topic/binary, "_two">>,
    %% GIVEN: keepalive is 2s
    {ok, C} = emqtt:start_link([{proto_ver, v5}, {force_ping, false}, {keepalive, 2} | Config]),
    {ok, _} = emqtt:quic_connect(C),

    %% WHEN: we have active data on data stream only
    %% but keep ctrl stream quiet with meck
    meck:new(emqtt, [no_link, passthrough, no_history]),
    meck:expect(emqtt, connected, fun
        (info, {timeout, _TRef, keepalive}, State) ->
            {keep_state, State};
        (Arg1, Arg2, Arg3) ->
            meck:passthrough([Arg1, Arg2, Arg3])
    end),
    {ok, _, [SubQos]} = emqtt:subscribe_via(C, {new_data_stream, []}, #{}, [
        {Topic, [{qos, SubQos}]}
    ]),
    {ok, _, [SubQos]} = emqtt:subscribe_via(C, {new_data_stream, []}, #{}, [
        {Topic2, [{qos, SubQos}]}
    ]),
    ok = emqtt:publish_async(
        C,
        {new_data_stream, []},
        Topic,
        <<"stream data 1">>,
        [{qos, PubQos}],
        undefined
    ),
    ok = emqtt:publish_async(
        C,
        {new_data_stream, []},
        Topic2,
        <<"stream data 2">>,
        [{qos, PubQos}],
        undefined
    ),
    PubRecvs = recv_pub(2),

    ?assertMatch(
        [
            {publish, #{
                client_pid := C,
                packet_id := PktId1,
                payload := <<"stream data", _/binary>>,
                qos := RecQos
            }},
            {publish, #{
                client_pid := C,
                packet_id := PktId1,
                payload := <<"stream data", _/binary>>,
                qos := RecQos
            }}
        ],
        PubRecvs
    ),
    Payloads = [P || {publish, #{payload := P}} <- PubRecvs],
    ?assert(
        [<<"stream data 1">>, <<"stream data 2">>] == Payloads orelse
            [<<"stream data 2">>, <<"stream data 1">>] == Payloads
    ),

    %% WHEN: keep data stream still active
    timer:sleep(1000),
    ok = emqtt:publish_async(
        C,
        {new_data_stream, []},
        Topic,
        <<"stream data 1">>,
        [{qos, PubQos}],
        undefined
    ),
    ok = emqtt:publish_async(
        C,
        {new_data_stream, []},
        Topic2,
        <<"stream data 2">>,
        [{qos, PubQos}],
        undefined
    ),

    %% THEN: after 4s, client should NOT get disconnected,
    %%       because data stream is active.
    receive
        {disconnected, ?RC_KEEP_ALIVE_TIMEOUT, _} ->
            meck:unload(emqtt),
            ct:fail("Should not disconnect")
        %% 4s - 1s
    after 3000 ->
        meck:unload(emqtt),
        ok
    end.

%%--------------------------------------------------------------------
%% Helper functions
%%--------------------------------------------------------------------
send_and_recv_with(Sock) ->
    {ok, {IP, _}} = emqtt_quic:sockname(Sock),
    ?assert(lists:member(tuple_size(IP), [4, 8])),
    ok = emqtt_quic:send(Sock, <<"ping">>),
    emqtt_quic:setopts(Sock, [{active, false}]),
    {ok, <<"pong">>} = emqtt_quic:recv(Sock, 0),
    ok = emqtt_quic:setopts(Sock, [{active, 100}]),
    {ok, Stats} = emqtt_quic:getstat(Sock, [send_cnt, recv_cnt]),
    %% connection level counters, not stream level
    [{send_cnt, _}, {recv_cnt, _}] = Stats.

certfile(Config) ->
    filename:join([test_dir(Config), "certs", "test.crt"]).

keyfile(Config) ->
    filename:join([test_dir(Config), "certs", "test.key"]).

test_dir(Config) ->
    filename:dirname(filename:dirname(proplists:get_value(data_dir, Config))).

recv_pub(Count) ->
    recv_pub(Count, [], 100).

recv_pub(Count, Tout) ->
    recv_pub(Count, [], Tout).

recv_pub(0, Acc, _Tout) ->
    lists:reverse(Acc);
recv_pub(Count, Acc, Tout) ->
    receive
        {publish, _Prop} = Pub ->
            recv_pub(Count - 1, [Pub | Acc], Tout)
    after Tout ->
        timeout
    end.

all_tc() ->
    code:add_patha(filename:join(code:lib_dir(emqx), "ebin/")),
    emqx_common_test_helpers:all(?MODULE).

-spec calc_qos(0 | 1 | 2, 0 | 1 | 2) -> 0 | 1 | 2.
calc_qos(PubQos, SubQos) ->
    if
        PubQos > SubQos ->
            SubQos;
        SubQos > PubQos ->
            PubQos;
        true ->
            PubQos
    end.
-spec calc_pkt_id(0 | 1 | 2, non_neg_integer()) -> undefined | non_neg_integer().
calc_pkt_id(0, _Id) ->
    undefined;
calc_pkt_id(1, Id) ->
    Id;
calc_pkt_id(2, Id) ->
    Id.

%% select a random port picked by OS
-spec select_port() -> inet:port_number().
select_port() ->
    emqx_common_test_helpers:select_free_port(quic).

-spec via_stream({quic, quicer:connection_handle(), quicer:stream_handle()}) ->
    quicer:stream_handle().
via_stream({quic, _Conn, Stream}) ->
    Stream.

assert_client_die(C) ->
    assert_client_die(C, 100, 10).
assert_client_die(C, _, 0) ->
    ct:fail("Client ~p did not die: stacktrace: ~p", [C, process_info(C, current_stacktrace)]);
assert_client_die(C, Delay, Retries) ->
    try emqtt:info(C) of
        Info when is_list(Info) ->
            timer:sleep(Delay),
            assert_client_die(C, Delay, Retries - 1)
    catch
        exit:Error ->
            ct:comment("client die with ~p", [Error])
    end.

%% BUILD_WITHOUT_QUIC
-else.
-endif.
