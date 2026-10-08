%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_sn_proxy_cluster_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include("emqx_mqttsn.hrl").

-include_lib("common_test/include/ct.hrl").
-include_lib("emqx/include/asserts.hrl").
-include_lib("emqx/include/emqx_session_mem.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(HOST, {127, 0, 0, 1}).
-define(ON(NODE, BODY), erpc:call(NODE, fun() -> BODY end)).
-define(DUP(FLAG), (FLAG):1).

all() ->
    [{group, legacy}, {group, hardened}].

groups() ->
    Tests = emqx_common_test_helpers:all(?MODULE),
    [{legacy, [], Tests}, {hardened, [], Tests}].

init_per_suite(Config) ->
    emqx_common_test_helpers:clear_security_profile(),
    Config.

end_per_suite(_Config) ->
    emqx_common_test_helpers:clear_security_profile().

init_per_group(Profile, Config) when Profile =:= legacy; Profile =:= hardened ->
    ok = emqx_common_test_helpers:set_security_profile(Profile),
    [{security_profile, Profile} | Config].

end_per_group(_Profile, _Config) ->
    emqx_common_test_helpers:clear_security_profile().

init_per_testcase(TestCase, Config) ->
    [{testcase, TestCase} | Config].

t_asleep_pingreq_resumes_across_nodes(Config) ->
    QoS = 1,
    SleepDuration = 5,
    ClientId = <<"cluster-asleep-proxy-reroute">>,
    TopicName = <<"cluster/asleep/proxy/reroute">>,
    Payload1 = <<"cluster-queued-before-reroute">>,
    Payload2 = <<"cluster-queued-after-stale-old-tuple">>,
    MsgId = 41,
    Retain = 0,
    WillBit = 0,
    CleanSession = 0,
    {Nodes, Port1, Port2} = start_mqttsn_cluster(Config),
    [Node1, Node2] = Nodes,
    {ok, Socket1} = gen_udp:open(0, [binary]),
    {ok, Socket2} = gen_udp:open(0, [binary]),
    try
        send_connect_msg(Socket1, Port1, ClientId, CleanSession),
        ?assertEqual(<<3, ?SN_CONNACK, ?SN_RC_ACCEPTED>>, receive_response(Socket1)),

        send_subscribe_msg_normal_topic(Socket1, Port1, QoS, TopicName, MsgId),
        SubAck = receive_response(Socket1),
        ?assertMatch(
            <<8, ?SN_SUBACK, ?DUP(0), QoS:2, Retain:1, WillBit:1, CleanSession:1,
                ?SN_NORMAL_TOPIC:2, _TopicId:16, MsgId:16, ?SN_RC_ACCEPTED>>,
            SubAck
        ),
        <<8, ?SN_SUBACK, ?DUP(0), QoS:2, Retain:1, WillBit:1, CleanSession:1, ?SN_NORMAL_TOPIC:2,
            TopicId:16, MsgId:16,
            ?SN_RC_ACCEPTED>> =
            SubAck,

        send_disconnect_msg(Socket1, Port1, SleepDuration),
        ?assertEqual(<<2, ?SN_DISCONNECT>>, receive_response(Socket1)),
        ?retry(
            50,
            20,
            #{conn_state := asleep} = erpc:call(Node1, emqx_gateway_cm, get_chan_info, [
                mqttsn, ClientId
            ])
        ),

        publish(Node1, QoS, TopicName, Payload1),
        timer:sleep(100),

        send_pingreq_msg(Socket2, Port2, ClientId),
        PubMsgId1 = receive_publish(
            Socket2, _Dup = 0, QoS, Retain, WillBit, CleanSession, TopicId, Payload1
        ),
        send_puback_msg(Socket2, Port2, TopicId, PubMsgId1),
        ?assertEqual(<<2, ?SN_PINGRESP>>, receive_response(Socket2)),
        ?retry(
            50,
            20,
            #{conn_state := asleep} = local_chan_info(Node2, ClientId)
        ),
        %% A stale datagram from the old node1 proxy must not affect the
        %% current node2 channel.
        send_disconnect_msg(Socket1, Port1, undefined),
        _ = receive_response(Socket1, 500),

        publish(Node2, QoS, TopicName, Payload2),
        timer:sleep(100),

        send_pingreq_msg(Socket2, Port2, ClientId),
        UdpData2 = receive_response(Socket2),
        PubMsgId2 = emqx_sn_protocol_SUITE:check_publish_msg_on_udp(
            {0, QoS, Retain, WillBit, CleanSession, ?SN_NORMAL_TOPIC, TopicId, Payload2},
            UdpData2
        ),
        send_puback_msg(Socket2, Port2, TopicId, PubMsgId2),
        send_pingreq_msg(Socket2, Port2, ClientId),
        ?assertEqual(<<2, ?SN_PINGRESP>>, receive_response(Socket2))
    after
        _ = catch erpc:call(Node1, emqx_gateway_cm, kick_session, [mqttsn, ClientId]),
        gen_udp:close(Socket1),
        gen_udp:close(Socket2),
        emqx_cth_cluster:stop(Nodes)
    end.

-doc "Wakeup imports a remote legacy session and drains queued QoS0 messages in order.".
t_asleep_pingreq_resume_legacy_session(Config) ->
    test_asleep_pingreq_resume_legacy_session(Config, legacy).

t_asleep_pingreq_resume_pre_63_session(Config) ->
    test_asleep_pingreq_resume_legacy_session(Config, pre63).

test_asleep_pingreq_resume_legacy_session(Config, Protocol) ->
    ClientId = <<"legacy-wakeup">>,
    Topic = <<"legacy/wakeup">>,
    Count = 2 * ?DEFAULT_BATCH_N + 1,
    {Nodes, Port1, Port2} = start_mqttsn_cluster(Config),
    [Node1, Node2] = Nodes,
    {ok, Socket1} = gen_udp:open(0, [binary]),
    {ok, Socket2} = gen_udp:open(0, [binary]),
    try
        select_takeover_protocol(Node1, Node2, Protocol),
        send_connect_msg(Socket1, Port1, ClientId, 0),
        ?assertEqual(<<3, ?SN_CONNACK, 0>>, receive_response(Socket1)),
        send_subscribe_msg_normal_topic(Socket1, Port1, 0, Topic, 1),
        <<8, ?SN_SUBACK, _, TopicId:16, 1:16, 0>> = receive_response(Socket1),
        send_disconnect_msg(Socket1, Port1, 60),
        ?assertEqual(<<2, ?SN_DISCONNECT>>, receive_response(Socket1)),
        [Pid1] = ?ON(Node1, emqx_gateway_cm:lookup_by_clientid(mqttsn, ClientId)),
        [
            publish(Node1, 0, Topic, integer_to_binary(N))
         || N <- lists:seq(1, Count)
        ],
        ?assertEqual(
            Count,
            proplists:get_value(mqueue_len, ?ON(Node1, emqx_gateway_conn:stats(Pid1)))
        ),
        ok = mock_outgoing_channel_call(
            Node2,
            fun
                (Pid, {takeover, 'begin', _Owner, _} = Call, _) when Pid =:= Pid1 ->
                    %% Legacy begin must omit the explicit owner/attempt pair.
                    error({"Legacy owner received non-legacy takeover call", Call});
                (Pid, {takeover, 'end', _Owner} = Call, _) when Pid =:= Pid1 ->
                    %% Legacy completion must omit the explicit owner/attempt pair.
                    error({"Legacy owner received non-legacy takeover call", Call});
                (Pid, {takeover, 'begin', #{peercert := _}} = Request, Timeout) when Pid =:= Pid1 ->
                    %% Run the owner's compatibility handler and check its legacy session reply.
                    {ok, #{session := Session}} =
                        Result = meck:passthrough([Pid, Request, Timeout]),
                    ?assertMatch(
                        #{session := S} when is_tuple(S),
                        Session,
                        "Legacy session is not a tuple"
                    ),
                    Result;
                (Pid, Request, Timeout) ->
                    meck:passthrough([Pid, Request, Timeout])
            end
        ),
        send_pingreq_msg(Socket2, Port2, ClientId),
        [
            receive_publish(Socket2, 0, 0, 0, 0, 0, TopicId, integer_to_binary(N))
         || N <- lists:seq(1, Count)
        ],
        ?assertEqual(<<2, ?SN_PINGRESP>>, receive_response(Socket2)),
        ?assertEqual(false, ?ON(Node1, is_process_alive(Pid1)))
    after
        ?ON(Node2, meck:unload()),
        emqx_cth_cluster:stop(Nodes)
    end.

-doc "An unsupported remote legacy wakeup request leaves the sleeping owner intact.".
t_asleep_pingreq_resume_rejects_legacy_authorized_begin(Config) ->
    ClientId = <<"unsupported-legacy-wakeup">>,
    {Nodes, Port1, Port2} = start_mqttsn_cluster(Config),
    [Node1, Node2] = Nodes,
    {ok, Socket1} = gen_udp:open(0, [binary]),
    {ok, Socket2} = gen_udp:open(0, [binary]),
    try
        select_takeover_protocol(Node1, Node2, legacy),
        send_connect_msg(Socket1, Port1, ClientId),
        ?assertEqual(<<3, ?SN_CONNACK, 0>>, receive_response(Socket1)),
        send_disconnect_msg(Socket1, Port1, 60),
        ?assertEqual(<<2, ?SN_DISCONNECT>>, receive_response(Socket1)),
        [Pid1] = ?ON(Node1, emqx_gateway_cm:lookup_by_clientid(mqttsn, ClientId)),
        ok = mock_outgoing_channel_call(
            Node2,
            fun
                (Pid, {takeover, 'begin', _Owner, _} = Call, _) when Pid =:= Pid1 ->
                    %% Legacy begin must omit the explicit owner/attempt pair.
                    error({"Legacy owner received non-legacy takeover call", Call});
                (Pid, {takeover, 'end', _Owner} = Call, _) when Pid =:= Pid1 ->
                    %% Legacy completion must omit the explicit owner/attempt pair.
                    error({"Legacy owner received non-legacy takeover call", Call});
                (Pid, {takeover, 'begin', #{peercert := _}}, _) when Pid =:= Pid1 ->
                    %% Simulate an older owner without wakeup support, leaving OldPid untouched.
                    ignored;
                (Pid, Request, Timeout) ->
                    meck:passthrough([Pid, Request, Timeout])
            end
        ),
        send_pingreq_msg(Socket2, Port2, ClientId),
        ?assertEqual(<<2, ?SN_DISCONNECT>>, receive_response(Socket2)),
        ?assertMatch(#{conn_state := asleep}, local_chan_info(Node1, ClientId))
    after
        ?ON(Node2, meck:unload()),
        emqx_cth_cluster:stop(Nodes)
    end.

-doc "CONNECT takeover transfers registry, inflight and queued messages using the exported format.".
t_connect_takeover_exported(Config) ->
    test_connect_takeover(Config, current).

-doc "The legacy RPC boundary downgrades and upgrades session state without losing delivery order.".
t_connect_takeover_legacy(Config) ->
    test_connect_takeover(Config, legacy).

t_connect_takeover_pre_63(Config) ->
    test_connect_takeover(Config, pre63).

test_connect_takeover(Config, Protocol) ->
    ClientId = atom_to_binary(Protocol),
    Topic = <<"takeover/topic">>,
    {Nodes, Port1, Port2} = start_mqttsn_cluster(Config),
    [Node1, Node2] = Nodes,
    {ok, Socket1} = gen_udp:open(0, [binary]),
    {ok, Socket2} = gen_udp:open(0, [binary]),
    try
        select_takeover_protocol(Node1, Node2, Protocol),
        send_connect_msg(Socket1, Port1, ClientId, 0),
        ?assertEqual(<<3, ?SN_CONNACK, 0>>, receive_response(Socket1)),
        send_subscribe_msg_normal_topic(Socket1, Port1, 1, Topic, 1),
        <<8, ?SN_SUBACK, _, TopicId:16, 1:16, 0>> = receive_response(Socket1),
        publish(Node1, 1, Topic, <<"inflight">>),
        MsgId = receive_publish(Socket1, 0, 1, 0, 0, 0, TopicId, <<"inflight">>),
        send_disconnect_msg(Socket1, Port1, 60),
        ?assertEqual(<<2, ?SN_DISCONNECT>>, receive_response(Socket1)),
        publish(Node1, 1, Topic, <<"queued">>),
        [Pid1] = ?ON(Node1, emqx_gateway_cm:lookup_by_clientid(mqttsn, ClientId)),
        ?assertEqual(
            1,
            proplists:get_value(mqueue_len, ?ON(Node1, emqx_gateway_conn:stats(Pid1)))
        ),
        case Protocol of
            current ->
                ok;
            _Legacy ->
                %% Ordinary legacy takeover receives raw session state through the old RPC endpoint.
                ok = ?ON(Node2, meck:new(emqx_gateway_cm_proto_v1, [passthrough, no_link])),
                ok = ?ON(
                    Node2,
                    meck:expect(emqx_gateway_cm_proto_v1, takeover_session, fun
                        (mqttsn, ClientId0, Pid) when Pid =:= Pid1 ->
                            Result = meck:passthrough([mqttsn, ClientId0, Pid]),
                            ?assertMatch(
                                {ok, _, Pid, #{registry := _, session := S}} when is_tuple(S),
                                Result,
                                "Legacy takeover reply must contain a registry and session tuple"
                            ),
                            Result;
                        (Gateway, ClientId0, Pid) ->
                            meck:passthrough([Gateway, ClientId0, Pid])
                    end)
                )
        end,
        send_connect_msg(Socket2, Port2, ClientId, 0),
        ?assertEqual(<<3, ?SN_CONNACK, 0>>, receive_response(Socket2)),
        ?assertEqual(
            MsgId,
            receive_publish(Socket2, _Dup = 1, 1, 0, 0, 0, TopicId, <<"inflight">>)
        ),
        ?assertEqual(
            udp_receive_timeout,
            receive_response(Socket2, 100)
        ),
        send_puback_msg(Socket2, Port2, TopicId, MsgId),
        NextMsgId = receive_publish(Socket2, 0, 1, 0, 0, 0, TopicId, <<"queued">>),
        send_puback_msg(Socket2, Port2, TopicId, NextMsgId)
    after
        ?ON(Node2, meck:unload()),
        emqx_cth_cluster:stop(Nodes)
    end.

-doc """
Mock the requester connection wrapper to inspect or replace calls to a remote owner.
Passthrough keeps the real owner MQTT-SN callback running. Mocking that callback
on the requester node would observe local handling rather than outgoing calls.
""".
mock_outgoing_channel_call(RequesterNode, CallFun) ->
    ok = ?ON(RequesterNode, meck:new(emqx_gateway_conn, [passthrough, no_link])),
    ok = ?ON(RequesterNode, meck:expect(emqx_gateway_conn, call, CallFun)).

select_takeover_protocol(Node1, Node2, Protocol) ->
    case Protocol of
        current ->
            ?assertMatch(
                V when is_integer(V) andalso V >= 1,
                ?ON(Node2, emqx_bpapi:supported_version(Node1, emqx_gateway_cm_takeover))
            );
        legacy ->
            ok = ?ON(Node2, meck:new(emqx_bpapi, [passthrough, no_link])),
            ok = ?ON(
                Node2,
                meck:expect(emqx_bpapi, supported_version, fun
                    (N, emqx_gateway_cm_takeover) when N =:= Node1 -> undefined;
                    (N, API) -> meck:passthrough([N, API])
                end)
            );
        pre63 ->
            ok = ?ON(Node2, meck:new(emqx_bpapi, [passthrough, no_link])),
            ok = ?ON(
                Node2,
                meck:expect(emqx_bpapi, supported_version, fun
                    (N, emqx_gateway_cm_takeover) when N =:= Node1 -> undefined;
                    (N, emqx_cm) when N =:= Node1 -> 3;
                    (N, API) -> meck:passthrough([N, API])
                end)
            ),
            %% The old RPC carries no requester metadata. Verify that its worker
            %% discovers Node2 through its parent, including the pre-6.3 branch.
            ok = ?ON(Node1, meck:new(emqx_bpapi, [passthrough, no_link])),
            ok = ?ON(
                Node1,
                meck:expect(emqx_bpapi, supported_version, fun
                    (N, emqx_cm) when N =:= Node2 -> 3;
                    (N, API) -> meck:passthrough([N, API])
                end)
            )
    end.

local_chan_info(Node, ClientId) ->
    Pids = erpc:call(Node, emqx_gateway_cm_registry, lookup_channels, [mqttsn, ClientId]),
    case [Pid || Pid <- Pids, node(Pid) =:= Node] of
        [Pid | _] ->
            erpc:call(Node, emqx_gateway_cm, get_chan_info, [mqttsn, ClientId, Pid]);
        [] ->
            undefined
    end.

start_mqttsn_cluster(Config) ->
    Port1 = emqx_common_test_helpers:select_free_port(udp),
    Port2 = emqx_common_test_helpers:select_free_port(udp),
    Apps1 = mqttsn_apps(Port1),
    Apps2 = mqttsn_apps(Port2),
    NodeSpecs = emqx_cth_cluster:mk_nodespecs(
        [
            {cluster_node_name(?FUNCTION_NAME, 1), #{apps => Apps1}},
            {cluster_node_name(?FUNCTION_NAME, 2), #{apps => Apps2}}
        ],
        #{
            work_dir => emqx_cth_suite:work_dir(?config(testcase, Config), Config),
            env_vars => [
                {"EMQX_SECURITY_PROFILE", atom_to_list(?config(security_profile, Config))}
            ]
        }
    ),
    [Node1, Node2] = Nodes = emqx_cth_cluster:start(NodeSpecs),
    ?retry(
        50,
        20,
        true = is_pid(erpc:call(Node1, erlang, whereis, [emqx_gateway_sup]))
    ),
    ?retry(
        50,
        20,
        true = is_pid(erpc:call(Node2, erlang, whereis, [emqx_gateway_sup]))
    ),
    {Nodes, Port1, Port2}.

cluster_node_name(TestCase, N) ->
    binary_to_atom(iolist_to_binary(io_lib:format("~s_~B", [TestCase, N]))).

mqttsn_apps(Port) ->
    [
        {emqx_conf, mqttsn_conf(Port)},
        emqx_gateway,
        emqx_auth
    ].

mqttsn_conf(Port) ->
    iolist_to_binary(
        io_lib:format(
            ~S"""
            gateway.mqttsn {
                gateway_id = 1
                broadcast = false
                enable_qos3 = true
                clientinfo_override {
                    username = "user1"
                    password = "pw123"
                }
                listeners.udp.default {
                    bind = "127.0.0.1:~B"
                    enable_authn = false
                }
            }
            """,
            [Port]
        )
    ).

publish(Node, QoS, TopicName, Payload) ->
    Msg = emqx_message:make(<<"ct">>, QoS, TopicName, Payload),
    _ = erpc:call(Node, emqx_broker, publish, [Msg]),
    ok.

receive_publish(Socket, Dup, QoS, Retain, WillBit, CleanSession, TopicId, Payload) ->
    UdpData = receive_response(Socket),
    emqx_sn_protocol_SUITE:check_publish_msg_on_udp(
        {Dup, QoS, Retain, WillBit, CleanSession, ?SN_NORMAL_TOPIC, TopicId, Payload},
        UdpData
    ).

send_connect_msg(Socket, Port, ClientId) ->
    send_connect_msg(Socket, Port, ClientId, 1).

send_connect_msg(Socket, Port, ClientId, CleanSession) ->
    Packet = emqx_sn_protocol_SUITE:make_connect_msg(ClientId, CleanSession),
    ok = gen_udp:send(Socket, ?HOST, Port, Packet).

send_subscribe_msg_normal_topic(Socket, Port, QoS, Topic, MsgId) ->
    MsgType = ?SN_SUBSCRIBE,
    Dup = 0,
    Retain = 0,
    Will = 0,
    CleanSession = 0,
    TopicIdType = ?SN_NORMAL_TOPIC,
    Length = byte_size(Topic) + 5,
    SubscribePacket =
        <<Length:8, MsgType:8, Dup:1, QoS:2, Retain:1, Will:1, CleanSession:1, TopicIdType:2,
            MsgId:16, Topic/binary>>,
    ok = gen_udp:send(Socket, ?HOST, Port, SubscribePacket).

send_disconnect_msg(Socket, Port, Duration) ->
    Packet = emqx_sn_protocol_SUITE:make_disconnect_msg(Duration),
    ok = gen_udp:send(Socket, ?HOST, Port, Packet).

send_pingreq_msg(Socket, Port, ClientId) ->
    Length = 2 + byte_size(ClientId),
    MsgType = ?SN_PINGREQ,
    PingReq = <<Length:8, MsgType:8, ClientId/binary>>,
    ok = gen_udp:send(Socket, ?HOST, Port, PingReq).

send_puback_msg(Socket, Port, TopicId, MsgId) ->
    Length = 7,
    MsgType = ?SN_PUBACK,
    ReturnCode = ?SN_RC_ACCEPTED,
    PubAckPacket = <<Length:8, MsgType:8, TopicId:16, MsgId:16, ReturnCode:8>>,
    ok = gen_udp:send(Socket, ?HOST, Port, PubAckPacket).

receive_response(Socket) ->
    receive_response(Socket, 2000).

receive_response(Socket, Timeout) ->
    receive
        {udp, Socket, _, _, Bin} ->
            Bin
    after Timeout ->
        udp_receive_timeout
    end.
