%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_sn_proxy_cluster_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include("emqx_mqttsn.hrl").

-include_lib("common_test/include/ct.hrl").
-include_lib("emqx/include/asserts.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(HOST, {127, 0, 0, 1}).
-define(FNU, 0).

all() ->
    emqx_common_test_helpers:all(?MODULE).

t_asleep_pingreq_reroutes_across_nodes_and_retires_old_proxy(Config) ->
    QoS = 1,
    SleepDuration = 5,
    ClientId = <<"cluster-asleep-proxy-reroute">>,
    TopicName = <<"cluster/asleep/proxy/reroute">>,
    Payload1 = <<"cluster-queued-before-reroute">>,
    Payload2 = <<"cluster-queued-after-stale-old-tuple">>,
    MsgId = 41,
    Dup = 0,
    Retain = 0,
    WillBit = 0,
    CleanSession = 0,
    {Nodes, Port1, Port2} = start_mqttsn_cluster(Config),
    [Node1, _Node2] = Nodes,
    {ok, Socket1} = gen_udp:open(0, [binary]),
    {ok, Socket2} = gen_udp:open(0, [binary]),
    try
        send_connect_msg(Socket1, Port1, ClientId),
        ?assertEqual(<<3, ?SN_CONNACK, ?SN_RC_ACCEPTED>>, receive_response(Socket1)),

        send_subscribe_msg_normal_topic(Socket1, Port1, QoS, TopicName, MsgId),
        SubAck = receive_response(Socket1),
        ?assertMatch(
            <<8, ?SN_SUBACK, Dup:1, QoS:2, Retain:1, WillBit:1, CleanSession:1, ?SN_NORMAL_TOPIC:2,
                _TopicId:16, MsgId:16, ?SN_RC_ACCEPTED>>,
            SubAck
        ),
        <<8, ?SN_SUBACK, Dup:1, QoS:2, Retain:1, WillBit:1, CleanSession:1, ?SN_NORMAL_TOPIC:2,
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
            Socket2, Dup, QoS, Retain, WillBit, CleanSession, TopicId, Payload1
        ),
        send_puback_msg(Socket2, Port2, TopicId, PubMsgId1),
        ?assertEqual(<<2, ?SN_PINGRESP>>, receive_response(Socket2)),
        ?retry(
            50,
            20,
            #{conn_state := asleep} = erpc:call(Node1, emqx_gateway_cm, get_chan_info, [
                mqttsn, ClientId
            ])
        ),

        %% If the old node1 proxy was not retired, this stale datagram
        %% has no ClientId but is still routed to ClientId's old channel.
        send_disconnect_msg(Socket1, Port1, undefined),
        _ = receive_response(Socket1, 500),

        publish(Node1, QoS, TopicName, Payload2),
        timer:sleep(100),

        send_pingreq_msg(Socket2, Port2, ClientId),
        PubMsgId2 = receive_publish(
            Socket2, Dup, QoS, Retain, WillBit, CleanSession, TopicId, Payload2
        ),
        send_puback_msg(Socket2, Port2, TopicId, PubMsgId2),
        ?assertEqual(<<2, ?SN_PINGRESP>>, receive_response(Socket2))
    after
        _ = catch erpc:call(Node1, emqx_gateway_cm, kick_session, [mqttsn, ClientId]),
        gen_udp:close(Socket1),
        gen_udp:close(Socket2),
        emqx_cth_cluster:stop(Nodes)
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
        #{work_dir => emqx_cth_suite:work_dir(?FUNCTION_NAME, Config)}
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
            "gateway.mqttsn {\n"
            "  gateway_id = 1\n"
            "  broadcast = false\n"
            "  enable_qos3 = true\n"
            "  clientinfo_override {\n"
            "    username = \"user1\"\n"
            "    password = \"pw123\"\n"
            "  }\n"
            "  listeners.udp.default {\n"
            "    bind = \"127.0.0.1:~B\"\n"
            "  }\n"
            "}\n",
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
    Packet = emqx_sn_protocol_SUITE:make_connect_msg(ClientId, 1),
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
