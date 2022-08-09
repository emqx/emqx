-module(simple_example4).

-include("emqx_sn.hrl").

-define(HOST, {127,0,0,1}).
-define(PORT, 1884).

-export([start/1]).

start(LoopTimes) ->
    io:format("start to connect ~p:~p~n", [?HOST, ?PORT]),

    %% create udp socket
    {ok, Socket} = gen_udp:open(0, [binary]),

    %% connect to emqx_sn broker
    Packet = gen_connect_packet(<<"client1">>),
    ok = gen_udp:send(Socket, ?HOST, ?PORT, Packet),
    io:format("send connect packet=~p~n", [Packet]),
    %% receive message
    wait_response(),

    %% register topic_id
    RegisterPacket = gen_register_packet(<<"TopicA">>, 0),
    ok = gen_udp:send(Socket, ?HOST, ?PORT, RegisterPacket),
    io:format("send register packet=~p~n", [RegisterPacket]),
    TopicId = wait_response(),

    %% subscribe
    SubscribePacket = gen_subscribe_packet(TopicId),
    ok = gen_udp:send(Socket, ?HOST, ?PORT, SubscribePacket),
    io:format("send subscribe packet=~p~n", [SubscribePacket]),
    wait_response(),

    %% loop publish
    [begin
         timer:sleep(1000),
         io:format("~n-------------------- publish ~p start --------------------~n", [N]),

         PublishPacket = gen_publish_packet(TopicId, <<"Payload...">>),
         ok = gen_udp:send(Socket, ?HOST, ?PORT, PublishPacket),
         io:format("send publish packet=~p~n", [PublishPacket]),
         % wait for publish ack
         wait_response(),
         % wait for subscribed message from broker
         wait_response(),

         PingReqPacket = gen_pingreq_packet(),
         ok = gen_udp:send(Socket, ?HOST, ?PORT, PingReqPacket),
         % wait for pingresp
         wait_response(),

         io:format("--------------------- publish ~p end ---------------------~n", [N])
     end || N <- lists:seq(1, LoopTimes)],

    %% disconnect from emqx_sn broker
    DisConnectPacket = gen_disconnect_packet(),
    ok = gen_udp:send(Socket, ?HOST, ?PORT, DisConnectPacket),
    io:format("send disconnect packet=~p~n", [DisConnectPacket]).



gen_connect_packet(ClientId) ->
    Length = 6+byte_size(ClientId),
    MsgType = ?SN_CONNECT,
    Dup = 0,
    QoS = 0,
    Retain = 0,
    Will = 0,
    CleanSession = 1,
    TopicIdType = 0,
    Flag = <<Dup:1, QoS:2, Retain:1, Will:1, CleanSession:1, TopicIdType:2>>,
    ProtocolId = 1,
    Duration = 10,
    <<Length:8, MsgType:8, Flag/binary, ProtocolId:8, Duration:16, ClientId/binary>>.

gen_subscribe_packet(TopicId) ->
    Length = 7,
    MsgType = ?SN_SUBSCRIBE,
    Dup = 0,
    Retain = 0,
    Will = 0,
    QoS = 1,
    CleanSession = 0,
    TopicIdType = 1,
    Flag = <<Dup:1, QoS:2, Retain:1, Will:1, CleanSession:1, TopicIdType:2>>,
    MsgId = 1,
    <<Length:8, MsgType:8, Flag/binary, MsgId:16, TopicId:16>>.

gen_register_packet(Topic, TopicId) ->
    Length = 6+byte_size(Topic),
    MsgType = ?SN_REGISTER,
    MsgId = 1,
    <<Length:8, MsgType:8, TopicId:16, MsgId:16, Topic/binary>>.

gen_publish_packet(TopicId, Payload) ->
    Length = 7+byte_size(Payload),
    MsgType = ?SN_PUBLISH,
    Dup = 0,
    QoS = 1,
    Retain = 0,
    Will = 0,
    CleanSession = 0,
    MsgId = 1,
    TopicIdType = 1,
    Flag = <<Dup:1, QoS:2, Retain:1, Will:1, CleanSession:1, TopicIdType:2>>,
    <<Length:8, MsgType:8, Flag/binary, TopicId:16, MsgId:16, Payload/binary>>.

gen_puback_packet(TopicId, MsgId) ->
    Length = 7,
    MsgType = ?SN_PUBACK,
    <<Length:8, MsgType:8, TopicId:16, MsgId:16, 0:8>>.

gen_pingreq_packet() ->
    Length = 2,
    MsgType = ?SN_PINGREQ,
    <<Length:8, MsgType:8>>.

gen_disconnect_packet()->
    Length = 2,
    MsgType = ?SN_DISCONNECT,
    <<Length:8, MsgType:8>>.

wait_response() ->
    receive
        {udp, Socket, _, _, Bin} ->
            case Bin of
                <<_Len:8, ?SN_PUBLISH, _Flag:8, TopicId:16, MsgId:16, Data/binary>> ->
                    io:format("recv publish TopicId: ~p, MsgId: ~p, Data: ~p~n", [TopicId, MsgId, Data]),
                    ok = gen_udp:send(Socket, ?HOST, ?PORT, gen_puback_packet(TopicId, MsgId));
                <<_Len:8, ?SN_CONNACK, 0:8>> ->
                    io:format("recv connect ack~n");
                <<_Len:8, ?SN_REGACK, TopicId:16, MsgId:16, 0:8>> ->
                    io:format("recv regack TopicId=~p, MsgId=~p~n", [TopicId, MsgId]),
                    TopicId;
                <<_Len:8, ?SN_SUBACK, Flags:8, TopicId:16, MsgId:16, 0:8>> ->
                    io:format("recv suback Flags=~p TopicId=~p, MsgId=~p~n", [Flags, TopicId, MsgId]);
                <<_Len:8, ?SN_PUBACK, TopicId:16, MsgId:16, 0:8>> ->
                    io:format("recv puback TopicId=~p, MsgId=~p~n", [TopicId, MsgId]);
                <<_Len:8, ?SN_PINGRESP>> ->
                    io:format("recv pingresp~n");
                _ ->
                    io:format("ignore bin=~p~n", [Bin])
            end;
        Any ->
            io:format("recv something else from udp socket ~p~n", [Any])
    after
        2000 ->
            io:format("Error: receive timeout!~n"),
            wait_response()
    end.
