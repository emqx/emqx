-module(simple_example3).

-include("emqx_sn.hrl").

-define(HOST, "localhost").
-define(PORT, 1884).

-export([start/0]).
-export([gen_register_packet/2]).

start() ->
    io:format("start to connect ~p:~p~n", [?HOST, ?PORT]),

    %% create udp socket
    {ok, Socket} = gen_udp:open(0, [binary]),

    %% connect to emqx_sn broker
    Packet = gen_connect_packet(<<"client1">>),
    ok = gen_udp:send(Socket, ?HOST, ?PORT, Packet),
    io:format("send connect packet=~p~n", [Packet]),
    %% receive message
    wait_response(),

    %% subscribe  normal topic name
    SubscribePacket = gen_subscribe_packet(<<"T3">>),
    ok = gen_udp:send(Socket, ?HOST, ?PORT, SubscribePacket),
    io:format("send subscribe packet=~p~n", [SubscribePacket]),
    wait_response(),

    %% publish   SHORT TOPIC NAME
    PublishPacket = gen_publish_packet(<<"T3">>, <<"Payload...">>),
    ok = gen_udp:send(Socket, ?HOST, ?PORT, PublishPacket),
    io:format("send publish packet=~p~n", [PublishPacket]),
    wait_response(),

    % wait for subscribed message from broker
    wait_response(),

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

gen_subscribe_packet(ShortTopic) ->
    Length = 7,
    MsgType = ?SN_SUBSCRIBE,
    Dup = 0,
    Retain = 0,
    Will = 0,
    QoS = 1,
    CleanSession = 0,
    TopicIdType = 0,  % normal topic name
    Flag = <<Dup:1, QoS:2, Retain:1, Will:1, CleanSession:1, TopicIdType:2>>,
    MsgId = 1,
    <<Length:8, MsgType:8, Flag/binary, MsgId:16, ShortTopic/binary>>.

gen_register_packet(Topic, TopicId) ->
    Length = 6+byte_size(Topic),
    MsgType = ?SN_REGISTER,
    MsgId = 1,
    <<Length:8, MsgType:8, TopicId:16, MsgId:16, Topic/binary>>.

gen_publish_packet(ShortTopic, Payload) ->
    Length = 7+byte_size(Payload),
    MsgType = ?SN_PUBLISH,
    Dup = 0,
    QoS = 1,
    Retain = 0,
    Will = 0,
    CleanSession = 0,
    MsgId = 1,
    TopicIdType = 2,  % SHORT TOPIC NAME
    Flag = <<Dup:1, QoS:2, Retain:1, Will:1, CleanSession:1, TopicIdType:2>>,
    <<Length:8, MsgType:8, Flag/binary, ShortTopic/binary, MsgId:16, Payload/binary>>.

gen_disconnect_packet()->
    Length = 2,
    MsgType = ?SN_DISCONNECT,
    <<Length:8, MsgType:8>>.

wait_response() ->
    receive
        {udp, _Socket, _, _, Bin} ->
            case Bin of
                <<_Len:8, ?SN_PUBLISH, _Flag:8, TopicId:16, MsgId:16, Data/binary>> ->
                    io:format("recv publish TopicId: ~p, MsgId: ~p, Data: ~p~n", [TopicId, MsgId, Data]);
                <<_Len:8, ?SN_CONNACK, 0:8>> ->
                    io:format("recv connect ack~n");
                <<_Len:8, ?SN_REGACK, TopicId:16, MsgId:16, 0:8>> ->
                    io:format("recv regack TopicId=~p, MsgId=~p~n", [TopicId, MsgId]),
                    TopicId;
                <<_Len:8, ?SN_SUBACK, Flags:8, TopicId:16, MsgId:16, 0:8>> ->
                    io:format("recv suback Flags=~p TopicId=~p, MsgId=~p~n", [Flags, TopicId, MsgId]);
                <<_Len:8, ?SN_PUBACK, TopicId:16, MsgId:16, 0:8>> ->
                    io:format("recv puback TopicId=~p, MsgId=~p~n", [TopicId, MsgId]);
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
