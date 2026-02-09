%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_channel_await_rel_expire_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("emqx/include/emqx_hooks.hrl").

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [
            {emqx, "mqtt { await_rel_timeout = 100ms, max_awaiting_rel = 5 }"}
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    emqx_cth_suite:stop(?config(apps, Config)).

%%--------------------------------------------------------------------
%% Test cases
%%--------------------------------------------------------------------

%% We test here that Packet Ids are released even if the client is not connected
t_check_rel_cleanup(_Config) ->
    %% Connect client and fill the awaiting_rel queue
    Host = "127.0.0.1",
    Port = 1883,
    ConnectPacket = ?CONNECT_PACKET(
        #mqtt_packet_connect{
            proto_ver = ?MQTT_PROTO_V5,
            properties = #{'Session-Expiry-Interval' => 30000},
            clientid = <<"clientid">>,
            clean_start = false
        }
    ),
    {ok, Client0} = emqx_mqtt_test_client:start_link(Host, Port),
    ok = emqx_mqtt_test_client:send(Client0, ConnectPacket),
    {ok, ?CONNACK_PACKET(?RC_SUCCESS, _, _)} = emqx_mqtt_test_client:receive_packet(),
    ok = lists:foreach(
        fun(N) ->
            PacketId = N,
            ok = emqx_mqtt_test_client:publish(Client0, PacketId, <<"topic">>, <<"hello">>, 2),
            ?assertMatch(
                {ok, ?PUBREC_PACKET(PacketId, ?RC_NO_MATCHING_SUBSCRIBERS, _)},
                emqx_mqtt_test_client:receive_packet()
            )
        end,
        lists:seq(1, 5)
    ),

    %% Check that the queue is full
    ok = emqx_mqtt_test_client:publish(Client0, 6, <<"topic">>, <<"hello">>, 2),
    ?assertMatch(
        {ok, ?DISCONNECT_PACKET(?RC_RECEIVE_MAXIMUM_EXCEEDED, _)},
        emqx_mqtt_test_client:receive_packet()
    ),

    %% Wait for the queue to be cleaned up
    ct:sleep(200),

    %% Reconnect after the queue is cleaned up
    {ok, Client1} = emqx_mqtt_test_client:start_link(Host, Port),
    ok = emqx_mqtt_test_client:send(Client1, ConnectPacket),
    {ok, ?CONNACK_PACKET(?RC_SUCCESS, _, _)} = emqx_mqtt_test_client:receive_packet(),

    %% Check that the packets are expired
    PacketId = 6,
    ok = emqx_mqtt_test_client:publish(Client1, PacketId, <<"topic">>, <<"hello">>, 2),
    ?assertMatch(
        {ok, ?PUBREC_PACKET(PacketId, ?RC_NO_MATCHING_SUBSCRIBERS, _)},
        emqx_mqtt_test_client:receive_packet()
    ),

    %% Disconnect the client
    emqx_mqtt_test_client:stop(Client1).

t_qos2_dup_publish_not_redelivered_after_await_rel_expired(_Config) ->
    Topic = <<"topic/qos2/dup/rel-expire">>,
    Payload = <<"hello">>,
    PacketId = 100,
    Host = "127.0.0.1",
    Port = 1883,
    ConnectPacket = ?CONNECT_PACKET(
        #mqtt_packet_connect{
            proto_ver = ?MQTT_PROTO_V5,
            properties = #{'Session-Expiry-Interval' => 30000},
            clientid = <<"clientid-dup-check">>,
            clean_start = true
        }
    ),
    ok = emqx_broker:subscribe(Topic),
    try
        {ok, Client} = emqx_mqtt_test_client:start_link(Host, Port),
        ok = emqx_mqtt_test_client:send(Client, ConnectPacket),
        {ok, ?CONNACK_PACKET(?RC_SUCCESS, _, _)} = emqx_mqtt_test_client:receive_packet(),
        Publish = ?PUBLISH_PACKET(?QOS_2, Topic, PacketId, Payload),
        ok = emqx_mqtt_test_client:send(Client, Publish),
        ?assertMatch(
            {ok, ?PUBREC_PACKET(PacketId, ?RC_SUCCESS, _)},
            emqx_mqtt_test_client:receive_packet()
        ),
        ?assertMatch({deliver, Topic, #message{payload = Payload}}, receive_deliver(1000)),

        %% let awaiting_rel expire, then resend PUBLISH with DUP=1
        ct:sleep(200),
        HeaderDup = (Publish#mqtt_packet.header)#mqtt_packet_header{dup = true},
        PublishDup = Publish#mqtt_packet{header = HeaderDup},
        ok = emqx_mqtt_test_client:send(Client, PublishDup),
        ?assertMatch(
            {ok, ?PUBREC_PACKET(PacketId, ?RC_SUCCESS, _)},
            emqx_mqtt_test_client:receive_packet()
        ),
        ?assertEqual(timeout, receive_deliver(300)),
        ok = emqx_mqtt_test_client:stop(Client)
    after
        ok = emqx_broker:unsubscribe(Topic)
    end.

receive_deliver(Timeout) ->
    receive
        {deliver, Topic, #message{} = Msg} ->
            {deliver, Topic, Msg}
    after Timeout ->
        timeout
    end.
