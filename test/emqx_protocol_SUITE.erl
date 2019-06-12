%% Copyright (c) 2013-2019 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_protocol_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

-include_lib("common_test/include/ct.hrl").

-include("emqx_mqtt.hrl").

-define(TOPICS, [<<"TopicA">>, <<"TopicA/B">>, <<"Topic/C">>, <<"TopicA/C">>,
                 <<"/TopicA">>]).

-define(CLIENT, ?CONNECT_PACKET(#mqtt_packet_connect{
                                   client_id = <<"mqtt_client">>,
                                   username  = <<"emqx">>,
                                   password  = <<"public">>})).

all() ->
    [{group, mqtt_common},
     {group, mqttv4},
     {group, mqttv5},
     {group, acl},
     {group, frame_partial}].

groups() ->
    [{mqtt_common, [sequence],
      [will_topic_check,
       will_acl_check]},
     {mqttv4, [sequence],
      [connect_v4,
       subscribe_v4]},
     {mqttv5, [sequence],
      [connect_v5,
       subscribe_v5]},
     {acl, [sequence],
      [acl_deny_action_ct]},
     {frame_partial, [sequence],
      [handle_followed_packet]}].

init_per_suite(Config) ->
    emqx_ct_helpers:start_apps([], fun set_special_configs/1),
    MqttCaps = emqx_zone:get_env(external, '$mqtt_caps'),
    emqx_zone:set_env(external, '$mqtt_caps', MqttCaps#{max_topic_alias => 20}),
    Config.

end_per_suite(_Config) ->
    emqx_ct_helpers:stop_apps([]).

batch_connect(NumberOfConnections) ->
    batch_connect([], NumberOfConnections).

batch_connect(Socks, 0) ->
    Socks;
batch_connect(Socks, NumberOfConnections) ->
    {ok, Sock} = emqx_client_sock:connect({127, 0, 0, 1}, 1883,
                                          [binary, {packet, raw}, {active, false}],
                                          3000),
    batch_connect([Sock | Socks], NumberOfConnections - 1).

with_connection(DoFun, NumberOfConnections) ->
    Socks = batch_connect(NumberOfConnections),
    try
        DoFun(Socks)
    after
        lists:foreach(fun(Sock) ->
                         emqx_client_sock:close(Sock)
                      end, Socks)
    end.

with_connection(DoFun) ->
    with_connection(DoFun, 1).

handle_followed_packet(_Config) ->
    ConnPkt = <<16,12,0,4,77,81,84,84,4,2,0,60,0,0>>,
    PartialPkt1 = <<50,182,1,0,4,116,101,115,116,0,1,48,48,48,48,48,48,48,48,48,48,48,48,48,
                    48,48,48,48,48,48,48,48,48,48,48,48,48,48,48,48,48,48,48,48,48,48,48,48,
                    48,48,48,48,48,48,48,48,48,48,48,48,48,48,48,48,48,48,48,48,48,48,48,48,
                    48,48,48,48,48,48,48,48,48,48,48,48,48,48,48,48,48,48,48,48,48,48,48,48,
                    48,48,48,48,48,48,48,48,48,48,48,48,48,48,48,48,48,48,48,48,48,48,48,48>>,
    PartialPkt2 = <<48,48,48,48,48,48,48,48,48,48,48,48,48,48,48,48,48,48,48,48,48,48,48,48,
                    48,48,48,48,48,48,48,48,48,48,48,48,48,48,48,48,48,48,48,48,48,48,48,48,
                    48,48,48,48,48,48,48,48,48,48,48,48,48,48,48,48,48>>,

    %% This is a PUBLISH message (Qos=1)
    PubPkt = <<PartialPkt1/binary, PartialPkt2/binary>>,
    ComplexPkt = <<PubPkt/binary, PubPkt/binary, PubPkt/binary, PartialPkt1/binary>>,

    AssertConnAck = fun(R) -> ?assertEqual({ok, <<32,2,0,0>>}, R) end,
    AssertPubAck  = fun(R) -> ?assertEqual({ok, <<64,2,0,1>>}, R) end,

    {ok, Sock} = gen_tcp:connect("127.0.0.1", 1883, [{active, false}, binary]),

    %% CONNECT
    ok = gen_tcp:send(Sock, ConnPkt),
    AssertConnAck(gen_tcp:recv(Sock, 4, 500)),

    %% Once Publish
    ok = gen_tcp:send(Sock, PubPkt),
    AssertPubAck(gen_tcp:recv(Sock, 4, 500)),

    %% Complex Packet
    ok = gen_tcp:send(Sock, ComplexPkt),
    AssertPubAck(gen_tcp:recv(Sock, 4, 500)),
    AssertPubAck(gen_tcp:recv(Sock, 4, 500)),
    AssertPubAck(gen_tcp:recv(Sock, 4, 500)),

    ok = gen_tcp:send(Sock, PartialPkt2),
    AssertPubAck(gen_tcp:recv(Sock, 4, 500)),
    gen_tcp:close(Sock).

connect_v4(_) ->
    with_connection(fun([Sock]) ->
                            emqx_client_sock:send(Sock, raw_send_serialize(?PACKET(?PUBLISH))),
                            {error, closed} =gen_tcp:recv(Sock, 0)
                    end),
    with_connection(fun([Sock]) ->
                            ConnectPacket = raw_send_serialize(?CONNECT_PACKET
                                                                   (#mqtt_packet_connect{
                                                                       client_id = <<"mqttv4_client">>,
                                                                       username = <<"admin">>,
                                                                       password = <<"public">>,
                                                                       proto_ver = ?MQTT_PROTO_V4
                                                                      })),
                            emqx_client_sock:send(Sock, ConnectPacket),
                            {ok, Data} = gen_tcp:recv(Sock, 0),
                            {ok, ?CONNACK_PACKET(?CONNACK_ACCEPT), <<>>, _} = raw_recv_parse(Data, ?MQTT_PROTO_V4),

                            emqx_client_sock:send(Sock, ConnectPacket),
                            {error, closed} = gen_tcp:recv(Sock, 0)
                    end),
    ok.


connect_v5(_) ->
    with_connection(fun([Sock]) ->
                            emqx_client_sock:send(Sock,
                                                  raw_send_serialize(
                                                    ?CONNECT_PACKET(#mqtt_packet_connect{
                                                                       proto_ver  = ?MQTT_PROTO_V5,
                                                                       properties =
                                                                           #{'Request-Response-Information' => -1}}))),
                            {ok, Data} = gen_tcp:recv(Sock, 0),
                            {ok, ?DISCONNECT_PACKET(?RC_PROTOCOL_ERROR), <<>>, _} = raw_recv_parse(Data, ?MQTT_PROTO_V5)
                    end),

    with_connection(fun([Sock]) ->
                            emqx_client_sock:send(Sock,
                                                  raw_send_serialize(
                                                    ?CONNECT_PACKET(
                                                       #mqtt_packet_connect{
                                                          proto_ver  = ?MQTT_PROTO_V5,
                                                          properties =
                                                               #{'Request-Problem-Information' => 2}}))),
                            {ok, Data} = gen_tcp:recv(Sock, 0),
                            {ok, ?DISCONNECT_PACKET(?RC_PROTOCOL_ERROR), <<>>, _} = raw_recv_parse(Data, ?MQTT_PROTO_V5)
                    end),

    with_connection(fun([Sock]) ->
                            emqx_client_sock:send(Sock,
                                                  raw_send_serialize(
                                                    ?CONNECT_PACKET(
                                                       #mqtt_packet_connect{
                                                          proto_ver  = ?MQTT_PROTO_V5,
                                                          properties =
                                                              #{'Request-Response-Information' => 1}})
                                                  )),
                            {ok, Data} = gen_tcp:recv(Sock, 0),
                            {ok, ?CONNACK_PACKET(?RC_SUCCESS, 0, Props), <<>>, _} =
                                raw_recv_parse(Data, ?MQTT_PROTO_V5),
                            ?assertNot(maps:is_key('Response-Information', Props))
                    end),

    % topic alias = 0
    with_connection(fun([Sock]) ->

                            %% ct:log("emqx_protocol: ~p~n", [emqx_zone:get_zone(external, max_topic_alias)]),
                            emqx_client_sock:send(Sock,
                                                  raw_send_serialize(
                                                      ?CONNECT_PACKET(
                                                          #mqtt_packet_connect{
                                                              client_id  = "hello",
                                                              proto_ver  = ?MQTT_PROTO_V5,
                                                              properties =
                                                                  #{'Topic-Alias-Maximum' => 10}}),
                                                      #{version => ?MQTT_PROTO_V5}
                                                  )),
                            {ok, Data} = gen_tcp:recv(Sock, 0),
                            {ok, ?CONNACK_PACKET(?RC_SUCCESS, 0,
                                                 #{'Topic-Alias-Maximum' := 20}), <<>>, _} =
                                raw_recv_parse(Data, ?MQTT_PROTO_V5),
                            emqx_client_sock:send(Sock,
                                                  raw_send_serialize(
                                                      ?PUBLISH_PACKET(?QOS_1, <<"TopicA">>, 1, #{'Topic-Alias' => 0}, <<"hello">>),
                                                      #{version => ?MQTT_PROTO_V5}
                                                  )),

                            {ok, Data2} = gen_tcp:recv(Sock, 0),
                            {ok, ?DISCONNECT_PACKET(?RC_TOPIC_ALIAS_INVALID), <<>>, _} = raw_recv_parse(Data2, ?MQTT_PROTO_V5)
                    end),

    % topic alias maximum
    with_connection(fun([Sock]) ->
                            emqx_client_sock:send(Sock,
                                                  raw_send_serialize(
                                                      ?CONNECT_PACKET(
                                                          #mqtt_packet_connect{
                                                              proto_ver  = ?MQTT_PROTO_V5,
                                                              properties =
                                                                  #{'Topic-Alias-Maximum' => 10}}),
                                                      #{version => ?MQTT_PROTO_V5}
                                                  )),
                            {ok, Data} = gen_tcp:recv(Sock, 0),
                            {ok, ?CONNACK_PACKET(?RC_SUCCESS, 0,
                                                 #{'Topic-Alias-Maximum' := 20}), <<>>, _} =
                                raw_recv_parse(Data, ?MQTT_PROTO_V5),

                            emqx_client_sock:send(Sock, raw_send_serialize(?SUBSCRIBE_PACKET(1, [{<<"TopicA">>, #{rh  => 1,
                                             qos => ?QOS_2,
                                             rap => 0,
                                             nl  => 0,
                                             rc  => 0}}]), #{version => ?MQTT_PROTO_V5})),

                            {ok, Data2} = gen_tcp:recv(Sock, 0),
                            {ok, ?SUBACK_PACKET(1, #{}, [2]), <<>>, _} = raw_recv_parse(Data2, ?MQTT_PROTO_V5),

                            emqx_client_sock:send(Sock,
                                                  raw_send_serialize(
                                                      ?PUBLISH_PACKET(?QOS_1, <<"TopicA">>, 1, #{'Topic-Alias' => 15}, <<"hello">>),
                                                      #{version => ?MQTT_PROTO_V5}
                                                  )),

                            {ok, Data3} = gen_tcp:recv(Sock, 0),

                            {ok, ?PUBACK_PACKET(1, 0), <<>>, _} = raw_recv_parse(Data3, ?MQTT_PROTO_V5),

                            {ok, Data4} = gen_tcp:recv(Sock, 0),

                            {ok, ?PUBLISH_PACKET(?QOS_1, <<"TopicA">>, _, <<"hello">>), <<>>, _} = raw_recv_parse(Data4, ?MQTT_PROTO_V5),

                            emqx_client_sock:send(Sock,
                                                  raw_send_serialize(
                                                      ?PUBLISH_PACKET(?QOS_1, <<"TopicA">>, 2, #{'Topic-Alias' => 21}, <<"hello">>),
                                                      #{version => ?MQTT_PROTO_V5}
                                                  )),

                            {ok, Data5} = gen_tcp:recv(Sock, 0),
                            {ok, ?DISCONNECT_PACKET(?RC_TOPIC_ALIAS_INVALID), <<>>, _} = raw_recv_parse(Data5, ?MQTT_PROTO_V5)
                    end),

    % test clean start
    with_connection(fun([Sock]) ->
                            emqx_client_sock:send(Sock,
                                                    raw_send_serialize(
                                                        ?CONNECT_PACKET(
                                                            #mqtt_packet_connect{
                                                                proto_ver   = ?MQTT_PROTO_V5,
                                                                clean_start = true,
                                                                client_id   = <<"myclient">>,
                                                                properties  =
                                                                    #{'Session-Expiry-Interval' => 10}})
                                                  )),
                            {ok, Data} = gen_tcp:recv(Sock, 0),
                            {ok, ?CONNACK_PACKET(?RC_SUCCESS, 0), <<>>, _} = raw_recv_parse(Data, ?MQTT_PROTO_V5),
                            emqx_client_sock:send(Sock, raw_send_serialize(
                                                            ?DISCONNECT_PACKET(?RC_SUCCESS)
                                                  ))
                    end),

    timer:sleep(1000),

    with_connection(fun([Sock]) ->
                            emqx_client_sock:send(Sock,
                                                    raw_send_serialize(
                                                        ?CONNECT_PACKET(
                                                            #mqtt_packet_connect{
                                                                proto_ver   = ?MQTT_PROTO_V5,
                                                                clean_start = false,
                                                                client_id   = <<"myclient">>,
                                                                properties  =
                                                                    #{'Session-Expiry-Interval' => 10}})
                                                  )),
                            {ok, Data} = gen_tcp:recv(Sock, 0),
                            {ok, ?CONNACK_PACKET(?RC_SUCCESS, 1), <<>>, _} = raw_recv_parse(Data, ?MQTT_PROTO_V5)
                    end),

    % test will message publish and cancel
    with_connection(fun([Sock]) ->
                            emqx_client_sock:send(Sock,
                                                    raw_send_serialize(
                                                        ?CONNECT_PACKET(
                                                            #mqtt_packet_connect{
                                                                proto_ver    = ?MQTT_PROTO_V5,
                                                                clean_start  = true,
                                                                client_id    = <<"myclient">>,
                                                                will_flag    = true,
                                                                will_qos     = ?QOS_1,
                                                                will_retain  = false,
                                                                will_props   = #{'Will-Delay-Interval' => 5},
                                                                will_topic   = <<"TopicA">>,
                                                                will_payload = <<"will message">>,
                                                                properties   = #{'Session-Expiry-Interval' => 0}
                                                            }
                                                        )
                                                    )
                            ),
                            {ok, Data} = gen_tcp:recv(Sock, 0),
                            {ok, ?CONNACK_PACKET(?RC_SUCCESS, 0), <<>>, _} = raw_recv_parse(Data, ?MQTT_PROTO_V5),

                            {ok, Sock2} = emqx_client_sock:connect({127, 0, 0, 1}, 1883,
                                                                   [binary, {packet, raw},
                                                                    {active, false}], 3000),

                            do_connect(Sock2, ?MQTT_PROTO_V5),

                            emqx_client_sock:send(Sock2, raw_send_serialize(?SUBSCRIBE_PACKET(1, [{<<"TopicA">>, #{rh  => 1,
                                             qos => ?QOS_2,
                                             rap => 0,
                                             nl  => 0,
                                             rc  => 0}}]), #{version => ?MQTT_PROTO_V5})),

                            {ok, SubData} = gen_tcp:recv(Sock2, 0),
                            {ok, ?SUBACK_PACKET(1, #{}, [2]), <<>>, _} = raw_recv_parse(SubData, ?MQTT_PROTO_V5),

                            emqx_client_sock:send(Sock, raw_send_serialize(
                                                            ?DISCONNECT_PACKET(?RC_SUCCESS))),

                            {error, timeout} = gen_tcp:recv(Sock2, 0, 2000),

                            % session resumed
                            {ok, Sock3} = emqx_client_sock:connect({127, 0, 0, 1}, 1883,
                                                                   [binary, {packet, raw},
                                                                    {active, false}], 3000),

                            emqx_client_sock:send(Sock3,
                                                    raw_send_serialize(
                                                        ?CONNECT_PACKET(
                                                            #mqtt_packet_connect{
                                                                proto_ver    = ?MQTT_PROTO_V5,
                                                                clean_start  = false,
                                                                client_id    = <<"myclient">>,
                                                                will_flag    = true,
                                                                will_qos     = ?QOS_1,
                                                                will_retain  = false,
                                                                will_props   = #{'Will-Delay-Interval' => 5},
                                                                will_topic   = <<"TopicA">>,
                                                                will_payload = <<"will message 2">>,
                                                                properties   = #{'Session-Expiry-Interval' => 3}
                                                            }
                                                        ),
                                                        #{version => ?MQTT_PROTO_V5}
                                                    )
                            ),
                            {ok, Data3} = gen_tcp:recv(Sock3, 0),
                            {ok, ?CONNACK_PACKET(?RC_SUCCESS, 0), <<>>, _} = raw_recv_parse(Data3, ?MQTT_PROTO_V5),

                            emqx_client_sock:send(Sock3, raw_send_serialize(
                                                            ?DISCONNECT_PACKET(?RC_DISCONNECT_WITH_WILL_MESSAGE),
                                                            #{version => ?MQTT_PROTO_V5}
                                                        )
                            ),

                            {ok, WillData} = gen_tcp:recv(Sock2, 0, 5000),
                            {ok, ?PUBLISH_PACKET(?QOS_1, <<"TopicA">>, _, <<"will message 2">>), <<>>, _}
                                = raw_recv_parse(WillData, ?MQTT_PROTO_V5)
                    end),

    % duplicate client id
    with_connection(fun([Sock, Sock1]) ->
                            emqx_zone:set_env(external, use_username_as_clientid, true),
                            emqx_client_sock:send(Sock,
                                                    raw_send_serialize(
                                                        ?CONNECT_PACKET(
                                                            #mqtt_packet_connect{
                                                                proto_ver   = ?MQTT_PROTO_V5,
                                                                clean_start = true,
                                                                client_id   = <<"myclient">>,
                                                                properties  =
                                                                    #{'Session-Expiry-Interval' => 10}})
                                                  )),
                            {ok, Data} = gen_tcp:recv(Sock, 0),
                            {ok, ?CONNACK_PACKET(?RC_SUCCESS, 0), <<>>, _} = raw_recv_parse(Data, ?MQTT_PROTO_V5),

                            emqx_client_sock:send(Sock1,
                                                    raw_send_serialize(
                                                        ?CONNECT_PACKET(
                                                            #mqtt_packet_connect{
                                                                proto_ver   = ?MQTT_PROTO_V5,
                                                                clean_start = false,
                                                                client_id   = <<"myclient">>,
                                                                username    = <<"admin">>,
                                                                password    = <<"public">>,
                                                                properties  =
                                                                    #{'Session-Expiry-Interval' => 10}})
                                                  )),
                            {ok, Data1} = gen_tcp:recv(Sock1, 0),
                            {ok, ?CONNACK_PACKET(?RC_SUCCESS, 0), <<>>, _} = raw_recv_parse(Data1, ?MQTT_PROTO_V5),

                            emqx_client_sock:send(Sock, raw_send_serialize(?SUBSCRIBE_PACKET(1, [{<<"TopicA">>, #{rh  => 1,
                                                                                                                  qos => ?QOS_2,
                                                                                                                  rap => 0,
                                                                                                                  nl  => 0,
                                                                                                                  rc  => 0}}]),
                                                                            #{version => ?MQTT_PROTO_V5})),

                            {ok, SubData} = gen_tcp:recv(Sock, 0),
                            {ok, ?SUBACK_PACKET(1, #{}, [2]), <<>>, _} = raw_recv_parse(SubData, ?MQTT_PROTO_V5),

                            emqx_client_sock:send(Sock1, raw_send_serialize(?SUBSCRIBE_PACKET(1, [{<<"TopicA">>, #{rh  => 1,
                                                                                                                   qos => ?QOS_2,
                                                                                                                   rap => 0,
                                                                                                                   nl  => 0,
                                                                                                                   rc  => 0}}]),
                                                                            #{version => ?MQTT_PROTO_V5})),

                            {ok, SubData1} = gen_tcp:recv(Sock1, 0),
                            {ok, ?SUBACK_PACKET(1, #{}, [2]), <<>>, _} = raw_recv_parse(SubData1, ?MQTT_PROTO_V5)
                    end, 2),

    ok.

do_connect(Sock, ProtoVer) ->
    emqx_client_sock:send(Sock, raw_send_serialize(
                                  ?CONNECT_PACKET(
                                     #mqtt_packet_connect{
                                        client_id  = <<"mqtt_client">>,
                                        proto_ver  = ProtoVer
                                       }))),
    {ok, Data} = gen_tcp:recv(Sock, 0),
    {ok, ?CONNACK_PACKET(?CONNACK_ACCEPT), <<>>, _} = raw_recv_parse(Data, ProtoVer).

subscribe_v4(_) ->
    with_connection(fun([Sock]) ->
                            do_connect(Sock, ?MQTT_PROTO_V4),
                            SubPacket = raw_send_serialize(
                                          ?SUBSCRIBE_PACKET(15,
                                                            [{<<"topic">>, #{rh => 1,
                                                                             qos => ?QOS_2,
                                                                             rap => 0,
                                                                             nl => 0,
                                                                             rc => 0}}])),
                            emqx_client_sock:send(Sock, SubPacket),
                            {ok, Data} = gen_tcp:recv(Sock, 0),
                            {ok, ?SUBACK_PACKET(15, _), <<>>, _} = raw_recv_parse(Data, ?MQTT_PROTO_V4)
                    end),
    ok.

subscribe_v5(_) ->
    with_connection(fun([Sock]) ->
                            do_connect(Sock, ?MQTT_PROTO_V5),
                            SubPacket = raw_send_serialize(?SUBSCRIBE_PACKET(15, #{'Subscription-Identifier' => -1},[]),
                                                            #{version => ?MQTT_PROTO_V5}),
                            emqx_client_sock:send(Sock, SubPacket),
                            {ok, DisConnData} = gen_tcp:recv(Sock, 0),
                            {ok, ?DISCONNECT_PACKET(?RC_TOPIC_FILTER_INVALID), <<>>, _} =
                                raw_recv_parse(DisConnData, ?MQTT_PROTO_V5)
                    end),
    with_connection(fun([Sock]) ->
                            do_connect(Sock, ?MQTT_PROTO_V5),
                            SubPacket = raw_send_serialize(
                                          ?SUBSCRIBE_PACKET(0, #{}, [{<<"TopicQos0">>,
                                                                      #{rh => 1, qos => ?QOS_2,
                                                                        rap => 0, nl => 0,
                                                                        rc => 0}}]),
                                          #{version => ?MQTT_PROTO_V5}),
                            emqx_client_sock:send(Sock, SubPacket),
                            {ok, DisConnData} = gen_tcp:recv(Sock, 0),
                            ?assertMatch(
                               {ok, ?DISCONNECT_PACKET(?RC_MALFORMED_PACKET), <<>>, _},
                               raw_recv_parse(DisConnData, ?MQTT_PROTO_V5))
                    end),
    with_connection(fun([Sock]) ->
                            do_connect(Sock, ?MQTT_PROTO_V5),
                            SubPacket = raw_send_serialize(
                                          ?SUBSCRIBE_PACKET(1, #{'Subscription-Identifier' => 0},
                                                            [{<<"TopicQos0">>,
                                                              #{rh => 1, qos => ?QOS_2,
                                                                rap => 0, nl => 0,
                                                                rc => 0}}]),
                                          #{version => ?MQTT_PROTO_V5}),
                            emqx_client_sock:send(Sock, SubPacket),
                            {ok, DisConnData} = gen_tcp:recv(Sock, 0),
                            ?assertMatch(
                               {ok, ?DISCONNECT_PACKET(?RC_SUBSCRIPTION_IDENTIFIERS_NOT_SUPPORTED), <<>>, _},
                               raw_recv_parse(DisConnData, ?MQTT_PROTO_V5))
                    end),
    with_connection(fun([Sock]) ->
                            do_connect(Sock, ?MQTT_PROTO_V5),
                            SubPacket = raw_send_serialize(
                                          ?SUBSCRIBE_PACKET(1, #{'Subscription-Identifier' => 1},
                                                            [{<<"TopicQos0">>,
                                                              #{rh => 1, qos => ?QOS_2,
                                                                rap => 0, nl => 0,
                                                                rc => 0}}]),
                                          #{version => ?MQTT_PROTO_V5}),
                            emqx_client_sock:send(Sock, SubPacket),
                            {ok, SubData} = gen_tcp:recv(Sock, 0),
                            {ok, ?SUBACK_PACKET(1, #{}, [2]), <<>>, _}
                                = raw_recv_parse(SubData, ?MQTT_PROTO_V5)
                    end),
    ok.

publish_v4(_) ->
    ok.

publish_v5(_) ->
    ok.

raw_send_serialize(Packet) ->
    emqx_frame:serialize(Packet).

raw_send_serialize(Packet, Opts) ->
    emqx_frame:serialize(Packet, Opts).

raw_recv_parse(Bin, ProtoVer) ->
    emqx_frame:parse(Bin, emqx_frame:initial_parse_state(#{version => ProtoVer})).

acl_deny_action_ct(_) ->
    emqx_zone:set_env(external, acl_deny_action, disconnect),
    process_flag(trap_exit, true),
    [acl_deny_do_disconnect(subscribe, QoS, <<"acl_deny_action">>) || QoS <- lists:seq(0, 2)],
    [acl_deny_do_disconnect(publish, QoS, <<"acl_deny_action">>) || QoS <- lists:seq(0, 2)],
    emqx_zone:set_env(external, acl_deny_action, ignore),
    ok.

will_topic_check(_) ->
    {ok, Client} = emqx_client:start_link([{username, <<"emqx">>},
                                           {will_flag, true},
                                           {will_topic, <<"aaa">>},
                                           {will_payload, <<"I have died">>},
                                           {will_qos, 0}]),
    {ok, _} = emqx_client:connect(Client),

    {ok, T} = emqx_client:start_link([{client_id, <<"client">>}]),
    emqx_client:connect(T),
    emqx_client:subscribe(T, <<"aaa">>),
    ct:sleep(200),

    emqx_client:stop(Client),
    ct:sleep(100),
    false = is_process_alive(Client),
    emqx_ct_helpers:wait_mqtt_payload(<<"I have died">>),
    emqx_client:stop(T).

will_acl_check(_) ->
    %% The connection will be rejected if publishing of the will message is not allowed by
    %% ACL rules
    process_flag(trap_exit, true),
    {ok, Client} = emqx_client:start_link([{username, <<"pub_deny">>},
                                           {will_flag, true},
                                           {will_topic, <<"pub_deny">>},
                                           {will_payload, <<"I have died">>},
                                           {will_qos, 0}]),
    ?assertMatch({error,{_,_}}, emqx_client:connect(Client)).

acl_deny_do_disconnect(publish, QoS, Topic) ->
    process_flag(trap_exit, true),
    {ok, Client} = emqx_client:start_link([{username, <<"emqx">>}]),
    {ok, _} = emqx_client:connect(Client),
    emqx_client:publish(Client, Topic, <<"test">>, QoS),
    receive
        {disconnected, shutdown, tcp_closed} ->
            ct:pal(info, "[OK] after publish, client got disconnected: tcp_closed", []);
        {'EXIT', Client, {shutdown,tcp_closed}} ->
            ct:pal(info, "[OK] after publish, received exit: {shutdown,tcp_closed}"),
            false = is_process_alive(Client);
        {'EXIT', Client, Reason} ->
            ct:pal(info, "[OK] after publish, client got disconnected: ~p", [Reason])
    after 1000 -> ct:fail({timeout, wait_tcp_closed})
    end;

acl_deny_do_disconnect(subscribe, QoS, Topic) ->
    process_flag(trap_exit, true),
    {ok, Client} = emqx_client:start_link([{username, <<"emqx">>}]),
    {ok, _} = emqx_client:connect(Client),
    {ok, _, [128]} = emqx_client:subscribe(Client, Topic, QoS),
    receive
        {disconnected, shutdown, tcp_closed} ->
            ct:pal(info, "[OK] after subscribe, client got disconnected: tcp_closed", []);
        {'EXIT', Client, {shutdown,tcp_closed}} ->
            ct:pal(info, "[OK] after subscribe, received exit: {shutdown,tcp_closed}"),
            false = is_process_alive(Client);
        {'EXIT', Client, Reason} ->
            ct:pal(info, "[OK] after subscribe, client got disconnected: ~p", [Reason])
    after 1000 -> ct:fail({timeout, wait_tcp_closed})
    end.

set_special_configs(emqx) ->
    application:set_env(emqx, enable_acl_cache, false),
    application:set_env(emqx, plugins_loaded_file,
                        emqx_ct_helpers:deps_path(emqx, "test/emqx_SUITE_data/loaded_plugins")),
    application:set_env(emqx, acl_deny_action, disconnect),
    application:set_env(emqx, acl_file,
                        emqx_ct_helpers:deps_path(emqx, "test/emqx_access_SUITE_data/acl_deny_action.conf"));
set_special_configs(_App) ->
    ok.
