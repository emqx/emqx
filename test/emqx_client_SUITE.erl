%%--------------------------------------------------------------------
%% Copyright (c) 2018-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_client_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-import(lists, [nth/2]).

-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-define(TOPICS, [<<"TopicA">>,
                 <<"TopicA/B">>,
                 <<"Topic/C">>,
                 <<"TopicA/C">>,
                 <<"/TopicA">>
                ]).

-define(WILD_TOPICS, [<<"TopicA/+">>,
                      <<"+/C">>,
                      <<"#">>,
                      <<"/#">>,
                      <<"/+">>,
                      <<"+/+">>,
                      <<"TopicA/#">>
                     ]).

-define(do_receive_tcp_packet(TIMEOUT, SOCKET, PACKET, EXPRESS),
    receive
        {tcp, SOCKET, PACKET} -> EXPRESS
    after TIMEOUT ->
        ct:fail({receive_timeout, TIMEOUT})
    end).

-define(receive_tcp_packet(TIMEOUT, SOCKET, PACKET, EXPRESS),
    fun() ->
        ?do_receive_tcp_packet(TIMEOUT, SOCKET, PACKET, EXPRESS)
    end()).

all() ->
    [{group, mqttv3},
     {group, mqttv4},
     {group, mqttv5},
     {group, others},
     {group, bugfixes}
    ].

groups() ->
    [{mqttv3, [non_parallel_tests],
      [t_basic_v3
      ]},
     {mqttv4, [non_parallel_tests],
      [t_basic_v4,
       t_cm,
       t_cm_registry,
       %% t_will_message,
       %% t_offline_message_queueing,
       t_overlapping_subscriptions,
       %% t_keepalive,
       %% t_redelivery_on_reconnect,
       %% subscribe_failure_test,
       t_dollar_topics,
       t_sub_non_utf8_topic
      ]},
     {mqttv5, [non_parallel_tests],
      [t_basic_with_props_v5
      ]},
     {others, [non_parallel_tests],
      [t_username_as_clientid,
       t_certcn_as_clientid_default_config_tls,
       t_certcn_as_clientid_tlsv1_3,
       t_certcn_as_clientid_tlsv1_2
      ]},
     {bugfixes, [non_parallel_tests],
      [ t_qos2_no_pubrel_received
      , t_retry_timer_after_session_taken_over
      ]}
    ].

init_per_suite(Config) ->
    emqx_ct_helpers:boot_modules(all),
    emqx_ct_helpers:start_apps([], fun set_special_confs/1),
    Config.

end_per_suite(_Config) ->
    emqx_ct_helpers:stop_apps([]).

init_per_testcase(Func, Cfg) ->
    maybe_run_fun(setup, Func, Cfg).

end_per_testcase(Func, Cfg) ->
    maybe_run_fun(teardown, Func, Cfg).

maybe_run_fun(Tag, Func, Cfg) ->
    try ?MODULE:Func(Tag, Cfg)
    catch
        error:undef -> Cfg
    end.

set_special_confs(emqx) ->
    emqx_ct_helpers:change_emqx_opts(ssl_twoway, [{peer_cert_as_username, cn}]);
set_special_confs(_) ->
    ok.

%%--------------------------------------------------------------------
%% Test cases for MQTT v3
%%--------------------------------------------------------------------

t_basic_v3(_) ->
    t_basic([{proto_ver, v3}]).

%%--------------------------------------------------------------------
%% Test cases for MQTT v4
%%--------------------------------------------------------------------

t_basic_v4(_Config) ->
    t_basic([{proto_ver, v4}]).

t_cm(_) ->
    IdleTimeout = emqx_zone:get_env(external, idle_timeout, 30000),
    emqx_zone:set_env(external, idle_timeout, 1000),
    ClientId = <<"myclient">>,
    {ok, C} = emqtt:start_link([{clientid, ClientId}]),
    {ok, _} = emqtt:connect(C),
    ct:sleep(500),
    #{clientinfo := #{clientid := ClientId}} = emqx_cm:get_chan_info(ClientId),
    emqtt:subscribe(C, <<"mytopic">>, 0),
    ct:sleep(1200),
    Stats = emqx_cm:get_chan_stats(ClientId),
    ?assertEqual(1, proplists:get_value(subscriptions_cnt, Stats)),
    emqx_zone:set_env(external, idle_timeout, IdleTimeout).

t_cm_registry(_) ->
    Info = supervisor:which_children(emqx_cm_sup),
    {_, Pid, _, _} = lists:keyfind(registry, 1, Info),
    ignored = gen_server:call(Pid, <<"Unexpected call">>),
    gen_server:cast(Pid, <<"Unexpected cast">>),
    Pid ! <<"Unexpected info">>.

t_will_message(_Config) ->
    {ok, C1} = emqtt:start_link([{clean_start, true},
                                       {will_topic, nth(3, ?TOPICS)},
                                       {will_payload, <<"client disconnected">>},
                                       {keepalive, 1}]),
    {ok, _} = emqtt:connect(C1),

    {ok, C2} = emqtt:start_link(),
    {ok, _} = emqtt:connect(C2),

    {ok, _, [2]} = emqtt:subscribe(C2, nth(3, ?TOPICS), 2),
    timer:sleep(5),
    ok = emqtt:stop(C1),
    timer:sleep(5),
    ?assertEqual(1, length(recv_msgs(1))),
    ok = emqtt:disconnect(C2),
    ct:pal("Will message test succeeded").

t_offline_message_queueing(_) ->
    {ok, C1} = emqtt:start_link([{clean_start, false},
                                       {clientid, <<"c1">>}]),
    {ok, _} = emqtt:connect(C1),

    {ok, _, [2]} = emqtt:subscribe(C1, nth(6, ?WILD_TOPICS), 2),
    ok = emqtt:disconnect(C1),
    {ok, C2} = emqtt:start_link([{clean_start, true},
                                       {clientid, <<"c2">>}]),
    {ok, _} = emqtt:connect(C2),

    ok = emqtt:publish(C2, nth(2, ?TOPICS), <<"qos 0">>, 0),
    {ok, _} = emqtt:publish(C2, nth(3, ?TOPICS), <<"qos 1">>, 1),
    {ok, _} = emqtt:publish(C2, nth(4, ?TOPICS), <<"qos 2">>, 2),
    timer:sleep(10),
    emqtt:disconnect(C2),
    {ok, C3} = emqtt:start_link([{clean_start, false}, {clientid, <<"c1">>}]),
    {ok, _} = emqtt:connect(C3),

    timer:sleep(10),
    emqtt:disconnect(C3),
    ?assertEqual(3, length(recv_msgs(3))).

t_overlapping_subscriptions(_) ->
    {ok, C} = emqtt:start_link([]),
    {ok, _} = emqtt:connect(C),

    {ok, _, [2, 1]} = emqtt:subscribe(C, [{nth(7, ?WILD_TOPICS), 2},
                                                {nth(1, ?WILD_TOPICS), 1}]),
    timer:sleep(10),
    {ok, _} = emqtt:publish(C, nth(4, ?TOPICS), <<"overlapping topic filters">>, 2),
    timer:sleep(10),

    Num = length(recv_msgs(2)),
    ?assert(lists:member(Num, [1, 2])),
    if
        Num == 1 ->
            ct:pal("This server is publishing one message for all
                   matching overlapping subscriptions, not one for each.");
        Num == 2 ->
            ct:pal("This server is publishing one message per each
                    matching overlapping subscription.");
        true -> ok
    end,
    emqtt:disconnect(C).

%% t_keepalive_test(_) ->
%%     ct:print("Keepalive test starting"),
%%     {ok, C1, _} = emqtt:start_link([{clean_start, true},
%%                                           {keepalive, 5},
%%                                           {will_flag, true},
%%                                           {will_topic, nth(5, ?TOPICS)},
%%                                           %% {will_qos, 2},
%%                                           {will_payload, <<"keepalive expiry">>}]),
%%     ok = emqtt:pause(C1),
%%     {ok, C2, _} = emqtt:start_link([{clean_start, true},
%%                                           {keepalive, 0}]),
%%     {ok, _, [2]} = emqtt:subscribe(C2, nth(5, ?TOPICS), 2),
%%     ok = emqtt:disconnect(C2),
%%     ?assertEqual(1, length(recv_msgs(1))),
%%     ct:print("Keepalive test succeeded").

t_redelivery_on_reconnect(_) ->
    ct:pal("Redelivery on reconnect test starting"),
    {ok, C1} = emqtt:start_link([{clean_start, false}, {clientid, <<"c">>}]),
    {ok, _} = emqtt:connect(C1),

    {ok, _, [2]} = emqtt:subscribe(C1, nth(7, ?WILD_TOPICS), 2),
    timer:sleep(10),
    ok = emqtt:pause(C1),
    {ok, _} = emqtt:publish(C1, nth(2, ?TOPICS), <<>>,
                                  [{qos, 1}, {retain, false}]),
    {ok, _} = emqtt:publish(C1, nth(4, ?TOPICS), <<>>,
                                  [{qos, 2}, {retain, false}]),
    timer:sleep(10),
    ok = emqtt:disconnect(C1),
    ?assertEqual(0, length(recv_msgs(2))),
    {ok, C2} = emqtt:start_link([{clean_start, false}, {clientid, <<"c">>}]),
    {ok, _} = emqtt:connect(C2),

    timer:sleep(10),
    ok = emqtt:disconnect(C2),
    ?assertEqual(2, length(recv_msgs(2))).

%% t_subscribe_sys_topics(_) ->
%%     ct:print("Subscribe failure test starting"),
%%     {ok, C, _} = emqtt:start_link([]),
%%     {ok, _, [2]} = emqtt:subscribe(C, <<"$SYS/#">>, 2),
%%     timer:sleep(10),
%%     ct:print("Subscribe failure test succeeded").

t_dollar_topics(_) ->
    ct:pal("$ topics test starting"),
    {ok, C} = emqtt:start_link([{clean_start, true},
                                      {keepalive, 0}]),
    {ok, _} = emqtt:connect(C),

    {ok, _, [1]} = emqtt:subscribe(C, nth(6, ?WILD_TOPICS), 1),
    {ok, _} = emqtt:publish(C, << <<"$">>/binary, (nth(2, ?TOPICS))/binary>>,
                                  <<"test">>, [{qos, 1}, {retain, false}]),
    timer:sleep(10),
    ?assertEqual(0, length(recv_msgs(1))),
    ok = emqtt:disconnect(C),
    ct:pal("$ topics test succeeded").

t_sub_non_utf8_topic(_) ->
    {ok, Socket} = gen_tcp:connect({127,0,0,1}, 1883, [{active, true}, binary]),
    ConnPacket = emqx_frame:serialize(#mqtt_packet{
                    header = #mqtt_packet_header{type = 1},
                    variable = #mqtt_packet_connect{
                        clientid = <<"abcdefg">>
                    }
                }),
    %% gen_tcp:send(Socket, <<16,19,0,4,77,81,84,84,4,2,0,0,0,7,97,98,99,100,101,102,103>>).
    ok = gen_tcp:send(Socket, ConnPacket),
    receive
        {tcp, _, _ConnAck = <<32,2,0,0>>} -> ok
    after
        3000 -> ct:fail({connect_ack_not_recv, process_info(self(), messages)})
    end,
    %% gen_tcp:send(Socket, <<130,18,25,178,0,13,128,10,10,12,178,159,162,47,115,1,1,1,1,1>>).
    SubHeader = <<130,18,25,178>>,
    SubTopicLen = <<0,13>>,
    %% this is not a valid utf8 topic
    SubTopic = <<128,10,10,12,178,159,162,47,115,1,1,1,1>>,
    SubQoS = <<1>>,
    SubPacket = <<SubHeader/binary, SubTopicLen/binary, SubTopic/binary, SubQoS/binary>>,
    ok = gen_tcp:send(Socket, SubPacket),
    receive
        {tcp_closed, _} -> ok
    after
        3000 -> ct:fail({should_get_disconnected, process_info(self(), messages)})
    end,
    timer:sleep(1000),
    ListenerCounts = esockd:get_shutdown_count({'mqtt:tcp',{{0,0,0,0},1883}}),
    TopicInvalidCount = proplists:get_value(topic_filter_invalid, ListenerCounts),
    ?assert(is_integer(TopicInvalidCount) andalso TopicInvalidCount > 0),
    ok.

%%--------------------------------------------------------------------
%% Test cases for MQTT v5
%%--------------------------------------------------------------------

t_basic_with_props_v5(_) ->
    t_basic([{proto_ver, v5},
             {properties, #{'Receive-Maximum' => 4}}
            ]).

%%--------------------------------------------------------------------
%% General test cases.
%%--------------------------------------------------------------------

t_basic(_Opts) ->
    Topic = nth(1, ?TOPICS),
    {ok, C} = emqtt:start_link([{proto_ver, v4}]),
    {ok, _} = emqtt:connect(C),
    {ok, _, [1]} = emqtt:subscribe(C, Topic, qos1),
    {ok, _, [2]} = emqtt:subscribe(C, Topic, qos2),
    {ok, _} = emqtt:publish(C, Topic, <<"qos 2">>, 2),
    {ok, _} = emqtt:publish(C, Topic, <<"qos 2">>, 2),
    {ok, _} = emqtt:publish(C, Topic, <<"qos 2">>, 2),
    ?assertEqual(3, length(recv_msgs(3))),
    ok = emqtt:disconnect(C).

t_username_as_clientid(_) ->
    emqx_zone:set_env(external, use_username_as_clientid, true),
    Username = <<"usera">>,
    {ok, C} = emqtt:start_link([{username, Username}]),
    {ok, _} = emqtt:connect(C),
    #{clientinfo := #{clientid := Username}} = emqx_cm:get_chan_info(Username),
    emqtt:disconnect(C),
    erlang:process_flag(trap_exit, true),
    {ok, C1} = emqtt:start_link([{username, <<>>}]),
    ?assertEqual({error, {client_identifier_not_valid, undefined}}, emqtt:connect(C1)),
    receive
        {'EXIT', _, {shutdown, client_identifier_not_valid}} -> ok
    after 100 ->
        throw({error, "expect_client_identifier_not_valid"})
    end.

t_certcn_as_clientid_default_config_tls(_) ->
    tls_certcn_as_clientid(default).

t_certcn_as_clientid_tlsv1_3(_) ->
    tls_certcn_as_clientid('tlsv1.3').

t_certcn_as_clientid_tlsv1_2(_) ->
    tls_certcn_as_clientid('tlsv1.2').

t_qos2_no_pubrel_received(setup, Cfg) ->
    OldMaxAwaitRel = emqx_zone:get_env(external, max_awaiting_rel),
    OldWaitTimeout = emqx_zone:get_env(external, await_rel_timeout),
    emqx_zone:set_env(external, max_awaiting_rel, 2),
    emqx_zone:set_env(external, await_rel_timeout, 1),
    [{old_max_await_rel, OldMaxAwaitRel},
     {old_wait_timeout, OldWaitTimeout} | Cfg];
t_qos2_no_pubrel_received(teardown, Cfg) ->
    OldMaxAwaitRel = ?config(old_max_await_rel, Cfg),
    OldWaitTimeout = ?config(old_wait_timeout, Cfg),
    emqx_zone:set_env(external, max_awaiting_rel, OldMaxAwaitRel),
    emqx_zone:set_env(external, await_rel_timeout, OldWaitTimeout).
t_qos2_no_pubrel_received(_) ->
    %% [The Scenario]:
    %% Client --- CONN: clean_session=false --> EMQX
    %% Client --- PUB: QoS2 ---> EMQX
    %% Client --- PUB: QoS2 ---> EMQX
    %% Client --- ...       ---> EMQX
    %% Client <---- PUBREC ---- EMQX
    %% Client <---- PUBREC ---- EMQX
    %% Client <---- ...    ---- EMQX
    %% Client --- PUBREL --X--> EMQX (PUBREL not received)
    %% Client <--- DISCONN: RC_RECEIVE_MAXIMUM_EXCEEDED --- EMQX
    %%
    %% [A few hours later..]:
    %% Client --- CONN: clean_session=false --> EMQX
    %% Client --- PUB: QoS2 ---> EMQX
    %% Client <--- DISCONN: RC_RECEIVE_MAXIMUM_EXCEEDED --- EMQX (we should clear the awaiting_rel queue but it is still full).
    ct:pal("1. reconnect after awaiting_rel is cleared"),
    qos2_no_pubrel_received(fun
        (1) -> timer:sleep(1500); %% reconnect 1.5s later, ensure the await_rel_timeout triggered
        (2) -> ok
    end),
    ct:pal("2. reconnect before awaiting_rel is cleared"),
    qos2_no_pubrel_received(fun
        (1) -> ok; %% reconnect as fast as possiable, ensure the await_rel_timeout NOT triggered
        (2) -> timer:sleep(1500) %% send msgs 1.5s later, ensure the await_rel_timeout triggered
    end).

qos2_no_pubrel_received(Actions) ->
    ClientId = emqx_guid:to_hexstr(emqx_guid:gen()),
    Topic = <<"t/foo">>,
    {ok, Sock} = gen_tcp:connect("127.0.0.1", 1883, [], 3000),
    gen_tcp:send(Sock, make_connect_packet(ClientId, false)),
    timer:sleep(100),
    ok = gen_tcp:send(Sock, make_publish_packet(Topic, 2, 100, <<"foo">>)),
    ok = gen_tcp:send(Sock, make_publish_packet(Topic, 2, 101, <<"foo">>)),
    %% Send the 3rd publish with id 103 will got disconnected:
    %%  - "Dropped the qos2 packet 103 due to awaiting_rel is full".
    ?assertMatch(ok, gen_tcp:send(Sock, make_publish_packet(Topic, 2, 103, <<"foo">>))),
    %% not the connections should be closed by the server due to ?RC_RECEIVE_MAXIMUM_EXCEEDED
    receive
        {tcp_closed, Sock} -> ok
    after 500 ->
        ct:fail({wait_tcp_close_timeout, 500})
    end,

    %% before reconnecting
    action_point(Actions, 1),

    {ok, Sock1} = gen_tcp:connect("127.0.0.1", 1883, [], 3000),
    gen_tcp:send(Sock1, make_connect_packet(ClientId, false)),

    %% after connected and before sending any msgs
    action_point(Actions, 2),

    ok = gen_tcp:send(Sock1, make_publish_packet(Topic, 2, 104, <<"foo">>)),

    receive
        {tcp_closed, Sock1} ->
            %% this is the bug, the wait_rel queue should have expired but not cleared
            ct:fail(unexpected_disconnect)
    after 500 -> ok
    end,
    gen_tcp:close(Sock1),
    ok.

t_retry_timer_after_session_taken_over(setup, Cfg) ->
    %emqx_logger:set_log_level(debug),
    OldRetryInterval = emqx_zone:get_env(external, retry_interval),
    emqx_zone:set_env(external, retry_interval, 1),
    [{old_retry_interval, OldRetryInterval} | Cfg];
t_retry_timer_after_session_taken_over(teardown, Cfg) ->
    %emqx_logger:set_log_level(warning),
    OldRetryInterval = ?config(old_retry_interval, Cfg),
    emqx_zone:set_env(external, retry_interval, OldRetryInterval).
t_retry_timer_after_session_taken_over(_) ->
    %% [The Scenario]:
    %% Client --- CONN: clean_session=false --> EMQX
    %% Client <--- PUB: QoS1 --- EMQX
    %% Client --X-- PUBACK ----> EMQX (the client doesn't send PUBACK)
    %% Client <---- PUB: QoS1 ---- EMQX (resend the PUBLISH msg)
    %% Client --- DISCONN ---> EMQX
    %% [A few seconds later..]:
    %% Client --- CONN: clean_session=false --> EMQX
    %% Client <--- PUB: QoS1 --- EMQX (resume session and resend the inflight messages)
    %% Client --X-- PUBACK ----> EMQX (the client doesn't send PUBACK)
    TcpOpts = [binary, {active, true}],

    ClientId = emqx_guid:to_hexstr(emqx_guid:gen()),
    Topic = <<"t/foo">>,
    Payload = <<"DO NOT REPLY!">>,

    %% CONNECT
    {ok, Sock1} = gen_tcp:connect("127.0.0.1", 1883, TcpOpts, 3000),
    ok = gen_tcp:send(Sock1, make_connect_packet(ClientId, false)),
    ?receive_tcp_packet(1000, Sock1, _, ok),

    %% SUBSCRIBE
    ok = gen_tcp:send(Sock1, make_subscribe_packet(Topic, 2, 1)),
    ?receive_tcp_packet(200, Sock1, _, ok),

    emqx_broker:publish(emqx_message:make(<<"publisher">>, 1, Topic, Payload)),
    %% note that here we don't reply the publish with puback
    ?receive_tcp_packet(200, Sock1, PubPacket1,
        begin
            ?assertMatch({ok, ?PUBLISH_PACKET(1, Topic, 1, Payload), <<>>, _},
                emqx_frame:parse(PubPacket1))
        end),

    ok = gen_tcp:close(Sock1),
    timer:sleep(100),

    %% CONNECT again
    {ok, Sock2} = gen_tcp:connect("127.0.0.1", 1883, TcpOpts, 3000),
    ok = gen_tcp:send(Sock2, make_connect_packet(ClientId, false)),
    ConnAckRem = ?do_receive_tcp_packet(200, Sock2, ConnPack2, begin
        ConnAck = iolist_to_binary(make_connack_packet(?CONNACK_ACCEPT, 1)),
        ct:pal("--- connack: ~p, got: ~p", [ConnAck, ConnPack2]),
        <<ConnAck:4/binary, Rem/binary>> = ConnPack2,
        Rem
    end),

    %% emqx should resend the non-ACKed messages now
    PubPacket2 = case ConnAckRem of
        <<>> ->
            ?receive_tcp_packet(200, Sock2, Packet, Packet);
        _ ->
            ConnAckRem
    end,
    ?assertMatch({ok, ?PUBLISH_PACKET(1, Topic, 1, Payload), <<>>, _},
        emqx_frame:parse(PubPacket2)),

    %% ... and emqx should resend the message 1s later, as we didn't ACK it.
    ?receive_tcp_packet(1200, Sock2, Packet,
        begin
            ?assertMatch({ok, ?PUBLISH_PACKET(1, Topic, 1, Payload), <<>>, _},
                emqx_frame:parse(Packet))
        end),

    %% we ACK it now, then emqx should stop the resending
    ok = gen_tcp:send(Sock2, make_connect_packet(ClientId, false)),
    receive
        {tcp, Sock2, _PACKET} ->
            {ok, MqttPacket, <<>>, _} = emqx_frame:parse(_PACKET),
            ?assertNotMatch(?PUBLISH_PACKET(_), MqttPacket)
    after 1200 -> ok
    end,
    ok = gen_tcp:close(Sock1).

%%--------------------------------------------------------------------
%% Helper functions
%%--------------------------------------------------------------------

action_point(Action, N) ->
    Action(N).

make_connect_packet(ClientId, CleanStart) ->
    emqx_frame:serialize(?CONNECT_PACKET(
        #mqtt_packet_connect{
            proto_name = <<"MQTT">>,
            clean_start = CleanStart,
            keepalive = 0,
            clientid = ClientId
        }
    )).
make_connack_packet(Code, SP) ->
    emqx_frame:serialize(?CONNACK_PACKET(Code, SP)).

make_publish_packet(Topic, QoS, PacketId, Payload) ->
    emqx_frame:serialize(
        ?PUBLISH_PACKET(QoS, Topic, PacketId, Payload)).

make_subscribe_packet(TopicFileter, QoS, PacketId) ->
    emqx_frame:serialize(
        ?SUBSCRIBE_PACKET(PacketId, [{TopicFileter, #{rh => 0, rap => 0, nl => 0, qos => QoS}}])).

recv_msgs(Count) ->
    recv_msgs(Count, []).

recv_msgs(0, Msgs) ->
    Msgs;
recv_msgs(Count, Msgs) ->
    receive
        {publish, Msg} ->
            recv_msgs(Count-1, [Msg|Msgs]);
        _Other -> recv_msgs(Count, Msgs) %%TODO:: remove the branch?
    after 100 ->
        Msgs
    end.


confirm_tls_version( Client, RequiredProtocol ) ->
    Info = emqtt:info(Client),
    SocketInfo = proplists:get_value( socket, Info ),
    %% emqtt_sock has #ssl_socket.ssl
    SSLSocket = element( 3, SocketInfo ),
    { ok, SSLInfo } = ssl:connection_information(SSLSocket),
    Protocol = proplists:get_value( protocol, SSLInfo ),
    RequiredProtocol = Protocol.


tls_certcn_as_clientid(default = TLSVsn) ->
    tls_certcn_as_clientid(TLSVsn, 'tlsv1.3');
tls_certcn_as_clientid(TLSVsn) ->
    tls_certcn_as_clientid(TLSVsn, TLSVsn).

tls_certcn_as_clientid(TLSVsn, RequiredTLSVsn) ->
    CN = <<"Client">>,
    emqx_zone:set_env(external, use_username_as_clientid, true),
    SslConf = emqx_ct_helpers:client_ssl_twoway(TLSVsn),
    {ok, Client} = emqtt:start_link([{port, 8883}, {ssl, true}, {ssl_opts, SslConf}]),
    {ok, _} = emqtt:connect(Client),
    #{clientinfo := #{clientid := CN}} = emqx_cm:get_chan_info(CN),
    confirm_tls_version( Client, RequiredTLSVsn ),
    %% verify that the peercert won't be stored in the conninfo
    [ChannPid] = emqx_cm:lookup_channels(CN),
    SysState = sys:get_state(ChannPid),
    ChannelRecord = lists:keyfind(channel, 1, tuple_to_list(SysState)),
    ConnInfo = lists:nth(2, tuple_to_list(ChannelRecord)),
    ?assertMatch(#{peercert := undefined}, ConnInfo),
    emqtt:disconnect(Client).
