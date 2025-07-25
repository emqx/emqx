%%--------------------------------------------------------------------
%% Copyright (c) 2018-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_client_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("emqx/include/emqx_hooks.hrl").
-include_lib("emqx/include/asserts.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-define(WAIT(EXPR, ATTEMPTS), ?retry(1000, ATTEMPTS, EXPR)).

all() ->
    [
        {group, gen_tcp_listener},
        {group, socket_listener}
    ].

groups() ->
    [
        {gen_tcp_listener, [], [
            {group, mqttv3},
            {group, mqttv4},
            {group, mqttv5},
            {group, others},
            {group, socket},
            {group, misbehaving}
        ]},
        {socket_listener, [], [
            {group, socket},
            {group, misbehaving}
        ]},
        {mqttv3, [], [
            t_basic,
            t_sock_closed_reason_normal,
            t_sock_closed_force_closed_by_client
        ]},
        {mqttv4, [], [
            t_basic,
            t_cm,
            t_cm_registry,
            %% t_will_message,
            t_offline_message_queueing,
            t_overlapping_subscriptions,
            %% t_keepalive,
            t_redelivery_on_reconnect,
            t_dollar_topics,
            t_sock_closed_reason_normal,
            t_sock_closed_force_closed_by_client
        ]},
        {mqttv5, [], [
            t_basic_with_props_v5,
            t_v5_receive_maximim_in_connack,
            t_sock_closed_reason_normal,
            t_sock_closed_force_closed_by_client
        ]},
        {others, [], [
            t_username_as_clientid,
            t_certcn_as_alias,
            t_certdn_as_alias,
            t_client_attr_from_user_property,
            t_certcn_as_clientid_default_config_tls,
            t_certcn_as_clientid_tlsv1_3,
            t_certcn_as_clientid_tlsv1_2,
            t_peercert_preserved_before_connected,
            t_clientid_override,
            t_clientid_override_fail_with_empty_render_result,
            t_clientid_override_fail_with_expression_exception
        ]},
        {misbehaving, [], [
            t_sock_closed_instantly,
            t_sock_closed_quickly,
            t_sub_non_utf8_topic,
            t_congestion_send_timeout,
            t_congestion_decongested,
            t_occasional_missing_pubacks
        ]},
        {socket, [], [
            t_sock_keepalive,
            t_sock_closed_reason_normal,
            t_sock_closed_force_closed_by_client
        ]}
    ].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(gen_tcp_listener, Config) ->
    Apps = emqx_cth_suite:start(
        [
            {emqx,
                %% t_congestion_send_timeout
                "listeners.tcp.default.tcp_backend = gen_tcp\n"
                "listeners.tcp.default.tcp_options.send_timeout = 2500\n"
                "listeners.tcp.default.tcp_options.sndbuf = 4KB\n"
                "listeners.tcp.default.tcp_options.recbuf = 4KB\n"
                "listeners.tcp.default.tcp_options.high_watermark = 160KB\n"
                %% t_congestion_decongested
                "conn_congestion.min_alarm_sustain_duration = 0\n"
                %% others
                "listeners.ssl.default.ssl_options.verify = verify_peer\n"}
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{group_apps, Apps} | Config];
init_per_group(socket_listener, Config) ->
    Apps = emqx_cth_suite:start(
        [
            {emqx,
                %% t_congestion_send_timeout
                "listeners.tcp.default.tcp_backend = socket\n"
                "listeners.tcp.default.tcp_options.send_timeout = 2500\n"
                "listeners.tcp.default.tcp_options.sndbuf = 4KB\n"
                "listeners.tcp.default.tcp_options.recbuf = 4KB\n"
                %% t_congestion_decongested
                "conn_congestion.min_alarm_sustain_duration = 0\n"
                %% others
                "listeners.ssl.default.ssl_options.verify = verify_peer\n"}
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{group_apps, Apps} | Config];
init_per_group(mqttv3, Config) ->
    [{proto_ver, v3} | Config];
init_per_group(mqttv4, Config) ->
    [{proto_ver, v4} | Config];
init_per_group(mqttv5, Config) ->
    [{proto_ver, v5} | Config];
init_per_group(_GroupName, Config) ->
    Config.

end_per_group(gen_tcp_listener, Config) ->
    emqx_cth_suite:stop(?config(group_apps, Config));
end_per_group(socket_listener, Config) ->
    emqx_cth_suite:stop(?config(group_apps, Config));
end_per_group(_GroupName, _Config) ->
    ok.

init_per_testcase(_Case, Config) ->
    ok = snabbkaffe:start_trace(),
    Config.

end_per_testcase(_Case, _Config) ->
    ok = snabbkaffe:stop(),
    %% restore default values
    emqx_config:put_zone_conf(default, [mqtt, idle_timeout], 15000),
    emqx_config:put_zone_conf(default, [mqtt, use_username_as_clientid], false),
    emqx_config:put_zone_conf(default, [mqtt, peer_cert_as_clientid], disabled),
    emqx_config:put_zone_conf(default, [mqtt, client_attrs_init], []),
    emqx_config:put_zone_conf(default, [mqtt, clientid_override], disabled),
    emqx_config:put_zone_conf(default, [mqtt, max_inflight], 32),
    emqx_config:put_listener_conf(tcp, default, [tcp_options, keepalive], "none"),
    ok.

%%--------------------------------------------------------------------
%% Test cases for MQTT v4
%%--------------------------------------------------------------------

t_cm(_) ->
    emqx_config:put_zone_conf(default, [mqtt, idle_timeout], 1000),
    ClientId = atom_to_binary(?FUNCTION_NAME),
    {ok, C} = emqtt:start_link([{clientid, ClientId}]),
    {ok, _} = emqtt:connect(C),
    ?WAIT(#{clientinfo := #{clientid := ClientId}} = emqx_cm:get_chan_info(ClientId), 2),
    emqtt:subscribe(C, <<"mytopic">>, 0),
    ?WAIT(
        begin
            Stats = emqx_cm:get_chan_stats(ClientId),
            ?assertEqual(1, proplists:get_value(subscriptions_cnt, Stats))
        end,
        2
    ),
    ok.

t_idle_timeout_infinity(_) ->
    emqx_config:put_zone_conf(default, [mqtt, idle_timeout], infinity),
    ClientId = atom_to_binary(?FUNCTION_NAME),
    {ok, C} = emqtt:start_link([{clientid, ClientId}]),
    {ok, _} = emqtt:connect(C),
    ?WAIT(#{clientinfo := #{clientid := ClientId}} = emqx_cm:get_chan_info(ClientId), 2),
    {ok, _, [0]} = emqtt:subscribe(C, <<"mytopic">>, 0).

t_cm_registry(_) ->
    Children = supervisor:which_children(emqx_cm_sup),
    {_, Pid, _, _} = lists:keyfind(emqx_cm_registry, 1, Children),
    ignored = gen_server:call(Pid, <<"Unexpected call">>),
    gen_server:cast(Pid, <<"Unexpected cast">>),
    Pid ! <<"Unexpected info">>.

t_will_message(_Config) ->
    WillTopic = <<"TopicA/C">>,
    {ok, C1} = emqtt:start_link([
        {clean_start, true},
        {will_topic, WillTopic},
        {will_payload, <<"client disconnected">>},
        {keepalive, 1}
    ]),
    {ok, _} = emqtt:connect(C1),

    {ok, C2} = emqtt:start_link(),
    {ok, _} = emqtt:connect(C2),

    {ok, _, [2]} = emqtt:subscribe(C2, WillTopic, 2),
    ok = emqtt:stop(C1),
    ?assertEqual(1, length(recv_msgs(1))),
    ok = emqtt:disconnect(C2).

t_offline_message_queueing(_) ->
    {ok, C1} = emqtt:start_link([
        {clean_start, false},
        {clientid, <<"c1">>}
    ]),
    {ok, _} = emqtt:connect(C1),
    {ok, _, [2]} = emqtt:subscribe(C1, <<"+/+">>, 2),
    ok = emqtt:disconnect(C1),

    {ok, C2} = emqtt:start_link([
        {clean_start, true},
        {clientid, <<"c2">>}
    ]),
    {ok, _} = emqtt:connect(C2),

    ok = emqtt:publish(C2, <<"TopicA/B">>, <<"qos 0">>, 0),
    {ok, _} = emqtt:publish(C2, <<"Topic/C">>, <<"qos 1">>, 1),
    {ok, _} = emqtt:publish(C2, <<"TopicA/C">>, <<"qos 2">>, 2),
    timer:sleep(10),
    emqtt:disconnect(C2),

    {ok, C3} = emqtt:start_link([{clean_start, false}, {clientid, <<"c1">>}]),
    {ok, _} = emqtt:connect(C3),
    ?assertEqual(3, length(recv_msgs(3))),
    ok = emqtt:disconnect(C3).

t_overlapping_subscriptions(_) ->
    {ok, C} = emqtt:start_link([]),
    {ok, _} = emqtt:connect(C),

    {ok, _, [2, 1]} = emqtt:subscribe(C, [
        {<<"TopicA/#">>, 2},
        {<<"TopicA/+">>, 1}
    ]),
    timer:sleep(10),
    {ok, _} = emqtt:publish(C, <<"TopicA/C">>, <<"overlapping topic filters">>, 2),
    Num = length(recv_msgs(2)),
    ?assert(lists:member(Num, [1, 2])),
    if
        Num == 1 ->
            ct:pal(
                "This server is publishing one message for all\n"
                "                   matching overlapping subscriptions, not one for each."
            );
        Num == 2 ->
            ct:pal(
                "This server is publishing one message per each\n"
                "                    matching overlapping subscription."
            );
        true ->
            ok
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
    {ok, C1} = emqtt:start_link([{clean_start, false}, {clientid, <<"c">>}]),
    {ok, _} = emqtt:connect(C1),
    {ok, _, [2]} = emqtt:subscribe(C1, <<"TopicA/#">>, 2),
    timer:sleep(10),
    ok = emqtt:pause(C1),
    {ok, _} = emqtt:publish(
        C1,
        <<"TopicA/B">>,
        <<>>,
        [{qos, 1}, {retain, false}]
    ),
    {ok, _} = emqtt:publish(
        C1,
        <<"TopicA/C">>,
        <<>>,
        [{qos, 2}, {retain, false}]
    ),
    timer:sleep(10),
    ok = emqtt:disconnect(C1),
    ?assertEqual(0, length(recv_msgs(2))),
    {ok, C2} = emqtt:start_link([{clean_start, false}, {clientid, <<"c">>}]),
    {ok, _} = emqtt:connect(C2),
    ?assertEqual(2, length(recv_msgs(2))),
    ok = emqtt:disconnect(C2).

t_dollar_topics(_) ->
    {ok, C} = emqtt:start_link([
        {clean_start, true},
        {keepalive, 0}
    ]),
    {ok, _} = emqtt:connect(C),
    {ok, _, [1]} = emqtt:subscribe(C, <<"+/+">>, 1),
    {ok, _} = emqtt:publish(
        C,
        <<"$TopicA/B">>,
        <<"test">>,
        [{qos, 1}, {retain, false}]
    ),
    ?assertEqual(0, length(recv_msgs(1))),
    ok = emqtt:disconnect(C).

%%--------------------------------------------------------------------
%% Test cases for MQTT v5
%%--------------------------------------------------------------------

v5_conn_props(ReceiveMaximum, Config) ->
    [{properties, #{'Receive-Maximum' => ReceiveMaximum}} | Config].

t_basic_with_props_v5(Config) ->
    t_basic(v5_conn_props(4, Config)).

t_v5_receive_maximim_in_connack(Config) ->
    ReceiveMaximum = 7,
    {ok, C} = emqtt:start_link(v5_conn_props(ReceiveMaximum, Config)),
    {ok, Props} = emqtt:connect(C),
    ?assertMatch(#{'Receive-Maximum' := ReceiveMaximum}, Props),
    ok = emqtt:disconnect(C),
    ok.

%%--------------------------------------------------------------------
%% General test cases.
%%--------------------------------------------------------------------

t_basic(Opts) ->
    Topic = <<"TopicA">>,
    {ok, C} = emqtt:start_link(Opts),
    {ok, _} = emqtt:connect(C),
    {ok, _, [1]} = emqtt:subscribe(C, Topic, qos1),
    {ok, _, [2]} = emqtt:subscribe(C, Topic, qos2),
    {ok, _} = emqtt:publish(C, Topic, <<"qos 2">>, 2),
    {ok, _} = emqtt:publish(C, Topic, <<"qos 2">>, 2),
    {ok, _} = emqtt:publish(C, Topic, <<"qos 2">>, 2),
    ?assertEqual(3, length(recv_msgs(3))),
    ok = emqtt:disconnect(C).

t_username_as_clientid(_) ->
    emqx_config:put_zone_conf(default, [mqtt, use_username_as_clientid], true),
    Username = <<"usera">>,
    {ok, C} = emqtt:start_link([{username, Username}]),
    {ok, _} = emqtt:connect(C),
    #{clientinfo := #{clientid := Username}} = emqx_cm:get_chan_info(Username),
    erlang:process_flag(trap_exit, true),
    {ok, C1} = emqtt:start_link([{username, <<>>}]),
    ?assertEqual({error, {client_identifier_not_valid, undefined}}, emqtt:connect(C1)),
    receive
        {'EXIT', _, {shutdown, client_identifier_not_valid}} -> ok
    after 100 ->
        throw({error, "expect_client_identifier_not_valid"})
    end,
    emqtt:disconnect(C).

t_certcn_as_alias(_) ->
    test_cert_extraction_as_alias(cn).

t_certdn_as_alias(_) ->
    test_cert_extraction_as_alias(dn).

test_cert_extraction_as_alias(Which) ->
    %% extract the first two chars
    ClientId = iolist_to_binary(["ClientIdFor_", atom_to_list(Which)]),
    {ok, Compiled} = emqx_variform:compile("substr(" ++ atom_to_list(Which) ++ ",0,2)"),
    emqx_config:put_zone_conf(default, [mqtt, client_attrs_init], [
        #{
            expression => Compiled,
            set_as_attr => <<"alias">>
        }
    ]),
    SslConf = emqx_common_test_helpers:client_mtls('tlsv1.2'),
    {ok, Client} = emqtt:start_link([
        {clientid, ClientId}, {port, 8883}, {ssl, true}, {ssl_opts, SslConf}
    ]),
    {ok, _} = emqtt:connect(Client),
    %% assert only two chars are extracted
    ?assertMatch(
        #{clientinfo := #{client_attrs := #{<<"alias">> := <<_, _>>}}},
        emqx_cm:get_chan_info(ClientId)
    ),
    emqtt:disconnect(Client).

t_client_attr_from_user_property(_Config) ->
    ClientId = atom_to_binary(?FUNCTION_NAME),
    {ok, Compiled} = emqx_variform:compile("user_property.group"),
    emqx_config:put_zone_conf(default, [mqtt, client_attrs_init], [
        #{
            expression => Compiled,
            set_as_attr => <<"group">>
        },
        #{
            expression => Compiled,
            set_as_attr => <<"group2">>
        }
    ]),
    SslConf = emqx_common_test_helpers:client_mtls('tlsv1.3'),
    {ok, Client} = emqtt:start_link([
        {clientid, ClientId},
        {port, 8883},
        {ssl, true},
        {ssl_opts, SslConf},
        {proto_ver, v5},
        {properties, #{'User-Property' => [{<<"group">>, <<"g1">>}]}}
    ]),
    {ok, _} = emqtt:connect(Client),
    %% assert only two chars are extracted
    ?assertMatch(
        #{clientinfo := #{client_attrs := #{<<"group">> := <<"g1">>, <<"group2">> := <<"g1">>}}},
        emqx_cm:get_chan_info(ClientId)
    ),
    emqtt:disconnect(Client).

t_sock_keepalive(Config) ->
    %% Configure TCP Keepalive:
    ok = emqx_config:put_listener_conf(tcp, default, [tcp_options, keepalive], "1,1,5"),
    %% Connect MQTT client:
    ClientId = atom_to_binary(?FUNCTION_NAME),
    {ok, C} = emqtt:start_link([{clientid, ClientId} | Config]),
    {
        {ok, _},
        {ok, #{?snk_meta := #{pid := CPid}}}
    } = ?wait_async_action(emqtt:connect(C), #{?snk_kind := connection_started}),
    %% Verify TCP settings handled smoothly:
    %% If actual keepalive probes are going around is notoriously difficult to verify.
    MRef = erlang:monitor(process, CPid),
    ok = timer:sleep(1_000),
    ok = emqtt:disconnect(C),
    ?assertReceive({'DOWN', MRef, process, CPid, normal}).

t_sock_closed_reason_normal(Config) ->
    ClientId = atom_to_binary(?FUNCTION_NAME),
    ?check_trace(
        begin
            {ok, C} = emqtt:start_link([{clientid, ClientId} | Config]),
            {ok, _} = emqtt:connect(C),
            ?wait_async_action(
                emqtt:disconnect(C),
                #{?snk_kind := sock_closed_normal},
                5_000
            )
        end,
        fun(Trace0) ->
            ?assertMatch([#{clientid := ClientId}], ?of_kind(sock_closed_normal, Trace0)),
            ok
        end
    ).

t_sock_closed_force_closed_by_client(Config) ->
    ClientId = atom_to_binary(?FUNCTION_NAME),
    ?check_trace(
        begin
            {ok, C} = emqtt:start_link([{clientid, ClientId} | Config]),
            {ok, _} = emqtt:connect(C),
            true = erlang:unlink(C),
            ?wait_async_action(
                exit(C, kill),
                #{?snk_kind := sock_closed_with_other_reason},
                5_000
            )
        end,
        fun(Trace0) ->
            ?assertMatch(
                [#{clientid := ClientId}], ?of_kind(sock_closed_with_other_reason, Trace0)
            ),
            ok
        end
    ).

t_clientid_override(_) ->
    ClientId = <<"original-clientid-0">>,
    Username = <<"username1">>,
    Override = <<"username">>,
    {ok, Rule1} = emqx_variform:compile(Override),
    emqx_config:put_zone_conf(default, [mqtt, clientid_override], Rule1),
    {ok, Client} = emqtt:start_link([{clientid, ClientId}, {port, 1883}, {username, Username}]),
    {ok, _} = emqtt:connect(Client),
    ?assertMatch(#{clientid := Username}, maps:get(clientinfo, emqx_cm:get_chan_info(Username))),
    ?assertMatch(undefined, emqx_cm:get_chan_info(ClientId)),
    emqtt:disconnect(Client).

t_clientid_override_fail_with_empty_render_result(_) ->
    test_clientid_override_fail(<<"original-clientid-1">>, <<"undefined_var">>).

t_clientid_override_fail_with_expression_exception(_) ->
    test_clientid_override_fail(<<"original-clientid-2">>, <<"nth(1,undefined_var)">>).

test_clientid_override_fail(ClientId, Expr) ->
    {ok, Rule1} = emqx_variform:compile(Expr),
    emqx_config:put_zone_conf(default, [mqtt, clientid_override], Rule1),
    {ok, Client} = emqtt:start_link([{clientid, ClientId}, {port, 1883}]),
    {ok, _} = emqtt:connect(Client),
    ?assertMatch(#{clientid := ClientId}, maps:get(clientinfo, emqx_cm:get_chan_info(ClientId))),
    emqtt:disconnect(Client).

t_certcn_as_clientid_default_config_tls(_) ->
    tls_certcn_as_clientid(default).

t_certcn_as_clientid_tlsv1_3(_) ->
    tls_certcn_as_clientid('tlsv1.3').

t_certcn_as_clientid_tlsv1_2(_) ->
    tls_certcn_as_clientid('tlsv1.2').

t_peercert_preserved_before_connected(_) ->
    ok = emqx_config:put_zone_conf(default, [mqtt, peer_cert_as_clientid], false),
    ok = emqx_hooks:add(
        'client.connect',
        {?MODULE, on_hook, ['client.connect', self()]},
        ?HP_HIGHEST
    ),
    ok = emqx_hooks:add(
        'client.connected',
        {?MODULE, on_hook, ['client.connected', self()]},
        ?HP_HIGHEST
    ),
    ClientId = atom_to_binary(?FUNCTION_NAME),
    SslConf = emqx_common_test_helpers:client_mtls(default),
    {ok, Client} = emqtt:start_link([
        {port, 8883},
        {clientid, ClientId},
        {ssl, true},
        {ssl_opts, SslConf}
    ]),
    {ok, _} = emqtt:connect(Client),
    _ = ?assertReceive({'client.connect', #{peercert := PC}} when is_binary(PC)),
    _ = ?assertReceive({'client.connected', #{peercert := PC}} when is_binary(PC)),
    [ConnPid] = emqx_cm:lookup_channels(ClientId),
    ?assertMatch(
        #{conninfo := ConnInfo} when not is_map_key(peercert, ConnInfo),
        emqx_connection:info(ConnPid)
    ),
    emqtt:disconnect(Client).

on_hook(ConnInfo, _, 'client.connect' = HP, Pid) ->
    _ = Pid ! {HP, ConnInfo},
    ok;
on_hook(_ClientInfo, ConnInfo, 'client.connected' = HP, Pid) ->
    _ = Pid ! {HP, ConnInfo},
    ok.

%%--------------------------------------------------------------------
%% Misbehaving clients
%%--------------------------------------------------------------------

t_sock_closed_instantly(_) ->
    %% Introduce scheduling delays:
    meck:new(esockd_transport, [no_history, passthrough]),
    meck:new(esockd_socket, [no_history, passthrough]),
    meck:expect(esockd_transport, type, fun meck_sched_delay/1),
    meck:expect(esockd_socket, type, fun meck_sched_delay/1),
    %% Start a tracing session, to catch exit reasons consistently:
    TS = trace:session_create(?MODULE, self(), []),
    %% Estabilish a connection:
    {
        {ok, Socket},
        {ok, #{?snk_meta := #{pid := CPid}}}
    } = ?wait_async_action(
        gen_tcp:connect({127, 0, 0, 1}, 1883, [{active, true}, binary]),
        #{?snk_kind := connection_started}
    ),
    %% Verify it handles instant socket close smoothly:
    trace:process(TS, CPid, true, [procs]),
    try
        ok = gen_tcp:close(Socket),
        ?assertReceive(
            {trace, CPid, exit, Reason} when
                Reason == {shutdown, tcp_closed} orelse Reason == normal
        )
    after
        trace:session_destroy(TS),
        meck:unload()
    end.

t_sock_closed_quickly(_) ->
    %% Start a tracing session:
    TS = trace:session_create(?MODULE, self(), []),
    %% Estabilish a connection:
    {
        {ok, Socket},
        {ok, #{?snk_meta := #{pid := CPid}}}
    } = ?wait_async_action(
        gen_tcp:connect({127, 0, 0, 1}, 1883, [{active, true}, binary]),
        #{?snk_kind := connection_started}
    ),
    %% Verify it handles quick socket close smoothly:
    trace:process(TS, CPid, true, [procs]),
    try
        ok = gen_tcp:close(Socket),
        ?assertReceive(
            {trace, CPid, exit, Reason} when
                Reason == {shutdown, tcp_closed} orelse Reason == normal
        )
    after
        trace:session_destroy(TS)
    end.

t_sub_non_utf8_topic(_) ->
    {ok, Socket} = gen_tcp:connect({127, 0, 0, 1}, 1883, [{active, true}, binary]),
    ConnPacket = ?CONNECT_PACKET(#mqtt_packet_connect{clientid = <<"abcdefg">>}),
    ok = gen_tcp:send(Socket, emqx_frame:serialize(ConnPacket)),
    receive
        {tcp, _, _ConnAck = <<32, 2, 0, 0>>} -> ok
    after 3000 -> ct:fail({connect_ack_not_recv, process_info(self(), messages)})
    end,
    SubHeader = <<130, 18, 25, 178>>,
    SubTopicLen = <<0, 13>>,
    %% this is not a valid utf8 topic
    SubTopic = <<128, 10, 10, 12, 178, 159, 162, 47, 115, 1, 1, 1, 1>>,
    SubQoS = <<1>>,
    SubPacket = <<SubHeader/binary, SubTopicLen/binary, SubTopic/binary, SubQoS/binary>>,
    ok = gen_tcp:send(Socket, SubPacket),
    receive
        {tcp_closed, _} -> ok
    after 3000 -> ct:fail({should_get_disconnected, process_info(self(), messages)})
    end,
    timer:sleep(1000),
    ListenerCounts = emqx_listeners:shutdown_count('tcp:default', 1883),
    TopicInvalidCount = proplists:get_value(topic_filter_invalid, ListenerCounts),
    ?assert(is_integer(TopicInvalidCount) andalso TopicInvalidCount > 0),
    ok.

t_congestion_send_timeout(_) ->
    ok = emqx_config:put_zone_conf(default, [mqtt, idle_timeout], 1000),
    {ok, Socket} = gen_tcp:connect({127, 0, 0, 1}, 1883, [{active, false}, binary]),
    %% Send manually constructed CONNECT:
    ok = gen_tcp:send(
        Socket,
        emqx_frame:serialize(
            ?CONNECT_PACKET(#mqtt_packet_connect{clientid = <<"t_congestion_send_timeout">>})
        )
    ),
    {ok, Frames1} = gen_tcp:recv(Socket, 0, 1000),
    {Pkt1, <<>>, Parser1} = emqx_frame:parse(Frames1, emqx_frame:initial_parse_state()),
    ?assertMatch(?CONNACK_PACKET(0), Pkt1),
    %% Send manually constructed SUBSCRIBE to subscribe to "t":
    Topic = <<"t">>,
    ok = gen_tcp:send(
        Socket,
        emqx_frame:serialize(
            ?SUBSCRIBE_PACKET(1, [{Topic, #{rh => 0, rap => 0, nl => 0, qos => 0}}])
        )
    ),
    {ok, Frames2} = gen_tcp:recv(Socket, 0, 1000),
    {Pkt2, <<>>, _Parser2} = emqx_frame:parse(Frames2, Parser1),
    ?assertMatch(?SUBACK_PACKET(1, [0]), Pkt2),
    %% Subscribe to alarms:
    AlarmTopic = <<"$SYS/brokers/+/alarms/activate">>,
    ok = emqx_broker:subscribe(AlarmTopic),
    %% Start filling up send buffers:
    Publisher = fun Publisher(N) ->
        %% Each message has 8000 bytes payload:
        Payload = binary:copy(<<N:64>>, 1000),
        _ = emqx:publish(emqx_message:make(<<"publisher">>, Topic, Payload)),
        ok = timer:sleep(50),
        Publisher(N + 1)
    end,
    _PublisherPid = spawn_link(fun() -> Publisher(1) end),
    %% Start lagging consumer:
    Consumer = fun Consumer() ->
        case gen_tcp:recv(Socket, 1000, 1000) of
            {ok, _Bytes} ->
                ok = timer:sleep(50),
                Consumer();
            {error, closed} ->
                closed
        end
    end,
    _ConsumerPid = spawn_link(fun() -> Consumer() end),
    %% Congestion alarm should be raised soon:
    {deliver, _, AlarmMsg} = ?assertReceive({deliver, AlarmTopic, _AlarmMsg}, 5_000),
    #{
        <<"name">> := <<"conn_congestion/t_congestion_send_timeout/undefined">>,
        <<"details">> := AlarmDetails
    } = emqx_utils_json:decode(emqx_message:payload(AlarmMsg)),
    %% Connection should be closed once send timeout passes.
    ConnPid = list_to_pid(binary_to_list(maps:get(<<"pid">>, AlarmDetails))),
    MRef = erlang:monitor(process, ConnPid),
    ?assertReceive({'DOWN', MRef, process, ConnPid, {shutdown, send_timeout}}, 5_000),
    ok = gen_tcp:close(Socket).

t_congestion_decongested(_) ->
    ok = emqx_config:put_zone_conf(default, [mqtt, idle_timeout], 1000),
    {ok, Socket} = gen_tcp:connect({127, 0, 0, 1}, 1883, [{active, false}, binary]),
    %% Send manually constructed CONNECT:
    ok = gen_tcp:send(
        Socket,
        emqx_frame:serialize(
            ?CONNECT_PACKET(#mqtt_packet_connect{clientid = <<"t_congestion_decongested">>})
        )
    ),
    {ok, Frames1} = gen_tcp:recv(Socket, 0, 1000),
    {Pkt1, <<>>, Parser1} = emqx_frame:parse(Frames1, emqx_frame:initial_parse_state()),
    ?assertMatch(?CONNACK_PACKET(0), Pkt1),
    %% Send manually constructed SUBSCRIBE to subscribe to "t":
    Topic = <<"t">>,
    ok = gen_tcp:send(
        Socket,
        emqx_frame:serialize(
            ?SUBSCRIBE_PACKET(1, [{Topic, #{rh => 0, rap => 0, nl => 0, qos => 0}}])
        )
    ),
    {ok, Frames2} = gen_tcp:recv(Socket, 0, 1000),
    {Pkt2, <<>>, _Parser2} = emqx_frame:parse(Frames2, Parser1),
    ?assertMatch(?SUBACK_PACKET(1, [0]), Pkt2),
    %% Subscribe to alarms:
    ok = emqx_broker:subscribe(<<"$SYS/brokers/+/alarms/activate">>),
    ok = emqx_broker:subscribe(<<"$SYS/brokers/+/alarms/deactivate">>),
    %% Start filling up send buffers:
    Publisher = fun Publisher(N) ->
        %% Each message has 8000 bytes payload:
        Payload = binary:copy(<<N:64>>, 1000),
        _ = emqx:publish(emqx_message:make(<<"publisher">>, Topic, Payload)),
        ok = timer:sleep(50),
        Publisher(N + 1)
    end,
    PublisherPid = spawn_link(fun() -> Publisher(1) end),
    %% Start consumer, initially paused:
    Consumer = fun
        Consumer(paused) ->
            receive
                activate ->
                    Consumer(active)
            after 5_000 ->
                exit(activate_timeout)
            end;
        Consumer(active) ->
            case gen_tcp:recv(Socket, 0, 1000) of
                {ok, _Bytes} ->
                    Consumer(active);
                {error, timeout} ->
                    Consumer(active);
                {error, closed} ->
                    exit(closed)
            end
    end,
    ConsumerPid = spawn_link(fun() -> Consumer(paused) end),
    %% Congestion alarm should be raised soon:
    {deliver, _, AlarmActivated} =
        ?assertReceive({deliver, <<"$SYS/brokers/+/alarms/activate">>, _}, 5_000),
    ?assertMatch(
        #{<<"name">> := <<"conn_congestion/t_congestion_decongested/undefined">>},
        emqx_utils_json:decode(emqx_message:payload(AlarmActivated))
    ),
    %% Activate consumer, congestion should resolve soon:
    ConsumerPid ! activate,
    {deliver, _, AlarmDeactivated} =
        ?assertReceive({deliver, <<"$SYS/brokers/+/alarms/deactivate">>, _}, 5_000),
    ?assertMatch(
        #{<<"name">> := <<"conn_congestion/t_congestion_decongested/undefined">>},
        emqx_utils_json:decode(emqx_message:payload(AlarmDeactivated))
    ),
    %% Connection should be alive and well:
    ?assertMatch(
        SS when SS == idle; SS == running,
        emqx_cth_broker:connection_info(sockstate, <<"t_congestion_decongested">>)
    ),
    %% Cleanup:
    true = unlink(PublisherPid),
    true = unlink(ConsumerPid),
    exit(PublisherPid, shutdown),
    exit(ConsumerPid, shutdown),
    ok = gen_tcp:close(Socket).

%% Verify that occasional missing PUBACKs from clients do not cause any trouble,
%% even if Packet-ID wraps around.
t_occasional_missing_pubacks(_Config) ->
    %% Configure more generous Max-Inflight:
    MaxInflight = 100,
    ok = emqx_config:put_zone_conf(default, [mqtt, max_inflight], 100),
    %% Keep each 1000th message unacked:
    %% Meaning that session should be able to process ~100k messages before getting
    %% its inflight full, which is larger than Packet-ID range.
    UnackedEach = 1000,
    %% Few knobs for Consumer state machine:
    PubBatchSize = 500,
    DrainTime = 5,
    DrainStuckTimeout = 100,
    %% Start the client, tell it to not ack anything automatically:
    ClientId = atom_to_binary(?FUNCTION_NAME),
    {ok, Client} = emqtt:start_link([
        {port, 1883},
        {clientid, ClientId},
        {auto_ack, never}
    ]),
    {ok, _} = emqtt:connect(Client),
    {ok, _, [1]} = emqtt:subscribe(Client, <<"t/+">>, ?QOS_1),
    %% Monitor respective channel process:
    [ChanPid] = emqx_cm:lookup_channels(ClientId),
    _MRef = erlang:monitor(process, ChanPid),
    %% Consumer state machine:
    Consumer = fun
        %% Batch is fully produced, get back to draining incoming messages:
        Consumer({batch, 0}, St) ->
            Consumer({drain, DrainTime}, St);
        %% Producing batch of publishes directly through broker:
        Consumer({batch, NBatch}, St = #{n_pubs := NR}) ->
            Topic = emqx_topic:join(["t", integer_to_binary(NR)]),
            Msg = emqx_message:make(<<?MODULE_STRING>>, ?QOS_1, Topic, <<>>),
            _ = emqx:publish(Msg),
            Consumer({batch, NBatch - 1}, St#{n_pubs := NR + 1});
        %% Draining incoming messages:
        Consumer({drain, Time}, St = #{n_recv := NR, n_pubs := NPubs}) ->
            case ?drainMailbox(Time) of
                Publishes = [_ | _] ->
                    %% Process incoming messages:
                    Consumer(Publishes, St);
                [] when NR =:= NPubs ->
                    %% Need another batch:
                    Consumer({batch, PubBatchSize}, St);
                [] when Time > DrainStuckTimeout ->
                    %% No incoming message for a while, bail out:
                    {timeout, St};
                [] ->
                    %% Retry:
                    Consumer({drain, Time * 2}, St)
            end;
        %% Processing incoming message:
        Consumer([{publish, Msg} | Rest], St = #{n_recv := NR, unacked := Acc}) ->
            #{packet_id := PacketId} = Msg,
            case NR rem UnackedEach of
                1 when length(Acc) =:= MaxInflight - 1 ->
                    {full, St#{unacked := [PacketId | Acc]}};
                1 ->
                    ct:pal("[msgin] NR:~p skipping puback PacketId:~p", [NR, PacketId]),
                    Consumer(Rest, St#{n_recv := NR + 1, unacked := [PacketId | Acc]});
                _ ->
                    ok = emqtt:puback(Client, PacketId),
                    Consumer(Rest, St#{n_recv := NR + 1})
            end;
        %% No more incoming messages, try to drain some more:
        Consumer([], St) ->
            Consumer({drain, DrainTime}, St)
    end,
    try
        true = erlang:unlink(Client),
        {full, St = #{unacked := Unacked}} = Consumer(
            {drain, DrainTime},
            #{n_pubs => 0, n_recv => 0, unacked => []}
        ),
        ct:pal("[tc] inflight is full: ~p", [self(), St]),
        lists:foreach(fun(PacketId) -> emqtt:puback(Client, PacketId) end, Unacked)
    after
        catch emqtt:stop(Client)
    end.

%%--------------------------------------------------------------------
%% Helper functions
%%--------------------------------------------------------------------

recv_msgs(Count) ->
    recv_msgs(Count, []).

recv_msgs(0, Msgs) ->
    Msgs;
recv_msgs(Count, Msgs) ->
    receive
        {publish, Msg} ->
            recv_msgs(Count - 1, [Msg | Msgs])
    after 1000 ->
        Msgs
    end.

confirm_tls_version(Client, RequiredProtocol) ->
    Info = emqtt:info(Client),
    SocketInfo = proplists:get_value(socket, Info),
    %% emqtt_sock has #ssl_socket.ssl
    SSLSocket = element(3, SocketInfo),
    {ok, SSLInfo} = ssl:connection_information(SSLSocket),
    Protocol = proplists:get_value(protocol, SSLInfo),
    ?assertEqual(RequiredProtocol, Protocol).

tls_certcn_as_clientid(default = TLSVsn) ->
    tls_certcn_as_clientid(TLSVsn, 'tlsv1.3');
tls_certcn_as_clientid(TLSVsn) ->
    tls_certcn_as_clientid(TLSVsn, TLSVsn).

tls_certcn_as_clientid(TLSVsn, RequiredTLSVsn) ->
    CN = <<"Client">>,
    emqx_config:put_zone_conf(default, [mqtt, peer_cert_as_clientid], cn),
    SslConf = emqx_common_test_helpers:client_mtls(TLSVsn),
    {ok, Client} = emqtt:start_link([{port, 8883}, {ssl, true}, {ssl_opts, SslConf}]),
    {ok, _} = emqtt:connect(Client),
    #{clientinfo := #{clientid := CN}} = emqx_cm:get_chan_info(CN),
    confirm_tls_version(Client, RequiredTLSVsn),
    emqtt:disconnect(Client).

meck_sched_delay(X) ->
    erlang:yield(),
    meck:passthrough([X]).
