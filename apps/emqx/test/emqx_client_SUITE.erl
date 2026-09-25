%%--------------------------------------------------------------------
%% Copyright (c) 2018-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
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
        {group, socket_listener},
        {group, ssl_listener}
    ].

groups() ->
    [
        {gen_tcp_listener, [], [
            {group, mqttv3},
            {group, mqttv4},
            {group, mqttv5},
            {group, others},
            {group, socket},
            {group, misbehaving},
            {group, connect_packet_limit}
        ]},
        {socket_listener, [], [
            {group, socket},
            {group, misbehaving},
            {group, connect_packet_limit}
        ]},
        {ssl_listener, [], [
            {group, connect_packet_limit}
        ]},
        {mqttv3, [], [
            t_basic,
            t_sock_closed_reason_normal,
            t_sock_closed_force_closed_by_client
        ]},
        {mqttv4, [], [
            t_basic,
            t_cm,
            t_idle_timeout_infinity,
            t_will_message,
            t_offline_message_queueing,
            t_overlapping_subscriptions,
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
            t_cert_common_name_as_alias,
            t_cert_subject_as_alias,
            t_client_attr_from_user_property,
            t_client_attr_from_password,
            t_certcn_as_clientid_default_config_tls,
            t_certcn_as_clientid_tlsv1_3,
            t_certcn_as_clientid_tlsv1_2,
            t_peercert_preserved_before_connected,
            t_clientid_override,
            t_clientid_override_fail_with_empty_render_result,
            t_clientid_override_fail_with_expression_exception,
            t_namespace_as_mountpoint_enabled,
            t_namespace_as_mountpoint_disabled,
            t_namespace_as_mountpoint_no_tns
        ]},
        {misbehaving, [], [
            t_sock_closed_instantly,
            t_sock_closed_quickly,
            t_sock_closed_on_shutdown,
            t_sock_closed_on_kick_shutdown,
            t_sub_non_utf8_topic,
            t_congestion_send_timeout,
            t_congestion_decongested,
            t_first_packet_not_connect,
            t_sock_closed_incomplete_qos2_transmission,
            t_connect_user_property_limit,
            t_connect_packet_too_large,
            t_large_connect_with_few_user_properties
        ]},
        {socket, [], [
            t_sock_keepalive,
            t_sock_closed_reason_normal,
            t_sock_closed_force_closed_by_client,
            t_large_publish_after_connect,
            t_pipelined_large_publish
        ]},
        {connect_packet_limit, [], [
            t_transport_refuses_oversized_connect,
            t_packet_size_raised_after_connect,
            t_pipelined_publish_larger_than_connect_limit
        ]}
    ].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(gen_tcp_listener, Config) ->
    Apps = emqx_cth_suite:start(
        [
            {emqx, emqx_config() ++ "\n" ++ """
                listeners.tcp.default.tcp_backend = gen_tcp
                listeners.tcp.default.tcp_options.high_watermark = 160KB
            """}
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    %% With `parse_unit = frame' the gen_tcp transport refuses an oversized
    %% first packet itself.
    [{group_apps, Apps}, {listener_type, tcp}, {connect_refusal, {transport, emsgsize}} | Config];
init_per_group(socket_listener, Config) ->
    Apps = emqx_cth_suite:start(
        [
            {emqx, emqx_config() ++ "\n" ++ """
                listeners.tcp.default.tcp_backend = socket
            """}
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    %% The socket backend runs the stream parser, which refuses it from the
    %% fixed header.
    [{group_apps, Apps}, {listener_type, tcp}, {connect_refusal, parser} | Config];
init_per_group(ssl_listener, Config) ->
    Apps = emqx_cth_suite:start(
        [{emqx, emqx_config()}],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [
        {group_apps, Apps},
        {listener_type, ssl},
        {connect_refusal, {transport, invalid_packet}}
        | Config
    ];
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
end_per_group(ssl_listener, Config) ->
    emqx_cth_suite:stop(?config(group_apps, Config));
end_per_group(_GroupName, _Config) ->
    ok.

emqx_config() ->
    """
    listeners.tcp.default.tcp_options {
        # t_congestion_send_timeout
        send_timeout = 2500
        sndbuf = 4KB
        recbuf = 4KB
    }
    # t_congestion_decongested
    conn_congestion.enable_alarm = true
    conn_congestion.min_alarm_sustain_duration = 0
    listeners.ssl.default.ssl_options.verify = verify_peer
    # connect_packet_limit: below max_packet_size (1MB), above the largest CONNECT the other cases send
    mqtt.max_connect_packet_size = 128KB
    """.

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
    emqx_config:put_zone_conf(default, [mqtt, namespace_as_mountpoint], false),
    emqx_config:put_zone_conf(default, [mqtt, max_connect_user_properties], 100),
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
    ?assert(emqx:subscribed(ClientId, <<"mytopic">>)),
    ?assertNot(emqx:subscribed(<<"dummy">>, <<"mytopic">>)),
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
    [ChanPid] = emqx_cm:lookup_channels(<<"c1">>),
    ok = emqtt:disconnect(C1),

    %% Wait until the broker has fully transitioned c1's channel to the
    %% 'disconnected' state before publishing. emqtt:disconnect/1 returns as soon
    %% as the client has sent DISCONNECT and closed its socket, but the broker-side
    %% channel may still be 'connected' for a brief window. A message delivered in
    %% that window takes the live-delivery path (emqx_channel:do_handle_deliver):
    %% QoS 1/2 messages land in the session *inflight* window, which mqueue_len
    %% does not count, and the QoS 0 message is written to the closing socket and
    %% dropped. Only once the channel is 'disconnected' do deliveries go straight
    %% to the offline mqueue, so mqueue_len can reach 3 deterministically.
    ?WAIT(?assertEqual(disconnected, maps:get(conn_state, emqx_connection:info(ChanPid))), 10),

    {ok, C2} = emqtt:start_link([
        {clean_start, true},
        {clientid, <<"c2">>}
    ]),
    {ok, _} = emqtt:connect(C2),

    ok = emqtt:publish(C2, <<"TopicA/B">>, <<"qos 0">>, 0),
    {ok, _} = emqtt:publish(C2, <<"Topic/C">>, <<"qos 1">>, 1),
    {ok, _} = emqtt:publish(C2, <<"TopicA/C">>, <<"qos 2">>, 2),
    %% Wait until all three messages have been dispatched into c1's offline
    %% session mqueue before tearing down the publisher. The publish calls can
    %% return before dispatch reaches the subscriber: QoS 0 has no broker ack at
    %% all, and the QoS 1/2 acks are sent before the broker drives the dispatch
    %% to subscribers. A fixed sleep here races the disconnect on slow runners.
    %%
    %% Read live stats straight from the channel process. emqx_cm:get_chan_stats/1
    %% returns a cached snapshot from ?CHAN_INFO_TAB that is only refreshed by the
    %% channel's emit_stats timer (default mqtt.idle_timeout = 15s). Once c1
    %% disconnects the channel hibernates and stops emitting stats, so the cached
    %% mqueue_len can lag reality for the whole retry budget.
    ?WAIT(?assertEqual(3, proplists:get_value(mqueue_len, emqx_connection:stats(ChanPid))), 30),
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

t_cert_common_name_as_alias(_) ->
    test_cert_extraction_as_alias(cert_common_name).

t_cert_subject_as_alias(_) ->
    test_cert_extraction_as_alias(cert_subject).

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

t_client_attr_from_password(_Config) ->
    ClientId = atom_to_binary(?FUNCTION_NAME),
    Password = <<"secret-password">>,
    {ok, Compiled} = emqx_variform:compile("password"),
    emqx_config:put_zone_conf(default, [mqtt, client_attrs_init], [
        #{
            expression => Compiled,
            set_as_attr => <<"pwd">>
        }
    ]),
    {ok, Client} = emqtt:start_link([
        {clientid, ClientId},
        {username, <<"user">>},
        {password, Password}
    ]),
    {ok, _} = emqtt:connect(Client),
    ChanInfo = emqx_cm:get_chan_info(ClientId),
    ?assertMatch(
        #{clientinfo := #{client_attrs := #{<<"pwd">> := Password}}},
        ChanInfo
    ),
    ClientInfo = maps:get(clientinfo, ChanInfo),
    ?assertNot(maps:is_key(password, ClientInfo)),
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
            %% Filter by this case's client id: the captured trace can
            %% contain sock_closed events from other clients (e.g. a late
            %% close from the previous case), so assert on this client's
            %% event rather than requiring it to be the only one in the trace.
            ?assertMatch(
                [#{clientid := ClientId}],
                [
                    E
                 || #{clientid := CId} = E <- ?of_kind(sock_closed_normal, Trace0),
                    CId =:= ClientId
                ]
            ),
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
            %% Filter by this case's client id, see
            %% t_sock_closed_reason_normal.
            ?assertMatch(
                [#{clientid := ClientId}],
                [
                    E
                 || #{clientid := CId} = E <-
                        ?of_kind(sock_closed_with_other_reason, Trace0),
                    CId =:= ClientId
                ]
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

t_namespace_as_mountpoint_enabled(_) ->
    Namespace = <<"n1">>,
    ClientId = <<"test-client-1">>,
    %% Set tns attribute from user property
    {ok, Compiled} = emqx_variform:compile("user_property.namespace"),
    emqx_config:put_zone_conf(default, [mqtt, client_attrs_init], [
        #{
            expression => Compiled,
            set_as_attr => <<"tns">>
        }
    ]),
    emqx_config:put_zone_conf(default, [mqtt, namespace_as_mountpoint], true),
    {ok, Client} = emqtt:start_link([
        {clientid, ClientId},
        {port, 1883},
        {proto_ver, v5},
        {properties, #{'User-Property' => [{<<"namespace">>, Namespace}]}}
    ]),
    {ok, _} = emqtt:connect(Client),
    ExpectedMountpoint = <<"n1/">>,
    ?assertMatch(
        #{mountpoint := ExpectedMountpoint},
        maps:get(clientinfo, emqx_cm:get_chan_info(ClientId))
    ),
    emqtt:disconnect(Client).

t_namespace_as_mountpoint_disabled(_) ->
    Namespace = <<"n1">>,
    ClientId = <<"test-client-2">>,
    %% Set tns attribute from user property
    {ok, Compiled} = emqx_variform:compile("user_property.namespace"),
    emqx_config:put_zone_conf(default, [mqtt, client_attrs_init], [
        #{
            expression => Compiled,
            set_as_attr => <<"tns">>
        }
    ]),
    emqx_config:put_zone_conf(default, [mqtt, namespace_as_mountpoint], false),
    {ok, Client} = emqtt:start_link([
        {clientid, ClientId},
        {port, 1883},
        {proto_ver, v5},
        {properties, #{'User-Property' => [{<<"namespace">>, Namespace}]}}
    ]),
    {ok, _} = emqtt:connect(Client),
    ?assertMatch(
        #{mountpoint := undefined},
        maps:get(clientinfo, emqx_cm:get_chan_info(ClientId))
    ),
    emqtt:disconnect(Client).

t_namespace_as_mountpoint_no_tns(_) ->
    ClientId = <<"test-client-3">>,
    %% Don't set tns attribute
    emqx_config:put_zone_conf(default, [mqtt, client_attrs_init], []),
    emqx_config:put_zone_conf(default, [mqtt, namespace_as_mountpoint], true),
    {ok, Client} = emqtt:start_link([
        {clientid, ClientId},
        {port, 1883}
    ]),
    {ok, _} = emqtt:connect(Client),
    ?assertMatch(
        #{mountpoint := undefined},
        maps:get(clientinfo, emqx_cm:get_chan_info(ClientId))
    ),
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

%% Connection process smoothly handles situations when socket is already closed
%% during channel shutdown.
t_sock_closed_on_shutdown(_) ->
    %% NOTE
    %% With socket-based listener, it's nearly impossible to trigger a situation when
    %% `socket:send/4` sees a socket error. That makes this testcase currently a _false
    %% positive_ for socket-based listener, however the relevant code path is still
    %% handled carefully in `emqx_socket_connection`.
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
    trace:process(TS, CPid, true, [procs]),
    %% Verify it handles closed socket smoothly in the context of shutdown:
    %% 1. Send a CONNECT that gets treated as banned through the 'client.connect' hook.
    %% 2. Disconnect the socket at the same time.
    ok = emqx_hooks:add(
        'client.connect',
        {?MODULE, h_sock_closed_on_shutdown, [Socket]},
        ?HP_HIGHEST
    ),
    try
        ConnPacket = ?CONNECT_PACKET(#mqtt_packet_connect{
            proto_ver = ?MQTT_PROTO_V5,
            clientid = atom_to_binary(?FUNCTION_NAME)
        }),
        ok = gen_tcp:send(Socket, emqx_frame:serialize(ConnPacket, ?MQTT_PROTO_V5)),
        ?assertReceive({trace, CPid, exit, {shutdown, banned}})
    after
        trace:session_destroy(TS),
        emqx_hooks:del('client.connect', {?MODULE, h_sock_closed_on_shutdown})
    end.

h_sock_closed_on_shutdown(_ConnInfo, _ConnProps, Socket) ->
    ok = gen_tcp:close(Socket),
    ok = timer:sleep(5),
    {stop, {error, ?RC_BANNED}}.

%% Connection process smoothly handles situations when socket is already closed
%% during channel shutdown as a result of a `kick` call.
t_sock_closed_on_kick_shutdown(_) ->
    %% NOTE
    %% With socket-based listener, it's nearly impossible to trigger a situation when
    %% `socket:send/4` sees a socket error. That makes this testcase currently a _false
    %% positive_ for socket-based listener, however the relevant code path is still
    %% handled carefully in `emqx_socket_connection`.
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
    trace:process(TS, CPid, true, [procs]),
    ok = emqx_hooks:add(
        'client.disconnected',
        {?MODULE, h_sock_closed_on_kick_shutdown, [Socket]},
        ?HP_HIGHEST
    ),
    try
        ClientId = atom_to_binary(?FUNCTION_NAME),
        ConnPacket = ?CONNECT_PACKET(#mqtt_packet_connect{
            proto_ver = ?MQTT_PROTO_V5,
            clientid = ClientId
        }),
        ok = gen_tcp:send(Socket, emqx_frame:serialize(ConnPacket, ?MQTT_PROTO_V5)),
        ?assertReceive({tcp, Socket, _ConnAck}),
        _Request = erpc:send_request(node(), emqx_cm, kick_session, [ClientId]),
        ?assertReceive({trace, CPid, exit, {shutdown, kicked}})
    after
        trace:session_destroy(TS),
        emqx_hooks:del('client.disconnected', {?MODULE, h_sock_closed_on_kick_shutdown})
    end.

h_sock_closed_on_kick_shutdown(_ClientInfo, _Reason, _ConnInfo, Socket) ->
    ok = gen_tcp:close(Socket),
    ok = timer:sleep(5).

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

t_first_packet_not_connect(_) ->
    {ok, Socket} = gen_tcp:connect({127, 0, 0, 1}, 1883, [{active, true}, binary]),
    %% Use a complete non-CONNECT MQTT packet to avoid packet=mqtt transport
    %% buffering an incomplete frame forever.
    ok = gen_tcp:send(Socket, <<?PINGREQ:4, 0:1, 0:2, 0:1, 0>>),
    receive
        {tcp_closed, Socket} -> ok
    after 5000 ->
        ct:fail("Expected socket to be closed")
    end.

t_sock_closed_incomplete_qos2_transmission(_) ->
    Topic = <<"t">>,
    SubscriberId = <<"s">>,
    PublisherId = <<"p">>,
    PacketId = 1,
    Payload = <<"payload">>,
    PublishPkt = ?PUBLISH_PACKET(?QOS_2, Topic, PacketId, Payload),
    Publish = emqx_frame:serialize(PublishPkt),
    IncompletePublish = binary:part(iolist_to_binary(Publish), 0, iolist_size(Publish) - 1),
    %% Connect the subscriber "s", subscribe to topic "t" with QoS2:
    {ok, Subscriber} = emqtt:start_link([{clientid, SubscriberId}]),
    {ok, _} = emqtt:connect(Subscriber),
    {ok, _, [?QOS_2]} = emqtt:subscribe(Subscriber, Topic, ?QOS_2),
    %% Connect the publisher "p":
    ConnPkt = ?CONNECT_PACKET(#mqtt_packet_connect{clean_start = false, clientid = PublisherId}),
    Connect = emqx_frame:serialize(ConnPkt),
    {ok, Socket1} = gen_tcp:connect({127, 0, 0, 1}, 1883, [{active, false}, binary]),
    ok = gen_tcp:send(Socket1, Connect),
    %% Send incomplete QoS2 publish and close the connection abruptly:
    ok = gen_tcp:send(Socket1, IncompletePublish),
    ok = timer:sleep(1000),
    ok = gen_tcp:close(Socket1),
    ?retry(100, 20, begin
        [ChanPid] = emqx_cm:lookup_channels(PublisherId),
        false = emqx_cm:is_channel_connected(ChanPid)
    end),
    %% Prepare a duplicate publish:
    #mqtt_packet{header = PublishHd} = PublishPkt,
    DupPublishPkt = PublishPkt#mqtt_packet{header = PublishHd#mqtt_packet_header{dup = true}},
    DupPublish = emqx_frame:serialize(DupPublishPkt),
    %% Connect the publisher "p" and resend the complete publish with DUP flag:
    {ok, Socket2} = gen_tcp:connect({127, 0, 0, 1}, 1883, [{active, false}, binary]),
    ok = gen_tcp:send(Socket2, Connect),
    ok = gen_tcp:send(Socket2, DupPublish),
    %% Subscriber should receive the publish:
    ?assertReceive(
        {publish, #{
            client_pid := Subscriber,
            topic := Topic,
            qos := ?QOS_2,
            dup := false,
            payload := Payload
        }},
        3000
    ),
    %% Cleanup:
    ok = gen_tcp:close(Socket2),
    ok = emqx_cm:discard_session(PublisherId),
    ok = emqtt:stop(Subscriber).

-doc """
The configured user-property limit is applied to new TCP, TLS, and WebSocket
connections. CONNECT and will property blocks are counted separately. Limit
shutdowns use their own listener counter.
""".
t_connect_user_property_limit(_) ->
    OldLimit = emqx_config:get_zone_conf(default, [mqtt, max_connect_user_properties]),
    Limit = 2,
    try
        emqx_config:put_zone_conf(default, [mqtt, max_connect_user_properties], Limit),
        CountBefore = listener_shutdown_count(too_many_user_properties),
        lists:foreach(
            fun(Transport) ->
                assert_connect_accepted(Transport, Limit, Limit),
                assert_connect_rejected(Transport, Limit + 1, Limit),
                assert_connect_rejected(Transport, Limit, Limit + 1)
            end,
            [tcp, tls, ws]
        ),
        ?WAIT(
            ?assert(listener_shutdown_count(too_many_user_properties) >= CountBefore + 2),
            5
        ),
        emqx_config:put_zone_conf(default, [mqtt, max_connect_user_properties], infinity),
        assert_connect_accepted(tcp, 20, 20)
    after
        emqx_config:put_zone_conf(default, [mqtt, max_connect_user_properties], OldLimit)
    end.

-doc """
A CONNECT larger than 64 KB with only one user property remains accepted under
the existing `mqtt.max_packet_size` setting.
""".
t_large_connect_with_few_user_properties(_) ->
    Credential = binary:copy(<<"x">>, 40_000),
    Opts = [
        {proto_ver, v5},
        {properties, #{'User-Property' => [{<<"k">>, <<"v">>}]}},
        {username, Credential},
        {password, Credential}
    ],
    {ok, Client} = emqtt:start_link(Opts),
    try
        ?assertMatch({ok, _}, emqtt:connect(Client)),
        ok = emqtt:disconnect(Client)
    after
        catch emqtt:stop(Client)
    end.

%%--------------------------------------------------------------------
%% Helper functions
%%--------------------------------------------------------------------

assert_connect_accepted(Transport, NConnect, NWill) ->
    ?assertMatch({ok, _}, connect_with_user_properties(Transport, NConnect, NWill)).

assert_connect_rejected(Transport, NConnect, NWill) ->
    ?assertMatch({error, _}, connect_with_user_properties(Transport, NConnect, NWill)).

-doc """
A CONNECT larger than `mqtt.max_connect_packet_size' is refused on TCP, TLS and
WebSocket listeners and counted as `connect_packet_too_large'. A CONNECT within
the limit connects.
""".
t_connect_packet_too_large(_) ->
    Old = emqx_config:get_zone_conf(default, [mqtt, max_connect_packet_size]),
    try
        emqx_config:put_zone_conf(default, [mqtt, max_connect_packet_size], 1024),
        CountBefore = listener_shutdown_count(connect_packet_too_large),
        lists:foreach(
            fun(Transport) ->
                ?assertMatch(
                    {error, _},
                    connect_with_username(Transport, binary:copy(<<"u">>, 2048)),
                    #{transport => Transport}
                ),
                ?assertMatch(
                    {ok, _}, connect_with_username(Transport, <<"u">>), #{transport => Transport}
                )
            end,
            [tcp, tls, ws]
        ),
        %% The counter is per listener; the tcp one is the one read here.
        ?WAIT(
            ?assertEqual(CountBefore + 1, listener_shutdown_count(connect_packet_too_large)),
            5
        )
    after
        emqx_config:put_zone_conf(default, [mqtt, max_connect_packet_size], Old)
    end.

connect_with_username(Transport, Username) ->
    {ok, Client} = emqtt:start_link(transport_opts(Transport) ++ [{username, Username}]),
    unlink(Client),
    try
        Result =
            case Transport of
                ws -> emqtt:ws_connect(Client);
                _ -> emqtt:connect(Client)
            end,
        case Result of
            {ok, _} -> ok = emqtt:disconnect(Client);
            {error, _} -> ok
        end,
        Result
    after
        catch emqtt:stop(Client)
    end.

connect_with_user_properties(Transport, NConnect, NWill) ->
    ConnectProps = #{'User-Property' => client_user_properties(NConnect)},
    WillProps = #{'User-Property' => client_user_properties(NWill)},
    Opts =
        transport_opts(Transport) ++
            [
                {proto_ver, v5},
                {properties, ConnectProps},
                {will_topic, <<"will">>},
                {will_payload, <<"payload">>},
                {will_props, WillProps}
            ],
    {ok, Client} = emqtt:start_link(Opts),
    unlink(Client),
    try
        Result =
            case Transport of
                ws -> emqtt:ws_connect(Client);
                _ -> emqtt:connect(Client)
            end,
        case Result of
            {ok, _} -> ok = emqtt:disconnect(Client);
            {error, _} -> ok
        end,
        Result
    after
        catch emqtt:stop(Client)
    end.

transport_opts(tcp) ->
    [];
transport_opts(tls) ->
    [
        {port, 8883},
        {ssl, true},
        {ssl_opts, emqx_common_test_helpers:client_mtls(default)}
    ];
transport_opts(ws) ->
    [{port, 8083}].

client_user_properties(N) ->
    [{<<"key">>, integer_to_binary(I)} || I <- lists:seq(1, N)].

listener_shutdown_count(Cause) ->
    Counts = emqx_listeners:shutdown_count('tcp:default', 1883),
    proplists:get_value(Cause, Counts, 0).

%%--------------------------------------------------------------------
%% Test cases for `mqtt.max_connect_packet_size' (group `connect_packet_limit',
%% run on the gen_tcp, socket-backend and ssl listeners, whose zone sets the
%% limit below `max_packet_size')
%%--------------------------------------------------------------------

-doc """
A first packet larger than `mqtt.max_connect_packet_size` is refused from its
fixed header and counted as `connect_packet_too_large`. With `parse_unit =
frame` the transport refuses it (gen_tcp reports `emsgsize`, ssl
`{invalid_packet, _}`) before the body is buffered and before the parser sees
it; the socket backend's stream parser refuses it itself. A CONNECT within the
limit still connects.
""".
t_transport_refuses_oversized_connect(Config) ->
    {Limit, MaxSize} = connect_packet_size_limits(),
    Connect = oversized_connect_frame(Limit * 2),
    ?assert(byte_size(Connect) > Limit),
    ?assert(byte_size(Connect) < MaxSize),
    CountBefore = listener_shutdown_count(Config, connect_packet_too_large),
    {ok, {ok, #{reason := {shutdown, Reason}}}} = ?wait_async_action(
        begin
            Socket = raw_connect(Config),
            ok = raw_send(Socket, Connect),
            ?assertEqual(ok, wait_socket_closed(Socket))
        end,
        #{?snk_kind := terminate, reason := {shutdown, #{cause := connect_packet_too_large}}},
        5000
    ),
    ?assertMatch(#{cause := connect_packet_too_large, limit := Limit}, Reason),
    case ?config(connect_refusal, Config) of
        {transport, TransportError} ->
            ?assertMatch(#{transport_error := TransportError}, Reason);
        parser ->
            ?assertNot(is_map_key(transport_error, Reason), Reason)
    end,
    ?WAIT(
        ?assertEqual(CountBefore + 1, listener_shutdown_count(Config, connect_packet_too_large)), 5
    ),
    ?assertMatch({ok, _}, connect_with_username(transport(Config), <<"u">>)).

-doc """
Once the client is connected the limit goes back to `mqtt.max_packet_size`: a
PUBLISH larger than `mqtt.max_connect_packet_size` still round-trips.
""".
t_packet_size_raised_after_connect(Config) ->
    {Limit, MaxSize} = connect_packet_size_limits(),
    Payload = binary:copy(<<"x">>, Limit * 2),
    ?assert(byte_size(Payload) < MaxSize),
    Topic = atom_to_binary(?FUNCTION_NAME),
    {ok, Client} = emqtt:start_link([{clientid, Topic} | transport_opts(transport(Config))]),
    {ok, _} = emqtt:connect(Client),
    {ok, _, [?QOS_1]} = emqtt:subscribe(Client, Topic, ?QOS_1),
    {ok, _} = emqtt:publish(Client, Topic, Payload, ?QOS_1),
    ?assertReceive({publish, #{topic := Topic, payload := Payload}}, 5000),
    ok = emqtt:disconnect(Client).

-doc """
A client may send packets right behind CONNECT without waiting for CONNACK.
A PUBLISH larger than `mqtt.max_connect_packet_size` sent in the same write as
CONNECT is framed against `mqtt.max_packet_size`, not refused against the
CONNECT limit: the transport is asked for one packet first, and the limit is
raised on the CONNECT before the rest is parsed.
""".
t_pipelined_publish_larger_than_connect_limit(Config) ->
    {Limit, MaxSize} = connect_packet_size_limits(),
    Payload = binary:copy(<<"p">>, Limit * 2),
    ?assert(byte_size(Payload) < MaxSize),
    Topic = atom_to_binary(?FUNCTION_NAME),
    Connect = emqx_frame:serialize(?CONNECT_PACKET(#mqtt_packet_connect{clientid = Topic})),
    Subscribe = emqx_frame:serialize(
        ?SUBSCRIBE_PACKET(1, [{Topic, #{rh => 0, rap => 0, nl => 0, qos => ?QOS_1}}])
    ),
    Publish = emqx_frame:serialize(?PUBLISH_PACKET(?QOS_1, Topic, 2, Payload)),
    Socket = raw_connect(Config),
    ok = raw_send(Socket, [Connect, Subscribe, Publish]),
    Packets = recv_packets(Socket, 4),
    Types = [Type || #mqtt_packet{header = #mqtt_packet_header{type = Type}} <- Packets],
    ?assertEqual([?CONNACK, ?SUBACK], lists:sublist(Types, 2), #{packets => Packets}),
    ?assertEqual(lists:sort([?PUBACK, ?PUBLISH]), lists:sort(lists:nthtail(2, Types)), #{
        packets => Packets
    }),
    ?assertMatch(
        [#mqtt_packet{payload = Payload}],
        [P || #mqtt_packet{header = #mqtt_packet_header{type = ?PUBLISH}} = P <- Packets]
    ),
    ok = raw_close(Socket).

connect_packet_size_limits() ->
    Limit = emqx_config:get_zone_conf(default, [mqtt, max_connect_packet_size]),
    MaxSize = emqx_config:get_zone_conf(default, [mqtt, max_packet_size]),
    ?assert(Limit < MaxSize),
    {Limit, MaxSize}.

%% `tcp' or `tls', for the helpers that take a transport.
transport(Config) ->
    case ?config(listener_type, Config) of
        tcp -> tcp;
        ssl -> tls
    end.

listener_shutdown_count(Config, Cause) ->
    Id =
        case ?config(listener_type, Config) of
            tcp -> 'tcp:default';
            ssl -> 'ssl:default'
        end,
    [ListenOn] = [L || {{I, L}, _Pid} <- esockd:listeners(), I =:= Id],
    proplists:get_value(Cause, emqx_listeners:shutdown_count(Id, ListenOn), 0).

%% Receive `N' MQTT packets from a raw socket in active mode.
recv_packets(Socket, N) ->
    recv_packets(Socket, N, [], emqx_frame:initial_parse_state(#{})).

recv_packets(_Socket, N, Acc, _PState) when length(Acc) >= N ->
    lists:sublist(Acc, N);
recv_packets({_Mod, Socket} = S, N, Acc, PState) ->
    receive
        {Tag, Socket, Data} when Tag =:= tcp; Tag =:= ssl ->
            {Packets, NPState} = parse_packets(Data, PState, []),
            recv_packets(S, N, Acc ++ Packets, NPState)
    after 5000 ->
        ct:fail({timeout_receiving_packets, N, Acc})
    end.

parse_packets(Data, PState, Acc) ->
    case emqx_frame:parse(Data, PState) of
        {Packet, Rest, NPState} ->
            parse_packets(Rest, NPState, [Packet | Acc]);
        {_More, NPState} ->
            {lists:reverse(Acc), NPState}
    end.

raw_connect(Config) when is_list(Config) ->
    raw_connect(transport(Config));
raw_connect(tcp) ->
    {ok, Socket} = gen_tcp:connect({127, 0, 0, 1}, 1883, [{active, true}, binary]),
    {gen_tcp, Socket};
raw_connect(tls) ->
    Opts = emqx_common_test_helpers:client_mtls(default) ++ [{active, true}, binary],
    {ok, Socket} = ssl:connect({127, 0, 0, 1}, 8883, Opts, 5000),
    {ssl, Socket}.

raw_send({Mod, Socket}, Data) ->
    Mod:send(Socket, Data).

raw_close({Mod, Socket}) ->
    Mod:close(Socket).

wait_socket_closed({_Mod, Socket}) ->
    receive
        {tcp_closed, Socket} -> ok;
        {ssl_closed, Socket} -> ok
    after 5000 ->
        ct:fail("Expected socket to be closed")
    end.

%% A well-formed MQTT v5 CONNECT of at least `MinSize' bytes, padded with user
%% properties, so that a whole-frame transport sees a complete packet.
oversized_connect_frame(MinSize) ->
    Props = iolist_to_binary([
        begin
            V = integer_to_binary(I),
            <<16#26, 0:16, (byte_size(V)):16, V/binary>>
        end
     || I <- lists:seq(1, MinSize div 8)
    ]),
    PropsSection = <<(encode_vbi(byte_size(Props)))/binary, Props/binary>>,
    VarHeader = <<4:16, "MQTT", 5:8, 2:8, 60:16, PropsSection/binary>>,
    Body = <<VarHeader/binary, 0:16>>,
    <<16#10, (encode_vbi(byte_size(Body)))/binary, Body/binary>>.

encode_vbi(N) when N < 16#80 ->
    <<N>>;
encode_vbi(N) ->
    <<1:1, (N rem 16#80):7, (encode_vbi(N div 16#80))/binary>>.

%%--------------------------------------------------------------------
%% Test cases for reading a large packet on a listener with a small `recbuf'
%% (both listener groups set `tcp_options.recbuf = 4KB')
%%--------------------------------------------------------------------

-define(LARGE_PAYLOAD_SIZE, 256 * 1024).

-doc """
A PUBLISH much larger than `tcp_options.recbuf' is read, acknowledged and
echoed back to the subscriber without stalling the connection.
""".
t_large_publish_after_connect(_) ->
    Topic = atom_to_binary(?FUNCTION_NAME),
    {Socket, State0} = raw_connect_and_subscribe(Topic),
    try
        ok = gen_tcp:send(Socket, large_publish(Topic)),
        {Types, _} = recv_packet_types(Socket, 2, State0),
        ?assertEqual(lists:sort([?PUBACK, ?PUBLISH]), lists:sort(Types))
    after
        gen_tcp:close(Socket)
    end.

-doc """
A client may send packets right behind CONNECT without waiting for CONNACK. A
PUBLISH much larger than `tcp_options.recbuf', written together with CONNECT
and SUBSCRIBE, is read to the end, acknowledged and echoed back.
""".
t_pipelined_large_publish(_) ->
    Topic = atom_to_binary(?FUNCTION_NAME),
    {ok, Socket} = gen_tcp:connect({127, 0, 0, 1}, 1883, [{active, true}, binary]),
    try
        ok = gen_tcp:send(Socket, [
            raw_connect_frame(Topic), raw_subscribe_frame(Topic), large_publish(Topic)
        ]),
        {Types, _} = recv_packet_types(Socket, 4, emqx_frame:initial_parse_state(#{})),
        ?assertEqual([?CONNACK, ?SUBACK], lists:sublist(Types, 2), #{types => Types}),
        ?assertEqual(lists:sort([?PUBACK, ?PUBLISH]), lists:sort(lists:nthtail(2, Types)))
    after
        gen_tcp:close(Socket)
    end.

raw_connect_and_subscribe(Topic) ->
    {ok, Socket} = gen_tcp:connect({127, 0, 0, 1}, 1883, [{active, true}, binary]),
    ok = gen_tcp:send(Socket, raw_connect_frame(Topic)),
    {[?CONNACK], State1} = recv_packet_types(Socket, 1, emqx_frame:initial_parse_state(#{})),
    ok = gen_tcp:send(Socket, raw_subscribe_frame(Topic)),
    {[?SUBACK], State2} = recv_packet_types(Socket, 1, State1),
    {Socket, State2}.

raw_connect_frame(ClientId) ->
    emqx_frame:serialize(?CONNECT_PACKET(#mqtt_packet_connect{clientid = ClientId})).

raw_subscribe_frame(Topic) ->
    emqx_frame:serialize(
        ?SUBSCRIBE_PACKET(1, [{Topic, #{rh => 0, rap => 0, nl => 0, qos => ?QOS_1}}])
    ).

large_publish(Topic) ->
    Payload = binary:copy(<<"p">>, ?LARGE_PAYLOAD_SIZE),
    emqx_frame:serialize(?PUBLISH_PACKET(?QOS_1, Topic, 2, Payload)).

%% Read `N' MQTT packets from a raw socket in active mode and return their
%% types, in arrival order, together with the parser state left over.
recv_packet_types(Socket, N, ParseState) ->
    recv_packet_types(Socket, N, [], ParseState).

recv_packet_types(_Socket, N, Acc, ParseState) when length(Acc) >= N ->
    {lists:sublist(Acc, N), ParseState};
recv_packet_types(Socket, N, Acc, ParseState) ->
    receive
        {tcp, Socket, Data} ->
            {Types, NParseState} = parse_packet_types(Data, ParseState, []),
            recv_packet_types(Socket, N, Acc ++ Types, NParseState)
    after 5000 ->
        ct:fail({timeout_receiving_packets, N, Acc})
    end.

parse_packet_types(Data, ParseState, Acc) ->
    case emqx_frame:parse(Data, ParseState) of
        {#mqtt_packet{header = #mqtt_packet_header{type = Type}}, Rest, NParseState} ->
            parse_packet_types(Rest, NParseState, [Type | Acc]);
        {_More, NParseState} ->
            {lists:reverse(Acc), NParseState}
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
