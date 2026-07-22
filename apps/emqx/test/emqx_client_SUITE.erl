%%--------------------------------------------------------------------
%% Copyright (c) 2018-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_client_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("emqx_utils/include/emqx_message.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("emqx/include/emqx_hooks.hrl").
-include_lib("emqx/include/asserts.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-define(WAIT(EXPR, ATTEMPTS), ?retry(1000, ATTEMPTS, EXPR)).

all() ->
    %% TODO:
    %% Cover rest of the listeners with those testcases:
    %% * t_connect_silent_idle_timeout
    %% * t_connect_idle_timeout
    [
        {group, gen_tcp_listener},
        {group, ssl_listener},
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
        {ssl_listener, [], [
            {group, socket},
            {group, misbehaving}
        ]},
        {socket_listener, [], [
            {group, mqttv3},
            {group, mqttv4},
            {group, mqttv5},
            {group, others},
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
            t_congestion_qos0_publish_storm,
            t_congestion_send_timeout,
            t_congestion_qos0_no_send_timeout,
            t_congestion_decongested,
            t_first_packet_not_connect,
            t_frame_error_shutdown_count_idle,
            t_frame_error_shutdown_count_connected,
            t_frame_error_shutdown_count_is_bounded
        ]},
        {socket, [], [
            t_connection_stats,
            t_connect_silent_idle_timeout,
            t_connect_idle_timeout,
            t_sock_keepalive,
            t_sock_async_set_keepalive,
            t_sock_closed_reason_normal,
            t_sock_closed_force_closed_by_client
        ]}
    ].

init_per_suite(Config) ->
    %% NOTE
    %% Silence `dropped_qos0_msg` / `dropped_msg_due_to_mqueue_is_full` messages.
    %% Logging them for large messages is expensive, and it disrupts stress tests
    %% expectations.
    logger:set_module_level(emqx_session_events, none),
    Config.

end_per_suite(_Config) ->
    logger:unset_module_level(emqx_session_events).

init_per_group(gen_tcp_listener, Config) ->
    Apps = emqx_cth_suite:start(
        [
            {emqx, emqx_config() ++ "\n" ++ """
                listeners.tcp.default.tcp_backend = gen_tcp
            """}
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{group_apps, Apps}, {listener_type, tcp} | Config];
init_per_group(ssl_listener, Config) ->
    Apps = emqx_cth_suite:start(
        [
            {emqx, emqx_config()}
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{group_apps, Apps}, {listener_type, ssl} | Config];
init_per_group(socket_listener, Config) ->
    Apps = emqx_cth_suite:start(
        [
            {emqx, emqx_config() ++ "\n" ++ """
                listeners.tcp.default.tcp_backend = socket
            """}
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{group_apps, Apps}, {listener_type, tcp} | Config];
init_per_group(mqttv3, Config) ->
    [{proto_ver, v3} | Config];
init_per_group(mqttv4, Config) ->
    [{proto_ver, v4} | Config];
init_per_group(mqttv5, Config) ->
    [{proto_ver, v5} | Config];
init_per_group(_GroupName, Config) ->
    Config.

emqx_config() ->
    """
    listeners.tcp.default {
        allow_log_packet_data_from = "127.0.0.0/8, ::1"
        tcp_options { send_timeout = 2500
                      sndbuf = 4KB
                      recbuf = 4KB
                      buffer = 4KB
                      high_watermark = 160KB
                      send_timeout_close = true
                    }
    }
    listeners.ssl.default {
        allow_log_packet_data_from = "127.0.0.0/8, ::1"
        tcp_options { send_timeout = 2500
                      sndbuf = 4KB
                      recbuf = 4KB
                      buffer = 4KB
                      high_watermark = 160KB
                      send_timeout_close = true
                    }
        ssl_options { verify = verify_peer }
    }
    """.

end_per_group(GroupName, Config) when
    GroupName == gen_tcp_listener;
    GroupName == ssl_listener;
    GroupName == socket_listener
->
    emqx_cth_suite:stop(?config(group_apps, Config));
end_per_group(_GroupName, _Config) ->
    ok.

init_per_testcase(Case, Config) ->
    ok = snabbkaffe:start_trace(),
    emqx_common_test_helpers:init_per_testcase(?MODULE, Case, Config).

end_per_testcase(_Case, Config) ->
    ok = snabbkaffe:stop(),
    restore_conf(Config),
    restore_listener_conf(Config).

%%--------------------------------------------------------------------
%% Test cases for MQTT v4
%%--------------------------------------------------------------------

t_cm(init, Config) ->
    override_conf([mqtt, idle_timeout], 1000, Config).

t_cm(_) ->
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

t_idle_timeout_infinity(init, Config) ->
    override_conf([mqtt, idle_timeout], infinity, Config).

t_idle_timeout_infinity(_) ->
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
    ?WAIT(
        ?assertEqual(
            disconnected, emqx_cth_broker:connection_info({channel, conn_state}, <<"c1">>)
        ),
        10
    ),

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
    ?WAIT(?assertEqual(3, emqx_cth_broker:connection_stat(mqueue_len, <<"c1">>)), 30),
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

t_username_as_clientid(init, Config) ->
    override_conf([mqtt, use_username_as_clientid], true, Config).

t_username_as_clientid(_) ->
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

t_certcn_as_alias(init, Config) ->
    save_conf([mqtt, client_attrs_init], Config).

t_certcn_as_alias(_) ->
    test_cert_extraction_as_alias(cn).

t_certdn_as_alias(init, Config) ->
    save_conf([mqtt, client_attrs_init], Config).

t_certdn_as_alias(_) ->
    test_cert_extraction_as_alias(dn).

t_cert_common_name_as_alias(init, Config) ->
    save_conf([mqtt, client_attrs_init], Config).

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

t_client_attr_from_user_property(init, Config) ->
    save_conf([mqtt, client_attrs_init], Config).

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

t_client_attr_from_password(init, Config) ->
    save_conf([mqtt, client_attrs_init], Config).

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

t_sock_keepalive(init, Config) ->
    override_listener_conf(default, [tcp_options, keepalive], "1,1,5", Config).

t_sock_keepalive(Config) ->
    %% Connect MQTT client:
    ClientId = atom_to_binary(?FUNCTION_NAME),
    {ok, C} = emqtt:start_link([{clientid, ClientId} | socket_emqtt_opts(Config)]),
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

t_sock_async_set_keepalive(Config) ->
    case os:type() of
        {unix, darwin} ->
            test_async_set_keepalive(Config);
        {unix, linux} ->
            test_async_set_keepalive(Config);
        _ ->
            %% don't support the feature on other OS
            ok
    end.

test_async_set_keepalive(Config) ->
    ClientID = <<"client-tcp-keepalive">>,
    {ok, Client} = emqtt:start_link([{clientid, ClientID} | Config]),
    {{ok, _Props}, {ok, _}} = ?wait_async_action(
        emqtt:connect(Client),
        #{?snk_kind := insert_channel_info, clientid := ClientID}
    ),
    {ConnMod, ConnPid} = emqx_cth_broker:connection_chanmod(ClientID),
    State = ConnMod:get_state(ConnPid),
    case State of
        #{transport := Transport, socket := Socket} ->
            ok;
        #{socket := Socket} ->
            %% TODO
            Transport = esockd_socket
    end,
    {Idle, Interval, Probes} = sock_get_keepalive(Transport, Socket),
    ct:pal("Idle=~p, Interval=~p, Probes=~p", [Idle, Interval, Probes]),
    {ok, {ok, _}} = ?wait_async_action(
        conn_set_keepalive(ConnMod, ConnPid, Idle + 1, Interval + 1, Probes + 1),
        #{?snk_kind := "custom_socket_options_successfully"}
    ),
    ?assertEqual(
        {Idle + 1, Interval + 1, Probes + 1},
        sock_get_keepalive(Transport, Socket)
    ),
    emqtt:stop(Client).

conn_set_keepalive(emqx_connection, ConnPid, Idle, Interval, Probes) ->
    emqx_connection:async_set_keepalive(os:type(), ConnPid, Idle, Interval, Probes);
conn_set_keepalive(emqx_socket_connection, ConnPid, Idle, Interval, Probes) ->
    emqx_socket_connection:async_set_keepalive(ConnPid, Idle, Interval, Probes).

sock_get_keepalive(esockd_transport, Sock) when is_port(Sock) ->
    {OptKeepIdle, OptKeepInterval, OptKeepCount} =
        case os:type() of
            {unix, darwin} ->
                {16#10, 16#101, 16#102};
            {unix, linux} ->
                {4, 5, 6};
            _ ->
                error(unsupported)
        end,
    {ok, Opts} = esockd_transport:getopts(Sock, [
        {raw, 6, OptKeepIdle, 4},
        {raw, 6, OptKeepInterval, 4},
        {raw, 6, OptKeepCount, 4}
    ]),
    [
        {raw, 6, OptKeepIdle, <<Idle:32/native>>},
        {raw, 6, OptKeepInterval, <<Interval:32/native>>},
        {raw, 6, OptKeepCount, <<Probes:32/native>>}
    ] = Opts,
    {Idle, Interval, Probes};
sock_get_keepalive(esockd_socket, Sock) ->
    {ok, Opts} = esockd_socket:getopts(Sock, [
        {tcp, keepcnt},
        {tcp, keepidle},
        {tcp, keepintvl}
    ]),
    [
        {{tcp, keepcnt}, Probes},
        {{tcp, keepidle}, Idle},
        {{tcp, keepintvl}, Interval}
    ] = Opts,
    {Idle, Interval, Probes}.

t_sock_closed_reason_normal(Config) ->
    ClientId = atom_to_binary(?FUNCTION_NAME),
    ?check_trace(
        begin
            {ok, C} = emqtt:start_link([{clientid, ClientId} | socket_emqtt_opts(Config)]),
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
            {ok, C} = emqtt:start_link([{clientid, ClientId} | socket_emqtt_opts(Config)]),
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

t_clientid_override(init, Config) ->
    Override = <<"username">>,
    {ok, Rule} = emqx_variform:compile(Override),
    override_conf([mqtt, clientid_override], Rule, Config).

t_clientid_override(_) ->
    ClientId = <<"original-clientid-0">>,
    Username = <<"username1">>,
    {ok, Client} = emqtt:start_link([{clientid, ClientId}, {port, 1883}, {username, Username}]),
    {ok, _} = emqtt:connect(Client),
    ?assertMatch(#{clientid := Username}, maps:get(clientinfo, emqx_cm:get_chan_info(Username))),
    ?assertMatch(undefined, emqx_cm:get_chan_info(ClientId)),
    emqtt:disconnect(Client).

t_clientid_override_fail_with_empty_render_result(init, Config) ->
    {ok, Rule} = emqx_variform:compile(<<"undefined_var">>),
    override_conf([mqtt, clientid_override], Rule, Config).

t_clientid_override_fail_with_empty_render_result(_) ->
    test_clientid_override_fail(<<"original-clientid-1">>).

t_clientid_override_fail_with_expression_exception(init, Config) ->
    {ok, Rule} = emqx_variform:compile(<<"nth(1,undefined_var)">>),
    override_conf([mqtt, clientid_override], Rule, Config).

t_clientid_override_fail_with_expression_exception(_) ->
    test_clientid_override_fail(<<"original-clientid-2">>).

test_clientid_override_fail(ClientId) ->
    {ok, Client} = emqtt:start_link([{clientid, ClientId}, {port, 1883}]),
    {ok, _} = emqtt:connect(Client),
    ?assertMatch(#{clientid := ClientId}, maps:get(clientinfo, emqx_cm:get_chan_info(ClientId))),
    emqtt:disconnect(Client).

t_namespace_as_mountpoint_enabled(init, Config) ->
    %% Set tns attribute from user property
    override_conf(
        #{
            [mqtt, client_attrs_init] => [mk_client_attrs_init_tns("user_property.namespace")],
            [mqtt, namespace_as_mountpoint] => true
        },
        Config
    ).

t_namespace_as_mountpoint_enabled(_) ->
    Namespace = <<"n1">>,
    ClientId = <<"test-client-1">>,
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

t_namespace_as_mountpoint_disabled(init, Config) ->
    %% Set tns attribute from user property
    override_conf(
        #{
            [mqtt, client_attrs_init] => [mk_client_attrs_init_tns("user_property.namespace")],
            [mqtt, namespace_as_mountpoint] => false
        },
        Config
    ).

t_namespace_as_mountpoint_disabled(_) ->
    Namespace = <<"n1">>,
    ClientId = <<"test-client-2">>,
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

t_namespace_as_mountpoint_no_tns(init, Config) ->
    %% Don't set tns attribute
    override_conf(
        #{
            [mqtt, client_attrs_init] => [],
            [mqtt, namespace_as_mountpoint] => true
        },
        Config
    ).

t_namespace_as_mountpoint_no_tns(_) ->
    ClientId = <<"test-client-3">>,
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

mk_client_attrs_init_tns(Expr) ->
    {ok, Compiled} = emqx_variform:compile(Expr),
    #{
        expression => Compiled,
        set_as_attr => <<"tns">>
    }.

t_certcn_as_clientid_default_config_tls(init, Config) ->
    override_conf([mqtt, peer_cert_as_clientid], cn, Config).

t_certcn_as_clientid_default_config_tls(_) ->
    tls_certcn_as_clientid(default).

t_certcn_as_clientid_tlsv1_3(init, Config) ->
    override_conf([mqtt, peer_cert_as_clientid], cn, Config).

t_certcn_as_clientid_tlsv1_3(_) ->
    tls_certcn_as_clientid('tlsv1.3').

t_certcn_as_clientid_tlsv1_2(init, Config) ->
    override_conf([mqtt, peer_cert_as_clientid], cn, Config).

t_certcn_as_clientid_tlsv1_2(_) ->
    tls_certcn_as_clientid('tlsv1.2').

t_peercert_preserved_before_connected(init, Config) ->
    override_conf([mqtt, peer_cert_as_clientid], false, Config).

t_peercert_preserved_before_connected(_) ->
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

t_sock_closed_instantly(Config) ->
    %% Introduce scheduling delays:
    meck:new(esockd_transport, [no_history, passthrough]),
    meck:new(esockd_socket, [no_history, passthrough]),
    meck:expect(esockd_transport, type, fun meck_sched_delay/1),
    meck:expect(esockd_socket, type, fun meck_sched_delay/1),
    %% Start a tracing session, to catch exit reasons consistently:
    TS = trace:session_create(?MODULE, self(), []),
    %% Estabilish a connection:
    {
        Socket,
        {ok, #{?snk_meta := #{pid := CPid}}}
    } = ?wait_async_action(
        socket_connect(Config, [{active, true}, binary]),
        #{?snk_kind := connection_started}
    ),
    %% Verify it handles instant socket close smoothly:
    trace:process(TS, CPid, true, [procs]),
    try
        ok = socket_close(Socket),
        ?assertReceive(
            {trace, CPid, exit, Reason} when
                Reason == {shutdown, tcp_closed} orelse
                    Reason == {shutdown, ssl_closed} orelse
                    Reason == {shutdown, einval} orelse
                    Reason == normal
        )
    after
        trace:session_destroy(TS),
        meck:unload()
    end.

t_sock_closed_quickly(Config) ->
    %% Start a tracing session:
    TS = trace:session_create(?MODULE, self(), []),
    %% Estabilish a connection:
    {
        Socket,
        {ok, #{?snk_meta := #{pid := CPid}}}
    } = ?wait_async_action(
        socket_connect(Config, [{active, true}, binary]),
        #{?snk_kind := connection_started}
    ),
    %% Verify it handles quick socket close smoothly:
    trace:process(TS, CPid, true, [procs]),
    try
        ok = socket_close(Socket),
        ?assertReceive(
            {trace, CPid, exit, Reason} when
                Reason == {shutdown, tcp_closed} orelse
                    Reason == {shutdown, ssl_closed} orelse
                    Reason == normal
        )
    after
        trace:session_destroy(TS)
    end.

%% Connection process smoothly handles situations when socket is already closed
%% during channel shutdown.
t_sock_closed_on_shutdown(Config) ->
    %% NOTE
    %% With socket-based listener, it's nearly impossible to trigger a situation when
    %% `socket:send/4` sees a socket error. That makes this testcase currently a _false
    %% positive_ for socket-based listener, however the relevant code path is still
    %% handled carefully in `emqx_socket_connection`.
    %% Start a tracing session:
    TS = trace:session_create(?MODULE, self(), []),
    %% Estabilish a connection:
    {
        Socket,
        {ok, #{?snk_meta := #{pid := CPid}}}
    } = ?wait_async_action(
        socket_connect(Config, [{active, true}, binary]),
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
        ok = socket_send(Socket, emqx_frame:serialize(ConnPacket, ?MQTT_PROTO_V5)),
        ?assertReceive({trace, CPid, exit, {shutdown, banned}})
    after
        trace:session_destroy(TS),
        emqx_hooks:del('client.connect', {?MODULE, h_sock_closed_on_shutdown})
    end.

h_sock_closed_on_shutdown(_ConnInfo, _ConnProps, Socket) ->
    ok = socket_close(Socket),
    ok = timer:sleep(5),
    {stop, {error, ?RC_BANNED}}.

%% Connection process smoothly handles situations when socket is already closed
%% during channel shutdown as a result of a `kick` call.
t_sock_closed_on_kick_shutdown(Config) ->
    %% NOTE
    %% With socket-based listener, it's nearly impossible to trigger a situation when
    %% `socket:send/4` sees a socket error. That makes this testcase currently a _false
    %% positive_ for socket-based listener, however the relevant code path is still
    %% handled carefully in `emqx_socket_connection`.
    %% Start a tracing session:
    TS = trace:session_create(?MODULE, self(), []),
    %% Estabilish a connection:
    {
        Socket = {Transport, TSock},
        {ok, #{?snk_meta := #{pid := CPid}}}
    } = ?wait_async_action(
        socket_connect(Config, [{active, true}, binary]),
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
        ok = socket_send(Socket, emqx_frame:serialize(ConnPacket, ?MQTT_PROTO_V5)),
        ?assertReceive({Transport, TSock, <<_/bytes>>}),
        _Request = erpc:send_request(node(), emqx_cm, kick_session, [ClientId]),
        ?assertReceive({trace, CPid, exit, {shutdown, kicked}})
    after
        trace:session_destroy(TS),
        emqx_hooks:del('client.disconnected', {?MODULE, h_sock_closed_on_kick_shutdown})
    end.

h_sock_closed_on_kick_shutdown(_ClientInfo, _Reason, _ConnInfo, Socket) ->
    ok = socket_close(Socket),
    ok = timer:sleep(5).

t_connection_stats(_) ->
    ClientId = atom_to_binary(?FUNCTION_NAME),
    {ok, Client} = emqtt:start_link([{port, 1883}, {clientid, ClientId}]),
    {ok, _} = emqtt:connect(Client),
    ?assertEqual(pong, emqtt:ping(Client)),
    ConnStats = emqx_cth_broker:connection_stats(ClientId),
    ct:pal("==== stats: ~p", [ConnStats]),
    ?assertMatch(
        #{
            recv_pkt := RecvPkt,
            recv_msg := RecvMsg,
            send_pkt := SendPkt,
            send_msg := SendMsg,
            recv_oct := RecvOct,
            recv_cnt := RecvCnt,
            send_oct := SendOct,
            send_cnt := SendCnt,
            send_pend := 0
        } when
            RecvPkt > 0 andalso
                RecvMsg >= 0 andalso
                SendPkt > 0 andalso
                SendMsg >= 0 andalso
                RecvOct > 0 andalso
                RecvCnt > 0 andalso
                SendOct > 0 andalso
                SendCnt > 0,
        maps:from_list(ConnStats)
    ),
    emqtt:disconnect(Client).

t_connect_silent_idle_timeout(Config) ->
    %% Connect, send nothing more.
    %% Connection should be dropped in roughly `IdleTimeout` ms.
    IdleTimeout = 2000,
    emqx_config:put_zone_conf(default, [mqtt, idle_timeout], IdleTimeout),
    Sock = socket_connect(Config, [{active, true}, {nodelay, true}, binary]),
    SockClosedMsg = socket_closed(Sock),
    ?assertReceive(SockClosedMsg, IdleTimeout * 2),
    ?assertMatch(
        {ok, #{reason := {shutdown, idle_timeout}}},
        ?block_until(#{?snk_kind := terminate}, IdleTimeout)
    ).

t_connect_idle_timeout(Config) ->
    %% Connect, send few bytes.
    %% Connection should be dropped in roughly `IdleTimeout` ms.
    IdleTimeout = 2000,
    emqx_config:put_zone_conf(default, [mqtt, idle_timeout], IdleTimeout),
    ConnectPacket = emqx_frame:serialize(?CONNECT_PACKET(#mqtt_packet_connect{})),
    Sock = socket_connect(Config, [{active, true}, {nodelay, true}, binary]),
    SockClosedMsg = socket_closed(Sock),
    {ok, Sockname} = socket_sockname(Sock),
    ClientSockname = iolist_to_binary(esockd:format(Sockname)),
    ok = socket_send(Sock, binary:part(iolist_to_binary(ConnectPacket), 0, 4)),
    ?assertReceive(SockClosedMsg, IdleTimeout * 2),
    ?assertMatch(
        {ok, #{reason := {shutdown, idle_timeout}, ?snk_meta := #{peername := ClientSockname}}},
        ?block_until(#{?snk_kind := terminate, reason := {shutdown, idle_timeout}}, IdleTimeout)
    ).

t_sub_non_utf8_topic(Config) ->
    Socket = socket_connect(Config, [{active, true}, binary]),
    ConnPacket = ?CONNECT_PACKET(#mqtt_packet_connect{clientid = <<"abcdefg">>}),
    ok = socket_send(Socket, emqx_frame:serialize(ConnPacket)),
    receive
        {tcp, _, _ConnAck = <<32, 2, 0, 0>>} -> ok;
        {ssl, _, _ConnAck = <<32, 2, 0, 0>>} -> ok
    after 3000 -> ct:fail({connect_ack_not_recv, process_info(self(), messages)})
    end,
    SubHeader = <<130, 18, 25, 178>>,
    SubTopicLen = <<0, 13>>,
    %% this is not a valid utf8 topic
    SubTopic = <<128, 10, 10, 12, 178, 159, 162, 47, 115, 1, 1, 1, 1>>,
    SubQoS = <<1>>,
    SubPacket = <<SubHeader/binary, SubTopicLen/binary, SubTopic/binary, SubQoS/binary>>,
    ok = socket_send(Socket, SubPacket),
    receive
        {tcp_closed, _} -> ok;
        {ssl_closed, _} -> ok
    after 3000 -> ct:fail({should_get_disconnected, process_info(self(), messages)})
    end,
    timer:sleep(1000),
    ListenerCounts = emqx_listeners:shutdown_count(listener_id(Config), listener_port(Config)),
    TopicInvalidCount = proplists:get_value(topic_filter_invalid, ListenerCounts),
    ?assert(is_integer(TopicInvalidCount) andalso TopicInvalidCount > 0),
    ok.

%% Verify that suspended send-congested MQTT client survives a storm of mixed QoS
%% publishes that's considerably over capacity of its forced shutdown policy.
%% Expectations:
%% 1. MQTT client connection survives the storm.
%% 2. Every QoS1 publish eventually reaches the client, even with
%%    `mqueue_store_qos0 = true`.
t_congestion_qos0_publish_storm(init, Config) ->
    override_conf(
        #{
            %% Provide capacity _not enough_ to absord whole storm:
            [force_shutdown] => #{
                enable => true,
                max_heap_size => 1024 * 1024 div erlang:system_info(wordsize),
                max_mailbox_size => 1000
            },
            %% Configure to keep QoS0 messages in the mqueue:
            %% As QoS0 messages are evicted from the mqueue first, it's expected
            %% that QoS1 messages will outlive them.
            [mqtt, mqueue_store_qos0] => true,
            [mqtt, max_mqueue_len] => 100
        },
        Config
    ).

t_congestion_qos0_publish_storm(Config) ->
    NQoS1Publishes = 16,
    QoS0StormSize = 2500,
    QoS0PayloadSize = 8000,
    Suffix = integer_to_list(erlang:unique_integer()),
    ClientId = iolist_to_binary([atom_to_binary(?FUNCTION_NAME), Suffix]),
    %% Estabilish TCP connection:
    Socket = socket_connect(Config, [
        {active, false},
        binary,
        {buffer, 1024},
        {recbuf, 1024},
        {sndbuf, 1024}
    ]),
    %% Connect MQTT client:
    Parser0 = emqx_frame:initial_parse_state(),
    ok = socket_send(
        Socket,
        emqx_frame:serialize(?CONNECT_PACKET(#mqtt_packet_connect{clientid = ClientId}))
    ),
    {ok, Frame1} = socket_recv(Socket, 0, 1000),
    {?CONNACK_PACKET(0), <<>>, Parser1} = emqx_frame:parse(Frame1, Parser0),
    %% Subscribe to 2 topics:
    QoS0Topic = emqx_topic:join(["slow-client", Suffix, "qos0"]),
    QoS1Topic = emqx_topic:join(["slow-client", Suffix, "qos1"]),
    SubOpts = #{rh => 0, rap => 0, nl => 0},
    ok = socket_send(
        Socket,
        emqx_frame:serialize(
            ?SUBSCRIBE_PACKET(1, [
                {QoS0Topic, SubOpts#{qos => ?QOS_0}},
                {QoS1Topic, SubOpts#{qos => ?QOS_1}}
            ])
        )
    ),
    {ok, Frame2} = socket_recv(Socket, 0, 1000),
    {?SUBACK_PACKET(1, [0, 1]), <<>>, Parser2} = emqx_frame:parse(Frame2, Parser1),
    %% Find the channel process on the broker side:
    [ConnPid] = emqx_cm:lookup_channels(ClientId),
    MRef = erlang:monitor(process, ConnPid),
    %% Construct a stream of messages to publish:
    QoS0Publisher = <<"qos0-storm-publisher">>,
    QoS1Publisher = <<"qos1-publisher">>,
    QoS0Payload = binary:copy(<<"qos0-storm">>, QoS0PayloadSize div 10),
    QoS1Payload = fun(I) -> iolist_to_binary("qos1-" ++ integer_to_list(I)) end,
    QoS1Messages = [
        emqx_message:make(QoS1Publisher, ?QOS_1, QoS1Topic, QoS1Payload(I))
     || I <- lists:seq(1, NQoS1Publishes)
    ],
    StreamQoS0 = emqx_utils_stream:const(
        emqx_message:make(QoS0Publisher, ?QOS_0, QoS0Topic, QoS0Payload)
    ),
    StreamQoS1 = emqx_utils_stream:list(QoS1Messages),
    Stream = emqx_utils_stream:chain(
        %% 1250 QoS0 messages, ...
        emqx_utils_stream:limit_length(QoS0StormSize div 2, StreamQoS0),
        %% Followed by 16 QoS1 messages evenly interspersed with 1250 QoS0 messages
        emqx_utils_stream:interleave(
            [
                {1, StreamQoS1},
                {QoS0StormSize div (2 * NQoS1Publishes), StreamQoS0}
            ],
            false
        )
    ),
    %% Publish them all at once:
    emqx_utils_stream:foreach(
        fun
            (Msg = #message{}) ->
                emqx:publish(Msg);
            (sleep) ->
                timer:sleep(1)
        end,
        emqx_utils_stream:interleave(
            [
                {100, Stream},
                emqx_utils_stream:const(sleep)
            ],
            false
        )
    ),
    %% Receive pending QoS1 publishes from the socket:
    {QoS1Received, _Parser} = drain_qos1_publishes(Socket, Parser2, NQoS1Publishes, 10_000),
    %% Verify ALL QoS1 publishes has successfully reached the socket:
    ?assertEqual(
        [],
        lists:foldl(
            fun({_PacketId, Payload}, Ms) -> lists:keydelete(Payload, #message.payload, Ms) end,
            QoS1Messages,
            QoS1Received
        )
    ),
    %% Verify channel process is alive and well:
    ?assertEqual([ConnPid], emqx_cm:lookup_channels(ClientId)),
    ?assert(is_process_alive(ConnPid)),
    ?assertNotReceive({'DOWN', MRef, process, ConnPid, _}, 0),
    ok = socket_close(Socket).

drain_qos1_publishes(Socket, Parser, N, Timeout) ->
    Deadline = erlang:monotonic_time(millisecond) + Timeout,
    drain_qos1_publishes(Socket, Parser, [], N, Deadline).

drain_qos1_publishes(Socket, Parser0, Acc, N, Deadline) ->
    Left = Deadline - erlang:monotonic_time(millisecond),
    maybe
        true ?= N > 0,
        true ?= Left > 0,
        {ok, Data} ?= socket_recv(Socket, 0, min(1000, Left)),
        {Parser, Seen} = parse_incoming_qos1(Socket, Data, Parser0, []),
        ok = lists:foreach(
            fun({PacketId, _}) ->
                case socket_send(Socket, emqx_frame:serialize(?PUBACK_PACKET(PacketId))) of
                    ok ->
                        ok;
                    {error, closed} ->
                        ct:fail(slow_client_socket_closed_while_ack_qos1)
                end
            end,
            Seen
        ),
        drain_qos1_publishes(Socket, Parser, Seen ++ Acc, N - length(Seen), Deadline)
    else
        false ->
            {Acc, Parser0};
        {error, timeout} ->
            drain_qos1_publishes(Socket, Parser0, Acc, Deadline);
        {error, closed} ->
            ct:fail(slow_client_socket_closed)
    end.

parse_incoming_qos1(Socket, Data, Parser0, Acc) ->
    case emqx_frame:parse(Data, Parser0) of
        {?PUBLISH_PACKET(?QOS_1, _Topic, PacketId, Payload), Rest, Parser} ->
            parse_incoming_qos1(Socket, Rest, Parser, [{PacketId, Payload} | Acc]);
        {?PUBLISH_PACKET(?QOS_0, _Topic, _PacketId, _Payload), Rest, Parser} ->
            parse_incoming_qos1(Socket, Rest, Parser, Acc);
        {_Packet, Rest, Parser} ->
            parse_incoming_qos1(Socket, Rest, Parser, Acc);
        {_More, Parser} ->
            {Parser, Acc}
    end.

%% Verify that slow send-congested MQTT client overwhelmed by a stream of QoS1 publishes gets
%% its connection terminated with `send_timeout` reason in configured timeout.
t_congestion_send_timeout(init, Config) ->
    override_conf(
        #{
            [conn_congestion, enable_alarm] => true,
            [conn_congestion, min_alarm_sustain_duration] => 0,
            %% This timeout drives congestion alarms, decrease to receive alarms sooner:
            [mqtt, idle_timeout] => 500,
            %% Congestion is exercised through QoS1 publishing, increase to avoid queueing:
            [mqtt, max_inflight] => 1000
        },
        Config
    ).

t_congestion_send_timeout(Config) ->
    PayloadSize = 12000,
    PublishInterval = 80,
    ConsumeRecvlen = 1000,
    ConsumeInterval = 100,
    Socket = socket_connect(Config, [{buffer, 4096}, {active, false}, binary]),
    %% Send manually constructed CONNECT:
    ok = socket_send(
        Socket,
        emqx_frame:serialize(
            ?CONNECT_PACKET(#mqtt_packet_connect{clientid = <<"t_congestion_send_timeout">>})
        )
    ),
    {ok, Frames1} = socket_recv(Socket, 0, 1000),
    {Pkt1, <<>>, Parser1} = emqx_frame:parse(Frames1, emqx_frame:initial_parse_state()),
    ?assertMatch(?CONNACK_PACKET(0), Pkt1),
    %% Send manually constructed SUBSCRIBE to subscribe to "t":
    Topic = <<"t">>,
    ok = socket_send(
        Socket,
        emqx_frame:serialize(
            ?SUBSCRIBE_PACKET(1, [{Topic, #{rh => 0, rap => 0, nl => 0, qos => 1}}])
        )
    ),
    {ok, Frames2} = socket_recv(Socket, 0, 1000),
    {Pkt2, <<>>, _Parser2} = emqx_frame:parse(Frames2, Parser1),
    ?assertMatch(?SUBACK_PACKET(1, [?QOS_1]), Pkt2),
    %% Subscribe to alarms:
    AlarmTopic = <<"$SYS/brokers/+/alarms/activate">>,
    ok = emqx_broker:subscribe(AlarmTopic),
    %% Start filling up send buffers:
    Publisher = fun Publisher(N) ->
        Payload = binary:copy(<<N:64>>, PayloadSize div 8),
        _ = emqx:publish(emqx_message:make(<<"publisher">>, ?QOS_1, Topic, Payload)),
        ok = timer:sleep(PublishInterval),
        Publisher(N + 1)
    end,
    _PublisherPid = spawn_link(fun() -> Publisher(1) end),
    %% Start lagging consumer:
    _ConsumerPid = spawn_link(fun() ->
        loop_slow_consumer(active, Socket, ConsumeRecvlen, ConsumeInterval)
    end),
    %% Congestion alarm should be raised soon:
    {deliver, _, AlarmMsg} = ?assertReceive({deliver, AlarmTopic, _AlarmMsg}, 10_000),
    #{
        <<"name">> := <<"conn_congestion/t_congestion_send_timeout/undefined">>,
        <<"details">> := AlarmDetails
    } = emqx_utils_json:decode(emqx_message:payload(AlarmMsg)),
    %% Connection should be closed once send timeout passes.
    ConnPid = list_to_pid(binary_to_list(maps:get(<<"pid">>, AlarmDetails))),
    MRef = erlang:monitor(process, ConnPid),
    ?assertReceive({'DOWN', MRef, process, ConnPid, {shutdown, send_timeout}}, 10_000),
    ok = socket_close(Socket).

%% Verify that slow send-congested MQTT client survives a stream of QoS0-only publishes,
%% the connection stays up and no `send_timeout` socket errors are observed.
%% Expectations:
%% 1. Connection observes socket congestion.
%% 2. Connection is never closed abnormally.
%% 3. Under congestion, QoS0 messages enter mqueue and get dropped once mqueue is full.
t_congestion_qos0_no_send_timeout(init, Config) ->
    override_conf(
        #{
            [conn_congestion, enable_alarm] => true,
            [conn_congestion, min_alarm_sustain_duration] => 0,
            %% This timeout drives congestion alarms, decrease to receive alarms sooner:
            [mqtt, idle_timeout] => 500,
            %% Once the connection is marked congested, QoS0 must not keep pushing
            %% the transport toward send_timeout through the session queue.
            %% Keep the mqueue severely constrained to make sure connection survives
            %% full mqueue flush each time it's considered decongested. In a better
            %% world, connection would have adaptive flow control for such situations.
            [mqtt, max_mqueue_len] => 20
        },
        Config
    ).

t_congestion_qos0_no_send_timeout(Config) ->
    %% Configure a mismatch between publishing pace and subscriber capacity:
    %% These are chosen to work similarly for all 3 listener types: `gen_tcp`,
    %% `socket` and `ssl`.
    PayloadSize = 5000,
    PublishInterval = 50,
    ConsumeRecvlen = 2000,
    ConsumeInterval = 60,
    %% Record dropped QoS0 count before the storm:
    DroppedBefore = emqx_metrics:val_global('delivery.dropped.queue_full'),
    ClientId = <<"t_congestion_qos0_no_send_timeout">>,
    Socket = socket_connect(Config, [{buffer, 4096}, {active, false}, binary]),
    %% Send manually constructed CONNECT:
    ok = socket_send(
        Socket,
        emqx_frame:serialize(
            ?CONNECT_PACKET(#mqtt_packet_connect{clientid = ClientId})
        )
    ),
    {ok, Frames1} = socket_recv(Socket, 0, 1000),
    {Pkt1, <<>>, Parser1} = emqx_frame:parse(Frames1, emqx_frame:initial_parse_state()),
    ?assertMatch(?CONNACK_PACKET(0), Pkt1),
    %% Send manually constructed SUBSCRIBE to subscribe to "t" at QoS0:
    Topic = <<"t">>,
    ok = socket_send(
        Socket,
        emqx_frame:serialize(
            ?SUBSCRIBE_PACKET(1, [{Topic, #{rh => 0, rap => 0, nl => 0, qos => ?QOS_0}}])
        )
    ),
    {ok, Frames2} = socket_recv(Socket, 0, 1000),
    {Pkt2, <<>>, _Parser2} = emqx_frame:parse(Frames2, Parser1),
    ?assertMatch(?SUBACK_PACKET(1, [?QOS_0]), Pkt2),
    %% Subscribe to alarms:
    AlarmTopic = <<"$SYS/brokers/+/alarms/activate">>,
    ok = emqx_broker:subscribe(AlarmTopic),
    %% Start filling up send buffers with QoS0 publishes:
    Publisher = fun Publisher(N) ->
        Payload = binary:copy(<<N:64>>, PayloadSize div 8),
        _ = emqx:publish(emqx_message:make(<<"publisher">>, ?QOS_0, Topic, Payload)),
        ok = timer:sleep(PublishInterval),
        Publisher(N + 1)
    end,
    _PublisherPid = spawn_link(fun() -> Publisher(1) end),
    %% Start lagging consumer:
    ConsumerPid = spawn_link(fun() ->
        loop_slow_consumer(active, Socket, ConsumeRecvlen, ConsumeInterval)
    end),
    %% Congestion alarm should be raised soon by QoS0 traffic:
    {deliver, _, AlarmMsg} = ?assertReceive({deliver, AlarmTopic, _AlarmMsg}, 10_000),
    #{
        <<"name">> := <<"conn_congestion/t_congestion_qos0_no_send_timeout/undefined">>,
        <<"details">> := AlarmDetails
    } = emqx_utils_json:decode(emqx_message:payload(AlarmMsg)),
    ConnPid = list_to_pid(binary_to_list(maps:get(<<"pid">>, AlarmDetails))),
    MRef = erlang:monitor(process, ConnPid),
    %% Wait 3 times listener send timeout:
    %% QoS0 congestion must not cause the connection to close with `send_timeout`.
    ?assertNotReceive({'DOWN', MRef, process, ConnPid, {shutdown, _}}, 7_500),
    %% Verify that some QoS0 messages were actually dropped due to congestion:
    ?assertMatch(
        Dropped when Dropped > DroppedBefore,
        emqx_metrics:val_global('delivery.dropped.queue_full')
    ),
    ?assertMatch(
        SS when SS == idle; SS == running; SS == congested,
        emqx_cth_broker:connection_info(sockstate, ClientId)
    ),
    true = unlink(ConsumerPid),
    exit(ConsumerPid, shutdown),
    ok = socket_close(Socket).

%% Verify that slow send-congested MQTT client overwhelmed by a stream of QoS1 publishes raises
%% an alarm, and this alarm deactivates once congestion is relieved.
t_congestion_decongested(init, Config) ->
    override_conf(
        #{
            [conn_congestion, enable_alarm] => true,
            [conn_congestion, min_alarm_sustain_duration] => 0,
            %% This timeout drives congestion alarms, decrease to receive alarms sooner:
            [mqtt, idle_timeout] => 500,
            %% Congestion is exercised through QoS1 publishing, increase to avoid queueing:
            [mqtt, max_inflight] => 1000
        },
        Config
    ).

t_congestion_decongested(Config) ->
    PayloadSize = 12000,
    PublishInterval = 80,
    Socket = socket_connect(Config, [{buffer, 4096}, {active, false}, binary]),
    %% Send manually constructed CONNECT:
    ok = socket_send(
        Socket,
        emqx_frame:serialize(
            ?CONNECT_PACKET(#mqtt_packet_connect{clientid = <<"t_congestion_decongested">>})
        )
    ),
    {ok, Frames1} = socket_recv(Socket, 0, 1000),
    {Pkt1, <<>>, Parser1} = emqx_frame:parse(Frames1, emqx_frame:initial_parse_state()),
    ?assertMatch(?CONNACK_PACKET(0), Pkt1),
    %% Send manually constructed SUBSCRIBE to subscribe to "t":
    Topic = <<"t">>,
    ok = socket_send(
        Socket,
        emqx_frame:serialize(
            ?SUBSCRIBE_PACKET(1, [{Topic, #{rh => 0, rap => 0, nl => 0, qos => 1}}])
        )
    ),
    {ok, Frames2} = socket_recv(Socket, 0, 1000),
    {Pkt2, <<>>, _Parser2} = emqx_frame:parse(Frames2, Parser1),
    ?assertMatch(?SUBACK_PACKET(1, [?QOS_1]), Pkt2),
    %% Subscribe to alarms:
    ok = emqx_broker:subscribe(<<"$SYS/brokers/+/alarms/activate">>),
    ok = emqx_broker:subscribe(<<"$SYS/brokers/+/alarms/deactivate">>),
    %% Start filling up send buffers:
    Publisher = fun Publisher(N) ->
        Payload = binary:copy(<<N:64>>, PayloadSize div 8),
        _ = emqx:publish(emqx_message:make(<<"publisher">>, ?QOS_1, Topic, Payload)),
        ok = timer:sleep(PublishInterval),
        Publisher(N + 1)
    end,
    PublisherPid = spawn_link(fun() -> Publisher(1) end),
    %% Start consumer, initially paused:
    ConsumerPid = spawn_link(fun() -> loop_slow_consumer(paused, Socket, 0, 0) end),
    %% Congestion alarm should be raised soon:
    {deliver, _, AlarmActivated} =
        ?assertReceive({deliver, <<"$SYS/brokers/+/alarms/activate">>, _}, 10_000),
    ?assertMatch(
        #{<<"name">> := <<"conn_congestion/t_congestion_decongested/undefined">>},
        emqx_utils_json:decode(emqx_message:payload(AlarmActivated))
    ),
    %% Activate consumer, congestion should resolve soon:
    ConsumerPid ! activate,
    {deliver, _, AlarmDeactivated} =
        ?assertReceive({deliver, <<"$SYS/brokers/+/alarms/deactivate">>, _}, 10_000),
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
    ok = socket_close(Socket).

loop_slow_consumer(paused, Socket, Recvlen, Interval) ->
    receive
        activate ->
            loop_slow_consumer(active, Socket, Recvlen, Interval)
    after 10_000 ->
        exit(activate_timeout)
    end;
loop_slow_consumer(active, Socket, Recvlen, Interval) ->
    case socket_recv(Socket, Recvlen, 1000) of
        {ok, _Bytes} ->
            ok = timer:sleep(Interval),
            loop_slow_consumer(active, Socket, Recvlen, Interval);
        {error, timeout} ->
            loop_slow_consumer(active, Socket, Recvlen, Interval);
        {error, closed} ->
            closed
    end.

t_first_packet_not_connect(Config) ->
    Socket = socket_connect(Config, [{active, true}, binary]),
    %% Use a complete non-CONNECT MQTT packet to avoid packet=mqtt transport
    %% buffering an incomplete frame forever.
    ok = socket_send(Socket, <<?PINGREQ:4, 0:1, 0:2, 0:1, 0>>),
    receive
        {tcp_closed, RawSocket} when RawSocket =:= element(2, Socket) -> ok;
        {ssl_closed, RawSocket} when RawSocket =:= element(2, Socket) -> ok
    after 5000 ->
        ct:fail("Expected socket to be closed")
    end.

-doc """
A non-MQTT first packet shuts the connection down under the fixed
`invalid_connect_packet` counter, rather than being reported as an unidentified
shutdown at error level. The cause stays in the shutdown reason.
""".
t_frame_error_shutdown_count_idle(Config) ->
    Socket = socket_connect(Config, [{active, true}, binary]),
    %% CONNECT header, but the protocol name is not 'MQTT' nor 'MQIsdp'.
    Malformed = <<16#10, 12, 0, 6, "test/1", 4, 2, 0, 60>>,
    ExitReason = assert_frame_error_shutdown(Config, Socket, Malformed, invalid_connect_packet),
    ?assertMatch(
        {shutdown, #{cause := invalid_proto_name, received := <<"test/1">>}}, ExitReason
    ).

-doc """
A malformed packet from an already connected client shuts the connection down
under the fixed `frame_error` counter, rather than being reported as an
unidentified shutdown at error level.
""".
t_frame_error_shutdown_count_connected(Config) ->
    Socket = socket_connect(Config, [{active, true}, binary]),
    ConnPacket = ?CONNECT_PACKET(#mqtt_packet_connect{
        proto_ver = ?MQTT_PROTO_V5,
        clientid = atom_to_binary(?FUNCTION_NAME)
    }),
    ok = socket_send(Socket, emqx_frame:serialize(ConnPacket)),
    receive
        {tcp, _, <<32, _/binary>>} -> ok;
        {ssl, _, <<32, _/binary>>} -> ok
    after 5000 -> ct:fail({connack_not_received, process_info(self(), messages)})
    end,
    %% SUBSCRIBE carrying an unknown MQTT v5 property code (16#2B).
    Malformed = <<16#82, 9, 0, 1, 2, 16#2B, 0, 0, 1, $t, 0>>,
    ExitReason = assert_frame_error_shutdown(Config, Socket, Malformed, frame_error),
    ?assertEqual({shutdown, frame_error}, ExitReason).

-doc """
Frame parse errors reported as a map share the `frame_error` counter, so the
detail they carry cannot mint counter names. Errors reported as a bare atom come
from a fixed set and keep their own counter.
""".
t_frame_error_shutdown_count_is_bounded(Config) ->
    MapCauses = [
        %% #{cause => invalid_property_code, ...}
        <<16#82, 9, 0, 1, 2, 16#2B, 0, 0, 1, $t, 0>>,
        %% #{cause => invalid_proto_name, ...}, as reported in #17903
        <<16#10, 12, 0, 6, "test/1", 4, 2, 0, 60>>
    ],
    %% bad_subqos, a bare atom
    AtomCause = <<16#82, 7, 0, 1, 0, 0, 1, $t, 3>>,
    GroupedBefore = shutdown_count(Config, frame_error),
    NamedBefore = shutdown_count(Config, bad_subqos),
    lists:foreach(
        fun(Malformed) -> send_malformed_when_connected(Config, Malformed) end,
        [AtomCause | MapCauses]
    ),
    %% Every map cause lands on the one counter ...
    ?WAIT(
        ?assertEqual(GroupedBefore + length(MapCauses), shutdown_count(Config, frame_error)),
        5
    ),
    ?assertEqual(
        [],
        [
            K
         || K <- shutdown_count_keys(Config),
            lists:member(K, [invalid_property_code, invalid_proto_name])
        ]
    ),
    %% ... while an atom cause keeps its own.
    ?WAIT(?assertEqual(NamedBefore + 1, shutdown_count(Config, bad_subqos)), 5).

send_malformed_when_connected(Config, Malformed) ->
    Socket = socket_connect(Config, [{active, true}, binary]),
    %% Each connection needs its own clientid: reusing one would trip the
    %% clientid registration throttle and let a takeover, rather than the frame
    %% error, decide the exit reason.
    ClientId = iolist_to_binary([
        "send_malformed_when_connected-",
        integer_to_binary(erlang:unique_integer([positive]))
    ]),
    ConnPacket = ?CONNECT_PACKET(#mqtt_packet_connect{
        proto_ver = ?MQTT_PROTO_V5,
        clientid = ClientId
    }),
    ok = socket_send(Socket, emqx_frame:serialize(ConnPacket)),
    receive
        {tcp, _, <<32, _/binary>>} -> ok;
        {ssl, _, <<32, _/binary>>} -> ok
    after 5000 -> ct:fail({connack_not_received, process_info(self(), messages)})
    end,
    ok = socket_send(Socket, Malformed),
    {ok, _} = ?block_until(#{?snk_kind := terminate}, 5000).

%% Send `Malformed' and assert the connection exits with a reason the connection
%% supervisor attributes to `Cause'. Returns the exit reason.
assert_frame_error_shutdown(Config, Socket, Malformed, Cause) ->
    CountBefore = shutdown_count(Config, Cause),
    Self = self(),
    Reports = emqx_cth_log_capture:capture(debug, fun() ->
        ok = socket_send(Socket, Malformed),
        {ok, #{reason := R}} = ?block_until(#{?snk_kind := terminate}, 5000),
        Self ! {exit_reason, R}
    end),
    ExitReason =
        receive
            {exit_reason, R0} -> R0
        after 0 -> ct:fail(no_terminate_event)
        end,
    %% The shutdown counter only names a kind, so the specific cause and the
    %% offending bytes must stay reachable through `emqx_trace'.
    ?assertMatch(
        [#{reason := #{cause := _}, input_bytes := Malformed, at_state := _} | _],
        [R1 || #{msg := "frame_parse_error"} = R1 <- Reports]
    ),
    %% esockd attributes a shutdown to a cause when the reason is either a plain
    %% atom or a map tagged with `shutdown_count'; anything else is reported as
    %% an unidentified shutdown at error level and left uncounted.
    case ExitReason of
        {shutdown, Cause} -> ok;
        {shutdown, #{shutdown_count := Cause}} -> ok;
        Other -> ct:fail({unattributable_shutdown_reason, Other})
    end,
    %% Counter only moves on the info-level branch, so this also proves no
    %% error-level supervisor report was emitted.
    ?WAIT(?assertEqual(CountBefore + 1, shutdown_count(Config, Cause)), 5),
    ExitReason.

shutdown_count(Config, Cause) ->
    proplists:get_value(Cause, shutdown_counts(Config), 0).

shutdown_count_keys(Config) ->
    lists:sort(proplists:get_keys(shutdown_counts(Config))).

shutdown_counts(Config) ->
    emqx_listeners:shutdown_count(listener_id(Config), listener_port(Config)).

%%--------------------------------------------------------------------
%% Helper functions
%%--------------------------------------------------------------------

listener_id(Config) when is_list(Config) ->
    emqx_listeners:listener_id(?config(listener_type, Config), default).

listener_port(Config) when is_list(Config) -> listener_port(?config(listener_type, Config));
listener_port(tcp) -> 1883;
listener_port(ssl) -> 8883.

socket_emqtt_opts(Config) ->
    case ?config(listener_type, Config) of
        ssl ->
            [
                {port, listener_port(ssl)},
                {ssl, true},
                {ssl_opts, emqx_common_test_helpers:client_mtls()}
            ];
        _ ->
            [{port, listener_port(tcp)}]
    end.

socket_connect(Config, Opts) ->
    case ?config(listener_type, Config) of
        ssl ->
            {ok, Socket} = ssl:connect(
                {127, 0, 0, 1},
                listener_port(ssl),
                emqx_common_test_helpers:client_mtls() ++ Opts,
                5000
            ),
            {ssl, Socket};
        tcp ->
            {ok, Socket} = gen_tcp:connect(
                {127, 0, 0, 1},
                listener_port(tcp),
                Opts
            ),
            {tcp, Socket}
    end.

socket_send({tcp, Socket}, Data) ->
    gen_tcp:send(Socket, Data);
socket_send({ssl, Socket}, Data) ->
    ssl:send(Socket, Data).

socket_recv({tcp, Socket}, Length, Timeout) ->
    gen_tcp:recv(Socket, Length, Timeout);
socket_recv({ssl, Socket}, Length, Timeout) ->
    ssl:recv(Socket, Length, Timeout).

socket_close({tcp, Socket}) ->
    gen_tcp:close(Socket);
socket_close({ssl, Socket}) ->
    ssl:close(Socket).

socket_closed({tcp, Socket}) ->
    {tcp_closed, Socket};
socket_closed({ssl, Socket}) ->
    {ssl_closed, Socket}.

socket_sockname({tcp, Socket}) ->
    inet:sockname(Socket);
socket_sockname({ssl, Socket}) ->
    ssl:sockname(Socket).

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

override_conf(KVs, Config) ->
    maps:fold(fun(KeyPath, X, Acc) -> override_conf(KeyPath, X, Acc) end, Config, KVs).

override_conf(KeyPath, X, Config0) ->
    Config = save_conf(KeyPath, Config0),
    emqx_config:put(KeyPath, X),
    Config.

save_conf(KeyPath, Config) ->
    X = emqx_config:get(KeyPath),
    [{conf_saved, {KeyPath, X}} | Config].

restore_conf(Config) ->
    [
        emqx_config:put(KeyPath, X)
     || {conf_saved, {KeyPath, X}} <- Config
    ].

override_listener_conf(Name, KeyPath, X, Config0) ->
    Type = ?config(listener_type, Config0),
    Config = save_listener_conf(Type, Name, KeyPath, Config0),
    emqx_config:put_listener_conf(Type, Name, KeyPath, X),
    Config.

save_listener_conf(Type, Name, KeyPath, Config) ->
    LConf = emqx_config:get_listener_conf(Type, Name, KeyPath),
    [{listener_conf_saved, {Type, Name, KeyPath, LConf}} | Config].

restore_listener_conf(Config) ->
    [
        ok = emqx_config:put_listener_conf(Type, Name, KeyPath, LConf)
     || {listener_conf_saved, {Type, Name, KeyPath, LConf}} <- Config
    ].

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
    SslConf = emqx_common_test_helpers:client_mtls(TLSVsn),
    {ok, Client} = emqtt:start_link([{port, 8883}, {ssl, true}, {ssl_opts, SslConf}]),
    {ok, _} = emqtt:connect(Client),
    #{clientinfo := #{clientid := CN}} = emqx_cm:get_chan_info(CN),
    confirm_tls_version(Client, RequiredTLSVsn),
    emqtt:disconnect(Client).

meck_sched_delay(X) ->
    erlang:yield(),
    meck:passthrough([X]).
