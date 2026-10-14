%%--------------------------------------------------------------------
%% Copyright (c) 2018-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_metrics_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("emqx/include/emqx_config.hrl").

all() -> emqx_common_test_helpers:all(?MODULE).

inc_recv(Name) ->
    emqx_metrics:inc_recv(Name, ?global_ns).

inc_sent(Name) ->
    emqx_metrics:inc_sent(Name, ?global_ns).

t_new(_) ->
    with_metrics_server(
        fun() ->
            ok = emqx_metrics:new('metrics.test'),
            ok = emqx_metrics:new('metrics.test'),
            0 = emqx_metrics:val_global('metrics.test'),
            ok = emqx_metrics:inc_global('metrics.test'),
            1 = emqx_metrics:val_global('metrics.test'),
            ok = emqx_metrics:new(counter, 'metrics.test.cnt'),
            0 = emqx_metrics:val_global('metrics.test.cnt'),
            ok = emqx_metrics:inc_global('metrics.test.cnt'),
            1 = emqx_metrics:val_global('metrics.test.cnt'),
            ok = emqx_metrics:new(gauge, 'metrics.test.total'),
            0 = emqx_metrics:val_global('metrics.test.total'),
            ok = emqx_metrics:inc_global('metrics.test.total'),
            1 = emqx_metrics:val_global('metrics.test.total')
        end
    ).

t_ensure(_) ->
    with_metrics_server(
        fun() ->
            ok = emqx_metrics:ensure('metrics.test'),
            ok = emqx_metrics:ensure('metrics.test'),
            0 = emqx_metrics:val_global('metrics.test'),
            ok = emqx_metrics:inc_global('metrics.test'),
            1 = emqx_metrics:val_global('metrics.test'),
            ok = emqx_metrics:ensure(counter, 'metrics.test.cnt'),
            0 = emqx_metrics:val_global('metrics.test.cnt'),
            ok = emqx_metrics:inc_global('metrics.test.cnt'),
            1 = emqx_metrics:val_global('metrics.test.cnt'),
            ok = emqx_metrics:ensure(gauge, 'metrics.test.total'),
            0 = emqx_metrics:val_global('metrics.test.total'),
            ok = emqx_metrics:inc_global('metrics.test.total'),
            1 = emqx_metrics:val_global('metrics.test.total')
        end
    ).

t_all(_) ->
    with_metrics_server(
        fun() ->
            Metrics = emqx_metrics:all_global(),
            ?assert(length(Metrics) > 50)
        end
    ).

t_inc_dec(_) ->
    with_metrics_server(
        fun() ->
            ?assertEqual(0, emqx_metrics:val_global('bytes.received')),
            ok = emqx_metrics:inc_global('bytes.received'),
            ok = emqx_metrics:inc_global('bytes.received', 2),
            ok = emqx_metrics:inc_global('bytes.received', 2),
            ?assertEqual(5, emqx_metrics:val_global('bytes.received'))
        end
    ).

t_inc_recv(_) ->
    with_metrics_server(
        fun() ->
            ok = inc_recv(?PACKET(?CONNECT)),
            ok = inc_recv(?PUBLISH_PACKET(0, 0)),
            ok = inc_recv(?PUBLISH_PACKET(1, 0)),
            ok = inc_recv(?PUBLISH_PACKET(2, 0)),
            ok = inc_recv(?PUBLISH_PACKET(3, 0)),
            ok = inc_recv(?PACKET(?PUBACK)),
            ok = inc_recv(?PACKET(?PUBREC)),
            ok = inc_recv(?PACKET(?PUBREL)),
            ok = inc_recv(?PACKET(?PUBCOMP)),
            ok = inc_recv(?PACKET(?SUBSCRIBE)),
            ok = inc_recv(?PACKET(?UNSUBSCRIBE)),
            ok = inc_recv(?PACKET(?PINGREQ)),
            ok = inc_recv(?PACKET(?DISCONNECT)),
            ok = inc_recv(?PACKET(?AUTH)),
            ok = inc_recv(?PACKET(?RESERVED)),
            ?assertEqual(15, emqx_metrics:val_global('packets.received')),
            ?assertEqual(1, emqx_metrics:val_global('packets.connect.received')),
            ?assertEqual(4, emqx_metrics:val_global('messages.received')),
            ?assertEqual(1, emqx_metrics:val_global('messages.qos0.received')),
            ?assertEqual(1, emqx_metrics:val_global('messages.qos1.received')),
            ?assertEqual(1, emqx_metrics:val_global('messages.qos2.received')),
            ?assertEqual(4, emqx_metrics:val_global('packets.publish.received')),
            ?assertEqual(1, emqx_metrics:val_global('packets.puback.received')),
            ?assertEqual(1, emqx_metrics:val_global('packets.pubrec.received')),
            ?assertEqual(1, emqx_metrics:val_global('packets.pubrel.received')),
            ?assertEqual(1, emqx_metrics:val_global('packets.pubcomp.received')),
            ?assertEqual(1, emqx_metrics:val_global('packets.subscribe.received')),
            ?assertEqual(1, emqx_metrics:val_global('packets.unsubscribe.received')),
            ?assertEqual(1, emqx_metrics:val_global('packets.pingreq.received')),
            ?assertEqual(1, emqx_metrics:val_global('packets.disconnect.received')),
            ?assertEqual(1, emqx_metrics:val_global('packets.auth.received'))
        end
    ).

t_inc_sent(_) ->
    with_metrics_server(
        fun() ->
            ok = inc_sent(?CONNACK_PACKET(0)),
            ok = inc_sent(?CONNACK_PACKET(0, 1)),
            ok = inc_sent(
                ?CONNACK_PACKET(0, 1, #{
                    'Maximum-Packet-Size' => 1048576,
                    'Retain-Available' => 1,
                    'Shared-Subscription-Available' => 1,
                    'Subscription-Identifier-Available' => 1,
                    'Topic-Alias-Maximum' => 65535,
                    'Wildcard-Subscription-Available' => 1
                })
            ),
            ok = inc_sent(?PUBLISH_PACKET(0, 0)),
            ok = inc_sent(?PUBLISH_PACKET(1, 0)),
            ok = inc_sent(?PUBLISH_PACKET(2, 0)),
            ok = inc_sent(?PUBACK_PACKET(0, 0)),
            ok = inc_sent(?PUBREC_PACKET(3, 0)),
            ok = inc_sent(?PACKET(?PUBREL)),
            ok = inc_sent(?PACKET(?PUBCOMP)),
            ok = inc_sent(?SUBACK_PACKET(0, [?RC_SUCCESS])),
            ok = inc_sent(?PACKET(?UNSUBACK)),
            ok = inc_sent(?PACKET(?PINGRESP)),
            ok = inc_sent(?PACKET(?DISCONNECT)),
            ok = inc_sent(?PACKET(?AUTH)),
            ?assertEqual(15, emqx_metrics:val_global('packets.sent')),
            ?assertEqual(3, emqx_metrics:val_global('packets.connack.sent')),
            ?assertEqual(3, emqx_metrics:val_global('messages.sent')),
            ?assertEqual(1, emqx_metrics:val_global('messages.qos0.sent')),
            ?assertEqual(1, emqx_metrics:val_global('messages.qos1.sent')),
            ?assertEqual(1, emqx_metrics:val_global('messages.qos2.sent')),
            ?assertEqual(3, emqx_metrics:val_global('packets.publish.sent')),
            ?assertEqual(1, emqx_metrics:val_global('packets.puback.sent')),
            ?assertEqual(1, emqx_metrics:val_global('packets.pubrec.sent')),
            ?assertEqual(1, emqx_metrics:val_global('packets.pubrel.sent')),
            ?assertEqual(1, emqx_metrics:val_global('packets.pubcomp.sent')),
            ?assertEqual(1, emqx_metrics:val_global('packets.suback.sent')),
            ?assertEqual(1, emqx_metrics:val_global('packets.unsuback.sent')),
            ?assertEqual(1, emqx_metrics:val_global('packets.pingresp.sent')),
            ?assertEqual(1, emqx_metrics:val_global('packets.disconnect.sent')),
            ?assertEqual(1, emqx_metrics:val_global('packets.auth.sent'))
        end
    ).

t_inc_sent_auth_error(_) ->
    %% verify the packets.publish.error, packets.publish.auth_error,
    %% packets.subscribe.error, packets.subscribe.auth_error can be incremented correctly
    with_metrics_server(
        fun() ->
            %% Subscribe
            ok = inc_sent(?SUBACK_PACKET(0, [?RC_NOT_AUTHORIZED])),
            ok = inc_sent(?SUBACK_PACKET(0, [?RC_UNSPECIFIED_ERROR])),
            ?assertEqual(1, emqx_metrics:val_global('packets.subscribe.auth_error')),
            ?assertEqual(2, emqx_metrics:val_global('packets.subscribe.error')),
            ?assertEqual(2, emqx_metrics:val_global('packets.suback.sent')),
            %% Publish - puback
            ok = inc_sent(?PUBACK_PACKET(0, ?RC_UNSPECIFIED_ERROR)),
            ok = inc_sent(?PUBACK_PACKET(0, ?RC_NOT_AUTHORIZED)),
            ?assertEqual(1, emqx_metrics:val_global('packets.publish.auth_error')),
            ?assertEqual(2, emqx_metrics:val_global('packets.publish.error')),
            ?assertEqual(2, emqx_metrics:val_global('packets.puback.sent')),
            %% Publish - pubrec
            ok = inc_sent(?PUBREC_PACKET(0, ?RC_UNSPECIFIED_ERROR)),
            ok = inc_sent(?PUBREC_PACKET(0, ?RC_NOT_AUTHORIZED)),
            ?assertEqual(2, emqx_metrics:val_global('packets.publish.auth_error')),
            ?assertEqual(4, emqx_metrics:val_global('packets.publish.error')),
            ?assertEqual(2, emqx_metrics:val_global('packets.pubrec.sent'))
        end
    ).

-doc "Packet counters update immediately in both the global and named namespaces.".
t_packet_metrics_namespace(_) ->
    with_metrics_server(
        fun() ->
            Namespace = <<"packet-metrics">>,
            ok = emqx_metrics:register_namespace(Namespace),
            try
                Received = [
                    ?PUBLISH_PACKET(0, <<"t">>, undefined, <<>>),
                    ?PUBLISH_PACKET(1, <<"$SYS/t">>, 1, <<>>),
                    ?PUBLISH_PACKET(2, <<"t">>, 2, <<>>),
                    ?PUBLISH_PACKET(3, 0),
                    ?PACKET(?RESERVED)
                ],
                Sent = [
                    ?PUBLISH_PACKET(0, <<"t">>, undefined, <<>>),
                    ?PUBLISH_PACKET(1, <<"t">>, 1, <<>>),
                    ?PUBLISH_PACKET(2, <<"t">>, 2, <<>>),
                    ?PUBLISH_PACKET(3, 0),
                    ?PUBLISH_PACKET(0, <<"$SYS/t">>, undefined, <<>>),
                    ?CONNACK_PACKET(?RC_NOT_AUTHORIZED),
                    ?PUBACK_PACKET(1, ?RC_NOT_AUTHORIZED),
                    ?PUBREC_PACKET(2, ?RC_UNSPECIFIED_ERROR),
                    ?SUBACK_PACKET(3, [?RC_NOT_AUTHORIZED, ?RC_UNSPECIFIED_ERROR]),
                    ?PACKET(?RESERVED)
                ],
                lists:foreach(
                    fun(Packet) -> ok = emqx_metrics:inc_recv(Packet, Namespace) end, Received
                ),
                lists:foreach(
                    fun(Packet) -> ok = emqx_metrics:inc_sent(Packet, Namespace) end, Sent
                ),
                Expected = [
                    {'packets.received', 5},
                    {'messages.received', 4},
                    {'packets.publish.received', 4},
                    {'messages.qos0.received', 1},
                    {'messages.qos1.received', 1},
                    {'messages.qos2.received', 1},
                    {'packets.sent', 9},
                    {'messages.sent', 4},
                    {'packets.publish.sent', 4},
                    {'messages.qos0.sent', 1},
                    {'messages.qos1.sent', 1},
                    {'messages.qos2.sent', 1},
                    {'packets.connack.sent', 1},
                    {'packets.connack.error', 1},
                    {'packets.connack.auth_error', 1},
                    {'packets.puback.sent', 1},
                    {'packets.pubrec.sent', 1},
                    {'packets.publish.error', 2},
                    {'packets.publish.auth_error', 1},
                    {'packets.suback.sent', 1},
                    {'packets.subscribe.error', 1},
                    {'packets.subscribe.auth_error', 1}
                ],
                assert_packet_metrics(?global_ns, Expected),
                assert_packet_metrics(Namespace, Expected),
                ?assertEqual(
                    lists:sort(emqx_metrics:all_global()),
                    lists:sort(emqx_metrics:all(Namespace))
                )
            after
                ok = emqx_metrics:unregister_namespace(Namespace)
            end
        end
    ).

-doc "Missing or recreated namespaces do not stop global packet accounting.".
t_packet_metrics_namespace_lifecycle(_) ->
    with_metrics_server(
        fun() ->
            Namespace = <<"packet-metrics-lifecycle">>,
            Received = ?PUBLISH_PACKET(0, 0),
            Sent = ?PUBLISH_PACKET(1, 0),
            Update = fun() ->
                ok = emqx_metrics:inc_recv(Received, Namespace),
                ok = emqx_metrics:inc_sent(Sent, Namespace)
            end,
            ok = Update(),
            assert_packet_metrics(?global_ns, [{'messages.received', 1}, {'messages.sent', 1}]),
            ok = emqx_metrics:register_namespace(Namespace),
            try
                ok = Update(),
                assert_packet_metrics(Namespace, [{'messages.received', 1}, {'messages.sent', 1}]),
                ok = emqx_metrics:unregister_namespace(Namespace),
                ok = Update(),
                assert_packet_metrics(?global_ns, [{'messages.received', 3}, {'messages.sent', 3}]),
                ok = emqx_metrics:register_namespace(Namespace),
                assert_packet_metrics(Namespace, [{'messages.received', 0}, {'messages.sent', 0}]),
                ok = Update(),
                assert_packet_metrics(Namespace, [{'messages.received', 1}, {'messages.sent', 1}]),
                assert_packet_metrics(?global_ns, [{'messages.received', 4}, {'messages.sent', 4}])
            after
                ok = emqx_metrics:unregister_namespace(Namespace)
            end
        end
    ).

assert_packet_metrics(Namespace, Expected) ->
    lists:foreach(
        fun({Name, Value}) -> ?assertEqual(Value, emqx_metrics:val(Namespace, Name)) end,
        Expected
    ).

with_metrics_server(Fun) ->
    try
        supervisor:terminate_child(emqx_kernel_sup, emqx_metrics)
    catch
        exit:_ ->
            ok
    end,
    {ok, _} = emqx_metrics:start_link(),
    try
        _ = Fun(),
        ok
    after
        ok = emqx_metrics:stop()
    end.
