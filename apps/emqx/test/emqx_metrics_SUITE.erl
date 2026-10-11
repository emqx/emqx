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

-doc "Flush receive totals once, including mixed QoS, invalid QoS and an incoming system topic.".
t_recv_deltas(_) ->
    with_metrics_server(fun() ->
        ok = emqx_metrics:inc_recv_bytes(128, ?global_ns),
        ok = emqx_metrics:inc_recv_bytes(256, ?global_ns),
        Packets = [
            ?PUBLISH_PACKET(0, 0),
            ?PUBLISH_PACKET(0, 0),
            ?PUBLISH_PACKET(1, 0),
            ?PUBLISH_PACKET(2, 0),
            ?PUBLISH_PACKET(3, 0),
            ?PUBLISH_PACKET(0, <<"$SYS/test">>, 0, <<>>),
            ?PACKET(?PINGREQ)
        ],
        lists:foreach(
            fun(Packet) ->
                ok = emqx_metrics:inc_recv_deferred(Packet, ?global_ns)
            end,
            Packets
        ),
        ?assertEqual(7, emqx_pd:get_counter(recv_pkt)),
        ?assertEqual(6, emqx_pd:get_counter(recv_msg)),
        ?assertEqual(0, emqx_metrics:val_global('bytes.received')),
        ?assertEqual(0, emqx_metrics:val_global('messages.received')),
        ?assertEqual(1, emqx_metrics:val_global('packets.received')),
        ?assertEqual(1, emqx_metrics:val_global('packets.pingreq.received')),
        ok = emqx_metrics:flush_connection(),
        ?assertEqual(384, emqx_metrics:val_global('bytes.received')),
        ?assertEqual(7, emqx_metrics:val_global('packets.received')),
        ?assertEqual(6, emqx_metrics:val_global('messages.received')),
        ?assertEqual(6, emqx_metrics:val_global('packets.publish.received')),
        ?assertEqual(3, emqx_metrics:val_global('messages.qos0.received')),
        ?assertEqual(1, emqx_metrics:val_global('messages.qos1.received')),
        ?assertEqual(1, emqx_metrics:val_global('messages.qos2.received')),
        ?assertEqual(384, get(recv_oct)),
        ?assertEqual(7, get(recv_pkt)),
        ?assertEqual(6, get(recv_msg)),
        ok = emqx_metrics:flush_connection(),
        ?assertEqual(6, emqx_metrics:val_global('messages.received'))
    end).

-doc "Keep namespace metrics immediate and tolerate namespace deletion before a flush.".
t_recv_deltas_namespace(_) ->
    with_metrics_server(fun() ->
        Namespace = <<"recv-deltas">>,
        ok = emqx_metrics:register_namespace(Namespace),
        ok = emqx_metrics:inc_recv_bytes(10, Namespace),
        ok = emqx_metrics:inc_recv_deferred(?PUBLISH_PACKET(1, 0), Namespace),
        ?assertEqual(0, emqx_metrics:val_global('messages.received')),
        ?assertEqual(10, emqx_metrics:val(Namespace, 'bytes.received')),
        ?assertEqual(1, emqx_metrics:val(Namespace, 'packets.received')),
        ?assertEqual(1, emqx_metrics:val(Namespace, 'messages.received')),
        ?assertEqual(1, emqx_metrics:val(Namespace, 'messages.qos1.received')),
        ok = emqx_metrics:unregister_namespace(Namespace),
        ok = emqx_metrics:inc_recv_bytes(20, Namespace),
        ok = emqx_metrics:inc_recv_deferred(?PUBLISH_PACKET(2, 0), Namespace),
        ok = emqx_metrics:flush_connection(),
        ?assertEqual(30, emqx_metrics:val_global('bytes.received')),
        ?assertEqual(2, emqx_metrics:val_global('messages.received'))
    end).

-doc "Batch send metrics while retaining system traffic in local counters and byte totals.".
t_sent_deltas(_) ->
    with_metrics_server(fun() ->
        Packets = [
            ?PUBLISH_PACKET(0, 0),
            ?PUBLISH_PACKET(0, <<"$SYS/first">>, 0, <<>>),
            ?PUBLISH_PACKET(1, <<"$SYS/second">>, 1, <<>>),
            ?PUBLISH_PACKET(1, 0),
            ?PUBLISH_PACKET(2, 0),
            ?PACKET(?PINGRESP)
        ],
        lists:foreach(
            fun(Packet) ->
                ok = emqx_metrics:inc_sent_deferred(Packet, ?global_ns)
            end,
            Packets
        ),
        ok = emqx_metrics:inc_sent_bytes(10, ?global_ns),
        ok = emqx_metrics:inc_sent_bytes(20, ?global_ns),
        ?assertEqual(6, emqx_pd:get_counter(send_pkt)),
        ?assertEqual(5, emqx_pd:get_counter(send_msg)),
        ?assertEqual(2, emqx_pd:get_counter('send_msg.qos0')),
        ?assertEqual(2, emqx_pd:get_counter('send_msg.qos1')),
        ?assertEqual(1, emqx_pd:get_counter('send_msg.qos2')),
        ?assertEqual(0, emqx_metrics:val_global('messages.sent')),
        ?assertEqual(1, emqx_metrics:val_global('packets.sent')),
        ?assertEqual(1, emqx_metrics:val_global('packets.pingresp.sent')),
        ok = emqx_metrics:flush_connection(),
        ?assertEqual(30, emqx_metrics:val_global('bytes.sent')),
        ?assertEqual(4, emqx_metrics:val_global('packets.sent')),
        ?assertEqual(3, emqx_metrics:val_global('messages.sent')),
        ?assertEqual(3, emqx_metrics:val_global('packets.publish.sent')),
        ?assertEqual(1, emqx_metrics:val_global('messages.qos0.sent')),
        ?assertEqual(1, emqx_metrics:val_global('messages.qos1.sent')),
        ?assertEqual(1, emqx_metrics:val_global('messages.qos2.sent')),
        ?assertEqual(6, get(send_pkt)),
        ?assertEqual(5, get(send_msg)),
        ok = emqx_metrics:inc_sent_deferred(
            ?PUBLISH_PACKET(0, <<"$SYS/after-flush">>, 0, <<>>), ?global_ns
        ),
        ?assertEqual(6, get(send_msg)),
        ok = emqx_metrics:flush_connection(),
        ?assertEqual(3, emqx_metrics:val_global('messages.sent'))
    end).

-doc "Keep namespace sends immediate and exclude system PUBLISH packets before namespace updates.".
t_sent_deltas_namespace(_) ->
    with_metrics_server(fun() ->
        Namespace = <<"sent-deltas">>,
        ok = emqx_metrics:register_namespace(Namespace),
        ok = emqx_metrics:inc_sent_deferred(?PUBLISH_PACKET(0, 0), Namespace),
        ok = emqx_metrics:inc_sent_deferred(
            ?PUBLISH_PACKET(0, <<"$SYS/test">>, 0, <<>>), Namespace
        ),
        ok = emqx_metrics:inc_sent_bytes(20, Namespace),
        ?assertEqual(0, emqx_metrics:val_global('messages.sent')),
        ?assertEqual(1, emqx_metrics:val(Namespace, 'messages.sent')),
        ?assertEqual(1, emqx_metrics:val(Namespace, 'packets.sent')),
        ?assertEqual(20, emqx_metrics:val(Namespace, 'bytes.sent')),
        ok = emqx_metrics:unregister_namespace(Namespace),
        ok = emqx_metrics:inc_sent_deferred(?PUBLISH_PACKET(1, 0), Namespace),
        ok = emqx_metrics:inc_sent_bytes(10, Namespace),
        ok = emqx_metrics:flush_connection(),
        ?assertEqual(2, emqx_metrics:val_global('messages.sent')),
        ?assertEqual(30, emqx_metrics:val_global('bytes.sent'))
    end).

-doc "Throttle busy-loop flushes and let an explicit idle flush report the final delta.".
t_recv_deltas_throttle(_) ->
    with_metrics_server(fun() ->
        ok = emqx_metrics:inc_recv_deferred(?PUBLISH_PACKET(0, 0), ?global_ns),
        ok = emqx_metrics:inc_sent_deferred(?PUBLISH_PACKET(0, 0), ?global_ns),
        ok = emqx_metrics:maybe_flush_connection(1000),
        ok = emqx_metrics:maybe_flush_connection(1099),
        ?assertEqual(0, emqx_metrics:val_global('messages.received')),
        ?assertEqual(0, emqx_metrics:val_global('messages.sent')),
        ok = emqx_metrics:maybe_flush_connection(1100),
        ?assertEqual(1, emqx_metrics:val_global('messages.received')),
        ?assertEqual(1, emqx_metrics:val_global('messages.sent')),
        ?assertEqual(1, emqx_pd:get_counter(recv_pkt)),
        ok = emqx_metrics:inc_recv_deferred(?PUBLISH_PACKET(0, 0), ?global_ns),
        ok = emqx_metrics:inc_sent_deferred(?PUBLISH_PACKET(0, 0), ?global_ns),
        ok = emqx_metrics:maybe_flush_connection(1101),
        ?assertEqual(1, emqx_metrics:val_global('messages.received')),
        ?assertEqual(2, emqx_pd:get_counter(recv_pkt)),
        ok = emqx_metrics:flush_connection(),
        ?assertEqual(2, emqx_metrics:val_global('messages.received')),
        ?assertEqual(2, emqx_metrics:val_global('messages.sent'))
    end).

-doc "Resolve the current counter reference when metrics restart with pending receive deltas.".
t_recv_deltas_restart(_) ->
    with_metrics_server(fun() ->
        ok = emqx_metrics:inc_recv_deferred(?PUBLISH_PACKET(0, 0), ?global_ns),
        ok = emqx_metrics:inc_sent_deferred(?PUBLISH_PACKET(0, 0), ?global_ns),
        ok = emqx_metrics:flush_connection(),
        ok = emqx_metrics:inc_recv_deferred(?PUBLISH_PACKET(1, 0), ?global_ns),
        ok = emqx_metrics:inc_sent_deferred(?PUBLISH_PACKET(2, 0), ?global_ns),
        ok = emqx_metrics:stop(),
        {ok, _} = emqx_metrics:start_link(),
        ok = emqx_metrics:flush_connection(),
        ?assertEqual(1, emqx_metrics:val_global('messages.received')),
        ?assertEqual(0, emqx_metrics:val_global('messages.qos0.received')),
        ?assertEqual(1, emqx_metrics:val_global('messages.qos1.received')),
        ?assertEqual(1, emqx_metrics:val_global('messages.sent')),
        ?assertEqual(0, emqx_metrics:val_global('messages.qos0.sent')),
        ?assertEqual(1, emqx_metrics:val_global('messages.qos2.sent')),
        ?assertEqual(2, emqx_pd:get_counter(recv_msg))
    end).

-doc "An idle process with no deltas needs no metrics server or dictionary allocation.".
t_recv_deltas_empty(_) ->
    ok = emqx_metrics:maybe_flush_connection(),
    ok = emqx_metrics:flush_connection(),
    ?assertEqual(undefined, get(recv_oct)),
    ?assertEqual(undefined, get(recv_msg)),
    ?assertEqual(undefined, get(send_oct)),
    ?assertEqual(undefined, get(send_msg)),
    ?assertEqual(undefined, get({emqx_metrics, connection_flush_time})).

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
