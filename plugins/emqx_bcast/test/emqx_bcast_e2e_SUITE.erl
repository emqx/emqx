%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bcast_e2e_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include("emqx_bcast.hrl").

-define(PAYLOAD, <<"e2e_test_payload">>).

all() -> emqx_common_test_helpers:all(?MODULE).

-define(EMQX_CONF, #{
    <<"listeners">> => #{<<"tcp">> => #{<<"default">> => #{<<"acceptors">> => 4}}},
    <<"authorization">> => #{<<"no_match">> => <<"allow">>}
}).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [{emqx, ?EMQX_CONF}, mria],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    %% Start the plugin application so it owns the ETS/Mnesia tables, hooks and
    %% metrics registry. This exercises the normal startup path instead of the
    %% suite wiring each resource by hand.
    {ok, _} = application:ensure_all_started(prometheus),
    {ok, _} = application:ensure_all_started(emqx_bcast),
    init_test_config(),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    ok = application:stop(emqx_bcast),
    ok = application:stop(prometheus),
    emqx_cth_suite:stop(?config(apps, Config)).

init_test_config() ->
    persistent_term:put({emqx_bcast, config}, #{
        msg_ttl => 15 * 86400,
        cleanup_interval => 60,
        max_device_count => 10000,
        max_message_size_batch => 10240,
        max_message_size_broadcast => 65536,
        broadcast_topic => <<"/sys/broadcast/${productKey}">>,
        batch_topic => <<"/${productKey}/${deviceName}/user/get">>,
        force_upgrade_qos => true
    }).

init_per_testcase(_Case, Config) ->
    emqx_bcast_metrics:init(),
    emqx_bcast_subscription:init(),
    Config.
end_per_testcase(_Case, _Config) -> ok.

%% helpers

connect(ClientId) ->
    {ok, C} = emqtt:start_link([{clean_start, true}, {clientid, ClientId}]),
    {ok, _} = emqtt:connect(C),
    put_clients(C, ClientId),
    C.

sub(C, Topic) ->
    emqtt:subscribe(C, Topic, 1).

sub_qos(C, Topic, Qos) ->
    emqtt:subscribe(C, Topic, Qos).

sub_default(C, DeviceName) ->
    sub(C, <<"/default/", DeviceName/binary, "/user/get">>).

unsub(C, Topic) ->
    emqtt:unsubscribe(C, Topic).

disconnect(C) ->
    ok = emqtt:disconnect(C),
    case take_clients(C) of
        {ok, ClientId} -> wait_channel_gone(ClientId);
        error -> ok
    end.

%% Reconnecting with the same clientid right after disconnect races with the
%% old channel's async cleanup: while the stale row is still registered,
%% emqx_cm refuses the new connection (CONNACK server_unavailable). Wait
%% until the old channel is fully unregistered before returning.
wait_channel_gone(ClientId) ->
    wait_channel_gone(ClientId, 100).

wait_channel_gone(ClientId, 0) ->
    error({channel_still_registered, ClientId});
wait_channel_gone(ClientId, N) ->
    case emqx_cm:lookup_channels(ClientId) of
        [] ->
            ok;
        _ ->
            ct:sleep(50),
            wait_channel_gone(ClientId, N - 1)
    end.

%% The client pid -> clientid map must survive across test cases, which run in
%% separate processes. persistent_term is process-independent, so it works no
%% matter which process calls connect/1 or disconnect/1.
put_clients(C, ClientId) ->
    Clients = persistent_term:get({?MODULE, clients}, []),
    persistent_term:put({?MODULE, clients}, [{C, ClientId} | Clients]).

take_clients(C) ->
    Clients = persistent_term:get({?MODULE, clients}, []),
    case lists:keytake(C, 1, Clients) of
        {value, {C, ClientId}, Rest} ->
            persistent_term:put({?MODULE, clients}, Rest),
            {ok, ClientId};
        false ->
            error
    end.

api_call(Body) -> emqx_bcast_api:handle(post, [<<"pub">>], #{body => Body}).

b64(S) -> base64:encode(S).

recv(Count) -> recv(Count, []).
recv(0, Msgs) ->
    Msgs;
recv(Count, Msgs) ->
    receive
        {publish, Msg} -> recv(Count - 1, [Msg | Msgs])
    after 2000 -> Msgs
    end.

topic(DN) -> <<"/default/", DN/binary, "/user/get">>.

%% The subscribe hook casts into emqx_bcast_pull_pool asynchronously; poll the
%% subscription table until the cast has been processed instead of sleeping.
wait_subscribed(ClientId, Topic) ->
    ?assert(
        wait_until(
            fun() -> emqx_bcast_subscription:match(ClientId, Topic) =/= false end,
            100
        )
    ).

%% The connected hook casts into emqx_bcast_pull_pool asynchronously; poll the
%% device table until the registration has landed instead of sleeping.
wait_registered(ClientId) ->
    ?assert(
        wait_until(
            fun() ->
                case emqx_bcast:lookup_device({<<"default">>, ClientId}) of
                    {ok, _} -> true;
                    {error, not_found} -> false
                end
            end,
            100
        )
    ).

%% tests

-doc "Plain MQTT pubsub works as a baseline sanity check.".
t_pubsub_works(_Config) ->
    C = connect(<<"test_sub">>),
    sub(C, <<"t1">>),
    ok = emqtt:publish(C, <<"t1">>, <<"hi">>, 0),
    Msgs = recv(1),
    ?assertEqual(1, length(Msgs)),
    ?assertMatch(#{payload := <<"hi">>}, hd(Msgs)),
    disconnect(C).

-doc "QoS=0 BatchPub delivers to two subscribed online devices.".
t_batch_pub_qos0_e2e(_Config) ->
    C1 = connect(<<"e2e_q0_1">>),
    C2 = connect(<<"e2e_q0_2">>),
    sub_default(C1, <<"e2e_q0_1">>),
    sub_default(C2, <<"e2e_q0_2">>),
    wait_subscribed(<<"e2e_q0_1">>, topic(<<"e2e_q0_1">>)),
    wait_subscribed(<<"e2e_q0_2">>, topic(<<"e2e_q0_2">>)),
    {ok, 200, _, Resp} = api_call(#{
        <<"Action">> => <<"BatchPub">>,
        <<"ProductKey">> => <<"default">>,
        <<"DeviceName">> => [<<"e2e_q0_1">>, <<"e2e_q0_2">>],
        <<"MessageContent">> => b64(?PAYLOAD),
        <<"Qos">> => 0
    }),
    ?assert(maps:get(<<"Success">>, Resp)),
    Msgs = recv(2),
    ?assertEqual(2, length(Msgs)),
    disconnect(C1),
    disconnect(C2).

-doc "QoS=1 BatchPub delivers to two subscribed online devices and waits for PUBACK.".
t_batch_pub_qos1_e2e(_Config) ->
    C1 = connect(<<"e2e_q1_1">>),
    C2 = connect(<<"e2e_q1_2">>),
    sub_default(C1, <<"e2e_q1_1">>),
    sub_default(C2, <<"e2e_q1_2">>),
    wait_subscribed(<<"e2e_q1_1">>, topic(<<"e2e_q1_1">>)),
    wait_subscribed(<<"e2e_q1_2">>, topic(<<"e2e_q1_2">>)),
    {ok, 200, _, Resp} = api_call(#{
        <<"Action">> => <<"BatchPub">>,
        <<"ProductKey">> => <<"default">>,
        <<"DeviceName">> => [<<"e2e_q1_1">>, <<"e2e_q1_2">>],
        <<"MessageContent">> => b64(?PAYLOAD),
        <<"Qos">> => 1
    }),
    ?assert(maps:get(<<"Success">>, Resp)),
    Msgs = recv(2),
    ?assertEqual(2, length(Msgs)),
    disconnect(C1),
    disconnect(C2).

-doc "BatchPub by MessageId reuses a pre-registered message payload.".
t_batch_pub_messageid_reuse_e2e(_Config) ->
    B64 = b64(<<"reuse_payload">>),
    {ok, 200, _, RegResp} = api_call(#{
        <<"Action">> => <<"RegisterMessage">>, <<"MessageContent">> => B64
    }),
    MsgId = maps:get(<<"MessageId">>, RegResp),
    C1 = connect(<<"e2e_reuse_1">>),
    sub_default(C1, <<"e2e_reuse_1">>),
    wait_subscribed(<<"e2e_reuse_1">>, topic(<<"e2e_reuse_1">>)),
    {ok, 200, _, Resp} = api_call(#{
        <<"Action">> => <<"BatchPub">>,
        <<"ProductKey">> => <<"default">>,
        <<"DeviceName">> => [<<"e2e_reuse_1">>],
        <<"MessageId">> => MsgId,
        <<"Qos">> => 1
    }),
    ?assert(maps:get(<<"Success">>, Resp)),
    ?assertEqual(MsgId, maps:get(<<"MessageId">>, Resp)),
    Msgs = recv(1),
    ?assertEqual(1, length(Msgs)),
    ?assertMatch(#{payload := <<"reuse_payload">>}, hd(Msgs)),
    disconnect(C1).

-doc "PubBroadcast reaches all online devices subscribed to the broadcast topic.".
t_pub_broadcast_e2e(_Config) ->
    C1 = connect(<<"e2e_bc_1">>),
    C2 = connect(<<"e2e_bc_2">>),
    C3 = connect(<<"e2e_bc_3">>),
    sub(C1, <<"/sys/broadcast/default">>),
    sub(C2, <<"/sys/broadcast/default">>),
    sub(C3, <<"/sys/broadcast/default">>),
    wait_subscribed(<<"e2e_bc_1">>, <<"/sys/broadcast/default">>),
    wait_subscribed(<<"e2e_bc_2">>, <<"/sys/broadcast/default">>),
    wait_subscribed(<<"e2e_bc_3">>, <<"/sys/broadcast/default">>),
    {ok, 200, _, Resp} = api_call(#{
        <<"Action">> => <<"PubBroadcast">>,
        <<"ProductKey">> => <<"default">>,
        <<"MessageContent">> => b64(?PAYLOAD)
    }),
    ?assert(maps:get(<<"Success">>, Resp)),
    Msgs = recv(3),
    ?assertEqual(3, length(Msgs)),
    disconnect(C1),
    disconnect(C2),
    disconnect(C3).

-doc "QoS=1 BatchPub delivers online now and replays to the offline device when it connects.".
t_batch_pub_partial_online_e2e(_Config) ->
    C1 = connect(<<"e2e_part_1">>),
    sub_default(C1, <<"e2e_part_1">>),
    wait_subscribed(<<"e2e_part_1">>, topic(<<"e2e_part_1">>)),
    {ok, 200, _, _} = api_call(#{
        <<"Action">> => <<"BatchPub">>,
        <<"ProductKey">> => <<"default">>,
        <<"DeviceName">> => [<<"e2e_part_1">>, <<"e2e_part_2">>],
        <<"MessageContent">> => b64(?PAYLOAD),
        <<"Qos">> => 1
    }),
    Msgs1 = recv(1),
    ?assertEqual(1, length(Msgs1)),
    C2 = connect(<<"e2e_part_2">>),
    sub_default(C2, <<"e2e_part_2">>),
    wait_subscribed(<<"e2e_part_2">>, topic(<<"e2e_part_2">>)),
    Msgs2 = recv(1),
    ?assertEqual(1, length(Msgs2)),
    disconnect(C1),
    disconnect(C2).

-doc "BatchPub honors a custom TopicTemplateName for the delivery topic.".
t_batch_pub_topic_template_e2e(_Config) ->
    CustomTopic = <<"/custom/${deviceName}/topic">>,
    C1 = connect(<<"e2e_tpl_1">>),
    sub(C1, <<"/custom/e2e_tpl_1/topic">>),
    wait_subscribed(<<"e2e_tpl_1">>, <<"/custom/e2e_tpl_1/topic">>),
    {ok, 200, _, _} = api_call(#{
        <<"Action">> => <<"BatchPub">>,
        <<"ProductKey">> => <<"default">>,
        <<"DeviceName">> => [<<"e2e_tpl_1">>],
        <<"MessageContent">> => b64(?PAYLOAD),
        <<"Qos">> => 0,
        <<"TopicTemplateName">> => CustomTopic
    }),
    Msgs = recv(1),
    ?assertEqual(1, length(Msgs)),
    disconnect(C1).

-doc "RegisterMessage deduplicates identical content to the same MessageId.".
t_register_message_e2e(_Config) ->
    B64 = b64(<<"reg_message">>),
    {ok, 200, _, R1} = api_call(#{
        <<"Action">> => <<"RegisterMessage">>, <<"MessageContent">> => B64
    }),
    Mid1 = maps:get(<<"MessageId">>, R1),
    {ok, 200, _, R2} = api_call(#{
        <<"Action">> => <<"RegisterMessage">>, <<"MessageContent">> => B64
    }),
    ?assertEqual(Mid1, maps:get(<<"MessageId">>, R2)).

-doc "QoS=0 BatchPub is not delivered to a device without a matching subscription.".
t_batch_pub_qos0_no_sub(_Config) ->
    C1 = connect(<<"e2e_nosub_1">>),
    wait_registered(<<"e2e_nosub_1">>),
    {ok, 200, _, Resp} = api_call(#{
        <<"Action">> => <<"BatchPub">>,
        <<"ProductKey">> => <<"default">>,
        <<"DeviceName">> => [<<"e2e_nosub_1">>],
        <<"MessageContent">> => b64(?PAYLOAD),
        <<"Qos">> => 0
    }),
    ?assert(maps:get(<<"Success">>, Resp)),
    Msgs = recv(1),
    ?assertEqual(0, length(Msgs)),
    disconnect(C1).

-doc "QoS=1 BatchPub stores for an unsubscribed device and replays after it subscribes.".
t_batch_pub_qos1_store_pending_no_sub(_Config) ->
    C1 = connect(<<"e2e_pend_1">>),
    wait_registered(<<"e2e_pend_1">>),
    {ok, 200, _, _} = api_call(#{
        <<"Action">> => <<"BatchPub">>,
        <<"ProductKey">> => <<"default">>,
        <<"DeviceName">> => [<<"e2e_pend_1">>],
        <<"MessageContent">> => b64(?PAYLOAD),
        <<"Qos">> => 1
    }),
    Msgs1 = recv(1),
    ?assertEqual(0, length(Msgs1)),
    sub_default(C1, <<"e2e_pend_1">>),
    wait_subscribed(<<"e2e_pend_1">>, topic(<<"e2e_pend_1">>)),
    Msgs2 = recv(1),
    ?assertEqual(1, length(Msgs2)),
    disconnect(C1).

-doc "QoS=1 delivery is not replayed to a device subscribed to a different topic.".
t_batch_pub_wrong_topic_no_replay(_Config) ->
    C1 = connect(<<"e2e_wrong_1">>),
    wait_registered(<<"e2e_wrong_1">>),
    {ok, 200, _, _} = api_call(#{
        <<"Action">> => <<"BatchPub">>,
        <<"ProductKey">> => <<"default">>,
        <<"DeviceName">> => [<<"e2e_wrong_1">>],
        <<"MessageContent">> => b64(?PAYLOAD),
        <<"Qos">> => 1
    }),
    Msgs1 = recv(1),
    ?assertEqual(0, length(Msgs1)),
    sub(C1, <<"/other/topic">>),
    wait_subscribed(<<"e2e_wrong_1">>, <<"/other/topic">>),
    Msgs2 = recv(1),
    ?assertEqual(0, length(Msgs2)),
    disconnect(C1).

-doc "A pending QoS=1 delivery replays only after the device subscribes again following a reconnect.".
t_replay_on_subscribe_after_reconnect(_Config) ->
    C1 = connect(<<"e2e_rply_a">>),
    sub_default(C1, <<"e2e_rply_a">>),
    wait_subscribed(<<"e2e_rply_a">>, topic(<<"e2e_rply_a">>)),
    {ok, 200, _, _} = api_call(#{
        <<"Action">> => <<"BatchPub">>,
        <<"ProductKey">> => <<"default">>,
        <<"DeviceName">> => [<<"e2e_rply_a">>, <<"e2e_rply_b">>],
        <<"MessageContent">> => b64(?PAYLOAD),
        <<"Qos">> => 1
    }),
    Msgs1 = recv(1),
    ?assertEqual(1, length(Msgs1)),
    disconnect(C1),
    C2 = connect(<<"e2e_rply_b">>),
    wait_registered(<<"e2e_rply_b">>),
    Msgs2 = recv(1),
    ?assertEqual(0, length(Msgs2)),
    sub_default(C2, <<"e2e_rply_b">>),
    wait_subscribed(<<"e2e_rply_b">>, topic(<<"e2e_rply_b">>)),
    Msgs3 = recv(1),
    ?assertEqual(1, length(Msgs3)),
    disconnect(C2).

-doc "PubBroadcast is skipped for online devices without a broadcast subscription.".
t_pub_broadcast_skip_no_sub(_Config) ->
    C1 = connect(<<"e2e_bc_ns_1">>),
    wait_registered(<<"e2e_bc_ns_1">>),
    {ok, 200, _, _} = api_call(#{
        <<"Action">> => <<"PubBroadcast">>,
        <<"ProductKey">> => <<"default">>,
        <<"MessageContent">> => b64(?PAYLOAD)
    }),
    Msgs = recv(1),
    ?assertEqual(0, length(Msgs)),
    disconnect(C1).

-doc "PubBroadcast matches wildcard subscriptions on the broadcast topic.".
t_pub_broadcast_wildcard_sub(_Config) ->
    C1 = connect(<<"e2e_bc_wc_1">>),
    sub(C1, <<"/sys/broadcast/#">>),
    wait_subscribed(<<"e2e_bc_wc_1">>, <<"/sys/broadcast/#">>),
    {ok, 200, _, _} = api_call(#{
        <<"Action">> => <<"PubBroadcast">>,
        <<"ProductKey">> => <<"default">>,
        <<"MessageContent">> => b64(?PAYLOAD)
    }),
    Msgs = recv(1),
    ?assertEqual(1, length(Msgs)),
    disconnect(C1).

-doc "A connected but unsubscribed device receives no BatchPub delivery.".
t_connect_only_no_sub_no_delivery(_Config) ->
    C1 = connect(<<"e2e_cnore_1">>),
    wait_registered(<<"e2e_cnore_1">>),
    {ok, 200, _, _} = api_call(#{
        <<"Action">> => <<"BatchPub">>,
        <<"ProductKey">> => <<"default">>,
        <<"DeviceName">> => [<<"e2e_cnore_1">>],
        <<"MessageContent">> => b64(?PAYLOAD),
        <<"Qos">> => 0
    }),
    Msgs = recv(1),
    ?assertEqual(0, length(Msgs)),
    disconnect(C1).

-doc "QoS=1 delivery created while offline replays on reconnect only after subscribing.".
t_reconnect_subscribe_replay(_Config) ->
    C1 = connect(<<"e2e_rcsr_1">>),
    sub_default(C1, <<"e2e_rcsr_1">>),
    wait_subscribed(<<"e2e_rcsr_1">>, topic(<<"e2e_rcsr_1">>)),
    disconnect(C1),
    {ok, 200, _, _} = api_call(#{
        <<"Action">> => <<"BatchPub">>,
        <<"ProductKey">> => <<"default">>,
        <<"DeviceName">> => [<<"e2e_rcsr_1">>],
        <<"MessageContent">> => b64(?PAYLOAD),
        <<"Qos">> => 1
    }),
    C2 = connect(<<"e2e_rcsr_1">>),
    wait_registered(<<"e2e_rcsr_1">>),
    Msgs1 = recv(1),
    ?assertEqual(0, length(Msgs1)),
    sub_default(C2, <<"e2e_rcsr_1">>),
    wait_subscribed(<<"e2e_rcsr_1">>, topic(<<"e2e_rcsr_1">>)),
    Msgs2 = recv(1),
    ?assertEqual(1, length(Msgs2)),
    disconnect(C2).

-doc "Unsubscribing stops further deliveries to the device.".
t_unsubscribe_no_delivery(_Config) ->
    C1 = connect(<<"e2e_unsub_1">>),
    sub_default(C1, <<"e2e_unsub_1">>),
    wait_subscribed(<<"e2e_unsub_1">>, topic(<<"e2e_unsub_1">>)),
    unsub(C1, topic(<<"e2e_unsub_1">>)),
    ?assert(
        wait_until(
            fun() ->
                emqx_bcast_subscription:match(<<"e2e_unsub_1">>, topic(<<"e2e_unsub_1">>)) =:= false
            end,
            100
        )
    ),
    {ok, 200, _, _} = api_call(#{
        <<"Action">> => <<"BatchPub">>,
        <<"ProductKey">> => <<"default">>,
        <<"DeviceName">> => [<<"e2e_unsub_1">>],
        <<"MessageContent">> => b64(?PAYLOAD),
        <<"Qos">> => 0
    }),
    Msgs = recv(1),
    ?assertEqual(0, length(Msgs)),
    disconnect(C1).

-doc "Pending QoS=1 delivery replays after unsubscribe + resubscribe.".
t_unsubscribe_then_resubscribe_replay(_Config) ->
    C1 = connect(<<"e2e_usr_1">>),
    sub_default(C1, <<"e2e_usr_1">>),
    wait_subscribed(<<"e2e_usr_1">>, topic(<<"e2e_usr_1">>)),
    unsub(C1, topic(<<"e2e_usr_1">>)),
    ?assert(
        wait_until(
            fun() ->
                emqx_bcast_subscription:match(<<"e2e_usr_1">>, topic(<<"e2e_usr_1">>)) =:= false
            end,
            100
        )
    ),
    {ok, 200, _, _} = api_call(#{
        <<"Action">> => <<"BatchPub">>,
        <<"ProductKey">> => <<"default">>,
        <<"DeviceName">> => [<<"e2e_usr_1">>],
        <<"MessageContent">> => b64(?PAYLOAD),
        <<"Qos">> => 1
    }),
    Msgs1 = recv(1),
    ?assertEqual(0, length(Msgs1)),
    sub_default(C1, <<"e2e_usr_1">>),
    ?assert(
        wait_until(
            fun() ->
                emqx_bcast_subscription:match(<<"e2e_usr_1">>, topic(<<"e2e_usr_1">>)) =/= false
            end,
            100
        )
    ),
    Msgs2 = recv(1),
    ?assertEqual(1, length(Msgs2)),
    disconnect(C1).

%%--------------------------------------------------------------------
%% Force upgrade QoS E2E tests
%%--------------------------------------------------------------------

-doc "force_upgrade_qos=false delivers a QoS=0 downgraded message to a QoS=0 subscriber.".
t_qos_downgrade_force_false(_Config) ->
    Cfg = persistent_term:get({emqx_bcast, config}),
    persistent_term:put({emqx_bcast, config}, Cfg#{force_upgrade_qos => false}),
    C1 = connect(<<"e2e_fuq_1">>),
    sub_qos(C1, topic(<<"e2e_fuq_1">>), 0),
    wait_subscribed(<<"e2e_fuq_1">>, topic(<<"e2e_fuq_1">>)),
    {ok, 200, _, _} = api_call(#{
        <<"Action">> => <<"BatchPub">>,
        <<"ProductKey">> => <<"default">>,
        <<"DeviceName">> => [<<"e2e_fuq_1">>],
        <<"MessageContent">> => b64(?PAYLOAD),
        <<"Qos">> => 1
    }),
    [Msg] = recv(1),
    ?assertEqual(0, maps:get(qos, Msg)),
    disconnect(C1),
    persistent_term:put({emqx_bcast, config}, Cfg).

-doc "force_upgrade_qos=false keeps QoS=1 for a QoS=1 subscriber.".
t_qos_no_downgrade_force_false(_Config) ->
    Cfg = persistent_term:get({emqx_bcast, config}),
    persistent_term:put({emqx_bcast, config}, Cfg#{force_upgrade_qos => false}),
    C1 = connect(<<"e2e_fuq_2">>),
    sub_qos(C1, topic(<<"e2e_fuq_2">>), 1),
    wait_subscribed(<<"e2e_fuq_2">>, topic(<<"e2e_fuq_2">>)),
    {ok, 200, _, _} = api_call(#{
        <<"Action">> => <<"BatchPub">>,
        <<"ProductKey">> => <<"default">>,
        <<"DeviceName">> => [<<"e2e_fuq_2">>],
        <<"MessageContent">> => b64(?PAYLOAD),
        <<"Qos">> => 1
    }),
    [Msg] = recv(1),
    ?assertEqual(1, maps:get(qos, Msg)),
    disconnect(C1),
    persistent_term:put({emqx_bcast, config}, Cfg).

-doc "force_upgrade_qos=true delivers at QoS=1 to a QoS=0 subscriber.".
t_qos_force_upgrade_true(_Config) ->
    C1 = connect(<<"e2e_fuq_3">>),
    sub_qos(C1, topic(<<"e2e_fuq_3">>), 0),
    wait_subscribed(<<"e2e_fuq_3">>, topic(<<"e2e_fuq_3">>)),
    {ok, 200, _, _} = api_call(#{
        <<"Action">> => <<"BatchPub">>,
        <<"ProductKey">> => <<"default">>,
        <<"DeviceName">> => [<<"e2e_fuq_3">>],
        <<"MessageContent">> => b64(?PAYLOAD),
        <<"Qos">> => 1
    }),
    [Msg] = recv(1),
    ?assertEqual(?PAYLOAD, maps:get(payload, Msg)),
    disconnect(C1).

%%--------------------------------------------------------------------
%% Review regression tests
%%--------------------------------------------------------------------

%% 32 identical inline QoS=1 BatchPub calls run concurrently per round must
%% all resolve to a single MessageId: the hash lookup-or-create must be
%% atomic, otherwise concurrent callers blind-write different records.
-doc "32 concurrent inline QoS=1 BatchPub calls with identical content resolve to one MessageId.".
t_batch_pub_concurrent_inline_dedup(_Config) ->
    Parent = self(),
    RoundIds =
        lists:map(
            fun(Round) ->
                Payload = base64:encode(crypto:strong_rand_bytes(16)),
                Body = #{
                    <<"Action">> => <<"BatchPub">>,
                    <<"ProductKey">> => <<"default">>,
                    <<"DeviceName">> => [<<"e2e_dedup_", (integer_to_binary(Round))/binary>>],
                    <<"MessageContent">> => Payload,
                    <<"Qos">> => 1
                },
                Pids = [
                    spawn(fun() ->
                        Res = api_call(Body),
                        Parent ! {dedup_result, self(), Res}
                    end)
                 || _ <- lists:seq(1, 32)
                ],
                Results = [
                    receive
                        {dedup_result, P, R} -> R
                    end
                 || P <- Pids
                ],
                ?assertEqual(32, length(Results)),
                SuccessIds = [maps:get(<<"MessageId">>, Resp) || {ok, 200, _, Resp} <- Results],
                ?assertEqual(32, length(SuccessIds)),
                lists:usort(SuccessIds)
            end,
            lists:seq(1, 20)
        ),
    lists:foreach(fun(Ids) -> ?assertEqual(1, length(Ids)) end, RoundIds).

%% With one subscribed client, pool size 1 and queue limit 1, 128 concurrent
%% QoS=1 calls must not lose submissions: every 200 response has its message
%% delivered. Admission (capacity check + reservation) must be atomic.
-doc "Concurrent QoS=1 BatchPub calls under admission pressure all get delivered.".
t_batch_pub_concurrent_qos1_e2e(_Config) ->
    N = 20,
    C1 = connect(<<"e2e_adm_1">>),
    sub_default(C1, <<"e2e_adm_1">>),
    wait_subscribed(<<"e2e_adm_1">>, topic(<<"e2e_adm_1">>)),
    Parent = self(),
    Pids = [
        spawn(fun() ->
            Body = #{
                <<"Action">> => <<"BatchPub">>,
                <<"ProductKey">> => <<"default">>,
                <<"DeviceName">> => [<<"e2e_adm_1">>],
                <<"MessageContent">> => base64:encode(crypto:strong_rand_bytes(8)),
                <<"Qos">> => 1
            },
            Res = api_call(Body),
            Parent ! {adm_result, self(), Res}
        end)
     || _ <- lists:seq(1, N)
    ],
    Results = [
        receive
            {adm_result, P, R} -> R
        end
     || P <- Pids
    ],
    ?assertEqual(N, length(Results)),
    lists:foreach(fun(R) -> ?assertMatch({ok, 200, _, _}, R) end, Results),
    %% One want_next batch is sent per ack cycle; buffer3 dedup serializes the
    %% deliveries for the same client. Collect them all with a generous budget.
    Msgs = collect_deliveries(N, [], 100),
    ?assertEqual(N, length(Msgs)),
    disconnect(C1).

collect_deliveries(Expected, Acc, 0) ->
    lists:sublist(Acc, Expected);
collect_deliveries(Expected, Acc, Attempts) ->
    New = recv(Expected - length(Acc)),
    Total = Acc ++ New,
    case length(Total) >= Expected of
        true ->
            lists:sublist(Total, Expected);
        false ->
            ct:sleep(100),
            collect_deliveries(Expected, Total, Attempts - 1)
    end.

wait_until(Fun, Attempts) when Attempts > 0 ->
    case Fun() of
        true ->
            true;
        _ ->
            ct:sleep(100),
            wait_until(Fun, Attempts - 1)
    end;
wait_until(_Fun, 0) ->
    false.
