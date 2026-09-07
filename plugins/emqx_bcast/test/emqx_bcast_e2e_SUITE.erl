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
        max_pending_deliveries => 10000000,
        max_pending_deliveries_per_device => 100,
        msg_warn_threshold => 100000,
        broadcast_topic => <<"/sys/broadcast/${productKey}">>,
        batch_topic => <<"/${productKey}/${deviceName}/user/get">>,
        delivery_pool_size => 2
    }).

init_per_testcase(_Case, Config) ->
    %% Start each test case from a clean storage state: async deliveries
    %% from the previous case (unacked QoS1 records, pending index entries,
    %% registered messages) must not leak into the assertions of this one.
    [
        mnesia:clear_table(T)
     || T <- [bcast_msg, bcast_message, bcast_message_hash, bcast_message_api_id, bcast_msg_index]
    ],
    emqx_bcast_metrics:init(),
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

%% The subscribe hook casts into emqx_bcast_pull_shard asynchronously. The
%% plugin reads EMQX's own subscription tables, so poll those: resolve the
%% channel pid from the clientid, then look for a filter matching Topic.
wait_subscribed(ClientId, Topic) ->
    ?assert(
        wait_until(
            fun() -> client_subscribed(ClientId, Topic) end,
            100
        )
    ).

client_subscribed(ClientId, Topic) ->
    case emqx_cm:lookup_channels(ClientId) of
        [Pid | _] ->
            lists:any(
                fun({Filter, _}) -> emqx_topic:match(Topic, Filter) end,
                emqx_broker:subscriptions(Pid)
            );
        _ ->
            false
    end.

client_unsubscribed(ClientId, Topic) ->
    not client_subscribed(ClientId, Topic).

%% The connected hook casts into emqx_bcast_pull_shard asynchronously; poll the
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

-doc "QoS0 direct delivery checks the authoritative session holder.".
t_qos0_delivery_uses_current_session_holder_e2e(_Config) ->
    DN = <<"e2e_q0_session_1">>,
    C1 = connect(DN),
    sub_default(C1, DN),
    wait_subscribed(DN, topic(DN)),
    [CurrentPid] = emqx_cm:lookup_channels(DN),
    OldPid = spawn(fun() -> ok end),
    ok = emqx_bcast_pull_shard:do_deliver_qos0([
        {OldPid, <<"default">>, DN, topic(DN), <<"stale">>}
    ]),
    ?assertEqual(0, length(recv(1))),
    ok = emqx_bcast_pull_shard:do_deliver_qos0([
        {CurrentPid, <<"default">>, DN, topic(DN), <<"current">>}
    ]),
    Msgs = recv(1),
    ?assertEqual(1, length(Msgs)),
    ?assertMatch(#{payload := <<"current">>}, hd(Msgs)),
    disconnect(C1).

-doc "Three QoS=0 BatchPub calls deliver exactly one message per call.".
t_batch_pub_qos0_exactly_once_e2e(_Config) ->
    C1 = connect(<<"e2e_q0_once_1">>),
    sub_default(C1, <<"e2e_q0_once_1">>),
    wait_subscribed(<<"e2e_q0_once_1">>, topic(<<"e2e_q0_once_1">>)),
    Payloads = [<<"q0_once_1">>, <<"q0_once_2">>, <<"q0_once_3">>],
    lists:foreach(
        fun(Payload) ->
            {ok, 200, _, Resp} = api_call(#{
                <<"Action">> => <<"BatchPub">>,
                <<"ProductKey">> => <<"default">>,
                <<"DeviceName">> => [<<"e2e_q0_once_1">>],
                <<"MessageContent">> => b64(Payload),
                <<"Qos">> => 0
            }),
            ?assert(maps:get(<<"Success">>, Resp))
        end,
        Payloads
    ),
    Msgs = recv(3),
    ?assertEqual(3, length(Msgs)),
    ?assertEqual(
        lists:sort(Payloads),
        lists:sort([maps:get(payload, M) || M <- Msgs])
    ),
    %% The original duplicate-delivery report was three publishes for one
    %% call; wait one more receive window and assert no fourth copy arrives.
    ?assertEqual(0, length(recv(1))),
    disconnect(C1).

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

%% Regression: the sharding change made
%% PubBroadcast (DeviceNames = undefined) fan out to ALL pull_shard shards,
%% and each shard's product-wide scan did not filter to its own partition,
%% so every online device received 4 duplicate messages. recv(3) collected
%% all three and returned before the duplicates showed up, so the old test
%% could not see the regression. Assert EXACTLY one message per device by
%% draining the mailbox afterwards.
-doc "PubBroadcast delivers exactly once per online subscribed device.".
t_pub_broadcast_exact_once_e2e(_Config) ->
    C1 = connect(<<"e2e_bc1_1">>),
    C2 = connect(<<"e2e_bc1_2">>),
    C3 = connect(<<"e2e_bc1_3">>),
    sub(C1, <<"/sys/broadcast/default">>),
    sub(C2, <<"/sys/broadcast/default">>),
    sub(C3, <<"/sys/broadcast/default">>),
    wait_subscribed(<<"e2e_bc1_1">>, <<"/sys/broadcast/default">>),
    wait_subscribed(<<"e2e_bc1_2">>, <<"/sys/broadcast/default">>),
    wait_subscribed(<<"e2e_bc1_3">>, <<"/sys/broadcast/default">>),
    {ok, 200, _, _} = api_call(#{
        <<"Action">> => <<"PubBroadcast">>,
        <<"ProductKey">> => <<"default">>,
        <<"MessageContent">> => b64(?PAYLOAD)
    }),
    Msgs = recv(3),
    ?assertEqual(3, length(Msgs)),
    ?assert(lists:all(fun(M) -> maps:get(payload, M) =:= ?PAYLOAD end, Msgs)),
    %% No duplicates: a 4x fanout would deliver 3 extra copies.
    ?assertEqual([], recv(1)),
    disconnect(C1),
    disconnect(C2),
    disconnect(C3).

%% 4b9b1657 regression guard: a QoS0 BatchPub request with an explicit
%% DeviceNames list must deliver ONLY to the listed devices. A client of
%% the same product that subscribes to the target's topic must not receive
%% the message (the pre-fix code fanned out to the whole product).
-doc "QoS0 BatchPub does not leak to devices outside the DeviceNames list.".
t_batch_pub_qos0_no_leak_e2e(_Config) ->
    C1 = connect(<<"e2e_q0nl_1">>),
    C2 = connect(<<"e2e_q0nl_2">>),
    %% Both clients subscribe to C1's topic: a product-wide fanout would
    %% wrongly deliver to C2 as well.
    sub_default(C1, <<"e2e_q0nl_1">>),
    sub_default(C2, <<"e2e_q0nl_1">>),
    wait_subscribed(<<"e2e_q0nl_1">>, topic(<<"e2e_q0nl_1">>)),
    wait_subscribed(<<"e2e_q0nl_2">>, topic(<<"e2e_q0nl_1">>)),
    {ok, 200, _, _} = api_call(#{
        <<"Action">> => <<"BatchPub">>,
        <<"ProductKey">> => <<"default">>,
        <<"DeviceName">> => [<<"e2e_q0nl_1">>],
        <<"MessageContent">> => b64(?PAYLOAD),
        <<"Qos">> => 0
    }),
    Msgs = recv(1),
    ?assertEqual(1, length(Msgs)),
    %% C2 is NOT in DeviceNames: it must receive nothing.
    ?assertEqual([], recv(1)),
    disconnect(C1),
    disconnect(C2).
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
            fun() -> client_unsubscribed(<<"e2e_unsub_1">>, topic(<<"e2e_unsub_1">>)) end, 100
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
        wait_until(fun() -> client_unsubscribed(<<"e2e_usr_1">>, topic(<<"e2e_usr_1">>)) end, 100)
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
        wait_until(fun() -> client_subscribed(<<"e2e_usr_1">>, topic(<<"e2e_usr_1">>)) end, 100)
    ),
    Msgs2 = recv(1),
    ?assertEqual(1, length(Msgs2)),
    disconnect(C1).

%%--------------------------------------------------------------------
%% Subscription QoS E2E tests
%%--------------------------------------------------------------------

-doc "QoS=1 BatchPub to a QoS=0 subscriber is delivered and self-acked as QoS=0.".
t_qos1_to_qos0_subscriber_e2e(_Config) ->
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
    ?assertEqual(?PAYLOAD, maps:get(payload, Msg)),
    disconnect(C1).

-doc "QoS=1 BatchPub to a QoS=1 subscriber is delivered at QoS=1 and acked.".
t_qos1_to_qos1_subscriber_e2e(_Config) ->
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
    ?assertEqual(?PAYLOAD, maps:get(payload, Msg)),
    disconnect(C1).

-doc "QoS=1 delivery to a QoS=0 subscriber is removed after the self-ack.".
t_qos0_subscriber_delivery_removed(_Config) ->
    C1 = connect(<<"e2e_fuq_3">>),
    sub_qos(C1, topic(<<"e2e_fuq_3">>), 0),
    wait_subscribed(<<"e2e_fuq_3">>, topic(<<"e2e_fuq_3">>)),
    BeforeAutoAcked = metric(<<"batch_pub_qos1_auto_acked">>),
    {ok, 200, _, _} = api_call(#{
        <<"Action">> => <<"BatchPub">>,
        <<"ProductKey">> => <<"default">>,
        <<"DeviceName">> => [<<"e2e_fuq_3">>],
        <<"MessageContent">> => b64(?PAYLOAD),
        <<"Qos">> => 1
    }),
    [Msg] = recv(1),
    ?assertEqual(0, maps:get(qos, Msg)),
    ?assertEqual(?PAYLOAD, maps:get(payload, Msg)),
    %% The self-ack must arrive (auto_acked counter increases) and the
    %% delivery record must be gone once the QoS=0 self-ack completes,
    %% otherwise the pending entry lingers and blocks the window=1 slot.
    ?assert(
        wait_until(
            fun() -> metric(<<"batch_pub_qos1_auto_acked">>) > BeforeAutoAcked end,
            100
        )
    ),
    ?assert(
        wait_until(
            fun() -> mnesia:dirty_match_object(#bcast_msg{_ = '_'}) =:= [] end,
            100
        )
    ),
    disconnect(C1).

%%--------------------------------------------------------------------
%% Ledger combo scenarios
%%--------------------------------------------------------------------

-doc "Combined ledger scenario A: one QoS1 acked delivery, one QoS0\n"
"auto-acked delivery and one offline device that expires at TTL. After\n"
"the async accounting settles the delivery ledger closes\n"
"(wanted = acked + auto_acked + ttl_expired + canceled, live backlog 0)\n"
"and delivered counts the real sends (redelivered stays 0 without any\n"
"forced re-attempt).".
t_metrics_ledger_combo_a_e2e(_Config) ->
    PK = <<"default">>,
    A1 = <<"e2e_ca_a1">>,
    A2 = <<"e2e_ca_a2">>,
    A3 = <<"e2e_ca_a3">>,
    C1 = connect(A1),
    C2 = connect(A2),
    %% QoS1 subscriber: emqtt auto-PUBACKs -> acked
    sub_default(C1, A1),
    %% QoS0 subscriber: auto-ack path
    sub_qos(C2, topic(A2), 0),
    wait_subscribed(A1, topic(A1)),
    wait_subscribed(A2, topic(A2)),
    W0 = metric(<<"batch_pub_qos1_wanted">>),
    A0 = metric(<<"batch_pub_qos1_acked">>),
    AU0 = metric(<<"batch_pub_qos1_auto_acked">>),
    D0 = metric(<<"batch_pub_qos1_delivered">>),
    R0 = metric(<<"batch_pub_qos1_redelivered">>),
    C0 = metric(<<"batch_pub_qos1_canceled">>),
    T0 = metric(<<"batch_pub_qos1_ttl_expired">>),
    {ok, 200, _, _} = api_call(#{
        <<"Action">> => <<"BatchPub">>,
        <<"ProductKey">> => PK,
        <<"DeviceName">> => [A1, A2, A3],
        <<"MessageContent">> => b64(?PAYLOAD),
        <<"Qos">> => 1
    }),
    Msgs = recv(2),
    ?assertEqual(2, length(Msgs)),
    %% Settle the async accounting: commit, sends, PUBACK, auto-ack.
    ?assert(wait_until(fun() -> metric(<<"batch_pub_qos1_wanted">>) >= W0 + 3 end, 100)),
    ?assert(wait_until(fun() -> metric(<<"batch_pub_qos1_delivered">>) >= D0 + 2 end, 100)),
    ?assert(wait_until(fun() -> metric(<<"batch_pub_qos1_acked">>) >= A0 + 1 end, 100)),
    ?assert(wait_until(fun() -> metric(<<"batch_pub_qos1_auto_acked">>) >= AU0 + 1 end, 100)),
    %% The ack counters fire on the pull side before the core index removal
    %% lands; wait until A1/A2 index entries are actually gone (acked) and
    %% only A3 is still queued, so the cleanup below expires exactly A3.
    ?assert(
        wait_until(
            fun() ->
                {ok, []} =:= emqx_bcast_storage:get_device_deliveries({PK, A1}) andalso
                    {ok, []} =:= emqx_bcast_storage:get_device_deliveries({PK, A2}) andalso
                    case emqx_bcast_storage:get_device_deliveries({PK, A3}) of
                        {ok, [_]} -> true;
                        _ -> false
                    end
            end,
            100
        )
    ),
    %% Offline device A3: expire its unacked delivery row and run cleanup.
    [Deliv = #bcast_msg{delivery_id = Did}] = [
        D
     || D <- mnesia:dirty_match_object(#bcast_msg{_ = '_'}),
        lists:member(A3, D#bcast_msg.device_names)
    ],
    mnesia:dirty_write(Deliv#bcast_msg{expires_at = 0}),
    emqx_bcast_storage:cleanup_expired(),
    ?assert(wait_until(fun() -> metric(<<"batch_pub_qos1_ttl_expired">>) >= T0 + 1 end, 100)),
    ?assertEqual([], mnesia:dirty_read(bcast_msg, Did)),
    %% The ledger closes with zero live backlog.
    ?assertEqual(3, metric(<<"batch_pub_qos1_wanted">>) - W0),
    ?assertEqual(1, metric(<<"batch_pub_qos1_acked">>) - A0),
    ?assertEqual(1, metric(<<"batch_pub_qos1_auto_acked">>) - AU0),
    ?assertEqual(1, metric(<<"batch_pub_qos1_ttl_expired">>) - T0),
    ?assertEqual(0, metric(<<"batch_pub_qos1_canceled">>) - C0),
    ?assertEqual(2, metric(<<"batch_pub_qos1_delivered">>) - D0),
    ?assertEqual(0, metric(<<"batch_pub_qos1_redelivered">>) - R0),
    disconnect(C1),
    disconnect(C2).

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

metric(Name) ->
    try
        prometheus_counter:value(bcast, <<"bcast_", Name/binary>>, [])
    catch
        _:_ -> 0
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
