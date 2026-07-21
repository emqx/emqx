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
    emqtt:disconnect(C).

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

%% tests

t_pubsub_works(_Config) ->
    C = connect(<<"test_sub">>),
    sub(C, <<"t1">>),
    ct:sleep(10),
    ok = emqtt:publish(C, <<"t1">>, <<"hi">>, 0),
    Msgs = recv(1),
    ?assertEqual(1, length(Msgs)),
    ?assertMatch(#{payload := <<"hi">>}, hd(Msgs)),
    disconnect(C).

t_batch_pub_qos0_e2e(_Config) ->
    C1 = connect(<<"e2e_q0_1">>),
    C2 = connect(<<"e2e_q0_2">>),
    sub_default(C1, <<"e2e_q0_1">>),
    sub_default(C2, <<"e2e_q0_2">>),
    ct:sleep(10),
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

t_batch_pub_qos1_e2e(_Config) ->
    C1 = connect(<<"e2e_q1_1">>),
    C2 = connect(<<"e2e_q1_2">>),
    sub_default(C1, <<"e2e_q1_1">>),
    sub_default(C2, <<"e2e_q1_2">>),
    ct:sleep(10),
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

t_batch_pub_messageid_reuse_e2e(_Config) ->
    B64 = b64(<<"reuse_payload">>),
    {ok, 200, _, RegResp} = api_call(#{
        <<"Action">> => <<"RegisterMessage">>, <<"MessageContent">> => B64
    }),
    MsgId = maps:get(<<"MessageId">>, RegResp),
    C1 = connect(<<"e2e_reuse_1">>),
    sub_default(C1, <<"e2e_reuse_1">>),
    ct:sleep(10),
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

t_pub_broadcast_e2e(_Config) ->
    C1 = connect(<<"e2e_bc_1">>),
    C2 = connect(<<"e2e_bc_2">>),
    C3 = connect(<<"e2e_bc_3">>),
    sub(C1, <<"/sys/broadcast/default">>),
    sub(C2, <<"/sys/broadcast/default">>),
    sub(C3, <<"/sys/broadcast/default">>),
    ct:sleep(10),
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

t_batch_pub_partial_online_e2e(_Config) ->
    C1 = connect(<<"e2e_part_1">>),
    sub_default(C1, <<"e2e_part_1">>),
    ct:sleep(10),
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
    ct:sleep(10),
    Msgs2 = recv(1),
    ?assertEqual(1, length(Msgs2)),
    disconnect(C1),
    disconnect(C2).

t_batch_pub_topic_template_e2e(_Config) ->
    CustomTopic = <<"/custom/${deviceName}/topic">>,
    C1 = connect(<<"e2e_tpl_1">>),
    sub(C1, <<"/custom/e2e_tpl_1/topic">>),
    ct:sleep(10),
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

t_batch_pub_qos0_no_sub(_Config) ->
    C1 = connect(<<"e2e_nosub_1">>),
    ct:sleep(10),
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

t_batch_pub_qos1_store_pending_no_sub(_Config) ->
    C1 = connect(<<"e2e_pend_1">>),
    ct:sleep(10),
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
    ct:sleep(10),
    Msgs2 = recv(1),
    ?assertEqual(1, length(Msgs2)),
    disconnect(C1).

t_batch_pub_wrong_topic_no_replay(_Config) ->
    C1 = connect(<<"e2e_wrong_1">>),
    ct:sleep(10),
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
    ct:sleep(10),
    Msgs2 = recv(1),
    ?assertEqual(0, length(Msgs2)),
    disconnect(C1).

t_replay_on_subscribe_after_reconnect(_Config) ->
    C1 = connect(<<"e2e_rply_a">>),
    sub_default(C1, <<"e2e_rply_a">>),
    ct:sleep(10),
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
    ct:sleep(10),
    Msgs2 = recv(1),
    ?assertEqual(0, length(Msgs2)),
    sub_default(C2, <<"e2e_rply_b">>),
    ct:sleep(10),
    Msgs3 = recv(1),
    ?assertEqual(1, length(Msgs3)),
    disconnect(C2).

t_pub_broadcast_skip_no_sub(_Config) ->
    C1 = connect(<<"e2e_bc_ns_1">>),
    ct:sleep(10),
    {ok, 200, _, _} = api_call(#{
        <<"Action">> => <<"PubBroadcast">>,
        <<"ProductKey">> => <<"default">>,
        <<"MessageContent">> => b64(?PAYLOAD)
    }),
    Msgs = recv(1),
    ?assertEqual(0, length(Msgs)),
    disconnect(C1).

t_pub_broadcast_wildcard_sub(_Config) ->
    C1 = connect(<<"e2e_bc_wc_1">>),
    sub(C1, <<"/sys/broadcast/#">>),
    ct:sleep(10),
    {ok, 200, _, _} = api_call(#{
        <<"Action">> => <<"PubBroadcast">>,
        <<"ProductKey">> => <<"default">>,
        <<"MessageContent">> => b64(?PAYLOAD)
    }),
    Msgs = recv(1),
    ?assertEqual(1, length(Msgs)),
    disconnect(C1).

t_connect_only_no_sub_no_delivery(_Config) ->
    C1 = connect(<<"e2e_cnore_1">>),
    ct:sleep(10),
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

t_reconnect_subscribe_replay(_Config) ->
    C1 = connect(<<"e2e_rcsr_1">>),
    sub_default(C1, <<"e2e_rcsr_1">>),
    ct:sleep(10),
    disconnect(C1),
    {ok, 200, _, _} = api_call(#{
        <<"Action">> => <<"BatchPub">>,
        <<"ProductKey">> => <<"default">>,
        <<"DeviceName">> => [<<"e2e_rcsr_1">>],
        <<"MessageContent">> => b64(?PAYLOAD),
        <<"Qos">> => 1
    }),
    C2 = connect(<<"e2e_rcsr_1">>),
    ct:sleep(10),
    Msgs1 = recv(1),
    ?assertEqual(0, length(Msgs1)),
    sub_default(C2, <<"e2e_rcsr_1">>),
    ct:sleep(10),
    Msgs2 = recv(1),
    ?assertEqual(1, length(Msgs2)),
    disconnect(C2).

t_unsubscribe_no_delivery(_Config) ->
    C1 = connect(<<"e2e_unsub_1">>),
    sub_default(C1, <<"e2e_unsub_1">>),
    ct:sleep(10),
    unsub(C1, topic(<<"e2e_unsub_1">>)),
    ct:sleep(10),
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

t_unsubscribe_then_resubscribe_replay(_Config) ->
    C1 = connect(<<"e2e_usr_1">>),
    sub_default(C1, <<"e2e_usr_1">>),
    ct:sleep(10),
    unsub(C1, topic(<<"e2e_usr_1">>)),
    ct:sleep(10),
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
    ct:sleep(10),
    Msgs2 = recv(1),
    ?assertEqual(1, length(Msgs2)),
    disconnect(C1).

%%--------------------------------------------------------------------
%% Force upgrade QoS E2E tests
%%--------------------------------------------------------------------

t_qos_downgrade_force_false(_Config) ->
    Cfg = persistent_term:get({emqx_bcast, config}),
    persistent_term:put({emqx_bcast, config}, Cfg#{force_upgrade_qos => false}),
    C1 = connect(<<"e2e_fuq_1">>),
    sub_qos(C1, topic(<<"e2e_fuq_1">>), 0),
    ct:sleep(10),
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

t_qos_no_downgrade_force_false(_Config) ->
    Cfg = persistent_term:get({emqx_bcast, config}),
    persistent_term:put({emqx_bcast, config}, Cfg#{force_upgrade_qos => false}),
    C1 = connect(<<"e2e_fuq_2">>),
    sub_qos(C1, topic(<<"e2e_fuq_2">>), 1),
    ct:sleep(10),
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

t_qos_force_upgrade_true(_Config) ->
    C1 = connect(<<"e2e_fuq_3">>),
    sub_qos(C1, topic(<<"e2e_fuq_3">>), 0),
    ct:sleep(10),
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
