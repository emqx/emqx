%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mq_cluster_SUITE).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("emqx/include/asserts.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include("../src/emqx_mq_internal.hrl").

-compile(export_all).
-compile(nowarn_export_all).

-define(PUB_PORT, 20000).
-define(SUB0_PORT, 20100).
-define(SUB1_PORT, 20200).

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(TCName, Config) ->
    StorageConfig =
        case TCName of
            t_cluster_reject_consumption ->
                #{<<"durable_storage">> => #{<<"mq_messages">> => #{<<"n_shards">> => 2}}};
            _ ->
                #{}
        end,
    Apps = [
        emqx_durable_storage,
        {emqx, emqx_mq_test_utils:cth_config(emqx, StorageConfig)},
        {emqx_mq,
            emqx_mq_test_utils:cth_config(emqx_mq, #{
                <<"mq">> => #{<<"find_queue_retry_interval">> => <<"100ms">>}
            })}
    ],
    ClusterSpec = [
        {pub, #{apps => Apps, base_port => ?PUB_PORT}},
        {sub0, #{apps => Apps, base_port => ?SUB0_PORT}},
        {sub1, #{apps => Apps, base_port => ?SUB1_PORT}}
    ],
    Nodes = emqx_cth_cluster:start(
        ClusterSpec,
        #{work_dir => emqx_cth_suite:work_dir(TCName, Config)}
    ),
    ok = snabbkaffe:start_trace(),
    %% Wait for the cluster to stabilize. TODO: this should be avoided
    %% by making `emqx_ds:wait_db' taking more readiness conditions
    %% into account, and by better error propagation, in particular
    %% between DS and MQTT publisher client.
    ct:sleep(5_000),
    [{nodes, Nodes} | Config].

end_per_testcase(_TCName, Config) ->
    Nodes = ?config(nodes, Config),
    ok = snabbkaffe:stop(),
    ok = emqx_cth_cluster:stop(Nodes).

%% Test that inter-cluster message dispatching works
t_cluster_smoke(Config) ->
    [PubNode, Sub0Node, Sub1Node] = _Nodes = ?config(nodes, Config),

    %% Create Message Queue and make sure it is available on all nodes
    _MQ = erpc:call(PubNode, emqx_mq_test_utils, ensure_mq_created, [
        #{
            name => <<"cluster_smoke">>,
            topic_filter => <<"q/1">>,
            is_lastvalue => false,
            dispatch_strategy => round_robin
        }
    ]),
    ?retry(
        5,
        100,
        begin
            ?assertMatch(
                {ok, #{name := <<"cluster_smoke">>}},
                erpc:call(Sub0Node, emqx_mq_registry, find, [<<"cluster_smoke">>])
            ),
            ?assertMatch(
                {ok, #{name := <<"cluster_smoke">>}},
                erpc:call(Sub1Node, emqx_mq_registry, find, [<<"cluster_smoke">>])
            )
        end
    ),

    %% Create subscribers and subscribe to the queue
    SubClient0 = emqx_mq_test_utils:emqtt_connect([{port, ?SUB0_PORT}]),
    SubClient1 = emqx_mq_test_utils:emqtt_connect([{port, ?SUB1_PORT}]),
    ok = emqx_mq_test_utils:emqtt_sub_mq(SubClient0, <<"cluster_smoke">>),
    ok = emqx_mq_test_utils:emqtt_sub_mq(SubClient1, <<"cluster_smoke">>),

    %% Publish 100 messages to the queue
    PubClient = emqx_mq_test_utils:emqtt_connect([{port, ?PUB_PORT}]),
    ok = lists:foreach(
        fun(I) ->
            IBin = integer_to_binary(I),
            emqx_mq_test_utils:emqtt_pub_mq(PubClient, <<"q/1">>, <<"msg-", IBin/binary>>)
        end,
        lists:seq(0, 99)
    ),

    %% Make sure the messages are distributed evenly (because of round_robin dispatching) between the subscribers
    {ok, Msgs} = emqx_mq_test_utils:emqtt_drain(_MinMsg = 100, _Timeout = 2000),
    MessagesByClientId = lists:foldl(
        fun(#{client_pid := Pid} = Msg, Acc) ->
            maps:update_with(Pid, fun(ClientMsgs) -> [Msg | ClientMsgs] end, [Msg], Acc)
        end,
        #{},
        Msgs
    ),
    ?assertEqual(50, length(maps:get(SubClient0, MessagesByClientId))),
    ?assertEqual(50, length(maps:get(SubClient1, MessagesByClientId))),

    %% Clean up
    ok = emqtt:disconnect(PubClient),
    ok = emqtt:disconnect(SubClient0),
    ok = emqtt:disconnect(SubClient1).

%% Verify that the subscribers ping the consumer
%% and try to reconnect if the consumer is not responding
t_ping_consumer(Config) ->
    [PubNode, Sub0Node, Sub1Node] = Nodes = ?config(nodes, Config),

    %% Create a non-lastvalue Queue
    erpc:call(PubNode, emqx_mq_test_utils, ensure_mq_created, [
        #{
            name => <<"ping_consumer">>,
            topic_filter => <<"t/#">>,
            ping_interval => 500
        }
    ]),
    ?retry(
        5,
        100,
        begin
            ?assertMatch(
                {ok, #{topic_filter := <<"t/#">>}},
                erpc:call(Sub0Node, emqx_mq_registry, find, [<<"ping_consumer">>])
            ),
            ?assertMatch(
                {ok, #{topic_filter := <<"t/#">>}},
                erpc:call(Sub1Node, emqx_mq_registry, find, [<<"ping_consumer">>])
            )
        end
    ),

    %% Connect clients and wait for the consumer to be started
    PubClient = emqx_mq_test_utils:emqtt_connect([{port, ?PUB_PORT}]),
    CSub0 = emqx_mq_test_utils:emqtt_connect([{port, ?SUB0_PORT}]),
    CSub1 = emqx_mq_test_utils:emqtt_connect([{port, ?SUB1_PORT}]),
    emqx_mq_test_utils:emqtt_sub_mq(CSub0, <<"ping_consumer">>),
    emqx_mq_test_utils:emqtt_sub_mq(CSub1, <<"ping_consumer">>),
    ?retry(
        10,
        100,
        ?assertMatch([_], all_consumers(Nodes))
    ),

    %% Verify that the consumer and the subscriber ping each other
    ?assertWaitEvent(
        ok,
        #{?snk_kind := mq_consumer_handle_ping},
        1000
    ),
    ?assertWaitEvent(
        ok,
        #{?snk_kind := mq_sub_ping},
        1000
    ),

    %% Kill the consumer
    ok = stop_all_consumers(Nodes),

    %% Publish a message
    emqx_mq_test_utils:emqtt_pub_mq(PubClient, <<"t/1">>, <<"test message">>),

    %% Verify that the consumer is restarted
    %% and we receive the message
    {ok, Msgs} = emqx_mq_test_utils:emqtt_drain(_MinMsg = 1, _Timeout = 5000),
    ?assertEqual(1, length(Msgs)),

    %% Clean up
    ok = emqtt:disconnect(PubClient),
    ok = emqtt:disconnect(CSub0),
    ok = emqtt:disconnect(CSub1).

%% Check per-shard rejection and quota release by a remote consumer across both shards.
t_cluster_reject_consumption(Config) ->
    [PubNode, SubNode, _] = ?config(nodes, Config),
    MQ = erpc:call(PubNode, emqx_mq_test_utils, ensure_mq_created, [
        #{
            name => <<"cluster_reject">>,
            topic_filter => <<"cluster/reject">>,
            limits => #{
                on_full => reject,
                max_shard_message_count => 1,
                max_shard_message_bytes => infinity
            }
        }
    ]),
    Publisher0 = <<"full-publisher">>,
    Pub = emqx_mq_test_utils:emqtt_connect([{port, ?PUB_PORT}, {clientid, Publisher0}]),
    ?assertMatch(
        {ok, #{reason_code := 0}}, emqtt:publish(Pub, <<"$queue/cluster_reject">>, <<"first">>, 1)
    ),
    ok = erpc:call(PubNode, emqx_mq_quota_buffer, flush, [mq_message_quota_buffer]),
    ?assertMatch(
        {ok, #{reason_code := 16#97}},
        emqtt:publish(Pub, <<"$queue/cluster_reject">>, <<"full">>, 1)
    ),
    Shard0 = erpc:call(PubNode, emqx_ds, shard_of, [?MQ_MESSAGE_LASTVALUE_DB, Publisher0]),
    [Publisher1 | _] = [
        ClientId
     || N <- lists:seq(1, 100),
        ClientId <- [integer_to_binary(N)],
        erpc:call(PubNode, emqx_ds, shard_of, [?MQ_MESSAGE_LASTVALUE_DB, ClientId]) =/= Shard0
    ],
    Pub1 = emqx_mq_test_utils:emqtt_connect([{port, ?PUB_PORT}, {clientid, Publisher1}]),
    ?assertMatch(
        {ok, #{reason_code := 0}},
        emqtt:publish(Pub1, <<"$queue/cluster_reject">>, <<"other-shard">>, 1)
    ),
    ok = erpc:call(PubNode, emqx_mq_quota_buffer, flush, [?MQ_QUOTA_BUFFER]),
    ?retry(
        50,
        100,
        ?assertMatch({ok, _}, erpc:call(SubNode, emqx_mq_registry, find, [<<"cluster_reject">>]))
    ),
    Sub = emqx_mq_test_utils:emqtt_connect([{port, ?SUB0_PORT}]),
    ok = emqx_mq_test_utils:emqtt_sub_mq(Sub, <<"cluster_reject">>),
    {ok, Received} = emqx_mq_test_utils:emqtt_drain(2, 1000),
    ?assertEqual([<<"first">>, <<"other-shard">>], lists:sort([P || #{payload := P} <- Received])),
    Shards = erpc:call(PubNode, emqx_ds, list_shards, [?MQ_MESSAGE_LASTVALUE_DB]),
    ?retry(
        50,
        100,
        lists:foreach(
            fun(Shard) ->
                ?assertMatch(
                    {ok, #{count := 0}},
                    erpc:call(PubNode, emqx_mq_message_db, occupancy, [MQ, Shard])
                )
            end,
            Shards
        )
    ),
    ?assertMatch(
        {ok, #{reason_code := 0}}, emqtt:publish(Pub, <<"$queue/cluster_reject">>, <<"second">>, 1)
    ),
    ok = emqtt:disconnect(Pub),
    ok = emqtt:disconnect(Pub1),
    ok = emqtt:disconnect(Sub).

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

all_consumers(Nodes) ->
    lists:flatmap(
        fun(Node) ->
            erpc:call(Node, emqx_mq_test_utils, all_consumers, [])
        end,
        Nodes
    ).

stop_all_consumers(Nodes) ->
    lists:foreach(
        fun(Node) ->
            erpc:call(Node, emqx_mq_test_utils, stop_all_consumers, [])
        end,
        Nodes
    ).
