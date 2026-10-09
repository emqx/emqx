%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
%% Multi-node e2e: BatchPub cross-node delivery and replay index
%% replication across a two-core-node cluster.
-module(emqx_bcast_cluster_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include("emqx_bcast.hrl").

-define(PAYLOAD, <<"cluster_test_payload">>).

all() -> emqx_common_test_helpers:all(?MODULE).

suite() -> [{timetrap, {minutes, 3}}].

init_per_testcase(Name, Config) ->
    Cluster = emqx_cth_cluster:mk_nodespecs(
        [
            {bcast_cl1, #{role => core, apps => apps()}},
            {bcast_cl2, #{role => core, apps => apps()}}
        ],
        #{work_dir => emqx_cth_suite:work_dir(Name, Config)}
    ),
    [{cluster, Cluster} | Config].

end_per_testcase(_, Config) ->
    emqx_cth_cluster:stop(?config(cluster, Config)).

apps() ->
    [
        {emqx, #{config => #{<<"authorization">> => #{<<"no_match">> => <<"allow">>}}}},
        emqx_bcast
    ].

%% helpers

connect(Node, ClientId) ->
    Port = emqx_cth_cluster:get_tcp_mqtt_port(Node),
    {ok, C} = emqtt:start_link([
        {host, "127.0.0.1"}, {port, Port}, {clean_start, true}, {clientid, ClientId}
    ]),
    {ok, _} = emqtt:connect(C),
    C.

sub(C, Topic) ->
    emqtt:subscribe(C, Topic, 1).

recv(Count) -> recv(Count, []).
recv(0, Msgs) ->
    Msgs;
recv(Count, Msgs) ->
    receive
        {publish, Msg} -> recv(Count - 1, [Msg | Msgs])
    after 3000 -> Msgs
    end.

topic(DN) -> <<"/default/", DN/binary, "/user/get">>.

api_call(Node, Body) ->
    erpc:call(Node, emqx_bcast_api, handle, [post, [<<"pub">>], #{body => Body}]).

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

%% Channel pids and subscription tables are node-local: resolve the channel
%% for ClientId on Node and check EMQX's subscription tables there.
node_client_subscribed(Node, ClientId, Topic) ->
    erpc:call(
        Node,
        fun() ->
            case emqx_cm:lookup_channels(ClientId) of
                [Pid | _] ->
                    lists:any(
                        fun({Filter, _}) -> emqx_topic:match(Topic, Filter) end,
                        emqx_broker:subscriptions(Pid)
                    );
                _ ->
                    false
            end
        end
    ).

%% Cold-start settle: the async QoS1 pipeline runs at full speed only
%% after mria converges and the plugin workers are warm. Start timing only
%% after the cluster has settled so delivery latency has a meaningful
%% budget (recv/1) instead of being polluted by cold-start. (The test
%% process runs on the CT node, so this is a plain settle sleep; the
%% emqx_cth_cluster:start/1 call already waits for the nodes to come up.)
settle_cluster() ->
    timer:sleep(3000).

%% tests

-doc "A failed remote delete-epoch bump must not let management delete return success.".
t_failed_remote_epoch_blocks_delete_e2e(Config) ->
    [N1, N2] = emqx_cth_cluster:start(?config(cluster, Config)),
    settle_cluster(),
    Payload = <<"epoch-delete-failure">>,
    Hash = crypto:hash(sha256, Payload),
    {ok, 200, _, R} = api_call(N1, #{
        <<"Action">> => <<"RegisterMessage">>,
        <<"MessageContent">> => base64:encode(Payload)
    }),
    ApiId = maps:get(<<"MessageId">>, R),
    BeforeRemote = erpc:call(N2, emqx_bcast, msg_epoch, [Hash]),
    {Result, Hits} = erpc:call(N1, fun() ->
        HitCounter = atomics:new(1, []),
        ok = meck:new(emqx_rpc, [passthrough, no_link]),
        try
            ok = meck:expect(emqx_rpc, call, fun(M, Node, FMod, F, Args, Timeout) ->
                case
                    Node =:= N2 andalso FMod =:= emqx_bcast andalso
                        F =:= bump_msg_epoch
                of
                    true ->
                        atomics:add(HitCounter, 1, 1),
                        {badrpc, injected_unreachable};
                    false ->
                        meck:passthrough([M, Node, FMod, F, Args, Timeout])
                end
            end),
            Reply = emqx_bcast_api:handle(delete, [<<"messages">>, ApiId], #{}),
            {Reply, atomics:get(HitCounter, 1)}
        after
            meck:unload(emqx_rpc)
        end
    end),
    ?assert(Hits > 0),
    ?assertEqual(BeforeRemote, erpc:call(N2, emqx_bcast, msg_epoch, [Hash])),
    ?assertMatch({error, 500, _, _}, Result),
    ?assertMatch(
        {ok, 200, _, _},
        api_call_message(N1, get, ApiId)
    ),
    ?assertMatch(
        {ok, 200, _, _},
        api_call_message(N2, get, ApiId)
    ),
    ?assertMatch({ok, 200, _, _}, api_call_message(N1, delete, ApiId)),
    ?assertEqual(BeforeRemote + 1, erpc:call(N2, emqx_bcast, msg_epoch, [Hash])).

api_call_message(Node, Method, ApiId) ->
    erpc:call(Node, emqx_bcast_api, handle, [Method, [<<"messages">>, ApiId], #{}]).

%% Devices connected to different nodes must all receive the message when the
%% API request lands on only one of them. Resolve happens on every node, and
%% pool workers hand the message to remote channel pids via Erlang `!`.
-doc "QoS=0 BatchPub reaches devices connected to different cluster nodes.".
t_cluster_cross_node_delivery(Config) ->
    [N1, N2] = emqx_cth_cluster:start(?config(cluster, Config)),
    settle_cluster(),
    DN1 = <<"cl_dn1">>,
    DN2 = <<"cl_dn2">>,
    C1 = connect(N1, DN1),
    C2 = connect(N2, DN2),
    sub(C1, topic(DN1)),
    sub(C2, topic(DN2)),
    %% The subscribe hook casts into pull_shard asynchronously; the plugin
    %% reads EMQX's own subscription tables, so poll those on each node
    %% (channel pid -> subscriptions) until the subscriptions landed.
    Subscribed =
        wait_until(
            fun() ->
                node_client_subscribed(N1, DN1, topic(DN1)) andalso
                    node_client_subscribed(N2, DN2, topic(DN2))
            end,
            50
        ),
    ?assert(Subscribed),
    {ok, 200, _, _Resp} = api_call(N1, #{
        <<"Action">> => <<"BatchPub">>,
        <<"ProductKey">> => <<"default">>,
        <<"DeviceName">> => [DN1, DN2],
        <<"MessageContent">> => base64:encode(?PAYLOAD),
        <<"Qos">> => 0
    }),
    Msgs1 = recv(1),
    Msgs2 = recv(1),
    ?assertEqual(1, length(Msgs1)),
    ?assertEqual(1, length(Msgs2)),
    ?assertMatch(#{payload := ?PAYLOAD}, hd(Msgs1)),
    ?assertMatch(#{payload := ?PAYLOAD}, hd(Msgs2)),
    emqtt:stop(C1),
    emqtt:stop(C2).

%% A QoS=1 delivery created via node1 must be replayable on node2: the
%% delivery record replicates through the mria shard, and the replay index
%% entry propagates to every node's local ETS via rpc cast.
-doc "QoS=1 delivery created on one node is replayed after reconnect on another node.".
t_cluster_offline_replay_on_other_node(Config) ->
    [N1, N2] = emqx_cth_cluster:start(?config(cluster, Config)),
    settle_cluster(),
    DN = <<"cl_offline_dn">>,
    PK = <<"default">>,
    {ok, 200, _, _Resp} = api_call(N1, #{
        <<"Action">> => <<"BatchPub">>,
        <<"ProductKey">> => PK,
        <<"DeviceName">> => [DN],
        <<"MessageContent">> => base64:encode(?PAYLOAD),
        <<"Qos">> => 1
    }),
    %% Index propagation and mria replication are async; wait until node2 can
    %% see both the index entry and the delivery record locally.
    Indexed =
        wait_until(
            fun() ->
                case erpc:call(N2, emqx_bcast_storage, get_device_deliveries, [{PK, DN}]) of
                    {ok, [_ | _] = DeliveryIds} ->
                        [] =/= erpc:call(N2, mnesia, dirty_read, [bcast_msg, hd(DeliveryIds)]);
                    _ ->
                        false
                end
            end,
            50
        ),
    ?assert(Indexed),
    C = connect(N2, DN),
    sub(C, topic(DN)),
    Msgs = recv(1),
    ?assertEqual(1, length(Msgs)),
    ?assertMatch(#{payload := ?PAYLOAD}, hd(Msgs)),
    emqtt:stop(C).

%% A QoS=1 delivery for D1 and D2 created while both are offline, replayed and
%% acked by D1 on node1, must not be replayed again when D1 reconnects on
%% node2: ack state must be cluster-wide, not node-local.
-doc "Ack state is cluster-wide: a delivery acked on one node is not replayed on another.".
t_cluster_ack_idempotent_across_nodes(Config) ->
    [N1, N2] = emqx_cth_cluster:start(?config(cluster, Config)),
    settle_cluster(),
    DN1 = <<"cl_ack_dn1">>,
    DN2 = <<"cl_ack_dn2">>,
    PK = <<"default">>,
    {ok, 200, _, _} = api_call(N1, #{
        <<"Action">> => <<"BatchPub">>,
        <<"ProductKey">> => PK,
        <<"DeviceName">> => [DN1, DN2],
        <<"MessageContent">> => base64:encode(?PAYLOAD),
        <<"Qos">> => 1
    }),
    %% Index propagation and mria replication are async; wait until node2 can
    %% see the index entry locally.
    Indexed =
        wait_until(
            fun() ->
                case erpc:call(N2, emqx_bcast_storage, get_device_deliveries, [{PK, DN1}]) of
                    {ok, [_ | _]} -> true;
                    _ -> false
                end
            end,
            50
        ),
    ?assert(Indexed),
    C1 = connect(N1, DN1),
    sub(C1, topic(DN1)),
    Msgs1 = recv(1),
    ?assertEqual(1, length(Msgs1)),
    %% D1 acks on node1; wait until node1 has processed the ack and removed
    %% the local index entry.
    Acked =
        wait_until(
            fun() ->
                {ok, []} =:= erpc:call(N1, emqx_bcast_storage, get_device_deliveries, [{PK, DN1}]) andalso
                    {ok, []} =:=
                        erpc:call(N2, emqx_bcast_storage, get_device_deliveries, [{PK, DN1}])
            end,
            50
        ),
    ?assert(Acked),
    emqtt:stop(C1),
    C2 = connect(N2, DN1),
    sub(C2, topic(DN1)),
    %% node2's local index still holds the entry, so with a node-local ack
    %% state it replays the already-acked delivery once more.
    Msgs2 = recv(1),
    ?assertEqual(0, length(Msgs2)),
    emqtt:stop(C2).

%%--------------------------------------------------------------------
%% Ledger combo B: cross-node terminal outcomes close the cluster ledger
%%--------------------------------------------------------------------

sub_qos0(C, Topic) ->
    emqtt:subscribe(C, Topic, 0).

node_metric(Node, Suffix) ->
    try
        erpc:call(Node, prometheus_counter, value, [
            ?BCAST_REGISTRY, <<"bcast_", Suffix/binary>>, []
        ])
    catch
        _:_ -> 0
    end.

cluster_metric(Nodes, Suffix) ->
    lists:sum([node_metric(N, Suffix) || N <- Nodes]).

-doc "Combined ledger scenario B (cluster): D1 acked as QoS1 on node2, D2\n"
"auto-acked as QoS0 on node1 and D3 canceled via management delete on\n"
"node1. When everything settles, the cluster-wide ledger closes\n"
"(sum(wanted) = sum(acked) + sum(auto_acked) + sum(canceled)) and the\n"
"delivered counter matches the real sends across nodes.".
t_metrics_ledger_combo_b_cluster(Config) ->
    [N1, N2] = emqx_cth_cluster:start(?config(cluster, Config)),
    settle_cluster(),
    Nodes = [N1, N2],
    PK = <<"default">>,
    D1 = <<"cl_b_d1">>,
    D2 = <<"cl_b_d2">>,
    D3 = <<"cl_b_d3">>,
    %% QoS1 subscriber: emqtt auto-PUBACKs -> acked
    C1 = connect(N2, D1),
    %% QoS0 subscriber: auto-ack path
    C2 = connect(N1, D2),
    sub(C1, topic(D1)),
    sub_qos0(C2, topic(D2)),
    Subscribed =
        wait_until(
            fun() ->
                node_client_subscribed(N2, D1, topic(D1)) andalso
                    node_client_subscribed(N1, D2, topic(D2))
            end,
            50
        ),
    ?assert(Subscribed),
    W0 = cluster_metric(Nodes, <<"batch_pub_qos1_wanted">>),
    A0 = cluster_metric(Nodes, <<"batch_pub_qos1_acked">>),
    AU0 = cluster_metric(Nodes, <<"batch_pub_qos1_auto_acked">>),
    C0 = cluster_metric(Nodes, <<"batch_pub_qos1_canceled">>),
    D0 = cluster_metric(Nodes, <<"batch_pub_qos1_delivered">>),
    R0 = cluster_metric(Nodes, <<"batch_pub_qos1_redelivered">>),
    {ok, 200, _, _} = api_call(N1, #{
        <<"Action">> => <<"BatchPub">>,
        <<"ProductKey">> => PK,
        <<"DeviceName">> => [D1, D2, D3],
        <<"MessageContent">> => base64:encode(?PAYLOAD),
        <<"Qos">> => 1
    }),
    Msgs = recv(2),
    ?assertEqual(2, length(Msgs)),
    %% Settle async accounting: commit, sends, PUBACK, auto-ack.
    ?assert(
        wait_until(
            fun() -> cluster_metric(Nodes, <<"batch_pub_qos1_wanted">>) >= W0 + 3 end,
            100
        )
    ),
    ?assert(
        wait_until(
            fun() -> cluster_metric(Nodes, <<"batch_pub_qos1_delivered">>) >= D0 + 2 end,
            100
        )
    ),
    ?assert(
        wait_until(
            fun() -> cluster_metric(Nodes, <<"batch_pub_qos1_acked">>) >= A0 + 1 end,
            100
        )
    ),
    ?assert(
        wait_until(
            fun() -> cluster_metric(Nodes, <<"batch_pub_qos1_auto_acked">>) >= AU0 + 1 end,
            100
        )
    ),
    %% Core index removals lag the pull-side counters: wait until D1/D2 are
    %% gone on both nodes and only D3 is still queued, then cancel D3.
    Indexed =
        wait_until(
            fun() ->
                {ok, []} =:= erpc:call(N2, emqx_bcast_storage, get_device_deliveries, [{PK, D1}]) andalso
                    {ok, []} =:=
                        erpc:call(N1, emqx_bcast_storage, get_device_deliveries, [{PK, D1}]) andalso
                    {ok, []} =:=
                        erpc:call(N1, emqx_bcast_storage, get_device_deliveries, [{PK, D2}]) andalso
                    case erpc:call(N1, emqx_bcast_storage, get_device_deliveries, [{PK, D3}]) of
                        {ok, [_ | _]} -> true;
                        _ -> false
                    end
            end,
            100
        ),
    ?assert(Indexed),
    {ok, [D3Did]} = erpc:call(N1, emqx_bcast_storage, get_device_deliveries, [{PK, D3}]),
    ok = erpc:call(N1, emqx_bcast_storage, delete_delivery, [D3Did]),
    ?assert(
        wait_until(
            fun() -> cluster_metric(Nodes, <<"batch_pub_qos1_canceled">>) >= C0 + 1 end,
            100
        )
    ),
    ?assert(
        wait_until(
            fun() ->
                erpc:call(N1, emqx_bcast_storage, get_device_deliveries, [{PK, D3}]) =:= {ok, []} andalso
                    mnesia_safe_rows(N1) =:= 0
            end,
            100
        )
    ),
    %% The cluster ledger closes with no live backlog.
    ?assertEqual(3, cluster_metric(Nodes, <<"batch_pub_qos1_wanted">>) - W0),
    ?assertEqual(1, cluster_metric(Nodes, <<"batch_pub_qos1_acked">>) - A0),
    ?assertEqual(1, cluster_metric(Nodes, <<"batch_pub_qos1_auto_acked">>) - AU0),
    ?assertEqual(1, cluster_metric(Nodes, <<"batch_pub_qos1_canceled">>) - C0),
    ?assertEqual(0, cluster_metric(Nodes, <<"batch_pub_qos1_ttl_expired">>)),
    ?assertEqual(2, cluster_metric(Nodes, <<"batch_pub_qos1_delivered">>) - D0),
    ?assertEqual(0, cluster_metric(Nodes, <<"batch_pub_qos1_redelivered">>) - R0),
    emqtt:stop(C1),
    emqtt:stop(C2).

mnesia_safe_rows(Node) ->
    try
        erpc:call(Node, fun() ->
            length(mnesia:dirty_match_object(bcast_msg, #bcast_msg{_ = '_'}))
        end)
    catch
        _:_ -> -1
    end.

-doc "A core whose plugin is stopped must hand its shards over instead of\n"
"keeping them on dead processes: stopping the plugin leaves its modules loaded\n"
"in that node's code server, so readiness has to follow the plugin's\n"
"supervision tree and the surviving core has to rebuild the partitions that\n"
"changed hands.".
t_core_with_a_stopped_plugin_hands_its_shards_over(Config) ->
    [N1, N2] = emqx_cth_cluster:start(?config(cluster, Config)),
    settle_cluster(),
    ?assertEqual([N1, N2], lists:sort(erpc:call(N1, emqx_bcast, core_nodes, []))),
    %% Stop the plugin the way the plugin framework does it: the supervision
    %% tree goes down, the modules stay loaded.
    ok = erpc:call(N2, application, stop, [emqx_bcast]),
    ?assertEqual(undefined, erpc:call(N2, erlang, whereis, [emqx_bcast_sup])),
    ?assert(
        erpc:call(N2, erlang, function_exported, [
            emqx_bcast_pull_server_pool, want_next_async, 4
        ])
    ),
    %% A module probe would still call this node ready; it must leave shard
    %% ownership and claim targeting instead.
    ?assert(
        wait_until(
            fun() -> not erpc:call(N1, emqx_bcast, is_ready_core, [N2]) end,
            100
        )
    ),
    ?assertEqual([N1], erpc:call(N1, emqx_bcast, core_nodes, [])),
    ?assertEqual([N2], erpc:call(N1, emqx_bcast, unready_core_nodes, [])),
    Shards = lists:seq(0, erpc:call(N1, emqx_bcast_index_owner, shard_count, []) - 1),
    Owners = [erpc:call(N1, emqx_bcast_index_owner, shard_owner, [S]) || S <- Shards],
    ?assertEqual([N1], lists:usort(Owners)),
    %% The partitions that changed hands are dormant on the surviving core
    %% until a drive loads them: without that rebuild the survivor defers every
    %% append and no claim can be served.
    ?assert(wait_until(fun() -> all_partitions_active(N1, Shards) end, 200)),
    %% End to end: a QoS=1 delivery to a device on the surviving core still
    %% reaches the device and completes.
    DN = <<"cl_dn_stopped_plugin">>,
    C = connect(N1, DN),
    sub(C, topic(DN)),
    Subscribed = wait_until(fun() -> node_client_subscribed(N1, DN, topic(DN)) end, 50),
    ?assert(Subscribed),
    {ok, 200, _, _} = api_call(N1, #{
        <<"Action">> => <<"BatchPub">>,
        <<"ProductKey">> => <<"default">>,
        <<"DeviceName">> => [DN],
        <<"MessageContent">> => base64:encode(?PAYLOAD),
        <<"Qos">> => 1
    }),
    Msgs = recv(1),
    ?assertEqual(1, length(Msgs)),
    ?assertMatch(#{payload := ?PAYLOAD}, hd(Msgs)),
    Completed =
        wait_until(
            fun() ->
                {ok, []} =:=
                    erpc:call(N1, emqx_bcast_storage, get_device_deliveries, [
                        {<<"default">>, DN}
                    ])
            end,
            100
        ),
    ?assert(Completed),
    emqtt:stop(C).

%% Every partition the cluster assigns to Node answers as serving.
all_partitions_active(Node, Shards) ->
    lists:all(
        fun(Shard) ->
            case
                erpc:call(Node, emqx_bcast_index_owner, local_handle, [
                    Shard, {shard_status}, 5000
                ])
            of
                {true, _, _} -> true;
                _ -> false
            end
        end,
        Shards
    ).

-doc "A claim trigger must arm a client that connected while this node's\n"
"plugin was not running. A node that has just joined the cluster is still\n"
"syncing and starting the plugin, and a node that receives evacuated clients\n"
"can host them before its plugin is ready: both have no registry entry for\n"
"those clients, and the deliveries used to stay queued until the client's next\n"
"keepalive (minutes at a 600s keepalive, and forever if it never pings).".
t_trigger_arms_a_client_that_connected_before_the_plugin_was_ready(Config) ->
    [N1, N2] = emqx_cth_cluster:start(?config(cluster, Config)),
    settle_cluster(),
    DN = <<"cl_arm_dn">>,
    PK = <<"default">>,
    %% N2's plugin goes away, the client attaches there, and only then does the
    %% plugin come back: exactly the window the rolling upgrade passes through.
    ok = erpc:call(N2, application, stop, [emqx_bcast]),
    %% The readiness probe is cached for a few seconds, so give it time to
    %% notice that the node stopped serving.
    NotServing = wait_until(
        fun() -> not lists:member(N2, erpc:call(N1, emqx_bcast, plugin_running_nodes, [])) end, 50
    ),
    ?assert(NotServing),
    C = connect(N2, DN),
    sub(C, topic(DN)),
    Subscribed = wait_until(fun() -> node_client_subscribed(N2, DN, topic(DN)) end, 50),
    ?assert(Subscribed),
    {ok, _} = erpc:call(N2, application, ensure_all_started, [emqx_bcast]),
    Ready = wait_until(
        fun() -> lists:member(N2, erpc:call(N1, emqx_bcast, plugin_running_nodes, [])) end, 50
    ),
    ?assert(Ready),
    %% The plugin never saw the connect or the subscribe of this client.
    ?assertEqual(0, erpc:call(N2, ets, info, [bcast_device_registry, size])),
    {ok, 200, _, _} = api_call(N1, #{
        <<"Action">> => <<"BatchPub">>,
        <<"ProductKey">> => PK,
        <<"DeviceName">> => [DN],
        <<"MessageContent">> => base64:encode(?PAYLOAD),
        <<"Qos">> => 1
    }),
    %% One trigger is enough: the node verifies the client locally, arms it and
    %% claims its deliveries instead of skipping the device silently.
    Msgs = recv(1),
    ?assertEqual(1, length(Msgs)),
    ?assertMatch(#{payload := ?PAYLOAD}, hd(Msgs)),
    ?assert(
        wait_until(fun() -> node_metric(N2, <<"batch_pub_qos1_armed_on_trigger">>) > 0 end, 50)
    ),
    ?assert(erpc:call(N2, ets, info, [bcast_device_registry, size]) > 0),
    emqtt:stop(C).
