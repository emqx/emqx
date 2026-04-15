%%--------------------------------------------------------------------
%% Copyright (c) 2024-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_ds_builtin_raft_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include("../../emqx/include/emqx.hrl").
-include("../../emqx/include/asserts.hrl").
-include_lib("emqx_durable_storage/include/emqx_ds.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-define(DB, testdb).

-define(ON(NODES, BODY),
    emqx_ds_test_helpers:on(NODES, fun() -> BODY end)
).

opts(_Config, Overrides) ->
    emqx_utils_maps:deep_merge(
        #{
            backend => builtin_raft,
            payload_type => ?ds_pt_mqtt,
            n_shards => 16,
            n_sites => 1,
            replication_factor => 3,
            replication_options => #{
                wal_max_size_bytes => 1_000_000
            },
            %% Half the default:
            ra_timeout => 2_500
        },
        Overrides
    ).

appspec(ra) ->
    {ra, #{
        %% NOTE: Recover quicker in case the node sending a snapshot goes down or crash.
        %% TODO: Probably need to have those tighter timeouts as defaults.
        override_env => [{receive_snapshot_timeout, 5_000}]
    }};
appspec(emqx_durable_storage) ->
    {emqx_durable_storage, #{
        before_start => fun snabbkaffe:fix_ct_logging/0
    }};
appspec(emqx_ds_builtin_raft) ->
    {emqx_ds_builtin_raft, #{
        after_start => fun() -> logger:set_module_level(ra_server, info) end
    }}.

-doc """
This testcase verifies that the shard leader supervises optimistic
transaction server (OTX).

1. ra server on the leader replica starts OTX.
2. ra server stops OTX when it loses leadership.
3. OTX gets restarted on the leader when it crashes or fails to start.
""".
t_leader_otx_supervion(init, Config) ->
    Apps = [appspec(emqx_durable_storage), appspec(emqx_ds_builtin_raft)],
    NodeSpecs = emqx_cth_cluster:mk_nodespecs(
        [
            {t_leader_otx_supervion1, #{apps => Apps}},
            {t_leader_otx_supervion2, #{apps => Apps}},
            {t_leader_otx_supervion3, #{apps => Apps}}
        ],
        #{work_dir => ?config(work_dir, Config)}
    ),
    Nodes = emqx_cth_cluster:start(NodeSpecs),
    [{nodes, Nodes}, {specs, NodeSpecs} | Config];
t_leader_otx_supervion('end', Config) ->
    ok = emqx_cth_cluster:stop(?config(nodes, Config)),
    ok = snabbkaffe:stop().

t_leader_otx_supervion(Config) ->
    DB = ?FUNCTION_NAME,
    Shard = <<"0">>,
    NShards = 1,
    DBOpts = #{
        backend => builtin_raft,
        n_shards => NShards,
        n_sites => 1,
        replication_factor => 3,
        replication_options => #{}
    },
    Nodes = proplists:get_value(nodes, Config),
    ?check_trace(
        #{timetrap => 30_000},
        begin
            %% Open DB on all nodes:
            [?assertMatch(ok, ?ON(N, emqx_ds:open_db(DB, DBOpts))) || N <- Nodes],
            %% Find the leader:
            {ok, #{?snk_meta := #{node := Leader0}}} = ?block_until(#{
                ?snk_kind := ds_otx_up, db := DB, shard := Shard
            }),
            %% 1. Kill OTX process on the leader. It should get restarted:
            ?tp(notice, test_otx_restarts, #{leader => Leader0}),
            ?wait_async_action(
                ?ON(
                    Leader0,
                    exit(emqx_ds_optimistic_tx:where(DB, Shard), shutdown)
                ),
                #{?snk_kind := ds_otx_up, db := DB, shard := Shard}
            ),
            %% Repeat the same (leader shouldn't change):
            ?wait_async_action(
                ?ON(
                    Leader0,
                    exit(emqx_ds_optimistic_tx:where(DB, Shard), shutdown)
                ),
                #{?snk_kind := ds_otx_up, db := DB, shard := Shard}
            ),
            %% 2. Test graceful stop of OTX when server role changes.
            {ok, Sub0} = snabbkaffe:subscribe(
                ?match_event(
                    #{?snk_kind := K} when
                        K =:= ds_otx_up; K =:= dsraft_shut_down_otx; K =:= ds_otx_terminate
                ),
                3,
                infinity
            ),
            %% Ask Leader0 to give up its leadership:
            ?ON(
                Leader0,
                begin
                    [Leader, Next | _] = emqx_ds_builtin_raft_shard:servers(
                        DB, Shard, leader_preferred
                    ),
                    ?tp(notice, test_transfer_leaderhip, #{from => Leader, to => Next}),
                    ok = ra:transfer_leadership(Leader, Next)
                end
            ),
            {ok, Events} = snabbkaffe:receive_events(Sub0),
            [
                #{?snk_kind := dsraft_shut_down_otx, db := DB, shard := Shard},
                #{?snk_kind := ds_otx_terminate},
                #{
                    ?snk_kind := ds_otx_up,
                    db := DB,
                    shard := Shard,
                    ?snk_meta := #{node := Leader1}
                }
            ] = Events,
            %% 3. Make OTX fail to start on Leader1 and restart it. Ra
            %% server should attempt to restart the leader:
            ?tp(notice, test_inject_start_failure, #{node => Leader1}),
            ?ON(
                Leader1,
                begin
                    ok = meck:new(emqx_ds_optimistic_tx, [no_link, no_history, passthrough]),
                    ok = meck:expect(
                        emqx_ds_optimistic_tx,
                        init,
                        fun(_Parent, _DB, _Shard, _CBM) ->
                            exit(mocked)
                        end
                    ),
                    exit(emqx_ds_optimistic_tx:where(DB, Shard), shutdown)
                end
            ),
            %% Wait for at least two attempts:
            ?block_until(#{?snk_kind := dsraft_optimistic_leader_start_fail}, infinity, 0),
            ?block_until(#{?snk_kind := dsraft_optimistic_leader_start_fail}, infinity, 0),
            %% Remove injected error. OTX should start:
            ?wait_async_action(
                ?ON(
                    Leader1,
                    ok = meck:unload(emqx_ds_optimistic_tx)
                ),
                #{?snk_kind := ds_otx_up, db := DB, shard := Shard}
            )
        end,
        []
    ).

t_metadata(init, Config) ->
    Apps = emqx_cth_suite:start([emqx_ds_builtin_raft], #{
        work_dir => ?config(work_dir, Config)
    }),
    [{apps, Apps} | Config];
t_metadata('end', Config) ->
    emqx_cth_suite:stop(?config(apps, Config)),
    Config.

t_metadata(_Config) ->
    DB = ?FUNCTION_NAME,
    NShards = 1,
    Options = #{
        backend => builtin_raft,
        n_shards => NShards,
        n_sites => 1,
        replication_factor => 1,
        replication_options => #{}
    },
    ?assertMatch(ok, open_db(DB, Options)),
    %% Check metadata:
    %%    We have only one site:
    [Site] = emqx_ds_builtin_raft_meta:sites(),
    %%    Check all shards:
    Shards = emqx_ds_builtin_raft_meta:shards(DB),
    %%    Since there is only one site all shards should be allocated
    %%    to this site:
    MyShards = emqx_ds_builtin_raft_meta:my_shards(DB),
    ?assertEqual(NShards, length(Shards)),
    lists:foreach(
        fun(Shard) ->
            ?assertEqual(
                [Site], emqx_ds_builtin_raft_meta:replica_set(DB, Shard)
            )
        end,
        Shards
    ),
    ?assertEqual(lists:sort(Shards), lists:sort(MyShards)).

%% This testcase verifies that shards are allocated sanely during DB startup:
%% * Lost sites are not assigned any shards.
t_shards_allocation(init, Config) ->
    Apps = [appspec(emqx_durable_storage), appspec(emqx_ds_builtin_raft)],
    NodeSpecs = emqx_cth_cluster:mk_nodespecs(
        [
            {t_shards_allocation1, #{apps => Apps}},
            {t_shards_allocation2, #{apps => Apps}},
            {t_shards_allocation3, #{apps => Apps}}
        ],
        #{work_dir => ?config(work_dir, Config)}
    ),
    Nodes = emqx_cth_cluster:start(NodeSpecs),
    ok = snabbkaffe:start_trace(),
    [{nodes, Nodes}, {specs, NodeSpecs} | Config];
t_shards_allocation('end', Config) ->
    ok = emqx_cth_cluster:stop(?config(nodes, Config)),
    ok = snabbkaffe:stop().

t_shards_allocation(Config) ->
    %% Find out which sites are there.
    Nodes = [NodeLost, Node | _] = ?config(nodes, Config),
    [SiteLost | SitesLive] = [ds_repl_meta(N, this_site) || N <- Nodes],

    %% Make the cluster consider `Node1` as lost.
    ok = emqx_cth_peer:kill(NodeLost),
    _ = ?retry(200, 5, [NodeLost] = [NodeLost] -- ?ON(Node, mria:cluster_nodes(running))),
    %% Simulate node loss that went unnoticed, taken from `mria:force_leave/1`.
    true = ?ON(Node, mnesia_lib:del(extra_db_nodes, Node)),
    ok = ?ON(Node, mria_mnesia:del_schema_copy(NodeLost)),
    ?assertEqual(
        [SiteLost],
        ?ON(Node, emqx_ds_builtin_raft_meta:sites(lost))
    ),

    %% Initialize DB on all nodes and wait for it to be online.
    %% Since there are only 2 live sites, these should be where DB is allocated.
    DB = ?FUNCTION_NAME,
    Opts = opts(Config, #{n_shards => 4, n_sites => 2, replication_factor => 3}),
    emqx_ds_raft_test_helpers:assert_db_open(Nodes -- [NodeLost], DB, Opts),

    ?assertSameSet(
        SitesLive,
        ?ON(Node, emqx_ds_builtin_raft_meta:db_sites(DB))
    ),

    %% Restart `Node`.
    [_SpecLost, Spec | _] = ?config(specs, Config),
    [Node] = emqx_cth_cluster:restart(Spec),
    emqx_ds_raft_test_helpers:assert_db_open([Node], DB, Opts),
    %% Forget the lost node.
    ?ON(Node, emqx_ds_builtin_raft_meta:forget_node(NodeLost)),
    ?assertEqual([], ?ON(Node, emqx_ds_builtin_raft_meta:sites(lost))).

%% This testcase verifies that dropping a DS DB cleans up all relevant state and data
%% across the whole cluster.
t_drop_replicated_db(init, Config) ->
    Apps = [appspec(ra), appspec(emqx_durable_storage), appspec(emqx_ds_builtin_raft)],
    NodeSpecs = emqx_cth_cluster:mk_nodespecs(
        [
            {t_drop_replicated_db1, #{apps => Apps}},
            {t_drop_replicated_db2, #{apps => Apps}}
        ],
        #{work_dir => ?config(work_dir, Config)}
    ),
    Nodes = emqx_cth_cluster:start(NodeSpecs),
    [{nodes, Nodes} | Config];
t_drop_replicated_db('end', Config) ->
    ok = emqx_cth_cluster:stop(?config(nodes, Config)).

t_drop_replicated_db(Config) ->
    Nodes = [N1 | _] = ?config(nodes, Config),
    ?check_trace(
        #{timetrap => 20_000},
        begin
            %% Initialize DB on all nodes and wait for it to be online.
            Opts = opts(Config, #{n_shards => 16, n_sites => 2}),
            emqx_ds_raft_test_helpers:assert_db_open(Nodes, ?DB, Opts),
            ?assertEqual(
                [16, 16],
                [n_shards_online(N, ?DB) || N <- Nodes],
                "Each site is responsible for each shard"
            ),

            %% Drop the DB.
            ?assertEqual(ok, ?ON(N1, emqx_ds:drop_db(?DB))),

            %% Verify DB was dropped cleanly.
            ?defer_assert(
                ?assertEqual(
                    [false || _ <- Nodes],
                    ?ON(Nodes, lists:member(?DB, emqx_ds_builtin_raft_meta:dbs())),
                    "no DB metadata is present"
                )
            ),
            ?defer_assert(
                ?assertEqual(
                    [[] || _ <- Nodes],
                    ?ON(Nodes, emqx_ds_builtin_raft_meta:shards(?DB)),
                    "no DB shards metadata is present"
                )
            ),
            ?defer_assert(
                ?assertEqual(
                    [undefined || _ <- Nodes],
                    ?ON(Nodes, emqx_dsch:get_db_schema(?DB)),
                    "no DB runtime schema is present"
                )
            ),
            ?defer_assert(
                ?assertEqual(
                    [undefined || _ <- Nodes],
                    ?ON(Nodes, emqx_ds_builtin_raft_db_sup:whereis_db(?DB)),
                    "no DB processes are still running"
                )
            ),
            ?defer_assert(
                ?assertEqual(
                    [[] || _ <- Nodes],
                    ?ON(
                        Nodes,
                        emqx_utils_fs:traverse_dir(
                            fun(AbsPath, _Info, Acc) -> [AbsPath | Acc] end,
                            [],
                            filename:join([emqx_ds_storage_layer:base_dir(), ?DB])
                        )
                    ),
                    "no leftover data files remain"
                )
            )
        end,
        [check_membership_consistent(?DB, Nodes)]
    ).

t_replication_transfers_snapshots(init, Config) ->
    Apps = [appspec(ra), appspec(emqx_durable_storage), appspec(emqx_ds_builtin_raft)],
    NodeSpecs = emqx_cth_cluster:mk_nodespecs(
        [
            {t_replication_transfers_snapshots1, #{apps => Apps}},
            {t_replication_transfers_snapshots2, #{apps => Apps}},
            {t_replication_transfers_snapshots3, #{apps => Apps}}
        ],
        #{work_dir => ?config(work_dir, Config)}
    ),
    Nodes = emqx_cth_cluster:start(NodeSpecs),
    [{nodes, Nodes}, {specs, NodeSpecs} | Config];
t_replication_transfers_snapshots('end', Config) ->
    ok = emqx_cth_cluster:stop(?config(nodes, Config)).

t_replication_transfers_snapshots(Config) ->
    %% NOTE
    %% This amounts to 1200 single-record OTX batches, which is enough to trigger
    %% Ra machine snapshot, see `?RA_RELEASE_LOG_MIN_FREQ` definition.
    %% TODO
    %% Introduce explicit snapshotting to the API and rely on it instead.
    NMsgs = 200,
    NClients = 6,
    ReplicationOpts = #{snapshot_interval => 100},
    {Stream, TopicStreams} = emqx_ds_test_helpers:interleaved_topic_messages(
        ?FUNCTION_NAME, NClients, NMsgs
    ),
    ?check_trace(
        #{timetrap => 20_000},
        begin
            Nodes = [Node, NodeOffline | _] = ?config(nodes, Config),
            [_, SpecOffline | _] = ?config(specs, Config),

            %% Initialize DB on all nodes and wait for it to be online.
            Opts = opts(
                Config,
                #{n_shards => 1, n_sites => 3, replication_options => ReplicationOpts}
            ),
            emqx_ds_raft_test_helpers:assert_db_open(Nodes, ?DB, Opts),

            %% Stop the DB on the "offline" node.
            ok = emqx_cth_cluster:stop_node(NodeOffline),
            ?retry(200, 5, [NodeOffline] = [NodeOffline] -- ?ON(Node, mria:cluster_nodes(running))),

            %% Fill the storage with enough messages to trigger snapshotting.
            emqx_ds_raft_test_helpers:apply_stream(?DB, Nodes -- [NodeOffline], Stream),

            %% Restart the node.
            %% Wait the replica to be restored from a snapshot.
            [NodeOffline] = emqx_cth_cluster:restart(SpecOffline),
            ok = ?ON(NodeOffline, open_db(?DB, opts(Config, #{n_shards => 1}))),

            %% Wait until any pending replication activities are finished (e.g. Raft log entries).
            ?ON(NodeOffline, emqx_ds_builtin_raft:wait_replicas(?DB, 5_000)),

            %% Check that the DB has been restored:
            emqx_ds_raft_test_helpers:verify_stream_effects(
                ?DB, ?FUNCTION_NAME, Nodes, TopicStreams
            ),

            {NodeOffline, Nodes}
        end,
        [
            {"snapshot taken on live nodes", fun({NodeOffline, Nodes}, Trace) ->
                NodesLive = Nodes -- [NodeOffline],
                Events = ?of_kind(dsraft_snapshot_write, Trace),
                ?assertMatch([[_], [_]], [?of_node(N, Events) || N <- NodesLive])
            end},
            {"single snapshot transfer to the node that was offline", fun({NodeOffline, _}, Trace) ->
                ?assertMatch(
                    [#{?snk_meta := #{node := NodeOffline}}],
                    ?of_kind(dsraft_snapshot_accept_complete, Trace)
                )
            end},
            {"no incomplete snapshot transfers", fun(Trace) ->
                ?assert(
                    ?strict_causality(
                        #{?snk_kind := dsraft_snapshot_read_started},
                        #{?snk_kind := dsraft_snapshot_accept_complete},
                        Trace
                    )
                )
            end}
        ]
    ).

t_rebalance(init, Config) ->
    Apps = [appspec(ra), appspec(emqx_durable_storage), appspec(emqx_ds_builtin_raft)],
    Nodes = emqx_cth_cluster:start(
        [
            {t_rebalance1, #{apps => Apps}},
            {t_rebalance2, #{apps => Apps}},
            {t_rebalance3, #{apps => Apps}},
            {t_rebalance4, #{apps => Apps}}
        ],
        #{work_dir => ?config(work_dir, Config)}
    ),
    [{nodes, Nodes} | Config];
t_rebalance('end', Config) ->
    ok = emqx_cth_cluster:stop(?config(nodes, Config)).

%% This testcase verifies that the storage rebalancing works correctly:
%% 1. Join/leave operations are applied successfully.
%% 2. Message data survives the rebalancing.
%% 3. Shard cluster membership converges to the target replica allocation.
%% 4. Replication factor is respected.
t_rebalance(Config) ->
    NMsgs = 100,
    NClients = 5,
    {Stream0, TopicStreams} = emqx_ds_test_helpers:interleaved_topic_messages(
        ?FUNCTION_NAME, NClients, NMsgs
    ),
    Nodes = [N1, N2 | _] = ?config(nodes, Config),
    ?check_trace(
        #{timetrap => 60_000},
        begin
            Sites = [S1, S2 | _] = [ds_repl_meta(N, this_site) || N <- Nodes],
            ct:pal("Sites: ~p~n", [Sites]),

            %% 1. Initialize DB on all nodes:
            Opts = opts(Config, #{n_shards => 16, n_sites => 1, replication_factor => 3}),
            emqx_ds_raft_test_helpers:assert_db_open(Nodes, ?DB, Opts),

            %% 1.1 Kick all sites except S1 from the replica set as
            %% the initial condition:
            ?assertMatch(
                {ok, [_]},
                ?ON(N1, emqx_ds_builtin_raft_meta:assign_db_sites(?DB, [S1]))
            ),
            ?ON(N1, emqx_ds_raft_test_helpers:wait_db_transitions_done(?DB)),

            %% 1.2 Verify that all nodes have the same view of metadata storage:
            ?defer_assert(
                ?assertEqual(
                    [[S1] || _ <- Nodes],
                    [?ON(Node, emqx_ds_builtin_raft_meta:db_sites(?DB)) || Node <- Nodes],
                    "Initially, only S1 should be responsible for all shards"
                )
            ),
            ?defer_assert(
                ?assertEqual(
                    [16, 0, 0, 0],
                    [n_shards_online(Node, ?DB) || Node <- Nodes],
                    "Only S1 should be responsible for all shards"
                )
            ),

            Sequence = [
                %% Join the second site to the DB replication sites:
                {N1, join_db_site, S2},
                %% Should be a no-op:
                {N2, join_db_site, S2},
                %% Now join the rest of the sites:
                {N2, assign_db_sites, Sites}
            ],
            Stream1 = emqx_utils_stream:interleave(
                [
                    {40, Stream0},
                    emqx_utils_stream:const(add_generation)
                ],
                false
            ),
            Stream = emqx_utils_stream:interleave(
                [
                    {50, Stream1},
                    emqx_utils_stream:list(Sequence)
                ],
                true
            ),

            %% 2. Start filling the storage and changing allocation:
            emqx_ds_raft_test_helpers:apply_stream(?DB, Nodes, Stream),
            emqx_ds_raft_test_helpers:verify_stream_effects(
                ?DB, ?FUNCTION_NAME, Nodes, TopicStreams
            ),
            ?ON(N1, emqx_ds_raft_test_helpers:wait_db_transitions_done(?DB)),

            %% Verify that the set of shard servers matches the target allocation.
            ?defer_assert(
                ?assertEqual(
                    [16 * 3 div length(Nodes) || _ <- Nodes],
                    [n_shards_online(Node, ?DB) || Node <- Nodes],
                    "Each site is now responsible for 3/4 of the shards"
                )
            ),
            ?defer_assert(
                ?assertEqual(
                    [],
                    [
                        {unready, Shard, Node}
                     || Node <- Nodes,
                        Shard <- ds_repl_meta(Node, my_shards, [?DB]),
                        emqx_ds_raft_test_helpers:shard_readiness(Node, ?DB, Shard) =/= ready
                    ],
                    "Shard servers are not ready after reaching target allocation"
                )
            ),

            %% Scale down the cluster by removing the first node.
            ?assertMatch({ok, _}, ds_repl_meta(N1, leave_db_site, [?DB, S1])),
            ?ON(N1, emqx_ds_raft_test_helpers:wait_db_transitions_done(?DB)),

            %% Verify that each node except for N1 is now responsible for each shard.
            ?defer_assert(
                ?assertEqual(
                    [0, 16, 16, 16],
                    [n_shards_online(N, ?DB) || N <- Nodes],
                    "Each of [S2, S3, S4] is responsible for each shard"
                )
            ),

            %% Verify that the messages are once again preserved after the rebalance:
            emqx_ds_raft_test_helpers:verify_stream_effects(
                ?DB, ?FUNCTION_NAME, Nodes, TopicStreams
            )
        end,
        [
            check_no_transition_crashes(),
            check_membership_consistent(?DB, Nodes)
        ]
    ).

t_join_leave_errors(init, Config) ->
    Apps = [appspec(emqx_durable_storage), appspec(emqx_ds_builtin_raft)],
    Nodes = emqx_cth_cluster:start(
        [
            {t_join_leave_errors1, #{apps => Apps}},
            {t_join_leave_errors2, #{apps => Apps}}
        ],
        #{work_dir => ?config(work_dir, Config)}
    ),
    [{nodes, Nodes} | Config];
t_join_leave_errors('end', Config) ->
    ok = emqx_cth_cluster:stop(?config(nodes, Config)).

t_join_leave_errors(Config) ->
    %% This testcase verifies that logical errors arising during handling of
    %% join/leave operations are reported correctly.
    DB = ?FUNCTION_NAME,
    [N1, N2] = ?config(nodes, Config),
    Opts = opts(Config, #{n_shards => 16, n_sites => 1, replication_factor => 3}),

    ?check_trace(
        begin
            ?assertEqual(ok, erpc:call(N1, emqx_ds, open_db, [DB, Opts])),
            ?assertEqual(ok, erpc:call(N2, emqx_ds, open_db, [DB, Opts])),

            [S1, S2] = [ds_repl_meta(N, this_site) || N <- [N1, N2]],

            ?assertEqual(
                lists:sort([S1, S2]), lists:sort(ds_repl_meta(N1, db_sites, [DB]))
            ),

            %% Attempts to join a nonexistent DB / site.
            ?assertEqual(
                {error, {nonexistent_db, boo}},
                ds_repl_meta(N1, join_db_site, [_DB = boo, S1])
            ),
            ?assertEqual(
                {error, {nonexistent_sites, [<<"NO-MANS-SITE">>]}},
                ds_repl_meta(N1, join_db_site, [DB, <<"NO-MANS-SITE">>])
            ),
            %% NOTE: Leaving a non-existent site is not an error.
            ?assertEqual(
                {ok, unchanged},
                ds_repl_meta(N1, leave_db_site, [DB, <<"NO-MANS-SITE">>])
            ),

            %% Should be no-op.
            ?assertEqual({ok, unchanged}, ds_repl_meta(N1, join_db_site, [DB, S1])),
            ?assertEqual([], ?ON(N1, emqx_ds_raft_test_helpers:db_transitions(DB))),

            %% Leave S2:
            ?assertEqual(
                {ok, [S1]},
                ds_repl_meta(N1, leave_db_site, [DB, S2])
            ),
            %% Impossible to leave the last site:
            ?assertEqual(
                {error, {too_few_sites, []}},
                ds_repl_meta(N1, leave_db_site, [DB, S1])
            ),

            %% "Move" the DB to the other node.
            ?assertMatch({ok, _}, ds_repl_meta(N1, join_db_site, [DB, S2])),
            ?assertMatch({ok, _}, ds_repl_meta(N2, leave_db_site, [DB, S1])),
            ?ON(N1, emqx_ds_raft_test_helpers:wait_db_transitions_done(DB)),

            %% Should be no-op.
            ?assertMatch({ok, _}, ds_repl_meta(N2, leave_db_site, [DB, S1])),
            ?assertEqual([], ?ON(N1, emqx_ds_raft_test_helpers:db_transitions(DB)))
        end,
        []
    ).

t_rebalance_chaotic_converges(init, Config) ->
    Apps = [appspec(ra), appspec(emqx_durable_storage), appspec(emqx_ds_builtin_raft)],
    Nodes = emqx_cth_cluster:start(
        [
            {t_rebalance_chaotic_converges1, #{apps => Apps}},
            {t_rebalance_chaotic_converges2, #{apps => Apps}},
            {t_rebalance_chaotic_converges3, #{apps => Apps}}
        ],
        #{work_dir => ?config(work_dir, Config)}
    ),
    [{nodes, Nodes} | Config];
t_rebalance_chaotic_converges('end', Config) ->
    ok = emqx_cth_cluster:stop(?config(nodes, Config)).

t_rebalance_chaotic_converges(Config) ->
    %% This testcase verifies that even a very chaotic sequence of join/leave
    %% operations will still be handled consistently, and that the shard
    %% allocation will converge to the target state.
    NMsgs = 400,
    Nodes = [N1, N2, N3] = ?config(nodes, Config),
    Sites = [S1, S2, S3] = [ds_repl_meta(N, this_site) || N <- Nodes],
    NClients = 5,
    {Stream0, TopicStreams} = emqx_ds_test_helpers:interleaved_topic_messages(
        ?FUNCTION_NAME, NClients, NMsgs
    ),
    Sequence = [
        {N1, join_db_site, S3},
        {N2, leave_db_site, S2},
        {N3, leave_db_site, S1},
        {N1, join_db_site, S2},
        {N2, join_db_site, S1},
        {N3, leave_db_site, S3},
        {N1, leave_db_site, S1},
        {N2, join_db_site, S3}
    ],

    %% Interleaved list of events:
    Stream = emqx_utils_stream:interleave(
        [
            {200, Stream0},
            emqx_utils_stream:list(Sequence)
        ],
        true
    ),

    ?check_trace(
        #{timetrap => 60_000},
        begin
            ct:pal("Sites: ~p~n", [Sites]),

            %% Initialize DB on first two nodes.
            Opts = opts(Config, #{n_shards => 16, n_sites => 2, replication_factor => 3}),

            %% Open DB:
            emqx_ds_raft_test_helpers:assert_db_open(Nodes, ?DB, Opts),

            %% Kick N3 from the replica set as the initial condition:
            ?assertMatch(
                {ok, [_, _]},
                ?ON(N1, emqx_ds_builtin_raft_meta:assign_db_sites(?DB, [S1, S2]))
            ),
            ?ON(N1, emqx_ds_raft_test_helpers:wait_db_transitions_done(?DB)),

            ?defer_assert(
                ?assertEqual(
                    lists:sort([S1, S2]),
                    ds_repl_meta(N1, db_sites, [?DB]),
                    "Initially, the DB is assigned to [S1, S2]"
                )
            ),
            ?defer_assert(
                ?assertEqual(
                    [16, 16, 0],
                    [n_shards_online(N, ?DB) || N <- Nodes],
                    "Initially, DB shards are online on [N1, N2]"
                )
            ),

            %% Store the messages + chaotically change the membership.
            emqx_ds_raft_test_helpers:apply_stream(?DB, Nodes, Stream),

            %% Wait for the last transition to complete.
            ?ON(N1, emqx_ds_raft_test_helpers:wait_db_transitions_done(?DB)),

            ?defer_assert(
                ?assertEqual(
                    lists:sort([S2, S3]),
                    ds_repl_meta(N1, db_sites, [?DB]),
                    "DB is now assigned to [S2, S3]"
                )
            ),
            ?defer_assert(
                ?assertEqual(
                    [0, 16, 16],
                    [n_shards_online(N, ?DB) || N <- Nodes],
                    "DB shards are now online only on [N2, N3]"
                )
            ),

            %% Check that all messages are still there.
            ?ON(Nodes, emqx_ds_builtin_raft:wait_replicas(?DB, infinity)),
            emqx_ds_raft_test_helpers:verify_stream_effects(
                ?DB, ?FUNCTION_NAME, Nodes, TopicStreams
            )
        end,
        [
            check_no_transition_crashes(),
            check_membership_consistent(?DB, Nodes)
        ]
    ).

t_rebalance_offline_restarts(init, Config) ->
    Apps = [appspec(emqx_durable_storage), appspec(emqx_ds_builtin_raft)],
    Specs = emqx_cth_cluster:mk_nodespecs(
        [
            {t_rebalance_offline_restarts1, #{apps => Apps}},
            {t_rebalance_offline_restarts2, #{apps => Apps}},
            {t_rebalance_offline_restarts3, #{apps => Apps}}
        ],
        #{work_dir => ?config(work_dir, Config)}
    ),
    Nodes = emqx_cth_cluster:start(Specs),
    [{nodes, Nodes}, {nodespecs, Specs} | Config];
t_rebalance_offline_restarts('end', Config) ->
    ok = emqx_cth_cluster:stop(?config(nodes, Config)).

t_rebalance_offline_restarts(Config) ->
    %% This testcase verifies that rebalancing progresses if nodes restart or
    %% go offline and never come back.
    Nodes = [N1, N2, N3] = ?config(nodes, Config),
    _Specs = [NS1, NS2, _] = ?config(nodespecs, Config),

    ?check_trace(
        #{timetrap => 30_000},
        begin
            %% Initialize DB on all 3 nodes.
            Opts = opts(Config, #{n_shards => 8, n_sites => 3, replication_factor => 3}),
            emqx_ds_raft_test_helpers:assert_db_open(Nodes, ?DB, Opts),
            ?assertEqual([8 || _ <- Nodes], [n_shards_online(N, ?DB) || N <- Nodes]),

            %% Find out which sites are there.
            Sites = [S1, S2, S3] = [ds_repl_meta(N, this_site) || N <- Nodes],
            ct:pal("Sites: ~p~n", [Sites]),

            %% Shut down N3 and then remove it from the DB.
            ok = emqx_cth_cluster:stop_node(N3),
            ?assertMatch({ok, _}, ds_repl_meta(N1, leave_db_site, [?DB, S3])),
            ct:pal("Transitions: ~p~n", [?ON(N1, emqx_ds_raft_test_helpers:db_transitions(?DB))]),

            %% Wait until at least one transition completes.
            ?block_until(#{?snk_kind := dsraft_shard_transition_end}),

            %% Restart N1 and N2.
            [N1] = emqx_cth_cluster:restart(NS1),
            [N2] = emqx_cth_cluster:restart(NS2),
            ?assertEqual([ok, ok], ?ON([N1, N2], emqx_ds:open_db(?DB, Opts))),

            %% Target state should still be reached eventually.
            ?ON([N1, N2], emqx_ds_raft_test_helpers:wait_db_transitions_done(?DB)),
            ?ON([N1, N2], emqx_ds_builtin_raft:wait_replicas(?DB, infinity)),
            ?assertEqual(lists:sort([S1, S2]), ds_repl_meta(N1, db_sites, [?DB]))
        end,
        [
            check_no_transition_crashes(),
            check_membership_consistent(?DB, [N1, N2])
        ]
    ).

t_rebalance_tolerate_lost(init, Config) ->
    Apps = [appspec(emqx_durable_storage), appspec(emqx_ds_builtin_raft)],
    Specs = emqx_cth_cluster:mk_nodespecs(
        [
            {t_rebalance_tolerate_lost1, #{apps => Apps}},
            {t_rebalance_tolerate_lost2, #{apps => Apps}},
            {t_rebalance_tolerate_lost3, #{apps => Apps}}
        ],
        #{work_dir => ?config(work_dir, Config)}
    ),
    Nodes = emqx_cth_cluster:start(Specs),
    [{nodes, Nodes}, {nodespecs, Specs} | Config];
t_rebalance_tolerate_lost('end', Config) ->
    ok = emqx_cth_cluster:stop(?config(nodes, Config)).

%% This testcase verifies that rebalancing can conclude if there are shards
%% with replicas residing exclusively on nodes that left the cluster (w/o
%% handing the data off first).
t_rebalance_tolerate_lost(Config) ->
    Nodes = [N1 | NodesAlive = [N2, N3]] = ?config(nodes, Config),
    NMsgs = 10,
    {MsgStream, TopicStreams} =
        emqx_ds_test_helpers:interleaved_topic_messages(?FUNCTION_NAME, 10, NMsgs),

    %% Find out which sites are there.
    Sites = [S1, S2, S3] = [ds_repl_meta(N, this_site) || N <- Nodes],
    ct:pal("Sites: ~p", [Sites]),

    ?check_trace(
        #{timetrap => 30_000},
        begin
            %% Start and initialize DB.
            Opts = opts(Config, #{n_shards => 8, n_sites => 1, replication_factor => 3}),
            emqx_ds_raft_test_helpers:assert_db_open(Nodes, ?DB, Opts),

            %% Make S1 exclusively responsible for all data.
            {ok, _} = ds_repl_meta(N1, assign_db_sites, [?DB, [S1]]),
            ?ON(N1, emqx_ds_raft_test_helpers:wait_db_transitions_done(?DB)),
            ?assertEqual([S1], ds_repl_meta(N2, db_sites, [?DB])),
            ?assertEqual([8, 0, 0], [n_shards_online(N, ?DB) || N <- Nodes]),

            %% Shut down N1 and then make it leave the cluster.
            %% This will lead to a situation when DB is residing on out-of-cluster nodes only.
            ok = emqx_cth_cluster:stop_node(N1),
            ?retry(200, 5, ?assertSameSet(NodesAlive, ?ON(N2, mria:cluster_nodes(running)))),
            ok = ?ON(N2, emqx_cluster:force_leave(N1)),
            ?retry(200, 5, ?assertSameSet(NodesAlive, ?ON(N2, mria:cluster_nodes(cores)))),

            %% Attempt to forget S1 should fail.
            ?assertEqual(
                {error, {member_of_replica_sets, [?DB]}},
                ?ON(N2, emqx_ds_builtin_raft_meta:forget_site(S1))
            ),

            %% Now turn S2 and S3 into members of DB replica set, excluding S1 in effect.
            {ok, _} = ds_repl_meta(N2, assign_db_sites, [?DB, [S2, S3]]),
            ct:pal("DS Status [lost node rebalancing]:", []),
            ?ON(N2, emqx_ds_builtin_raft_meta:print_status()),

            %% Target state should still be reached eventually.
            ?ON(N2, emqx_ds_raft_test_helpers:wait_db_transitions_done(?DB)),
            ?assertSameSet([S2, S3], ds_repl_meta(N2, db_sites, [?DB])),
            ?assertEqual([8, 8], [n_shards_online(N, ?DB) || N <- NodesAlive]),

            %% Messages can now again be persisted successfully.
            ?assertEqual(ok, emqx_ds_raft_test_helpers:apply_stream(?DB, NodesAlive, MsgStream)),
            emqx_ds_raft_test_helpers:verify_stream_effects(
                ?DB, ?FUNCTION_NAME, NodesAlive, TopicStreams
            ),

            %% Attempt to forget S1 should now succeed.
            ?assertEqual(ok, ?ON(N2, emqx_ds_builtin_raft_meta:forget_site(S1)))
        end,
        [
            check_no_transition_crashes(),
            check_membership_consistent(?DB, [N2, N3])
        ]
    ).

t_rebalance_tolerate_permanently_lost_quorum(init, Config) ->
    Apps = [appspec(emqx_durable_storage), appspec(emqx_ds_builtin_raft)],
    Specs = emqx_cth_cluster:mk_nodespecs(
        [
            {t_rebalance_tolerate_plq1, #{apps => Apps}},
            {t_rebalance_tolerate_plq2, #{apps => Apps}},
            {t_rebalance_tolerate_plq3, #{apps => Apps}},
            {t_rebalance_tolerate_plq4, #{apps => Apps}}
        ],
        #{work_dir => ?config(work_dir, Config)}
    ),
    ok = snabbkaffe:start_trace(),
    Nodes = emqx_cth_cluster:start(Specs),
    [{nodes, Nodes}, {nodespecs, Specs} | Config];
t_rebalance_tolerate_permanently_lost_quorum('end', Config) ->
    ok = emqx_cth_cluster:stop(?config(nodes, Config)),
    ok = snabbkaffe:stop().

%% This testcase verifies that rebalancing can still conclude if there are
%% shards where most of the replicas were permanently lost, such that quorum
%% is no longer reachable.
t_rebalance_tolerate_permanently_lost_quorum(Config) ->
    Nodes = [N1, N2, N3, N4] = ?config(nodes, Config),
    [_NS1, NS2 | _] = ?config(nodespecs, Config),

    NMsgs = 10,
    Clients = [<<"C1">>, <<"C2">>, <<"C3">>, <<"C4">>, <<"C5">>],
    {MsgStream, TopicStreams} =
        emqx_ds_test_helpers:interleaved_topic_messages(?FUNCTION_NAME, Clients, NMsgs),

    %% Find out which sites are there.
    [S1, S2, S3, S4] = [ds_repl_meta(N, this_site) || N <- Nodes],

    ?check_trace(
        #{timetrap => 30_000},
        begin
            %% Start and initialize DB on all 4 nodes.
            Opts = opts(Config, #{n_shards => 2, n_sites => 4, replication_factor => 4}),
            emqx_ds_raft_test_helpers:assert_db_open(Nodes, ?DB, Opts),
            ?ON(N1, emqx_ds_builtin_raft_meta:print_status()),

            %% Store a bunch of messages.
            {Msgs1, MsgStream1} = emqx_utils_stream:consume(20, MsgStream),
            emqx_ds_raft_test_helpers:apply_stream(?DB, [N1, N2], emqx_utils_stream:list(Msgs1)),

            %% Stop N2.
            ok = emqx_cth_cluster:stop_node(N2),
            ?retry(200, 5, [N1, N3, N4] = ?ON(N1, mria:cluster_nodes(running))),

            %% Store another bunch of messages.
            {Msgs2, MsgStream2} = emqx_utils_stream:consume(20, MsgStream1),
            emqx_ds_raft_test_helpers:apply_stream(?DB, [N1], emqx_utils_stream:list(Msgs2)),

            %% Stop N3 and N4 and expunge them out of the cluster.
            ok = emqx_cth_cluster:stop([N3, N4]),
            ?retry(200, 5, ?assertSameSet([N1], ?ON(N1, mria:cluster_nodes(running)))),
            ok = ?ON(N1, emqx_cluster:force_leave(N3)),
            ok = ?ON(N1, emqx_cluster:force_leave(N4)),
            ?retry(200, 5, ?assertSameSet([N1, N2], ?ON(N1, mria:cluster_nodes(cores)))),

            %% Tell the cluster that S3 is not responsible for the data anymore.
            %% Since that can lead to transitions involving other lost sites, it should
            %% be rejected.
            ?assertEqual(
                {error, {lost_sites, [S4]}},
                ds_repl_meta(N1, leave_db_site, [?DB, S3])
            ),

            %% Tell the cluster that both S3, S4 are not responsible for the data anymore.
            ?assertEqual(
                {ok, [S1, S2]},
                ds_repl_meta(N1, assign_db_sites, [?DB, [S1, S2]])
            ),

            %% Force-forget kicks in, but refuses to proceed since S2 is down, it's too
            %% unsafe.
            {ok, _} = ?block_until(#{
                ?snk_kind := "Forgetting shard replica failed",
                reason := {member_overview_unavailable, [{_Server, N2}]}
            }),

            %% Restart N2. There's no quorum, so N2 will keep lagging until then.
            %% Have to drop mnesia, otherwise N2 coming back online can actually bring
            %% N3 and N4 back into the cluster.
            ok = emqx_cth_suite:clean_work_dir(filename:join(maps:get(work_dir, NS2), mnesia)),
            [N2] = emqx_cth_cluster:start([NS2#{work_dir_dirty => true}]),
            ?assertEqual(ok, ?ON(N2, emqx_ds:open_db(?DB, Opts))),

            %% Force-forgetting should now succeed.
            %% Target state should still be reached eventually.
            ?ON(N1, emqx_ds_raft_test_helpers:wait_db_transitions_done(?DB)),
            ?ON(N1, emqx_ds_builtin_raft_meta:print_status()),
            ?assertSameSet([S1, S2], ds_repl_meta(N1, db_sites, [?DB])),

            %% Messages can now again be persisted successfully.
            emqx_ds_raft_test_helpers:apply_stream(?DB, [N1, N2], MsgStream2),
            %% ...And the original messages still available in the DB.
            emqx_ds_raft_test_helpers:verify_stream_effects(
                ?DB, ?FUNCTION_NAME, Nodes, TopicStreams
            ),

            %% Attempt to forget lost sites should succeed.
            ?assertEqual(ok, ?ON(N1, emqx_ds_builtin_raft_meta:forget_site(S3))),
            ?assertEqual(ok, ?ON(N1, emqx_ds_builtin_raft_meta:forget_site(S4))),

            ?ON(N1, emqx_ds_builtin_raft_meta:shards(?DB))
        end,
        [
            check_no_transition_crashes(),
            check_membership_consistent(?DB, [N1, N2]),
            {"Each shard had lost replicas removed / force-forgotten", fun(Shards, Trace) ->
                Events = lists:append([
                    ?of_kind("Unresponsive shard replica forcefully forgotten", Trace),
                    ?of_kind("Unresponsive shard replica removed", Trace)
                ]),
                ?assertMatch(
                    %% Accounting for concurrent removal attempts:
                    [[_, _ | _], [_, _ | _]],
                    [[E || E = #{shard := SE} <- Events, SE =:= S] || S <- Shards]
                )
            end},
            {"Each shard has seen dirty appends", fun(Shards, Trace) ->
                Events = ?of_kind(emqx_ds_storage_layer_prepare_kv_tx, Trace),
                ?assertSameSet(
                    [{?DB, S} || S <- Shards],
                    lists:usort(?projection(shard, Events))
                )
            end},
            {"Only N1 performed force-forgets", fun(Trace) ->
                %% Servers only on N1 should have been responsible for "force-forgetting",
                %% because their log is ahead (assuming previous check evaluated to true).
                Events = ?of_kind(emqx_ds_replshard_forgot_member, Trace),
                ?assertMatch(
                    [],
                    [E || #{server := {_Server, N}} = E <- Events, N =/= N1]
                )
            end}
        ]
    ).

t_drop_generation(Config) ->
    Apps = [appspec(emqx_durable_storage), emqx_ds_builtin_raft],
    [_, _, NS3] =
        NodeSpecs = emqx_cth_cluster:mk_nodespecs(
            [
                {t_drop_generation1, #{apps => Apps}},
                {t_drop_generation2, #{apps => Apps}},
                {t_drop_generation3, #{apps => Apps}}
            ],
            #{
                work_dir => ?config(work_dir, Config)
            }
        ),
    ?check_trace(
        #{timetrap => 30_000},
        begin
            Nodes = [N1, _, N3] = emqx_cth_cluster:start(NodeSpecs),
            try
                %% Initialize DB on all 3 nodes.
                Opts = opts(Config, #{n_shards => 1, n_sites => 3, replication_factor => 3}),
                emqx_ds_raft_test_helpers:assert_db_open(Nodes, ?DB, Opts),
                %% Create a generation while all nodes are online:
                ?ON(N1, ?assertMatch(ok, emqx_ds:add_generation(?DB))),
                ct:sleep(200),
                ?ON(
                    Nodes,
                    ?assertEqual(
                        [{<<"0">>, 1}, {<<"0">>, 2}],
                        maps:keys(emqx_ds:list_slabs(?DB, #{errors => crash}))
                    )
                ),
                %% Drop generation while all nodes are online:
                ?ON(N1, ?assertMatch(ok, emqx_ds:drop_slab(?DB, {<<"0">>, 1}))),
                ct:sleep(200),
                ?ON(
                    Nodes,
                    ?assertEqual(
                        [{<<"0">>, 2}],
                        maps:keys(emqx_ds:list_slabs(?DB, #{errors => crash}))
                    )
                ),
                %% Stop N3, then create and drop generation when it's offline:
                ok = emqx_cth_cluster:stop_node(N3),
                ct:sleep(1000),
                ?ON(
                    N1,
                    begin
                        ok = emqx_ds:add_generation(?DB),
                        ok = emqx_ds:drop_slab(?DB, {<<"0">>, 2})
                    end
                ),
                %% Restart N3 and verify that it reached the consistent state:
                emqx_cth_cluster:restart(NS3),
                emqx_ds_raft_test_helpers:assert_db_open([N3], ?DB, Opts),
                %% N3 can be in unstable state right now, but it still
                %% must successfully return streams:
                ?ON(
                    Nodes,
                    ?assertEqual([], emqx_ds:get_streams(?DB, ['#'], 0))
                ),
                ?ON(
                    Nodes,
                    ?assertEqual(
                        [{<<"0">>, 3}],
                        maps:keys(emqx_ds:list_slabs(?DB, #{errors => crash}))
                    )
                )
            after
                emqx_cth_cluster:stop(Nodes)
            end
        end,
        fun(Trace) ->
            ?assertMatch([], ?of_kind(ds_storage_layer_failed_to_drop_generation, Trace))
        end
    ).

t_crash_restart_recover(init, Config) ->
    Apps = [appspec(ra), appspec(emqx_durable_storage), appspec(emqx_ds_builtin_raft)],
    Specs = emqx_cth_cluster:mk_nodespecs(
        [
            {t_crash_stop_recover1, #{apps => Apps}},
            {t_crash_stop_recover2, #{apps => Apps}},
            {t_crash_stop_recover3, #{apps => Apps}}
        ],
        #{work_dir => ?config(work_dir, Config)}
    ),
    Nodes = emqx_cth_cluster:start(Specs),
    [{nodes, Nodes}, {nodespecs, Specs} | Config];
t_crash_restart_recover('end', Config) ->
    ok = emqx_cth_cluster:stop(?config(nodes, Config)).

t_crash_restart_recover(Config) ->
    %% This testcase verifies that in the event of abrupt site failure message data is
    %% correctly preserved.
    Nodes = [N1, N2, N3] = ?config(nodes, Config),
    _Specs = [_, NS2, NS3] = ?config(nodespecs, Config),
    DBOpts = opts(Config, #{
        n_shards => 1, n_sites => 3, replication_factor => 3, payload_type => ?ds_pt_mqtt
    }),

    %% Prepare test event stream.
    NMsgs = 400,
    NClients = 8,
    {Stream0, TopicStreams} =
        emqx_ds_test_helpers:interleaved_topic_messages(?FUNCTION_NAME, NClients, NMsgs),
    Stream1 = emqx_utils_stream:interleave(
        [
            {300, Stream0},
            emqx_utils_stream:const(add_generation)
        ],
        false
    ),
    Stream = emqx_utils_stream:interleave(
        [
            {1000, Stream1},
            emqx_utils_stream:list([
                fun() -> kill_restart_node_async(N2, NS2, DBOpts) end,
                fun() -> kill_restart_node_async(N3, NS3, DBOpts) end
            ])
        ],
        true
    ),

    ?check_trace(
        #{timetrap => 60_000},
        begin
            %% Initialize DB on all nodes.
            emqx_ds_raft_test_helpers:assert_db_open(Nodes, ?DB, DBOpts),

            %% Apply the test events, including simulated node crashes.
            NodeStream = emqx_utils_stream:const(N1),
            StartedAt = erlang:monotonic_time(millisecond),
            emqx_ds_raft_test_helpers:apply_stream(?DB, NodeStream, Stream, 0),

            %% It's expected to lose few messages when leaders are abruptly killed.
            MatchFlushFailed = ?match_event(#{?snk_kind := emqx_ds_buffer_flush_failed}),
            {ok, SubRef} = snabbkaffe:subscribe(MatchFlushFailed, NMsgs, _Timeout = 5000, infinity),
            {timeout, Events} = snabbkaffe:receive_events(SubRef),
            LostMessages = [
                emqx_ds_test_helpers:message_canonical_form(M)
             || #{batch := Messages} <- Events, M <- Messages
            ],
            ct:pal("Some messages were lost: ~p", [LostMessages]),
            ?assert(length(LostMessages) < NMsgs div 20),

            %% Wait until crashed nodes are ready.
            SinceStarted = erlang:monotonic_time(millisecond) - StartedAt,
            emqx_ds_raft_test_helpers:wait_db_bootstrapped([N2, N3], ?DB, infinity, SinceStarted),

            %% Verify that all the successfully persisted messages are there.
            VerifyClient = fun({ClientId, ExpectedStream}) ->
                Topic = emqx_ds_test_helpers:client_topic(?FUNCTION_NAME, ClientId),
                ClientNodes = nodes_of_clientid(ClientId, Nodes),
                DSStream1 = ds_topic_stream(ClientId, Topic, hd(ClientNodes)),
                %% Do nodes contain same messages for a client?
                lists:foreach(
                    fun(ClientNode) ->
                        DSStream = ds_topic_stream(ClientId, Topic, ClientNode),
                        ?defer_assert(emqx_ds_test_helpers:diff_messages(DSStream1, DSStream))
                    end,
                    tl(ClientNodes)
                ),
                %% Does any messages were lost unexpectedly?
                DSMessages = emqx_utils_stream:consume(DSStream1),
                ExpectedMessages = emqx_utils_stream:consume(ExpectedStream),
                MissingMessages = emqx_ds_test_helpers:message_set_subtract(
                    ExpectedMessages, DSMessages
                ),
                ?defer_assert(
                    emqx_ds_test_helpers:diff_messages(
                        ExpectedMessages,
                        MissingMessages -- LostMessages
                    )
                )
            end,
            lists:foreach(VerifyClient, TopicStreams)
        end,
        []
    ).

%% This testcase verifies that storage configuration can be updated
%% using `emqx_ds' module and the configuration changes are propagated
%% to the peers. It is expected that each node propagates
%% configuration to replicas of all shards where it is the leader.
%%
%% This testcase also verifies that inconsistent configuration doesn't
%% lead to replica divergence.
t_inconsistent_config_update(init, Config) ->
    Apps = [appspec(ra), appspec(emqx_durable_storage), appspec(emqx_ds_builtin_raft)],
    Specs = emqx_cth_cluster:mk_nodespecs(
        [
            {t_storage_config_change1, #{apps => Apps}},
            {t_storage_config_change2, #{apps => Apps}},
            {t_storage_config_change3, #{apps => Apps}}
        ],
        #{work_dir => ?config(work_dir, Config)}
    ),
    Nodes = emqx_cth_cluster:start(Specs),
    [{nodes, Nodes}, {nodespecs, Specs} | Config];
t_inconsistent_config_update('end', Config) ->
    ok = emqx_cth_cluster:stop(?config(nodes, Config)).
t_inconsistent_config_update(Config) ->
    Nodes = [N1, N2, N3] = ?config(nodes, Config),
    DBOpts = opts(Config, #{
        n_shards => 16, n_sites => 3, replication_factor => 3
    }),
    %% Initial configuration:
    ConfN1 = DBOpts#{
        storage =>
            {emqx_ds_storage_skipstream_lts_v2, #{
                lts_threshold_spec => {simple, {1, inf}}, wildcard_hash_bytes => 8
            }}
    },
    ConfN2 = DBOpts#{
        storage =>
            {emqx_ds_storage_skipstream_lts_v2, #{
                lts_threshold_spec => {simple, {2, inf}}, wildcard_hash_bytes => 9
            }}
    },
    ConfN3 = DBOpts#{
        storage =>
            {emqx_ds_storage_skipstream_lts_v2, #{
                lts_threshold_spec => {simple, {3, inf}}, wildcard_hash_bytes => 10
            }}
    },
    %% New DB configurations to be applied on different nodes:
    NewConfN1 = #{
        storage =>
            {emqx_ds_storage_skipstream_lts_v2, #{
                lts_threshold_spec => {simple, {10, inf}}, wildcard_hash_bytes => 8
            }}
    },
    NewConfN2 = #{
        storage =>
            {emqx_ds_storage_skipstream_lts_v2, #{
                lts_threshold_spec => {simple, {20, inf}}, wildcard_hash_bytes => 9
            }}
    },
    NewConfN3 = #{
        storage =>
            {emqx_ds_storage_skipstream_lts_v2, #{
                lts_threshold_spec => {simple, {30, inf}}, wildcard_hash_bytes => 10
            }}
    },
    ?check_trace(
        #{timetrap => 30_000},
        begin
            %% Initialize DB on all nodes. Initial configuration is
            %% different on all nodes:
            ?assertMatch(
                ok,
                ?ON(N1, emqx_ds:open_db(?DB, ConfN1))
            ),
            ?assertMatch(
                ok,
                ?ON(N2, emqx_ds:open_db(?DB, ConfN2))
            ),
            ?assertMatch(
                ok,
                ?ON(N3, emqx_ds:open_db(?DB, ConfN3))
            ),
            ?assertMatch(
                [ok, ok, ok],
                ?ON(Nodes, emqx_ds:wait_db(?DB, all, infinity))
            ),
            %% Apply config changes:
            ?assertMatch(
                ok,
                ?ON(N1, emqx_ds:update_db_config(?DB, NewConfN1))
            ),
            ?assertMatch(
                ok,
                ?ON(N2, emqx_ds:update_db_config(?DB, NewConfN2))
            ),
            ?assertMatch(
                ok,
                ?ON(N3, emqx_ds:update_db_config(?DB, NewConfN3))
            ),
            %% Add a generation:
            ?assertMatch(
                ok,
                ?ON(N1, emqx_ds:add_generation(?DB))
            ),
            %% Verify schema consistency
            [
                verify_storage_schema(Nodes, ?DB, Shard)
             || Shard <- ?ON(N1, emqx_ds:list_shards(?DB))
            ]
        end,
        []
    ).

verify_storage_schema([N0 | _], DB, Shard) ->
    [Leader | Replicas] = ?ON(N0, emqx_ds_builtin_raft_shard:servers(DB, Shard, leader_preferred)),
    GetSchema = fun({_Name, Node}) ->
        #{schema := Schema} = ?ON(Node, emqx_ds_storage_layer:print_state({DB, Shard})),
        maps:remove(prototype, Schema)
    end,
    Expected = GetSchema(Leader),
    lists:foreach(
        fun(Replica) ->
            ?assertEqual(
                Expected,
                GetSchema(Replica),
                #{
                    shard => Shard,
                    leader => Leader,
                    replica => Replica
                }
            )
        end,
        Replicas
    ).

nodes_of_clientid(ClientId, Nodes) ->
    emqx_ds_raft_test_helpers:nodes_of_clientid(?DB, ClientId, Nodes).

ds_topic_stream(ClientId, ClientTopic, Node) ->
    emqx_ds_raft_test_helpers:ds_topic_stream(?DB, ClientId, ClientTopic, Node).

kill_restart_node_async(Node, Spec, DBOpts) ->
    erlang:spawn_link(?MODULE, kill_restart_node, [Node, Spec, DBOpts]).

kill_restart_node(Node, Spec, DBOpts) ->
    ok = emqx_cth_peer:kill(Node),
    ?tp(test_cluster_node_killed, #{node => Node}),
    _ = emqx_cth_cluster:restart(Spec),
    ok = erpc:call(Node, emqx_ds, open_db, [?DB, DBOpts]).

%%

check_no_transition_crashes() ->
    {"No unexpected shard transition crashes", fun(Trace) ->
        ?assertEqual([], ?of_kind("Shard membership transition failed", Trace))
    end}.

check_membership_consistent(DB, Nodes) ->
    {"Shard membership consistent with shard metadata", fun(_Trace) ->
        lists:foreach(
            fun(N) ->
                Shards = ?ON(N, emqx_ds_builtin_raft_meta:my_shards(DB)),
                ShardServers = ?ON(N, [
                    emqx_ds_builtin_raft_shard:shard_servers(DB, S)
                 || S <- Shards
                ]),
                Memberships = ?ON(N, [
                    emqx_ds_builtin_raft_shard:server_info(
                        membership,
                        emqx_ds_builtin_raft_shard:local_server(DB, S)
                    )
                 || S <- Shards
                ]),
                ?assertEqual(
                    lists:map(fun lists:sort/1, ShardServers),
                    lists:map(fun lists:sort/1, Memberships),
                    {"Shard membership inconsistent", DB, N}
                )
            end,
            Nodes
        )
    end}.

%%

ds_repl_meta(Node, Fun) ->
    ds_repl_meta(Node, Fun, []).

ds_repl_meta(Node, Fun, Args) ->
    try
        erpc:call(Node, emqx_ds_builtin_raft_meta, Fun, Args)
    catch
        EC:Err:Stack ->
            ct:pal("emqx_ds_builtin_raft_meta:~p(~p) @~p failed:~n~p:~p~nStack: ~p", [
                Fun, Args, Node, EC, Err, Stack
            ]),
            error(meta_op_failed)
    end.

shards_online(Node, DB) ->
    ?ON(Node, emqx_ds_builtin_raft_db_sup:which_shards(DB)).

n_shards_online(Node, DB) ->
    length(shards_online(Node, DB)).

message(ClientId, Topic, Payload, PublishedAt) ->
    #message{
        from = ClientId,
        topic = Topic,
        payload = Payload,
        timestamp = PublishedAt,
        id = emqx_guid:gen()
    }.

consume(Node, DB, TopicFilter, StartTime) ->
    erpc:call(Node, emqx_ds_test_helpers, consume, [DB, TopicFilter, StartTime]).

consume_shard(Node, DB, Shard, TopicFilter, StartTime) ->
    erpc:call(Node, emqx_ds_test_helpers, storage_consume, [{DB, Shard}, TopicFilter, StartTime]).

%%

suite() -> [{timetrap, {seconds, 120}}].

all() ->
    Broken = [
        %% 1. Use TTV layout instead of MQTT wrapper. 2. It
        %% should be a property-based test?
        t_crash_restart_recover
    ],
    emqx_common_test_helpers:all(?MODULE) -- Broken.

init_per_testcase(TCName, Config0) ->
    Config1 = [{work_dir, emqx_cth_suite:work_dir(TCName, Config0)} | Config0],
    emqx_common_test_helpers:init_per_testcase(?MODULE, TCName, Config1).

end_per_testcase(TCName, Config) ->
    ok = snabbkaffe:stop(),
    Result = emqx_common_test_helpers:end_per_testcase(?MODULE, TCName, Config),
    catch emqx_ds:drop_db(TCName),
    emqx_cth_suite:clean_work_dir(?config(work_dir, Config)),
    Result.

open_db(DB, Opts) ->
    maybe
        ok ?= emqx_ds:open_db(DB, Opts),
        ok ?= emqx_ds:wait_db(DB, all, infinity)
    end.
