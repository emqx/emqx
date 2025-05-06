%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_ds_builtin_raft_metrics_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include("../../emqx/include/emqx.hrl").
-include("../../emqx/include/asserts.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-define(DB, mtestdb).

-define(ON(NODES, BODY),
    emqx_ds_test_helpers:on(NODES, fun() -> BODY end)
).

opts(Overrides) ->
    emqx_utils_maps:deep_merge(
        #{
            backend => builtin_raft,
            storage => {emqx_ds_storage_skipstream_lts, #{}},
            n_shards => 8,
            n_sites => 1,
            replication_factor => 3,
            replication_options => #{
                wal_max_size_bytes => 4096,
                wal_max_batch_size => 1024,
                snapshot_interval => 128
            }
        },
        Overrides
    ).

appspec(emqx_durable_storage) ->
    {emqx_durable_storage, #{override_env => [{egress_flush_interval, 1}]}};
appspec(emqx_ds_builtin_raft) ->
    {emqx_ds_builtin_raft, #{}}.

t_empty_metrics(init, Config) ->
    AppSpecs = [appspec(emqx_durable_storage), appspec(emqx_ds_builtin_raft)],
    Apps = emqx_cth_suite:start(AppSpecs, #{work_dir => ?config(work_dir, Config)}),
    [{apps, Apps} | Config];
t_empty_metrics('end', Config) ->
    ok = emqx_cth_suite:stop(?config(apps, Config)).

t_empty_metrics(_Config) ->
    ?assertEqual(
        #{cluster_sites_num => [{[{status, any}], 1}, {[{status, lost}], 0}]},
        emqx_ds_builtin_raft_metrics:cluster()
    ),
    ?assertEqual(#{}, emqx_ds_builtin_raft_metrics:dbs()),
    ?assertEqual(#{}, emqx_ds_builtin_raft_metrics:shards()).

t_cluster_metrics(init, Config) ->
    Apps = [appspec(emqx_durable_storage), appspec(emqx_ds_builtin_raft)],
    NodeSpecs = emqx_cth_cluster:mk_nodespecs(
        [
            {t_cluster_metrics1, #{apps => Apps}},
            {t_cluster_metrics2, #{apps => Apps}},
            {t_cluster_metrics3, #{apps => Apps}}
        ],
        #{work_dir => ?config(work_dir, Config)}
    ),
    ok = snabbkaffe:start_trace(),
    [{specs, NodeSpecs} | Config];
t_cluster_metrics('end', Config) ->
    ok = emqx_cth_cluster:stop(?config(specs, Config)),
    ok = snabbkaffe:stop().

t_cluster_metrics(Config) ->
    %% Start first 2 nodes.
    [NS1, NS2, NS3] = ?config(specs, Config),
    Nodes0 = [N1, _] = emqx_cth_cluster:start([NS1, NS2]),
    %% Initialize DB on first 2 nodes and wait for it to be online.
    Opts = opts(#{n_shards => 8, n_sites => 2, replication_factor => 3}),
    emqx_ds_raft_test_helpers:assert_db_open(Nodes0, ?DB, Opts),
    %% Start the last node, it won't obviously participate in DB replication.
    [N3] = emqx_cth_cluster:start([NS3]),
    %% Verify cluster metrics.
    MCluster = ?ON(N1, emqx_ds_builtin_raft_metrics:cluster()),
    ?assertEqual(
        #{cluster_sites_num => [{[{status, any}], 3}, {[{status, lost}], 0}]},
        MCluster
    ),
    %% Verify DBs metrics.
    ?assertEqual(
        #{
            db_shards_num => [{[{db, ?DB}], 8}],
            db_sites_num => [
                {[{status, current}, {db, ?DB}], 2},
                {[{status, assigned}, {db, ?DB}], 2}
            ]
        },
        ?ON(N1, emqx_ds_builtin_raft_metrics:dbs())
    ),
    %% Verify shard metrics.
    ?assertMatch(
        #{
            shard_replication_factor := [
                {[{db, ?DB}, {shard, <<"0">>}], 2},
                {[{db, ?DB}, {shard, <<"1">>}], 2}
                | _
            ] = SRF,
            shard_transition_queue_len := [
                {[{type, add}, {db, ?DB}, {shard, <<"0">>}], 0},
                {[{type, del}, {db, ?DB}, {shard, <<"0">>}], 0},
                {[{type, add}, {db, ?DB}, {shard, <<"1">>}], 0},
                {[{type, del}, {db, ?DB}, {shard, <<"1">>}], 0}
                | _
            ] = STQ
        } when length(SRF) == 8 andalso length(STQ) == 8 * 2,
        ?ON(N1, emqx_ds_builtin_raft_metrics:shards())
    ),

    %% All nodes are expected to report same metrics.
    Nodes = Nodes0 ++ [N3],
    ?assertEqual(
        [MCluster || _ <- Nodes],
        ?ON(Nodes, emqx_ds_builtin_raft_metrics:cluster())
    ),

    %% Join N3 to DB replica sets.
    %% No shard transitions will complete because DB is not started there yet.
    S3 = ?ON(N3, emqx_ds_builtin_raft_meta:this_site()),
    {ok, [_, _, _]} = ?ON(N3, emqx_ds_builtin_raft_meta:join_db_site(?DB, S3)),

    %% Verify DB metrics changed.
    ?assertMatch(
        #{
            db_sites_num := [
                {[{status, current}, {db, ?DB}], 2},
                {[{status, assigned}, {db, ?DB}], 3}
            ]
        },
        ?ON(N1, emqx_ds_builtin_raft_metrics:dbs())
    ),
    ?assertMatch(
        #{
            shard_transition_queue_len := [
                {[{type, add}, {db, ?DB}, {shard, <<"0">>}], 1},
                {[{type, del}, {db, ?DB}, {shard, <<"0">>}], 0},
                {[{type, add}, {db, ?DB}, {shard, <<"1">>}], 1},
                {[{type, del}, {db, ?DB}, {shard, <<"1">>}], 0}
                | _
            ]
        },
        ?ON(N1, emqx_ds_builtin_raft_metrics:shards())
    ),
    ?assertMatch(
        [
            #{db_shards_online_num := [{[{db, ?DB}], 8}]},
            #{db_shards_online_num := [{[{db, ?DB}], 8}]},
            %% No DBs / shards are online there yet:
            #{} = N3Ms
        ] when not is_map_key(db_shards_online_num, N3Ms),
        ?ON(Nodes, emqx_ds_builtin_raft_metrics:local_dbs([]))
    ),

    %% Spin DB up and wait until transitions are complete.
    emqx_ds_raft_test_helpers:assert_db_open([N3], ?DB, Opts),
    ?assertMatch(
        #{db_shards_online_num := [{[{db, ?DB}], N}]} when N < 8,
        ?ON(N3, emqx_ds_builtin_raft_metrics:local_dbs([]))
    ),

    ?ON(N1, emqx_ds_raft_test_helpers:wait_db_transitions_done(?DB)),

    %% Verify DB metrics changed again.
    ?assertMatch(
        #{
            db_sites_num := [
                {[{status, current}, {db, ?DB}], 3},
                {[{status, assigned}, {db, ?DB}], 3}
            ]
        },
        ?ON(N1, emqx_ds_builtin_raft_metrics:dbs())
    ),
    ?assertMatch(
        [
            #{db_shards_online_num := [{[{db, ?DB}], 8}]},
            #{db_shards_online_num := [{[{db, ?DB}], 8}]},
            #{db_shards_online_num := [{[{db, ?DB}], 8}]}
        ],
        ?ON(Nodes, emqx_ds_builtin_raft_metrics:local_dbs([]))
    ).

t_transitions_metrics(init, Config) ->
    Apps = [appspec(emqx_durable_storage), appspec(emqx_ds_builtin_raft)],
    NodeSpecs = emqx_cth_cluster:mk_nodespecs(
        [
            {t_transitions_metrics1, #{apps => Apps}},
            {t_transitions_metrics2, #{apps => Apps}},
            {t_transitions_metrics3, #{apps => Apps}}
        ],
        #{work_dir => ?config(work_dir, Config)}
    ),
    Nodes = emqx_cth_cluster:start(NodeSpecs),
    ok = snabbkaffe:start_trace(),
    [{cluster, Nodes}, {specs, NodeSpecs} | Config];
t_transitions_metrics('end', Config) ->
    ok = emqx_cth_cluster:stop(?config(cluster, Config)),
    ok = snabbkaffe:stop().

t_transitions_metrics(Config) ->
    %% Initialize DB on cluster nodes and wait for it to be online.
    Nodes = [N1, N2, N3] = ?config(cluster, Config),
    Opts = opts(#{n_shards => 4, n_sites => 3, replication_factor => 3}),
    emqx_ds_raft_test_helpers:assert_db_open(Nodes, ?DB, Opts),

    [S1, S2, S3] = ?ON(Nodes, emqx_ds_builtin_raft_meta:this_site()),

    %% Initially, DB should be replicated across all 3 sites.
    ?assertMatch(
        #{
            db_sites_num := [{[{status, current}, {db, ?DB}], 3} | _]
        },
        ?ON(N1, emqx_ds_builtin_raft_metrics:dbs())
    ),

    %% Absolve S1 from DB replication.
    {ok, _} = ?ON(N1, emqx_ds_builtin_raft_meta:assign_db_sites(?DB, [S2, S3])),
    ?ON(N1, emqx_ds_raft_test_helpers:wait_db_transitions_done(?DB)),

    %% Simulate occasional transition crashes.
    ?inject_crash(
        #{?snk_kind := dsrepl_shard_transition_begin},
        snabbkaffe_nemesis:recover_after(3)
    ),

    %% Changed my mind, bring back S1 and remove S3.
    {ok, _} = ?ON(N1, emqx_ds_builtin_raft_meta:assign_db_sites(?DB, [S1, S2])),
    ?ON(N1, emqx_ds_raft_test_helpers:wait_db_transitions_done(?DB)),

    [
        #{shard_transitions := N1MTransitions1},
        #{shard_transitions := N2MTransitions1},
        #{shard_transitions := N3MTransitions1}
    ] = ?ON(Nodes, emqx_ds_builtin_raft_metrics:local_shards([])),

    %% N1 performed both leave and join for each shard.
    ?assertMatch(
        [
            {[{type, add}, {status, started}, {db, ?DB}, {shard, <<"0">>}], 1},
            {[{type, del}, {status, started}, {db, ?DB}, {shard, <<"0">>}], 1},
            {[{type, add}, {status, completed}, {db, ?DB}, {shard, <<"0">>}], 1},
            {[{type, del}, {status, completed}, {db, ?DB}, {shard, <<"0">>}], 1},
            {[{type, add}, {status, skipped}, {db, ?DB}, {shard, <<"0">>}], 0},
            {[{type, del}, {status, skipped}, {db, ?DB}, {shard, <<"0">>}], 0},
            {[{type, add}, {status, crashed}, {db, ?DB}, {shard, <<"0">>}], _},
            {[{type, del}, {status, crashed}, {db, ?DB}, {shard, <<"0">>}], _},
            {[{type, add}, {status, started}, {db, ?DB}, {shard, <<"1">>}], 1},
            {[{type, del}, {status, started}, {db, ?DB}, {shard, <<"1">>}], 1},
            {[{type, add}, {status, completed}, {db, ?DB}, {shard, <<"1">>}], 1},
            {[{type, del}, {status, completed}, {db, ?DB}, {shard, <<"1">>}], 1}
            | _
        ],
        N1MTransitions1
    ),

    %% N2 was not involved in replica set transitions.
    ?assertMatch(
        [
            {[{type, add}, {status, started}, {db, ?DB}, {shard, <<"0">>}], 0},
            {[{type, del}, {status, started}, {db, ?DB}, {shard, <<"0">>}], 0},
            {[{type, add}, {status, completed}, {db, ?DB}, {shard, <<"0">>}], 0},
            {[{type, del}, {status, completed}, {db, ?DB}, {shard, <<"0">>}], 0},
            {[{type, add}, {status, skipped}, {db, ?DB}, {shard, <<"0">>}], 0},
            {[{type, del}, {status, skipped}, {db, ?DB}, {shard, <<"0">>}], 0},
            {[{type, add}, {status, crashed}, {db, ?DB}, {shard, <<"0">>}], 0},
            {[{type, del}, {status, crashed}, {db, ?DB}, {shard, <<"0">>}], 0}
            | _
        ],
        N2MTransitions1
    ),

    ?assertMatch(
        [
            {[{type, add}, {status, started}, {db, ?DB}, {shard, <<"0">>}], 0},
            {[{type, del}, {status, started}, {db, ?DB}, {shard, <<"0">>}], 1},
            {[{type, add}, {status, completed}, {db, ?DB}, {shard, <<"0">>}], 0},
            {[{type, del}, {status, completed}, {db, ?DB}, {shard, <<"0">>}], 1},
            {[{type, add}, {status, skipped}, {db, ?DB}, {shard, <<"0">>}], 0},
            {[{type, del}, {status, skipped}, {db, ?DB}, {shard, <<"0">>}], 0},
            {[{type, add}, {status, crashed}, {db, ?DB}, {shard, <<"0">>}], _},
            {[{type, del}, {status, crashed}, {db, ?DB}, {shard, <<"0">>}], _},
            {[{type, add}, {status, started}, {db, ?DB}, {shard, <<"1">>}], 0},
            {[{type, del}, {status, started}, {db, ?DB}, {shard, <<"1">>}], 1},
            {[{type, add}, {status, completed}, {db, ?DB}, {shard, <<"1">>}], 0},
            {[{type, del}, {status, completed}, {db, ?DB}, {shard, <<"1">>}], 1}
            | _
        ],
        N3MTransitions1
    ),

    %% Few crashes should have been observed.
    MTransitions1 = N1MTransitions1 ++ N2MTransitions1 ++ N3MTransitions1,
    ?assertMatch(
        [_ | _],
        [Metric || Metric = {[{type, _}, {status, crashed} | _], N} <- MTransitions1, N > 0]
    ),

    %% Stop both N1 & N2, and ask S3 to join back.
    ok = emqx_cth_cluster:stop([N1, N2]),
    {ok, _} = ?ON(N3, emqx_ds_builtin_raft_meta:assign_db_sites(?DB, [S1, S2, S3])),

    %% We should observe few transition errors, because quorum became unavailable.
    ok = timer:sleep(5_000),
    #{shard_transition_errors := MTransitionErrors} =
        ?ON(N3, emqx_ds_builtin_raft_metrics:local_shards([])),
    ?assertMatch(
        [_ | _],
        [Metric || Metric = {_, N} <- MTransitionErrors, N > 0]
    ).

t_snapshot_metrics(init, Config) ->
    Apps = [appspec(emqx_durable_storage), appspec(emqx_ds_builtin_raft)],
    NodeSpecs = emqx_cth_cluster:mk_nodespecs(
        [
            {t_snapshot_metrics1, #{apps => Apps}},
            {t_snapshot_metrics2, #{apps => Apps}},
            {t_snapshot_metrics3, #{apps => Apps}}
        ],
        #{work_dir => ?config(work_dir, Config)}
    ),
    ok = snabbkaffe:start_trace(),
    [{specs, NodeSpecs} | Config];
t_snapshot_metrics('end', Config) ->
    ok = emqx_cth_cluster:stop(?config(specs, Config)),
    ok = snabbkaffe:stop().

t_snapshot_metrics(Config) ->
    %% Start first node.
    [NS1, NS2, NS3] = ?config(specs, Config),
    [N1] = emqx_cth_cluster:start([NS1]),
    %% Initialize DB on the first node and wait for it to be online.
    Opts = opts(#{n_shards => 2, n_sites => 1, replication_factor => 3}),
    emqx_ds_raft_test_helpers:assert_db_open([N1], ?DB, Opts),
    %% Start rest of the nodes.
    [N2, N3] = emqx_cth_cluster:start([NS2, NS3]),
    emqx_ds_raft_test_helpers:assert_db_open([N2, N3], ?DB, Opts),

    %% Spin up workload large enough to require snapshot transfers later.
    {Stream, _} = emqx_ds_test_helpers:interleaved_topic_messages(?FUNCTION_NAME, 5, 200),
    emqx_ds_raft_test_helpers:apply_stream(?DB, [N1], Stream),
    ok = ?ON(N1, emqx_ds:add_generation(?DB)),

    %% Assign rest of nodes as DB replication sites.
    Nodes = [N1, N2, N3],
    Sites = ?ON(Nodes, emqx_ds_builtin_raft_meta:this_site()),
    {ok, _} = ?ON(N1, emqx_ds_builtin_raft_meta:assign_db_sites(?DB, Sites)),
    ?ON(N1, emqx_ds_raft_test_helpers:wait_db_transitions_done(?DB)),

    %% Verify snapshot metrics are updated.
    [N1MShard, N2MShard, N3MShard] = ?ON(Nodes, emqx_ds_builtin_raft_metrics:local_shards([])),
    ?assertMatch(
        #{
            snapshot_reads := [
                {[{status, started}, {db, ?DB}, {shard, <<"0">>}], 2},
                {[{status, completed}, {db, ?DB}, {shard, <<"0">>}], 2},
                {[{status, started}, {db, ?DB}, {shard, <<"1">>}], 2},
                {[{status, completed}, {db, ?DB}, {shard, <<"1">>}], 2}
            ],
            snapshot_read_errors := [
                {[{db, ?DB}, {shard, <<"0">>}], 0},
                {[{db, ?DB}, {shard, <<"1">>}], 0}
            ],
            snapshot_read_chunks := [
                {[{db, ?DB}, {shard, <<"0">>}], NC0},
                {[{db, ?DB}, {shard, <<"1">>}], _}
            ],
            snapshot_read_chunk_bytes := [
                {[{db, ?DB}, {shard, <<"0">>}], NBytes0},
                {[{db, ?DB}, {shard, <<"1">>}], _}
            ]
        } when NC0 > 2 andalso NBytes0 > 4096 * 2,
        N1MShard
    ),
    ?assertMatch(
        #{
            snapshot_writes := [
                {[{status, started}, {db, ?DB}, {shard, <<"0">>}], 1},
                {[{status, completed}, {db, ?DB}, {shard, <<"0">>}], 1},
                {[{status, started}, {db, ?DB}, {shard, <<"1">>}], 1},
                {[{status, completed}, {db, ?DB}, {shard, <<"1">>}], 1}
            ],
            snapshot_write_errors := [
                {[{db, ?DB}, {shard, <<"0">>}], 0},
                {[{db, ?DB}, {shard, <<"1">>}], 0}
            ],
            snapshot_write_chunks := [
                {[{db, ?DB}, {shard, <<"0">>}], NC0},
                {[{db, ?DB}, {shard, <<"1">>}], _}
            ],
            snapshot_write_chunk_bytes := [
                {[{db, ?DB}, {shard, <<"0">>}], NBytes0},
                {[{db, ?DB}, {shard, <<"1">>}], _}
            ]
        } when NC0 > 1 andalso NBytes0 > 4096,
        N2MShard
    ),

    %% Number of chunks / bytes should be consistent across source / recepient nodes.
    ?assertEqual(
        %% Aggregate of chunks / bytes read:
        #{
            chunks => lists:sum([
                V
             || {_Labels, V} <- maps:get(snapshot_read_chunks, N1MShard)
            ]),
            bytes => lists:sum([
                V
             || {_Labels, V} <- maps:get(snapshot_read_chunk_bytes, N1MShard)
            ])
        },
        %% Aggregate of chunks / bytes written:
        #{
            chunks => lists:sum([
                V
             || Ms <- [N2MShard, N3MShard],
                {_Labels, V} <- maps:get(snapshot_write_chunks, Ms)
            ]),
            bytes => lists:sum([
                V
             || Ms <- [N2MShard, N3MShard],
                {_Labels, V} <- maps:get(snapshot_write_chunk_bytes, Ms)
            ])
        }
    ).

%%

t_replication_metrics(init, Config) ->
    Apps = [appspec(emqx_durable_storage), appspec(emqx_ds_builtin_raft)],
    Nodes = emqx_cth_cluster:start(
        [
            {t_replication_metrics1, #{apps => Apps}},
            {t_replication_metrics2, #{apps => Apps}}
        ],
        #{work_dir => ?config(work_dir, Config)}
    ),
    ok = snabbkaffe:start_trace(),
    [{nodes, Nodes} | Config];
t_replication_metrics('end', Config) ->
    ok = emqx_cth_cluster:stop(?config(nodes, Config)),
    ok = snabbkaffe:stop().

t_replication_metrics(Config) ->
    %% Initialize DB and wait for it to be online.
    Nodes = [N1 | _] = ?config(nodes, Config),
    TS0 = ?ON(N1, emqx_ds:timestamp_us()),
    Opts = opts(#{n_shards => 2, n_sites => 1, replication_factor => 3}),
    emqx_ds_raft_test_helpers:assert_db_open(Nodes, ?DB, Opts),
    %% Spin up non-trivial workload.
    {Stream, _} = emqx_ds_test_helpers:interleaved_topic_messages(?FUNCTION_NAME, 25, 80),
    emqx_ds_raft_test_helpers:apply_stream(?DB, Nodes, Stream),
    ok = ?ON(N1, emqx_ds:add_generation(?DB)),
    ok = timer:sleep(1_000),
    %% Verify metrics
    ?assertMatch(
        #{
            current_timestamp_us := [
                {[{db, ?DB}, {shard, <<"0">>}], TSShard},
                {[{db, ?DB}, {shard, <<"1">>}], _}
            ],
            rasrvs_started := [{[{db, ?DB}, {shard, <<"0">>}], 1} | _],
            rasrvs_terminated := [{[{db, ?DB}, {shard, <<"0">>}], 0} | _],
            rasrv_term := [{[{db, ?DB}, {shard, <<"0">>}], Term} | _],
            rasrv_index := [
                {[{kind, commit}, {db, ?DB}, {shard, <<"0">>}], IdxCommit},
                {[{kind, last_applied}, {db, ?DB}, {shard, <<"0">>}], IdxLastApplied},
                {[{kind, last_written}, {db, ?DB}, {shard, <<"0">>}], _},
                {[{kind, snapshot}, {db, ?DB}, {shard, <<"0">>}], IdxSnapshot}
                | _
            ],
            rasrv_state_changes := [
                {[{state, candidate}, {db, ?DB}, {shard, <<"0">>}], _},
                {[{state, follower}, {db, ?DB}, {shard, <<"0">>}], NFollower},
                {[{state, leader}, {db, ?DB}, {shard, <<"0">>}], _}
                | _
            ],
            rasrv_commands := [{[{db, ?DB}, {shard, <<"0">>}], _} | _],
            rasrv_snapshot_writes := [{[{db, ?DB}, {shard, <<"0">>}], NSnapshots} | _],
            rasrv_replication_msgs := [
                {[{kind, sent}, {db, ?DB}, {shard, <<"0">>}], NReplMsgs},
                {[{kind, dropped}, {db, ?DB}, {shard, <<"0">>}], 0}
                | _
            ]
        } when
            TSShard > TS0 andalso
                Term > 0 andalso
                NFollower > 0 andalso
                NReplMsgs > 0 andalso
                IdxCommit >= IdxLastApplied andalso
                IdxSnapshot > 0 andalso
                NSnapshots > 0,
        ?ON(N1, emqx_ds_builtin_raft_metrics:local_shards([]))
    ).

%%

suite() -> [{timetrap, {seconds, 60}}].

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_testcase(TCName, Config0) ->
    Config = [{work_dir, emqx_cth_suite:work_dir(TCName, Config0)} | Config0],
    emqx_common_test_helpers:init_per_testcase(?MODULE, TCName, Config).

end_per_testcase(TCName, Config) ->
    ok = snabbkaffe:stop(),
    Result = emqx_common_test_helpers:end_per_testcase(?MODULE, TCName, Config),
    catch emqx_ds:drop_db(TCName),
    emqx_cth_suite:clean_work_dir(?config(work_dir, Config)),
    Result.
