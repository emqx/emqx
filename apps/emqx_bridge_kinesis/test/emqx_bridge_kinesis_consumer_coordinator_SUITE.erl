%%--------------------------------------------------------------------
%% Copyright (c) 2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_kinesis_consumer_coordinator_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-import(emqx_common_test_helpers, [on_exit/1]).

-define(LEADER_NAME, <<"leader_name">>).
-define(STREAM_NAME, <<"stream0">>).

%%------------------------------------------------------------------------------
%% CT boilerplate
%%------------------------------------------------------------------------------

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, _Config) ->
    emqx_common_test_helpers:call_janitor(60_000),
    ok.

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

cluster(TestCase, Config, NodeRoles) ->
    AppSpecs = app_specs(Config),
    Cluster = lists:map(
        fun
            ({N, {Role, Opts}}) ->
                Name = binary_to_atom(
                    <<"kinesis_coordinator_SUITE", (integer_to_binary(N))/binary>>
                ),
                {Name, Opts#{role => Role, apps => AppSpecs}};
            ({N, Role}) ->
                Name = binary_to_atom(
                    <<"kinesis_coordinator_SUITE", (integer_to_binary(N))/binary>>
                ),
                {Name, #{role => Role, apps => AppSpecs}}
        end,
        lists:enumerate(NodeRoles)
    ),
    ClusterOpts = #{work_dir => emqx_cth_suite:work_dir(TestCase, Config)},
    NodeSpecs = emqx_cth_cluster:mk_nodespecs(Cluster, ClusterOpts),
    Nodes = lists:map(fun emqx_cth_cluster:node_name/1, [N || {N, _} <- Cluster]),
    on_exit(fun() -> ok = emqx_cth_cluster:stop(Nodes) end),
    NodeSpecs.

app_specs(_Config) ->
    [
        %% fixme: if this is not added, listener ports fail to be allocated properly...
        emqx,
        emqx_bridge
    ].

cluster_opts(TestCase, Config) ->
    #{work_dir => emqx_cth_suite:work_dir(TestCase, Config)}.

start_cluster(NodeSpecs, Opts) ->
    Nodes = emqx_cth_cluster:start_nodespecs(NodeSpecs),
    Res = erpc:multicall(Nodes, fun() -> setup_node(Opts) end),
    assert_all_erpc_ok(Res),
    Nodes.

setup_node(Opts) ->
    #{shard_ids := ShardIds} = Opts,
    MembershipChangeTimeout = maps:get(membership_change_timeout, Opts, 300),
    RedistributeStaleTimeout = maps:get(redistribute_stale_timeout, Opts, 500),
    ok = emqx_bridge_kinesis_consumer_sup:ensure_supervisor_started(),
    ChildSpec = emqx_bridge_kinesis_consumer_sup:coordinator_spec(
        ?LEADER_NAME,
        #{
            leader_name => ?LEADER_NAME,
            stream_name => ?STREAM_NAME,
            membership_change_timeout => MembershipChangeTimeout,
            redistribute_stale_timeout => RedistributeStaleTimeout
        }
    ),
    mock_list_shards(ShardIds),
    {ok, _} = emqx_bridge_kinesis_consumer_sup:start_child(?LEADER_NAME, ChildSpec),
    ok.

mock_list_shards(ShardIds) ->
    catch meck:new(emqx_bridge_kinesis_consumer, [passthrough, no_history, no_link]),
    meck:expect(
        emqx_bridge_kinesis_consumer,
        list_shard_ids,
        fun(?LEADER_NAME, ?STREAM_NAME) -> {ok, ShardIds} end
    ),
    ok.

assert_all_erpc_ok(Res) ->
    ?assert(
        lists:all(
            fun
                ({ok, ok}) ->
                    true;
                ({ok, {ok, _}}) ->
                    true;
                (_) ->
                    false
            end,
            Res
        ),
        #{result => Res}
    ).

stop_coordinator() ->
    ok = emqx_bridge_kinesis_consumer_sup:ensure_child_deleted(?LEADER_NAME),
    ok.

shard_ids(N) ->
    lists:map(fun integer_to_binary/1, lists:seq(1, N)).

wait_coordinators_stabilized(Nodes) ->
    ?retry(
        _Interval = 200,
        _NAttempts = 20,
        begin
            NumNodes = length(Nodes),
            States = [State || {_Pid, {State, _Data}} <- get_coordinator_states(Nodes)],
            NumNodes = length(States),
            [] = lists:filter(fun(S) -> S =:= candidate end, States)
        end
    ).

assert_exactly_one_leader(Nodes) ->
    States = [{node(Pid), State} || {Pid, {State, _Data}} <- get_coordinator_states(Nodes)],
    Grouped = maps:groups_from_list(
        fun({_N, State}) -> State end, fun({N, _State}) -> N end, States
    ),
    ?assertMatch(#{leader := [_]}, Grouped, #{states => Grouped}),
    ?assertMatch([], maps:get(candidate, Grouped, []), #{states => Grouped}),
    ExpectedNumFollowers = length(Nodes) - 1,
    ?assertEqual(ExpectedNumFollowers, length(maps:get(follower, Grouped, [])), #{states => Grouped}),
    ok.

get_coordinator_states(Nodes) ->
    Res = erpc:multicall(Nodes, fun() ->
        [{_, Pid, _, _}] = supervisor:which_children(emqx_bridge_kinesis_consumer_sup),
        {State, Data} = sys:get_state(Pid),
        {Pid, {State, Data}}
    end),
    %% assert
    lists:map(fun({ok, R}) -> R end, Res).

inspect_trace(Trace) ->
    ct:pal("trace:\n  ~p", [Trace]),
    ok.

get_all_shard_assignments() ->
    {atomic, Res} = emqx_bridge_kinesis_consumer:transaction(fun() ->
        emqx_bridge_kinesis_consumer:get_all_shard_assignments(?LEADER_NAME)
    end),
    Res.

set_shard_assignments(NodeAssignments) ->
    {atomic, ok} =
        emqx_bridge_kinesis_consumer:transaction(fun() ->
            emqx_bridge_kinesis_consumer:set_shard_assignments(
                ?LEADER_NAME, NodeAssignments
            )
        end),
    ok.

wait_shard_assignment_ok(Nodes, ShardIds) ->
    [N | _] = Nodes,
    ?retry(
        _Interval = 500,
        _NAttempts = 20,
        begin
            Assignments = erpc:call(N, fun get_all_shard_assignments/0),
            %% no repetitions nor holes
            ?assertEqual(
                ShardIds,
                lists:sort(lists:concat(maps:values(Assignments))),
                #{assignments => Assignments}
            ),
            %% all nodes have been taken into account
            ?assertEqual(
                lists:sort(Nodes),
                lists:sort(maps:keys(Assignments)),
                #{
                    assignments => Assignments
                }
            ),
            Assignments
        end
    ).

wait_nodeup(Node) ->
    ?retry(
        _Sleep0 = 500,
        _Attempts0 = 50,
        pong = net_adm:ping(Node)
    ).

start_dirty_node(NSpec, Opts) ->
    Name = maps:get(name, NSpec),
    Apps = maps:get(apps, NSpec),
    Node = emqx_cth_cluster:start_bare_node(Name, NSpec),
    ok = erpc:call(Node, emqx_cth_suite, load_apps, [Apps]),
    _ = erpc:call(Node, emqx_cth_suite, start_apps, [Apps, NSpec]),
    ok = emqx_cth_cluster:set_node_opts(Node, NSpec),
    ok = snabbkaffe:forward_trace(Node),
    ok = emqx_cth_cluster:maybe_join_cluster(Node, NSpec),
    ok = erpc:call(Node, fun() -> setup_node(Opts) end),
    ok.

%%------------------------------------------------------------------------------
%% Testcases
%%------------------------------------------------------------------------------

t_resolve_name_clash(Config) ->
    NodeOpts = #{
        join_to => undefined,
        extra_erl_flags => "-kernel prevent_overlapping_partitions false"
    },
    NSpecs =
        cluster(
            ?FUNCTION_NAME,
            Config,
            [
                {core, NodeOpts},
                {core, NodeOpts}
            ]
        ),
    ShardIds = shard_ids(5),
    Opts = #{shard_ids => ShardIds},
    ?check_trace(
        begin
            [N1, N2] = Nodes = start_cluster(NSpecs, Opts),
            wait_coordinators_stabilized(Nodes),
            assert_exactly_one_leader(Nodes),
            ?tp(partitioning, #{}),
            ok = erpc:cast(N1, erlang, disconnect_node, [N2]),
            ok = erpc:cast(N2, erlang, disconnect_node, [N1]),
            wait_coordinators_stabilized([N1]),
            wait_coordinators_stabilized([N2]),
            States0 = [S || {_P, {S, _D}} <- get_coordinator_states(Nodes)],
            ?assertEqual([leader, leader], States0),
            ?tp(merging_clusters, #{}),
            {pong, {ok, _}} =
                ?wait_async_action(
                    erpc:call(N2, net_adm, ping, [N1]),
                    #{?snk_kind := "kinesis_coordinator_step_down"},
                    3_000
                ),
            wait_coordinators_stabilized(Nodes),
            assert_exactly_one_leader(Nodes),
            ok
        end,
        [fun ?MODULE:inspect_trace/1]
    ),
    ok.

t_replicant_elected(Config) ->
    NSpecs = cluster(?FUNCTION_NAME, Config, [core, replicant]),
    do_t_node_elected(NSpecs).

t_core_elected(Config) ->
    NSpecs = cluster(?FUNCTION_NAME, Config, [core, core]),
    do_t_node_elected(NSpecs).

do_t_node_elected(NSpecs) ->
    [NSpec1, NSpec2] = NSpecs,
    ShardIds = shard_ids(5),
    Opts = #{shard_ids => ShardIds},
    ?check_trace(
        begin
            [N1] = start_cluster([NSpec1], Opts),
            wait_coordinators_stabilized([N1]),
            assert_exactly_one_leader([N1]),
            [N2] = start_cluster([NSpec2], Opts),
            Nodes = [N1, N2],
            wait_coordinators_stabilized(Nodes),
            assert_exactly_one_leader(Nodes),
            [
                {LeaderPid, {leader, _LData}},
                {FollowerPid, {follower, _FData}}
            ] = get_coordinator_states(Nodes),
            ?force_ordering(
                #{?snk_kind := "kinesis_coordinator_elected", leader := FollowerPid},
                #{
                    ?snk_kind := "kinesis_coordinator_state_enter",
                    current_state := candidate,
                    ?snk_meta := #{pid := P}
                } when P =/= FollowerPid
            ),
            {_, {ok, _}} =
                ?wait_async_action(
                    begin
                        ?tp(killing_leader, #{leader => LeaderPid, follower => FollowerPid}),
                        Ref = monitor(process, LeaderPid),
                        exit(LeaderPid, kill),
                        receive
                            {'DOWN', Ref, process, LeaderPid, killed} -> ok
                        after 1_000 -> ct:fail("leader didn't die")
                        end,
                        ?tp(killied_leader, #{leader => LeaderPid, follower => FollowerPid}),
                        ok
                    end,
                    #{?snk_kind := "kinesis_coordinator_elected"},
                    20_000
                ),
            ?assertMatch([{FollowerPid, {leader, _}}], get_coordinator_states([N2])),
            assert_exactly_one_leader(Nodes),
            ok
        end,
        [fun ?MODULE:inspect_trace/1]
    ),
    ok.

t_core_join_leave_join(Config) ->
    Role = core,
    NSpecs = cluster(?FUNCTION_NAME, Config, [core, replicant, Role]),
    do_t_join_leave_join(Config, NSpecs, Role).

t_replicant_join_leave_join(Config) ->
    Role = replicant,
    NSpecs = cluster(?FUNCTION_NAME, Config, [core, replicant, Role]),
    do_t_join_leave_join(Config, NSpecs, Role).

do_t_join_leave_join(_Config, NSpecs, Role) ->
    [NSpec1, NSpec2, NSpec3] = NSpecs,
    NodeAddTimeout = 10_000,
    NodeRemoveTimeout = 10_000,
    ShardIds = shard_ids(5),
    Opts = #{shard_ids => ShardIds},
    ?check_trace(
        begin
            {[N1, _N2] = Nodes0, {ok, _}} =
                ?wait_async_action(
                    start_cluster([NSpec1, NSpec2], Opts),
                    #{?snk_kind := "kinesis_coordinator_redistributed"},
                    20_000
                ),
            wait_coordinators_stabilized(Nodes0),
            assert_exactly_one_leader(Nodes0),
            wait_shard_assignment_ok(Nodes0, ShardIds),

            %% new node joins
            {[N3], {ok, _}} =
                ?wait_async_action(
                    start_cluster([NSpec3], Opts),
                    #{?snk_kind := "kinesis_coordinator_nodes_added", new_nodes := [_]},
                    NodeAddTimeout
                ),
            Nodes1 = [N3 | Nodes0],
            wait_coordinators_stabilized(Nodes1),
            assert_exactly_one_leader(Nodes1),
            Assignments0 = wait_shard_assignment_ok(Nodes1, ShardIds),
            N3SIds0 = maps:get(N3, Assignments0, []),
            %% it got some assignments
            ?assertMatch([_ | _], N3SIds0, #{assignments => Assignments0}),

            %% now it leaves
            {ok, {ok, _}} =
                ?wait_async_action(
                    begin
                        ok = erpc:call(N3, ekka, leave, []),
                        case Role of
                            core ->
                                %% When a node leaves, the applications are restarted.  So it's safe to
                                %% assume the coordinator will go down during that process.
                                ok = erpc:call(N3, fun stop_coordinator/0);
                            replicant ->
                                emqx_cth_cluster:stop_node(N3)
                        end,
                        ok
                    end,
                    #{?snk_kind := "kinesis_coordinator_nodes_removed"},
                    NodeRemoveTimeout
                ),
            assert_exactly_one_leader(Nodes0),
            Assignments1 = wait_shard_assignment_ok(Nodes0, ShardIds),
            ?assertNot(is_map_key(N3, Assignments1), #{assignments => Assignments1}),

            %% join again
            {ok, {ok, _}} =
                ?wait_async_action(
                    case Role of
                        replicant ->
                            start_dirty_node(NSpec3, Opts);
                        core ->
                            ok = erpc:call(N3, ekka, join, [N1]),
                            ok = erpc:call(N3, fun() -> setup_node(Opts) end)
                    end,
                    #{?snk_kind := "kinesis_coordinator_nodes_added"},
                    NodeAddTimeout
                ),
            assert_exactly_one_leader(Nodes1),
            Assignments2 = wait_shard_assignment_ok(Nodes1, ShardIds),
            N3SIds1 = maps:get(N3, Assignments2, []),
            ?assertMatch([_ | _], N3SIds1, #{assignments => Assignments2}),

            ok
        end,
        [fun ?MODULE:inspect_trace/1]
    ),
    ok.

t_unexpected_shards(Config) ->
    NSpecs = cluster(?FUNCTION_NAME, Config, [core, replicant]),
    ShardIds = shard_ids(5),
    Opts = #{shard_ids => ShardIds},
    ?check_trace(
        begin
            {[N1, N2] = Nodes, {ok, _}} =
                ?wait_async_action(
                    start_cluster(NSpecs, Opts),
                    #{?snk_kind := "kinesis_coordinator_redistributed"},
                    20_000
                ),
            wait_coordinators_stabilized(Nodes),
            assert_exactly_one_leader(Nodes),
            ?force_ordering(
                #{?snk_kind := inserted_unexpected},
                #{?snk_kind := "kinesis_coordinator_redistributed"}
            ),
            UnexpectedSId = <<"unexpected">>,
            {_, {ok, _}} =
                ?wait_async_action(
                    begin
                        NodeAssignments0 = erpc:call(N1, fun get_all_shard_assignments/0),
                        NodeAssignments = maps:map(
                            fun(_Node, SIds) -> [UnexpectedSId | SIds] end, NodeAssignments0
                        ),
                        ok = erpc:call(N2, fun() -> set_shard_assignments(NodeAssignments) end),
                        ?tp(inserted_unexpected, #{})
                    end,
                    #{?snk_kind := "kinesis_coordinator_redistributed"},
                    20_000
                ),
            FinalNodeAssignments = erpc:call(N1, fun get_all_shard_assignments/0),
            ?assertNot(
                lists:any(
                    fun(SIds) -> lists:member(UnexpectedSId, SIds) end,
                    maps:values(FinalNodeAssignments)
                ),
                #{node_assignments => FinalNodeAssignments}
            ),
            ok
        end,
        [fun ?MODULE:inspect_trace/1]
    ),
    ok.
