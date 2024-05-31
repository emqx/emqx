%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(emqx_foreman_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-import(emqx_common_test_helpers, [on_exit/1]).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-define(SUP, emqx_foreman_sup).
-define(ON(NODE, BODY), erpc:call(NODE, fun() -> BODY end)).

%%------------------------------------------------------------------------------
%% CT Boilerplate
%%------------------------------------------------------------------------------

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, _Config) ->
    emqx_common_test_helpers:call_janitor(),
    snabbkaffe:stop(),
    ok.

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

app_specs(AppOpts) ->
    [
        {emqx_foreman, #{
            after_start => fun() -> setup_node(AppOpts) end
        }}
    ].

mk_node_name(N) ->
    binary_to_atom(<<"foreman_SUITE", (integer_to_binary(N))/binary>>).

mk_cluster(TestCase, Config, NodeRoles) ->
    ExtraVMArgs = ["-kernel", "prevent_overlapping_partitions", "false"],
    Cluster = lists:map(
        fun
            ({N, {Role, NodeOpts, AppOpts}}) ->
                Name = mk_node_name(N),
                {Name, NodeOpts#{
                    role => Role,
                    apps => app_specs(AppOpts),
                    extra_vm_args => ExtraVMArgs
                }};
            ({N, {Role, AppOpts}}) ->
                Name = mk_node_name(N),
                {Name, #{
                    role => Role,
                    apps => app_specs(AppOpts),
                    extra_vm_args => ExtraVMArgs
                }}
        end,
        lists:enumerate(NodeRoles)
    ),
    ClusterOpts = #{
        work_dir => emqx_cth_suite:work_dir(TestCase, Config)
    },
    NodeSpecs = emqx_cth_cluster:mk_nodespecs(Cluster, ClusterOpts),
    Nodes = [emqx_cth_cluster:node_name(N) || {N, _} <- Cluster],
    on_exit(fun() -> ok = emqx_cth_cluster:stop(Nodes) end),
    NodeSpecs.

start_cluster(NodeSpecs) ->
    emqx_cth_cluster:start(NodeSpecs).

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

setup_node(Opts) ->
    #{name := Name} = Opts,
    InitOptsDefault = #{
        name => Name,
        scope => Name,
        compute_allocation_fn => fun(#{members := Members}) ->
            lists:foldl(
                fun({N, Member}, Acc) ->
                    Acc#{Member => N}
                end,
                #{},
                lists:enumerate(lists:sort(Members))
            )
        end,
        on_stage_fn => fun(Ctx) ->
            ?tp(stage, Ctx#{member => node()}),
            ok
        end,
        on_commit_fn => fun(Ctx) ->
            ?tp(commit, Ctx#{member => node()}),
            ok
        end
    },
    InitOpts0 = maps:get(init_opts, Opts, #{}),
    InitOpts = emqx_utils_maps:deep_merge(InitOptsDefault, InitOpts0),
    {ok, _} = ?SUP:start_child(
        #{
            id => foreman,
            start => {emqx_foreman, start_link, [InitOpts]}
        }
    ),
    ok.

inspect_trace(Trace) ->
    ct:pal("trace:\n  ~p", [Trace]),
    ok.

trace_prop_no_unexpected_events() ->
    {"no unexpected events received", fun ?MODULE:trace_prop_no_unexpected_events/1}.
trace_prop_no_unexpected_events(Trace) ->
    ?assertEqual([], ?of_kind("foreman_unexpected_event", Trace)),
    ok.

get_foremen_states(Nodes) ->
    Res = erpc:multicall(Nodes, fun() ->
        [Pid] = [Pid || {foreman, Pid, _, _} <- supervisor:which_children(?SUP)],
        {State, Data} = sys:get_state(Pid),
        {Pid, {State, Data}}
    end),
    %% assert
    lists:map(fun({ok, R}) -> R end, Res).

wait_foremen_stabilized(Nodes) ->
    ?retry(
        _Interval = 200,
        _NAttempts = 20,
        begin
            NumNodes = length(Nodes),
            States = [State || {_Pid, {State, _Data}} <- get_foremen_states(Nodes)],
            NumNodes = length(States),
            [] = lists:filter(fun(S) -> S =:= candidate end, States)
        end
    ).

assert_exactly_one_leader(Nodes) ->
    States = [{node(Pid), State} || {Pid, {State, _Data}} <- get_foremen_states(Nodes)],
    Grouped = maps:groups_from_list(
        fun({_N, State}) -> State end, fun({N, _State}) -> N end, States
    ),
    ?assertMatch(#{leader := [_]}, Grouped, #{states => Grouped}),
    ?assertMatch([], maps:get(candidate, Grouped, []), #{states => Grouped}),
    ExpectedNumFollowers = length(Nodes) - 1,
    ?assertEqual(ExpectedNumFollowers, length(maps:get(follower, Grouped, [])), #{states => Grouped}),
    #{leader := [LeaderNode]} = Grouped,
    ?ON(LeaderNode, ?assertEqual(core, mria_rlog:role())),
    ok.

restart(NodeSpec) ->
    [Node] = emqx_cth_cluster:restart(NodeSpec),
    [Node].

%%------------------------------------------------------------------------------
%% Test cases
%%------------------------------------------------------------------------------

%% Test that, after a split brain happens and later the two cliques are merged, only one
%% leader emerges.
t_resolve_name_clash(Config) ->
    NodeOpts = #{join_to => undefined},
    AppOpts = #{name => ?FUNCTION_NAME},
    NSpecs =
        mk_cluster(
            ?FUNCTION_NAME,
            Config,
            [
                {core, NodeOpts, AppOpts},
                {core, NodeOpts, AppOpts}
            ]
        ),
    ?check_trace(
        begin
            [N1, N2] = Nodes = start_cluster(NSpecs),
            wait_foremen_stabilized(Nodes),
            assert_exactly_one_leader(Nodes),
            ?tp(partitioning, #{}),
            ok = erpc:cast(N1, erlang, disconnect_node, [N2]),
            ok = erpc:cast(N2, erlang, disconnect_node, [N1]),
            wait_foremen_stabilized([N1]),
            wait_foremen_stabilized([N2]),
            States0 = [S || {_P, {S, _D}} <- get_foremen_states(Nodes)],
            %% Split brain
            ?assertEqual([leader, leader], States0),
            ?tp(merging_clusters, #{}),
            {pong, {ok, _}} =
                ?wait_async_action(
                    erpc:call(N2, net_adm, ping, [N1]),
                    #{?snk_kind := "foreman_step_down"},
                    3_000
                ),
            wait_foremen_stabilized(Nodes),
            assert_exactly_one_leader(Nodes),
            ok
        end,
        [
            fun ?MODULE:inspect_trace/1,
            trace_prop_no_unexpected_events()
        ]
    ),
    ok.

%% We limit leaders to core nodes only, to avoid them reading stale data.
t_replicants_are_never_elected(Config) ->
    AppOpts = #{name => ?FUNCTION_NAME, init_opts => #{retry_election_timeout => 500}},
    NSpecs =
        [SpecCore, _SpecRep] =
        mk_cluster(
            ?FUNCTION_NAME,
            Config,
            [
                %% We need one core so that the replicant starts up.
                {core, AppOpts},
                {replicant, AppOpts}
            ]
        ),
    ?check_trace(
        #{timetrap => 10_000},
        begin
            [Core, Rep] = Nodes = start_cluster(NSpecs),
            wait_foremen_stabilized(Nodes),
            assert_exactly_one_leader(Nodes),
            ?block_until(#{?snk_kind := foreman_replicant_try_follow_leader}),
            %% Stop the leader.  The replicant should be stuck in `?candidate' state.
            {ok, {ok, _}} =
                ?wait_async_action(
                    emqx_cth_cluster:stop_node(Core),
                    #{?snk_kind := foreman_replicant_try_follow_leader}
                ),
            ?retry(
                500,
                10,
                begin
                    States = [S || {_P, {S, _D}} <- get_foremen_states([Rep])],
                    ?assertMatch([candidate], States)
                end
            ),
            %% Revive the core node, which shall become the leader again.
            [Core] = restart(SpecCore),
            wait_foremen_stabilized(Nodes),
            assert_exactly_one_leader(Nodes),
            ok
        end,
        [
            fun ?MODULE:inspect_trace/1,
            trace_prop_no_unexpected_events()
        ]
    ),
    ok.

%% Checks that, once a leader dies, a new leader is elected in the cluster.
t_new_leader_takeover(Config) ->
    NodeOpts = #{},
    AppOpts = #{name => ?FUNCTION_NAME},
    NSpecs =
        mk_cluster(
            ?FUNCTION_NAME,
            Config,
            [
                {core, NodeOpts, AppOpts},
                {core, NodeOpts, AppOpts},
                {core, NodeOpts, AppOpts}
            ]
        ),
    ?check_trace(
        #{timetrap => 10_000},
        begin
            [_N1, _N2, _N3] = Nodes = start_cluster(NSpecs),
            wait_foremen_stabilized(Nodes),
            assert_exactly_one_leader(Nodes),
            [
                {follower, FollowerPid1},
                {follower, FollowerPid2},
                {leader, LeaderPid}
            ] = lists:sort([{S, P} || {P, {S, _D}} <- get_foremen_states(Nodes)]),
            {_, {ok, _}} =
                ?wait_async_action(
                    ?tp_span(
                        kill_leader,
                        #{leader => LeaderPid},
                        ?ON(node(LeaderPid), ok = ?SUP:delete_child(foreman))
                    ),
                    #{?snk_kind := "foreman_elected"}
                ),
            %% New leader should have been elected in one of the previous follower nodes.
            FollowerNodes = lists:map(fun node/1, [FollowerPid1, FollowerPid2]),
            [
                follower,
                leader
            ] = lists:sort([S || {_P, {S, _D}} <- get_foremen_states(FollowerNodes)]),
            assert_exactly_one_leader(FollowerNodes),
            ok
        end,
        [
            fun ?MODULE:inspect_trace/1,
            trace_prop_no_unexpected_events()
        ]
    ),
    ok.

%% Smoke test for basic happy-path allocations.
t_allocate(Config) ->
    Name = ?FUNCTION_NAME,
    [N1, N2, N3] = [emqx_cth_cluster:node_name(mk_node_name(N)) || N <- lists:seq(1, 3)],
    FixedAllocation = #{
        N1 => [1, 2, 3],
        N2 => [4, 5, 6],
        N3 => [7, 8, 9]
    },
    AppOpts = #{
        name => Name,
        init_opts => #{
            compute_allocation_fn => fun(#{members := Members}) ->
                maps:with(Members, FixedAllocation)
            end
        }
    },
    NSpecs =
        mk_cluster(
            ?FUNCTION_NAME,
            Config,
            [
                {core, AppOpts},
                {core, AppOpts},
                {core, AppOpts}
            ]
        ),
    ?check_trace(
        #{timetrap => 10_000},
        begin
            [N1, N2, N3] = Nodes = start_cluster(NSpecs),
            NumNodes = length(Nodes),

            wait_foremen_stabilized(Nodes),
            assert_exactly_one_leader(Nodes),

            ?tp(notice, "waiting for assignments to be committed", #{}),
            {ok, _} = snabbkaffe:block_until(
                ?match_n_events(NumNodes, #{?snk_kind := "foreman_committed_assignments"}),
                infinity
            ),
            lists:foreach(
                fun(N) ->
                    ?assertEqual(
                        {committed, maps:get(N, FixedAllocation)},
                        ?ON(N, emqx_foreman:get_assignments(Name)),
                        #{node => N}
                    )
                end,
                Nodes
            ),

            #{nodes => Nodes}
        end,
        [
            fun ?MODULE:inspect_trace/1,
            trace_prop_no_unexpected_events(),
            fun(#{nodes := Nodes}, Trace) ->
                %% Stage callback called
                ?projection_complete(member, ?of_kind(stage, Trace), Nodes),
                %% Commit callback called
                ?projection_complete(member, ?of_kind(commit, Trace), Nodes),
                ?assert(
                    ?strict_causality(
                        #{?snk_kind := "foreman_staged_assignments"},
                        #{?snk_kind := "foreman_committed_assignments"},
                        Trace
                    )
                ),
                %% TODO: more interesting/relevant properties?
                ok
            end
        ]
    ),
    ok.

%% Check that if the follower doesn't respond with acks for some time but later recovers,
%% we eventually reach a stable state.
t_allocate_crash_waiting_for_ack(Config) ->
    Name = ?FUNCTION_NAME,
    [N1, N2, N3] = Nodes = [emqx_cth_cluster:node_name(mk_node_name(N)) || N <- lists:seq(1, 3)],
    NumNodes = length(Nodes),
    FixedAllocation = #{
        N1 => [1, 2, 3],
        N2 => [4, 5, 6],
        N3 => [7, 8, 9]
    },
    AppOpts = #{
        name => Name,
        init_opts => #{
            compute_allocation_fn => fun(#{members := Members}) ->
                maps:with(Members, FixedAllocation)
            end
        }
    },
    NSpecs =
        mk_cluster(
            ?FUNCTION_NAME,
            Config,
            [
                {core, AppOpts},
                {core, AppOpts},
                {core, AppOpts}
            ]
        ),
    ?check_trace(
        #{timetrap => 10_000},
        begin
            ?inject_crash(
                #{?snk_kind := "foreman_new_assignments", state := follower},
                snabbkaffe_nemesis:recover_after((NumNodes - 1) * 3)
            ),

            Nodes = start_cluster(NSpecs),

            wait_foremen_stabilized(Nodes),
            assert_exactly_one_leader(Nodes),

            ?tp(notice, "waiting for recovery", #{}),
            {ok, _} = snabbkaffe:block_until(
                ?match_n_events(NumNodes, #{?snk_kind := "foreman_committed_assignments"}),
                infinity
            ),
            lists:foreach(
                fun(N) ->
                    ?assertEqual(
                        {committed, maps:get(N, FixedAllocation)},
                        ?ON(N, emqx_foreman:get_assignments(Name)),
                        #{node => N}
                    )
                end,
                Nodes
            ),

            ok
        end,
        [
            fun ?MODULE:inspect_trace/1,
            trace_prop_no_unexpected_events(),
            fun(Trace) ->
                ?assertMatch([_, _ | _], ?of_kind(snabbkaffe_crash, Trace)),
                ok
            end
        ]
    ),
    ok.

%% Check that if the follower doesn't commit for some time but later recovers, we
%% eventually reach a stable state.
t_allocate_crash_during_commit(Config) ->
    Name = ?FUNCTION_NAME,
    [N1, N2, N3] = Nodes = [emqx_cth_cluster:node_name(mk_node_name(N)) || N <- lists:seq(1, 3)],
    NumNodes = length(Nodes),
    FixedAllocation = #{
        N1 => [1, 2, 3],
        N2 => [4, 5, 6],
        N3 => [7, 8, 9]
    },
    AppOpts = #{
        name => Name,
        init_opts => #{
            compute_allocation_fn => fun(#{members := Members}) ->
                maps:with(Members, FixedAllocation)
            end
        }
    },
    NSpecs =
        mk_cluster(
            ?FUNCTION_NAME,
            Config,
            [
                {core, AppOpts},
                {core, AppOpts},
                {core, AppOpts}
            ]
        ),
    ?check_trace(
        #{timetrap => 10_000},
        begin
            ?inject_crash(
                #{?snk_kind := "foreman_commit_assignments", state := follower},
                snabbkaffe_nemesis:recover_after((NumNodes - 1) * 3)
            ),

            Nodes = start_cluster(NSpecs),

            wait_foremen_stabilized(Nodes),
            assert_exactly_one_leader(Nodes),

            ?tp(notice, "waiting for recovery", #{}),
            {ok, _} = snabbkaffe:block_until(
                ?match_n_events(NumNodes, #{?snk_kind := "foreman_committed_assignments"}),
                infinity
            ),
            lists:foreach(
                fun(N) ->
                    ?assertEqual(
                        {committed, maps:get(N, FixedAllocation)},
                        ?ON(N, emqx_foreman:get_assignments(Name)),
                        #{node => N}
                    )
                end,
                Nodes
            ),

            ok
        end,
        [
            fun ?MODULE:inspect_trace/1,
            trace_prop_no_unexpected_events(),
            fun(Trace) ->
                ?assertMatch([_, _ | _], ?of_kind(snabbkaffe_crash, Trace)),
                ok
            end,
            fun(Trace) ->
                %% Stage callback called
                ?projection_complete(member, ?of_kind(stage, Trace), Nodes),
                %% Commit callback called
                ?projection_complete(member, ?of_kind(commit, Trace), Nodes),
                ?assert(
                    ?strict_causality(
                        #{?snk_kind := "foreman_staged_assignments"},
                        #{?snk_kind := "foreman_committed_assignments"},
                        Trace
                    )
                ),
                %% TODO: more interesting/relevant properties?
                ok
            end
        ]
    ),
    ok.

%% Checks that, if a member goes away for long enough, its resources will be eventually
%% re-assigned.
t_allocation_handover(Config) ->
    Name = ?FUNCTION_NAME,
    AllResources = lists:seq(0, 10),
    AppOpts = #{
        name => Name,
        init_opts => #{
            compute_allocation_fn => fun(#{members := Members}) ->
                CurrentNumNodes = length(Members),
                maps:from_list([
                    {N, [
                        X
                     || X <- AllResources,
                        X rem CurrentNumNodes =:= I
                    ]}
                 || {I, N} <- lists:enumerate(0, Members)
                ])
            end
        }
    },
    NSpecs =
        mk_cluster(
            ?FUNCTION_NAME,
            Config,
            [
                {core, AppOpts},
                {core, AppOpts},
                {core, AppOpts}
            ]
        ),
    ?check_trace(
        #{timetrap => 10_000},
        begin
            Nodes = [N1 | RestNodes] = start_cluster(NSpecs),
            NumNodes = length(Nodes),

            wait_foremen_stabilized(Nodes),
            assert_exactly_one_leader(Nodes),

            {ok, _} = snabbkaffe:block_until(
                ?match_n_events(NumNodes, #{?snk_kind := "foreman_committed_assignments"}),
                infinity
            ),
            Committed1 = lists:flatmap(
                fun(N) ->
                    {Status, Rs} = ?ON(N, emqx_foreman:get_assignments(Name)),
                    ?assertEqual(committed, Status, #{node => N}),
                    Rs
                end,
                Nodes
            ),
            ?assertEqual(AllResources, lists:sort(Committed1)),

            %% Now, we take down one node.
            {ok, SRef1} = snabbkaffe:subscribe(
                ?match_event(#{?snk_kind := "foreman_committed_assignments"}),
                NumNodes - 1,
                infinity
            ),
            ok = emqx_cth_cluster:stop_node(N1),
            {ok, _} = snabbkaffe:receive_events(SRef1),

            Committed2 = lists:flatmap(
                fun(N) ->
                    {Status, Rs} = ?ON(N, emqx_foreman:get_assignments(Name)),
                    ?assertEqual(committed, Status, #{node => N}),
                    Rs
                end,
                RestNodes
            ),
            ?assertEqual(AllResources, lists:sort(Committed2)),

            ok
        end,
        [
            fun ?MODULE:inspect_trace/1,
            trace_prop_no_unexpected_events()
        ]
    ),
    ok.
