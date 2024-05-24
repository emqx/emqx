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

-module(emqx_connector_foreman_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-import(emqx_common_test_helpers, [on_exit/1]).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-define(SUP, emqx_connector_foreman_sup).
-define(ON(NODE, BODY), erpc:call(NODE, fun() -> BODY end)).

%%------------------------------------------------------------------------------
%% CT Boilerplate
%%------------------------------------------------------------------------------

all() ->
    emqx_common_test_helpers:all(?MODULE).

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

app_specs(_Config) ->
    [
        emqx_connector_foreman
    ].

mk_node_name(N) ->
    binary_to_atom(<<"foreman_SUITE", (integer_to_binary(N))/binary>>).

mk_cluster(TestCase, Config, NodeRoles) ->
    AppSpecs = app_specs(Config),
    ExtraVMArgs = ["-kernel", "prevent_overlapping_partitions", "false"],
    Cluster = lists:map(
        fun
            ({N, {Role, Opts}}) ->
                Name = mk_node_name(N),
                {Name, Opts#{role => Role, apps => AppSpecs, extra_vm_args => ExtraVMArgs}};
            ({N, Role}) ->
                Name = mk_node_name(N),
                {Name, #{role => Role, apps => AppSpecs, extra_vm_args => ExtraVMArgs}}
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

start_cluster(NodeSpecs, Opts) ->
    Nodes = emqx_cth_cluster:start(NodeSpecs),
    Res = erpc:multicall(Nodes, fun() -> setup_node(Opts) end),
    assert_all_erpc_ok(Res),
    Nodes.

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
    ok = emqx_connector_foreman:ensure_pg_scope_started(?SUP, Name),
    InitOptsDefault = #{
        name => Name,
        scope => Name,
        compute_allocation_fn => fun(#{members := Members}) ->
            maps:from_keys(Members, [dummy_resource])
        end
    },
    InitOpts0 = maps:get(init_opts, Opts, #{}),
    InitOpts = emqx_utils_maps:deep_merge(InitOptsDefault, InitOpts0),
    {ok, _} = ?SUP:start_child(
        #{
            id => foreman,
            start => {emqx_connector_foreman, start_link, [InitOpts]}
        }
    ),
    ok.

inspect_trace(Trace) ->
    ct:pal("trace:\n  ~p", [Trace]),
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

restart(NodeSpec, Opts) ->
    [Node] = emqx_cth_cluster:restart(NodeSpec),
    ok = ?ON(Node, setup_node(Opts)),
    [Node].

%%------------------------------------------------------------------------------
%% Test cases
%%------------------------------------------------------------------------------

%% Test that, after a split brain happens and later the two cliques are merged, only one
%% leader emerges.
t_resolve_name_clash(Config) ->
    NodeOpts = #{join_to => undefined},
    NSpecs =
        mk_cluster(
            ?FUNCTION_NAME,
            Config,
            [
                {core, NodeOpts},
                {core, NodeOpts}
            ]
        ),
    Opts = #{name => ?FUNCTION_NAME},
    ?check_trace(
        begin
            [N1, N2] = Nodes = start_cluster(NSpecs, Opts),
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
        [fun ?MODULE:inspect_trace/1]
    ),
    ok.

%% We limit leaders to core nodes only, to avoid them reading stale data.
t_replicants_are_never_elected(Config) ->
    NodeOpts = #{},
    NSpecs =
        [SpecCore, _SpecRep] =
        mk_cluster(
            ?FUNCTION_NAME,
            Config,
            [
                %% We need one core so that the replicant starts up.
                {core, NodeOpts},
                {replicant, NodeOpts}
            ]
        ),
    Opts = #{name => ?FUNCTION_NAME, init_opts => #{retry_election_timeout => 500}},
    ?check_trace(
        #{timetrap => 10_000},
        begin
            [Core, Rep] = Nodes = start_cluster(NSpecs, Opts),
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
            [Core] = restart(SpecCore, Opts),
            wait_foremen_stabilized(Nodes),
            assert_exactly_one_leader(Nodes),
            ok
        end,
        [fun ?MODULE:inspect_trace/1]
    ),
    ok.

%% Checks that, once a leader dies, a new leader is elected in the cluster.
t_new_leader_takeover(Config) ->
    NodeOpts = #{join_to => undefined},
    NSpecs =
        mk_cluster(
            ?FUNCTION_NAME,
            Config,
            [
                {core, NodeOpts},
                {core, NodeOpts},
                {core, NodeOpts}
            ]
        ),
    Opts = #{name => ?FUNCTION_NAME},
    ?check_trace(
        begin
            [_N1, _N2, _N3] = Nodes = start_cluster(NSpecs, Opts),
            wait_foremen_stabilized(Nodes),
            assert_exactly_one_leader(Nodes),
            [
                {follower, FollowerPid1},
                {follower, FollowerPid2},
                {leader, LeaderPid}
            ] = lists:sort([{S, P} || {P, {S, _D}} <- get_foremen_states(Nodes)]),
            %% To avoid the old leader being revived too fast by the supervisor and
            %% electing itself.
            ?force_ordering(
                #{?snk_kind := "foreman_elected"},
                #{
                    ?snk_kind := "foreman_state_enter",
                    current_state := candidate,
                    ?snk_meta := #{pid := P}
                } when P =/= FollowerPid1 andalso P =/= FollowerPid2
            ),
            {_, {ok, _}} =
                ?wait_async_action(
                    ?tp_span(
                        kill_leader,
                        #{leader => LeaderPid},
                        begin
                            Ref = monitor(process, LeaderPid),
                            exit(LeaderPid, kill),
                            receive
                                {'DOWN', Ref, process, LeaderPid, killed} -> ok
                            after 1_000 -> ct:fail("leader didn't die")
                            end,
                            ok
                        end
                    ),
                    #{?snk_kind := "foreman_elected"}
                ),
            %% New leader should have been elected in one of the previous follower nodes.
            FollowerNodes = lists:map(fun node/1, [FollowerPid1, FollowerPid2]),
            [
                follower,
                leader
            ] = lists:sort([S || {_P, {S, _D}} <- get_foremen_states(FollowerNodes)]),
            assert_exactly_one_leader(Nodes),
            ok
        end,
        [fun ?MODULE:inspect_trace/1]
    ),
    ok.
