%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_security_profile_monitor_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-import(emqx_common_test_helpers, [on_exit/1]).

-define(ON(NODE, BODY), erpc:call(NODE, fun() -> BODY end)).

-define(ALARM, security_profile_divergence).
-define(ENV_VAR, "EMQX_SECURITY_PROFILE").
-define(TIMEOUT, 15_000).

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_testcase(TestCase, Config) ->
    snabbkaffe:start_trace(),
    [{work_dir, emqx_cth_suite:work_dir(TestCase, Config)} | Config].

end_per_testcase(_TestCase, _Config) ->
    emqx_common_test_helpers:call_janitor(),
    snabbkaffe:stop(),
    ok.

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

node_specs(TestCase, Config, Profiles) ->
    Nodes = [
        {node_name(TestCase, I), #{
            apps => [{emqx, #{override_env => [{security_profile_check_interval, 500}]}}],
            role => Role,
            env_vars => [{?ENV_VAR, atom_to_list(Profile)}]
        }}
     || {I, {Role, Profile}} <- lists:enumerate(Profiles)
    ],
    emqx_cth_cluster:mk_nodespecs(Nodes, #{work_dir => ?config(work_dir, Config)}).

node_name(TestCase, I) ->
    list_to_atom(atom_to_list(TestCase) ++ "_" ++ integer_to_list(I)).

start_cluster(Specs) ->
    Nodes = emqx_cth_cluster:start(Specs),
    on_exit(fun() -> emqx_cth_cluster:stop(Nodes) end),
    Nodes.

active_alarms(Node) ->
    [A || #{name := ?ALARM} = A <- ?ON(Node, emqx_alarm:get_alarms(activated))].

deactivated_alarms(Node) ->
    [A || #{name := ?ALARM} = A <- ?ON(Node, emqx_alarm:get_alarms(deactivated))].

%% Block until `Node` evaluated the cluster and classified `Peers` under `Key`.
wait_evaluated(Node, Key, Peers) ->
    ?block_until(
        #{
            ?snk_kind := security_profile_evaluated,
            ?snk_meta := #{node := Node},
            Key := Peers
        },
        ?TIMEOUT
    ).

wait_alarm(Node) ->
    ?retry(200, 50, begin
        [Alarm] = active_alarms(Node),
        Alarm
    end).

wait_no_alarm(Node) ->
    ?retry(200, 50, ?assertEqual([], active_alarms(Node))).

%% Legacy nodes do not run the monitor at all.
assert_monitor_not_running(Node) ->
    ?assertEqual(undefined, ?ON(Node, erlang:whereis(emqx_security_profile_monitor))),
    ?assertEqual([], active_alarms(Node)).

%%--------------------------------------------------------------------
%% Test cases
%%--------------------------------------------------------------------

-doc "A single hardened node has no peers and never raises the alarm.".
t_single_node_no_alarm(Config) ->
    [N1] = start_cluster(node_specs(?FUNCTION_NAME, Config, [{core, hardened}])),
    {ok, _} = wait_evaluated(N1, hardened_nodes, []),
    ?assertEqual([], active_alarms(N1)).

-doc "Two hardened nodes see each other and neither raises the alarm.".
t_same_profile_no_alarm(Config) ->
    Specs = node_specs(?FUNCTION_NAME, Config, [{core, hardened}, {core, hardened}]),
    [N1, N2] = start_cluster(Specs),
    {ok, _} = wait_evaluated(N1, hardened_nodes, [N2]),
    {ok, _} = wait_evaluated(N2, hardened_nodes, [N1]),
    ?assertEqual([], active_alarms(N1)),
    ?assertEqual([], active_alarms(N2)).

-doc "Legacy nodes do not run the monitor and never raise the alarm.".
t_legacy_nodes_never_raise(Config) ->
    Specs = node_specs(?FUNCTION_NAME, Config, [{core, legacy}, {core, legacy}]),
    [N1, N2] = start_cluster(Specs),
    assert_monitor_not_running(N1),
    assert_monitor_not_running(N2).

-doc """
The hardened node raises the alarm and names the legacy peer in both
the message and the details. The legacy node does not raise it.
""".
t_divergence_raises(Config) ->
    Specs = node_specs(?FUNCTION_NAME, Config, [{core, hardened}, {core, legacy}]),
    [N1, N2] = start_cluster(Specs),
    #{message := Message, details := Details} = wait_alarm(N1),
    ?assertEqual(
        #{
            local_profile => hardened,
            legacy_nodes => [N2],
            hardened_nodes => [],
            unknown_nodes => []
        },
        Details
    ),
    ?assertNotEqual(nomatch, binary:match(Message, atom_to_binary(N2))),
    assert_monitor_not_running(N2).

-doc "A replicant announces its profile through a core, and the core raises the alarm.".
t_divergence_raises_for_replicant(Config) ->
    Specs = node_specs(?FUNCTION_NAME, Config, [{core, hardened}, {replicant, legacy}]),
    [N1, N2] = start_cluster(Specs),
    #{details := #{legacy_nodes := [N2]}} = wait_alarm(N1),
    assert_monitor_not_running(N2).

-doc """
Restarting the legacy node with the hardened profile clears the alarm,
and the alarm is not raised again once the restarted node announces itself.
""".
t_converge_clears(Config) ->
    [Spec1, Spec2] = node_specs(?FUNCTION_NAME, Config, [{core, hardened}, {core, legacy}]),
    [N1, N2] = start_cluster([Spec1, Spec2]),
    _ = wait_alarm(N1),
    [N2] = emqx_cth_cluster:restart([Spec2#{env_vars => [{?ENV_VAR, "hardened"}]}]),
    {ok, _} = wait_evaluated(N1, hardened_nodes, [N2]),
    ?assertEqual([], active_alarms(N1)),
    ?assertMatch([_], deactivated_alarms(N1)),
    ?assertEqual([], active_alarms(N2)).

-doc """
With two legacy peers, the alarm stays active while one of them converges:
the details shrink to the remaining legacy node and the message is kept.
""".
t_partial_convergence_updates_details(Config) ->
    [Spec1, Spec2, Spec3] = node_specs(
        ?FUNCTION_NAME, Config, [{core, hardened}, {core, legacy}, {core, legacy}]
    ),
    [N1, N2, N3] = start_cluster([Spec1, Spec2, Spec3]),
    #{message := Message, activate_at := ActivateAt} =
        ?retry(200, 50, begin
            [#{details := #{legacy_nodes := [N2, N3]}} = Alarm] = active_alarms(N1),
            Alarm
        end),
    [N3] = emqx_cth_cluster:restart([Spec3#{env_vars => [{?ENV_VAR, "hardened"}]}]),
    {ok, _} = wait_evaluated(N1, hardened_nodes, [N3]),
    ?assertMatch(
        [
            #{
                message := Message,
                activate_at := ActivateAt,
                details := #{legacy_nodes := [N2], hardened_nodes := [N3]}
            }
        ],
        active_alarms(N1)
    ),
    ?assertEqual([], deactivated_alarms(N1)).

-doc "The alarm clears when the only legacy node leaves the cluster.".
t_leave_clears(Config) ->
    Specs = node_specs(?FUNCTION_NAME, Config, [{core, hardened}, {core, legacy}]),
    [N1, N2] = start_cluster(Specs),
    _ = wait_alarm(N1),
    ok = ?ON(N2, ekka:leave()),
    wait_no_alarm(N1),
    ?assertMatch([_], deactivated_alarms(N1)).

-doc "The alarm clears when the only legacy node goes down.".
t_peer_down_clears(Config) ->
    Specs = node_specs(?FUNCTION_NAME, Config, [{core, hardened}, {core, legacy}]),
    [N1, N2] = start_cluster(Specs),
    _ = wait_alarm(N1),
    ok = emqx_cth_cluster:stop([N2]),
    wait_no_alarm(N1),
    ?assertMatch([_], deactivated_alarms(N1)).

-doc """
A peer that announces its BPAPIs without `emqx_security_profile` runs an older
release without security profiles and counts as `legacy`.
""".
t_old_release_peer_is_legacy(Config) ->
    Specs = node_specs(?FUNCTION_NAME, Config, [{core, hardened}, {core, hardened}]),
    [N1, N2] = start_cluster(Specs),
    {ok, _} = wait_evaluated(N1, hardened_nodes, [N2]),
    ?assertEqual([], active_alarms(N1)),
    ok = ?ON(N1, meck:new(emqx_bpapi, [passthrough, no_link])),
    on_exit(fun() -> catch ?ON(N1, meck:unload(emqx_bpapi)) end),
    ok = ?ON(
        N1,
        meck:expect(
            emqx_bpapi,
            supported_version,
            fun
                (_Node, emqx_security_profile) -> undefined;
                (Node, API) -> meck:passthrough([Node, API])
            end
        )
    ),
    %% Every tick re-resolves all peers: no restart of the monitor is needed.
    ?assertMatch(#{details := #{legacy_nodes := [N2]}}, wait_alarm(N1)),
    ok = ?ON(N1, meck:unload(emqx_bpapi)),
    wait_no_alarm(N1).

-doc """
A peer that restarts with the `legacy` profile is picked up by a later check
even though it was seen as `hardened` before.
""".
t_divergence_after_restart(Config) ->
    [Spec1, Spec2] = node_specs(?FUNCTION_NAME, Config, [{core, hardened}, {core, hardened}]),
    [N1, N2] = start_cluster([Spec1, Spec2]),
    {ok, _} = wait_evaluated(N1, hardened_nodes, [N2]),
    ?assertEqual([], active_alarms(N1)),
    [N2] = emqx_cth_cluster:restart([Spec2#{env_vars => [{?ENV_VAR, "legacy"}]}]),
    ?assertMatch(#{details := #{legacy_nodes := [N2]}}, wait_alarm(N1)).
