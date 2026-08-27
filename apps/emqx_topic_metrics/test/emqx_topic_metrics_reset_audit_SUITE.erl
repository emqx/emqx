%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% Cluster regression tests for #18534: a topic-metrics reset is
%% fanned out via emqx_cluster_rpc. The cluster_rpc callback must
%% stay side-effect-only — a crash inside it (as the removed
%% per-node audit emission demonstrated) leaves counters reset on
%% some nodes only while the REST API reports success.

-module(emqx_topic_metrics_reset_audit_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("emqx/include/emqx_config.hrl").

-define(BIN_NAME, <<"a">>).

suite() -> [{timetrap, {seconds, 120}}].

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_testcase(Case, Config) ->
    Names = [
        binary_to_atom(<<(atom_to_binary(Case))/binary, "1">>),
        binary_to_atom(<<(atom_to_binary(Case))/binary, "2">>)
    ],
    Specs = [{Name, #{role => core, apps => apps(Case)}} || Name <- Names],
    Nodes = emqx_cth_cluster:start(
        Specs,
        #{work_dir => emqx_cth_suite:work_dir(Case, Config)}
    ),
    [{nodes, Nodes} | Config].

end_per_testcase(_Case, Config) ->
    ok = emqx_cth_cluster:stop(?config(nodes, Config)).

apps(t_reset_with_audit_disabled) ->
    [
        emqx_conf,
        {emqx, #{override_env => [{boot_modules, [broker]}]}},
        emqx_topic_metrics
    ];
apps(_Case) ->
    [
        {emqx_conf, #{config => #{log => #{audit => #{enable => true, level => info}}}}},
        {emqx, #{override_env => [{boot_modules, [broker]}]}},
        emqx_audit,
        emqx_topic_metrics
    ].

%%--------------------------------------------------------------------
%% Cases
%%--------------------------------------------------------------------

-doc """
Regression test for #18534: with audit logging enabled, a reset must
zero counters on EVERY node and commit the cluster_rpc transaction.
""".
t_reset_with_audit_enabled(Config) ->
    [N1, N2] = ?config(nodes, Config),
    ok = register_and_bump(N1, N2),
    ok = erpc:call(N1, emqx_topic_metrics2, reset, [?BIN_NAME, ?global_ns]),
    ?assertEqual(0, msg_in_count(N1)),
    ?retry(200, 50, ?assertEqual(0, msg_in_count(N2))),
    ok = assert_caught_up(N1, [N1, N2]).

-doc "Reset still zeroes counters on every node when audit logging is disabled.".
t_reset_with_audit_disabled(Config) ->
    [N1, N2] = ?config(nodes, Config),
    ok = register_and_bump(N1, N2),
    ok = erpc:call(N1, emqx_topic_metrics2, reset, [?BIN_NAME, ?global_ns]),
    ?assertEqual(0, msg_in_count(N1)),
    ?retry(200, 50, ?assertEqual(0, msg_in_count(N2))),
    ok = assert_caught_up(N1, [N1, N2]).

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

register_and_bump(N1, N2) ->
    %% The install multicall is synchronous across nodes, so the
    %% collection exists on N2 when register/3 returns.
    ok = erpc:call(N1, emqx_topic_metrics2, register, [?BIN_NAME, <<"a/#">>, ?global_ns]),
    ok = bump(N1, 3),
    ok = bump(N2, 5),
    ?assertEqual(3, msg_in_count(N1)),
    ?assertEqual(5, msg_in_count(N2)),
    ok.

%% Counters are per-node atomics, so they must be bumped on the node
%% that owns them.
bump(Node, N) ->
    erpc:call(Node, fun() ->
        {ok, #{counter_ref := CRef}} =
            emqx_topic_metrics_registry:lookup({?global_ns, ?BIN_NAME}),
        counters:add(CRef, 1, N)
    end).

msg_in_count(Node) ->
    {ok, #{metrics := #{'messages.in.count' := C}}} =
        erpc:call(Node, emqx_topic_metrics2, lookup, [?BIN_NAME, ?global_ns]),
    C.

%% All nodes committed the same latest transaction id — no node is
%% stuck replaying a failed transaction.
assert_caught_up(Node, Nodes) ->
    ?retry(200, 50, begin
        {atomic, Status} = erpc:call(Node, emqx_cluster_rpc, status, []),
        TnxIds = [Id || #{node := N, tnx_id := Id} <- Status, lists:member(N, Nodes)],
        ?assertEqual(length(Nodes), length(TnxIds)),
        ?assertMatch([_], lists:usort(TnxIds))
    end),
    ok.
