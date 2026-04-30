%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_modules_conf_cluster_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-define(ON(NODE, BODY), erpc:call(NODE, fun() -> BODY end)).

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(TestCase, Config) ->
    NodeSpec = #{
        role => core,
        apps => [emqx, emqx_conf, emqx_modules]
    },
    Nodes = emqx_cth_cluster:start(
        [
            {modules_conf_cluster1, NodeSpec},
            {modules_conf_cluster2, NodeSpec}
        ],
        #{work_dir => emqx_cth_suite:work_dir(TestCase, Config)}
    ),
    [{cluster, Nodes} | Config].

end_per_testcase(_TestCase, Config) ->
    emqx_cth_cluster:stop(?config(cluster, Config)),
    ok.

%%--------------------------------------------------------------------
%% Regression tests
%%--------------------------------------------------------------------

-doc """
Regression: raw-config drift on the replicate used to make
`pre_config_update/3` return `{error, not_found}`, which
`emqx_cluster_rpc:apply_mfa/3` logs as `cluster_rpc_apply_failed` and raises
as an alarm; the replicate's commit lag then stops advancing.

After making the removal idempotent at the `pre_config_update/3` layer (while
keeping `{error, not_found}` at the `emqx_modules_conf:remove_topic_metrics/1`
entrypoint for API callers), the replicate applies the update as a no-op and
the cluster commit advances cleanly.
""".
t_remove_topic_metrics_replicate_drift_is_tolerated(Config) ->
    [N1, N2] = ?config(cluster, Config),
    Topic = <<"t/1">>,
    {ok, Topic} = ?ON(N1, emqx_modules_conf:add_topic_metrics(Topic)),
    ?assertEqual([Topic], ?ON(N1, emqx_modules_conf:topic_metrics())),
    ?assertEqual([Topic], ?ON(N2, emqx_modules_conf:topic_metrics())),
    %% Simulate raw-config drift: clear only N2's local raw config.
    ok = ?ON(N2, emqx_config:put_raw([topic_metrics], [])),
    %% Both initiator and replicate must apply successfully.
    ok = ?ON(N1, emqx_modules_conf:remove_topic_metrics(Topic)),
    %% No alarm raised on the replicate.
    no_alarm = wait_no_alarm(N2, cluster_rpc_apply_failed, 10),
    %% Replicate's commit lag caught up.
    #{my_id := MyId, latest := LatestId} =
        ?ON(N2, emqx_cluster_rpc:get_commit_lag()),
    ?assertEqual(LatestId, MyId),
    %% Topic removed on both nodes.
    ?assertEqual([], ?ON(N1, emqx_modules_conf:topic_metrics())),
    ?assertEqual([], ?ON(N2, emqx_modules_conf:topic_metrics())),
    ok.

-doc """
API contract preserved: removing a topic that the caller's local view does not
know about still returns `{error, not_found}` at the entrypoint, even though
the underlying `pre_config_update/3` is now idempotent.
""".
t_remove_topic_metrics_api_still_returns_not_found(Config) ->
    [N1, _N2] = ?config(cluster, Config),
    ?assertEqual(
        {error, not_found},
        ?ON(N1, emqx_modules_conf:remove_topic_metrics(<<"never/added">>))
    ),
    ok.

%%--------------------------------------------------------------------
%% helpers
%%--------------------------------------------------------------------

wait_no_alarm(Node, Name, 0) ->
    case [A || A = #{name := N0} <- ?ON(Node, emqx_alarm:get_alarms(activated)), N0 =:= Name] of
        [] -> no_alarm;
        [Alarm | _] -> error({unexpected_alarm, Alarm})
    end;
wait_no_alarm(Node, Name, N) ->
    case [A || A = #{name := N0} <- ?ON(Node, emqx_alarm:get_alarms(activated)), N0 =:= Name] of
        [Alarm | _] ->
            error({unexpected_alarm, Alarm});
        [] ->
            timer:sleep(200),
            wait_no_alarm(Node, Name, N - 1)
    end.
