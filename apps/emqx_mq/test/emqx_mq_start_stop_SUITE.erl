%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mq_start_stop_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("emqx/include/asserts.hrl").

-import(
    emqx_mq_api_helpers,
    [
        api_get/1,
        api_put/2
    ]
).
-import(emqx_common_test_helpers, [on_exit/1]).

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_testcase(TestCase = t_cluster_runtime_enable, Config) ->
    Apps = [
        emqx_conf,
        emqx_durable_storage,
        {emqx, emqx_mq_test_utils:cth_config(emqx)},
        {emqx_mq, #{config => mq_initial_config(TestCase)}},
        emqx_management
    ],
    ClusterSpec = [
        {t_cluster_runtime_enable1, #{apps => Apps ++ [emqx_mgmt_api_test_util:emqx_dashboard()]}},
        {t_cluster_runtime_enable2, #{apps => Apps}},
        {t_cluster_runtime_enable3, #{apps => Apps}}
    ],
    Nodes = emqx_cth_cluster:start(
        ClusterSpec,
        #{work_dir => emqx_cth_suite:work_dir(TestCase, Config)}
    ),
    snabbkaffe:start_trace(),
    [{cluster_nodes, Nodes} | Config];
init_per_testcase(TestCase, Config) ->
    Apps = emqx_cth_suite:start(
        [
            emqx_conf,
            {emqx, emqx_mq_test_utils:cth_config(emqx)},
            {emqx_mq, #{config => mq_initial_config(TestCase)}},
            emqx_management,
            emqx_mgmt_api_test_util:emqx_dashboard()
        ],
        #{work_dir => emqx_cth_suite:work_dir(TestCase, Config)}
    ),
    snabbkaffe:start_trace(),
    [{suite_apps, Apps} | Config].

end_per_testcase(t_cluster_runtime_enable, Config) ->
    ok = snabbkaffe:stop(),
    ok = emqx_common_test_helpers:call_janitor(),
    ok = emqx_cth_cluster:stop(?config(cluster_nodes, Config));
end_per_testcase(_TestCase, Config) ->
    ok = snabbkaffe:stop(),
    ok = emqx_common_test_helpers:call_janitor(),
    ok = emqx_cth_suite:stop(?config(suite_apps, Config)).

mq_initial_config(t_config) ->
    #{<<"mq">> => #{<<"enable">> => false}};
mq_initial_config(t_auto_no_queues) ->
    #{<<"mq">> => #{<<"enable">> => auto}};
mq_initial_config(t_auto_with_queues) ->
    #{<<"mq">> => #{<<"enable">> => true}};
mq_initial_config(t_idempotency) ->
    #{<<"mq">> => #{<<"enable">> => true}};
mq_initial_config(t_reverse_start) ->
    #{<<"mq">> => #{<<"enable">> => false}};
mq_initial_config(t_reverse_stop) ->
    #{<<"mq">> => #{<<"enable">> => true}};
mq_initial_config(t_reconcile_worker_crash) ->
    #{<<"mq">> => #{<<"enable">> => false}};
mq_initial_config(t_restart_during_stop) ->
    #{<<"mq">> => #{<<"enable">> => true}};
mq_initial_config(t_cluster_runtime_enable) ->
    #{<<"mq">> => #{<<"enable">> => false}}.

%%--------------------------------------------------------------------
%% Test cases
%%--------------------------------------------------------------------

%% Verify that enabling and disabling MQ at runtime completes on all nodes.
t_cluster_runtime_enable(Config) ->
    [N1 | _] = Nodes = ?config(cluster_nodes, Config),

    %% Enable MQ via config (multicalls to all nodes)
    {ok, _} = erpc:call(N1, emqx_mq_config, update_config, [
        #{<<"enable">> => true}
    ]),

    %% Wait for all nodes to reach ready
    Timeout = 10_000,
    ?assertEqual(
        [{ok, started} || _ <- Nodes],
        erpc:multicall(Nodes, emqx_mq_controller, wait_status, [Timeout])
    ),

    %% Verify status on each node
    ?assertEqual(
        [{ok, started} || _ <- Nodes],
        erpc:multicall(Nodes, emqx_mq_controller, status, [])
    ),

    %% Disable MQ without waiting for database shutdown in the config handler.
    {ok, _} = erpc:call(
        N1,
        emqx_mq_config,
        update_config,
        [#{<<"enable">> => false}],
        5_000
    ),

    %% A new target must be accepted while shutdown is still in progress.
    {ok, _} = erpc:call(
        N1,
        emqx_mq_config,
        update_config,
        [#{<<"enable">> => true}],
        5_000
    ),

    TransitionTimeout = 30_000,
    ?assertEqual(
        [{ok, started} || _ <- Nodes],
        erpc:multicall(Nodes, emqx_mq_controller, wait_status, [TransitionTimeout])
    ),

    %% Disable once more and wait for asynchronous shutdown on all nodes.
    {ok, _} = erpc:call(
        N1,
        emqx_mq_config,
        update_config,
        [#{<<"enable">> => false}],
        5_000
    ),

    ?assertEqual(
        [{ok, stopped} || _ <- Nodes],
        erpc:multicall(Nodes, emqx_mq_controller, wait_status, [TransitionTimeout])
    ),

    ?assertEqual(
        [{ok, stopped} || _ <- Nodes],
        erpc:multicall(Nodes, emqx_mq_controller, status, [])
    ).

%% Verify that MQ subsystem may be started in runtime.
t_config(_Config) ->
    #{id := MetricsWorker} = emqx_mq_metrics:child_spec(),
    #{id := GCScheduler} = emqx_mq_gc:child_spec(),
    %% We started with disabled MQ subsystem, so queue API should be unavailable.
    ?assertMatch(
        {ok, 503, #{<<"code">> := <<"SERVICE_UNAVAILABLE">>, <<"message">> := <<"Not enabled">>}},
        api_get([message_queues, queues])
    ),

    %% Start MQ subsystem via API.
    ?assertMatch(
        {ok, 204},
        api_put([message_queues, config], #{<<"enable">> => true})
    ),
    started = emqx_mq_controller:wait_status(5000),
    ?assert(is_pid(whereis(MetricsWorker))),
    ?assert(is_pid(whereis(GCScheduler))),
    ok = emqx_mq_controller:start_mqs(),
    %% Verify that queue API is now available.
    ?assertMatch(
        {ok, 200, _},
        api_get([message_queues, queues])
    ),

    %% Verify that we can disable MQ subsystem via API if no queues exist.
    ?assertMatch(
        {ok, 204},
        api_put([message_queues, config], #{<<"enable">> => false})
    ),
    stopped = emqx_mq_controller:wait_status(5000),
    ?assertEqual(undefined, whereis(MetricsWorker)),
    ?assertEqual(undefined, whereis(GCScheduler)),
    ok = emqx_mq_controller:stop_mqs(),

    %% Start MQ subsystem via API again.
    ?assertMatch(
        {ok, 204},
        api_put([message_queues, config], #{<<"enable">> => true})
    ),
    started = emqx_mq_controller:wait_status(5000),
    ?assert(is_pid(whereis(MetricsWorker))),
    ?assert(is_pid(whereis(GCScheduler))),

    %% Create a queue.
    _ = emqx_mq_test_utils:ensure_mq_created(#{topic_filter => <<"test">>, name => <<"test">>}),

    %% Verify that we cannot disable MQ subsystem via API if any queues exist.
    ?assertMatch(
        {ok, 400, #{
            <<"code">> := <<"BAD_REQUEST">>,
            <<"message">> :=
                <<"Cannot disable MQ subsystem via API when there are existing queues">>
        }},
        api_put([message_queues, config], #{<<"enable">> => false})
    ).

%% Verify that auto starts MQ when there are queues.
t_auto_with_queues(_Config) ->
    %% Create a queue.
    _ = emqx_mq_test_utils:ensure_mq_created(#{topic_filter => <<"test">>, name => <<"test">>}),
    {ok, _} = emqx:update_config([mq], #{<<"enable">> => auto}),

    %% Stop/start MQ subsystem.
    ok = application:stop(emqx_mq),
    ok = application:start(emqx_mq),
    started = emqx_mq_controller:wait_status(5000).

%% Verify that auto does not start MQ when there are no queues.
t_auto_no_queues(_Config) ->
    stopped = emqx_mq_controller:wait_status(5000).

%% Verify that disable waits for startup before stopping.
t_reverse_start(_Config) ->
    TestPid = self(),
    ok = meck:new(emqx_mq_message_db, [passthrough, no_history, no_link]),
    on_exit(fun() -> meck:unload(emqx_mq_message_db) end),
    ok = meck:expect(emqx_mq_message_db, wait_readiness, fun(infinity) ->
        TestPid ! {readiness_waiting, self()},
        receive
            continue -> ok
        end
    end),

    ok = emqx_mq_controller:start_mqs(),
    Worker =
        receive
            {readiness_waiting, Pid} -> Pid
        after 5_000 ->
            ct:fail(readiness_not_reached)
        end,
    ok = emqx_mq_controller:stop_mqs(),
    starting = emqx_mq_controller:status(),
    Worker ! continue,
    stopped = emqx_mq_controller:wait_status(5_000),

    true = meck:validate(emqx_mq_message_db).

%% Verify that enable changes the target while database shutdown is blocked.
t_reverse_stop(_Config) ->
    started = emqx_mq_controller:wait_status(5_000),
    TestPid = self(),
    ok = meck:new(emqx_mq_message_db, [passthrough, no_history, no_link]),
    on_exit(fun() -> meck:unload(emqx_mq_message_db) end),
    ok = meck:expect(emqx_mq_message_db, close, fun() ->
        TestPid ! {shutdown_waiting, self()},
        receive
            continue -> ok
        end
    end),

    ok = emqx_mq_controller:stop_mqs(),
    Worker =
        receive
            {shutdown_waiting, Pid} -> Pid
        after 5_000 ->
            ct:fail(shutdown_not_reached)
        end,
    ok = emqx_mq_controller:start_mqs(),
    stopping = emqx_mq_controller:status(),
    Worker ! continue,
    started = emqx_mq_controller:wait_status(10_000),

    true = meck:validate(emqx_mq_message_db).

%% Verify that a worker crash starts the operation required by the latest target.
t_reconcile_worker_crash(_Config) ->
    TestPid = self(),
    ok = meck:new(emqx_mq_message_db, [passthrough, no_history, no_link]),
    on_exit(fun() -> meck:unload(emqx_mq_message_db) end),
    ok = meck:expect(emqx_mq_message_db, open, fun() ->
        TestPid ! {open_waiting, self()},
        receive
            crash -> meck:exception(error, test_worker_crash)
        end
    end),

    ControllerPid = whereis(emqx_mq_controller),
    ok = emqx_mq_controller:start_mqs(),
    Worker1 =
        receive
            {open_waiting, Pid1} -> Pid1
        after 5_000 ->
            ct:fail(open_not_reached)
        end,
    Worker1 ! crash,
    Worker2 =
        receive
            {open_waiting, Pid2} -> Pid2
        after 5_000 ->
            ct:fail(start_not_retried)
        end,
    ?assertNotEqual(Worker1, Worker2),
    ?assertEqual(ControllerPid, whereis(emqx_mq_controller)),

    ok = emqx_mq_controller:stop_mqs(),
    Worker2 ! crash,
    stopped = emqx_mq_controller:wait_status(5_000),
    ?assertEqual(ControllerPid, whereis(emqx_mq_controller)),

    true = meck:validate(emqx_mq_message_db).

%% Verify that a restarted controller resumes an interrupted shutdown.
t_restart_during_stop(_Config) ->
    started = emqx_mq_controller:wait_status(5_000),
    ok = meck:new(emqx_mq_message_db, [passthrough, no_history, no_link]),
    on_exit(fun() -> meck:unload(emqx_mq_message_db) end),
    ok = meck:expect(emqx_mq_message_db, close, fun() -> timer:sleep(infinity) end),

    ControllerPid = whereis(emqx_mq_controller),
    ?assertWaitEvent(
        {ok, _} = emqx:update_config([mq], #{<<"enable">> => false}),
        #{?snk_kind := mq_controller_worker_start, operation := stop, status := stopping},
        5_000
    ),
    stopping = emqx_mq_controller:status(),

    ?assertWaitEvent(
        exit(ControllerPid, kill),
        #{?snk_kind := mq_controller_init_cleanup, previous_status := stopping},
        5_000
    ),

    true = meck:validate(emqx_mq_message_db).

%% Verify that MQ subsystem start is idempotent and does not break MQ functioning.
t_idempotency(_Config) ->
    %% Make controller crash and start MQs twice.
    started = emqx_mq_controller:wait_status(5000),
    ControllerPid0 = whereis(emqx_mq_controller),
    ?assertWaitEvent(
        exit(ControllerPid0, kill),
        #{?snk_kind := mq_controller_start_mqs_done},
        5000
    ),
    started = emqx_mq_controller:wait_status(5000),

    %% Create a queue.
    _ = emqx_mq_test_utils:ensure_mq_created(#{
        topic_filter => <<"test/#">>, name => <<"test">>, is_lastvalue => false
    }),
    emqx_mq_test_utils:populate(10, #{topic_prefix => <<"test/">>}),

    %% Verify that queue is still working.
    CSub = emqx_mq_test_utils:emqtt_connect([]),
    emqx_mq_test_utils:emqtt_sub_mq(CSub, <<"test">>),
    {ok, _Msgs} = emqx_mq_test_utils:emqtt_drain(_MinMsg = 10, _Timeout = 1000),
    ok = emqtt:disconnect(CSub),

    %% Delete the queues.
    emqx_mq_test_utils:cleanup_mqs(),

    %% Disable MQ subsystem.
    ?assertWaitEvent(
        {ok, _} = emqx:update_config([mq], #{<<"enable">> => false}),
        #{?snk_kind := mq_controller_stop_mqs_done},
        5000
    ),
    stopped = emqx_mq_controller:wait_status(5000),

    %% Kill the controller and verify that MQ stays stopped without another stop operation.
    ControllerPid1 = whereis(emqx_mq_controller),
    exit(ControllerPid1, kill),
    ?retry(
        100,
        50,
        begin
            NewControllerPid = whereis(emqx_mq_controller),
            ?assert(is_pid(NewControllerPid)),
            ?assertNotEqual(ControllerPid1, NewControllerPid)
        end
    ),
    stopped = emqx_mq_controller:wait_status(5000).
