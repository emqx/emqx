%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_streams_start_stop_SUITE).

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
        {emqx, emqx_streams_test_utils:cth_config(emqx)},
        {emqx_mq, emqx_streams_test_utils:cth_config(emqx_mq)},
        {emqx_streams, #{config => streams_initial_config(TestCase)}},
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
            {emqx, emqx_streams_test_utils:cth_config(emqx)},
            {emqx_mq, emqx_streams_test_utils:cth_config(emqx_mq)},
            {emqx_streams, #{config => streams_initial_config(TestCase)}},
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

streams_initial_config(t_config) ->
    #{<<"streams">> => #{<<"enable">> => false}};
streams_initial_config(t_auto_no_streams) ->
    #{<<"streams">> => #{<<"enable">> => auto}};
streams_initial_config(t_auto_with_streams) ->
    #{<<"streams">> => #{<<"enable">> => true}};
streams_initial_config(t_idempotency) ->
    #{<<"streams">> => #{<<"enable">> => true}};
streams_initial_config(t_reverse_start) ->
    #{<<"streams">> => #{<<"enable">> => false}};
streams_initial_config(t_reverse_stop) ->
    #{<<"streams">> => #{<<"enable">> => true}};
streams_initial_config(t_reconcile_worker_crash) ->
    #{<<"streams">> => #{<<"enable">> => false}};
streams_initial_config(t_restart_during_stop) ->
    #{<<"streams">> => #{<<"enable">> => true}};
streams_initial_config(t_cluster_runtime_enable) ->
    #{<<"streams">> => #{<<"enable">> => false}}.

%%--------------------------------------------------------------------
%% Test cases
%%--------------------------------------------------------------------

%% Verify that enabling and disabling streams at runtime completes on all nodes.
t_cluster_runtime_enable(Config) ->
    [N1 | _] = Nodes = ?config(cluster_nodes, Config),

    %% Enable streams via config (multicalls to all nodes)
    {ok, _} = erpc:call(N1, emqx_streams_config, update_config, [
        #{<<"enable">> => true}
    ]),

    %% Wait for all nodes to reach ready
    Timeout = 10_000,
    ?assertEqual(
        [{ok, started} || _ <- Nodes],
        erpc:multicall(Nodes, emqx_streams_controller, wait_status, [Timeout])
    ),

    %% Verify status on each node
    ?assertEqual(
        [{ok, started} || _ <- Nodes],
        erpc:multicall(Nodes, emqx_streams_controller, status, [])
    ),

    %% Disable streams without waiting for database shutdown in the config handler.
    {ok, _} = erpc:call(
        N1,
        emqx_streams_config,
        update_config,
        [#{<<"enable">> => false}],
        5_000
    ),

    %% A new target must be accepted while shutdown is still in progress.
    {ok, _} = erpc:call(
        N1,
        emqx_streams_config,
        update_config,
        [#{<<"enable">> => true}],
        5_000
    ),

    TransitionTimeout = 30_000,
    ?assertEqual(
        [{ok, started} || _ <- Nodes],
        erpc:multicall(Nodes, emqx_streams_controller, wait_status, [TransitionTimeout])
    ),

    %% Disable once more and wait for asynchronous shutdown on all nodes.
    {ok, _} = erpc:call(
        N1,
        emqx_streams_config,
        update_config,
        [#{<<"enable">> => false}],
        5_000
    ),

    ?assertEqual(
        [{ok, stopped} || _ <- Nodes],
        erpc:multicall(Nodes, emqx_streams_controller, wait_status, [TransitionTimeout])
    ),

    ?assertEqual(
        [{ok, stopped} || _ <- Nodes],
        erpc:multicall(Nodes, emqx_streams_controller, status, [])
    ).

%% Verify that Streams subsystem may be started in runtime.
t_config(_Config) ->
    %% We started with disabled Streams subsystem, so stream API should be unavailable.
    ?assertMatch(
        {ok, 503, #{<<"code">> := <<"SERVICE_UNAVAILABLE">>, <<"message">> := <<"Not enabled">>}},
        api_get([message_streams, streams])
    ),

    %% Start Streams subsystem via API.
    ?assertMatch(
        {ok, 204},
        api_put([message_streams, config], #{<<"enable">> => true})
    ),
    started = emqx_streams_controller:wait_status(5000),
    ok = emqx_streams_controller:start_streams(),

    %% Verify that stream API is now available.
    ?assertMatch(
        {ok, 200, _},
        api_get([message_streams, streams])
    ),

    %% Verify that we can disable Streams subsystem via API if no streams exist.
    ?assertMatch(
        {ok, 204},
        api_put([message_streams, config], #{<<"enable">> => false})
    ),
    stopped = emqx_streams_controller:wait_status(5000),
    ok = emqx_streams_controller:stop_streams(),

    %% Start Streams subsystem via API again.
    ?assertMatch(
        {ok, 204},
        api_put([message_streams, config], #{<<"enable">> => true})
    ),
    started = emqx_streams_controller:wait_status(5000),

    %% Create a stream.
    _ = emqx_streams_test_utils:ensure_stream_created(#{
        topic_filter => <<"test/#">>, name => <<"test">>
    }),

    %% Verify that we cannot disable Streams subsystem via API if any streams exist.
    ?assertMatch(
        {ok, 400, #{
            <<"code">> := <<"BAD_REQUEST">>,
            <<"message">> :=
                <<"Cannot disable streams subsystem via API when there are existing streams">>
        }},
        api_put([message_streams, config], #{<<"enable">> => false})
    ).

%% Verify that auto starts Streams when there are streams.
t_auto_with_streams(_Config) ->
    %% Create a stream.
    _ = emqx_streams_test_utils:ensure_stream_created(#{
        topic_filter => <<"test/#">>, name => <<"test">>
    }),
    {ok, _} = emqx:update_config([streams], #{<<"enable">> => auto}),

    %% Stop/start Streams subsystem.
    ok = application:stop(emqx_streams),
    ok = application:start(emqx_streams),
    started = emqx_streams_controller:wait_status(5000).

%% Verify that auto does not start Streams when there are no streams.
t_auto_no_streams(_Config) ->
    stopped = emqx_streams_controller:wait_status(5000).

%% Verify that disable waits for startup before stopping.
t_reverse_start(_Config) ->
    TestPid = self(),
    ok = meck:new(emqx_streams_message_db, [passthrough, no_history, no_link]),
    on_exit(fun() -> meck:unload(emqx_streams_message_db) end),
    ok = meck:expect(emqx_streams_message_db, wait_readiness, fun(infinity) ->
        TestPid ! {readiness_waiting, self()},
        receive
            continue -> ok
        end
    end),

    ok = emqx_streams_controller:start_streams(),
    Worker =
        receive
            {readiness_waiting, Pid} -> Pid
        after 5_000 ->
            ct:fail(readiness_not_reached)
        end,
    ok = emqx_streams_controller:stop_streams(),
    starting = emqx_streams_controller:status(),
    Worker ! continue,
    stopped = emqx_streams_controller:wait_status(5_000),

    true = meck:validate(emqx_streams_message_db).

%% Verify that enable changes the target while database shutdown is blocked.
t_reverse_stop(_Config) ->
    started = emqx_streams_controller:wait_status(5_000),
    TestPid = self(),
    ok = meck:new(emqx_streams_message_db, [passthrough, no_history, no_link]),
    on_exit(fun() -> meck:unload(emqx_streams_message_db) end),
    ok = meck:expect(emqx_streams_message_db, close, fun() ->
        TestPid ! {shutdown_waiting, self()},
        receive
            continue -> ok
        end
    end),

    ok = emqx_streams_controller:stop_streams(),
    Worker =
        receive
            {shutdown_waiting, Pid} -> Pid
        after 5_000 ->
            ct:fail(shutdown_not_reached)
        end,
    ok = emqx_streams_controller:start_streams(),
    stopping = emqx_streams_controller:status(),
    Worker ! continue,
    started = emqx_streams_controller:wait_status(10_000),

    true = meck:validate(emqx_streams_message_db).

%% Verify that a worker crash starts the operation required by the latest target.
t_reconcile_worker_crash(_Config) ->
    TestPid = self(),
    ok = meck:new(emqx_streams_message_db, [passthrough, no_history, no_link]),
    on_exit(fun() -> meck:unload(emqx_streams_message_db) end),
    ok = meck:expect(emqx_streams_message_db, open, fun() ->
        TestPid ! {open_waiting, self()},
        receive
            crash -> meck:exception(error, test_worker_crash)
        end
    end),

    ControllerPid = whereis(emqx_streams_controller),
    ok = emqx_streams_controller:start_streams(),
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
    ?assertEqual(ControllerPid, whereis(emqx_streams_controller)),

    ok = emqx_streams_controller:stop_streams(),
    Worker2 ! crash,
    stopped = emqx_streams_controller:wait_status(5_000),
    ?assertEqual(ControllerPid, whereis(emqx_streams_controller)),

    true = meck:validate(emqx_streams_message_db).

%% Verify that a restarted controller resumes an interrupted shutdown.
t_restart_during_stop(_Config) ->
    started = emqx_streams_controller:wait_status(5_000),
    ok = meck:new(emqx_streams_message_db, [passthrough, no_history, no_link]),
    on_exit(fun() -> meck:unload(emqx_streams_message_db) end),
    ok = meck:expect(emqx_streams_message_db, close, fun() -> timer:sleep(infinity) end),

    ControllerPid = whereis(emqx_streams_controller),
    ?assertWaitEvent(
        {ok, _} = emqx:update_config([streams], #{<<"enable">> => false}),
        #{?snk_kind := streams_controller_worker_start, operation := stop, status := stopping},
        5_000
    ),
    stopping = emqx_streams_controller:status(),

    ?assertWaitEvent(
        exit(ControllerPid, kill),
        #{?snk_kind := streams_controller_init_cleanup, previous_status := stopping},
        5_000
    ),

    true = meck:validate(emqx_streams_message_db).

%% Verify that Streams subsystem start is idempotent and does not break Streams functioning.
t_idempotency(_Config) ->
    %% Make controller crash and start Streams twice.
    started = emqx_streams_controller:wait_status(5000),
    ControllerPid0 = whereis(emqx_streams_controller),
    ?assertWaitEvent(
        exit(ControllerPid0, kill),
        #{?snk_kind := streams_controller_start_streams_done},
        5000
    ),
    started = emqx_streams_controller:wait_status(5000),

    %% Create a stream and publish messages.
    _ = emqx_streams_test_utils:ensure_stream_created(#{
        topic_filter => <<"test/#">>, name => <<"test">>, is_lastvalue => false
    }),
    emqx_streams_test_utils:populate(10, #{topic_prefix => <<"test/">>}),

    %% Verify that stream is still working.
    CSub = emqx_streams_test_utils:emqtt_connect([]),
    emqx_streams_test_utils:emqtt_sub(CSub, <<"$stream/test/test/#">>, [
        {<<"stream-offset">>, <<"earliest">>}
    ]),
    {ok, _Msgs} = emqx_streams_test_utils:emqtt_drain(_MinMsg = 10, _Timeout = 1000),
    ok = emqtt:disconnect(CSub),

    %% Delete the streams.
    emqx_streams_test_utils:cleanup_streams(),

    %% Disable Streams subsystem.
    ?assertWaitEvent(
        {ok, _} = emqx:update_config([streams], #{<<"enable">> => false}),
        #{?snk_kind := streams_controller_stop_streams_done},
        5000
    ),
    stopped = emqx_streams_controller:wait_status(5000),

    %% Kill the controller and verify that Streams stay stopped without another stop operation.
    ControllerPid1 = whereis(emqx_streams_controller),
    exit(ControllerPid1, kill),
    ?retry(
        100,
        50,
        begin
            NewControllerPid = whereis(emqx_streams_controller),
            ?assert(is_pid(NewControllerPid)),
            ?assertNotEqual(ControllerPid1, NewControllerPid)
        end
    ),
    stopped = emqx_streams_controller:wait_status(5000).
