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

all() ->
    emqx_common_test_helpers:all(?MODULE).

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

end_per_testcase(_TestCase, Config) ->
    ok = snabbkaffe:stop(),
    ok = emqx_cth_suite:stop(?config(suite_apps, Config)).

mq_initial_config(t_config) ->
    #{<<"mq">> => #{<<"enable">> => false}};
mq_initial_config(t_auto_no_queues) ->
    #{<<"mq">> => #{<<"enable">> => auto}};
mq_initial_config(t_auto_with_queues) ->
    #{<<"mq">> => #{<<"enable">> => true}};
mq_initial_config(t_idempotency) ->
    #{<<"mq">> => #{<<"enable">> => true}}.

%%--------------------------------------------------------------------
%% Test cases
%%--------------------------------------------------------------------

%% Verify that MQ subsystem may be started in runtime.
t_config(_Config) ->
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

    %% Start MQ subsystem via API again.
    ?assertMatch(
        {ok, 204},
        api_put([message_queues, config], #{<<"enable">> => true})
    ),
    started = emqx_mq_controller:wait_status(5000),

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

    %% Kill and verify that MQ subsystem is successfully stopped even if it was not started.
    ControllerPid1 = whereis(emqx_mq_controller),
    ?assertWaitEvent(
        exit(ControllerPid1, kill),
        #{?snk_kind := mq_controller_stop_mqs_done},
        5000
    ),
    stopped = emqx_mq_controller:wait_status(5000).
