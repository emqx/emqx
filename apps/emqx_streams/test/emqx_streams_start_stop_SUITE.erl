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

all() ->
    emqx_common_test_helpers:all(?MODULE).

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

end_per_testcase(_TestCase, Config) ->
    ok = snabbkaffe:stop(),
    ok = emqx_cth_suite:stop(?config(suite_apps, Config)).

streams_initial_config(t_config) ->
    #{<<"streams">> => #{<<"enable">> => false}};
streams_initial_config(t_auto_no_streams) ->
    #{<<"streams">> => #{<<"enable">> => auto}};
streams_initial_config(t_auto_with_streams) ->
    #{<<"streams">> => #{<<"enable">> => true}};
streams_initial_config(t_idempotency) ->
    #{<<"streams">> => #{<<"enable">> => true}}.

%%--------------------------------------------------------------------
%% Test cases
%%--------------------------------------------------------------------

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

    %% Kill and verify that Streams subsystem is successfully stopped even if it was not started.
    ControllerPid1 = whereis(emqx_streams_controller),
    ?assertWaitEvent(
        exit(ControllerPid1, kill),
        #{?snk_kind := streams_controller_stop_streams_done},
        5000
    ),
    stopped = emqx_streams_controller:wait_status(5000).
