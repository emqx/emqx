%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mq_startup_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("emqx/include/asserts.hrl").

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_testcase(TestCase, Config) ->
    DurableSessions = TestCase =:= t_resume_durable_while_starting,
    Apps = emqx_cth_suite:start(
        [
            emqx_conf,
            emqx_durable_storage,
            {emqx,
                emqx_mq_test_utils:cth_config(emqx, #{
                    <<"durable_sessions">> => #{<<"enable">> => DurableSessions}
                })},
            {emqx_mq,
                emqx_mq_test_utils:cth_config(emqx_mq, #{
                    <<"mq">> => #{<<"find_queue_retry_interval">> => <<"100ms">>}
                })}
        ],
        #{work_dir => emqx_cth_suite:work_dir(TestCase, Config)}
    ),
    [{suite_apps, Apps} | Config].

end_per_testcase(_TestCase, Config) ->
    lists:foreach(
        fun(Module) -> _ = catch meck:unload(Module) end,
        [
            emqx_mq_message_db,
            emqx_mq_sub,
            emqx_mq_state_storage,
            emqx_mq_registry,
            emqx_log_throttler
        ]
    ),
    emqx_cth_suite:stop(?config(suite_apps, Config)).

-doc "Fresh subscriptions attach after MQ startup without another SUBSCRIBE.".
t_subscribe_while_starting(_Config) ->
    startup_subscription(false).

-doc "Resumed subscriptions attach after MQ startup without another SUBSCRIBE.".
t_resume_while_starting(_Config) ->
    startup_subscription(true).

-doc "Durable subscriptions resume during MQ startup and attach after readiness.".
t_resume_durable_while_starting(_Config) ->
    startup_subscription(true).

startup_subscription(Resume) ->
    emqx_mq_test_utils:ensure_mq_created(#{
        name => <<"named">>, topic_filter => <<"named/#">>, is_lastvalue => false
    }),
    LegacyMQ = emqx_mq_test_utils:fill_mq_defaults(#{
        topic_filter => <<"filtered/#">>, is_lastvalue => false
    }),
    ok = emqx_mq_registry:create_pre_611_queue(LegacyMQ),
    Opts = [
        {clientid, <<"startup-subscriber">>},
        {clean_start, false},
        {properties, #{'Session-Expiry-Interval' => 300}}
    ],
    Topics = [{<<"$queue/named">>, 1}, {<<"$q/filtered/#">>, 1}],
    case Resume of
        true ->
            C0 = connect(Opts),
            {ok, _, [1, 1]} = emqtt:subscribe(C0, Topics),
            ok = emqtt:disconnect(C0);
        false ->
            ok
    end,
    emqx_mq_test_utils:populate(1, #{topic_prefix => <<"named/">>}),
    emqx_mq_test_utils:populate(1, #{topic_prefix => <<"filtered/">>}),
    Controller = restart_and_hold_readiness(),
    try
        C = connect(Opts),
        case Resume of
            false -> {ok, _, [1, 1]} = emqtt:subscribe(C, Topics);
            true -> ok
        end,
        ?assertEqual({ok, []}, emqx_mq_test_utils:emqtt_drain(0, 300)),
        Controller ! continue,
        started = emqx_mq_controller:wait_status(5000),
        {ok, Messages} = emqx_mq_test_utils:emqtt_drain(2, 5000),
        ?assertEqual(
            [<<"filtered/0">>, <<"named/0">>],
            lists:sort([Topic || #{topic := Topic} <- Messages])
        ),
        ok = emqtt:disconnect(C)
    after
        Controller ! continue
    end.

-doc "Subscriptions made during startup retry sooner than `find_queue_retry_interval`.".
t_short_retry_while_starting(_Config) ->
    {ok, _} = emqx:update_config([mq, find_queue_retry_interval], <<"10s">>),
    emqx_mq_test_utils:ensure_mq_created(#{
        name => <<"named">>, topic_filter => <<"named/#">>, is_lastvalue => false
    }),
    emqx_mq_test_utils:populate(1, #{topic_prefix => <<"named/">>}),
    Controller = restart_and_hold_readiness(),
    try
        C = connect([]),
        {ok, _, [1]} = emqtt:subscribe(C, {<<"$queue/named">>, 1}),
        Controller ! continue,
        started = emqx_mq_controller:wait_status(5000),
        %% With the configured 10 s interval, nothing arrives within this window.
        {ok, [_]} = emqx_mq_test_utils:emqtt_drain(1, 3000),
        ok = emqtt:disconnect(C)
    after
        Controller ! continue
    end.

-doc "Discovery skips auto-creation until MQ reports readiness.".
t_defer_auto_create(_Config) ->
    enable_auto_create(),
    ok = meck:new(emqx_mq_sub, [passthrough, no_link, non_strict]),
    ok = meck:expect(emqx_mq_sub, mq_status, fun() -> starting end),
    C = connect([]),
    {ok, _, [1]} = emqtt:subscribe(C, {<<"$queue/deferred/deferred/#">>, 1}),
    ?assertEqual(not_found, emqx_mq_registry:find(<<"deferred">>)),
    ok = meck:expect(emqx_mq_sub, mq_status, fun() -> stopped end),
    ?assertEqual({ok, []}, emqx_mq_test_utils:emqtt_drain(0, 300)),
    ?assertEqual(not_found, emqx_mq_registry:find(<<"deferred">>)),
    ok = meck:expect(emqx_mq_sub, mq_status, fun() -> started end),
    ?retry(100, 50, ?assertMatch({ok, _}, emqx_mq_registry:find(<<"deferred">>))),
    emqx_mq_test_utils:populate(1, #{topic_prefix => <<"deferred/">>}),
    {ok, [_]} = emqx_mq_test_utils:emqtt_drain(1, 5000),
    ok = emqtt:disconnect(C).

-doc "Auto-creation and consumer startup failures retry despite a ready status.".
t_retry_after_ready_race(_Config) ->
    enable_auto_create(),
    ok = meck:new(emqx_mq_sub, [passthrough, no_link, non_strict]),
    ok = meck:expect(emqx_mq_sub, mq_status, fun() -> started end),
    ok = meck:new(emqx_mq_state_storage, [passthrough, no_link]),
    ok = meck:expect(emqx_mq_state_storage, create_mq_state, fun(_) ->
        {error, {recoverable, startup_race}}
    end),
    ok = meck:expect(emqx_mq_state_storage, open_consumer_state, fun(_) ->
        {error, {recoverable, startup_race}}
    end),
    C = connect([]),
    {ok, _, [1]} = emqtt:subscribe(C, {<<"$queue/race/race/#">>, 1}),
    ?retry(
        100, 50, ?assert(meck:num_calls(emqx_mq_state_storage, create_mq_state, '_') >= 2)
    ),
    ok = meck:expect(emqx_mq_state_storage, create_mq_state, fun(MQ) -> meck:passthrough([MQ]) end),
    ?retry(
        100, 50, ?assert(meck:num_calls(emqx_mq_state_storage, open_consumer_state, '_') >= 2)
    ),
    ok = meck:unload(emqx_mq_state_storage),
    emqx_mq_test_utils:populate(1, #{topic_prefix => <<"race/">>}),
    {ok, [_]} = emqx_mq_test_utils:emqtt_drain(1, 5000),
    ok = emqtt:disconnect(C).

-doc "Repeated auto-create failures are logged through the log throttler.".
t_auto_create_error_throttled(_Config) ->
    enable_auto_create(),
    ok = meck:new(emqx_mq_state_storage, [passthrough, no_link]),
    ok = meck:expect(emqx_mq_state_storage, create_mq_state, fun(_) ->
        {error, {recoverable, persistent_failure}}
    end),
    ok = meck:new(emqx_log_throttler, [passthrough, no_link]),
    C = connect([]),
    {ok, _, [1]} = emqtt:subscribe(C, {<<"$queue/failing/failing/#">>, 1}),
    ?retry(
        100, 50, ?assert(meck:num_calls(emqx_mq_state_storage, create_mq_state, '_') >= 3)
    ),
    Results = [
        Allowed
     || {_Pid, {emqx_log_throttler, allow, [mq_sub_auto_create_error, _]}, Allowed} <-
            meck:history(emqx_log_throttler)
    ],
    %% The first error is logged; the following ones within the time window are dropped.
    ?assertMatch([true, false | _], Results),
    ?assertNot(lists:member(true, tl(Results))),
    ok = emqtt:disconnect(C).

-doc "Discovery for `$q/` subscriptions does not try to auto-create a legacy queue.".
t_no_auto_create_for_legacy_name(_Config) ->
    enable_auto_create(),
    ok = meck:new(emqx_mq_registry, [passthrough, no_link]),
    C = connect([]),
    {ok, _, [1]} = emqtt:subscribe(C, {<<"$q/legacy/#">>, 1}),
    %% The retry interval is 100 ms, so discovery runs several times.
    ?retry(100, 50, ?assert(meck:num_calls(emqx_mq_registry, find, '_') >= 3)),
    ?assertEqual(0, meck:num_calls(emqx_mq_registry, create, '_')),
    ok = emqtt:disconnect(C).

%% Restart the MQ application and block its startup before DB readiness.
%% Send `continue` to the returned controller pid to let the startup complete.
restart_and_hold_readiness() ->
    ok = application:stop(emqx_mq),
    Parent = self(),
    ok = meck:new(emqx_mq_message_db, [passthrough, no_link]),
    ok = meck:expect(emqx_mq_message_db, wait_readiness, fun(infinity) ->
        Parent ! {waiting, self()},
        receive
            continue -> meck:passthrough([infinity])
        end
    end),
    ok = application:start(emqx_mq),
    Controller =
        receive
            {waiting, Pid} -> Pid
        after 5000 -> ct:fail(startup_timeout)
        end,
    ?assertEqual(starting, emqx_mq_controller:status()),
    Controller.

enable_auto_create() ->
    {ok, _} = emqx:update_config([mq, auto_create], #{
        <<"regular">> => #{}, <<"lastvalue">> => false
    }),
    ok.

connect(Opts) ->
    Client = emqx_mq_test_utils:emqtt_connect(Opts),
    unlink(Client),
    Client.
