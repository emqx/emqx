%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_plugins_install_serializer_tests).
-include_lib("eunit/include/eunit.hrl").

-define(SERIALIZER, emqx_plugins_install_serializer).

busy_callers_can_retry_test_() ->
    [
        ?_test(with_serializer(fun() -> assert_busy(Name) end))
     || Name <- [
            "plugin_a-1.0", <<"plugin_a-1.0">>, "plugin_b-2.0"
        ]
    ].

assert_busy(Name) ->
    Holder = hold_lock(),
    try
        ?assertEqual(#{owner => Holder, waiting => []}, ?SERIALIZER:lock_status()),
        ?assertMatch(
            {error, #{reason := installation_in_progress}},
            ?SERIALIZER:run(Name, fun() -> error(unexpected_callback) end)
        ),
        ?assertEqual(#{owner => Holder, waiting => []}, ?SERIALIZER:lock_status()),
        release_holder(Holder),
        ?assertEqual(ok, ?SERIALIZER:run(Name, fun() -> ok end))
    after
        exit(Holder, kill)
    end.

holder_exit_releases_lock_test() ->
    with_serializer(fun() ->
        Holder = hold_lock(),
        exit(Holder, kill),
        wait_until_free(100),
        ?assertEqual(ok, ?SERIALIZER:run("plugin_a-1.0", fun() -> ok end))
    end).

unknown_release_preserves_holder_test() ->
    with_serializer(fun() ->
        Holder = hold_lock(),
        try
            ok = ?SERIALIZER:release_lock(make_ref()),
            ?assertEqual(Holder, maps:get(owner, ?SERIALIZER:lock_status())),
            ?assertMatch(
                {error, #{reason := installation_in_progress}},
                ?SERIALIZER:run("plugin_b-1.0", fun() -> error(unexpected_callback) end)
            ),
            release_holder(Holder)
        after
            exit(Holder, kill)
        end
    end).

callback_exception_releases_lock_test() ->
    with_serializer(fun() ->
        ?assertThrow(boom, ?SERIALIZER:run("plugin_a-1.0", fun() -> throw(boom) end)),
        ?assertEqual(ok, ?SERIALIZER:run("plugin_a-1.0", fun() -> ok end))
    end).

callback_result_test() ->
    with_serializer(fun() ->
        ?assertEqual({ok, 42}, ?SERIALIZER:run("plugin_a-1.0", fun() -> {ok, 42} end))
    end).

%% The steps of one installation are composed by taking the lock once around the
%% whole sequence: the nested calls of that process run in the same critical
%% section instead of failing with a busy result.
nested_call_runs_in_the_same_critical_section_test() ->
    with_serializer(fun() ->
        ?assertEqual(
            ok,
            ?SERIALIZER:run("plugin_a-1.0", fun() ->
                ?assertEqual(
                    ok,
                    ?SERIALIZER:run("plugin_a-1.0", fun() -> ok end)
                )
            end)
        ),
        ?assertEqual(ok, ?SERIALIZER:run("plugin_a-1.0", fun() -> ok end))
    end).

%% A nested call which fails releases the lock with the outer one, and the
%% failure is returned to the caller.
nested_call_failure_releases_lock_test() ->
    with_serializer(fun() ->
        ?assertThrow(
            nested_boom,
            ?SERIALIZER:run("plugin_a-1.0", fun() ->
                ?SERIALIZER:run("plugin_b-1.0", fun() -> throw(nested_boom) end)
            end)
        ),
        ?assertEqual(ok, ?SERIALIZER:run("plugin_a-1.0", fun() -> ok end))
    end).

missing_serializer_returns_error_test() ->
    stop_serializer(),
    ?assertMatch(
        {error, #{msg := "failed_to_acquire_plugin_install_lock"}},
        ?SERIALIZER:run("plugin_a-1.0", fun() -> error(unexpected_callback) end)
    ).

%% The lock is handed out by the coordinator, which is a core node, so the
%% server of a replicant node is not started at all.
replicant_node_does_not_start_the_server_test() ->
    stop_serializer(),
    ok = meck:new(mria_rlog, [passthrough]),
    try
        ok = meck:expect(mria_rlog, role, fun() -> replicant end),
        ?assertEqual(ignore, ?SERIALIZER:start_link()),
        ?assertEqual(undefined, whereis(?SERIALIZER))
    after
        ok = meck:unload(mria_rlog)
    end.

unavailable_coordinator_returns_error_test() ->
    with_serializer(fun() ->
        ok = meck:new(mria_membership, [passthrough]),
        try
            ok = meck:expect(mria_membership, coordinator, fun() -> 'no-such-node@nowhere' end),
            ?assertMatch(
                {error, #{msg := "failed_to_acquire_plugin_install_lock"}},
                ?SERIALIZER:run("plugin_a-1.0", fun() -> error(unexpected_callback) end)
            ),
            ?assertEqual(undefined, maps:get(owner, ?SERIALIZER:lock_status()))
        after
            ok = meck:unload(mria_membership)
        end
    end).

hold_lock() ->
    Parent = self(),
    Holder = spawn(fun() ->
        Result = ?SERIALIZER:run("plugin_a-1.0", fun() ->
            Parent ! {entered, self()},
            receive
                release -> ok
            end
        end),
        Parent ! {result, self(), Result}
    end),
    receive
        {entered, Holder} -> Holder
    after 5000 ->
        exit(Holder, kill),
        error(holder_not_started)
    end.

release_holder(Holder) ->
    Holder ! release,
    receive
        {result, Holder, ok} -> ok
    after 5000 -> error(holder_not_released)
    end.

wait_until_free(0) ->
    error(lock_not_released);
wait_until_free(Attempts) ->
    case maps:get(owner, ?SERIALIZER:lock_status()) of
        undefined ->
            ok;
        _ ->
            timer:sleep(10),
            wait_until_free(Attempts - 1)
    end.

with_serializer(Fun) ->
    stop_serializer(),
    {ok, Pid} = ?SERIALIZER:start_link(),
    try
        Fun()
    after
        gen_server:stop(Pid)
    end.

stop_serializer() ->
    case whereis(?SERIALIZER) of
        undefined -> ok;
        Pid -> gen_server:stop(Pid)
    end.
