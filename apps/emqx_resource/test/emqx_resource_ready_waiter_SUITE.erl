%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_resource_ready_waiter_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-import(emqx_common_test_helpers, [on_exit/1]).

-define(WAITER, emqx_resource_ready_waiter).

%%------------------------------------------------------------------------------
%% CT boilerplate
%%------------------------------------------------------------------------------

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(TCConfig) ->
    Apps = emqx_cth_suite:start(
        [emqx, emqx_resource],
        #{work_dir => emqx_cth_suite:work_dir(TCConfig)}
    ),
    [{apps, Apps} | TCConfig].

end_per_suite(TCConfig) ->
    emqx_cth_suite:stop(?config(apps, TCConfig)).

init_per_testcase(_TestCase, TCConfig) ->
    snabbkaffe:start_trace(),
    TCConfig.

end_per_testcase(_TestCase, _TCConfig) ->
    emqx_common_test_helpers:call_janitor(),
    true = ets:delete_all_objects(?WAITER),
    snabbkaffe:stop(),
    ok.

%%------------------------------------------------------------------------------
%% Deferred starts
%%------------------------------------------------------------------------------

notify(Pid, Tag) ->
    Pid ! {started, Tag},
    ok.

fail_first(Pid, Tag, Counter, How) ->
    case counters:get(Counter, 1) of
        0 ->
            ok = counters:add(Counter, 1, 1),
            case How of
                error -> {error, boom};
                raise -> error(boom)
            end;
        _ ->
            notify(Pid, Tag)
    end.

defer_again(Pid, Key) ->
    ok = emqx_node_readiness:mark_not_ready(),
    deferred = ?WAITER:when_ready(Key, {?MODULE, notify, [Pid, second]}),
    notify(Pid, first).

cancel_other(Pid, Tag, OtherKey) ->
    ok = ?WAITER:cancel(OtherKey),
    notify(Pid, Tag).

bad_return(Pid, Tag) ->
    ok = notify(Pid, Tag),
    foo.

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

key(Name) ->
    {?MODULE, Name}.

mark_not_ready() ->
    on_exit(fun emqx_node_readiness:mark_ready/0),
    ok = emqx_node_readiness:mark_not_ready().

mark_ready_and_wait_run() ->
    ?wait_async_action(
        emqx_node_readiness:mark_ready(),
        #{?snk_kind := resource_ready_waiter_ran},
        5_000
    ).

assert_started(Tag) ->
    receive
        {started, Tag} -> ok
    after 5_000 -> ct:fail({not_started, Tag})
    end.

assert_not_started() ->
    receive
        {started, _} = Msg -> ct:fail({unexpected, Msg})
    after 0 -> ok
    end.

waiter_state() ->
    sys:get_state(?WAITER).

%%------------------------------------------------------------------------------
%% Test cases
%%------------------------------------------------------------------------------

-doc """
A start deferred while the node is not ready runs once, after the node is
ready; a start requested once the node is ready is left to the caller.
""".
t_runs_once_node_is_ready(_TCConfig) ->
    Key = key(?FUNCTION_NAME),
    Self = self(),
    ?assertEqual(now, ?WAITER:when_ready(Key, {?MODULE, notify, [Self, ready]})),
    mark_not_ready(),
    ?assertEqual(deferred, ?WAITER:when_ready(Key, {?MODULE, notify, [Self, deferred]})),
    assert_not_started(),
    ?assertMatch({ok, {ok, #{keys := [Key]}}}, mark_ready_and_wait_run()),
    assert_started(deferred),
    ?assertEqual([], ets:lookup(?WAITER, Key)),
    assert_not_started(),
    ok.

-doc "A second start deferred under the same key replaces the first.".
t_same_key_replaces(_TCConfig) ->
    Key = key(?FUNCTION_NAME),
    Self = self(),
    mark_not_ready(),
    deferred = ?WAITER:when_ready(Key, {?MODULE, notify, [Self, first]}),
    deferred = ?WAITER:when_ready(Key, {?MODULE, notify, [Self, second]}),
    ?assertMatch({ok, {ok, #{keys := [Key]}}}, mark_ready_and_wait_run()),
    assert_started(second),
    assert_not_started(),
    ok.

-doc "A start that returns an error is retried until it returns `ok`.".
t_failed_start_retried(_TCConfig) ->
    retried_start(?FUNCTION_NAME, error).

-doc "A start that raises is retried until it returns `ok`.".
t_crashed_start_retried(_TCConfig) ->
    retried_start(?FUNCTION_NAME, raise).

retried_start(TestCase, How) ->
    Key = key(TestCase),
    Pid = whereis(?WAITER),
    mark_not_ready(),
    Counter = counters:new(1, []),
    deferred = ?WAITER:when_ready(Key, {?MODULE, fail_first, [self(), TestCase, Counter, How]}),
    {ok, SRef} = snabbkaffe:subscribe(
        ?match_event(#{?snk_kind := resource_ready_waiter_ran, keys := [Key]}),
        2,
        10_000
    ),
    ok = emqx_node_readiness:mark_ready(),
    ?assertMatch({ok, [_, _]}, snabbkaffe:receive_events(SRef)),
    assert_started(TestCase),
    ?assertEqual([], ets:lookup(?WAITER, Key)),
    ?assertEqual(Pid, whereis(?WAITER)),
    ok.

-doc """
A start that defers itself again under the same key while it runs keeps the new
entry, which runs once the node is ready again.
""".
t_kept_again_during_call(_TCConfig) ->
    Key = key(?FUNCTION_NAME),
    Self = self(),
    mark_not_ready(),
    deferred = ?WAITER:when_ready(Key, {?MODULE, defer_again, [Self, Key]}),
    ?assertMatch({ok, {ok, #{keys := [Key]}}}, mark_ready_and_wait_run()),
    assert_started(first),
    ?assertMatch([{Key, {?MODULE, notify, [Self, second]}, _}], ets:lookup(?WAITER, Key)),
    assert_not_started(),
    ?assertMatch({ok, {ok, #{keys := [Key]}}}, mark_ready_and_wait_run()),
    assert_started(second),
    ?assertEqual([], ets:lookup(?WAITER, Key)),
    ok.

-doc "Starts deferred before the waiter restarts run after the restart.".
t_entries_survive_waiter_restart(_TCConfig) ->
    Key = key(?FUNCTION_NAME),
    mark_not_ready(),
    deferred = ?WAITER:when_ready(Key, {?MODULE, notify, [self(), restarted]}),
    Pid0 = whereis(?WAITER),
    MRef = monitor(process, Pid0),
    exit(Pid0, kill),
    receive
        {'DOWN', MRef, process, Pid0, killed} -> ok
    after 1_000 -> ct:fail(waiter_not_killed)
    end,
    ?retry(
        100,
        20,
        ?assertMatch(Pid when is_pid(Pid) andalso Pid =/= Pid0, whereis(?WAITER))
    ),
    ?assertMatch({ok, {ok, #{keys := [Key]}}}, mark_ready_and_wait_run()),
    assert_started(restarted),
    ok.

-doc """
A cancelled start does not run once the node is ready, and the waiter goes idle
when no start is left.
""".
t_cancel(_TCConfig) ->
    Key = key(?FUNCTION_NAME),
    Barrier = key(barrier),
    Self = self(),
    mark_not_ready(),
    deferred = ?WAITER:when_ready(Key, {?MODULE, notify, [Self, cancelled]}),
    ?assertEqual(armed, waiter_state()),
    ok = ?WAITER:cancel(Key),
    ?retry(100, 10, ?assertEqual(idle, waiter_state())),
    deferred = ?WAITER:when_ready(Barrier, {?MODULE, notify, [Self, barrier]}),
    ?assertMatch({ok, {ok, #{keys := [Barrier]}}}, mark_ready_and_wait_run()),
    assert_started(barrier),
    assert_not_started(),
    ok.

-doc "A start cancelled by an earlier start in the same pass does not run.".
t_cancel_during_pass(_TCConfig) ->
    KeyA = key(a),
    KeyB = key(b),
    Self = self(),
    mark_not_ready(),
    deferred = ?WAITER:when_ready(KeyA, {?MODULE, cancel_other, [Self, a, KeyB]}),
    deferred = ?WAITER:when_ready(KeyB, {?MODULE, cancel_other, [Self, b, KeyA]}),
    {ok, {ok, #{keys := Keys}}} = mark_ready_and_wait_run(),
    ?assertMatch([_], Keys),
    receive
        {started, _} -> ok
    after 5_000 -> ct:fail(not_started)
    end,
    assert_not_started(),
    ?assertEqual([], ets:tab2list(?WAITER)),
    ok.

-doc "A start with an unexpected return value runs once and is dropped.".
t_bad_return_dropped(_TCConfig) ->
    Key = key(?FUNCTION_NAME),
    Pid = whereis(?WAITER),
    mark_not_ready(),
    deferred = ?WAITER:when_ready(Key, {?MODULE, bad_return, [self(), bad]}),
    ?assertMatch({ok, {ok, #{keys := [Key]}}}, mark_ready_and_wait_run()),
    assert_started(bad),
    ?assertEqual([], ets:lookup(?WAITER, Key)),
    ?assertEqual(idle, waiter_state()),
    ?assertEqual(Pid, whereis(?WAITER)),
    ok.

-doc """
The waiter has no timer running while no start is deferred, even when the node
is not ready.
""".
t_idle_without_entries(_TCConfig) ->
    Key = key(?FUNCTION_NAME),
    ?assertEqual(idle, waiter_state()),
    mark_not_ready(),
    ?assertEqual(idle, waiter_state()),
    deferred = ?WAITER:when_ready(Key, {?MODULE, notify, [self(), idle]}),
    ?assertEqual(armed, waiter_state()),
    ?assertMatch({ok, {ok, #{keys := [Key]}}}, mark_ready_and_wait_run()),
    assert_started(idle),
    ?assertEqual(idle, waiter_state()),
    ok.
