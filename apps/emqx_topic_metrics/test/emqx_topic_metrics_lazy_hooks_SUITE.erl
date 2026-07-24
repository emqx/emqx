%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_topic_metrics_lazy_hooks_SUITE).
-moduledoc """
The v2 topic-metrics message hooks are installed lazily: they are put
when the first collection appears on a node and deleted when the last
one is gone, so a node with no collections pays no per-message cost.
""".

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("emqx/include/emqx_config.hrl").

-define(HOOK_POINTS, ['message.publish', 'message.delivered', 'message.dropped']).

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [
            emqx_conf,
            {emqx, #{override_env => [{boot_modules, [broker]}]}},
            emqx_topic_metrics
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    Apps = ?config(apps, Config),
    emqx_cth_suite:stop(Apps),
    ok.

init_per_testcase(_Case, Config) ->
    ok = emqx_topic_metrics2:deregister_all(),
    Config.

end_per_testcase(_Case, _Config) ->
    ok = emqx_topic_metrics2:deregister_all(),
    ok.

%%--------------------------------------------------------------------
%% Cases
%%--------------------------------------------------------------------

-doc "With no collections registered, none of the message hooks are installed.".
t_no_hooks_without_metrics(_Config) ->
    assert_hooks_absent().

-doc "Registering the first collection installs the message hooks.".
t_hooks_installed_on_first_register(_Config) ->
    assert_hooks_absent(),
    ok = emqx_topic_metrics2:register(<<"alpha">>, <<"alpha/#">>, ?global_ns),
    assert_hooks_present().

-doc "Deregistering the only collection uninstalls the message hooks.".
t_hooks_removed_on_last_deregister(_Config) ->
    ok = emqx_topic_metrics2:register(<<"alpha">>, <<"alpha/#">>, ?global_ns),
    assert_hooks_present(),
    ok = emqx_topic_metrics2:deregister(<<"alpha">>, ?global_ns),
    assert_hooks_absent().

-doc "Hooks stay installed while any collection remains, and go only with the last one.".
t_hooks_follow_last_metric(_Config) ->
    ok = emqx_topic_metrics2:register(<<"alpha">>, <<"alpha/#">>, ?global_ns),
    ok = emqx_topic_metrics2:register(<<"beta">>, <<"beta/#">>, ?global_ns),
    assert_hooks_present(),
    ok = emqx_topic_metrics2:deregister(<<"alpha">>, ?global_ns),
    assert_hooks_present(),
    ok = emqx_topic_metrics2:deregister(<<"beta">>, ?global_ns),
    assert_hooks_absent().

-doc "`deregister_all/0' uninstalls the hooks once the table is emptied.".
t_hooks_removed_on_deregister_all(_Config) ->
    ok = emqx_topic_metrics2:register(<<"alpha">>, <<"alpha/#">>, ?global_ns),
    ok = emqx_topic_metrics2:register(<<"beta">>, <<"beta/#">>, ?global_ns),
    assert_hooks_present(),
    ok = emqx_topic_metrics2:deregister_all(),
    assert_hooks_absent().

-doc "Re-registering an already installed collection does not disturb the hooks.".
t_reregister_is_idempotent(_Config) ->
    ok = emqx_topic_metrics2:register(<<"alpha">>, <<"alpha/#">>, ?global_ns),
    ok = emqx_topic_metrics2:register(<<"alpha">>, <<"alpha/#">>, ?global_ns),
    assert_hooks_present(),
    ok = emqx_topic_metrics2:deregister(<<"alpha">>, ?global_ns),
    assert_hooks_absent().

-doc """
The local cluster_rpc callbacks — the code every follower node runs
when a collection is created or deleted elsewhere — toggle the hooks
on that node too.
""".
t_replicated_side_effects_toggle_hooks(_Config) ->
    Name = {?global_ns, <<"gamma">>},
    ok = emqx_topic_metrics_registry:do_install_local(Name, <<"gamma/#">>, <<"2026-07-22">>),
    assert_hooks_present(),
    ok = emqx_topic_metrics_registry:do_uninstall_local(Name),
    assert_hooks_absent().

-doc "Resetting a collection's counters leaves the hooks installed.".
t_reset_keeps_hooks(_Config) ->
    ok = emqx_topic_metrics2:register(<<"alpha">>, <<"alpha/#">>, ?global_ns),
    ok = emqx_topic_metrics2:reset(<<"alpha">>, ?global_ns),
    assert_hooks_present().

-doc "Counting still works once the hooks have been installed lazily.".
t_counting_works_after_lazy_install(_Config) ->
    ok = emqx_topic_metrics2:register(<<"alpha">>, <<"alpha/#">>, ?global_ns),
    emqx:publish(emqx_message:make(<<"alpha/1">>, <<"hello">>)),
    {ok, #{metrics := #{'messages.in.count' := In}}} =
        emqx_topic_metrics2:lookup(<<"alpha">>, ?global_ns),
    ?assertEqual(1, In).

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

assert_hooks_present() ->
    ?assertEqual(?HOOK_POINTS, hooked_points()).

assert_hooks_absent() ->
    ?assertEqual([], hooked_points()).

hooked_points() ->
    [HookPoint || HookPoint <- ?HOOK_POINTS, has_hook(HookPoint)].

%% `#callback{}' is defined in emqx_hooks.erl (not in a public header),
%% so the action is picked out positionally.
has_hook(HookPoint) ->
    lists:any(
        fun(Callback) ->
            case element(2, Callback) of
                {emqx_topic_metrics_hooks, _F, _A} -> true;
                _ -> false
            end
        end,
        emqx_hooks:lookup(HookPoint)
    ).
