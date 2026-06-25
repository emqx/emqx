%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_topic_metrics_registry_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("emqx/include/emqx_config.hrl").
-include("../include/emqx_topic_metrics.hrl").

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

t_register_and_lookup(_Config) ->
    ?assertEqual({error, not_found}, emqx_topic_metrics2:lookup(<<"alpha">>, ?global_ns)),
    ok = emqx_topic_metrics2:register(<<"alpha">>, <<"alpha/#">>, ?global_ns),
    {ok, Rec} = emqx_topic_metrics2:lookup(<<"alpha">>, ?global_ns),
    ?assertMatch(
        #{
            bin_name := <<"alpha">>,
            owner_ns := ?global_ns,
            topic_filter := <<"alpha/#">>,
            metrics := #{'messages.in.count' := 0}
        },
        Rec
    ).

t_register_duplicate_is_idempotent(_Config) ->
    %% Registering the same name twice is a no-op at the registry layer.
    ok = emqx_topic_metrics2:register(<<"a">>, <<"a/#">>, ?global_ns),
    ok = emqx_topic_metrics2:register(<<"a">>, <<"a/#">>, ?global_ns),
    {ok, #{counter_ref := CRef}} = emqx_topic_metrics_registry:lookup({?global_ns, <<"a">>}),
    counters:add(CRef, 1, 3),
    %% Re-registering must not zero counters either.
    ok = emqx_topic_metrics2:register(<<"a">>, <<"a/#">>, ?global_ns),
    {ok, #{counter_ref := CRef}} = emqx_topic_metrics_registry:lookup({?global_ns, <<"a">>}),
    ?assertEqual(3, counters:get(CRef, 1)).

t_deregister(_Config) ->
    ok = emqx_topic_metrics2:register(<<"a">>, <<"a/#">>, ?global_ns),
    ok = emqx_topic_metrics2:deregister(<<"a">>, ?global_ns),
    ?assertEqual({error, not_found}, emqx_topic_metrics2:lookup(<<"a">>, ?global_ns)),
    ?assertEqual({error, not_found}, emqx_topic_metrics2:deregister(<<"a">>, ?global_ns)).

t_namespace_keyed_isolation(_Config) ->
    %% With ns in the DB key, the same bin-name can coexist in
    %% different namespaces, and a caller can only see/touch their
    %% own namespace's row via the facade.
    ok = emqx_topic_metrics2:register(<<"shared">>, <<"a/#">>, <<"acme">>),
    ok = emqx_topic_metrics2:register(<<"shared">>, <<"b/#">>, <<"bravo">>),
    ok = emqx_topic_metrics2:register(<<"shared">>, <<"g/#">>, ?global_ns),
    %% Cross-ns lookups are not_found, never leak the other ns's row.
    {ok, #{topic_filter := <<"a/#">>}} = emqx_topic_metrics2:lookup(<<"shared">>, <<"acme">>),
    {ok, #{topic_filter := <<"b/#">>}} = emqx_topic_metrics2:lookup(<<"shared">>, <<"bravo">>),
    {ok, #{topic_filter := <<"g/#">>}} = emqx_topic_metrics2:lookup(<<"shared">>, ?global_ns),
    %% Likewise for deregister — acme can only delete its own.
    ?assertEqual(
        {error, not_found},
        emqx_topic_metrics2:deregister(<<"nonexistent">>, <<"acme">>)
    ),
    ok = emqx_topic_metrics2:deregister(<<"shared">>, <<"acme">>),
    {ok, _} = emqx_topic_metrics2:lookup(<<"shared">>, <<"bravo">>),
    {ok, _} = emqx_topic_metrics2:lookup(<<"shared">>, ?global_ns).

t_list_filters_by_namespace(_Config) ->
    ok = emqx_topic_metrics2:register(<<"g1">>, <<"g/#">>, ?global_ns),
    ok = emqx_topic_metrics2:register(<<"a">>, <<"a/#">>, <<"acme">>),
    ok = emqx_topic_metrics2:register(<<"b">>, <<"b/#">>, <<"bravo">>),
    All = emqx_topic_metrics2:list(all_ns),
    ?assertEqual(3, length(All)),
    Acme = emqx_topic_metrics2:list(<<"acme">>),
    ?assertEqual([<<"a">>], [N || #{bin_name := N} <- Acme]),
    Global = emqx_topic_metrics2:list(?global_ns),
    ?assertEqual([<<"g1">>], [N || #{bin_name := N} <- Global]).

t_cap_at_max_collections(_Config) ->
    Cap = ?MAX_COLLECTIONS,
    [
        ok = emqx_topic_metrics2:register(
            integer_to_binary(I),
            <<"t/", (integer_to_binary(I))/binary, "/#">>,
            ?global_ns
        )
     || I <- lists:seq(1, Cap)
    ],
    ?assertEqual(
        {error, quota_exceeded},
        emqx_topic_metrics2:register(<<"overflow">>, <<"t/x/#">>, ?global_ns)
    ).

t_topic_filter_match(_Config) ->
    ok = emqx_topic_metrics2:register(<<"a">>, <<"a/#">>, ?global_ns),
    ok = emqx_topic_metrics2:register(<<"b">>, <<"b/+/x">>, ?global_ns),
    ok = emqx_topic_metrics2:register(<<"c">>, <<"a/+/x">>, ?global_ns),
    ?assertEqual(
        lists:sort([{?global_ns, <<"a">>}, {?global_ns, <<"c">>}]),
        lists:sort(emqx_topic_metrics_registry:matches(<<"a/foo/x">>))
    ),
    ?assertEqual(
        [{?global_ns, <<"a">>}],
        emqx_topic_metrics_registry:matches(<<"a/foo">>)
    ),
    ?assertEqual([], emqx_topic_metrics_registry:matches(<<"z/foo">>)).

t_reset(_Config) ->
    ok = emqx_topic_metrics2:register(<<"a">>, <<"a/#">>, ?global_ns),
    {ok, #{counter_ref := CRef}} = emqx_topic_metrics_registry:lookup({?global_ns, <<"a">>}),
    counters:add(CRef, 1, 7),
    {ok, #{metrics := #{'messages.in.count' := 7}}} =
        emqx_topic_metrics2:lookup(<<"a">>, ?global_ns),
    ok = emqx_topic_metrics2:reset(<<"a">>, ?global_ns),
    {ok, #{metrics := #{'messages.in.count' := 0}}} =
        emqx_topic_metrics2:lookup(<<"a">>, ?global_ns).

t_reset_namespace_isolation(_Config) ->
    %% Reset only sees the caller's namespace. A different namespace
    %% gets `not_found' (no information about the existence of the
    %% other ns's row).
    ok = emqx_topic_metrics2:register(<<"a">>, <<"a/#">>, <<"acme">>),
    ?assertEqual({error, not_found}, emqx_topic_metrics2:reset(<<"a">>, <<"bravo">>)),
    ?assertEqual({error, not_found}, emqx_topic_metrics2:reset(<<"a">>, ?global_ns)),
    ok = emqx_topic_metrics2:reset(<<"a">>, <<"acme">>).
