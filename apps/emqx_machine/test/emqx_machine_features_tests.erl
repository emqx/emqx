%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_machine_features_tests).

-compile([nowarn_export_all, export_all]).

-include_lib("eunit/include/eunit.hrl").

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

-define(ENV_VAR, "EMQX_FEATURES").

-define(with_features(NAME, ENV_VAR, BODY),
    {setup, local, fun() -> setup_env(ENV_VAR) end, fun clean_env/1, fun(_) ->
        {NAME, ?_test(BODY)}
    end}
).

-define(with_env(ENV_VAR, BODY), with_env(ENV_VAR, fun() -> BODY end)).

info() ->
    emqx_machine_features:info().

known_features() ->
    emqx_machine_features:known_features().

features() ->
    emqx_machine_features:features().

sorted_reboot_apps() ->
    emqx_machine_boot:sorted_reboot_apps().

setup_env(false) ->
    load_umbrella_apps(),
    ok;
setup_env(EnvVar) ->
    os:putenv(?ENV_VAR, EnvVar),
    load_umbrella_apps(),
    ok.

clean_env(_) ->
    emqx_machine_features:clear_features(),
    os:unsetenv(?ENV_VAR),
    unload_umbrella_apps(),
    ok.

with_env(EnvVar, Fn) ->
    try
        os:putenv(?ENV_VAR, EnvVar),
        Fn()
    after
        emqx_machine_features:clear_features(),
        os:unsetenv(?ENV_VAR)
    end.

load_umbrella_apps() ->
    lists:foreach(
        fun(App) ->
            case application:load(App) of
                ok -> ok;
                {error, {already_loaded, _}} -> ok
            end
        end,
        emqx_common_test_helpers:list_umbrella_apps()
    ).

unload_umbrella_apps() ->
    lists:foreach(
        fun application:unload/1,
        emqx_common_test_helpers:list_umbrella_apps()
    ).

app_deps(App, RebootApps) ->
    case application:get_key(App, applications) of
        undefined -> undefined;
        {ok, List} -> lists:filter(fun(A) -> lists:member(A, RebootApps) end, List)
    end.

%% verifies that the enabled set of apps does not depend on any umbrella app that is not
%% enabled.
assert_consistent_deps(Enabled0) ->
    Apps0 = umbrella_apps(),
    AppsWithDeps = emqx_machine_boot:reboot_apps_with_deps(),
    Apps1 = [App || {App, _} <- AppsWithDeps, lists:member(App, Apps0)],
    NotEnabled = Apps1 -- Enabled0,
    Enabled = [A || A = {App, _Deps} <- AppsWithDeps, lists:member(App, Enabled0)],
    Violations0 = lists:flatmap(
        fun({_App1, Deps}) ->
            lists:filter(
                fun(App2) -> lists:member(App2, NotEnabled) end,
                Deps
            )
        end,
        Enabled
    ),
    %% apps that are enabled but are not explicitly allowed nor from a feature dependency
    Violations = lists:usort(Violations0),
    ?assertEqual([], Violations).

umbrella_apps() ->
    emqx_common_test_helpers:list_umbrella_apps().

enabled_apps() ->
    Apps = umbrella_apps(),
    lists:filter(fun emqx_machine_features:is_umbrella_application_enabled/1, Apps).

%%------------------------------------------------------------------------------
%% Test cases
%%------------------------------------------------------------------------------

test() ->
    eunit:test({inorder, ?MODULE}).

full_test_() ->
    Check = fun() ->
        Apps = umbrella_apps(),
        Enabled = enabled_apps(),
        NotEnabled = Apps -- Enabled,
        ?assertEqual([], NotEnabled),
        assert_consistent_deps(Enabled),
        %% info is formatted just fine
        ?assertMatch(
            #{
                preset := full,
                enabled := [_ | _],
                disabled := []
            },
            info()
        ),
        ok
    end,
    [
        ?with_features("unset var", false, Check()),
        ?with_features("empty var", "", Check()),
        ?with_features("correctly set", "FULL", Check())
    ].

essential_test_() ->
    Check = fun() ->
        Enabled = enabled_apps(),
        CoreApps = emqx_machine_features:core_apps(),
        MissingCoreApps = CoreApps -- Enabled,
        ExtraApps = Enabled -- CoreApps,
        ?assertEqual([], ExtraApps),
        ?assertEqual([], MissingCoreApps),
        assert_consistent_deps(Enabled),
        %% info is formatted just fine
        ?assertMatch(
            #{
                preset := essential,
                enabled := [],
                disabled := [_ | _]
            },
            info()
        ),
        ok
    end,
    [
        ?with_features("correctly set", "ESSENTIAL", Check())
    ].

custom_test_() ->
    [
        ?with_features("two features (separated by comma)", "ai,dashboard", begin
            #{
                preset := custom,
                enabled := Enabled
            } = info(),
            ?assert(lists:member(ai, Enabled), #{enabled => Enabled}),
            ?assert(lists:member(dashboard, Enabled), #{enabled => Enabled}),
            ok
        end),
        ?with_features("two features (separated by space)", "ai dashboard", begin
            #{
                preset := custom,
                enabled := Enabled
            } = info(),
            ?assert(lists:member(ai, Enabled), #{enabled => Enabled}),
            ?assert(lists:member(dashboard, Enabled), #{enabled => Enabled}),
            ok
        end)
    ].

-doc """
Checks that each individual feature is consistent:

- If it has no dependencies, allowed apps are consistent (dependency umbrella apps are
  allowed).
- If it does have deps, it automatically allows them, even if not explicitly listed.
  Also, all apps are consistent.
""".
individual_features_test_() ->
    {setup, fun load_umbrella_apps/0, fun(_) -> unload_umbrella_apps() end, fun(_) ->
        [
            test_individual_feature(Feature, Info)
         || {Feature, Info} <- maps:to_list(known_features())
        ]
    end}.

test_individual_feature(Feature, Info) ->
    Deps = maps:get(deps, Info),
    KnownFeatures = known_features(),
    F = atom_to_list(Feature),
    #{apps := Apps0} = Info,
    Apps1 = lists:usort(
        lists:flatmap(
            fun(D) ->
                #{apps := As} = maps:get(D, KnownFeatures),
                As
            end,
            Deps
        )
    ),
    Apps = lists:usort(Apps0 ++ Apps1),
    ?with_features(F, F, begin
        Enabled = lists:usort(enabled_apps()),
        CoreApps = emqx_machine_features:core_apps(),
        %% usort: some core apps (e.g. auth drivers) are also listed in a
        %% feature's app list, so the concatenation can contain duplicates.
        Expected = lists:usort(CoreApps ++ Apps),
        Missing = Expected -- Enabled,
        Extra = Enabled -- Expected,
        ?assertEqual([], Extra),
        ?assertEqual([], Missing),
        assert_consistent_deps(Enabled),
        test_individual_feature_specific_assertions(Feature),
        %% info is formatted just fine
        ?assertMatch(
            #{
                preset := custom,
                enabled := [_ | _],
                disabled := [_ | _]
            },
            info()
        ),
        ok
    end).

test_individual_feature_specific_assertions(data_integration) ->
    Enabled = enabled_apps(),
    %% just sampling a few of the expected dynamically injected apps.  the whole set is
    %% frequently changing.
    ?assertMatch(
        #{
            emqx_bridge_http := _,
            emqx_bridge_mqtt := _
        },
        maps:from_keys(Enabled, true)
    ),
    ok;
test_individual_feature_specific_assertions(gateways) ->
    Enabled = enabled_apps(),
    %% not an exhaustive list, since it's dynamically injected by name convention.
    ?assertMatch(
        #{
            emqx_gateway_coap := _,
            emqx_gateway_jt808 := _
        },
        maps:from_keys(Enabled, true)
    ),
    ok;
test_individual_feature_specific_assertions(Feature) when
    %% list individual features here to avoid missing one due to typos.
    Feature == dashboard;
    Feature == message_transformation;
    Feature == schema_validation;
    Feature == schema_registry;
    Feature == metrics;
    Feature == cluster_link;
    Feature == multi_tenancy;
    Feature == plugins;
    Feature == mqtt_extensions;
    Feature == ai;
    Feature == file_transfer;
    Feature == gcp_device;
    Feature == exhook;
    Feature == opentelemetry
->
    ok.

-doc """
Simple sanity test for the filtered sorted reboot apps list for different presets.

- Any individual feature has strictly more apps than ESSENTIAL (core apps).
- Any feature selection contains the core apps.
- FULL has more (due to bundled apps) or the same apps than the union of all features.
""".
sorted_reboot_list_test_() ->
    {setup, fun load_umbrella_apps/0, fun(_) -> unload_umbrella_apps() end, fun(_) ->
        ?_test(begin
            CoreApps = ?with_env("ESSENTIAL", sorted_reboot_apps()),
            Apps = lists:map(
                fun(F) -> {F, ?with_env(atom_to_list(F), sorted_reboot_apps())} end,
                maps:keys(known_features())
            ),
            FullApps = ?with_env("FULL", ?with_env("FULL", sorted_reboot_apps())),
            lists:foreach(
                fun({F, Apps0}) ->
                    ?assertMatch([_ | _], Apps0 -- CoreApps, #{feature => F}),
                    ?assertMatch([], CoreApps -- Apps0, #{feature => F})
                end,
                Apps
            ),
            UnionApps = lists:usort(lists:flatmap(fun({_, As}) -> As end, Apps)),
            ?assertMatch([], UnionApps -- FullApps),
            ok
        end)
    end}.

-doc """
Checks that we exit loudly when at least one invalid feature or preset is given.
""".
unknown_feature_test_() ->
    Check = fun(Name, EnvVar) ->
        ?with_features(
            Name,
            EnvVar,
            ?assertExit(
                #{
                    known := _,
                    reason := unknown_feature,
                    feature := _,
                    hint := _
                },
                info()
            )
        )
    end,
    [
        Check("unknown preset", "UNKNOWN"),
        Check("unknown feature (single)", "data_integratio"),
        Check("unknown feature (middle)", "ai,aa,cluster_link")
    ].
