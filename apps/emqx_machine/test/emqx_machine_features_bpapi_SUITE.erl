%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_machine_features_bpapi_SUITE).

-compile([nowarn_export_all, export_all]).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("emqx/include/asserts.hrl").

%%------------------------------------------------------------------------------
%% Defs
%%------------------------------------------------------------------------------

-import(emqx_common_test_helpers, [on_exit/1]).

-define(ENV_VAR, "EMQX_FEATURES").

%%------------------------------------------------------------------------------
%% CT Boilerplate
%%------------------------------------------------------------------------------

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(TCConfig) ->
    load_umbrella_apps(),
    TCConfig.

end_per_suite(_TCConfig) ->
    unload_umbrella_apps(),
    ok.

init_per_testcase(_TestCase, TCConfig) ->
    TCConfig.

end_per_testcase(_TestCase, _TCConfig) ->
    emqx_common_test_helpers:call_janitor(),
    ok.

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

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

start_with_features(EnvVar, TestCase, TCConfig) ->
    on_exit(fun clear_env/0),
    os:putenv(?ENV_VAR, EnvVar),
    Apps = emqx_cth_suite:start([emqx], #{work_dir => emqx_cth_suite:work_dir(TestCase, TCConfig)}),
    on_exit(fun() -> ok = emqx_cth_suite:stop(Apps) end),
    Apps.

clear_env() ->
    os:unsetenv(?ENV_VAR),
    emqx_machine_features:clear_features(),
    ok.

supported_apis() ->
    supported_apis(node()).

supported_apis(Node) ->
    maps:from_list(emqx_bpapi:supported_apis(Node)).

assert_contains_sample(APIs, Which) ->
    assert_contains_sample(APIs, Which, _Extra = #{}).

assert_contains_sample(APIs, Which, Extra) when is_atom(Which) ->
    Sample = sample_apis(Which),
    assert_contains_sample(APIs, Sample, Extra#{api => Which});
assert_contains_sample(APIs, Sample, Extra) when is_map(Sample) ->
    Intersection = maps:intersect_with(fun(_, _, _) -> true end, APIs, Sample),
    ?assertEqual(Sample, Intersection, Extra).

assert_not_contains_sample(APIs, Which) ->
    assert_not_contains_sample(APIs, Which, _Extra = #{}).

assert_not_contains_sample(APIs, Which, Extra) when is_atom(Which) ->
    Sample = sample_apis(Which),
    assert_not_contains_sample(APIs, Sample, Extra#{api => Which});
assert_not_contains_sample(APIs, Sample, Extra) when is_map(Sample) ->
    Intersection = maps:intersect_with(fun(_, _, _) -> true end, APIs, Sample),
    ?assertEqual(#{}, Intersection, Extra).

%% no sample here is an exhaustive list, as those change over time.
sample_apis() ->
    #{
        core =>
            maps:from_keys(
                [
                    emqx,
                    emqx_conf,
                    emqx_retainer,
                    emqx_resource,
                    emqx_cluster,
                    emqx_license,
                    emqx_ds_builtin_raft,
                    emqx_ds_beamsplitter,
                    emqx_eviction_agent,
                    emqx_node_rebalance
                ],
                true
            ),
        data_integration =>
            maps:from_keys(
                [
                    emqx_connector,
                    emqx_bridge,
                    emqx_bridge_kinesis,
                    emqx_rule_engine
                ],
                true
            ),
        dashboard =>
            maps:from_keys(
                [
                    emqx_dashboard,
                    emqx_dashboard_sso_oidc,
                    emqx_management,
                    emqx_mgmt_cluster,
                    emqx_mgmt_data_backup,
                    emqx_mgmt_api_plugins
                ],
                true
            ),
        gateways =>
            maps:from_keys([emqx_gateway_cm], true),
        auth =>
            maps:from_keys(
                [
                    emqx_authn,
                    emqx_authz,
                    emqx_auth_cache
                ],
                true
            ),
        cluster_link =>
            maps:from_keys([emqx_cluster_link], true),
        exhook =>
            maps:from_keys([emqx_exhook], true),
        mqtt_extensions =>
            maps:from_keys(
                [
                    emqx_delayed,
                    emqx_topic_metrics,
                    %% turns out this is currently an `emqx_management` dependency too
                    %% , emqx_setopts
                    emqx_mq_sub
                ],
                true
            ),
        metrics =>
            maps:from_keys([emqx_prometheus], true),
        schema_registry =>
            maps:from_keys([emqx_schema_registry_http_api], true),
        multi_tenancy =>
            maps:from_keys([emqx_mt_config], true),
        file_transfer =>
            maps:from_keys([emqx_ft_storage_fs], true)
    }.

sample_apis(Which) ->
    maps:get(Which, sample_apis()).

with_sample_apis(APIs, Which) ->
    maps:with(maps:keys(sample_apis(Which)), APIs).

resolve_dependent_features(Feature) ->
    KnownFeatures = emqx_machine_features:known_features(),
    Deps0 = do_resolve_dependent_features(Feature, KnownFeatures),
    lists:usort(Deps0).

do_resolve_dependent_features(Feature, KnownFeatures) ->
    #{deps := FeatDeps} = maps:get(Feature, KnownFeatures),
    FeatDeps ++
        lists:flatmap(
            fun(FeatDep) -> do_resolve_dependent_features(FeatDep, KnownFeatures) end,
            FeatDeps
        ).

%%------------------------------------------------------------------------------
%% Test cases
%%------------------------------------------------------------------------------

t_essential(TCConfig) ->
    _ = start_with_features("ESSENTIAL", ?FUNCTION_NAME, TCConfig),
    APIs = supported_apis(),
    %% sample of expected core apis should be present
    assert_contains_sample(APIs, core),
    %% anything else should not.
    OtherFeatures = maps:keys(sample_apis()) -- [core],
    lists:foreach(
        fun(Which) -> assert_not_contains_sample(APIs, Which) end,
        OtherFeatures
    ),
    ok.

t_full(TCConfig) ->
    _ = start_with_features("FULL", ?FUNCTION_NAME, TCConfig),
    APIs = supported_apis(),
    lists:foreach(
        fun(Which) -> assert_contains_sample(APIs, Which) end,
        maps:keys(sample_apis())
    ),
    ok.

t_custom(TCConfig) ->
    maps:foreach(
        fun(Feature, _Info) ->
            ct:pal("checking feature ~s", [Feature]),
            Case0 = emqx_bridge_v2_testlib:fmt(
                <<"${fn}_${feat}">>, #{fn => ?FUNCTION_NAME, feat => Feature}
            ),
            Case = binary_to_list(Case0),
            Apps = start_with_features(atom_to_list(Feature), Case, TCConfig),
            APIs = supported_apis(),
            %% should contain core
            assert_contains_sample(APIs, core, #{feature => Feature}),
            %% should contain its own apis
            Sample = maps:get(Feature, sample_apis(), #{}),
            assert_contains_sample(APIs, Sample, #{feature => Feature}),
            %% should not contain other features which are not transitive dependencies
            Deps = resolve_dependent_features(Feature),
            Exempt = [core, Feature] ++ Deps,
            lists:foreach(
                fun(Which) -> assert_not_contains_sample(APIs, Which, #{feature => Feature}) end,
                maps:keys(sample_apis()) -- Exempt
            ),
            ok = emqx_cth_suite:stop(Apps),
            clear_env(),
            ok
        end,
        emqx_machine_features:known_features()
    ),
    ok.
