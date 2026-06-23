%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_machine_features).

%% API
-export([
    info/0,
    is_umbrella_application_enabled/1
]).

%% only for testing
-export([clear_features/0, core_apps/0, known_features/0, features/0]).

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

-define(PT_KEY, {?MODULE, features}).
-define(ENV_VAR, "EMQX_FEATURES").

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

info() ->
    format_info(features()).

is_umbrella_application_enabled(Application) when is_atom(Application) ->
    lists:member(Application, core_apps()) orelse
        case features() of
            #{preset := full} ->
                true;
            #{allowed_apps := Apps} ->
                is_map_key(Application, Apps)
        end.

%% only for tests
clear_features() ->
    persistent_term:erase(?PT_KEY).

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------

features() ->
    case persistent_term:get(?PT_KEY, undefined) of
        undefined ->
            cache_features();
        Features ->
            Features
    end.

core_apps() ->
    [
        emqx,
        emqx_machine,
        emqx_conf,
        emqx_ctl,
        emqx_bpapi,
        emqx_license,
        emqx_durable_storage,
        emqx_ds_backends,
        emqx_ds_builtin_local,
        emqx_ds_builtin_raft,
        emqx_durable_timer,
        emqx_audit,
        emqx_eviction_agent,
        emqx_node_rebalance,
        emqx_retainer,
        emqx_psk,
        emqx_telemetry,
        emqx_resource,
        emqx_utils,
        emqx_extsub,
        emqx_gen_bridge
    ].

%% remember to update `test_emqx_boot.py` test with new features when they appear.
known_features() ->
    #{
        dashboard => #{
            apps => [
                emqx_dashboard,
                emqx_dashboard_rbac,
                emqx_dashboard_sso,
                emqx_management,
                %% used by mgmt
                emqx_setopts,
                %% used by sso
                emqx_ldap
            ],
            deps => []
        },
        auth => #{
            apps => [
                emqx_auth,
                emqx_ldap,
                emqx_mongodb,
                emqx_redis,
                emqx_bridge_http
            ] ++ auth_dynamic_apps(),
            deps => []
        },
        data_integration => #{
            apps => [
                emqx_connector,
                emqx_connector_jwt,
                emqx_connector_aggregator,
                emqx_bridge,
                emqx_rule_engine,
                emqx_postgresql,
                emqx_mysql,
                emqx_oracle,
                emqx_redis,
                emqx_s3,
                emqx_mongodb
            ] ++ data_integration_dynamic_apps(),
            deps => [schema_registry]
        },
        message_transformation => #{
            apps => [emqx_message_transformation],
            deps => [schema_registry]
        },
        schema_validation => #{
            apps => [emqx_schema_validation],
            deps => [schema_registry]
        },
        schema_registry => #{
            apps => [
                emqx_schema_registry,
                %% used by external registries
                emqx_bridge_http
            ],
            deps => []
        },
        gateways => #{
            apps => [emqx_gateway] ++ gateway_dynamic_apps(),
            deps => [auth]
        },
        cluster_link => #{
            apps => [emqx_cluster_link],
            deps => []
        },
        multi_tenancy => #{
            apps => [emqx_mt],
            deps => []
        },
        plugins => #{
            apps => [emqx_plugins],
            deps => []
        },
        ai => #{
            apps => [emqx_ai_completion, emqx_a2a_registry],
            deps => [schema_registry]
        },
        metrics => #{
            apps => [emqx_prometheus],
            deps => [dashboard, auth]
        },
        mqtt_extensions => #{
            apps => [
                emqx_setopts,
                emqx_modules,
                emqx_auto_subscribe,
                emqx_slow_subs,
                emqx_streams,
                emqx_mq
            ],
            deps => []
        },
        file_transfer => #{
            apps => [emqx_ft, emqx_s3],
            deps => []
        },
        gcp_device => #{
            apps => [emqx_gcp_device],
            deps => [auth]
        },
        exhook => #{
            apps => [emqx_exhook],
            deps => []
        },
        opentelemetry => #{
            apps => [emqx_opentelemetry],
            deps => [
                %% needs emqx_management
                dashboard
            ]
        }
    }.

data_integration_dynamic_apps() ->
    RebootApps = full_sorted_reboot_apps(),
    [
        App
     || App <- RebootApps,
        case atom_to_list(App) of
            "emqx_bridge_" ++ _ ->
                true;
            _ ->
                false
        end
    ].

auth_dynamic_apps() ->
    RebootApps = full_sorted_reboot_apps(),
    [
        App
     || App <- RebootApps,
        case atom_to_list(App) of
            "emqx_auth_" ++ _ ->
                true;
            _ ->
                false
        end
    ].

gateway_dynamic_apps() ->
    RebootApps = full_sorted_reboot_apps(),
    [
        App
     || App <- RebootApps,
        case atom_to_list(App) of
            "emqx_gateway_" ++ _ ->
                true;
            _ ->
                false
        end
    ].

full_sorted_reboot_apps() ->
    emqx_machine_boot:full_sorted_reboot_apps().

default_preset() ->
    full_preset().

full_preset() ->
    #{preset => full}.

essential_preset() ->
    #{
        preset => essential,
        allowed_apps => #{}
    }.

cache_features() ->
    Features =
        case os:getenv(?ENV_VAR) of
            false ->
                default_preset();
            "" ->
                default_preset();
            "FULL" ->
                default_preset();
            "ESSENTIAL" ->
                essential_preset();
            Str ->
                parse_features(Str)
        end,
    _ = persistent_term:put(?PT_KEY, Features),
    Features.

resolve_dependent_apps(Feature, KnownFeatures) ->
    #{apps := Apps, deps := FeatDeps} = maps:get(Feature, KnownFeatures),
    Apps ++
        lists:flatmap(
            fun(FeatDep) -> resolve_dependent_apps(FeatDep, KnownFeatures) end,
            FeatDeps
        ).

resolve_dependent_features(Feature, KnownFeatures) ->
    #{deps := FeatDeps} = maps:get(Feature, KnownFeatures),
    FeatDeps ++
        lists:flatmap(
            fun(FeatDep) -> resolve_dependent_features(FeatDep, KnownFeatures) end,
            FeatDeps
        ).

parse_features(Str0) ->
    KnownFeatures = known_features(),
    Features =
        lists:foldl(
            fun(Str, Acc0) ->
                #{
                    allowed_apps := Allowed0,
                    enabled_features := Enabled0
                } = Acc0,
                case emqx_utils:safe_to_existing_atom(Str, utf8) of
                    {ok, Feature} when is_map_key(Feature, KnownFeatures) ->
                        Allowed1 = resolve_dependent_apps(Feature, KnownFeatures),
                        Allowed2 = maps:from_keys(Allowed1, true),
                        Allowed = maps:merge(Allowed0, Allowed2),
                        Enabled1 = resolve_dependent_features(Feature, KnownFeatures),
                        Enabled2 = maps:from_keys(Enabled1, true),
                        Enabled3 = maps:merge(Enabled0, Enabled2),
                        Enabled = Enabled3#{Feature => true},
                        Acc0#{
                            allowed_apps := Allowed,
                            enabled_features := Enabled
                        };
                    _ ->
                        exit(#{
                            reason => unknown_feature,
                            feature => Str,
                            known => maps:keys(KnownFeatures),
                            hint => did_you_mean(Str, KnownFeatures)
                        })
                end
            end,
            #{
                preset => custom,
                allowed_apps => #{},
                enabled_features => #{}
            },
            string:tokens(Str0, ", ")
        ),
    Features.

did_you_mean(Str, KnownFeatures) ->
    KnownNames = [atom_to_list(N) || N <- maps:keys(KnownFeatures)],
    Suggestions0 = lists:filtermap(
        fun(N) ->
            Similarity = string:jaro_similarity(Str, N),
            Similarity >= 0.77 andalso {true, {N, -Similarity}}
        end,
        KnownNames
    ),
    Suggestions = lists:keysort(2, Suggestions0),
    case Suggestions of
        [] ->
            "please check the known feature list";
        [{Suggestion, _} | _] ->
            "did you mean " ++ Suggestion ++ "?"
    end.

format_info(#{preset := full}) ->
    KnownFeatures = known_features(),
    KnownNames = maps:keys(KnownFeatures),
    #{
        preset => full,
        enabled => lists:sort(KnownNames),
        disabled => []
    };
format_info(#{preset := essential}) ->
    KnownFeatures = known_features(),
    KnownNames = maps:keys(KnownFeatures),
    #{
        preset => essential,
        enabled => [],
        disabled => KnownNames
    };
format_info(#{preset := custom} = Info) ->
    KnownFeatures = known_features(),
    KnownNames = maps:keys(KnownFeatures),
    Enabled = maps:keys(maps:get(enabled_features, Info)),
    Disabled = KnownNames -- Enabled,
    #{
        preset => custom,
        enabled => Enabled,
        disabled => Disabled
    }.
