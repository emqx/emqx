%%--------------------------------------------------------------------
%% Copyright (c) 2020-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mgmt_api_key_scopes).

-moduledoc """
API Key scope management.

Each minirest_api module declares its scope via a `scopes/0` callback
that returns either a scope name binary (all paths share the same
scope) or a `#{Path => ScopeName}` map (for modules whose endpoints
span multiple scopes).

This module collects those declarations, builds a path → scope cache,
and exposes the user-visible scope catalogue.

Scopes are decoupled from OpenAPI tags: scope names are stable
identifiers defined in `emqx_mgmt_api_key_scopes.hrl`.  The internal
mapping from paths to scopes can change across versions without
affecting user-facing API key configurations.
""".

-include_lib("emqx/include/logger.hrl").
-include_lib("emqx/include/emqx_api_key_scopes.hrl").

-export([
    scope_catalogue/0,
    path_to_scope/1,
    init_cache/0,
    clear_cache/0,
    validate_scopes/1,
    is_denied_scope/1
]).

-ifdef(TEST).
-export([
    collect_scopes_from_modules/0,
    find_api_modules/0
]).
-endif.

-define(CACHE_KEY, {?MODULE, scope_cache}).

%%--------------------------------------------------------------------
%% Scope catalogue — user-visible scope list
%%--------------------------------------------------------------------

-doc """
Return the catalogue of all user-visible scopes.
Each entry has a `name` (the stable identifier stored in API key
records) and a `desc` (human-readable description for the UI).

The `$denied` scope is excluded — it is internal-only.
""".
-spec scope_catalogue() ->
    [#{name := binary(), desc := binary()}].
scope_catalogue() ->
    [
        #{
            name => ?SCOPE_CONNECTIONS,
            desc => <<
                "Client connections, subscriptions, topics, banning, "
                "retained messages, file transfer, and delayed messages"
            >>
        },
        #{
            name => ?SCOPE_PUBLISH,
            desc => <<"MQTT message publishing">>
        },
        #{
            name => ?SCOPE_DATA_INTEGRATION,
            desc => <<
                "Rules, bridges, connectors, schema registry, "
                "schema validation, message transformation, ExHook, and AI completion"
            >>
        },
        #{
            name => ?SCOPE_ACCESS_CONTROL,
            desc => <<"Client authentication and authorization configuration">>
        },
        #{
            name => ?SCOPE_GATEWAYS,
            desc => <<
                "Protocol gateways (CoAP, LwM2M, etc.) "
                "and their authentication, clients, and listeners"
            >>
        },
        #{
            name => ?SCOPE_MONITORING,
            desc => <<
                "Metrics, monitoring, alarms, trace, slow subscriptions, "
                "telemetry, and Prometheus data endpoints"
            >>
        },
        #{
            name => ?SCOPE_CLUSTER_OPERATIONS,
            desc => <<
                "Cluster management, node operations, "
                "load rebalancing, node eviction, and multi-tenancy"
            >>
        },
        #{
            name => ?SCOPE_SYSTEM,
            desc => <<
                "Core configuration, listeners, plugins, storage, backup, "
                "status, hot upgrade, Prometheus settings, and OpenTelemetry"
            >>
        },
        #{
            name => ?SCOPE_AUDIT,
            desc => <<"Audit log query">>
        },
        #{
            name => ?SCOPE_LICENSE,
            desc => <<"License management">>
        }
    ].

%%--------------------------------------------------------------------
%% Path → scope lookup
%%--------------------------------------------------------------------

-doc """
Given a route template path (e.g. <<"/clients/:clientid">>),
return the scope name for this path, or `undefined` if unmapped.
""".
-spec path_to_scope(binary()) -> binary() | undefined.
path_to_scope(Path) ->
    case get_cache() of
        undefined ->
            init_cache(),
            do_path_to_scope(Path);
        #{path_to_scope := PathMap} ->
            maps:get(Path, PathMap, undefined)
    end.

do_path_to_scope(Path) ->
    case get_cache() of
        #{path_to_scope := PathMap} ->
            maps:get(Path, PathMap, undefined);
        _ ->
            undefined
    end.

%%--------------------------------------------------------------------
%% Scope validation
%%--------------------------------------------------------------------

-doc "Validate that all given scopes exist in the catalogue.".
-spec validate_scopes([binary()]) -> ok | {error, binary()}.
validate_scopes(Scopes) when is_list(Scopes) ->
    case lists:all(fun is_binary/1, Scopes) of
        false ->
            {error, <<"scopes must be a list of strings">>};
        true ->
            validate_scopes_values(Scopes)
    end;
validate_scopes(_) ->
    {error, <<"scopes must be a list of strings">>}.

validate_scopes_values(Scopes) ->
    Available = [Name || #{name := Name} <- scope_catalogue()],
    Invalid = [S || S <- Scopes, not lists:member(S, Available)],
    case Invalid of
        [] ->
            ok;
        _ ->
            InvalidBin = iolist_to_binary(lists:join(<<", ">>, Invalid)),
            {error, <<"Unknown scopes: ", InvalidBin/binary>>}
    end.

%%--------------------------------------------------------------------
%% Denied scope check
%%--------------------------------------------------------------------

-doc "Check if a scope is the denied scope (internal, not user-assignable).".
-spec is_denied_scope(binary()) -> boolean().
is_denied_scope(?SCOPE_DENIED) -> true;
is_denied_scope(_) -> false.

%%--------------------------------------------------------------------
%% Cache management
%%--------------------------------------------------------------------

-doc """
Initialize the scope cache by collecting `scopes/0` from all API modules.
Should be called once after the dashboard HTTP server has started.
""".
-spec init_cache() -> ok.
init_cache() ->
    PathToScope = collect_scopes_from_modules(),
    persistent_term:put(?CACHE_KEY, #{path_to_scope => PathToScope}),
    ok.

-doc "Clear the scope cache.".
-spec clear_cache() -> ok.
clear_cache() ->
    _ = persistent_term:erase(?CACHE_KEY),
    ok.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

get_cache() ->
    persistent_term:get(?CACHE_KEY, undefined).

%% @doc Collect path → scope mappings from all API modules that export scopes/0.
%% Returns a flat map: #{<<"/clients">> => <<"connections">>, ...}.
-spec collect_scopes_from_modules() -> #{binary() => binary()}.
collect_scopes_from_modules() ->
    Modules = find_api_modules(),
    lists:foldl(fun collect_module_scopes/2, #{}, Modules).

find_api_modules() ->
    Apps = [
        App
     || {App, _, _} <- application:loaded_applications(),
        is_emqx_app(App)
    ],
    lists:usort(lists:flatmap(fun find_api_modules_in_app/1, Apps)).

is_emqx_app(App) ->
    case re:run(atom_to_list(App), "^emqx") of
        {match, [{0, 4}]} -> true;
        _ -> false
    end.

find_api_modules_in_app(App) ->
    case application:get_key(App, modules) of
        {ok, Modules} ->
            [M || M <- Modules, is_api_module(M)];
        _ ->
            []
    end.

is_api_module(Module) ->
    case is_test_module(Module) of
        true ->
            false;
        false ->
            Behaviours =
                proplists:get_value(behaviour, apply(Module, module_info, [attributes]), []) ++
                    proplists:get_value(behavior, apply(Module, module_info, [attributes]), []),
            lists:member(minirest_api, Behaviours)
    end.

%% Exclude test-only minirest_api modules (CT suites that implement
%% minirest_api for swagger testing purposes). They intentionally do
%% not export scopes/0, and are not reachable via the production router.
is_test_module(Module) ->
    lists:suffix("_SUITE", atom_to_list(Module)).

%% Collect scopes from a single API module.
%% The module must export scopes/0 returning either:
%%   - a binary (all paths share this scope)
%%   - a #{Path => Scope} map (per-path scope assignment)
collect_module_scopes(Module, Acc) ->
    try
        case erlang:function_exported(Module, scopes, 0) of
            false ->
                ?SLOG(warning, #{
                    msg => "api_module_missing_scopes_callback",
                    module => Module
                }),
                Acc;
            true ->
                Paths = apply(Module, paths, []),
                ScopeSpec = apply(Module, scopes, []),
                collect_paths_with_scope(Module, Paths, ScopeSpec, Acc)
        end
    catch
        Class:Reason ->
            ?SLOG(warning, #{
                msg => "failed_to_collect_scopes",
                module => Module,
                class => Class,
                reason => Reason
            }),
            Acc
    end.

collect_paths_with_scope(_Module, Paths, ScopeName, Acc) when is_binary(ScopeName) ->
    %% Simple form: all paths share the same scope
    lists:foldl(
        fun(Path, InnerAcc) ->
            PathBin = path_to_binary(Path),
            InnerAcc#{PathBin => ScopeName}
        end,
        Acc,
        Paths
    );
collect_paths_with_scope(Module, Paths, ScopeMap, Acc) when is_map(ScopeMap) ->
    %% Map form: per-path scope assignment
    lists:foldl(
        fun(Path, InnerAcc) ->
            PathBin = path_to_binary(Path),
            case maps:get(PathBin, ScopeMap, maps:get(Path, ScopeMap, undefined)) of
                undefined ->
                    ?SLOG(warning, #{
                        msg => "path_missing_from_scopes_map",
                        module => Module,
                        path => PathBin
                    }),
                    InnerAcc;
                ScopeName ->
                    InnerAcc#{PathBin => ScopeName}
            end
        end,
        Acc,
        Paths
    ).

path_to_binary(Path) when is_binary(Path) ->
    ensure_leading_slash(Path);
path_to_binary(Path) when is_list(Path) ->
    ensure_leading_slash(iolist_to_binary(filename:join("/", Path))).

ensure_leading_slash(<<"/", _/binary>> = Path) -> Path;
ensure_leading_slash(Path) -> <<"/", Path/binary>>.
