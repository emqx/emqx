%%--------------------------------------------------------------------
%% Copyright (c) 2020-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% @doc API Key scope definitions based on OpenAPI tags.
%%
%% Scopes are derived from the `tags` field in each API module's schema.
%% Each scope corresponds to a lowercase OpenAPI tag (e.g., <<"Clients">> → <<"clients">>).
%% The module builds and caches a mapping from API paths to their scopes.
%%
%% Cache is stored in persistent_term and should be initialized once after
%% the dashboard HTTP server has started (all API modules are loaded).

-module(emqx_mgmt_api_key_scopes).

-include_lib("emqx/include/logger.hrl").

-export([
    available_scopes/0,
    path_to_scopes/1,
    init_cache/0,
    clear_cache/0,
    validate_scopes/1,
    preset_groups/0,
    expand_groups/1,
    all_preset_tags/0,
    denied_scopes/0,
    is_denied_scope/1
]).

-ifdef(TEST).
-export([
    collect_scopes_from_modules/0
]).
-endif.

-define(CACHE_KEY, {?MODULE, scope_cache}).

-type scope_info() :: #{
    name := binary(),
    paths := [binary()]
}.

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

%% @doc Return all available scopes with their associated paths.
%% Each scope is derived from the OpenAPI tags defined in API modules.
-spec available_scopes() -> [scope_info()].
available_scopes() ->
    case get_cache() of
        undefined ->
            %% Cache not initialized yet; build it on the fly
            init_cache(),
            get_available_scopes();
        _ ->
            get_available_scopes()
    end.

%% @doc Given a request path (relative, e.g., <<"/clients/:clientid">>),
%% return the list of scope names that cover this path.
-spec path_to_scopes(binary()) -> [binary()].
path_to_scopes(Path) ->
    case get_cache() of
        undefined ->
            init_cache(),
            path_to_scopes_from_cache(Path);
        #{path_to_scopes := PathMap} ->
            find_scopes_for_path(Path, PathMap)
    end.

%% @private Lookup after cache is guaranteed to exist.
path_to_scopes_from_cache(Path) ->
    case get_cache() of
        #{path_to_scopes := PathMap} ->
            find_scopes_for_path(Path, PathMap);
        _ ->
            []
    end.

%% @doc Validate that all given scopes exist in available_scopes
%% and are not in the denied list.
-spec validate_scopes([binary()]) -> ok | {error, binary()}.
validate_scopes(Scopes) when is_list(Scopes) ->
    %% Validate element types first to avoid crash in error message formatting
    case lists:all(fun is_binary/1, Scopes) of
        false ->
            {error, <<"scopes must be a list of strings">>};
        true ->
            validate_scopes_values(Scopes)
    end;
validate_scopes(_) ->
    {error, <<"scopes must be a list of strings">>}.

validate_scopes_values(Scopes) ->
    %% Check denied scopes first (takes priority over unknown)
    Denied = [S || S <- Scopes, is_denied_scope(S)],
    case Denied of
        [_ | _] ->
            DeniedBin = iolist_to_binary(lists:join(<<", ">>, Denied)),
            {error, <<"Denied scopes (not available for API keys): ", DeniedBin/binary>>};
        [] ->
            Available = [Name || #{name := Name} <- available_scopes()],
            Invalid = [S || S <- Scopes, not lists:member(S, Available)],
            case Invalid of
                [] ->
                    ok;
                _ ->
                    InvalidBin = iolist_to_binary(lists:join(<<", ">>, Invalid)),
                    {error, <<"Unknown scopes: ", InvalidBin/binary>>}
            end
    end.

%% @doc Initialize the scope cache by scanning all API modules.
%% Should be called once after the dashboard HTTP server has started.
-spec init_cache() -> ok.
init_cache() ->
    ScopeData = collect_scopes_from_modules(),
    Cache = build_cache(ScopeData),
    persistent_term:put(?CACHE_KEY, Cache),
    ok.

%% @doc Clear the scope cache.
-spec clear_cache() -> ok.
clear_cache() ->
    _ = persistent_term:erase(?CACHE_KEY),
    ok.

%%--------------------------------------------------------------------
%% Denied Scopes — tags that API Keys must never access
%%--------------------------------------------------------------------

%% @doc Tags that API Keys should NEVER be allowed to access,
%% regardless of scope configuration. These correspond to
%% dashboard-only functionality (user sessions, SSO, API key self-management).
-spec denied_scopes() -> [binary()].
denied_scopes() ->
    [
        <<"dashboard">>,
        <<"dashboard single sign-on">>,
        <<"api keys">>
    ].

%% @doc Check if a scope name is in the denied list.
-spec is_denied_scope(binary()) -> boolean().
is_denied_scope(Scope) ->
    lists:member(Scope, denied_scopes()).

%%--------------------------------------------------------------------
%% Preset Groups — display-layer aliases for common tag bundles
%%--------------------------------------------------------------------

%% @doc Return preset scope groups.
%% Each group is a display-layer alias that bundles multiple OpenAPI tags.
%% These groups are for UI convenience only — the API key `scopes` field
%% always stores individual tag names, never group names.
%%
%% The `all_scopes` group is dynamic: it contains all available scopes
%% minus denied scopes. Other groups may overlap with `all_scopes`.
-spec preset_groups() -> [#{name := binary(), desc := binary(), scopes := [binary()]}].
preset_groups() ->
    Static = [
        #{
            name => <<"connections">>,
            desc => <<"Client connections, subscriptions, topics, publish, and banning">>,
            scopes => [
                <<"clients">>, <<"subscriptions">>, <<"topics">>, <<"publish">>, <<"banned">>
            ]
        },
        #{
            name => <<"data_integration">>,
            desc => <<"Rules, actions, sources, bridges, connectors, and schema registry">>,
            scopes => [
                <<"rules">>,
                <<"actions">>,
                <<"sources">>,
                <<"bridges">>,
                <<"connectors">>,
                <<"schema registry">>
            ]
        },
        #{
            name => <<"access_control">>,
            desc => <<"Authentication, listener authentication, and authorization">>,
            scopes => [<<"authentication">>, <<"listener authentication">>, <<"authorization">>]
        },
        #{
            name => <<"gateways">>,
            desc => <<"Protocol gateways (CoAP, LwM2M, etc.) and gateway auth/clients/listeners">>,
            scopes => [
                <<"gateways">>,
                <<"coap gateways">>,
                <<"lwm2m gateways">>,
                <<"gateway authentication">>,
                <<"gateway clients">>,
                <<"gateway listeners">>
            ]
        },
        #{
            name => <<"monitoring">>,
            desc => <<"Metrics, alarms, trace, slow subscriptions, telemetry, and OpenTelemetry">>,
            scopes => [
                <<"metrics">>,
                <<"monitor">>,
                <<"alarms">>,
                <<"trace">>,
                <<"slow subscriptions">>,
                <<"telemetry">>,
                <<"opentelemetry">>
            ]
        },
        #{
            name => <<"system">>,
            desc => <<"Nodes, cluster, configs, listeners, plugins, status, relup, and storage">>,
            scopes => [
                <<"nodes">>,
                <<"cluster">>,
                <<"configs">>,
                <<"listeners">>,
                <<"plugins">>,
                <<"status">>,
                <<"relup">>,
                <<"durable storage">>,
                <<"durable queues">>
            ]
        },
        #{
            name => <<"extensions">>,
            desc =>
                <<"Auto subscribe, data backup, ExHook, GCP devices, load rebalance, and more">>,
            scopes => [
                <<"auto subscribe">>,
                <<"data backup">>,
                <<"exhook">>,
                <<"gcp devices">>,
                <<"load rebalance">>,
                <<"node eviction">>,
                <<"multi-tenancy">>,
                <<"ai completion">>,
                <<"schema validation">>,
                <<"message transformation">>,
                <<"error codes">>
            ]
        },
        #{
            name => <<"audit">>,
            desc => <<"Audit log query">>,
            scopes => [<<"audit">>]
        },
        #{
            name => <<"license">>,
            desc => <<"License management">>,
            scopes => [<<"license">>]
        }
    ],
    AllScopes = [Name || #{name := Name} <- available_scopes()],
    Static ++
        [
            #{
                name => <<"all_scopes">>,
                desc => <<"All available scopes (excludes dashboard-only endpoints)">>,
                scopes => AllScopes
            }
        ].

%% @doc Expand a list of mixed group names and/or individual scope names
%% into a flat, deduplicated list of individual scope names.
%%
%% Example:
%%   expand_groups([<<"connections">>, <<"rules">>])
%%   → [<<"banned">>, <<"clients">>, <<"publish">>, <<"rules">>, <<"subscriptions">>, <<"topics">>]
-spec expand_groups([binary()]) -> [binary()].
expand_groups(NamesOrGroups) ->
    GroupMap = maps:from_list(
        [{Name, Scopes} || #{name := Name, scopes := Scopes} <- preset_groups()]
    ),
    Expanded = lists:flatmap(
        fun(Name) ->
            case maps:get(Name, GroupMap, undefined) of
                undefined ->
                    %% Not a group name — treat as individual scope
                    [Name];
                Scopes ->
                    Scopes
            end
        end,
        NamesOrGroups
    ),
    lists:usort(Expanded).

%% @doc Return all tag names covered by preset groups (for test assertions).
-spec all_preset_tags() -> [binary()].
all_preset_tags() ->
    lists:usort(
        lists:flatmap(
            fun(#{scopes := Scopes}) -> Scopes end,
            preset_groups()
        )
    ).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

get_cache() ->
    persistent_term:get(?CACHE_KEY, undefined).

get_available_scopes() ->
    case get_cache() of
        undefined ->
            [];
        #{scopes := Scopes} ->
            Denied = denied_scopes(),
            [S || S = #{name := Name} <- Scopes, not lists:member(Name, Denied)]
    end.

%% @doc Collect scope information from all loaded API modules.
%% Iterates over all emqx* applications, finds modules implementing
%% the minirest_api behaviour, and extracts tags from their schemas.
-spec collect_scopes_from_modules() -> #{binary() => [binary()]}.
collect_scopes_from_modules() ->
    Modules = find_api_modules(),
    lists:foldl(
        fun(Module, Acc) ->
            collect_module_scopes(Module, Acc)
        end,
        #{},
        Modules
    ).

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
    Behaviours =
        proplists:get_value(behaviour, apply(Module, module_info, [attributes]), []) ++
            proplists:get_value(behavior, apply(Module, module_info, [attributes]), []),
    lists:member(minirest_api, Behaviours).

collect_module_scopes(Module, Acc) ->
    try
        Paths = apply(Module, paths, []),
        lists:foldl(
            fun(Path, InnerAcc) ->
                collect_path_scopes(Module, Path, InnerAcc)
            end,
            Acc,
            Paths
        )
    catch
        _:_ ->
            Acc
    end.

collect_path_scopes(Module, Path, Acc) ->
    try
        Schema = apply(Module, schema, [Path]),
        PathBin = iolist_to_binary(filename:join("/", Path)),
        Methods = maps:without(['operationId', 'filter'], Schema),
        %% Get tags from any method definition (they should be the same for all methods)
        Tags = extract_tags(Methods),
        lists:foldl(
            fun(Tag, InnerAcc) ->
                ScopeName = string:lowercase(Tag),
                ExistingPaths = maps:get(ScopeName, InnerAcc, []),
                case lists:member(PathBin, ExistingPaths) of
                    true -> InnerAcc;
                    false -> InnerAcc#{ScopeName => [PathBin | ExistingPaths]}
                end
            end,
            Acc,
            Tags
        )
    catch
        _:_ ->
            Acc
    end.

extract_tags(Methods) ->
    maps:fold(
        fun
            (_Method, Meta, Acc) when is_map(Meta) ->
                case maps:get(tags, Meta, []) of
                    Tags when is_list(Tags) ->
                        lists:usort(Acc ++ [to_bin(T) || T <- Tags]);
                    _ ->
                        Acc
                end;
            (_Method, _Meta, Acc) ->
                Acc
        end,
        [],
        Methods
    ).

build_cache(ScopeData) ->
    %% ScopeData: #{<<"clients">> => [<<"/clients">>, <<"/clients/:clientid">>], ...}
    Scopes = lists:sort(
        maps:fold(
            fun(Name, Paths, Acc) ->
                [#{name => Name, paths => lists:sort(Paths)} | Acc]
            end,
            [],
            ScopeData
        )
    ),
    %% Build reverse mapping: path -> [scope_name]
    PathToScopes = maps:fold(
        fun(ScopeName, Paths, Acc) ->
            lists:foldl(
                fun(P, InnerAcc) ->
                    Existing = maps:get(P, InnerAcc, []),
                    InnerAcc#{P => lists:usort([ScopeName | Existing])}
                end,
                Acc,
                Paths
            )
        end,
        #{},
        ScopeData
    ),
    #{scopes => Scopes, path_to_scopes => PathToScopes}.

%% @doc Find scopes for a given request path.
%% The path from the request may contain actual parameter values
%% (e.g., "/clients/myclient") while the registered paths have
%% placeholders (e.g., "/clients/:clientid").
%% We first try exact match, then try pattern matching.
find_scopes_for_path(Path, PathMap) ->
    case maps:get(Path, PathMap, undefined) of
        undefined ->
            %% Try pattern matching against registered paths
            match_path_pattern(Path, PathMap);
        Scopes ->
            Scopes
    end.

match_path_pattern(Path, PathMap) ->
    PathSegments = binary:split(Path, <<"/">>, [global, trim_all]),
    maps:fold(
        fun(RegisteredPath, Scopes, Acc) ->
            RegisteredSegments = binary:split(RegisteredPath, <<"/">>, [global, trim_all]),
            case segments_match(PathSegments, RegisteredSegments) of
                true -> lists:usort(Acc ++ Scopes);
                false -> Acc
            end
        end,
        [],
        PathMap
    ).

%% Match path segments, treating segments starting with ':' as wildcards.
segments_match([], []) ->
    true;
segments_match([_S | RestA], [<<$:, _/binary>> | RestB]) ->
    segments_match(RestA, RestB);
segments_match([S | RestA], [S | RestB]) ->
    segments_match(RestA, RestB);
segments_match(_, _) ->
    false.

to_bin(A) when is_atom(A) -> atom_to_binary(A, utf8);
to_bin(L) when is_list(L) -> list_to_binary(L);
to_bin(B) when is_binary(B) -> B.
