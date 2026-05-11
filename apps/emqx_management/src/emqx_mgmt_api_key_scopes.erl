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
and exposes the user-visible scope catalog.

Scopes are decoupled from OpenAPI tags: scope names are stable
identifiers defined in `emqx_mgmt_api_key_scopes.hrl`.  The internal
mapping from paths to scopes can change across versions without
affecting user-facing API key configurations.
""".

-include_lib("emqx/include/logger.hrl").
-include_lib("emqx/include/emqx_api_key_scopes.hrl").

-export([
    path_to_scope/1,
    init_cache/0,
    clear_cache/0,
    validate_scopes/1,
    filter_valid_scopes/1,
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
%% Path → scope lookup
%%--------------------------------------------------------------------

-doc """
Given a request path, return the scope name for this path, or
`undefined` if unmapped.

The cache stores OpenAPI route templates such as
`<<"/users/:username/mfa">>`. Callers that already have a template
(e.g. minirest's `HandlerInfo.path` for API-key authorisation) get
an O(1) map lookup. Callers that have a concrete cowboy request path
such as `<<"/users/john/mfa">>` (e.g. dashboard login user RBAC,
which receives `cowboy_req:path/1`) fall through to a segment-wise
match against every template — `:`-prefixed segments wildcard.

Both forms must produce the same scope; otherwise scope checks would
silently fail-open for any endpoint with a path parameter.
""".
-spec path_to_scope(binary()) -> binary() | undefined.
path_to_scope(Path) ->
    case get_cache() of
        undefined ->
            init_cache(),
            do_path_to_scope(Path);
        #{path_to_scope := PathMap} ->
            lookup_path(Path, PathMap)
    end.

do_path_to_scope(Path) ->
    case get_cache() of
        #{path_to_scope := PathMap} ->
            lookup_path(Path, PathMap);
        _ ->
            undefined
    end.

lookup_path(Path, PathMap) ->
    case maps:get(Path, PathMap, undefined) of
        undefined ->
            match_template(Path, PathMap);
        Scope ->
            Scope
    end.

%% Iterate templates and return the scope for the first one whose
%% segments match the request path. Concrete path segments must equal
%% template segments verbatim except where the template segment starts
%% with `:' (path parameter), which matches any single segment.
%%
%% Match cost is O(n*m) where n is the number of templates and m is
%% the average path depth. The cache is small (~250 entries) and this
%% function is called once per authorised request, so the cost is
%% acceptable.
match_template(Path, PathMap) ->
    PathSegs = split_segments(Path),
    Iter = maps:iterator(PathMap),
    match_template_iter(PathSegs, Iter).

match_template_iter(PathSegs, Iter) ->
    case maps:next(Iter) of
        none ->
            undefined;
        {Tmpl, Scope, Iter1} ->
            case segments_match(PathSegs, split_segments(Tmpl)) of
                true -> Scope;
                false -> match_template_iter(PathSegs, Iter1)
            end
    end.

split_segments(Path) ->
    %% Drop the leading empty segment from the leading slash.
    case binary:split(Path, <<"/">>, [global]) of
        [<<>> | Rest] -> Rest;
        Other -> Other
    end.

segments_match([], []) ->
    true;
segments_match([_ | _], []) ->
    false;
segments_match([], [_ | _]) ->
    false;
segments_match([Seg | Rest1], [TmplSeg | Rest2]) ->
    case is_param_segment(TmplSeg) of
        true -> segments_match(Rest1, Rest2);
        false when Seg =:= TmplSeg -> segments_match(Rest1, Rest2);
        false -> false
    end.

is_param_segment(<<":", _/binary>>) -> true;
is_param_segment(_) -> false.

%%--------------------------------------------------------------------
%% Scope validation
%%--------------------------------------------------------------------

-doc "Validate that all given scopes exist in the catalog.".
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
    Available = [Name || #{name := Name} <- emqx_scope_catalog:scope_catalog()],
    Invalid = [S || S <- Scopes, not lists:member(S, Available)],
    case Invalid of
        [] ->
            ok;
        _ ->
            InvalidBin = iolist_to_binary(lists:join(<<", ">>, Invalid)),
            {error, <<"Unknown scopes: ", InvalidBin/binary>>}
    end.

-doc """
Lenient counterpart to `validate_scopes/1`: drop scope names that
are not in `emqx_scope_catalog:scope_catalog/0` instead of rejecting the whole list,
and report the dropped names so the caller can log a warning.

Used by the bootstrap-file loader so a typo in one scope on one line
does not abort loading the rest of the file. The HTTP create/update
API keeps using the strict `validate_scopes/1`.

Returns `{Valid, Rejected}` where both are sublists of the input
preserving original order. Non-binary elements are rejected.
""".
-spec filter_valid_scopes([term()]) -> {[binary()], [term()]}.
filter_valid_scopes(Scopes) when is_list(Scopes) ->
    Available = [Name || #{name := Name} <- emqx_scope_catalog:scope_catalog()],
    lists:foldr(
        fun(S, {Valid, Rejected}) ->
            case is_binary(S) andalso lists:member(S, Available) of
                true -> {[S | Valid], Rejected};
                false -> {Valid, [S | Rejected]}
            end
        end,
        {[], []},
        Scopes
    ).

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
    Behaviours =
        proplists:get_value(behaviour, apply(Module, module_info, [attributes]), []) ++
            proplists:get_value(behavior, apply(Module, module_info, [attributes]), []),
    lists:member(minirest_api, Behaviours).

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
