%%--------------------------------------------------------------------
%% Copyright (c) 2020-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mgmt_api_key_scopes).

-moduledoc """
API Key scope management.

Each minirest_api module declares its scope via a `scopes/0` callback
that returns either a scope declaration (all paths share it) or a
`#{Path => ScopeDeclaration}` map (for modules whose endpoints span
multiple scopes).

A scope declaration is either a single scope name binary or a
non-empty list of scope names. A list means the path is reachable by
a holder of *any one* of the listed scopes. Use it only where an
endpoint genuinely has two legitimate audiences — for example the
plugin API gateway, which a dedicated restricted scope reaches and
which `system` must keep reaching for backward compatibility.

This module collects those declarations, builds a path → scopes
cache, and exposes the user-visible scope catalog.

Scopes are decoupled from OpenAPI tags: scope names are stable
identifiers defined in `emqx_mgmt_api_key_scopes.hrl`.  The internal
mapping from paths to scopes can change across versions without
affecting user-facing API key configurations.
""".

-include_lib("emqx/include/logger.hrl").
-include_lib("emqx_utils/include/emqx_api_key_scopes.hrl").

-export([
    path_to_scopes/1,
    any_scope_granted/2,
    classify_path/1,
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
-define(CATCH_ALL_SEGMENT, <<"[...]">>).
%%--------------------------------------------------------------------
%% Path → scope lookup
%%--------------------------------------------------------------------

-doc """
Given a request path, return the scopes that grant access to it, or
`undefined` if unmapped. Holding any one of the returned scopes is
enough — see `any_scope_granted/2`.

The cache stores OpenAPI route templates such as
`<<"/users/:username/mfa">>`. Callers that already have a template
(e.g. minirest's `HandlerInfo.path` for API-key authorisation) get
an O(1) map lookup. Callers that have a concrete cowboy request path
such as `<<"/users/john/mfa">>` (e.g. dashboard login user RBAC,
which receives `cowboy_req:path/1`) fall through to a segment-wise
match against every template — `:`-prefixed segments match one
segment, a trailing `[...]` matches the rest of the path.

Both forms must produce the same scopes; otherwise scope checks would
silently fail-open for any endpoint with a path parameter.
""".
-spec path_to_scopes(binary()) -> [binary(), ...] | undefined.
path_to_scopes(Path) ->
    %% Two-way view kept for the API-key authorisation path
    %% (`emqx_mgmt_auth:check_path_in_scopes/2'), which treats both
    %% "public" and "unmapped" as unscoped. Callers that must tell the
    %% two apart use `classify_path/1'.
    case classify_path(Path) of
        {scopes, Scopes} -> Scopes;
        public -> undefined;
        not_found -> undefined
    end.

-doc """
Return `true` when the holder's scope list covers a path, i.e. it
contains at least one of the scopes the path declares.

A path may declare several acceptable scopes, so membership of any
one of them grants access. Both the API-key check
(`emqx_mgmt_auth:check_path_in_scopes/2`) and the login-user check
(`emqx_dashboard_rbac:check_login_user_scopes/2`) must use this
predicate, so the two never diverge on a multi-scope path.
""".
-spec any_scope_granted([binary()], [binary()]) -> boolean().
any_scope_granted(PathScopes, HeldScopes) ->
    lists:any(fun(S) -> lists:member(S, HeldScopes) end, PathScopes).

-doc """
Three-way classification of a request path:

* `{scopes, Names}' — any one of `Names' grants access to the path.
* `public'          — the path exactly matches a `?SCOPE_PUBLIC' sentinel.
* `not_found'       — the path maps to no scope at all.

Unlike `path_to_scopes/1', which collapses `public' and `not_found'
into `undefined', this keeps them distinct so a caller can allow
public paths while denying genuinely-unmapped ones (fail closed).
""".
-spec classify_path(binary()) -> {scopes, [binary(), ...]} | public | not_found.
classify_path(Path) ->
    case get_cache() of
        undefined ->
            init_cache(),
            classify_with_cache(Path);
        #{path_to_scopes := PathMap} ->
            classify(Path, PathMap)
    end.

classify_with_cache(Path) ->
    case get_cache() of
        #{path_to_scopes := PathMap} ->
            classify(Path, PathMap);
        _ ->
            not_found
    end.

classify(Path, PathMap) ->
    case maps:get(Path, PathMap, undefined) of
        undefined ->
            case match_template(Path, PathMap) of
                undefined -> not_found;
                Scopes -> {scopes, Scopes}
            end;
        [?SCOPE_PUBLIC] ->
            %% Exact-match hit on a path explicitly declared public.
            public;
        Scopes ->
            {scopes, Scopes}
    end.

%% Iterate templates and return the scopes of the first one whose
%% segments match the request path. Concrete path segments must equal
%% template segments verbatim except where the template segment starts
%% with `:' (path parameter), which matches any single segment, or is
%% the trailing `[...]' catch-all, which matches the rest of the path.
%%
%% Entries whose value is `[?SCOPE_PUBLIC]' are skipped: they are kept in
%% the cache as sentinels (so exact-match lookup can distinguish
%% "intentionally public" from "genuinely unmapped"), but they must
%% not claim a sibling concrete path via wildcard segment match.
%% Without this skip, e.g. `/sso/running' (public) would be claimed
%% by the sibling template `/sso/:backend' (sso_management).
%%
%% Match cost is O(n*m) where n is the number of templates and m is
%% the average path depth. The cache is small (~250 entries) and this
%% function is called once per authorised request, so the cost is
%% acceptable.
match_template(Path, PathMap) ->
    PathSegs = normalize_segments(split_segments(Path)),
    Iter = maps:iterator(PathMap),
    match_template_iter(PathSegs, Iter).

match_template_iter(PathSegs, Iter) ->
    case maps:next(Iter) of
        none ->
            undefined;
        {_Tmpl, [?SCOPE_PUBLIC], Iter1} ->
            match_template_iter(PathSegs, Iter1);
        {Tmpl, Scopes, Iter1} ->
            case segments_match(PathSegs, split_segments(Tmpl)) of
                true -> Scopes;
                false -> match_template_iter(PathSegs, Iter1)
            end
    end.

split_segments(Path) ->
    %% Drop the leading empty segment from the leading slash.
    case binary:split(Path, <<"/">>, [global]) of
        [<<>> | Rest] -> Rest;
        Other -> Other
    end.

%% Canonicalise concrete request-path segments the same way
%% `cowboy_router' does before it selects a handler: percent-decode
%% each segment (`cow_uri:urldecode/1', the exact decoder the router
%% uses) and resolve `.'/`..' segments. The lookup must operate on the
%% same segments the router dispatched on; comparing raw request
%% segments against decoded templates would let a request reach a
%% handler while its scope lookup matches nothing. Decoding is done
%% after splitting so an encoded separator stays within its segment
%% and never introduces an extra boundary.
normalize_segments(Segments) ->
    remove_dot_segments([urldecode(S) || S <- Segments], []).

%% `cow_uri:urldecode/1' raises on a byte that is not valid in a URI
%% path. Cowboy validates a request path before it dispatches, so such
%% a byte can only reach here from a value that did not come from the
%% router — a route template carrying `[...]', for one. Keep the
%% segment verbatim in that case: it then matches only a template
%% segment that is literally equal, and the authorisation path returns
%% a decision instead of raising.
urldecode(Segment) ->
    try
        cow_uri:urldecode(Segment)
    catch
        _:_ -> Segment
    end.

remove_dot_segments([], Acc) ->
    lists:reverse(Acc);
remove_dot_segments([<<".">> | Segments], Acc) ->
    remove_dot_segments(Segments, Acc);
remove_dot_segments([<<"..">> | Segments], []) ->
    remove_dot_segments(Segments, []);
remove_dot_segments([<<"..">> | Segments], [_ | Acc]) ->
    remove_dot_segments(Segments, Acc);
remove_dot_segments([Segment | Segments], Acc) ->
    remove_dot_segments(Segments, [Segment | Acc]).

%% A trailing `[...]' is cowboy's catch-all: it matches the whole
%% remainder of the path, including an empty remainder. It may only
%% appear as the last template segment, so this clause consumes
%% everything that is left.
segments_match(_PathSegs, [?CATCH_ALL_SEGMENT]) ->
    true;
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
    PathToScopes = collect_scopes_from_modules(),
    persistent_term:put(?CACHE_KEY, #{path_to_scopes => PathToScopes}),
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

%% @doc Collect path → scopes mappings from all API modules that export scopes/0.
%% Returns a flat map: #{<<"/clients">> => [<<"connections">>], ...}.
-spec collect_scopes_from_modules() -> #{binary() => [binary(), ...]}.
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
%%   - a scope declaration (all paths share it)
%%   - a #{Path => ScopeDeclaration} map (per-path assignment)
%% A scope declaration is a scope name binary, or a non-empty list of
%% scope names meaning "any one of these grants access".
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

collect_paths_with_scope(Module, Paths, ScopeMap, Acc) when is_map(ScopeMap) ->
    %% Map form: per-path scope assignment. The sentinel ?SCOPE_PUBLIC
    %% marks paths that are intentionally unscoped (pre-login entry
    %% points and static catalog endpoints). Such paths ARE inserted
    %% into the cache, carrying `[?SCOPE_PUBLIC]' as the value, so that:
    %%
    %%   * exact-match lookup can distinguish "explicitly public" from
    %%     "genuinely unmapped" and return `undefined' for the former
    %%     (preventing a sibling wildcard template from silently
    %%     claiming an endpoint the module owner declared public); and
    %%   * the template iterator can skip these entries when doing
    %%     segment matching, so e.g. `/sso/:backend' does not absorb
    %%     `/sso/running'.
    %%
    %% Genuinely missing paths still warn.
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
                Declared ->
                    insert_path_scopes(Module, PathBin, Declared, InnerAcc)
            end
        end,
        Acc,
        Paths
    );
collect_paths_with_scope(Module, Paths, Declared, Acc) ->
    %% Simple form: all paths share the same scope declaration.
    lists:foldl(
        fun(Path, InnerAcc) ->
            insert_path_scopes(Module, path_to_binary(Path), Declared, InnerAcc)
        end,
        Acc,
        Paths
    ).

insert_path_scopes(Module, PathBin, Declared, Acc) ->
    case normalize_declaration(Declared) of
        invalid ->
            %% Leave the path unmapped rather than guessing. Same
            %% outcome as a module whose scopes/0 crashes: the path
            %% falls through to the unmapped fail-open, and the warning
            %% is the signal that the declaration needs fixing.
            ?SLOG(warning, #{
                msg => "invalid_scope_declaration",
                module => Module,
                path => PathBin,
                declared => Declared
            }),
            Acc;
        Scopes ->
            Acc#{PathBin => Scopes}
    end.

%% Normalize a declared scope value to a non-empty list of scope
%% names. `?SCOPE_PUBLIC' is exclusive: a public path is unscoped, so
%% listing it alongside a real scope is a contradiction, not a union.
normalize_declaration(Scope) when is_binary(Scope) ->
    [Scope];
normalize_declaration([?SCOPE_PUBLIC]) ->
    [?SCOPE_PUBLIC];
normalize_declaration([_ | _] = Scopes) ->
    IsValid =
        lists:all(fun is_binary/1, Scopes) andalso
            not lists:member(?SCOPE_PUBLIC, Scopes),
    case IsValid of
        true -> Scopes;
        false -> invalid
    end;
normalize_declaration(_Other) ->
    invalid.

path_to_binary(Path) when is_binary(Path) ->
    ensure_leading_slash(Path);
path_to_binary(Path) when is_list(Path) ->
    ensure_leading_slash(iolist_to_binary(filename:join("/", Path))).

ensure_leading_slash(<<"/", _/binary>> = Path) -> Path;
ensure_leading_slash(Path) -> <<"/", Path/binary>>.
