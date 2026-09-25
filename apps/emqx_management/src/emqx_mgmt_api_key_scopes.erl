%%--------------------------------------------------------------------
%% Copyright (c) 2020-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mgmt_api_key_scopes).

-moduledoc """
API Key scope management.

Each minirest_api module declares its scope via a `scopes/0` callback
that returns either a scope declaration (all paths share it) or a
`#{Path => ScopeDeclaration}` map (for modules whose endpoints span
multiple scopes). A map is keyed by the exact terms `paths/0` returns;
a key that is not one of them is rejected, and a path that is missing
from the map is left unmapped. Both warn at collection.

A scope declaration is either a single scope name binary or a
non-empty list of scope names. A list means the endpoint is reachable
by a holder of *any one* of the listed scopes. Use it only where an
endpoint genuinely has two legitimate audiences — for example the
plugin API gateway, which a dedicated restricted scope reaches and
which `system` must keep reaching for backward compatibility.

This module collects those declarations into a cache keyed by the
handler that serves each path: `{Module, OperationId}`, where
`OperationId` is the `'operationId'` of the path's `schema/1` entry.
That is the same `{module, function}` pair minirest puts in the
`HandlerInfo` of every authorised request, so a scope lookup never
re-derives which route a request matched. Cowboy's router is the only
route selector.

Cowboy handlers that are not minirest API modules have no `scopes/0`
for the collector to find. Their scopes are listed in
`non_minirest_handler_scopes/0`.

Scopes are decoupled from OpenAPI tags: scope names are stable
identifiers defined in `emqx_api_key_scopes.hrl`. The internal mapping
from handlers to scopes can change across versions without affecting
user-facing API key configurations.
""".

-include_lib("emqx/include/logger.hrl").
-include_lib("emqx_utils/include/emqx_api_key_scopes.hrl").

-export([
    handler_scopes/1,
    classify_handler/1,
    any_scope_granted/2,
    init_cache/0,
    clear_cache/0,
    validate_scopes/1,
    filter_valid_scopes/1,
    is_denied_scope/1
]).

-ifdef(TEST).
-export([
    collect_scopes_from_modules/0,
    find_api_modules/0,
    operation_id/2
]).
-endif.

-define(CACHE_KEY, {?MODULE, scope_cache}).

-type handler_info() :: #{module := module(), function := atom(), _ => _}.
-type handler_key() :: {module(), atom()}.

%%--------------------------------------------------------------------
%% Handler → scope lookup
%%--------------------------------------------------------------------

-doc """
Given the `HandlerInfo` of a request, return the scopes that grant
access to its endpoint, or `undefined` if the handler is unmapped.
Holding any one of the returned scopes is enough — see
`any_scope_granted/2`.

The lookup is an exact map hit on `{module, function}`. Both are set
by minirest from the route cowboy dispatched to, so the lookup agrees
with the router by construction.
""".
-spec handler_scopes(handler_info() | handler_key()) -> [binary(), ...] | undefined.
handler_scopes(Handler) ->
    %% Two-way view kept for the API-key authorisation path
    %% (`emqx_mgmt_auth:check_scopes/2'), which treats both "public"
    %% and "unmapped" as unscoped. Callers that must tell the two apart
    %% use `classify_handler/1'.
    case classify_handler(Handler) of
        {scopes, Scopes} -> Scopes;
        public -> undefined;
        not_found -> undefined
    end.

-doc """
Return `true` when the holder's scope list covers an endpoint, i.e. it
contains at least one of the scopes the endpoint declares.

An endpoint may declare several acceptable scopes, so membership of
any one of them grants access. Both the API-key check
(`emqx_mgmt_auth:check_scopes/2`) and the login-user check
(`emqx_dashboard_rbac:check_login_user_scopes/3`) must use this
predicate, so the two never diverge on a multi-scope endpoint.
""".
-spec any_scope_granted([binary()], [binary()]) -> boolean().
any_scope_granted(Declared, HeldScopes) ->
    lists:any(fun(S) -> lists:member(S, HeldScopes) end, Declared).

-doc """
Three-way classification of a request handler:

* `{scopes, Names}' — any one of `Names' grants access to the endpoint.
* `public'          — the endpoint is declared `?SCOPE_PUBLIC'.
* `not_found'       — the handler maps to no scope at all.

Unlike `handler_scopes/1', which collapses `public' and `not_found'
into `undefined', this keeps them distinct so a caller can allow
public endpoints while denying genuinely-unmapped ones (fail closed).
""".
-spec classify_handler(handler_info() | handler_key()) ->
    {scopes, [binary(), ...]} | public | not_found.
classify_handler(#{module := Module, function := Function}) ->
    classify_handler({Module, Function});
classify_handler({_Module, _Function} = Key) ->
    case get_cache() of
        undefined ->
            init_cache(),
            classify_with_cache(Key);
        #{handler_scopes := HandlerMap} ->
            classify(Key, HandlerMap)
    end.

classify_with_cache(Key) ->
    case get_cache() of
        #{handler_scopes := HandlerMap} ->
            classify(Key, HandlerMap);
        _ ->
            not_found
    end.

classify(Key, HandlerMap) ->
    case maps:get(Key, HandlerMap, undefined) of
        undefined -> not_found;
        [?SCOPE_PUBLIC] -> public;
        Scopes -> {scopes, Scopes}
    end.

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
    HandlerMap = maps:merge(collect_scopes_from_modules(), non_minirest_handler_scopes()),
    persistent_term:put(?CACHE_KEY, #{handler_scopes => HandlerMap}),
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

%% Scopes of cowboy handlers that authenticate through the same
%% primitives as minirest but are not minirest API modules, so the
%% collector cannot find them. Every key here is a deliberate exception.
%%
%% `emqx_dashboard_api_spec_handler' serves the OpenAPI documents
%% (`/api-docs/swagger.json', `/api-spec*'). Any authenticated user or
%% API key may read them, whatever scopes it holds. The handler still
%% rejects unauthenticated requests with 401.
non_minirest_handler_scopes() ->
    #{
        {emqx_dashboard_api_spec_handler, handle_get} => [?SCOPE_PUBLIC]
    }.

%% @doc Collect handler → scopes mappings from all API modules that export scopes/0.
%% Returns a flat map: #{{emqx_mgmt_api_clients, clients} => [<<"connections">>], ...}.
-spec collect_scopes_from_modules() -> #{handler_key() => [binary(), ...]}.
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
                ok = check_scope_map_keys(Module, Paths, ScopeSpec),
                lists:foldl(
                    fun(Path, InnerAcc) ->
                        collect_path(Module, Path, ScopeSpec, InnerAcc)
                    end,
                    Acc,
                    Paths
                )
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

%% Resolve one declared path to its handler and insert the declared
%% scopes under that key. A path that cannot be resolved or whose
%% declaration is malformed is left unmapped. A warning is the signal
%% that the module needs fixing; the CT invariant test fails on it.
collect_path(Module, Path, ScopeSpec, Acc) ->
    maybe
        {ok, Declared} ?= declared_scopes(Module, Path, ScopeSpec),
        {ok, Scopes} ?= normalize_declaration(Module, Path, Declared),
        {ok, OperationId} ?= operation_id(Module, Path),
        insert_handler_scopes(Module, Path, {Module, OperationId}, Scopes, Acc)
    else
        error -> Acc
    end.

%% A map-form scopes/0 is keyed by the exact terms `paths/0' returns.
%% A key that is not one of them names nothing the router serves: a
%% typo, or a path that was removed or renamed. Warn on each; the CT
%% invariant `t_scope_map_keys_are_declared_paths' fails on it.
check_scope_map_keys(Module, Paths, ScopeMap) when is_map(ScopeMap) ->
    lists:foreach(
        fun(Key) ->
            ?SLOG(warning, #{
                msg => "scope_map_key_not_a_declared_path",
                module => Module,
                key => Key
            })
        end,
        maps:keys(ScopeMap) -- Paths
    );
check_scope_map_keys(_Module, _Paths, _Declared) ->
    ok.

%% Map form: per-path scope assignment, looked up by the exact path
%% term with no normalization. The sentinel ?SCOPE_PUBLIC marks paths
%% that are intentionally unscoped (pre-login entry points and static
%% catalog endpoints). Such paths ARE inserted into the cache, carrying
%% `[?SCOPE_PUBLIC]', so that lookups can tell "explicitly public" from
%% "genuinely unmapped". Genuinely missing paths warn.
declared_scopes(Module, Path, ScopeMap) when is_map(ScopeMap) ->
    case maps:get(Path, ScopeMap, undefined) of
        undefined ->
            ?SLOG(warning, #{
                msg => "path_missing_from_scopes_map",
                module => Module,
                path => Path
            }),
            error;
        Declared ->
            {ok, Declared}
    end;
declared_scopes(_Module, _Path, Declared) ->
    %% Simple form: all paths share the same scope declaration.
    {ok, Declared}.

%% Normalize a declared scope value to a non-empty list of scope
%% names. `?SCOPE_PUBLIC' is exclusive: a public endpoint is unscoped,
%% so listing it alongside a real scope is a contradiction, not a union.
normalize_declaration(_Module, _Path, Scope) when is_binary(Scope) ->
    {ok, [Scope]};
normalize_declaration(_Module, _Path, [?SCOPE_PUBLIC]) ->
    {ok, [?SCOPE_PUBLIC]};
normalize_declaration(Module, Path, Declared) ->
    IsValid =
        is_list(Declared) andalso Declared =/= [] andalso
            lists:all(fun is_binary/1, Declared) andalso
            not lists:member(?SCOPE_PUBLIC, Declared),
    case IsValid of
        true ->
            {ok, Declared};
        false ->
            ?SLOG(warning, #{
                msg => "invalid_scope_declaration",
                module => Module,
                path => Path,
                declared => Declared
            }),
            error
    end.

%% The handler that serves `Path' is the `'operationId'' of its
%% `schema/1' entry: `emqx_dashboard_swagger:spec/2' registers exactly
%% that atom as the minirest route function.
-spec operation_id(module(), string() | binary()) -> {ok, atom()} | error.
operation_id(Module, Path) ->
    try apply(Module, schema, [Path]) of
        #{'operationId' := OperationId} when is_atom(OperationId) ->
            {ok, OperationId};
        _ ->
            ?SLOG(warning, #{
                msg => "path_has_no_operation_id",
                module => Module,
                path => Path
            }),
            error
    catch
        Class:Reason ->
            ?SLOG(warning, #{
                msg => "failed_to_resolve_operation_id",
                module => Module,
                path => Path,
                class => Class,
                reason => Reason
            }),
            error
    end.

%% Two paths of one module may share an operationId (one function
%% serving both), so the same key can be reached twice. Equal
%% declarations are fine. Unequal ones are a contradiction: keep the
%% first, which is deterministic (`paths/0' order), and warn.
insert_handler_scopes(Module, Path, Key, Scopes, Acc) ->
    case maps:get(Key, Acc, undefined) of
        undefined ->
            Acc#{Key => Scopes};
        Scopes ->
            Acc;
        Existing ->
            ?SLOG(warning, #{
                msg => "conflicting_scope_declaration",
                module => Module,
                path => Path,
                handler => Key,
                declared => Scopes,
                kept => Existing
            }),
            Acc
    end.
