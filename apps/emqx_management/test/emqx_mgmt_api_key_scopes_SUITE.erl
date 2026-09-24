%%--------------------------------------------------------------------
%% Copyright (c) 2020-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_mgmt_api_key_scopes_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("emqx_dashboard/include/emqx_dashboard_rbac.hrl").
-include_lib("emqx_utils/include/emqx_api_key_scopes.hrl").

%%--------------------------------------------------------------------
%% CT boilerplate
%%--------------------------------------------------------------------

all() ->
    [
        {group, unit_tests},
        {group, integration_tests},
        {group, api_tests}
    ].

suite() -> [{timetrap, {minutes, 1}}].

groups() ->
    [
        {unit_tests, [], [
            t_init_cache,
            t_scope_catalog,
            t_handler_scopes,
            t_handler_scopes_login_only,
            t_handler_scopes_no_cache,
            t_classify_handler,
            t_multi_scope_handler,
            t_authorize_denied_handler,
            t_validate_scopes,
            t_validate_scopes_bad_input,
            t_is_denied_scope,
            t_all_modules_have_scopes,
            t_all_endpoints_covered_by_scopes,
            t_no_conflicting_declarations,
            t_scope_map_keys_are_declared_paths,
            t_init_cache_no_missing_path_warnings,
            t_public_handlers_are_unscoped
        ]},
        {integration_tests, [parallel], [
            t_authorize_with_scopes,
            t_authorize_no_scopes,
            t_authorize_empty_scopes,
            t_check_scopes_unmapped_handler
        ]},
        {api_tests, [parallel], [
            t_api_list_scopes,
            t_api_create_with_scopes,
            t_api_update_scopes,
            t_api_post_materialises_default_scopes,
            t_api_legacy_record_shows_unset_sentinel
        ]}
    ].

init_per_suite(Config) ->
    application:ensure_all_started(hackney),
    Apps = emqx_cth_suite:start(
        [
            emqx_conf,
            emqx_management,
            emqx_mgmt_api_test_util:emqx_dashboard()
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{suite_apps, Apps} | Config].

end_per_suite(Config) ->
    ok = emqx_cth_suite:stop(?config(suite_apps, Config)),
    application:stop(hackney).

init_per_group(unit_tests, Config) ->
    emqx_mgmt_api_key_scopes:clear_cache(),
    Config;
init_per_group(_Group, Config) ->
    emqx_mgmt_api_key_scopes:init_cache(),
    Config.

end_per_group(_Group, Config) ->
    Config.

%%--------------------------------------------------------------------
%% Unit tests
%%--------------------------------------------------------------------

t_init_cache(_Config) ->
    emqx_mgmt_api_key_scopes:clear_cache(),
    ?assertEqual(
        undefined, persistent_term:get({emqx_mgmt_api_key_scopes, scope_cache}, undefined)
    ),
    ?assertEqual(ok, emqx_mgmt_api_key_scopes:init_cache()),
    Cache = persistent_term:get({emqx_mgmt_api_key_scopes, scope_cache}, undefined),
    ?assertNotEqual(undefined, Cache),
    ?assertMatch(#{handler_scopes := _}, Cache),
    ?assertEqual(ok, emqx_mgmt_api_key_scopes:clear_cache()),
    ?assertEqual(
        undefined, persistent_term:get({emqx_mgmt_api_key_scopes, scope_cache}, undefined)
    ).

t_scope_catalog(_Config) ->
    Catalog = emqx_scope_catalog:scope_catalog(),
    ?assert(is_list(Catalog)),
    %% Each entry has name (binary) and desc (i18n handle).
    lists:foreach(
        fun(Entry) ->
            ?assertMatch(#{name := _, desc := _}, Entry),
            #{name := Name, desc := Desc} = Entry,
            ?assert(is_binary(Name)),
            %% desc is the `?DESC(Mod, Id)' tuple; runtime callers
            %% resolve it via emqx_dashboard_swagger:get_i18n/4.
            ?assertMatch({desc, _Mod, _Id}, Desc)
        end,
        Catalog
    ),
    %% Known scopes must be present
    Names = [N || #{name := N} <- Catalog],
    ?assert(lists:member(?SCOPE_CONNECTIONS, Names)),
    ?assert(lists:member(?SCOPE_PUBLISH, Names)),
    ?assert(lists:member(?SCOPE_DATA_INTEGRATION, Names)),
    ?assert(lists:member(?SCOPE_ACCESS_CONTROL, Names)),
    ?assert(lists:member(?SCOPE_GATEWAYS, Names)),
    ?assert(lists:member(?SCOPE_MONITORING, Names)),
    ?assert(lists:member(?SCOPE_CLUSTER_OPERATIONS, Names)),
    ?assert(lists:member(?SCOPE_SYSTEM, Names)),
    ?assert(lists:member(?SCOPE_AUDIT, Names)),
    ?assert(lists:member(?SCOPE_LICENSE, Names)),
    %% $denied must NOT be in the catalog
    ?assertNot(lists:member(?SCOPE_DENIED, Names)).

-doc """
A handler resolves to the scopes its module declares for the path the
handler serves. The key is the `{module, function}' minirest puts in
`HandlerInfo', so the lookup is an exact map hit.
""".
t_handler_scopes(_Config) ->
    emqx_mgmt_api_key_scopes:init_cache(),
    ?assertEqual(
        [?SCOPE_CONNECTIONS],
        emqx_mgmt_api_key_scopes:handler_scopes({emqx_mgmt_api_clients, clients})
    ),
    %% The HandlerInfo map form is accepted too.
    ?assertEqual(
        [?SCOPE_CONNECTIONS],
        emqx_mgmt_api_key_scopes:handler_scopes(
            #{method => get, module => emqx_mgmt_api_clients, function => client}
        )
    ),
    ?assertEqual(
        [?SCOPE_PUBLISH],
        emqx_mgmt_api_key_scopes:handler_scopes({emqx_mgmt_api_publish, publish})
    ),
    %% Unknown handler -> undefined
    ?assertEqual(
        undefined,
        emqx_mgmt_api_key_scopes:handler_scopes({no_such_api_module, no_such_function})
    ),
    emqx_mgmt_api_key_scopes:clear_cache().

t_handler_scopes_login_only(_Config) ->
    emqx_mgmt_api_key_scopes:init_cache(),
    %% Dashboard / API-key management handlers map to login-only
    %% scopes. API keys still cannot reach these endpoints: minirest's
    %% bearer-only `security' declaration rejects API key
    %% authentication before the scope check.
    ?assertEqual(
        [?SCOPE_USER_MGMT],
        emqx_mgmt_api_key_scopes:handler_scopes({emqx_dashboard_api, users})
    ),
    ?assertEqual(
        [?SCOPE_API_KEY_MGMT],
        emqx_mgmt_api_key_scopes:handler_scopes({emqx_mgmt_api_api_keys, api_key})
    ),
    emqx_mgmt_api_key_scopes:clear_cache().

t_handler_scopes_no_cache(_Config) ->
    emqx_mgmt_api_key_scopes:clear_cache(),
    %% Should lazy-init and return correct scope
    ?assertEqual(
        [?SCOPE_CONNECTIONS],
        emqx_mgmt_api_key_scopes:handler_scopes({emqx_mgmt_api_clients, clients})
    ).

-doc """
`classify_handler/1' distinguishes the three cases the login-user scope
check needs to tell apart: a known scope, an explicitly public
endpoint, and a genuinely-unmapped handler. `handler_scopes/1' keeps
collapsing the last two to `undefined' for the API-key path.
""".
t_classify_handler(_Config) ->
    emqx_mgmt_api_key_scopes:init_cache(),
    %% Mapped handler -> {scopes, Names}.
    ?assertEqual(
        {scopes, [?SCOPE_CONNECTIONS]},
        emqx_mgmt_api_key_scopes:classify_handler({emqx_mgmt_api_clients, clients})
    ),
    %% Genuinely unmapped -> not_found (login-user path denies these).
    ?assertEqual(
        not_found,
        emqx_mgmt_api_key_scopes:classify_handler({no_such_api_module, no_such_function})
    ),
    %% Explicitly public -> public (login-user path allows these).
    [PublicHandler | _] = collect_public_handlers(emqx_mgmt_api_key_scopes:find_api_modules()),
    ?assertEqual(public, emqx_mgmt_api_key_scopes:classify_handler(PublicHandler)),
    %% handler_scopes/1 still collapses public and unmapped to undefined.
    ?assertEqual(undefined, emqx_mgmt_api_key_scopes:handler_scopes(PublicHandler)),
    ?assertEqual(
        undefined,
        emqx_mgmt_api_key_scopes:handler_scopes({no_such_api_module, no_such_function})
    ),
    emqx_mgmt_api_key_scopes:clear_cache().

-doc """
An endpoint may declare several acceptable scopes. `classify_handler/1'
returns all of them and `any_scope_granted/2' grants access to a
holder of any single one, so an endpoint with two legitimate
audiences does not have to pick one of them.
""".
t_multi_scope_handler(_Config) ->
    Handler = {test_multi_scope_module, test_multi_scope_function},
    with_scope_cache(
        #{Handler => [?SCOPE_MONITORING, ?SCOPE_SYSTEM]},
        fun() ->
            ?assertEqual(
                {scopes, [?SCOPE_MONITORING, ?SCOPE_SYSTEM]},
                emqx_mgmt_api_key_scopes:classify_handler(Handler)
            ),
            ?assertEqual(
                [?SCOPE_MONITORING, ?SCOPE_SYSTEM],
                emqx_mgmt_api_key_scopes:handler_scopes(Handler)
            )
        end
    ),
    Declared = [?SCOPE_MONITORING, ?SCOPE_SYSTEM],
    ?assert(emqx_mgmt_api_key_scopes:any_scope_granted(Declared, [?SCOPE_SYSTEM])),
    ?assert(emqx_mgmt_api_key_scopes:any_scope_granted(Declared, [?SCOPE_MONITORING])),
    ?assert(
        emqx_mgmt_api_key_scopes:any_scope_granted(
            Declared, [?SCOPE_PUBLISH, ?SCOPE_MONITORING]
        )
    ),
    ?assertNot(emqx_mgmt_api_key_scopes:any_scope_granted(Declared, [?SCOPE_PUBLISH])),
    ?assertNot(emqx_mgmt_api_key_scopes:any_scope_granted(Declared, [])).

%% Run `Fun' against a synthetic handler -> scopes cache, so the
%% lookup semantics can be exercised without depending on which API
%% modules happen to be loaded. Restores the previous cache after.
with_scope_cache(HandlerMap, Fun) ->
    Key = {emqx_mgmt_api_key_scopes, scope_cache},
    Prev = persistent_term:get(Key, undefined),
    persistent_term:put(Key, #{handler_scopes => HandlerMap}),
    try
        Fun()
    after
        case Prev of
            undefined -> emqx_mgmt_api_key_scopes:clear_cache();
            _ -> persistent_term:put(Key, Prev)
        end
    end.

t_validate_scopes(_Config) ->
    %% Valid: known scope names
    ?assertEqual(ok, emqx_mgmt_api_key_scopes:validate_scopes([?SCOPE_CONNECTIONS])),
    ?assertEqual(
        ok,
        emqx_mgmt_api_key_scopes:validate_scopes([?SCOPE_CONNECTIONS, ?SCOPE_PUBLISH])
    ),
    %% Valid: empty list
    ?assertEqual(ok, emqx_mgmt_api_key_scopes:validate_scopes([])),
    %% Invalid: unknown scope
    ?assertMatch(
        {error, <<"Unknown scopes: ", _/binary>>},
        emqx_mgmt_api_key_scopes:validate_scopes([<<"nonexistent_scope_xyz">>])
    ),
    %% Invalid: $denied is not a valid user scope
    ?assertMatch(
        {error, <<"Unknown scopes: ", _/binary>>},
        emqx_mgmt_api_key_scopes:validate_scopes([?SCOPE_DENIED])
    ).

t_validate_scopes_bad_input(_Config) ->
    ?assertMatch(
        {error, <<"scopes must be a list of strings">>},
        emqx_mgmt_api_key_scopes:validate_scopes(<<"not_a_list">>)
    ),
    ?assertMatch(
        {error, <<"scopes must be a list of strings">>},
        emqx_mgmt_api_key_scopes:validate_scopes(42)
    ),
    ?assertMatch(
        {error, <<"scopes must be a list of strings">>},
        emqx_mgmt_api_key_scopes:validate_scopes([1, 2, 3])
    ),
    ?assertMatch(
        {error, <<"scopes must be a list of strings">>},
        emqx_mgmt_api_key_scopes:validate_scopes([null])
    ).

t_is_denied_scope(_Config) ->
    ?assert(emqx_mgmt_api_key_scopes:is_denied_scope(?SCOPE_DENIED)),
    ?assertNot(emqx_mgmt_api_key_scopes:is_denied_scope(?SCOPE_CONNECTIONS)),
    ?assertNot(emqx_mgmt_api_key_scopes:is_denied_scope(?SCOPE_PUBLISH)),
    ?assertNot(emqx_mgmt_api_key_scopes:is_denied_scope(<<"random">>)).

t_all_modules_have_scopes(_Config) ->
    %% Critical coverage test: every minirest_api module must export scopes/0.
    %% This is the compile-time-equivalent CI check done at test time.
    emqx_mgmt_api_key_scopes:init_cache(),
    HandlerToScopes = emqx_mgmt_api_key_scopes:collect_scopes_from_modules(),
    ?assert(map_size(HandlerToScopes) > 0),
    %% Every handler should map to a known scope, $denied, $public, or
    %% one of the four login-only scopes (user/mfa/sso/api_key_management
    %% — these apply to dashboard login users only and are not in the
    %% API key scope catalog).
    AllValidScopes =
        [N || #{name := N} <- emqx_scope_catalog:scope_catalog()] ++
            [?SCOPE_DENIED] ++
            [?SCOPE_PUBLIC] ++
            ?LOGIN_ONLY_SCOPES,
    maps:foreach(
        fun(Handler, Scopes) ->
            Unknown = [S || S <- Scopes, not lists:member(S, AllValidScopes)],
            ?assertEqual(
                [],
                Unknown,
                lists:flatten(
                    io_lib:format("Handler ~p mapped to unknown scope(s) ~p", [Handler, Unknown])
                )
            )
        end,
        HandlerToScopes
    ),
    emqx_mgmt_api_key_scopes:clear_cache().

-doc """
Every path every API module declares must resolve to a handler that is
in the scope cache. This is the invariant the handler-keyed lookup
rests on: a path whose `schema/1' yields no `operationId', or whose
declaration the collector rejected, is unmapped, and unmapped means
fail-open for API keys.

Public paths count as covered: they are in the cache under
`?SCOPE_PUBLIC', so a genuinely forgotten path is the only way to
fail here.
""".
t_all_endpoints_covered_by_scopes(_Config) ->
    emqx_mgmt_api_key_scopes:init_cache(),
    HandlerToScopes = emqx_mgmt_api_key_scopes:collect_scopes_from_modules(),
    Modules = emqx_mgmt_api_key_scopes:find_api_modules(),
    Uncovered = lists:flatmap(
        fun(M) ->
            [
                {M, path_to_binary(P), Reason}
             || P <- safe_paths(M),
                Reason <- [coverage_gap(M, P, HandlerToScopes)],
                Reason =/= covered
            ]
        end,
        Modules
    ),
    ?assertEqual(
        [],
        Uncovered,
        lists:flatten(
            io_lib:format(
                "~p endpoint path(s) not covered by any scope: ~p",
                [length(Uncovered), Uncovered]
            )
        )
    ),
    emqx_mgmt_api_key_scopes:clear_cache().

coverage_gap(Module, Path, HandlerToScopes) ->
    case emqx_mgmt_api_key_scopes:operation_id(Module, Path) of
        {ok, OperationId} ->
            case maps:is_key({Module, OperationId}, HandlerToScopes) of
                true -> covered;
                false -> no_scope_declared
            end;
        error ->
            no_operation_id
    end.

-doc """
Two paths of one module may share an `operationId' (one function serves
both). The cache then holds one entry for both, so their declarations
must agree, or the collector keeps the first and the second silently
loses its own scope.
""".
t_no_conflicting_declarations(_Config) ->
    Modules = emqx_mgmt_api_key_scopes:find_api_modules(),
    Conflicts = lists:flatmap(fun conflicting_declarations/1, Modules),
    ?assertEqual(
        [],
        Conflicts,
        lists:flatten(
            io_lib:format("handlers with conflicting scope declarations: ~p", [Conflicts])
        )
    ).

conflicting_declarations(Module) ->
    Declared = [
        {OperationId, declared_for(Module, P)}
     || P <- safe_paths(Module),
        {ok, OperationId} <- [emqx_mgmt_api_key_scopes:operation_id(Module, P)]
    ],
    ByHandler = maps:groups_from_list(
        fun({OperationId, _}) -> OperationId end, fun({_, S}) -> S end, Declared
    ),
    [
        {Module, OperationId, lists:usort(Scopes)}
     || {OperationId, Scopes} <- maps:to_list(ByHandler),
        length(lists:usort(Scopes)) > 1
    ].

declared_for(Module, Path) ->
    case safe_scopes(Module) of
        Map when is_map(Map) -> maps:get(Path, Map, undefined);
        Scope -> Scope
    end.

-doc """
A map-form scopes/0 is keyed by the exact terms paths/0 returns. A key
that is not one of them names nothing the router serves: a typo, or a
path that was removed or renamed, whose scope entry would otherwise
outlive it unnoticed.
""".
t_scope_map_keys_are_declared_paths(_Config) ->
    Modules = emqx_mgmt_api_key_scopes:find_api_modules(),
    Stray = lists:flatmap(
        fun(M) ->
            case safe_scopes(M) of
                Map when is_map(Map) -> [{M, K} || K <- maps:keys(Map) -- safe_paths(M)];
                _ -> []
            end
        end,
        Modules
    ),
    ?assertEqual(
        [],
        Stray,
        lists:flatten(io_lib:format("scopes/0 map keys that are not declared paths: ~p", [Stray]))
    ).

-doc """
Every map-form scopes/0 callback must list every path returned by
that module's paths/0. This is the precondition that determines
whether the collector emits `path_missing_from_scopes_map` at boot;
checking it directly here avoids the need to scrape the live logger
and keeps the failure message specific (module + missing path).
""".
t_init_cache_no_missing_path_warnings(_Config) ->
    Modules = emqx_mgmt_api_key_scopes:find_api_modules(),
    Missing = lists:flatmap(
        fun(M) ->
            case safe_scopes(M) of
                Map when is_map(Map) ->
                    [{M, P} || P <- safe_paths(M), not maps:is_key(P, Map)];
                _ ->
                    []
            end
        end,
        Modules
    ),
    ?assertEqual(
        [],
        Missing,
        lists:flatten(
            io_lib:format(
                "modules with paths missing from their scopes/0 map: ~p", [Missing]
            )
        )
    ).

safe_scopes(M) ->
    try
        apply(M, scopes, [])
    catch
        _:_ -> undefined
    end.

safe_paths(M) ->
    try
        apply(M, paths, [])
    catch
        _:_ -> []
    end.

-doc """
?SCOPE_PUBLIC endpoints must keep the unmapped/fail-open semantics that
production relies on for `/login' and friends: `handler_scopes/1' must
return `undefined' for their handlers so the API-key authorisation
layer treats them as unscoped, while `classify_handler/1' reports them
as `public' so the login-user layer allows them for every user.
""".
t_public_handlers_are_unscoped(_Config) ->
    emqx_mgmt_api_key_scopes:init_cache(),
    PublicHandlers = collect_public_handlers(emqx_mgmt_api_key_scopes:find_api_modules()),
    %% Sanity: the production code under test declares at least one
    %% public path. If this drops to zero, the test no longer guards
    %% anything; loudly fail instead of silently passing.
    ?assert(PublicHandlers =/= [], "no ?SCOPE_PUBLIC paths declared -- test now vacuous"),
    lists:foreach(
        fun(Handler) ->
            ?assertEqual(
                undefined,
                emqx_mgmt_api_key_scopes:handler_scopes(Handler),
                lists:flatten(io_lib:format("public handler ~p carries a scope", [Handler]))
            ),
            ?assertEqual(public, emqx_mgmt_api_key_scopes:classify_handler(Handler))
        end,
        PublicHandlers
    ),
    emqx_mgmt_api_key_scopes:clear_cache().

%% The handlers of every path declared ?SCOPE_PUBLIC, derived from each
%% module's scopes/0 map so adding a public path only requires editing
%% that module.
collect_public_handlers(Modules) ->
    lists:usort(
        lists:flatmap(
            fun(M) ->
                case safe_scopes(M) of
                    Map when is_map(Map) ->
                        [
                            {M, OperationId}
                         || P <- safe_paths(M),
                            maps:get(P, Map, undefined) =:= ?SCOPE_PUBLIC,
                            {ok, OperationId} <- [emqx_mgmt_api_key_scopes:operation_id(M, P)]
                        ];
                    _ ->
                        []
                end
            end,
            Modules
        )
    ).

path_to_binary(P) when is_binary(P) ->
    case P of
        <<"/", _/binary>> -> P;
        _ -> <<"/", P/binary>>
    end;
path_to_binary(P) when is_list(P) ->
    path_to_binary(iolist_to_binary(filename:join("/", P))).

%%--------------------------------------------------------------------
%% Integration tests
%%--------------------------------------------------------------------

%%--------------------------------------------------------------------
%% Integration tests
%%--------------------------------------------------------------------

t_authorize_with_scopes(_Config) ->
    Name = <<"SCOPES-TEST-WITH">>,
    {ok, #{<<"api_key">> := ApiKey, <<"api_secret">> := ApiSecret}} =
        create_app(Name, #{scopes => [?SCOPE_CONNECTIONS]}),
    %% /clients should succeed (connections scope)
    ?assertMatch({ok, _}, auth_authorize(clients, ApiKey, ApiSecret)),
    ?assertMatch({ok, _}, auth_authorize(client, ApiKey, ApiSecret)),
    %% /alarms should be denied (monitoring scope, not granted)
    ?assertMatch({error, _}, auth_authorize(alarms, ApiKey, ApiSecret)),
    %% /publish should be denied (publish scope, not granted)
    ?assertMatch({error, _}, auth_authorize(publish, ApiKey, ApiSecret)),
    delete_app(Name).

t_authorize_no_scopes(_Config) ->
    Name = <<"SCOPES-TEST-NONE">>,
    {ok, #{<<"api_key">> := ApiKey, <<"api_secret">> := ApiSecret}} =
        create_app(Name),
    %% No scopes = full access to non-denied endpoints
    ?assertMatch({ok, _}, auth_authorize(clients, ApiKey, ApiSecret)),
    ?assertMatch({ok, _}, auth_authorize(alarms, ApiKey, ApiSecret)),
    ?assertMatch({ok, _}, auth_authorize(publish, ApiKey, ApiSecret)),
    delete_app(Name).

t_authorize_empty_scopes(_Config) ->
    Name = <<"SCOPES-TEST-EMPTY">>,
    {ok, #{<<"api_key">> := ApiKey, <<"api_secret">> := ApiSecret}} =
        create_app(Name, #{scopes => []}),
    %% Empty scopes = all mapped endpoints denied
    ?assertMatch({error, _}, auth_authorize(clients, ApiKey, ApiSecret)),
    ?assertMatch({error, _}, auth_authorize(alarms, ApiKey, ApiSecret)),
    delete_app(Name).

-doc """
A handler that maps to `?SCOPE_DENIED' is rejected for every API key,
with or without an explicit scope list. The sentinel is declared by
the SSO public-flow modules (OIDC callback, SAML ACS, SSO MFA setup),
which are not in this test app's dependency graph, so a synthetic
cache injects one. The synthetic cache replaces the shared one for the
duration of the case, so the case must not run in a parallel group.
""".
t_authorize_denied_handler(_Config) ->
    DeniedHandler = #{method => get, module => test_denied_module, function => test_denied},
    with_scope_cache(
        #{{test_denied_module, test_denied} => [?SCOPE_DENIED]},
        fun() ->
            Extra = #{role => ?ROLE_API_SUPERUSER},
            ?assertMatch(
                {error, unauthorized_role},
                emqx_mgmt_auth:check_scopes(Extra, DeniedHandler)
            ),
            %% Even with explicit scopes, denied handlers are still blocked
            ExtraWithScopes = #{
                role => ?ROLE_API_SUPERUSER, scopes => [?SCOPE_CONNECTIONS]
            },
            ?assertMatch(
                {error, unauthorized_role},
                emqx_mgmt_auth:check_scopes(ExtraWithScopes, DeniedHandler)
            )
        end
    ).

t_check_scopes_unmapped_handler(_Config) ->
    Extra = #{role => ?ROLE_API_SUPERUSER, scopes => [?SCOPE_CONNECTIONS]},
    Unmapped = handler_info(no_such_api_module, no_such_function),
    %% Unmapped handler -> allowed (fail-open for unknown handlers)
    ?assertEqual(ok, emqx_mgmt_auth:check_scopes(Extra, Unmapped)),
    %% Mapped handler in wrong scope -> denied
    ?assertMatch(
        {error, unauthorized_role},
        emqx_mgmt_auth:check_scopes(Extra, handler_info(emqx_mgmt_api_alarms, alarms))
    ),
    %% No scopes -> all allowed
    ExtraNoScopes = #{role => ?ROLE_API_SUPERUSER},
    ?assertEqual(
        ok, emqx_mgmt_auth:check_scopes(ExtraNoScopes, handler_info(emqx_mgmt_api_alarms, alarms))
    ),
    ?assertEqual(
        ok,
        emqx_mgmt_auth:check_scopes(ExtraNoScopes, handler_info(emqx_mgmt_api_clients, clients))
    ).

%%--------------------------------------------------------------------
%% API endpoint tests
%%--------------------------------------------------------------------

t_api_list_scopes(_Config) ->
    AuthHeader = emqx_dashboard_SUITE:auth_header_(),
    Path = emqx_mgmt_api_test_util:api_path(["api_key_scopes"]),
    {ok, Res} = emqx_mgmt_api_test_util:request_api(get, Path, AuthHeader),
    Body = emqx_utils_json:decode(Res),
    %% New format: #{scopes => [...]}
    ?assertMatch(#{<<"scopes">> := _}, Body),
    Scopes = maps:get(<<"scopes">>, Body),
    ?assert(is_list(Scopes)),
    ?assertEqual(length(emqx_scope_catalog:scope_catalog()), length(Scopes)),
    %% Each entry has name and desc (no paths)
    lists:foreach(
        fun(Scope) ->
            ?assertMatch(#{<<"name">> := _, <<"desc">> := _}, Scope),
            ?assert(is_binary(maps:get(<<"name">>, Scope))),
            ?assert(is_binary(maps:get(<<"desc">>, Scope))),
            %% No paths field
            ?assertNot(maps:is_key(<<"paths">>, Scope))
        end,
        Scopes
    ).

t_api_create_with_scopes(_Config) ->
    Name = <<"SCOPES-API-CREATE">>,
    {ok, Created} = create_app(Name, #{scopes => [?SCOPE_CONNECTIONS]}),
    ?assertMatch(#{<<"name">> := Name, <<"scopes">> := [?SCOPE_CONNECTIONS]}, Created),
    {ok, ReadBack} = read_app(Name),
    ?assertMatch(#{<<"scopes">> := [?SCOPE_CONNECTIONS]}, ReadBack),
    delete_app(Name).

t_api_update_scopes(_Config) ->
    Name = <<"SCOPES-API-UPDATE">>,
    {ok, Created} = create_app(Name),
    %% POST without `scopes' now materialises the role-default scope list
    %% (administrator -> the common management scopes). The legacy
    %% `<<"unset">>' sentinel is reserved for records that pre-date the
    %% scopes feature and was thus upgraded without a scopes field.
    DefaultAdminScopes = lists:sort(?GENERIC_SCOPES),
    ?assertEqual(
        DefaultAdminScopes,
        lists:sort(maps:get(<<"scopes">>, Created))
    ),
    %% Update with scopes overwrites the materialised default.
    {ok, Updated1} = update_app(Name, #{scopes => [?SCOPE_CONNECTIONS]}),
    ?assertMatch(#{<<"scopes">> := [?SCOPE_CONNECTIONS]}, Updated1),
    %% Update to multiple scopes
    {ok, Updated2} = update_app(Name, #{scopes => [?SCOPE_CONNECTIONS, ?SCOPE_PUBLISH]}),
    ?assertEqual(
        lists:sort([?SCOPE_CONNECTIONS, ?SCOPE_PUBLISH]),
        lists:sort(maps:get(<<"scopes">>, Updated2))
    ),
    %% Update to empty scopes (explicit deny-all)
    {ok, Updated3} = update_app(Name, #{scopes => []}),
    ?assertMatch(#{<<"scopes">> := []}, Updated3),
    delete_app(Name).

%% POST without an explicit `scopes' field materialises the role-default
%% scope list and persists it (it is not merely a response-time
%% projection). This is what distinguishes a freshly-created key
%% from a legacy upgraded one.
t_api_post_materialises_default_scopes(_Config) ->
    Name = <<"SCOPES-API-MATERIALISE">>,
    {ok, Created} = create_app(Name),
    %% Response carries the materialised admin defaults.
    DefaultAdminScopes = lists:sort(?GENERIC_SCOPES),
    ?assertEqual(
        DefaultAdminScopes,
        lists:sort(maps:get(<<"scopes">>, Created))
    ),
    %% Persisted state matches — `scopes' really is in the extra map,
    %% not synthesised at response time. Read raw mnesia row directly.
    [Record] = mnesia:dirty_read(emqx_app, Name),
    %% Record is #emqx_app{name, api_key, api_secret_hash, enable, expired_at,
    %%                    extra, created_at}. The `extra' field is at index 6
    %% (1=record_name, 2=name, ...). We avoid hard-coding the index by using
    %% emqx_mgmt_auth's normalisation helper via lookup.
    {ok, MapForm} = emqx_mgmt_auth:read(Name),
    %% to_map projects materialised scopes verbatim — no sentinel.
    %% MapForm is the internal atom-keyed shape (read/1 returns the raw
    %% to_map/1 projection, before the API layer converts keys to
    %% binaries for the JSON response).
    ?assertEqual(
        DefaultAdminScopes,
        lists:sort(maps:get(scopes, MapForm))
    ),
    %% Defensive: also assert the raw extra map carries `scopes' (not
    %% the absence of the key, which would have been the legacy state).
    Extra = element(6, Record),
    ?assert(is_map(Extra)),
    ?assert(maps:is_key(scopes, Extra)),
    delete_app(Name).

%% Records created before the scopes feature shipped have no `scopes'
%% key in their extra map. Such legacy records still need a sensible
%% response — the contract is to surface the binary sentinel
%% `<<"unset">>'. We simulate this by writing a record directly into
%% mnesia with a stripped extra map.
t_api_legacy_record_shows_unset_sentinel(_Config) ->
    Name = <<"SCOPES-API-LEGACY">>,
    %% First create a normal record via the API so we get a valid record
    %% layout, then mutate its extra to remove the scopes key.
    {ok, _} = create_app(Name),
    [Record0] = mnesia:dirty_read(emqx_app, Name),
    Extra0 = element(6, Record0),
    ExtraNoScopes = maps:remove(scopes, Extra0),
    Record1 = setelement(6, Record0, ExtraNoScopes),
    ok = mnesia:dirty_write(emqx_app, Record1),
    %% Now read it back through the API and verify the sentinel surfaces.
    {ok, ReadBack} = read_app(Name),
    ?assertEqual(<<"unset">>, maps:get(<<"scopes">>, ReadBack)),
    delete_app(Name).

%%--------------------------------------------------------------------
%% Helper functions
%%--------------------------------------------------------------------

%% Authorize an API key against the handler that serves an endpoint,
%% with the HandlerInfo minirest passes to the authorize callback. The
%% scope check keys on `module' and `function'; `path' is what
%% minirest would carry for that route.
auth_authorize(Endpoint, Key, Secret) ->
    {Module, Function, RelPath} = endpoint(Endpoint),
    AbsPath = erlang:list_to_binary(emqx_dashboard_swagger:relative_uri(RelPath)),
    FakeReq = #{method => <<"GET">>, path => AbsPath},
    HandlerInfo = #{method => get, module => Module, function => Function, path => RelPath},
    emqx_mgmt_auth:authorize(HandlerInfo, FakeReq, Key, Secret).

endpoint(clients) -> {emqx_mgmt_api_clients, clients, "/clients"};
endpoint(client) -> {emqx_mgmt_api_clients, client, "/clients/:clientid"};
endpoint(alarms) -> {emqx_mgmt_api_alarms, alarms, "/alarms"};
endpoint(publish) -> {emqx_mgmt_api_publish, publish, "/publish"}.

handler_info(Module, Function) ->
    #{method => get, module => Module, function => Function}.

create_app(Name) ->
    create_app(Name, #{}).

create_app(Name, Extra) ->
    AuthHeader = emqx_dashboard_SUITE:auth_header_(),
    Path = emqx_mgmt_api_test_util:api_path(["api_key"]),
    ExpiredAt = to_rfc3339(erlang:system_time(second) + 1000),
    App = Extra#{
        name => Name,
        expired_at => ExpiredAt,
        desc => <<"Test scopes"/utf8>>,
        enable => true
    },
    case emqx_mgmt_api_test_util:request_api(post, Path, "", AuthHeader, App) of
        {ok, Res} -> {ok, emqx_utils_json:decode(Res)};
        Error -> Error
    end.

read_app(Name) ->
    AuthHeader = emqx_dashboard_SUITE:auth_header_(),
    Path = emqx_mgmt_api_test_util:api_path(["api_key", Name]),
    case emqx_mgmt_api_test_util:request_api(get, Path, AuthHeader) of
        {ok, Res} -> {ok, emqx_utils_json:decode(Res)};
        Error -> Error
    end.

delete_app(Name) ->
    AuthHeader = emqx_dashboard_SUITE:auth_header_(),
    DeletePath = emqx_mgmt_api_test_util:api_path(["api_key", Name]),
    emqx_mgmt_api_test_util:request_api(delete, DeletePath, AuthHeader).

update_app(Name, Change) ->
    AuthHeader = emqx_dashboard_SUITE:auth_header_(),
    UpdatePath = emqx_mgmt_api_test_util:api_path(["api_key", Name]),
    case emqx_mgmt_api_test_util:request_api(put, UpdatePath, "", AuthHeader, Change) of
        {ok, Update} -> {ok, emqx_utils_json:decode(Update)};
        Error -> Error
    end.

to_rfc3339(Sec) ->
    list_to_binary(calendar:system_time_to_rfc3339(Sec)).
