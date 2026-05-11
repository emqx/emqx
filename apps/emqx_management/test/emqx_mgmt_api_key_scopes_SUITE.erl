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
            t_path_to_scope,
            t_path_to_scope_denied,
            t_path_to_scope_no_cache,
            t_validate_scopes,
            t_validate_scopes_bad_input,
            t_is_denied_scope,
            t_all_modules_have_scopes,
            t_all_endpoints_covered_by_scopes
        ]},
        {integration_tests, [parallel], [
            t_authorize_with_scopes,
            t_authorize_no_scopes,
            t_authorize_empty_scopes,
            t_authorize_denied_path,
            t_check_scopes_unmapped_path
        ]},
        {api_tests, [parallel], [
            t_api_list_scopes,
            t_api_create_with_scopes,
            t_api_update_scopes
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
    ?assertMatch(#{path_to_scope := _}, Cache),
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

t_path_to_scope(_Config) ->
    emqx_mgmt_api_key_scopes:init_cache(),
    %% "/clients" should resolve to <<"connections">>
    ?assertEqual(?SCOPE_CONNECTIONS, emqx_mgmt_api_key_scopes:path_to_scope(<<"/clients">>)),
    %% "/clients/:clientid" should also be connections
    ?assertEqual(
        ?SCOPE_CONNECTIONS,
        emqx_mgmt_api_key_scopes:path_to_scope(<<"/clients/:clientid">>)
    ),
    %% "/publish" should resolve to <<"publish">>
    ?assertEqual(?SCOPE_PUBLISH, emqx_mgmt_api_key_scopes:path_to_scope(<<"/publish">>)),
    %% Unknown path → undefined
    ?assertEqual(undefined, emqx_mgmt_api_key_scopes:path_to_scope(<<"/nonexistent">>)),
    emqx_mgmt_api_key_scopes:clear_cache().

t_path_to_scope_denied(_Config) ->
    emqx_mgmt_api_key_scopes:init_cache(),
    %% Dashboard / API-key management paths formerly resolved to
    %% $denied. They now map to login-only scopes
    %% (api_key_management, user_management, mfa_management,
    %% sso_management). API keys still cannot reach these endpoints
    %% — minirest's bearer-only `security` declaration
    %% rejects API key authentication before scope check.
    ?assertEqual(?SCOPE_USER_MGMT, emqx_mgmt_api_key_scopes:path_to_scope(<<"/users">>)),
    ?assertEqual(?SCOPE_API_KEY_MGMT, emqx_mgmt_api_key_scopes:path_to_scope(<<"/api_key">>)),
    emqx_mgmt_api_key_scopes:clear_cache().

t_path_to_scope_no_cache(_Config) ->
    emqx_mgmt_api_key_scopes:clear_cache(),
    %% Should lazy-init and return correct scope
    ?assertEqual(?SCOPE_CONNECTIONS, emqx_mgmt_api_key_scopes:path_to_scope(<<"/clients">>)).

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
    PathToScope = emqx_mgmt_api_key_scopes:collect_scopes_from_modules(),
    ?assert(map_size(PathToScope) > 0),
    %% Every path should map to a known scope, $denied, or one of the
    %% four login-only scopes (user/mfa/sso/api_key_management — these
    %% apply to dashboard login users only and are not in the API key
    %% scope catalog).
    AllValidScopes =
        [N || #{name := N} <- emqx_scope_catalog:scope_catalog()] ++
            [?SCOPE_DENIED] ++
            ?LOGIN_ONLY_SCOPES,
    maps:foreach(
        fun(Path, Scope) ->
            ?assert(
                lists:member(Scope, AllValidScopes),
                lists:flatten(
                    io_lib:format("Path ~s mapped to unknown scope ~s", [Path, Scope])
                )
            )
        end,
        PathToScope
    ),
    emqx_mgmt_api_key_scopes:clear_cache().

t_all_endpoints_covered_by_scopes(_Config) ->
    emqx_mgmt_api_key_scopes:init_cache(),
    PathToScope = emqx_mgmt_api_key_scopes:collect_scopes_from_modules(),
    Modules = emqx_mgmt_api_key_scopes:find_api_modules(),
    AllDeclaredPaths = lists:usort(
        lists:flatmap(
            fun(M) ->
                try
                    [path_to_binary(P) || P <- apply(M, paths, [])]
                catch
                    _:_ -> []
                end
            end,
            Modules
        )
    ),
    %% Some paths are intentionally unmapped (fail-open) — public auth
    %% entry points and scope catalog endpoints. They are guarded by
    %% minirest `security` declarations rather than by the scope map.
    IntentionallyUnmapped = [
        %% Authentication / session entry points
        <<"/login">>,
        <<"/logout">>,
        %% Public SSO login flow endpoints
        <<"/sso/login/:backend">>,
        <<"/sso/token_exchange">>,
        <<"/sso/oidc/callback">>,
        <<"/sso/saml/acs">>,
        <<"/sso/saml/metadata">>,
        %% Probed by the dashboard login page (pre-auth) to render
        %% the "Log in with X" SSO button list.
        <<"/sso/running">>,
        %% Public SSO MFA setup/verify (token-authenticated by short-lived JWT)
        <<"/sso/mfa/setup_info">>,
        <<"/sso/mfa/setup">>,
        <<"/sso/mfa/verify">>,
        %% Scope catalog endpoints — public to any authenticated login user.
        %% Top-level paths chosen to avoid wildcard routing collision with
        %% /api_key/:name and /users/:username (sibling to /action_types,
        %% /source_types).
        <<"/api_key_scopes">>,
        <<"/user_scopes">>
    ],
    MappedPaths = lists:sort(maps:keys(PathToScope)),
    Uncovered = (AllDeclaredPaths -- MappedPaths) -- IntentionallyUnmapped,
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

t_authorize_with_scopes(_Config) ->
    Name = <<"SCOPES-TEST-WITH">>,
    {ok, #{<<"api_key">> := ApiKey, <<"api_secret">> := ApiSecret}} =
        create_app(Name, #{scopes => [?SCOPE_CONNECTIONS]}),
    %% /clients should succeed (connections scope)
    ?assertMatch({ok, _}, auth_authorize(<<"/clients">>, ApiKey, ApiSecret)),
    ?assertMatch({ok, _}, auth_authorize(<<"/clients/:clientid">>, ApiKey, ApiSecret)),
    %% /alarms should be denied (monitoring scope, not granted)
    ?assertMatch({error, _}, auth_authorize(<<"/alarms">>, ApiKey, ApiSecret)),
    %% /publish should be denied (publish scope, not granted)
    ?assertMatch({error, _}, auth_authorize(<<"/publish">>, ApiKey, ApiSecret)),
    delete_app(Name).

t_authorize_no_scopes(_Config) ->
    Name = <<"SCOPES-TEST-NONE">>,
    {ok, #{<<"api_key">> := ApiKey, <<"api_secret">> := ApiSecret}} =
        create_app(Name),
    %% No scopes = full access to non-denied paths
    ?assertMatch({ok, _}, auth_authorize(<<"/clients">>, ApiKey, ApiSecret)),
    ?assertMatch({ok, _}, auth_authorize(<<"/alarms">>, ApiKey, ApiSecret)),
    ?assertMatch({ok, _}, auth_authorize(<<"/publish">>, ApiKey, ApiSecret)),
    delete_app(Name).

t_authorize_empty_scopes(_Config) ->
    Name = <<"SCOPES-TEST-EMPTY">>,
    {ok, #{<<"api_key">> := ApiKey, <<"api_secret">> := ApiSecret}} =
        create_app(Name, #{scopes => []}),
    %% Empty scopes = all mapped paths denied
    ?assertMatch({error, _}, auth_authorize(<<"/clients">>, ApiKey, ApiSecret)),
    ?assertMatch({error, _}, auth_authorize(<<"/alarms">>, ApiKey, ApiSecret)),
    delete_app(Name).

t_authorize_denied_path(_Config) ->
    Name = <<"SCOPES-TEST-DENIED">>,
    {ok, #{<<"api_key">> := _ApiKey, <<"api_secret">> := _ApiSecret}} =
        create_app(Name),
    %% Dashboard / SSO / API-key-management paths no longer use
    %% ?SCOPE_DENIED — they map to login-only scopes.
    %% ?SCOPE_DENIED stays as an internal sentinel for SSO public flow
    %% modules (OIDC callback / SAML ACS / SSO MFA setup) which are not
    %% loaded in this test app's dep graph. We mock the scope cache to
    %% inject a denied entry and verify that emqx_mgmt_auth still
    %% rejects API key access to such paths.
    DeniedPath = <<"/__test_denied__">>,
    meck:new(emqx_mgmt_api_key_scopes, [passthrough]),
    meck:expect(emqx_mgmt_api_key_scopes, path_to_scope, fun
        (P) when P =:= DeniedPath -> ?SCOPE_DENIED;
        (P) -> meck:passthrough([P])
    end),
    try
        Extra = #{role => ?ROLE_API_SUPERUSER},
        ?assertMatch(
            {error, unauthorized_role},
            emqx_mgmt_auth:check_scopes(Extra, DeniedPath, <<"GET">>)
        ),
        %% Even with explicit scopes, denied paths are still blocked
        ExtraWithScopes = #{
            role => ?ROLE_API_SUPERUSER, scopes => [?SCOPE_CONNECTIONS]
        },
        ?assertMatch(
            {error, unauthorized_role},
            emqx_mgmt_auth:check_scopes(ExtraWithScopes, DeniedPath, <<"GET">>)
        )
    after
        meck:unload(emqx_mgmt_api_key_scopes)
    end,
    delete_app(Name).

t_check_scopes_unmapped_path(_Config) ->
    Extra = #{role => ?ROLE_API_SUPERUSER, scopes => [?SCOPE_CONNECTIONS]},
    %% Unmapped path → allowed (fail-open for unknown paths)
    ?assertEqual(ok, emqx_mgmt_auth:check_scopes(Extra, <<"/nonexistent/path">>, <<"GET">>)),
    %% Mapped path in wrong scope → denied
    ?assertMatch(
        {error, unauthorized_role},
        emqx_mgmt_auth:check_scopes(Extra, <<"/alarms">>, <<"GET">>)
    ),
    %% No scopes → all allowed
    ExtraNoScopes = #{role => ?ROLE_API_SUPERUSER},
    ?assertEqual(ok, emqx_mgmt_auth:check_scopes(ExtraNoScopes, <<"/alarms">>, <<"GET">>)),
    ?assertEqual(ok, emqx_mgmt_auth:check_scopes(ExtraNoScopes, <<"/clients">>, <<"GET">>)).

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
    ?assertEqual(false, maps:is_key(<<"scopes">>, Created)),
    %% Update with scopes
    {ok, Updated1} = update_app(Name, #{scopes => [?SCOPE_CONNECTIONS]}),
    ?assertMatch(#{<<"scopes">> := [?SCOPE_CONNECTIONS]}, Updated1),
    %% Update to multiple scopes
    {ok, Updated2} = update_app(Name, #{scopes => [?SCOPE_CONNECTIONS, ?SCOPE_PUBLISH]}),
    ?assertEqual(
        lists:sort([?SCOPE_CONNECTIONS, ?SCOPE_PUBLISH]),
        lists:sort(maps:get(<<"scopes">>, Updated2))
    ),
    %% Update to empty scopes
    {ok, Updated3} = update_app(Name, #{scopes => []}),
    ?assertMatch(#{<<"scopes">> := []}, Updated3),
    delete_app(Name).

%%--------------------------------------------------------------------
%% Helper functions
%%--------------------------------------------------------------------

auth_authorize(RelPath, Key, Secret) ->
    AbsPath = erlang:list_to_binary(
        emqx_dashboard_swagger:relative_uri(binary_to_list(RelPath))
    ),
    FakeReq = #{method => <<"GET">>, path => AbsPath},
    HandlerInfo = #{
        method => get,
        module => dummy_module,
        function => dummy_func,
        path => binary_to_list(RelPath)
    },
    emqx_mgmt_auth:authorize(HandlerInfo, FakeReq, Key, Secret).

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
