%%--------------------------------------------------------------------
%% Copyright (c) 2023-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_dashboard_rbac_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include("../../emqx_dashboard/include/emqx_dashboard.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("emqx/include/emqx_api_key_scopes.hrl").

-import(emqx_dashboard_api_test_helpers, [uri/1]).

-define(DEFAULT_SUPERUSER, <<"admin_user">>).
-define(DEFAULT_SUPERUSER_PASS, <<"admin_password">>).
-define(ADD_DESCRIPTION, <<>>).

-define(global_superuser, global_superuser).
-define(global_viewer, global_viewer).
-define(namespaced_superuser, namespaced_superuser).
-define(namespaced_viewer, namespaced_viewer).

all() ->
    emqx_common_test_helpers:all_with_matrix(?MODULE).

groups() ->
    emqx_common_test_helpers:groups_with_matrix(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [
            emqx,
            emqx_conf,
            emqx_management,
            emqx_mgmt_api_test_util:emqx_dashboard(),
            emqx_dashboard_rbac
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    Apps = ?config(apps, Config),
    emqx_cth_suite:stop(Apps),
    ok.

end_per_testcase(_, _Config) ->
    All = emqx_dashboard_admin:all_users(),
    [emqx_dashboard_admin:remove_user(Name) || #{username := Name} <- All].

role_of(TCConfig) ->
    Alternatives = [
        ?global_superuser,
        ?global_viewer,
        ?namespaced_superuser,
        ?namespaced_viewer
    ],
    emqx_common_test_helpers:get_matrix_prop(TCConfig, Alternatives, ?global_superuser).

t_create_bad_role(_) ->
    ?assertEqual(
        {error, <<"Role does not exist">>},
        emqx_dashboard_admin:add_user(
            ?DEFAULT_SUPERUSER,
            ?DEFAULT_SUPERUSER_PASS,
            <<"bad_role">>,
            ?ADD_DESCRIPTION
        )
    ).

t_permission(_) ->
    add_default_superuser(),

    ViewerUser = <<"viewer_user">>,
    ViewerPassword = <<"add_password">>,

    %% add by superuser
    {ok, 200, Payload} = emqx_dashboard_api_test_helpers:request(
        ?DEFAULT_SUPERUSER,
        ?DEFAULT_SUPERUSER_PASS,
        post,
        uri([users]),
        #{
            username => ViewerUser,
            password => ViewerPassword,
            role => ?ROLE_VIEWER,
            description => ?ADD_DESCRIPTION
        }
    ),

    ?assertMatch(
        #{
            <<"username">> := ViewerUser,
            <<"role">> := ?ROLE_VIEWER,
            <<"description">> := ?ADD_DESCRIPTION
        },
        emqx_utils_json:decode(Payload)
    ),

    %% add by viewer
    ?assertMatch(
        {ok, 403, _},
        emqx_dashboard_api_test_helpers:request(
            ViewerUser,
            ViewerPassword,
            post,
            uri([users]),
            #{
                username => ViewerUser,
                password => ViewerPassword,
                role => ?ROLE_VIEWER,
                description => ?ADD_DESCRIPTION
            }
        )
    ),

    ok.

t_update_role(_) ->
    add_default_superuser(),

    %% update role by superuser
    {ok, 200, Payload} = emqx_dashboard_api_test_helpers:request(
        ?DEFAULT_SUPERUSER,
        ?DEFAULT_SUPERUSER_PASS,
        put,
        uri([users, ?DEFAULT_SUPERUSER]),
        #{
            role => ?ROLE_VIEWER,
            description => ?ADD_DESCRIPTION
        }
    ),

    ?assertMatch(
        #{
            <<"username">> := ?DEFAULT_SUPERUSER,
            <<"role">> := ?ROLE_VIEWER,
            <<"description">> := ?ADD_DESCRIPTION
        },
        emqx_utils_json:decode(Payload)
    ),

    %% update role by viewer
    ?assertMatch(
        {ok, 403, _},
        emqx_dashboard_api_test_helpers:request(
            ?DEFAULT_SUPERUSER,
            ?DEFAULT_SUPERUSER_PASS,
            put,
            uri([users, ?DEFAULT_SUPERUSER]),
            #{
                role => ?ROLE_SUPERUSER,
                description => ?ADD_DESCRIPTION
            }
        )
    ),
    ok.

t_clean_token(_) ->
    Username = <<"admin_token">>,
    Password = <<"public_www1">>,
    Desc = <<"desc">>,
    NewDesc = <<"new desc">>,
    {ok, _} = emqx_dashboard_admin:add_user(Username, Password, ?ROLE_SUPERUSER, Desc),
    {ok, #{token := Token}} = emqx_dashboard_admin:sign_token(Username, Password),
    FakeReq = #{},
    FakeHandlerInfo = #{method => get, module => any, function => any},
    {ok, #{actor := Username}} = emqx_dashboard_admin:verify_token(FakeReq, FakeHandlerInfo, Token),
    %% change description
    {ok, _} = emqx_dashboard_admin:update_user(Username, ?ROLE_SUPERUSER, NewDesc),
    timer:sleep(5),
    {ok, #{actor := Username}} = emqx_dashboard_admin:verify_token(FakeReq, FakeHandlerInfo, Token),
    %% change role
    {ok, _} = emqx_dashboard_admin:update_user(Username, ?ROLE_VIEWER, NewDesc),
    timer:sleep(5),
    {error, not_found} = emqx_dashboard_admin:verify_token(FakeReq, FakeHandlerInfo, Token),
    ok.

%% Regression for #17122 0c7f370c: when a user's role changes,
%% emqx_dashboard_admin:update_user/3 calls
%% emqx_dashboard_token:destroy_by_username/1 to invalidate older tokens.
%% If destroy_by_username is asynchronous (gen_server cast) and races with a
%% subsequent sign_token, the cast may delete the freshly-issued token after
%% it has been written to mnesia. The fix makes destroy_by_username
%% synchronous; this test asserts that a token signed AFTER the role update
%% survives any in-flight cleanup.
t_role_change_new_token_survives(_) ->
    Username = <<"admin_role_change">>,
    Password = <<"public_www1">>,
    Desc = <<"desc">>,
    {ok, _} = emqx_dashboard_admin:add_user(Username, Password, ?ROLE_SUPERUSER, Desc),
    FakeReq = #{},
    FakeHandlerInfo = #{method => get, module => any, function => any},
    {ok, _} = emqx_dashboard_admin:update_user(Username, ?ROLE_VIEWER, Desc),
    {ok, #{token := Token}} = emqx_dashboard_admin:sign_token(Username, Password),
    %% Drain any pending gen_server messages to flush out any racing async
    %% destroy operations before we verify the new token.
    ok = gen_server:call(emqx_dashboard_token, dummy, infinity),
    {ok, #{actor := Username}} =
        emqx_dashboard_admin:verify_token(FakeReq, FakeHandlerInfo, Token),
    ok.

t_logout() ->
    [{matrix, true}].
t_logout(matrix) ->
    [
        [?global_superuser],
        [?global_viewer],
        [?namespaced_superuser],
        [?namespaced_viewer]
    ];
t_logout(TCConfig) when is_list(TCConfig) ->
    Username = <<"admin_token">>,
    Password = <<"public_www1">>,
    Desc = <<"desc">>,
    Role =
        case role_of(TCConfig) of
            ?global_superuser -> ?ROLE_SUPERUSER;
            ?global_viewer -> ?ROLE_VIEWER;
            ?namespaced_superuser -> <<"ns:ns1::", ?ROLE_SUPERUSER/binary>>;
            ?namespaced_viewer -> <<"ns:ns1::", ?ROLE_VIEWER/binary>>
        end,
    {ok, _} = emqx_dashboard_admin:add_user(Username, Password, Role, Desc),
    {ok, #{token := Token}} = emqx_dashboard_admin:sign_token(Username, Password),
    FakeReq = #{},
    FakeHandlerInfo = #{method => post, function => logout, module => emqx_dashboard_api},
    {ok, #{actor := Username}} = emqx_dashboard_admin:verify_token(FakeReq, FakeHandlerInfo, Token),
    ok.

t_change_pwd(_) ->
    Viewer1 = <<"viewer1">>,
    Viewer2 = <<"viewer2">>,
    SuperUser = <<"super_user">>,
    Password = <<"public_www1">>,
    Desc = <<"desc">>,
    {ok, _} = emqx_dashboard_admin:add_user(Viewer1, Password, ?ROLE_VIEWER, Desc),
    {ok, _} = emqx_dashboard_admin:add_user(Viewer2, Password, ?ROLE_VIEWER, Desc),
    {ok, _} = emqx_dashboard_admin:add_user(SuperUser, Password, ?ROLE_SUPERUSER, Desc),
    {ok, #{role := ?ROLE_VIEWER, token := Viewer1Token}} = emqx_dashboard_admin:sign_token(
        Viewer1, Password
    ),
    {ok, #{role := ?ROLE_SUPERUSER, token := SuperToken}} = emqx_dashboard_admin:sign_token(
        SuperUser, Password
    ),
    %% viewer can change own password
    ?assertMatch({ok, #{actor := Viewer1}}, change_pwd(Viewer1Token, Viewer1)),
    %% viewer can't change other's password
    ?assertEqual({error, unauthorized_role}, change_pwd(Viewer1Token, Viewer2)),
    ?assertEqual({error, unauthorized_role}, change_pwd(Viewer1Token, SuperUser)),
    %% superuser can change other's password
    ?assertMatch({ok, #{actor := SuperUser}}, change_pwd(SuperToken, Viewer1)),
    ?assertMatch({ok, #{actor := SuperUser}}, change_pwd(SuperToken, Viewer2)),
    ?assertMatch({ok, #{actor := SuperUser}}, change_pwd(SuperToken, SuperUser)),
    ok.

change_pwd(Token, Username) ->
    Req = #{bindings => #{username => Username}},
    HandlerInfo = #{method => post, function => change_pwd, module => emqx_dashboard_api},
    emqx_dashboard_admin:verify_token(Req, HandlerInfo, Token).

t_setup_mfa(_) ->
    test_mfa(fun setup_mfa/2).

t_delete_mfa(_) ->
    test_mfa(fun delete_mfa/2).

%% Port from release-510 #17117 874c2f08: a SSO viewer must not be allowed
%% to disable MFA for themselves while their backend has `force_mfa = true`,
%% otherwise they could lock themselves out of the next forced setup.
%% A local user (BACKEND_LOCAL) and a SSO viewer with `force_mfa = false`
%% must still be allowed.
t_delete_mfa_sso_force_mfa(_) ->
    SsoBackend = saml,
    SsoUser = <<"sso_viewermfa">>,
    LocalUser = <<"local_viewermfa">>,
    Password = <<"xyz124abc">>,
    Desc = <<"desc">>,
    SsoConfig = emqx:get_config([dashboard, sso, SsoBackend], #{}),
    {ok, _} = emqx_dashboard_admin:add_sso_user(SsoBackend, SsoUser, ?ROLE_VIEWER, Desc),
    {ok, _} = emqx_dashboard_admin:add_user(LocalUser, Password, ?ROLE_VIEWER, Desc),
    {ok, #{role := ?ROLE_VIEWER, token := SsoToken}} = emqx_dashboard_admin:sign_token(
        ?SSO_USERNAME(SsoBackend, SsoUser), <<>>
    ),
    {ok, #{role := ?ROLE_VIEWER, token := LocalToken}} = emqx_dashboard_admin:sign_token(
        LocalUser, Password
    ),
    try
        ok = emqx_config:put([dashboard, sso, SsoBackend], SsoConfig#{force_mfa => false}),
        ?assertMatch({ok, #{actor := SsoUser}}, delete_mfa(SsoToken, SsoUser)),
        ok = emqx_config:put([dashboard, sso, SsoBackend], SsoConfig#{force_mfa => true}),
        ?assertEqual({error, unauthorized_role}, delete_mfa(SsoToken, SsoUser)),
        ?assertMatch({ok, #{actor := LocalUser}}, delete_mfa(LocalToken, LocalUser))
    after
        ok = emqx_config:put([dashboard, sso, SsoBackend], SsoConfig)
    end,
    ok.

%% Port from release-510 #17122 c4ab6b15: SSO usernames may contain `@`
%% (e.g. email addresses). The HTTP layer URL-encodes them as `%40`.
%% On release-60, cowboy_router automatically URL-decodes path segments
%% before populating `bindings`, so the RBAC binding match against the
%% logged-in actor still works. This is the end-to-end equivalent of the
%% release-510 path-string regression: it exercises cowboy + minirest +
%% RBAC + the MFA handler with `backend=saml` and `force_mfa = false`/`true`.
t_delete_mfa_sso_force_mfa_urlencoded_username_http(_) ->
    %% Full HTTP path: RBAC + emqx_dashboard_api:authorize_mfa_change/3.
    %% Lock state is derived from the per-user `admin_override' field,
    %% so the test pins the contract: override=mfa_required denies
    %% self-DELETE, override=undefined (or mfa_exempted) allows it,
    %% regardless of the backend's live force_mfa flag.
    SsoBackend = saml,
    SsoUser = <<"jackson-http@example.com">>,
    Desc = <<"desc">>,
    SsoUsername = ?SSO_USERNAME(SsoBackend, SsoUser),
    {ok, _} = emqx_dashboard_admin:add_sso_user(SsoBackend, SsoUser, ?ROLE_VIEWER, Desc),
    {ok, #{role := ?ROLE_VIEWER, token := SsoToken}} = emqx_dashboard_admin:sign_token(
        SsoUsername, <<>>
    ),
    %% override=undefined (no admin decision): self-DELETE succeeds.
    {ok, ok} = emqx_dashboard_admin:set_admin_override(SsoUsername, undefined),
    ?assertMatch(
        {ok, 204, _},
        delete_mfa_urlencoded_username_http(SsoToken, SsoBackend, SsoUser)
    ),
    %% override=mfa_required: self-DELETE denied with MFA_LOCKED.
    {ok, ok} = emqx_dashboard_admin:set_admin_override(SsoUsername, ?ADMIN_MFA_REQUIRED),
    ?assertMatch(
        {ok, 403, _},
        delete_mfa_urlencoded_username_http(SsoToken, SsoBackend, SsoUser)
    ),
    ok.

t_delete_mfa_sso_force_mfa(_) ->
    %% Post feat/dashboard-user-scopes: RBAC layer no longer consults the
    %% live SSO backend `force_mfa` flag for self-DELETE on /users/:self/mfa.
    %% Policy state (snapshot + admin_required) is decided in
    %% emqx_dashboard_api:authorize_mfa_change/3. This RBAC-only test
    %% therefore asserts that self-DELETE always passes the RBAC layer;
    %% snapshot-driven enforcement is covered by the full-HTTP test below
    %% and by emqx_dashboard_user_scopes_SUITE.
    SsoBackend = saml,
    SsoUser = <<"sso_viewermfa">>,
    LocalUser = <<"local_viewermfa">>,
    Password = <<"xyz124abc">>,
    Desc = <<"desc">>,
    SsoConfig = emqx:get_config([dashboard, sso, SsoBackend], #{}),
    {ok, _} = emqx_dashboard_admin:add_sso_user(SsoBackend, SsoUser, ?ROLE_VIEWER, Desc),
    {ok, _} = emqx_dashboard_admin:add_user(LocalUser, Password, ?ROLE_VIEWER, Desc),
    {ok, #{role := ?ROLE_VIEWER, token := SsoToken}} = emqx_dashboard_admin:sign_token(
        ?SSO_USERNAME(SsoBackend, SsoUser), <<>>
    ),
    {ok, #{role := ?ROLE_VIEWER, token := LocalToken}} = emqx_dashboard_admin:sign_token(
        LocalUser, Password
    ),
    try
        %% RBAC is policy-independent for self-DELETE: passes regardless
        %% of the backend's current force_mfa value.
        ok = emqx_config:put([dashboard, sso, SsoBackend], SsoConfig#{force_mfa => false}),
        ?assertEqual({ok, SsoUser}, delete_mfa(SsoToken, SsoUser)),
        ok = emqx_config:put([dashboard, sso, SsoBackend], SsoConfig#{force_mfa => true}),
        ?assertEqual({ok, SsoUser}, delete_mfa(SsoToken, SsoUser)),
        ?assertEqual({ok, LocalUser}, delete_mfa(LocalToken, LocalUser))
    after
        ok = emqx_config:put([dashboard, sso, SsoBackend], SsoConfig)
    end,
    ok.


test_mfa(VerifyFn) ->
    Viewer1 = <<"viewermfa1">>,
    Viewer2 = <<"viewermfa2">>,
    SuperUser = <<"adminmfa">>,
    NamespacedSuperUser = <<"nsadminmfa">>,
    Password = <<"xyz124abc">>,
    Desc = <<"desc">>,
    {ok, _} = emqx_dashboard_admin:add_user(Viewer1, Password, ?ROLE_VIEWER, Desc),
    {ok, _} = emqx_dashboard_admin:add_user(Viewer2, Password, ?ROLE_VIEWER, Desc),
    {ok, _} = emqx_dashboard_admin:add_user(SuperUser, Password, ?ROLE_SUPERUSER, Desc),
    {ok, _} = emqx_dashboard_admin:add_user(
        NamespacedSuperUser,
        Password,
        <<"ns:ns1::", ?ROLE_SUPERUSER/binary>>,
        Desc
    ),
    {ok, #{role := ?ROLE_VIEWER, token := Viewer1Token}} = emqx_dashboard_admin:sign_token(
        Viewer1, Password
    ),
    {ok, #{role := ?ROLE_SUPERUSER, token := SuperToken}} = emqx_dashboard_admin:sign_token(
        SuperUser, Password
    ),
    {ok, #{role := ?ROLE_SUPERUSER, token := NamespacedSuperToken}} =
        emqx_dashboard_admin:sign_token(NamespacedSuperUser, Password),
    %% viewer can change own MFA
    ?assertMatch({ok, #{actor := Viewer1}}, VerifyFn(Viewer1Token, Viewer1)),
    %% viewer can't change other's MFA
    ?assertEqual({error, unauthorized_role}, VerifyFn(Viewer1Token, Viewer2)),
    ?assertEqual({error, unauthorized_role}, VerifyFn(Viewer1Token, SuperUser)),
    %% superuser can change other's MFA
    ?assertMatch({ok, #{actor := SuperUser}}, VerifyFn(SuperToken, Viewer1)),
    ?assertMatch({ok, #{actor := SuperUser}}, VerifyFn(SuperToken, Viewer2)),
    ?assertMatch({ok, #{actor := SuperUser}}, VerifyFn(SuperToken, SuperUser)),
    %% namespaced superuser can change own MFA, but not other dashboard users' MFA
    ?assertMatch(
        {ok, #{actor := NamespacedSuperUser}},
        VerifyFn(NamespacedSuperToken, NamespacedSuperUser)
    ),
    ?assertEqual({error, unauthorized_role}, VerifyFn(NamespacedSuperToken, Viewer1)),
    ok.

%%--------------------------------------------------------------------
%% check_login_user_scopes/2 — scope-deny main path coverage.
%% See SPEC-dashboard-user-scopes.md sec 7.8.1, 7.11.
%%
%% Tests live here (and not in emqx_dashboard_user_scopes_SUITE) because
%% emqx_dashboard does not depend on emqx_dashboard_rbac, so the predicate
%% is not loadable from that SUITE.
%%--------------------------------------------------------------------

%% scopes=undefined uses the role-default fallback (admin -> all 14,
%% viewer -> 10 generic). A viewer with no explicit scopes therefore
%% holds only generic scopes and is denied on /users (user_management
%% scope). Self-targeted user endpoints are an exception (see
%% t_check_login_user_scopes_self_user_endpoints_bypass below).
t_check_login_user_scopes_undefined_falls_back(_) ->
    Username = <<"login_user_scopes_undef">>,
    {ok, _} = emqx_dashboard_admin:add_user(
        Username, <<"P@ssw0rd">>, ?ROLE_VIEWER, <<>>
    ),
    %% No set_user_scopes call.
    ?assertEqual(undefined, emqx_dashboard_admin:scopes_of(Username)),
    %% Viewer default = 10 generic scopes (no user_management).
    ?assertEqual(
        false,
        emqx_dashboard_rbac:check_login_user_scopes(Username, <<"/users">>)
    ),
    %% /clients is mapped to connections — viewer default holds it.
    ?assertEqual(
        true,
        emqx_dashboard_rbac:check_login_user_scopes(Username, <<"/clients">>)
    ).

%% Self-targeted user endpoints (own change_pwd, own MFA) bypass the
%% scope check — they are gated by RBAC's self-check and, for MFA, by
%% emqx_dashboard_api:authorize_mfa_change/3.
t_check_login_user_scopes_self_user_endpoints_bypass(_) ->
    Username = <<"login_user_scopes_self">>,
    {ok, _} = emqx_dashboard_admin:add_user(
        Username, <<"P@ssw0rd">>, ?ROLE_VIEWER, <<>>
    ),
    %% Viewer default does NOT contain user_management/mfa_management,
    %% but self-targeted paths are still allowed.
    ?assertEqual(
        true,
        emqx_dashboard_rbac:check_login_user_scopes(
            Username, <<"/users/", Username/binary, "/change_pwd">>
        )
    ),
    ?assertEqual(
        true,
        emqx_dashboard_rbac:check_login_user_scopes(
            Username, <<"/users/", Username/binary, "/mfa">>
        )
    ),
    %% Other users' endpoints still respect scope rules.
    ?assertEqual(
        false,
        emqx_dashboard_rbac:check_login_user_scopes(
            Username, <<"/users/somebody_else/mfa">>
        )
    ).

%% scopes=[] denies every mapped path (semantically: "explicitly no
%% permissions"). Distinct from undefined.
t_check_login_user_scopes_explicit_empty_denies(_) ->
    Username = <<"login_user_scopes_empty">>,
    {ok, _} = emqx_dashboard_admin:add_user(
        Username, <<"P@ssw0rd">>, ?ROLE_SUPERUSER, <<>>
    ),
    {ok, ok} = emqx_dashboard_admin:set_user_scopes(Username, []),
    ?assertEqual([], emqx_dashboard_admin:scopes_of(Username)),
    %% A path mapped to a known scope is denied.
    ?assertEqual(
        false,
        emqx_dashboard_rbac:check_login_user_scopes(Username, <<"/users">>)
    ),
    %% Unmapped paths still fail-open (consistent with API key semantics
    %% in emqx_mgmt_auth:check_path_in_scopes/2).
    ?assertEqual(
        true,
        emqx_dashboard_rbac:check_login_user_scopes(
            Username, <<"/some/unmapped/path">>
        )
    ).

%% scopes=[user_management] grants /users access; a path mapped to
%% a different scope on ANOTHER user is denied. Self mfa path is
%% bypassed and stays allowed (see self-bypass test).
t_check_login_user_scopes_user_mgmt_grants_users(_) ->
    Username = <<"login_user_scopes_um">>,
    {ok, _} = emqx_dashboard_admin:add_user(
        Username, <<"P@ssw0rd">>, ?ROLE_SUPERUSER, <<>>
    ),
    {ok, ok} = emqx_dashboard_admin:set_user_scopes(
        Username, [?SCOPE_USER_MGMT]
    ),
    ?assertEqual(
        true,
        emqx_dashboard_rbac:check_login_user_scopes(Username, <<"/users">>)
    ),
    %% Another user's mfa endpoint — denied (scope mfa_management not held).
    ?assertEqual(
        false,
        emqx_dashboard_rbac:check_login_user_scopes(
            Username, <<"/users/somebody_else/mfa">>
        )
    ).

%% scopes=[mfa_management] grants MFA paths but not /users.
t_check_login_user_scopes_mfa_mgmt_grants_only_mfa(_) ->
    Username = <<"login_user_scopes_mm">>,
    {ok, _} = emqx_dashboard_admin:add_user(
        Username, <<"P@ssw0rd">>, ?ROLE_SUPERUSER, <<>>
    ),
    {ok, ok} = emqx_dashboard_admin:set_user_scopes(
        Username, [?SCOPE_MFA_MGMT]
    ),
    ?assertEqual(
        true,
        emqx_dashboard_rbac:check_login_user_scopes(
            Username, <<"/users/", Username/binary, "/mfa">>
        )
    ),
    ?assertEqual(
        false,
        emqx_dashboard_rbac:check_login_user_scopes(Username, <<"/users">>)
    ).

%%--------------------------------------------------------------------
%% Login user holding generic (API-key catalogue) scopes
%%
%% SPEC sec 3.1 / 7.11: login users may hold any of the 10 generic
%% catalogue scopes alongside the 4 login-only scopes. The scope
%% predicate is uniform — there is no role-based or scope-class-based
%% branching in check_login_user_scopes/2 — so a viewer or
%% administrator carrying scopes=[<<"connections">>] should be allowed
%% on /clients and denied on every endpoint mapped to a different
%% scope. These tests guard that uniformity.
%%--------------------------------------------------------------------

%% scopes=[connections] grants paths mapped to the connections scope.
t_check_login_user_scopes_connections_grants_clients(_) ->
    Username = <<"login_user_scopes_conn_grant">>,
    {ok, _} = emqx_dashboard_admin:add_user(
        Username, <<"P@ssw0rd">>, ?ROLE_VIEWER, <<>>
    ),
    {ok, ok} = emqx_dashboard_admin:set_user_scopes(
        Username, [?SCOPE_CONNECTIONS]
    ),
    %% Direct top-level resource.
    ?assertEqual(
        true,
        emqx_dashboard_rbac:check_login_user_scopes(Username, <<"/clients">>)
    ),
    %% Concrete clientid concretizes the /clients/:clientid template
    %% via match_template/2; should still resolve to connections.
    ?assertEqual(
        true,
        emqx_dashboard_rbac:check_login_user_scopes(
            Username, <<"/clients/test-client-id">>
        )
    ).

%% scopes=[connections] denies paths mapped to other scopes.
t_check_login_user_scopes_connections_denies_other_scopes(_) ->
    Username = <<"login_user_scopes_conn_deny">>,
    {ok, _} = emqx_dashboard_admin:add_user(
        Username, <<"P@ssw0rd">>, ?ROLE_VIEWER, <<>>
    ),
    {ok, ok} = emqx_dashboard_admin:set_user_scopes(
        Username, [?SCOPE_CONNECTIONS]
    ),
    %% /alarms is mapped to monitoring, not connections.
    ?assertEqual(
        false,
        emqx_dashboard_rbac:check_login_user_scopes(Username, <<"/alarms">>)
    ),
    %% /users is mapped to user_management — generic scope cannot
    %% reach a login-only-mapped path.
    ?assertEqual(
        false,
        emqx_dashboard_rbac:check_login_user_scopes(Username, <<"/users">>)
    ).

%% scopes=[monitoring] grants /alarms but denies /clients.
t_check_login_user_scopes_monitoring_grants_alarms(_) ->
    Username = <<"login_user_scopes_mon">>,
    {ok, _} = emqx_dashboard_admin:add_user(
        Username, <<"P@ssw0rd">>, ?ROLE_VIEWER, <<>>
    ),
    {ok, ok} = emqx_dashboard_admin:set_user_scopes(
        Username, [?SCOPE_MONITORING]
    ),
    ?assertEqual(
        true,
        emqx_dashboard_rbac:check_login_user_scopes(Username, <<"/alarms">>)
    ),
    ?assertEqual(
        false,
        emqx_dashboard_rbac:check_login_user_scopes(Username, <<"/clients">>)
    ).

%% A generic scope on a login user must NOT grant access to login-only
%% paths (/users/:username/mfa requires mfa_management).
t_check_login_user_scopes_generic_does_not_grant_login_only_paths(_) ->
    Username = <<"login_user_scopes_xover">>,
    {ok, _} = emqx_dashboard_admin:add_user(
        Username, <<"P@ssw0rd">>, ?ROLE_SUPERUSER, <<>>
    ),
    {ok, ok} = emqx_dashboard_admin:set_user_scopes(
        Username, [?SCOPE_CONNECTIONS]
    ),
    %% NOTE: /sso/* paths are not asserted here because
    %% emqx_dashboard_sso is not in this SUITE's app start list, so
    %% those paths do not appear in the path_to_scope cache and would
    %% fall through to the unmapped fail-open branch (true). The cross-
    %% module assertion that /sso is mapped to sso_management lives in
    %% emqx_dashboard_sso/test/emqx_dashboard_sso_mfa_SUITE.erl.
    ?assertEqual(
        false,
        emqx_dashboard_rbac:check_login_user_scopes(Username, <<"/users">>)
    ),
    %% Another user's mfa path — generic scope does not grant it.
    ?assertEqual(
        false,
        emqx_dashboard_rbac:check_login_user_scopes(
            Username, <<"/users/somebody_else/mfa">>
        )
    ).

%% scopes=[] denies every mapped path including generic ones.
t_check_login_user_scopes_explicit_empty_denies_generic_paths(_) ->
    Username = <<"login_user_scopes_empty_gen">>,
    {ok, _} = emqx_dashboard_admin:add_user(
        Username, <<"P@ssw0rd">>, ?ROLE_SUPERUSER, <<>>
    ),
    {ok, ok} = emqx_dashboard_admin:set_user_scopes(Username, []),
    ?assertEqual(
        false,
        emqx_dashboard_rbac:check_login_user_scopes(Username, <<"/clients">>)
    ),
    ?assertEqual(
        false,
        emqx_dashboard_rbac:check_login_user_scopes(Username, <<"/alarms">>)
    ).

%% scopes=undefined falls back to RBAC default (allow), including for
%% generic-mapped paths. This is the lazy-migration path.
t_check_login_user_scopes_undefined_allows_generic_paths(_) ->
    Username = <<"login_user_scopes_undef_gen">>,
    {ok, _} = emqx_dashboard_admin:add_user(
        Username, <<"P@ssw0rd">>, ?ROLE_SUPERUSER, <<>>
    ),
    %% No set_user_scopes call.
    ?assertEqual(undefined, emqx_dashboard_admin:scopes_of(Username)),
    ?assertEqual(
        true,
        emqx_dashboard_rbac:check_login_user_scopes(Username, <<"/clients">>)
    ),
    ?assertEqual(
        true,
        emqx_dashboard_rbac:check_login_user_scopes(Username, <<"/alarms">>)
    ).

%%--------------------------------------------------------------------
%% SSO users: explicit scopes are consulted via the reconstructed
%% ?SSO_USERNAME(Backend, Name) admin record key.
%%
%% Regression for the bug where emqx_dashboard_token:check_rbac/2 was
%% passing the bare JWT username binary into the scope check, so SSO
%% users with explicit scopes always fell through to the
%% undefined -> allow fallback.
%%--------------------------------------------------------------------

t_check_login_user_scopes_sso_explicit_empty_denies(_) ->
    Backend = saml,
    Name = <<"sso_scope_test">>,
    {ok, _} = emqx_dashboard_admin:add_sso_user(Backend, Name, ?ROLE_VIEWER, <<>>),
    SsoKey = ?SSO_USERNAME(Backend, Name),
    {ok, ok} = emqx_dashboard_admin:set_user_scopes(SsoKey, []),
    %% Bare binary lookup misses — that path returns undefined and
    %% would falsely allow. The tuple key resolves correctly.
    ?assertEqual(undefined, emqx_dashboard_admin:scopes_of(Name)),
    ?assertEqual([], emqx_dashboard_admin:scopes_of(SsoKey)),
    %% Predicate fed the proper key: explicit empty scopes deny.
    ?assertEqual(
        false,
        emqx_dashboard_rbac:check_login_user_scopes(SsoKey, <<"/clients">>)
    ),
    ?assertEqual(
        false,
        emqx_dashboard_rbac:check_login_user_scopes(SsoKey, <<"/users">>)
    ).

t_check_login_user_scopes_sso_explicit_scope_grants_only_that_path(_) ->
    Backend = saml,
    Name = <<"sso_scope_conn">>,
    {ok, _} = emqx_dashboard_admin:add_sso_user(Backend, Name, ?ROLE_VIEWER, <<>>),
    SsoKey = ?SSO_USERNAME(Backend, Name),
    {ok, ok} = emqx_dashboard_admin:set_user_scopes(SsoKey, [?SCOPE_CONNECTIONS]),
    ?assertEqual(
        true,
        emqx_dashboard_rbac:check_login_user_scopes(SsoKey, <<"/clients">>)
    ),
    ?assertEqual(
        false,
        emqx_dashboard_rbac:check_login_user_scopes(SsoKey, <<"/alarms">>)
    ).

delete_mfa(Token, Username) ->
    Req = #{bindings => #{username => Username}},
    HandlerInfo = #{method => delete, module => emqx_dashboard_api, function => change_mfa},
    emqx_dashboard_admin:verify_token(Req, HandlerInfo, Token).

delete_mfa_urlencoded_username_http(Token, Backend, Username) ->
    Url = emqx_mgmt_api_test_util:api_path([
        "users", uri_string:quote(binary_to_list(Username)), "mfa"
    ]),
    emqx_mgmt_api_test_util:request_api(
        delete,
        Url,
        [{backend, atom_to_binary(Backend)}],
        [bearer_auth_header(Token)],
        [],
        #{compatible_mode => true}
    ).

bearer_auth_header(Token) ->
    {"Authorization", "Bearer " ++ binary_to_list(Token)}.

setup_mfa(Token, Username) ->
    Req = #{bindings => #{username => Username}},
    HandlerInfo = #{method => post, module => emqx_dashboard_api, function => change_mfa},
    emqx_dashboard_admin:verify_token(Req, HandlerInfo, Token).

add_default_superuser() ->
    {ok, _NewUser} = emqx_dashboard_admin:add_user(
        ?DEFAULT_SUPERUSER,
        ?DEFAULT_SUPERUSER_PASS,
        ?ROLE_SUPERUSER,
        ?ADD_DESCRIPTION
    ).
