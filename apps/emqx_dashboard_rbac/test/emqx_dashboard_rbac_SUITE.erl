%%--------------------------------------------------------------------
%% Copyright (c) 2023-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_dashboard_rbac_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include("../../emqx_dashboard/include/emqx_dashboard.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("emqx_utils/include/emqx_api_key_scopes.hrl").

-import(emqx_dashboard_api_test_helpers, [request/4, uri/1]).

-define(DEFAULT_SUPERUSER, <<"admin_user">>).
-define(DEFAULT_SUPERUSER_PASS, <<"admin_password">>).
-define(ADD_DESCRIPTION, <<>>).
-define(REDACTED, <<"******">>).
-define(SENTINEL, <<"sec431-sentinel">>).
-define(VIEWER_USER, <<"viewer_user_for_configs">>).
-define(VIEWER_PASS, <<"viewer_pass_for_configs">>).
%% Raw-socket requests must bypass `httpc' URL normalization (see below).
-define(DASHBOARD_HOST, "127.0.0.1").
-define(DASHBOARD_PORT, 18083).
-define(TIMEOUT, 5000).

all() ->
    emqx_common_test_helpers:all(?MODULE).

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
    FakePath = erlang:list_to_binary(emqx_dashboard_swagger:relative_uri("/fake")),
    FakeReq = #{method => <<"GET">>, path => FakePath},
    {ok, Username} = emqx_dashboard_admin:verify_token(FakeReq, Token),
    %% change description
    {ok, _} = emqx_dashboard_admin:update_user(Username, ?ROLE_SUPERUSER, NewDesc),
    timer:sleep(5),
    {ok, Username} = emqx_dashboard_admin:verify_token(FakeReq, Token),
    %% change role
    {ok, _} = emqx_dashboard_admin:update_user(Username, ?ROLE_VIEWER, NewDesc),
    timer:sleep(5),
    {error, not_found} = emqx_dashboard_admin:verify_token(FakeReq, Token),
    ok.

t_role_change_new_token_survives(_) ->
    Username = <<"admin_role_change">>,
    Password = <<"public_www1">>,
    Desc = <<"desc">>,
    {ok, _} = emqx_dashboard_admin:add_user(Username, Password, ?ROLE_SUPERUSER, Desc),
    FakePath = erlang:list_to_binary(emqx_dashboard_swagger:relative_uri("/fake")),
    FakeReq = #{method => <<"GET">>, path => FakePath},
    {ok, _} = emqx_dashboard_admin:update_user(Username, ?ROLE_VIEWER, Desc),
    {ok, #{token := Token}} = emqx_dashboard_admin:sign_token(Username, Password),
    ok = gen_server:call(emqx_dashboard_token, dummy, infinity),
    {ok, Username} = emqx_dashboard_admin:verify_token(FakeReq, Token),
    ok.

t_login_out(_) ->
    Username = <<"admin_token">>,
    Password = <<"public_www1">>,
    Desc = <<"desc">>,
    {ok, _} = emqx_dashboard_admin:add_user(Username, Password, ?ROLE_SUPERUSER, Desc),
    {ok, #{token := Token}} = emqx_dashboard_admin:sign_token(Username, Password),
    FakePath = erlang:list_to_binary(emqx_dashboard_swagger:relative_uri("/logout")),
    FakeReq = #{method => <<"POST">>, path => FakePath},
    {ok, Username} = emqx_dashboard_admin:verify_token(FakeReq, Token),
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
    ?assertEqual({ok, Viewer1}, change_pwd(Viewer1Token, Viewer1)),
    %% viewer can't change other's password
    ?assertEqual({error, {unauthorized_role, Viewer1}}, change_pwd(Viewer1Token, Viewer2)),
    ?assertEqual({error, {unauthorized_role, Viewer1}}, change_pwd(Viewer1Token, SuperUser)),
    %% superuser can change other's password
    ?assertEqual({ok, SuperUser}, change_pwd(SuperToken, Viewer1)),
    ?assertEqual({ok, SuperUser}, change_pwd(SuperToken, Viewer2)),
    ?assertEqual({ok, SuperUser}, change_pwd(SuperToken, SuperUser)),
    ok.

change_pwd(Token, Username) ->
    Path = "/users/" ++ binary_to_list(Username) ++ "/change_pwd",
    Path1 = erlang:list_to_binary(emqx_dashboard_swagger:relative_uri(Path)),
    Req = #{method => <<"POST">>, path => Path1},
    emqx_dashboard_admin:verify_token(Req, Token).

t_setup_mfa(_) ->
    test_mfa(fun setup_mfa/2).

t_delete_mfa(_) ->
    test_mfa(fun delete_mfa/2).

t_delete_mfa_sso_force_mfa_urlencoded_username(_) ->
    SsoBackend = saml,
    SsoUser = <<"jackson@example.com">>,
    Desc = <<"desc">>,
    SsoConfig = emqx:get_config([dashboard, sso, SsoBackend], #{}),
    {ok, _} = emqx_dashboard_admin:add_sso_user(SsoBackend, SsoUser, ?ROLE_VIEWER, Desc),
    {ok, #{role := ?ROLE_VIEWER, token := SsoToken}} = emqx_dashboard_admin:sign_token(
        ?SSO_USERNAME(SsoBackend, SsoUser), <<>>
    ),
    try
        ok = emqx_config:put([dashboard, sso, SsoBackend], SsoConfig#{force_mfa => false}),
        ?assertEqual({ok, SsoUser}, delete_mfa_urlencoded_username(SsoToken, SsoUser))
    after
        ok = emqx_config:put([dashboard, sso, SsoBackend], SsoConfig)
    end,
    ok.

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
    %% RBAC layer no longer consults the live SSO backend `force_mfa'
    %% flag for self-DELETE on /users/:self/mfa. Policy state (the
    %% admin_override decision) is decided in
    %% emqx_dashboard_api:authorize_mfa_change/3. This RBAC-only test
    %% therefore asserts that self-DELETE always passes the RBAC
    %% layer; admin_override enforcement is covered by the full-HTTP
    %% test below and by emqx_dashboard_user_scopes_SUITE.
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
    Password = <<"xyz124abc">>,
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
    ?assertEqual({ok, Viewer1}, VerifyFn(Viewer1Token, Viewer1)),
    %% viewer can't change other's password
    ?assertEqual({error, {unauthorized_role, Viewer1}}, VerifyFn(Viewer1Token, Viewer2)),
    ?assertEqual({error, {unauthorized_role, Viewer1}}, VerifyFn(Viewer1Token, SuperUser)),
    %% superuser can change other's password
    ?assertEqual({ok, SuperUser}, VerifyFn(SuperToken, Viewer1)),
    ?assertEqual({ok, SuperUser}, VerifyFn(SuperToken, Viewer2)),
    ?assertEqual({ok, SuperUser}, VerifyFn(SuperToken, SuperUser)),
    ok.

%%--------------------------------------------------------------------
%% check_login_user_scopes/2 — scope-deny main path coverage.
%%
%% Tests live here (and not in emqx_dashboard_user_scopes_SUITE) because
%% emqx_dashboard does not depend on emqx_dashboard_rbac, so the predicate
%% is not loadable from that SUITE.
%%--------------------------------------------------------------------

%% scopes=undefined uses the role-default fallback (admin -> common + login-only,
%% viewer -> common scopes only). A viewer with no explicit scopes therefore
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
    %% Viewer default = common scopes only (no user_management).
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

%% Self-bypass is restricted to change_pwd and mfa. PUT /users/<self>
%% (modifying one's own record) MUST still be subject to the scope
%% check — otherwise a user with explicit `scopes = []' could PUT
%% itself to add scopes back, defeating the self-restriction.
t_check_login_user_scopes_self_user_record_not_bypassed(_) ->
    Username = <<"login_user_scopes_self_put">>,
    {ok, _} = emqx_dashboard_admin:add_user(
        Username, <<"P@ssw0rd">>, ?ROLE_SUPERUSER, <<>>
    ),
    {ok, ok} = emqx_dashboard_admin:set_user_scopes(Username, []),
    %% Self change_pwd / mfa still allowed.
    ?assertEqual(
        true,
        emqx_dashboard_rbac:check_login_user_scopes(
            Username, <<"/users/", Username/binary, "/change_pwd">>
        )
    ),
    %% But PUT /users/<self> (the record itself) is NOT bypassed —
    %% the user record path maps to user_management which the user
    %% explicitly does not hold.
    ?assertEqual(
        false,
        emqx_dashboard_rbac:check_login_user_scopes(
            Username, <<"/users/", Username/binary>>
        )
    ),
    %% Likewise GET /users (list) is not bypassed.
    ?assertEqual(
        false,
        emqx_dashboard_rbac:check_login_user_scopes(Username, <<"/users">>)
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
    %% Unmapped paths fail closed for scope-restricted login users (here
    %% an explicit []): a path that maps to no known scope is denied.
    ?assertEqual(
        false,
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
%% Login user holding generic (API-key catalog) scopes
%%
%% Login users may hold any common catalog scope alongside the
%% login-only scopes. The scope predicate is uniform — there is
%% no role-based or scope-class-based branching in
%% check_login_user_scopes/2 — so a viewer or administrator carrying
%% scopes=[<<"connections">>] should be allowed on /clients and denied
%% on every endpoint mapped to a different scope. These tests guard
%% that uniformity.
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

%% A user holding only non-privilege scopes cannot reach the user
%% endpoints at all, not even its own record. `PUT /users/<self>' is the
%% one write that could widen the caller's own scope list, so the scope
%% check must gate it; only the self change_pwd / mfa endpoints are
%% exempt (see t_check_login_user_scopes_self_user_endpoints_bypass).
t_check_login_user_scopes_monitoring_cannot_reach_own_user_record(_) ->
    Username = <<"login_user_scopes_mon_self">>,
    {ok, _} = emqx_dashboard_admin:add_user(
        Username, <<"P@ssw0rd">>, ?ROLE_VIEWER, <<>>
    ),
    {ok, ok} = emqx_dashboard_admin:set_user_scopes(
        Username, [?SCOPE_MONITORING]
    ),
    ?assertEqual(
        false,
        emqx_dashboard_rbac:check_login_user_scopes(Username, <<"/users">>)
    ),
    ?assertEqual(
        false,
        emqx_dashboard_rbac:check_login_user_scopes(
            Username, <<"/users/", Username/binary>>
        )
    ),
    %% The self-service endpoints stay reachable.
    ?assertEqual(
        true,
        emqx_dashboard_rbac:check_login_user_scopes(
            Username, <<"/users/", Username/binary, "/change_pwd">>
        )
    ).

%% The counterpart: user_management does reach the caller's own record,
%% so a holder can rewrite its own scope list. That is by design —
%% user_management is a privilege scope and its holder can already
%% provision another user with any scopes.
t_check_login_user_scopes_user_mgmt_reaches_own_user_record(_) ->
    Username = <<"login_user_scopes_um_self">>,
    {ok, _} = emqx_dashboard_admin:add_user(
        Username, <<"P@ssw0rd">>, ?ROLE_SUPERUSER, <<>>
    ),
    {ok, ok} = emqx_dashboard_admin:set_user_scopes(
        Username, [?SCOPE_USER_MGMT]
    ),
    ?assertEqual(
        true,
        emqx_dashboard_rbac:check_login_user_scopes(
            Username, <<"/users/", Username/binary>>
        )
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
    %% fall through to the unmapped fail-closed branch (false). The cross-
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
    Path = "/users/" ++ binary_to_list(Username) ++ "/mfa",
    Path1 = erlang:list_to_binary(emqx_dashboard_swagger:relative_uri(Path)),
    Req = #{method => <<"DELETE">>, path => Path1},
    emqx_dashboard_admin:verify_token(Req, Token).

delete_mfa_urlencoded_username(Token, Username) ->
    Path = "/users/" ++ uri_string:quote(binary_to_list(Username)) ++ "/mfa",
    Path1 = erlang:list_to_binary(emqx_dashboard_swagger:relative_uri(Path)),
    Req = #{method => <<"DELETE">>, path => Path1},
    emqx_dashboard_admin:verify_token(Req, Token).

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
    Path = "/users/" ++ binary_to_list(Username) ++ "/mfa",
    Path1 = erlang:list_to_binary(emqx_dashboard_swagger:relative_uri(Path)),
    Req = #{method => <<"POST">>, path => Path1},
    emqx_dashboard_admin:verify_token(Req, Token).

%% `GET /api/v5/configs' with `Accept: text/plain' dumps the whole configuration as
%% cleartext HOCON, so it is restricted to administrators. Viewers keep the redacted
%% JSON variant of the same endpoint. See PLANS.md.
t_configs_plaintext_permission(_) ->
    add_default_superuser(),
    {ok, _} = put_sentinel(),
    Admin = admin_auth_header(),
    Viewer = viewer_auth_header(),
    ViewerKey = create_api_key(<<"sec431-viewer-key">>, ?ROLE_API_VIEWER),
    AdminKey = create_api_key(<<"sec431-admin-key">>, ?ROLE_API_SUPERUSER),
    try
        %% A viewer (dashboard token) must not be able to read the cleartext dump,
        %% in any of the request shapes that negotiate to `text/plain'. Every denial
        %% must use the existing RBAC `UNAUTHORIZED_ROLE' code.
        assert_configs_denied(Viewer, <<"text/plain">>, no_key),
        assert_configs_denied(Viewer, no_accept, no_key),
        assert_configs_denied(Viewer, <<"*/*">>, no_key),
        assert_configs_denied(Viewer, <<"application/json, */*;q=0.8">>, no_key),
        %% `?key=' is served by the same cleartext dump, so it is denied as well.
        assert_configs_denied(Viewer, <<"text/plain">>, {key, <<"sysmon">>}),
        %% The rule is evaluated on the node that receives the request, before any
        %% RPC, so `?node=' is covered by the same path check.
        assert_configs_denied(Viewer, <<"text/plain">>, {node, atom_to_list(node())}),
        %% Path aliases must not bypass the rule: `cowboy_router' routes on the
        %% percent-decoded, dot-segment-collapsed path, so these all reach the
        %% `configs' handler. `httpc' normalizes URLs before sending them
        %% (`uri_string:normalize/2'), which would hide the aliases, so the raw
        %% request bytes are sent instead.
        ?assertEqual(<<"403">>, raw_configs_status(Viewer, "/api/v5/%63onfigs")),
        ?assertEqual(<<"403">>, raw_configs_status(Viewer, "/api/v5/conf%69gs")),
        ?assertEqual(<<"403">>, raw_configs_status(Viewer, "/api/v5/./configs")),
        ?assertEqual(<<"403">>, raw_configs_status(Viewer, "/api/v5/x/../configs")),
        %% `cowboy_router' also drops the trailing empty segment, so these reach
        %% the same handler too.
        ?assertEqual(<<"403">>, raw_configs_status(Viewer, "/api/v5/configs/")),
        ?assertEqual(<<"403">>, raw_configs_status(Viewer, "/api/v5/configs/.")),
        ?assertEqual(<<"403">>, raw_configs_status(Viewer, "/api/v5/configs/%2E")),
        %% Non-vacuity: those aliases really do reach the `configs' handler, so an
        %% administrator gets the cleartext dump through them.
        {<<"200">>, AdminAliasBody} = raw_configs_response(Admin, "/api/v5/%63onfigs"),
        ?assertNotEqual(nomatch, binary:match(AdminAliasBody, ?SENTINEL)),
        {<<"200">>, AdminTrailingBody} = raw_configs_response(Admin, "/api/v5/configs/"),
        ?assertNotEqual(nomatch, binary:match(AdminTrailingBody, ?SENTINEL)),
        %% The redacted JSON path is not affected.
        {{200, JsonBody}, "application/json"} = configs_get(
            Viewer, <<"application/json">>, no_key
        ),
        #{<<"sysmon">> := #{<<"top">> := #{<<"db_password">> := ?REDACTED}}} =
            emqx_utils_json:decode(JsonBody, [return_maps]),
        ?assertEqual(nomatch, binary:match(JsonBody, ?SENTINEL)),

        %% A viewer API key goes through the same RBAC rules.
        assert_configs_denied(ViewerKey, <<"text/plain">>, no_key),
        assert_configs_denied(ViewerKey, no_accept, no_key),
        assert_configs_denied(ViewerKey, <<"*/*">>, no_key),
        ?assertEqual(<<"403">>, raw_configs_status(ViewerKey, "/api/v5/conf%69gs")),
        ?assertEqual(<<"403">>, raw_configs_status(ViewerKey, "/api/v5/configs/")),
        {{200, ViewerKeyJson}, "application/json"} = configs_get(
            ViewerKey, <<"application/json">>, no_key
        ),
        ?assertEqual(nomatch, binary:match(ViewerKeyJson, ?SENTINEL)),

        %% Administrators keep the cleartext dump: it is the export half of the
        %% `GET /configs' -> `PUT /configs' (export/reload) workflow.
        {{200, PlainBody}, "text/plain"} = configs_get(Admin, <<"text/plain">>, no_key),
        ?assertNotEqual(nomatch, binary:match(PlainBody, ?SENTINEL)),
        {{200, NoAcceptBody}, "text/plain"} = configs_get(Admin, no_accept, no_key),
        ?assertNotEqual(nomatch, binary:match(NoAcceptBody, ?SENTINEL)),
        {{200, StarBody}, "text/plain"} = configs_get(Admin, <<"*/*">>, no_key),
        ?assertNotEqual(nomatch, binary:match(StarBody, ?SENTINEL)),
        {{200, MixedBody}, "text/plain"} = configs_get(
            Admin, <<"application/json, */*;q=0.8">>, no_key
        ),
        ?assertNotEqual(nomatch, binary:match(MixedBody, ?SENTINEL)),
        {{200, KeyBody}, "text/plain"} = configs_get(
            Admin, <<"text/plain">>, {key, <<"sysmon">>}
        ),
        ?assertNotEqual(nomatch, binary:match(KeyBody, ?SENTINEL)),
        %% ... and the JSON variant stays redacted for them too.
        {{200, AdminJson}, "application/json"} = configs_get(
            Admin, <<"application/json">>, no_key
        ),
        #{<<"sysmon">> := #{<<"top">> := #{<<"db_password">> := ?REDACTED}}} =
            emqx_utils_json:decode(AdminJson, [return_maps]),
        ?assertEqual(nomatch, binary:match(AdminJson, ?SENTINEL)),

        %% An administrator API key can still export the cleartext HOCON.
        {{200, AdminKeyBody}, "text/plain"} = configs_get(AdminKey, <<"text/plain">>, no_key),
        ?assertNotEqual(nomatch, binary:match(AdminKeyBody, ?SENTINEL)),
        ok
    after
        emqx_mgmt_auth:delete(<<"sec431-viewer-key">>),
        emqx_mgmt_auth:delete(<<"sec431-admin-key">>),
        emqx_conf:remove([sysmon, top, db_password], #{override_to => cluster})
    end.

configs_get(Auth, Accept, Query) ->
    configs_get_uri(
        emqx_mgmt_api_test_util:api_path(["configs" ++ query_string(Query)]), Auth, Accept
    ).

configs_get_uri(URI, Auth, Accept) ->
    Headers = accept_header(Accept) ++ [Auth],
    Opts = #{return_all => true, httpc_req_opts => [{body_format, binary}]},
    case emqx_mgmt_api_test_util:request_api(get, URI, [], Headers, [], Opts) of
        {ok, {{_, Code, _}, RespHeaders, Body}} ->
            {{Code, Body}, proplists:get_value("content-type", RespHeaders)};
        {error, {{_, Code, _}, RespHeaders, Body}} ->
            {{Code, Body}, proplists:get_value("content-type", RespHeaders)}
    end.

query_string(no_key) -> "";
query_string({key, Key}) -> "?key=" ++ binary_to_list(Key);
query_string({node, Node}) -> "?node=" ++ Node.

%% Asserts the denial status *and* the error code promised by the RBAC layer.
assert_configs_denied(Auth, Accept, Query) ->
    {{403, Body}, ContentType} = configs_get(Auth, Accept, Query),
    #{<<"code">> := <<"UNAUTHORIZED_ROLE">>} = emqx_utils_json:decode(Body, [return_maps]),
    {{403, Body}, ContentType}.

%% Sends a raw request line, bypassing the client-side URL normalization that
%% `httpc' would apply, and returns the HTTP status code.
raw_configs_status(Auth, RawPath) ->
    {Code, _Resp} = raw_configs_response(Auth, RawPath),
    Code.

raw_configs_response({_AuthName, AuthValue}, RawPath) ->
    {ok, Socket} = gen_tcp:connect(
        ?DASHBOARD_HOST, ?DASHBOARD_PORT, [binary, {active, false}, {packet, raw}], ?TIMEOUT
    ),
    try
        ok = gen_tcp:send(Socket, [
            "GET ",
            RawPath,
            " HTTP/1.1\r\n",
            "Host: ",
            ?DASHBOARD_HOST,
            ":",
            integer_to_list(?DASHBOARD_PORT),
            "\r\n",
            "Authorization: ",
            AuthValue,
            "\r\n",
            "Accept: text/plain\r\n",
            "Connection: close\r\n\r\n"
        ]),
        Resp = recv_raw(Socket, <<>>),
        [StatusLine | _] = binary:split(Resp, <<"\r\n">>),
        <<"HTTP/1.1 ", Code:3/binary, _/binary>> = StatusLine,
        {Code, Resp}
    after
        gen_tcp:close(Socket)
    end.

recv_raw(Socket, Acc) ->
    case gen_tcp:recv(Socket, 0, ?TIMEOUT) of
        {ok, Data} -> recv_raw(Socket, <<Acc/binary, Data/binary>>);
        {error, closed} -> Acc;
        {error, Reason} -> ct:fail({recv_failed, Reason, Acc})
    end.

accept_header(no_accept) -> [];
accept_header(Accept) -> [{"accept", Accept}].

admin_auth_header() ->
    {ok, #{token := Token}} = emqx_dashboard_admin:sign_token(
        ?DEFAULT_SUPERUSER, ?DEFAULT_SUPERUSER_PASS
    ),
    {"Authorization", "Bearer " ++ binary_to_list(Token)}.

viewer_auth_header() ->
    {ok, _} = emqx_dashboard_admin:add_user(
        ?VIEWER_USER, ?VIEWER_PASS, ?ROLE_VIEWER, ?ADD_DESCRIPTION
    ),
    {ok, #{token := Token}} = emqx_dashboard_admin:sign_token(?VIEWER_USER, ?VIEWER_PASS),
    {"Authorization", "Bearer " ++ binary_to_list(Token)}.

create_api_key(Name, Role) ->
    ApiKey = <<Name/binary, "-key">>,
    ApiSecret = <<Name/binary, "-secret">>,
    ExpiredAt = erlang:system_time(second) + 3600,
    {ok, _} = emqx_mgmt_auth:create(
        Name, ApiKey, ApiSecret, true, ExpiredAt, <<"configs rbac test">>, Role
    ),
    emqx_common_test_http:auth_header(binary_to_list(ApiKey), binary_to_list(ApiSecret)).

%% A sentinel in a `sensitive => true' field, used to tell cleartext from redacted.
put_sentinel() ->
    emqx_conf:update([sysmon, top, db_password], ?SENTINEL, #{
        rawconf_with_defaults => true, override_to => cluster
    }).

add_default_superuser() ->
    {ok, _NewUser} = emqx_dashboard_admin:add_user(
        ?DEFAULT_SUPERUSER,
        ?DEFAULT_SUPERUSER_PASS,
        ?ROLE_SUPERUSER,
        ?ADD_DESCRIPTION
    ).
