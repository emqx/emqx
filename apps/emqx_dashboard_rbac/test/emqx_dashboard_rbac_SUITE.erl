%%--------------------------------------------------------------------
%% Copyright (c) 2023-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_dashboard_rbac_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include("../../emqx_dashboard/include/emqx_dashboard.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("emqx/include/emqx_config.hrl").
-include_lib("emqx_utils/include/emqx_api_key_scopes.hrl").

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

t_reserved_namespace_role(_) ->
    %% Reserved namespace names cannot be used as the namespace tag of a role.
    lists:foreach(
        fun(Ns) ->
            Role = <<"ns:", Ns/binary, "::", ?ROLE_SUPERUSER/binary>>,
            ?assertEqual(
                {error, <<"Denied namespace">>},
                emqx_dashboard_rbac:parse_dashboard_role(Role),
                #{ns => Ns}
            ),
            ?assertEqual(
                {error, <<"Denied namespace">>},
                emqx_dashboard_rbac:parse_api_role(Role),
                #{ns => Ns}
            )
        end,
        [<<"global">>, <<"undefined">>, <<"null">>, <<"none">>]
    ),
    %% Regression guard: a non-reserved namespace still parses.
    ?assertMatch(
        {ok, #{?namespace := <<"tenant-a">>, ?role := ?ROLE_SUPERUSER}},
        emqx_dashboard_rbac:parse_dashboard_role(<<"ns:tenant-a::", ?ROLE_SUPERUSER/binary>>)
    ),
    ok.

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
    FakeReq = #{path => <<"/api/v5/users">>},
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
    FakeReq = #{path => <<"/api/v5/clients">>},
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
    FakeReq = #{path => <<"/api/v5/logout">>},
    FakeHandlerInfo = #{method => post, function => logout, module => emqx_dashboard_api},
    {ok, #{actor := Username}} = emqx_dashboard_admin:verify_token(FakeReq, FakeHandlerInfo, Token),
    ok.

%% Self-service moved to `/current_user/*'. RBAC's rule for those routes
%% is "any authenticated dashboard user", with no `:username' binding to
%% compare the actor against — so every role passes, and a viewer with an
%% explicitly emptied scope list still passes (the scope layer treats the
%% paths as public). Replaces the old viewer-self / namespaced-self
%% clauses, which are deleted along with `is_self_service_endpoint/2'.
t_current_user_allowed_for_every_role(_) ->
    Password = <<"public_www1">>,
    Desc = <<"desc">>,
    Users = [
        {<<"cu_viewer">>, ?ROLE_VIEWER},
        {<<"cu_admin">>, ?ROLE_SUPERUSER},
        {<<"cu_ns_admin">>, <<"ns:ns1::", ?ROLE_SUPERUSER/binary>>},
        {<<"cu_ns_viewer">>, <<"ns:ns1::", ?ROLE_VIEWER/binary>>}
    ],
    lists:foreach(
        fun({Username, Role}) ->
            {ok, _} = emqx_dashboard_admin:add_user(Username, Password, Role, Desc),
            %% Deliberately restricted to "no permissions" — self-service
            %% must not be gated by the scope list.
            {ok, ok} = emqx_dashboard_admin:set_user_scopes(Username, []),
            {ok, #{token := Token}} = emqx_dashboard_admin:sign_token(Username, Password),
            lists:foreach(
                fun({Method, Fn, Path}) ->
                    ?assertMatch(
                        {ok, #{actor := Username}},
                        current_user_req(Token, Method, Fn, Path),
                        #{username => Username, role => Role, function => Fn}
                    )
                end,
                [
                    {get, current_user, <<"/api/v5/current_user">>},
                    {post, current_user_change_pwd, <<"/api/v5/current_user/change_pwd">>},
                    {post, current_user_mfa, <<"/api/v5/current_user/mfa">>},
                    {delete, current_user_mfa, <<"/api/v5/current_user/mfa">>}
                ]
            )
        end,
        Users
    ),
    ok.

current_user_req(Token, Method, Fn, Path) ->
    %% No `bindings' at all — these routes carry no path parameter.
    Req = #{bindings => #{}, path => Path},
    HandlerInfo = #{method => Method, function => Fn, module => emqx_dashboard_api},
    emqx_dashboard_admin:verify_token(Req, HandlerInfo, Token).

t_setup_mfa(_) ->
    test_mfa(fun setup_mfa/2).

t_delete_mfa(_) ->
    test_mfa(fun delete_mfa/2).

%% Descendant of the #17122 regression (SSO usernames may contain `@`,
%% which the HTTP layer percent-encodes as `%40`, and RBAC used to match
%% the decoded path segment against the logged-in actor).
%%
%% `/current_user/mfa' carries no username segment at all, so there is
%% nothing to encode, decode or compare -- the class of bug is gone
%% rather than fixed. What this test still pins is the part that
%% survives: the self-MFA lock is driven by the per-user
%% `admin_override' field, not by the SSO backend's live `force_mfa'
%% flag, and it applies to an SSO identity whose name needs escaping.
t_delete_own_mfa_sso_admin_override_http(_) ->
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
    ?assertMatch({ok, 204, _}, delete_own_mfa_http(SsoToken)),
    %% override=mfa_required: self-DELETE denied with MFA_LOCKED.
    {ok, ok} = emqx_dashboard_admin:set_admin_override(SsoUsername, ?ADMIN_MFA_REQUIRED),
    ?assertMatch({ok, 403, _}, delete_own_mfa_http(SsoToken)),
    ok.

t_delete_own_mfa_sso_force_mfa(_) ->
    %% RBAC does not consult the SSO backend's live `force_mfa' flag for
    %% self-MFA: `/current_user/mfa' is allowed for any authenticated
    %% user and the lock decision belongs to the handler
    %% (`authorize_self_mfa/2', driven by `admin_override'). Assert RBAC
    %% stays policy-independent across both values of the flag.
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
    Delete = fun(Token) ->
        current_user_req(Token, delete, current_user_mfa, <<"/api/v5/current_user/mfa">>)
    end,
    try
        ok = emqx_config:put([dashboard, sso, SsoBackend], SsoConfig#{force_mfa => false}),
        ?assertMatch({ok, #{actor := SsoUser}}, Delete(SsoToken)),
        ok = emqx_config:put([dashboard, sso, SsoBackend], SsoConfig#{force_mfa => true}),
        ?assertMatch({ok, #{actor := SsoUser}}, Delete(SsoToken)),
        ?assertMatch({ok, #{actor := LocalUser}}, Delete(LocalToken))
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
    %% `/users/:username/mfa' is now purely administrative: it manages
    %% ANOTHER user, and the self case lives at `/current_user/mfa'.
    %% A viewer is denied on every target, its own account included --
    %% there is no longer a viewer-self clause to fall into.
    ?assertMatch({error, {unauthorized_role, _}}, VerifyFn(Viewer1Token, Viewer1)),
    ?assertMatch({error, {unauthorized_role, _}}, VerifyFn(Viewer1Token, Viewer2)),
    ?assertMatch({error, {unauthorized_role, _}}, VerifyFn(Viewer1Token, SuperUser)),
    %% A global administrator reaches every target. (The handler then
    %% refuses the self target and points at /current_user/mfa; that is a
    %% handler decision, not an RBAC one, and is asserted over HTTP in
    %% emqx_dashboard_current_user_SUITE.)
    ?assertMatch({ok, #{actor := SuperUser}}, VerifyFn(SuperToken, Viewer1)),
    ?assertMatch({ok, #{actor := SuperUser}}, VerifyFn(SuperToken, Viewer2)),
    ?assertMatch({ok, #{actor := SuperUser}}, VerifyFn(SuperToken, SuperUser)),
    %% A namespaced administrator is denied on every target, its own
    %% account included. Resetting a tenant user's MFA is the vector this
    %% keeps closed; the namespaced admin's own MFA is at /current_user/mfa.
    ?assertMatch(
        {error, {unauthorized_role, _}},
        VerifyFn(NamespacedSuperToken, NamespacedSuperUser)
    ),
    ?assertMatch({error, {unauthorized_role, _}}, VerifyFn(NamespacedSuperToken, Viewer1)),
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
%% scope). Self-service lives on /current_user/* and is unscoped (see
%% t_check_login_user_scopes_current_user_is_public below).
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

%% Self-service is unscoped because `/current_user/*' is declared
%% ?SCOPE_PUBLIC, not because of a path-parsing exception -- the old
%% `is_self_service_endpoint/2' whitelist is deleted. A user with an
%% explicit empty scope list still reaches its own account.
t_check_login_user_scopes_current_user_is_public(_) ->
    Username = <<"login_user_scopes_self">>,
    {ok, _} = emqx_dashboard_admin:add_user(
        Username, <<"P@ssw0rd">>, ?ROLE_VIEWER, <<>>
    ),
    {ok, ok} = emqx_dashboard_admin:set_user_scopes(Username, []),
    lists:foreach(
        fun(Path) ->
            ?assertEqual(
                true,
                emqx_dashboard_rbac:check_login_user_scopes(Username, Path),
                #{path => Path}
            )
        end,
        [
            <<"/current_user">>,
            <<"/current_user/change_pwd">>,
            <<"/current_user/mfa">>,
            %% The deprecated shim is unscoped for the same reason. It is
            %% a templated ?SCOPE_PUBLIC entry, so it only classifies as
            %% public through segment matching, not exact lookup.
            <<"/users/", Username/binary, "/change_pwd">>,
            <<"/users/somebody_else/change_pwd">>
        ]
    ),
    %% Nothing under /users/ is exempt any more, not even a path that
    %% names the caller: those routes manage OTHER users now.
    lists:foreach(
        fun(Path) ->
            ?assertEqual(
                false,
                emqx_dashboard_rbac:check_login_user_scopes(Username, Path),
                #{path => Path}
            )
        end,
        [
            <<"/users/", Username/binary, "/mfa">>,
            <<"/users/somebody_else/mfa">>,
            <<"/users/", Username/binary>>,
            <<"/users">>
        ]
    ).

%% A user literally named `current_user' does not gain the self-service
%% exemption for its own record: `/users/current_user' is still scope-
%% checked. Guards against a prefix/segment confusion between the
%% top-level `/current_user' route and a username of the same text.
t_check_login_user_scopes_username_named_current_user(_) ->
    Username = <<"current_user">>,
    {ok, _} = emqx_dashboard_admin:add_user(
        Username, <<"P@ssw0rd">>, ?ROLE_SUPERUSER, <<>>
    ),
    {ok, ok} = emqx_dashboard_admin:set_user_scopes(Username, []),
    ?assertEqual(
        true,
        emqx_dashboard_rbac:check_login_user_scopes(Username, <<"/current_user">>)
    ),
    ?assertEqual(
        false,
        emqx_dashboard_rbac:check_login_user_scopes(Username, <<"/users/current_user">>)
    ),
    ?assertEqual(
        false,
        emqx_dashboard_rbac:check_login_user_scopes(Username, <<"/users/current_user/mfa">>)
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

%% scopes=[mfa_management] grants the other-user MFA path but not /users.
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
            Username, <<"/users/somebody_else/mfa">>
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

t_global_only_message_endpoints_reject_namespaced_actors(_) ->
    Req = #{},
    lists:foreach(
        fun(HandlerInfo) ->
            lists:foreach(
                fun(ActorContext) ->
                    ?assertMatch(
                        {error, _},
                        emqx_dashboard_rbac:check_rbac(Req, HandlerInfo, ActorContext)
                    )
                end,
                namespaced_actor_contexts()
            ),
            ?assertMatch(
                {ok, _},
                emqx_dashboard_rbac:check_rbac(Req, HandlerInfo, global_admin_actor_context())
            ),
            assert_global_viewer_rbac(Req, HandlerInfo)
        end,
        global_only_message_endpoint_handlers()
    ).

-doc """
File Transfer file listing and download endpoints expose client-uploaded file
content from the global FT store, so they must reject all namespaced actors
(login users and API keys, any role) while remaining available to global
principals.  The FT config endpoint (`'/file_transfer'`) is not restricted.
""".
t_file_transfer_endpoints_reject_namespaced_actors(_) ->
    Req = #{},
    Expected = {error, <<"File Transfer endpoints are not available to namespaced users">>},
    lists:foreach(
        fun(HandlerInfo) ->
            lists:foreach(
                fun(ActorContext) ->
                    ?assertEqual(
                        Expected,
                        emqx_dashboard_rbac:check_rbac(Req, HandlerInfo, ActorContext)
                    )
                end,
                namespaced_actor_contexts()
            ),
            ?assertMatch(
                {ok, _},
                emqx_dashboard_rbac:check_rbac(Req, HandlerInfo, global_admin_actor_context())
            ),
            assert_global_viewer_rbac(Req, HandlerInfo)
        end,
        file_transfer_content_endpoint_handlers()
    ),
    %% Sanity: the FT config endpoint stays readable for namespaced actors.
    ConfigHandlerInfo = #{method => get, module => emqx_ft_api, function => '/file_transfer'},
    lists:foreach(
        fun(ActorContext) ->
            ?assertMatch(
                {ok, _},
                emqx_dashboard_rbac:check_rbac(Req, ConfigHandlerInfo, ActorContext)
            )
        end,
        namespaced_actor_contexts() ++
            [global_admin_actor_context(), global_viewer_actor_context()]
    ).

t_tracing_config_update_rejects_namespaced_actors(_) ->
    Req = #{},
    HandlerInfo = #{method => put, module => emqx_mgmt_api_trace, function => config},
    Expected = {error, <<"Namespaced users may not update global tracing configuration">>},
    lists:foreach(
        fun(ActorContext) ->
            ?assertEqual(
                Expected,
                emqx_dashboard_rbac:check_rbac(Req, HandlerInfo, ActorContext)
            )
        end,
        namespaced_actor_contexts()
    ),
    ?assertMatch(
        {ok, _},
        emqx_dashboard_rbac:check_rbac(Req, HandlerInfo, global_admin_actor_context())
    ),
    ?assertMatch(
        {error, _},
        emqx_dashboard_rbac:check_rbac(Req, HandlerInfo, global_viewer_actor_context())
    ).

global_only_message_endpoint_handlers() ->
    [
        #{method => get, module => emqx_mgmt_api_clients, function => mqueue_msgs},
        #{method => get, module => emqx_mgmt_api_clients, function => inflight_msgs},
        #{method => get, module => emqx_retainer_api, function => '/messages'},
        #{method => delete, module => emqx_retainer_api, function => '/messages'},
        #{method => get, module => emqx_retainer_api, function => with_topic_warp},
        #{method => delete, module => emqx_retainer_api, function => with_topic_warp},
        #{method => get, module => emqx_delayed_api, function => delayed_messages},
        #{method => get, module => emqx_delayed_api, function => delayed_message},
        #{method => delete, module => emqx_delayed_api, function => delayed_message},
        #{method => delete, module => emqx_delayed_api, function => delayed_message_topic}
    ].

file_transfer_content_endpoint_handlers() ->
    [
        #{method => get, module => emqx_ft_api, function => '/file_transfer/files'},
        #{
            method => get,
            module => emqx_ft_api,
            function => '/file_transfer/files/:clientid/:fileid'
        },
        #{
            method => get,
            module => emqx_ft_storage_exporter_fs_api,
            function => '/file_transfer/file'
        }
    ].

namespaced_actor_contexts() ->
    [
        #{?actor => <<"ns_admin">>, ?role => ?ROLE_SUPERUSER, ?namespace => <<"ns1">>},
        #{?actor => <<"ns_viewer">>, ?role => ?ROLE_VIEWER, ?namespace => <<"ns1">>},
        #{?actor => <<"ns_api_admin">>, ?role => ?ROLE_API_SUPERUSER, ?namespace => <<"ns1">>},
        #{?actor => <<"ns_api_viewer">>, ?role => ?ROLE_API_VIEWER, ?namespace => <<"ns1">>}
    ].

global_admin_actor_context() ->
    #{?actor => <<"global_admin">>, ?role => ?ROLE_SUPERUSER, ?namespace => ?global_ns}.

global_viewer_actor_context() ->
    #{?actor => <<"global_viewer">>, ?role => ?ROLE_VIEWER, ?namespace => ?global_ns}.

assert_global_viewer_rbac(Req, #{method := get} = HandlerInfo) ->
    ?assertMatch(
        {ok, _},
        emqx_dashboard_rbac:check_rbac(Req, HandlerInfo, global_viewer_actor_context())
    );
assert_global_viewer_rbac(Req, HandlerInfo) ->
    ?assertMatch(
        {error, _},
        emqx_dashboard_rbac:check_rbac(Req, HandlerInfo, global_viewer_actor_context())
    ).

delete_mfa(Token, Username) ->
    Req = #{
        bindings => #{username => Username},
        path => <<"/api/v5/users/", Username/binary, "/mfa">>
    },
    HandlerInfo = #{method => delete, module => emqx_dashboard_api, function => change_mfa},
    emqx_dashboard_admin:verify_token(Req, HandlerInfo, Token).

delete_own_mfa_http(Token) ->
    Url = emqx_mgmt_api_test_util:api_path(["current_user", "mfa"]),
    emqx_mgmt_api_test_util:request_api(
        delete,
        Url,
        [],
        [bearer_auth_header(Token)],
        [],
        #{compatible_mode => true}
    ).

bearer_auth_header(Token) ->
    {"Authorization", "Bearer " ++ binary_to_list(Token)}.

setup_mfa(Token, Username) ->
    Req = #{
        bindings => #{username => Username},
        path => <<"/api/v5/users/", Username/binary, "/mfa">>
    },
    HandlerInfo = #{method => post, module => emqx_dashboard_api, function => change_mfa},
    emqx_dashboard_admin:verify_token(Req, HandlerInfo, Token).

add_default_superuser() ->
    {ok, _NewUser} = emqx_dashboard_admin:add_user(
        ?DEFAULT_SUPERUSER,
        ?DEFAULT_SUPERUSER_PASS,
        ?ROLE_SUPERUSER,
        ?ADD_DESCRIPTION
    ).
