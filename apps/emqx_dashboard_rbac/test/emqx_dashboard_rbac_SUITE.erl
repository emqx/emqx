%%--------------------------------------------------------------------
%% Copyright (c) 2023-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_dashboard_rbac_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include("../../emqx_dashboard/include/emqx_dashboard.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

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
    SsoBackend = saml,
    SsoUser = <<"jackson-http@example.com">>,
    Desc = <<"desc">>,
    SsoConfig = emqx:get_config([dashboard, sso, SsoBackend], #{}),
    {ok, _} = emqx_dashboard_admin:add_sso_user(SsoBackend, SsoUser, ?ROLE_VIEWER, Desc),
    {ok, #{role := ?ROLE_VIEWER, token := SsoToken}} = emqx_dashboard_admin:sign_token(
        ?SSO_USERNAME(SsoBackend, SsoUser), <<>>
    ),
    try
        ok = emqx_config:put([dashboard, sso, SsoBackend], SsoConfig#{force_mfa => false}),
        ?assertMatch(
            {ok, 204, _},
            delete_mfa_urlencoded_username_http(SsoToken, SsoBackend, SsoUser)
        ),
        ok = emqx_config:put([dashboard, sso, SsoBackend], SsoConfig#{force_mfa => true}),
        ?assertMatch(
            {ok, 403, _},
            delete_mfa_urlencoded_username_http(SsoToken, SsoBackend, SsoUser)
        )
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
    ?assertMatch({ok, #{actor := Viewer1}}, VerifyFn(Viewer1Token, Viewer1)),
    %% viewer can't change other's password
    ?assertEqual({error, unauthorized_role}, VerifyFn(Viewer1Token, Viewer2)),
    ?assertEqual({error, unauthorized_role}, VerifyFn(Viewer1Token, SuperUser)),
    %% superuser can change other's password
    ?assertMatch({ok, #{actor := SuperUser}}, VerifyFn(SuperToken, Viewer1)),
    ?assertMatch({ok, #{actor := SuperUser}}, VerifyFn(SuperToken, Viewer2)),
    ?assertMatch({ok, #{actor := SuperUser}}, VerifyFn(SuperToken, SuperUser)),
    ok.

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
