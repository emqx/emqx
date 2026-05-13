%%--------------------------------------------------------------------
%% Copyright (c) 2026-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
%%
%% Coverage for dashboard login user scope checking
%% (emqx_dashboard_rbac:check_login_user_scopes/2) and the MFA
%% self-lock matrix (emqx_dashboard_api:authorize_mfa_change/3).
%%
%% MFA self-lock decision matrix:
%%
%%   IsFirstSetup = (Op == setup) AND (target.mfa_state == not_configured)
%%   IsSelf       = (caller.username == target)
%%   HasMfaMgmt   = caller effective scopes contain mfa_management
%%   Locked       = target.admin_override == mfa_required
%%                  (admin_override == mfa_exempted or undefined => unlocked)
%%
%%   IsFirstSetup                                    => allow (deadlock prevention)
%%   IsSelf       AND HasMfaMgmt                     => allow (self-exempt)
%%   IsSelf       AND NOT HasMfaMgmt AND NOT Locked  => allow
%%   IsSelf       AND NOT HasMfaMgmt AND Locked      => deny  mfa_locked
%%   NOT IsSelf   AND HasMfaMgmt AND administrator   => allow (admin reset)
%%   NOT IsSelf   AND HasMfaMgmt AND non-admin       => deny  self_only
%%   NOT IsSelf   AND NOT HasMfaMgmt                 => deny  missing_mfa_mgmt
%%
%% Field-write tests verify the admin_override write rules:
%% admin reinit writes mfa_required; admin disable writes mfa_exempted;
%% self operations leave admin_override untouched (self cannot revoke
%% an admin decision).
%%--------------------------------------------------------------------

-module(emqx_dashboard_user_scopes_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("emqx_utils/include/emqx_api_key_scopes.hrl").
-include("emqx_dashboard.hrl").

-define(HOST, "http://127.0.0.1:18083").
-define(BASE_PATH, "/api/v5").

-define(EE_ONLY(EXPR, NON_EE),
    case emqx_release:edition() of
        ee -> EXPR;
        _ -> NON_EE
    end
).

all() ->
    ?EE_ONLY(emqx_common_test_helpers:all(?MODULE), []).

init_per_suite(Config) ->
    ?EE_ONLY(
        begin
            Apps = emqx_cth_suite:start(
                [
                    emqx,
                    emqx_conf,
                    emqx_management,
                    emqx_mgmt_api_test_util:emqx_dashboard()
                ],
                #{work_dir => emqx_cth_suite:work_dir(Config)}
            ),
            [{apps, Apps} | Config]
        end,
        Config
    ).

end_per_suite(Config) ->
    ?EE_ONLY(
        begin
            mnesia:clear_table(?ADMIN),
            mnesia:clear_table(?ADMIN_JWT),
            emqx_cth_suite:stop(?config(apps, Config))
        end,
        ok
    ).

init_per_testcase(_Case, Config) ->
    %% Each testcase starts from a clean table — the decision matrix
    %% requires precise control over admin_override, which is writable
    %% but not easily resettable between cases.
    mnesia:clear_table(?ADMIN),
    mnesia:clear_table(?ADMIN_JWT),
    Config.

end_per_testcase(_Case, _Config) ->
    ok.

%%--------------------------------------------------------------------
%% role x scope schema validation (POST /users)
%%--------------------------------------------------------------------

%% Administrator can hold any of the four login-only scopes.
t_admin_can_hold_all_4_new_scopes(_Config) ->
    add_admin(<<"admin">>),
    Token = jwt(<<"admin">>, test_password()),
    AllNew = ?LOGIN_ONLY_SCOPES,
    Body = #{
        <<"username">> => <<"admin2">>,
        <<"password">> => test_password(),
        <<"role">> => ?ROLE_SUPERUSER,
        <<"description">> => <<"test">>,
        <<"scopes">> => AllNew
    },
    {ok, 200, _} = request_api(post, api_path(["users"]), auth_header(Token), Body),
    %% Verify it persisted
    [Admin] = emqx_dashboard_admin:lookup_user(<<"admin2">>),
    Stored = emqx_dashboard_admin:scopes_of(Admin#?ADMIN.username),
    ?assertEqual(lists:sort(AllNew), lists:sort(Stored)).

%% Response from POST /users must include the just-set scopes so the
%% client can round-trip the assignment (regression for the
%% to_external_user/1 omission caught in code review).
t_post_users_response_includes_scopes(_Config) ->
    add_admin(<<"admin">>),
    Token = jwt(<<"admin">>, test_password()),
    Body = #{
        <<"username">> => <<"with_scopes">>,
        <<"password">> => test_password(),
        <<"role">> => ?ROLE_SUPERUSER,
        <<"description">> => <<"test">>,
        <<"scopes">> => [?SCOPE_USER_MGMT, ?SCOPE_MFA_MGMT]
    },
    {ok, 200, RespBody} = request_api(
        post, api_path(["users"]), auth_header(Token), Body
    ),
    Resp = emqx_utils_json:decode(RespBody),
    ?assertEqual([<<"user_management">>, <<"mfa_management">>], maps:get(<<"scopes">>, Resp)),
    ?assertEqual(<<"with_scopes">>, maps:get(<<"username">>, Resp)),
    ?assertEqual(?ROLE_SUPERUSER, maps:get(<<"role">>, Resp)).

%% Response from POST /users without an explicit `scopes' field
%% projects the role-default. Admin (?ROLE_SUPERUSER) default is the
%% full 14-scope list.
t_post_users_response_role_default_scopes_when_not_set(_Config) ->
    add_admin(<<"admin">>),
    Token = jwt(<<"admin">>, test_password()),
    Body = #{
        <<"username">> => <<"no_scopes">>,
        <<"password">> => test_password(),
        <<"role">> => ?ROLE_SUPERUSER,
        <<"description">> => <<"test">>
    },
    {ok, 200, RespBody} = request_api(
        post, api_path(["users"]), auth_header(Token), Body
    ),
    Resp = emqx_utils_json:decode(RespBody),
    ScopesOut = maps:get(<<"scopes">>, Resp),
    ?assert(is_list(ScopesOut)),
    %% Admin default contains every login-only scope plus every
    %% common scope — assert membership rather than exact equality
    %% to avoid coupling to catalog ordering or count.
    CommonNames = [N || #{name := N} <- emqx_scope_catalog:common_scope_catalog()],
    LoginOnlyNames = [N || #{name := N} <- emqx_scope_catalog:admin_only_scope_catalog()],
    lists:foreach(
        fun(N) -> ?assert(lists:member(N, ScopesOut)) end,
        CommonNames ++ LoginOnlyNames
    ),
    ?assertEqual(length(CommonNames) + length(LoginOnlyNames), length(ScopesOut)).

%% Viewer default = common scopes only (no login-only).
%% Viewer default = the common scopes, no login-only ones.
t_post_users_response_viewer_default_scopes(_Config) ->
    add_admin(<<"admin">>),
    Token = jwt(<<"admin">>, test_password()),
    Body = #{
        <<"username">> => <<"viewer_no_scopes">>,
        <<"password">> => test_password(),
        <<"role">> => ?ROLE_VIEWER,
        <<"description">> => <<"test">>
    },
    {ok, 200, RespBody} = request_api(
        post, api_path(["users"]), auth_header(Token), Body
    ),
    Resp = emqx_utils_json:decode(RespBody),
    ScopesOut = maps:get(<<"scopes">>, Resp),
    ?assert(is_list(ScopesOut)),
    CommonNames = [N || #{name := N} <- emqx_scope_catalog:common_scope_catalog()],
    ?assertEqual(length(CommonNames), length(ScopesOut)),
    ?assertNot(lists:member(?SCOPE_USER_MGMT, ScopesOut)),
    ?assertNot(lists:member(?SCOPE_MFA_MGMT, ScopesOut)).

%% Response from PUT /users/:name must reflect the updated scopes.
t_put_users_response_includes_updated_scopes(_Config) ->
    add_admin(<<"admin">>),
    Token = jwt(<<"admin">>, test_password()),
    {ok, _} = emqx_dashboard_admin:add_user(
        <<"u_update">>, test_password(), ?ROLE_SUPERUSER, "u"
    ),
    PutBody = #{
        <<"role">> => ?ROLE_SUPERUSER,
        <<"description">> => <<"updated">>,
        <<"scopes">> => [?SCOPE_MFA_MGMT]
    },
    {ok, 200, RespBody} = request_api(
        put, api_path(["users", "u_update"]), auth_header(Token), PutBody
    ),
    Resp = emqx_utils_json:decode(RespBody),
    ?assertEqual([<<"mfa_management">>], maps:get(<<"scopes">>, Resp)),
    ?assertEqual(<<"updated">>, maps:get(<<"description">>, Resp)).

%% Viewer cannot hold user_management.
t_viewer_cannot_hold_user_management(_Config) ->
    add_admin(<<"admin">>),
    Token = jwt(<<"admin">>, test_password()),
    Body = #{
        <<"username">> => <<"v">>,
        <<"password">> => test_password(),
        <<"role">> => ?ROLE_VIEWER,
        <<"description">> => <<"test">>,
        <<"scopes">> => [?SCOPE_USER_MGMT]
    },
    ?assertMatch(
        {ok, 400, _},
        request_api(post, api_path(["users"]), auth_header(Token), Body)
    ).

%% Viewer CAN hold mfa_management — non-admin self-exemption rule
%% (viewer self-exemption rule).
t_viewer_can_hold_mfa_management(_Config) ->
    add_admin(<<"admin">>),
    Token = jwt(<<"admin">>, test_password()),
    Body = #{
        <<"username">> => <<"v">>,
        <<"password">> => test_password(),
        <<"role">> => ?ROLE_VIEWER,
        <<"description">> => <<"test">>,
        <<"scopes">> => [?SCOPE_MFA_MGMT]
    },
    ?assertMatch(
        {ok, 200, _},
        request_api(post, api_path(["users"]), auth_header(Token), Body)
    ).

%% Viewer cannot hold sso_management.
t_viewer_cannot_hold_sso_management(_Config) ->
    add_admin(<<"admin">>),
    Token = jwt(<<"admin">>, test_password()),
    Body = #{
        <<"username">> => <<"v">>,
        <<"password">> => test_password(),
        <<"role">> => ?ROLE_VIEWER,
        <<"description">> => <<"test">>,
        <<"scopes">> => [?SCOPE_SSO_MGMT]
    },
    ?assertMatch(
        {ok, 400, _},
        request_api(post, api_path(["users"]), auth_header(Token), Body)
    ).

%% Unknown scope name is rejected.
t_unknown_scope_returns_400(_Config) ->
    add_admin(<<"admin">>),
    Token = jwt(<<"admin">>, test_password()),
    Body = #{
        <<"username">> => <<"u">>,
        <<"password">> => test_password(),
        <<"role">> => ?ROLE_SUPERUSER,
        <<"description">> => <<"test">>,
        <<"scopes">> => [<<"bogus_scope_name">>]
    },
    ?assertMatch(
        {ok, 400, _},
        request_api(post, api_path(["users"]), auth_header(Token), Body)
    ).

%%--------------------------------------------------------------------
%% Default administrator protection
%%
%% The user configured via `dashboard.default_username' is a
%% break-glass account: it cannot be deleted, demoted, or assigned
%% an explicit scope list. These guards keep the cluster recoverable
%% even after other admins are accidentally restricted or removed.
%%--------------------------------------------------------------------

%% PUT may not change the default admin's role.
t_default_admin_cannot_be_demoted(_Config) ->
    add_admin(<<"admin">>),
    Token = jwt(<<"admin">>, test_password()),
    Body = #{
        <<"role">> => ?ROLE_VIEWER,
        <<"description">> => <<"trying to demote">>
    },
    ?assertMatch(
        {ok, 400, _},
        request_api(put, api_path(["users", "admin"]), auth_header(Token), Body)
    ).

%% PUT may not set an explicit scope list on the default admin,
%% even when keeping the administrator role.
t_default_admin_cannot_have_explicit_scopes(_Config) ->
    add_admin(<<"admin">>),
    Token = jwt(<<"admin">>, test_password()),
    Body = #{
        <<"role">> => ?ROLE_SUPERUSER,
        <<"description">> => <<"trying to restrict">>,
        <<"scopes">> => [?SCOPE_CONNECTIONS]
    },
    ?assertMatch(
        {ok, 400, _},
        request_api(put, api_path(["users", "admin"]), auth_header(Token), Body)
    ).

%% Even an empty scope list (the self-restriction case) is rejected.
t_default_admin_cannot_be_set_to_empty_scopes(_Config) ->
    add_admin(<<"admin">>),
    Token = jwt(<<"admin">>, test_password()),
    Body = #{
        <<"role">> => ?ROLE_SUPERUSER,
        <<"description">> => <<"trying to clear">>,
        <<"scopes">> => []
    },
    ?assertMatch(
        {ok, 400, _},
        request_api(put, api_path(["users", "admin"]), auth_header(Token), Body)
    ).

%% PUT that only updates the description (no role / scopes) is allowed.
t_default_admin_description_can_be_updated(_Config) ->
    add_admin(<<"admin">>),
    Token = jwt(<<"admin">>, test_password()),
    Body = #{
        <<"role">> => ?ROLE_SUPERUSER,
        <<"description">> => <<"updated desc">>
    },
    ?assertMatch(
        {ok, 200, _},
        request_api(put, api_path(["users", "admin"]), auth_header(Token), Body)
    ).

%% DELETE is unconditionally rejected for the default admin.
t_default_admin_cannot_be_deleted(_Config) ->
    add_admin(<<"admin">>),
    {ok, _} = emqx_dashboard_admin:add_user(
        <<"another">>, test_password(), ?ROLE_SUPERUSER, "other admin"
    ),
    Token = jwt(<<"another">>, test_password()),
    ?assertMatch(
        {ok, 400, _},
        request_api(delete, api_path(["users", "admin"]), auth_header(Token), #{})
    ).

%% H8: The break-glass protection must only apply to the local default
%% administrator. An SSO `DELETE /users/<name>?backend=<x>' request
%% targets `{Backend, Name}', not the local `Name', and must not be
%% rejected even when `Name' happens to match
%% `dashboard.default_username'. We assert the response is not the
%% break-glass `Cannot delete the default administrator user' one
%% (the request itself goes on to fail for an unrelated reason because
%% no SSO backend is started in this suite — only the dispatch path is
%% under test).
t_default_admin_protection_does_not_apply_to_sso_users(_Config) ->
    add_admin(<<"admin">>),
    {ok, _} = emqx_dashboard_admin:add_user(
        <<"another">>, test_password(), ?ROLE_SUPERUSER, "other admin"
    ),
    %% Use a different admin to issue the request so the response is not
    %% short-circuited by the self-delete guard further down the handler.
    Token = jwt(<<"another">>, test_password()),
    {ok, _Code, RespBody} = emqx_common_test_http:request_api(
        delete,
        api_path(["users", "admin"]),
        "backend=ldap",
        auth_header(Token)
    ),
    ?assertNotMatch(
        {match, _},
        re:run(
            iolist_to_binary(RespBody),
            <<"default administrator">>,
            [caseless]
        )
    ).

%% H6: Role demotion must consider persisted scopes, not just the
%% request body. A user with persisted admin-only scopes that is
%% demoted to viewer via a partial-update PUT (no `scopes' field) must
%% be rejected — otherwise the viewer would silently retain
%% admin-only scopes such as `user_management'.
t_role_demotion_with_persisted_admin_scopes_is_rejected(_Config) ->
    add_admin(<<"admin">>),
    Token = jwt(<<"admin">>, test_password()),
    {ok, _} = emqx_dashboard_admin:add_user(
        <<"u">>, test_password(), ?ROLE_SUPERUSER, "demote-target"
    ),
    %% Give the admin user an admin-only scope, then attempt a
    %% partial-update that drops them to viewer without touching
    %% `scopes'. The persisted `user_management' scope makes the
    %% effective set incompatible with the new role.
    {ok, ok} = emqx_dashboard_admin:set_user_scopes(<<"u">>, [?SCOPE_USER_MGMT]),
    PutBody = #{
        <<"role">> => ?ROLE_VIEWER,
        <<"description">> => <<"demoted">>
    },
    ?assertMatch(
        {ok, 400, _},
        request_api(put, api_path(["users", "u"]), auth_header(Token), PutBody)
    ),
    %% The persisted role + scopes must stay intact after the rejection.
    [Admin] = emqx_dashboard_admin:lookup_user(<<"u">>),
    ?assertEqual(?ROLE_SUPERUSER, Admin#?ADMIN.role),
    ?assertEqual([?SCOPE_USER_MGMT], emqx_dashboard_admin:scopes_of(<<"u">>)).

%% Counterpart of the H6 rejection: when persisted scopes are
%% compatible with the new role (or fall back to the role defaults),
%% the partial-update PUT must succeed.
t_role_demotion_with_compatible_persisted_scopes_succeeds(_Config) ->
    add_admin(<<"admin">>),
    Token = jwt(<<"admin">>, test_password()),
    {ok, _} = emqx_dashboard_admin:add_user(
        <<"u">>, test_password(), ?ROLE_SUPERUSER, "demote-target"
    ),
    %% mfa_management is allowed for any role, so the persisted
    %% scope remains compatible after demotion.
    {ok, ok} = emqx_dashboard_admin:set_user_scopes(<<"u">>, [?SCOPE_MFA_MGMT]),
    PutBody = #{
        <<"role">> => ?ROLE_VIEWER,
        <<"description">> => <<"demoted">>
    },
    {ok, 200, _} =
        request_api(put, api_path(["users", "u"]), auth_header(Token), PutBody),
    [Admin] = emqx_dashboard_admin:lookup_user(<<"u">>),
    ?assertEqual(?ROLE_VIEWER, Admin#?ADMIN.role).

%%--------------------------------------------------------------------
%% MFA self-lock matrix (the 7-line decision table)
%%--------------------------------------------------------------------

%% Row 1: First-time setup is always allowed, regardless of locks.
%% Without this, a user with force_mfa=true would be unable to
%% complete initial MFA setup (deadlock).
t_first_time_setup_always_allowed(_Config) ->
    add_admin(<<"admin">>),
    {ok, _} = emqx_dashboard_admin:add_user(
        <<"u">>, test_password(), ?ROLE_VIEWER, "u"
    ),
    %% Force the lock state — would normally block a non-first-time
    %% rotate. mfa_state is absent (not_configured), so the matrix
    %% short-circuits to allow.
    {ok, ok} = emqx_dashboard_admin:set_admin_override(<<"u">>, ?ADMIN_MFA_REQUIRED),
    Token = jwt(<<"u">>, test_password()),
    Body = #{<<"mechanism">> => <<"totp">>},
    ?assertMatch(
        {ok, 204, _},
        request_api(post, api_path(["users", "u", "mfa"]), auth_header(Token), Body)
    ).

%% Row 2: Self with mfa_management scope, locked — allowed.
t_self_with_mfa_mgmt_can_rotate_under_force_mfa_lock(_Config) ->
    add_admin(<<"admin">>),
    {ok, _} = emqx_dashboard_admin:add_user(
        <<"u">>, test_password(), ?ROLE_VIEWER, "u"
    ),
    {ok, ok} = emqx_dashboard_admin:set_user_scopes(<<"u">>, [?SCOPE_MFA_MGMT]),
    %% First setup MFA so subsequent POST is a rotate, not first-setup
    {ok, ok} = emqx_dashboard_admin:set_mfa_state(
        <<"u">>, #{mechanism => totp, secret => <<"S1">>, first_verify_ts => 1}
    ),
    %% Now lock the user
    {ok, ok} = emqx_dashboard_admin:set_admin_override(<<"u">>, ?ADMIN_MFA_REQUIRED),
    Token = jwt(<<"u">>, test_password()),
    Body = #{<<"mechanism">> => <<"totp">>},
    ?assertMatch(
        {ok, 204, _},
        request_api(post, api_path(["users", "u", "mfa"]), auth_header(Token), Body)
    ).

%% Row 3: Self without mfa_management, NOT locked — allowed.
t_self_can_rotate_when_not_locked(_Config) ->
    add_admin(<<"admin">>),
    {ok, _} = emqx_dashboard_admin:add_user(
        <<"u">>, test_password(), ?ROLE_VIEWER, "u"
    ),
    {ok, ok} = emqx_dashboard_admin:set_mfa_state(
        <<"u">>, #{mechanism => totp, secret => <<"S1">>, first_verify_ts => 1}
    ),
    Token = jwt(<<"u">>, test_password()),
    Body = #{<<"mechanism">> => <<"totp">>},
    ?assertMatch(
        {ok, 204, _},
        request_api(post, api_path(["users", "u", "mfa"]), auth_header(Token), Body)
    ).

%% Row 4: Self without mfa_management, force_mfa locked — denied.
t_self_cannot_rotate_when_force_mfa_locked(_Config) ->
    add_admin(<<"admin">>),
    {ok, _} = emqx_dashboard_admin:add_user(
        <<"u">>, test_password(), ?ROLE_VIEWER, "u"
    ),
    {ok, ok} = emqx_dashboard_admin:set_mfa_state(
        <<"u">>, #{mechanism => totp, secret => <<"S1">>, first_verify_ts => 1}
    ),
    {ok, ok} = emqx_dashboard_admin:set_admin_override(<<"u">>, ?ADMIN_MFA_REQUIRED),
    Token = jwt(<<"u">>, test_password()),
    Body = #{<<"mechanism">> => <<"totp">>},
    {ok, 403, RespBody} = request_api(
        post, api_path(["users", "u", "mfa"]), auth_header(Token), Body
    ),
    Json = emqx_utils_json:decode(RespBody),
    ?assertEqual(<<"MFA_LOCKED">>, maps:get(<<"code">>, Json)).

%% Row 4 variant: admin override mfa_required instead of force_mfa.
t_self_cannot_rotate_when_admin_override_required_locked(_Config) ->
    add_admin(<<"admin">>),
    {ok, _} = emqx_dashboard_admin:add_user(
        <<"u">>, test_password(), ?ROLE_VIEWER, "u"
    ),
    {ok, ok} = emqx_dashboard_admin:set_mfa_state(
        <<"u">>, #{mechanism => totp, secret => <<"S1">>, first_verify_ts => 1}
    ),
    {ok, ok} = emqx_dashboard_admin:set_admin_override(<<"u">>, ?ADMIN_MFA_REQUIRED),
    Token = jwt(<<"u">>, test_password()),
    Body = #{<<"mechanism">> => <<"totp">>},
    {ok, 403, RespBody} = request_api(
        post, api_path(["users", "u", "mfa"]), auth_header(Token), Body
    ),
    Json = emqx_utils_json:decode(RespBody),
    ?assertEqual(<<"MFA_LOCKED">>, maps:get(<<"code">>, Json)).

%% admin_override = mfa_required locks self changes: holding
%% mfa_management scope self-exempts from both.
t_self_with_mfa_mgmt_can_rotate_under_admin_override_required_lock(_Config) ->
    add_admin(<<"admin">>),
    {ok, _} = emqx_dashboard_admin:add_user(
        <<"u">>, test_password(), ?ROLE_VIEWER, "u"
    ),
    {ok, ok} = emqx_dashboard_admin:set_user_scopes(<<"u">>, [?SCOPE_MFA_MGMT]),
    {ok, ok} = emqx_dashboard_admin:set_mfa_state(
        <<"u">>, #{mechanism => totp, secret => <<"S1">>, first_verify_ts => 1}
    ),
    {ok, ok} = emqx_dashboard_admin:set_admin_override(<<"u">>, ?ADMIN_MFA_REQUIRED),
    Token = jwt(<<"u">>, test_password()),
    Body = #{<<"mechanism">> => <<"totp">>},
    ?assertMatch(
        {ok, 204, _},
        request_api(post, api_path(["users", "u", "mfa"]), auth_header(Token), Body)
    ).

%% Self-DELETE under admin_override=mfa_required without mfa_management — denied.
t_self_cannot_delete_when_admin_override_required_locked(_Config) ->
    add_admin(<<"admin">>),
    {ok, _} = emqx_dashboard_admin:add_user(
        <<"u">>, test_password(), ?ROLE_VIEWER, "u"
    ),
    {ok, ok} = emqx_dashboard_admin:set_mfa_state(
        <<"u">>, #{mechanism => totp, secret => <<"S1">>, first_verify_ts => 1}
    ),
    {ok, ok} = emqx_dashboard_admin:set_admin_override(<<"u">>, ?ADMIN_MFA_REQUIRED),
    Token = jwt(<<"u">>, test_password()),
    {ok, 403, RespBody} = request_api(
        delete, api_path(["users", "u", "mfa"]), auth_header(Token), #{}
    ),
    Json = emqx_utils_json:decode(RespBody),
    ?assertEqual(<<"MFA_LOCKED">>, maps:get(<<"code">>, Json)).

%% Self-DELETE under admin_override=mfa_required WITH mfa_management — allowed
%% (self-exemption applies to DELETE too, not just POST/rotate).
t_self_with_mfa_mgmt_can_delete_under_admin_override_required_lock(_Config) ->
    add_admin(<<"admin">>),
    {ok, _} = emqx_dashboard_admin:add_user(
        <<"u">>, test_password(), ?ROLE_VIEWER, "u"
    ),
    {ok, ok} = emqx_dashboard_admin:set_user_scopes(<<"u">>, [?SCOPE_MFA_MGMT]),
    {ok, ok} = emqx_dashboard_admin:set_mfa_state(
        <<"u">>, #{mechanism => totp, secret => <<"S1">>, first_verify_ts => 1}
    ),
    {ok, ok} = emqx_dashboard_admin:set_admin_override(<<"u">>, ?ADMIN_MFA_REQUIRED),
    Token = jwt(<<"u">>, test_password()),
    ?assertMatch(
        {ok, 204, _},
        request_api(delete, api_path(["users", "u", "mfa"]), auth_header(Token), #{})
    ).

%% Row 5: Admin can reset another user's MFA — admin has implicit
%% mfa_management via role-default fallback.
t_admin_can_reset_others_mfa(_Config) ->
    add_admin(<<"admin">>),
    {ok, _} = emqx_dashboard_admin:add_user(
        <<"u">>, test_password(), ?ROLE_VIEWER, "u"
    ),
    {ok, ok} = emqx_dashboard_admin:set_mfa_state(
        <<"u">>, #{mechanism => totp, secret => <<"S1">>, first_verify_ts => 1}
    ),
    Token = jwt(<<"admin">>, test_password()),
    ?assertMatch(
        {ok, 204, _},
        request_api(
            delete, api_path(["users", "u", "mfa"]), auth_header(Token), #{}
        )
    ).

%% End-to-end: admin disables a policy-locked user's MFA, then the
%% user can self-setup MFA again (admin_override=mfa_exempted unlocks
%% them despite snapshot=true). This exercises the full HTTP path
%% across admin disable → self POST.
t_admin_disable_unlocks_user_for_self_setup(_Config) ->
    add_admin(<<"admin">>),
    {ok, _} = emqx_dashboard_admin:add_user(
        <<"u">>, test_password(), ?ROLE_VIEWER, "u"
    ),
    {ok, ok} = emqx_dashboard_admin:set_admin_override(<<"u">>, ?ADMIN_MFA_REQUIRED),
    {ok, ok} = emqx_dashboard_admin:set_mfa_state(
        <<"u">>, #{mechanism => totp, secret => <<"S1">>, first_verify_ts => 1}
    ),
    AdminToken = jwt(<<"admin">>, test_password()),
    %% Admin disables MFA — writes admin_override=mfa_exempted.
    ?assertMatch(
        {ok, 204, _},
        request_api(
            delete, api_path(["users", "u", "mfa"]), auth_header(AdminToken), #{}
        )
    ),
    ?assertEqual(?ADMIN_MFA_EXEMPTED, emqx_dashboard_admin:admin_override_of(<<"u">>)),
    %% User self-setup MFA — would be locked by snapshot=true, but
    %% admin_override=mfa_exempted overrides.
    UserToken = jwt(<<"u">>, test_password()),
    ?assertMatch(
        {ok, 204, _},
        request_api(
            post,
            api_path(["users", "u", "mfa"]),
            auth_header(UserToken),
            #{<<"mechanism">> => <<"totp">>}
        )
    ).

%% End-to-end: admin forces MFA on a previously unlocked user
%% (snapshot=false). User cannot self-disable afterwards even though
%% snapshot says no lock — admin_override=mfa_required overrides.
t_admin_force_locks_user_against_self_disable(_Config) ->
    add_admin(<<"admin">>),
    {ok, _} = emqx_dashboard_admin:add_user(
        <<"u">>, test_password(), ?ROLE_VIEWER, "u"
    ),
    {ok, ok} = emqx_dashboard_admin:set_admin_override(<<"u">>, undefined),
    AdminToken = jwt(<<"admin">>, test_password()),
    %% Admin forces MFA setup — writes admin_override=mfa_required.
    ?assertMatch(
        {ok, 204, _},
        request_api(
            post,
            api_path(["users", "u", "mfa"]),
            auth_header(AdminToken),
            #{<<"mechanism">> => <<"totp">>}
        )
    ),
    ?assertEqual(?ADMIN_MFA_REQUIRED, emqx_dashboard_admin:admin_override_of(<<"u">>)),
    %% Make MFA actually enabled (post-verify); set state directly to
    %% bypass the verify step.
    {ok, ok} = emqx_dashboard_admin:set_mfa_state(
        <<"u">>, #{mechanism => totp, secret => <<"S2">>, first_verify_ts => 1}
    ),
    UserToken = jwt(<<"u">>, test_password()),
    %% User self-disable denied — admin_override=mfa_required locks
    %% even with snapshot=false.
    {ok, 403, RespBody} = request_api(
        delete, api_path(["users", "u", "mfa"]), auth_header(UserToken), #{}
    ),
    Json = emqx_utils_json:decode(RespBody),
    ?assertEqual(<<"MFA_LOCKED">>, maps:get(<<"code">>, Json)).

%% Row 6: Non-admin with mfa_management cannot manage other users' MFA.
%%
%% Note: in the current code path the request is rejected by the
%% existing RBAC layer (PR #16943, preserved unchanged) BEFORE the
%% MFA self-lock check has a chance to run — the RBAC clause for
%% ?ROLE_VIEWER POST /users/<other>/mfa returns false. So the
%% expected error code is UNAUTHORIZED_ROLE, not MFA_SELF_ONLY.
%%
%% This is the correct RBAC × scope stacking behaviour. MFA_SELF_ONLY
%% would only be reachable if a future change ever loosens the RBAC
%% viewer rule for cross-user MFA paths; the scope check serves as
%% defense-in-depth for that scenario.
t_non_admin_with_mfa_mgmt_cannot_reset_others(_Config) ->
    add_admin(<<"admin">>),
    {ok, _} = emqx_dashboard_admin:add_user(
        <<"v1">>, test_password(), ?ROLE_VIEWER, "v1"
    ),
    {ok, ok} = emqx_dashboard_admin:set_user_scopes(<<"v1">>, [?SCOPE_MFA_MGMT]),
    {ok, _} = emqx_dashboard_admin:add_user(
        <<"v2">>, test_password(), ?ROLE_VIEWER, "v2"
    ),
    {ok, ok} = emqx_dashboard_admin:set_mfa_state(
        <<"v2">>, #{mechanism => totp, secret => <<"S1">>, first_verify_ts => 1}
    ),
    Token = jwt(<<"v1">>, test_password()),
    {ok, 403, _RespBody} = request_api(
        delete, api_path(["users", "v2", "mfa"]), auth_header(Token), #{}
    ).

%% Row 7: Non-admin without mfa_management cannot manage other users' MFA.
t_viewer_cannot_reset_other_users_mfa(_Config) ->
    add_admin(<<"admin">>),
    {ok, _} = emqx_dashboard_admin:add_user(
        <<"v1">>, test_password(), ?ROLE_VIEWER, "v1"
    ),
    {ok, _} = emqx_dashboard_admin:add_user(
        <<"v2">>, test_password(), ?ROLE_VIEWER, "v2"
    ),
    Token = jwt(<<"v1">>, test_password()),
    {ok, 403, RespBody} = request_api(
        delete, api_path(["users", "v2", "mfa"]), auth_header(Token), #{}
    ),
    Json = emqx_utils_json:decode(RespBody),
    %% RBAC layer rejects this BEFORE the scope check kicks in (viewer
    %% cannot DELETE on /users/<other>/mfa). The exact error code
    %% depends on which layer rejects first, but it must be 403.
    ?assert(maps:is_key(<<"code">>, Json)).

%%--------------------------------------------------------------------
%% Field-write triggers (admin_override write rules)
%%--------------------------------------------------------------------

%% Self reinit_mfa does NOT touch admin_override — self cannot revoke
%% an admin decision.
t_self_reinit_does_not_touch_admin_override(_Config) ->
    {ok, _} = emqx_dashboard_admin:add_user(
        <<"u">>, test_password(), ?ROLE_VIEWER, "u"
    ),
    {ok, ok} = emqx_dashboard_admin:set_admin_override(<<"u">>, ?ADMIN_MFA_REQUIRED),
    ok = emqx_dashboard_admin:reinit_mfa(<<"u">>, totp, _ByAdmin = false),
    ?assertEqual(?ADMIN_MFA_REQUIRED, emqx_dashboard_admin:admin_override_of(<<"u">>)).

%% Admin reinit_mfa writes admin_override=mfa_required regardless of
%% snapshot — admin's decision overrides policy.
t_admin_reinit_writes_admin_override_required(_Config) ->
    {ok, _} = emqx_dashboard_admin:add_user(
        <<"u">>, test_password(), ?ROLE_VIEWER, "u"
    ),
    ok = emqx_dashboard_admin:reinit_mfa(<<"u">>, totp, _ByAdmin = true),
    ?assertEqual(?ADMIN_MFA_REQUIRED, emqx_dashboard_admin:admin_override_of(<<"u">>)).

%% Admin reinit on a policy-locked user STILL writes admin_override=
%% mfa_required — admin's decision is independent of and overrides
%% the snapshot.
t_admin_reinit_writes_required_even_when_snapshot_true(_Config) ->
    {ok, _} = emqx_dashboard_admin:add_user(
        <<"u">>, test_password(), ?ROLE_VIEWER, "u"
    ),
    {ok, ok} = emqx_dashboard_admin:set_admin_override(<<"u">>, ?ADMIN_MFA_REQUIRED),
    ok = emqx_dashboard_admin:reinit_mfa(<<"u">>, totp, _ByAdmin = true),
    ?assertEqual(?ADMIN_MFA_REQUIRED, emqx_dashboard_admin:admin_override_of(<<"u">>)).

%% Admin disable_mfa writes admin_override=mfa_exempted regardless of
%% snapshot — admin explicitly exempts the user from any future lock.
t_admin_disable_writes_admin_override_exempted(_Config) ->
    {ok, _} = emqx_dashboard_admin:add_user(
        <<"u">>, test_password(), ?ROLE_VIEWER, "u"
    ),
    {ok, ok} = emqx_dashboard_admin:set_admin_override(<<"u">>, ?ADMIN_MFA_REQUIRED),
    {ok, ok} = emqx_dashboard_admin:set_mfa_state(
        <<"u">>, #{mechanism => totp, secret => <<"S1">>, first_verify_ts => 1}
    ),
    ok = emqx_dashboard_admin:disable_mfa(<<"u">>, _ByAdmin = true),
    ?assertEqual(?ADMIN_MFA_EXEMPTED, emqx_dashboard_admin:admin_override_of(<<"u">>)).

%% Self disable_mfa does NOT touch admin_override.
t_self_disable_does_not_touch_admin_override(_Config) ->
    {ok, _} = emqx_dashboard_admin:add_user(
        <<"u">>, test_password(), ?ROLE_VIEWER, "u"
    ),
    {ok, ok} = emqx_dashboard_admin:set_admin_override(<<"u">>, ?ADMIN_MFA_REQUIRED),
    {ok, ok} = emqx_dashboard_admin:set_mfa_state(
        <<"u">>, #{mechanism => totp, secret => <<"S1">>, first_verify_ts => 1}
    ),
    %% Lock state should still prevent self-disable in real handler,
    %% but here we test the field-write rule directly.
    ok = emqx_dashboard_admin:disable_mfa(<<"u">>, _ByAdmin = false),
    ?assertEqual(?ADMIN_MFA_REQUIRED, emqx_dashboard_admin:admin_override_of(<<"u">>)).

%% NOTE: scope-deny path coverage for emqx_dashboard_rbac:check_login_user_scopes/2
%% lives in apps/emqx_dashboard_rbac/test/emqx_dashboard_rbac_SUITE.erl,
%% because emqx_dashboard does not depend on emqx_dashboard_rbac and the
%% predicate is not loadable from this SUITE's app graph.

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

add_admin(Username) ->
    %% Password complexity policy: must contain mixed character classes
    %% (letters + at least one digit or special). Use a constant
    %% complex pass for all test admins; we never authenticate against
    %% it directly except via jwt/2 below.
    Pass = <<"P@ssw0rd">>,
    {ok, _} = emqx_dashboard_admin:add_user(Username, Pass, ?ROLE_SUPERUSER, "admin"),
    ok.

%% Same complexity rule — use the constant for all viewer test users
%% added via add_user/4 in this SUITE.
test_password() -> <<"P@ssw0rd">>.

jwt(Username, Password) ->
    {ok, #{token := Token}} = emqx_dashboard_admin:sign_token(
        Username, Password, ?TRUSTED_MFA_TOKEN
    ),
    Token.

auth_header(JwtToken) ->
    {"Authorization", "Bearer " ++ binary_to_list(JwtToken)}.

api_path(Parts) ->
    ?HOST ++ filename:join([?BASE_PATH | Parts]).

request_api(Method, Url, Auth) ->
    emqx_common_test_http:request_api(Method, Url, _QueryParams = [], Auth).

request_api(Method, Url, Auth, Body) ->
    emqx_common_test_http:request_api(
        Method, Url, _QueryParams = [], Auth, Body
    ).
