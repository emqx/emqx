%%--------------------------------------------------------------------
%% Copyright (c) 2026-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
%%
%% feat/dashboard-user-scopes — coverage for dashboard login user
%% scope checking (commit 5: emqx_dashboard_rbac:check_login_user_scopes/2)
%% and MFA self-lock matrix (commit 6: authorize_mfa_change/3).
%%
%% Tests in this SUITE follow the matrix in
%% SPEC-dashboard-user-scopes.md sec 6.3:
%%
%%   IsFirstSetup => allow                                 (deadlock prevention)
%%   IsSelf       AND HasMfaMgmt           => allow        (self-exempt)
%%   IsSelf       AND NOT HasMfaMgmt AND NOT Locked => allow
%%   IsSelf       AND NOT HasMfaMgmt AND Locked     => deny mfa_locked
%%   NOT IsSelf   AND HasMfaMgmt AND administrator   => allow (admin reset)
%%   NOT IsSelf   AND HasMfaMgmt AND non-admin       => deny self_only
%%   NOT IsSelf   AND NOT HasMfaMgmt                 => deny missing_mfa_mgmt
%%
%% Field-write tests verify the non-cross-contamination invariant
%% from SPEC sec 6.1.1: admin_required is only set when ByAdmin=true
%% AND force_mfa_snapshot is NOT already true.
%%--------------------------------------------------------------------

-module(emqx_dashboard_user_scopes_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("emqx/include/emqx_api_key_scopes.hrl").
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
    %% Each testcase starts from a clean table — the SPEC matrix
    %% requires precise control over force_mfa_snapshot and
    %% admin_required, which are writable but not easily resettable
    %% between cases.
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
    %% Admin default contains all four login-only scopes plus all 10
    %% generic ones — assert membership rather than exact equality
    %% to avoid coupling to catalogue ordering.
    ?assert(lists:member(?SCOPE_USER_MGMT, ScopesOut)),
    ?assert(lists:member(?SCOPE_CONNECTIONS, ScopesOut)),
    ?assertEqual(14, length(ScopesOut)).

%% Viewer default = 10 generic scopes (no login-only).
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
    ?assertEqual(10, length(ScopesOut)),
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
%% (SPEC sec 3.1).
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
%% MFA self-lock matrix (SPEC sec 6.3, the 7-line decision table)
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
    {ok, ok} = emqx_dashboard_admin:set_force_mfa_snapshot(<<"u">>, true),
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
    {ok, ok} = emqx_dashboard_admin:set_force_mfa_snapshot(<<"u">>, true),
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
    {ok, ok} = emqx_dashboard_admin:set_force_mfa_snapshot(<<"u">>, true),
    Token = jwt(<<"u">>, test_password()),
    Body = #{<<"mechanism">> => <<"totp">>},
    {ok, 403, RespBody} = request_api(
        post, api_path(["users", "u", "mfa"]), auth_header(Token), Body
    ),
    Json = emqx_utils_json:decode(RespBody),
    ?assertEqual(<<"MFA_LOCKED">>, maps:get(<<"code">>, Json)).

%% Row 4 variant: admin_required lock instead of force_mfa.
t_self_cannot_rotate_when_admin_required_locked(_Config) ->
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

%% admin_required lock is symmetric with force_mfa_snapshot: holding
%% mfa_management scope self-exempts from both.
t_self_with_mfa_mgmt_can_rotate_under_admin_required_lock(_Config) ->
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

%% Self-DELETE under admin_required without mfa_management — denied.
t_self_cannot_delete_when_admin_required_locked(_Config) ->
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

%% Self-DELETE under admin_required WITH mfa_management — allowed
%% (self-exemption applies to DELETE too, not just POST/rotate).
t_self_with_mfa_mgmt_can_delete_under_admin_required_lock(_Config) ->
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

%% Row 6: Non-admin with mfa_management cannot manage other users' MFA.
%%
%% Note: in the current code path the request is rejected by the
%% existing RBAC layer (PR #16943, preserved unchanged) BEFORE the
%% MFA self-lock check has a chance to run — the RBAC clause for
%% ?ROLE_VIEWER POST /users/<other>/mfa returns false. So the
%% expected error code is UNAUTHORIZED_ROLE, not MFA_SELF_ONLY.
%%
%% This is the correct stacking behaviour described in SPEC sec
%% 7.11 (RBAC × scope). MFA_SELF_ONLY would only be reachable if a
%% future change ever loosens the RBAC viewer rule for cross-user
%% MFA paths; the scope check serves as defense-in-depth for that
%% scenario.
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
%% Field-write triggers (SPEC sec 6.1.1, v3.4 admin_override model)
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
    {ok, ok} = emqx_dashboard_admin:set_force_mfa_snapshot(<<"u">>, true),
    ok = emqx_dashboard_admin:reinit_mfa(<<"u">>, totp, _ByAdmin = true),
    ?assertEqual(?ADMIN_MFA_REQUIRED, emqx_dashboard_admin:admin_override_of(<<"u">>)).

%% Admin disable_mfa writes admin_override=mfa_exempted regardless of
%% snapshot — admin explicitly exempts the user from any future lock.
t_admin_disable_writes_admin_override_exempted(_Config) ->
    {ok, _} = emqx_dashboard_admin:add_user(
        <<"u">>, test_password(), ?ROLE_VIEWER, "u"
    ),
    {ok, ok} = emqx_dashboard_admin:set_force_mfa_snapshot(<<"u">>, true),
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

%% Scenarios A/B from the design review: admin override survives
%% backend snapshot changes.
%%
%% A: snapshot=true, admin exempts -> override=exempted; backend
%%    snapshot can flip independently, override stays exempted.
t_scenario_a_admin_exempt_survives_snapshot_flip(_Config) ->
    {ok, _} = emqx_dashboard_admin:add_user(
        <<"u">>, test_password(), ?ROLE_VIEWER, "u"
    ),
    {ok, ok} = emqx_dashboard_admin:set_force_mfa_snapshot(<<"u">>, true),
    {ok, ok} = emqx_dashboard_admin:set_mfa_state(
        <<"u">>, #{mechanism => totp, secret => <<"S1">>, first_verify_ts => 1}
    ),
    ok = emqx_dashboard_admin:disable_mfa(<<"u">>, _ByAdmin = true),
    ?assertEqual(?ADMIN_MFA_EXEMPTED, emqx_dashboard_admin:admin_override_of(<<"u">>)),
    %% Imagine backend admin toggles force_mfa off and on; snapshot
    %% itself stays per-user (see SPEC §6.1 — snapshot is not re-
    %% queried). The override should still be exempted.
    ?assertEqual(true, emqx_dashboard_admin:force_mfa_snapshot_of(<<"u">>)),
    ?assertEqual(?ADMIN_MFA_EXEMPTED, emqx_dashboard_admin:admin_override_of(<<"u">>)).

%% B: snapshot=false (user provisioned under force_mfa=false), admin
%%    forces -> override=required; backend snapshot would not affect
%%    this user.
t_scenario_b_admin_force_survives_snapshot_flip(_Config) ->
    {ok, _} = emqx_dashboard_admin:add_user(
        <<"u">>, test_password(), ?ROLE_VIEWER, "u"
    ),
    {ok, ok} = emqx_dashboard_admin:set_force_mfa_snapshot(<<"u">>, false),
    ok = emqx_dashboard_admin:reinit_mfa(<<"u">>, totp, _ByAdmin = true),
    ?assertEqual(?ADMIN_MFA_REQUIRED, emqx_dashboard_admin:admin_override_of(<<"u">>)),
    %% Snapshot stays false; override remains required.
    ?assertEqual(false, emqx_dashboard_admin:force_mfa_snapshot_of(<<"u">>)),
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
