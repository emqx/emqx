%%--------------------------------------------------------------------
%% Copyright (c) 2026-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
%%
%% Coverage for dashboard login user scope checking
%% (emqx_dashboard_rbac:check_login_user_scopes/2) and the MFA
%% authorization rules on both sides of the self/admin split.
%%
%% Self-MFA (`/current_user/mfa', emqx_dashboard_api:authorize_self_mfa/2):
%%
%%   first-time setup          => allow (deadlock prevention)
%%   admin_override = required => deny  MFA_LOCKED
%%   otherwise                 => allow
%%
%% Administrator MFA (`/users/:username/mfa'): global administrator
%% holding `mfa_management', target must be another user.
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

%% common_test enumerates test cases via Mod:all/0 — without it
%% `export_all' alone resolves to an empty case list and the suite
%% silently runs zero cases.
all() ->
    ?EE_ONLY(emqx_common_test_helpers:all(?MODULE), []).

-doc """
The default admin bootstrapped at boot holds no explicit scope list: it
follows the role default implicitly and keeps forward-compatible scopes.
Re-runs the same code path the boot sequence takes.
""".
t_default_admin_bootstrap_unset_scopes(_Config) ->
    {ok, _} = emqx_dashboard_admin:add_default_user(),
    Username = emqx_dashboard_admin:default_username(),
    ?assertEqual(undefined, emqx_dashboard_admin:scopes_of(Username)).

-doc """
A default admin carrying a frozen explicit scope list (seeded by an
earlier release at bootstrap) is healed back to unset on boot.
""".
t_default_admin_boot_clears_frozen_scopes(_Config) ->
    {ok, _} = emqx_dashboard_admin:add_default_user(),
    Username = emqx_dashboard_admin:default_username(),
    Frozen = emqx_dashboard_admin:role_default_scopes(?ROLE_SUPERUSER),
    {ok, ok} = emqx_dashboard_admin:set_user_scopes(Username, Frozen),
    ?assertEqual(Frozen, emqx_dashboard_admin:scopes_of(Username)),
    ?assertEqual({ok, default_user_exists}, emqx_dashboard_admin:add_default_user()),
    ?assertEqual(undefined, emqx_dashboard_admin:scopes_of(Username)).

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

%% Administrator can hold the three privilege login scopes together
%% (a privilege-only list is allowed). mfa_management is admin-only too
%% but is NOT a privilege scope, so it cannot share an explicit list
%% with them; it is exercised separately below and in
%% t_user_mfa_mgmt_not_privilege/1.
t_admin_can_hold_all_4_new_scopes(_Config) ->
    add_admin(<<"admin">>),
    Token = jwt(<<"admin">>, test_password()),
    PrivLoginScopes = ?ADMIN_ONLY_SCOPES -- [?SCOPE_MFA_MGMT],
    Body = #{
        <<"username">> => <<"admin2">>,
        <<"password">> => test_password(),
        <<"role">> => ?ROLE_SUPERUSER,
        <<"description">> => <<"test">>,
        <<"scopes">> => PrivLoginScopes
    },
    {ok, 200, _} = request_api(post, api_path(["users"]), auth_header(Token), Body),
    %% Verify it persisted
    [Admin] = emqx_dashboard_admin:lookup_user(<<"admin2">>),
    Stored = emqx_dashboard_admin:scopes_of(Admin#?ADMIN.username),
    ?assertEqual(lists:sort(PrivLoginScopes), lists:sort(Stored)),
    %% mfa_management can be held alongside a non-privilege scope.
    Body2 = Body#{
        <<"username">> => <<"admin3">>,
        <<"scopes">> => [?SCOPE_MFA_MGMT, ?SCOPE_CONNECTIONS]
    },
    {ok, 200, _} = request_api(post, api_path(["users"]), auth_header(Token), Body2).

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
        <<"scopes">> => [?SCOPE_USER_MGMT, ?SCOPE_API_KEY_MGMT]
    },
    {ok, 200, RespBody} = request_api(
        post, api_path(["users"]), auth_header(Token), Body
    ),
    Resp = emqx_utils_json:decode(RespBody),
    ?assertEqual([<<"user_management">>, <<"api_key_management">>], maps:get(<<"scopes">>, Resp)),
    ?assertEqual(<<"with_scopes">>, maps:get(<<"username">>, Resp)),
    ?assertEqual(?ROLE_SUPERUSER, maps:get(<<"role">>, Resp)).

%% Response from POST /users without an explicit `scopes' field
%% materialises the role-default scope list (administrator -> 10 common
%% + 4 login-only = 14 scopes). The legacy `<<"unset">>' sentinel is
%% reserved for records that survived an upgrade without scopes
%% (pre-#17235); fresh POSTs never produce that state.
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
    %% Response carries the materialised admin defaults (10 common + 4 login-only).
    CommonNames = [N || #{name := N} <- emqx_scope_catalog:common_scope_catalog()],
    LoginOnlyNames = [N || #{name := N} <- emqx_scope_catalog:admin_only_scope_catalog()],
    ExpectedScopes = lists:sort(CommonNames ++ LoginOnlyNames),
    ?assertEqual(ExpectedScopes, lists:sort(maps:get(<<"scopes">>, Resp))),
    %% Materialisation is real (persisted to mnesia), not a response-only
    %% projection — `effective_scopes_of/1' returns the same list.
    EffectiveScopes = emqx_dashboard_admin:effective_scopes_of(<<"no_scopes">>),
    ?assertEqual(ExpectedScopes, lists:sort(EffectiveScopes)).

%% Viewer default = the 10 common scopes, no login-only ones.
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
    %% Response carries the materialised viewer defaults (10 common, no login-only).
    CommonNames = [N || #{name := N} <- emqx_scope_catalog:common_scope_catalog()],
    ExpectedScopes = lists:sort(CommonNames),
    ?assertEqual(ExpectedScopes, lists:sort(maps:get(<<"scopes">>, Resp))),
    %% Viewer never gets login-only scopes via the role default.
    ?assertNot(lists:member(?SCOPE_USER_MGMT, maps:get(<<"scopes">>, Resp))),
    ?assertNot(lists:member(?SCOPE_MFA_MGMT, maps:get(<<"scopes">>, Resp))),
    %% Confirm persisted state matches.
    EffectiveScopes = emqx_dashboard_admin:effective_scopes_of(<<"viewer_no_scopes">>),
    ?assertEqual(ExpectedScopes, lists:sort(EffectiveScopes)).

%% POST without an explicit `scopes' field materialises the role-default
%% scope list and persists it (it is not merely a response-time
%% projection). This is what distinguishes a freshly-created user
%% from a legacy upgraded one.
t_post_users_materialises_default_scopes(_Config) ->
    add_admin(<<"admin">>),
    Token = jwt(<<"admin">>, test_password()),
    Body = #{
        <<"username">> => <<"materialise_admin">>,
        <<"password">> => test_password(),
        <<"role">> => ?ROLE_SUPERUSER,
        <<"description">> => <<"test">>
    },
    {ok, 200, _RespBody} = request_api(
        post, api_path(["users"]), auth_header(Token), Body
    ),
    %% Verify the raw mnesia extra map carries `scopes' (not absence,
    %% which would be the legacy state).
    [Admin] = mnesia:dirty_read(?ADMIN, <<"materialise_admin">>),
    Extra = element(6, Admin),
    ?assert(is_map(Extra)),
    ?assert(maps:is_key(scopes, Extra)),
    %% Persisted list equals the role default.
    CommonNames = [N || #{name := N} <- emqx_scope_catalog:common_scope_catalog()],
    LoginOnlyNames = [N || #{name := N} <- emqx_scope_catalog:admin_only_scope_catalog()],
    ExpectedScopes = lists:sort(CommonNames ++ LoginOnlyNames),
    ?assertEqual(ExpectedScopes, lists:sort(maps:get(scopes, Extra))).

%% Records created before the user scopes feature shipped have no
%% `scopes' key in their extra map. Such legacy records still need
%% a sensible response — the contract is to surface the binary
%% sentinel `<<"unset">>'. We simulate this by writing a record
%% directly into mnesia with a stripped extra map.
t_legacy_record_shows_unset_sentinel(_Config) ->
    add_admin(<<"admin">>),
    Token = jwt(<<"admin">>, test_password()),
    %% Create via the API then mutate mnesia to drop the scopes key.
    {ok, _} = emqx_dashboard_admin:add_user(
        <<"legacy_user">>, test_password(), ?ROLE_SUPERUSER, "u"
    ),
    {ok, _} = emqx_dashboard_admin:set_user_scopes(
        <<"legacy_user">>, [?SCOPE_USER_MGMT]
    ),
    [Record0] = mnesia:dirty_read(?ADMIN, <<"legacy_user">>),
    Extra0 = element(6, Record0),
    ExtraNoScopes = maps:remove(scopes, Extra0),
    Record1 = setelement(6, Record0, ExtraNoScopes),
    ok = mnesia:dirty_write(?ADMIN, Record1),
    %% Read it back through the API and verify the sentinel surfaces.
    {ok, 200, RespBody} = request_api(
        get, api_path(["users"]), auth_header(Token), []
    ),
    Resp = emqx_utils_json:decode(RespBody),
    [LegacyEntry] = [E || E <- Resp, maps:get(<<"username">>, E) =:= <<"legacy_user">>],
    ?assertEqual(<<"unset">>, maps:get(<<"scopes">>, LegacyEntry)).

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

%%--------------------------------------------------------------------
%% "unset"-equivalent write handling (issue #17931)
%%
%% The default administrator holds no explicit scopes (GET => "unset");
%% a read-modify-write from the dashboard round-trips GET's *expanded*
%% full catalog. The write paths must treat that expanded list (and the
%% `unset' sentinel) as "no explicit scopes" so a plain note edit does
%% not fail, and such users keep their forward-compatible implicit set
%% rather than a frozen explicit list.
%%--------------------------------------------------------------------

%% Editing only the note of the default admin while sending back GET's
%% expanded full catalog list succeeds (200) and does not sediment: GET
%% keeps returning "unset".
t_default_admin_note_only_edit_full_list(_Config) ->
    Default = emqx_dashboard_admin:default_username(),
    add_admin(Default),
    Token = jwt(Default, test_password()),
    FullList = emqx_dashboard_admin:role_default_scopes(?ROLE_SUPERUSER),
    PutBody = #{
        <<"role">> => ?ROLE_SUPERUSER,
        <<"description">> => <<"edited note">>,
        <<"scopes">> => FullList
    },
    {ok, 200, RespBody} = request_api(
        put, api_path(["users", binary_to_list(Default)]), auth_header(Token), PutBody
    ),
    Resp = emqx_utils_json:decode(RespBody),
    ?assertEqual(<<"edited note">>, maps:get(<<"description">>, Resp)),
    ?assertEqual(<<"unset">>, maps:get(<<"scopes">>, Resp)),
    ?assertEqual(undefined, emqx_dashboard_admin:scopes_of(Default)).

%% The default admin can round-trip the `unset' sentinel verbatim; it
%% stays unset.
t_default_admin_put_unset_sentinel(_Config) ->
    Default = emqx_dashboard_admin:default_username(),
    add_admin(Default),
    Token = jwt(Default, test_password()),
    PutBody = #{
        <<"role">> => ?ROLE_SUPERUSER,
        <<"description">> => <<"note">>,
        <<"scopes">> => <<"unset">>
    },
    {ok, 200, RespBody} = request_api(
        put, api_path(["users", binary_to_list(Default)]), auth_header(Token), PutBody
    ),
    Resp = emqx_utils_json:decode(RespBody),
    ?assertEqual(<<"unset">>, maps:get(<<"scopes">>, Resp)),
    ?assertEqual(undefined, emqx_dashboard_admin:scopes_of(Default)).

%% The role-default equivalence is order-insensitive: the full catalog
%% sent in a shuffled order is still recognised as "unset".
t_default_admin_put_full_list_shuffled(_Config) ->
    Default = emqx_dashboard_admin:default_username(),
    add_admin(Default),
    Token = jwt(Default, test_password()),
    Shuffled = lists:reverse(emqx_dashboard_admin:role_default_scopes(?ROLE_SUPERUSER)),
    PutBody = #{
        <<"role">> => ?ROLE_SUPERUSER,
        <<"description">> => <<"note">>,
        <<"scopes">> => Shuffled
    },
    {ok, 200, RespBody} = request_api(
        put, api_path(["users", binary_to_list(Default)]), auth_header(Token), PutBody
    ),
    Resp = emqx_utils_json:decode(RespBody),
    ?assertEqual(<<"unset">>, maps:get(<<"scopes">>, Resp)),
    ?assertEqual(undefined, emqx_dashboard_admin:scopes_of(Default)).

%% A genuinely different explicit list (a strict subset of the catalog)
%% for the default admin is still rejected with 400 NOT_ALLOWED.
t_default_admin_put_different_list_rejected(_Config) ->
    Default = emqx_dashboard_admin:default_username(),
    add_admin(Default),
    Token = jwt(Default, test_password()),
    PutBody = #{
        <<"role">> => ?ROLE_SUPERUSER,
        <<"description">> => <<"note">>,
        <<"scopes">> => [?SCOPE_USER_MGMT]
    },
    ?assertMatch(
        {ok, 400, _},
        request_api(
            put, api_path(["users", binary_to_list(Default)]), auth_header(Token), PutBody
        )
    ),
    %% Storage untouched — still no explicit scopes.
    ?assertEqual(undefined, emqx_dashboard_admin:scopes_of(Default)).

%% Changing the default admin's role away from administrator is still
%% rejected (unchanged break-glass protection).
t_default_admin_role_change_rejected(_Config) ->
    Default = emqx_dashboard_admin:default_username(),
    add_admin(Default),
    Token = jwt(Default, test_password()),
    PutBody = #{
        <<"role">> => ?ROLE_VIEWER,
        <<"description">> => <<"note">>
    },
    ?assertMatch(
        {ok, 400, _},
        request_api(
            put, api_path(["users", binary_to_list(Default)]), auth_header(Token), PutBody
        )
    ).

%% A regular (non-default) user with unset scopes round-tripping the
%% expanded full catalog stays unset — the implicit set is not sedimented.
t_regular_user_round_trip_full_list_stays_unset(_Config) ->
    add_admin(<<"admin">>),
    Token = jwt(<<"admin">>, test_password()),
    {ok, _} = emqx_dashboard_admin:add_user(
        <<"reguser">>, test_password(), ?ROLE_SUPERUSER, "u"
    ),
    ?assertEqual(undefined, emqx_dashboard_admin:scopes_of(<<"reguser">>)),
    FullList = emqx_dashboard_admin:role_default_scopes(?ROLE_SUPERUSER),
    PutBody = #{
        <<"role">> => ?ROLE_SUPERUSER,
        <<"description">> => <<"note">>,
        <<"scopes">> => FullList
    },
    {ok, 200, RespBody} = request_api(
        put, api_path(["users", "reguser"]), auth_header(Token), PutBody
    ),
    Resp = emqx_utils_json:decode(RespBody),
    ?assertEqual(<<"unset">>, maps:get(<<"scopes">>, Resp)),
    ?assertEqual(undefined, emqx_dashboard_admin:scopes_of(<<"reguser">>)).

%% PUT `scopes: "unset"' on a regular user clears a previously-explicit
%% list back to the unset state.
t_regular_user_put_unset_clears_explicit(_Config) ->
    add_admin(<<"admin">>),
    Token = jwt(<<"admin">>, test_password()),
    {ok, _} = emqx_dashboard_admin:add_user(
        <<"reguser2">>, test_password(), ?ROLE_SUPERUSER, "u"
    ),
    {ok, ok} = emqx_dashboard_admin:set_user_scopes(<<"reguser2">>, [?SCOPE_MFA_MGMT]),
    ?assertEqual([?SCOPE_MFA_MGMT], emqx_dashboard_admin:scopes_of(<<"reguser2">>)),
    PutBody = #{
        <<"role">> => ?ROLE_SUPERUSER,
        <<"description">> => <<"note">>,
        <<"scopes">> => <<"unset">>
    },
    {ok, 200, RespBody} = request_api(
        put, api_path(["users", "reguser2"]), auth_header(Token), PutBody
    ),
    Resp = emqx_utils_json:decode(RespBody),
    ?assertEqual(<<"unset">>, maps:get(<<"scopes">>, Resp)),
    ?assertEqual(undefined, emqx_dashboard_admin:scopes_of(<<"reguser2">>)).

%% POST create with `scopes: "unset"' creates the user with no explicit
%% scopes (GET returns "unset").
t_post_create_unset_sentinel(_Config) ->
    add_admin(<<"admin">>),
    Token = jwt(<<"admin">>, test_password()),
    Body = #{
        <<"username">> => <<"created_unset">>,
        <<"password">> => test_password(),
        <<"role">> => ?ROLE_SUPERUSER,
        <<"description">> => <<"u">>,
        <<"scopes">> => <<"unset">>
    },
    {ok, 200, RespBody} = request_api(
        post, api_path(["users"]), auth_header(Token), Body
    ),
    Resp = emqx_utils_json:decode(RespBody),
    ?assertEqual(<<"unset">>, maps:get(<<"scopes">>, Resp)),
    ?assertEqual(undefined, emqx_dashboard_admin:scopes_of(<<"created_unset">>)).

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

%% Viewer cannot hold mfa_management: the scope means "manage another
%% user's MFA" and is administrator-only. Its former non-administrator
%% meaning (a self-exemption key) is gone -- managing one's own MFA is
%% an identity-authorized operation on /current_user/mfa and needs no
%% scope.
t_viewer_cannot_hold_mfa_management(_Config) ->
    add_admin(<<"admin">>),
    Token = jwt(<<"admin">>, test_password()),
    Body = #{
        <<"username">> => <<"v">>,
        <<"password">> => test_password(),
        <<"role">> => ?ROLE_VIEWER,
        <<"description">> => <<"test">>,
        <<"scopes">> => [?SCOPE_MFA_MGMT]
    },
    {ok, 400, RespBody} = request_api(
        post, api_path(["users"]), auth_header(Token), Body
    ),
    ?assertMatch(
        #{<<"message">> := <<"Non-administrator users cannot hold admin-only scopes:", _/binary>>},
        emqx_utils_json:decode(RespBody)
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
%% Namespaced administrator scope validation
%%
%% A namespaced administrator (role = "ns:test::administrator")
%% receives a restricted scope subset (common + login that are
%% useful within a namespace). Before the fix in
%% validate_role_scope_compat/2 and role_default_scopes/1, the raw
%% role string "ns:test::administrator" did not pattern-match
%% ?ROLE_SUPERUSER (<<"administrator">>), causing the handler to
%% treat namespaced admins as non-administrators.
%%--------------------------------------------------------------------

-define(NS_ADMIN_USER, <<"ns_admin">>).
-define(NS_CONTROL_USER, <<"ns_admin_control">>).
-define(NS_ROLE, <<"ns:test::administrator">>).

%% POST a namespaced administrator with an allowed scope
%% (api_key_management) — must succeed.
t_ns_admin_can_hold_allowed_scopes(_Config) ->
    add_admin(<<"admin">>),
    Token = jwt(<<"admin">>, test_password()),
    Body = #{
        <<"username">> => ?NS_ADMIN_USER,
        <<"password">> => test_password(),
        <<"role">> => ?NS_ROLE,
        <<"description">> => <<"ns admin">>,
        <<"scopes">> => [?SCOPE_API_KEY_MGMT]
    },
    ?assertMatch(
        {ok, 200, _},
        request_api(post, api_path(["users"]), auth_header(Token), Body)
    ),
    [Admin] = emqx_dashboard_admin:lookup_user(?NS_ADMIN_USER),
    Stored = emqx_dashboard_admin:scopes_of(Admin#?ADMIN.username),
    ?assert(lists:member(?SCOPE_API_KEY_MGMT, Stored)),
    %% The stored role is the parsed base role (namespace goes into extra).
    ?assertEqual(?ROLE_SUPERUSER, Admin#?ADMIN.role).

%% POST a namespaced administrator without an explicit `scopes'
%% field — must materialise the restricted role defaults
%% (7 common + 2 login-only), not the global-admin full set.
t_ns_admin_gets_restricted_role_default_scopes(_Config) ->
    add_admin(<<"admin">>),
    Token = jwt(<<"admin">>, test_password()),
    Body = #{
        <<"username">> => ?NS_CONTROL_USER,
        <<"password">> => test_password(),
        <<"role">> => ?NS_ROLE,
        <<"description">> => <<"ns admin no scopes">>
    },
    {ok, 200, _} = request_api(
        post, api_path(["users"]), auth_header(Token), Body
    ),
    EffectiveScopes = emqx_dashboard_admin:effective_scopes_of(?NS_CONTROL_USER),
    %% Must have the restricted subset (7 common).
    ?assert(lists:member(?SCOPE_CONNECTIONS, EffectiveScopes)),
    ?assert(lists:member(?SCOPE_MONITORING, EffectiveScopes)),
    ?assert(lists:member(?SCOPE_DATA_INTEGRATION, EffectiveScopes)),
    ?assert(lists:member(?SCOPE_ACCESS_CONTROL, EffectiveScopes)),
    ?assert(lists:member(?SCOPE_SYSTEM, EffectiveScopes)),
    %% Read-only cluster info (e.g. `GET /nodes`) and license info
    %% (`GET /license*`).  RBAC denies the mutating endpoints in both
    %% groups for namespaced callers.
    ?assert(lists:member(?SCOPE_CLUSTER_OPERATIONS, EffectiveScopes)),
    ?assert(lists:member(?SCOPE_LICENSE, EffectiveScopes)),
    %% Must have the two allowed login-only scopes.
    ?assert(lists:member(?SCOPE_USER_MGMT, EffectiveScopes)),
    ?assert(lists:member(?SCOPE_API_KEY_MGMT, EffectiveScopes)),
    %% Must NOT have gateways, audit, publish.
    ?assertNot(lists:member(?SCOPE_GATEWAYS, EffectiveScopes)),
    ?assertNot(lists:member(?SCOPE_PUBLISH, EffectiveScopes)),
    ?assertNot(lists:member(?SCOPE_AUDIT, EffectiveScopes)),
    %% Must NOT have mfa, sso login-only scopes.
    ?assertNot(lists:member(?SCOPE_MFA_MGMT, EffectiveScopes)),
    ?assertNot(lists:member(?SCOPE_SSO_MGMT, EffectiveScopes)),
    %% Exact count: 9 scopes (7 common + 2 login-only).
    ?assertEqual(9, length(EffectiveScopes)).

%% PUT a namespaced administrator with only the description field
%% updated (role + scopes unchanged).  The persisted scopes are the
%% ns-admin default (restricted subset), which the new validation
%% must accept.
t_ns_admin_update_description_succeeds(_Config) ->
    add_admin(<<"admin">>),
    Token = jwt(<<"admin">>, test_password()),
    {ok, _} = emqx_dashboard_admin:add_user(?NS_ADMIN_USER, test_password(), ?NS_ROLE, "ns admin"),
    {ok, _} = emqx_dashboard_admin:set_user_scopes(
        ?NS_ADMIN_USER,
        ?NS_ADMIN_ALLOWED_SCOPES
    ),
    PutBody = #{
        <<"role">> => ?NS_ROLE,
        <<"description">> => <<"updated description">>
    },
    ?assertMatch(
        {ok, 200, _},
        request_api(
            put,
            api_path(["users", binary_to_list(?NS_ADMIN_USER)]),
            auth_header(Token),
            PutBody
        )
    ).

%% PUT a namespaced administrator with allowed scopes explicitly
%% in the body — the restricted subset must be accepted.
t_ns_admin_can_be_assigned_allowed_scopes_via_put(_Config) ->
    add_admin(<<"admin">>),
    Token = jwt(<<"admin">>, test_password()),
    {ok, _} = emqx_dashboard_admin:add_user(?NS_ADMIN_USER, test_password(), ?NS_ROLE, "ns admin"),
    PutBody = #{
        <<"role">> => ?NS_ROLE,
        <<"description">> => <<"scoped ns admin">>,
        <<"scopes">> => [?SCOPE_USER_MGMT, ?SCOPE_API_KEY_MGMT, ?SCOPE_CONNECTIONS]
    },
    {ok, 200, RespBody} = request_api(
        put,
        api_path(["users", binary_to_list(?NS_ADMIN_USER)]),
        auth_header(Token),
        PutBody
    ),
    Resp = emqx_utils_json:decode(RespBody),
    ?assertEqual(
        [<<"user_management">>, <<"api_key_management">>, <<"connections">>],
        maps:get(<<"scopes">>, Resp)
    ),
    Stored = emqx_dashboard_admin:scopes_of(?NS_ADMIN_USER),
    ?assertEqual(3, length(Stored)).

%% Namespaced administrator can hold system scope — RBAC explicitly
%% allows ns admins on data_backup (export/import/list), which is
%% scoped under ?SCOPE_SYSTEM. RBAC remains the primary gate for
%% other system endpoints.
t_ns_admin_can_hold_system_scope(_Config) ->
    add_admin(<<"admin">>),
    Token = jwt(<<"admin">>, test_password()),
    Body = #{
        <<"username">> => <<"ns_system">>,
        <<"password">> => test_password(),
        <<"role">> => ?NS_ROLE,
        <<"description">> => <<"ns admin with system">>,
        <<"scopes">> => [?SCOPE_SYSTEM]
    },
    ?assertMatch(
        {ok, 200, _},
        request_api(post, api_path(["users"]), auth_header(Token), Body)
    ).

%% Namespaced administrator cannot hold mfa_management — MFA control
%% is global-admin territory.
t_ns_admin_cannot_hold_mfa_management(_Config) ->
    add_admin(<<"admin">>),
    Token = jwt(<<"admin">>, test_password()),
    Body = #{
        <<"username">> => <<"ns_mfa">>,
        <<"password">> => test_password(),
        <<"role">> => ?NS_ROLE,
        <<"description">> => <<"ns admin with mfa">>,
        <<"scopes">> => [?SCOPE_MFA_MGMT]
    },
    ?assertMatch(
        {ok, 400, _},
        request_api(post, api_path(["users"]), auth_header(Token), Body)
    ).

%% Viewer still cannot hold admin-only scopes — regression test
%% proving the fix didn't weaken the non-admin guard.
t_viewer_still_cannot_hold_api_key_management(_Config) ->
    add_admin(<<"admin">>),
    Token = jwt(<<"admin">>, test_password()),
    Body = #{
        <<"username">> => <<"v">>,
        <<"password">> => test_password(),
        <<"role">> => ?ROLE_VIEWER,
        <<"description">> => <<"test">>,
        <<"scopes">> => [?SCOPE_API_KEY_MGMT]
    },
    ?assertMatch(
        {ok, 400, _},
        request_api(post, api_path(["users"]), auth_header(Token), Body)
    ).

%%--------------------------------------------------------------------
%% Privilege scope mutual-exclusion (POST /users, PUT /users/:name)
%%
%% The four privilege scopes (system, user_management,
%% api_key_management, sso_management) are administrator-equivalent in
%% effect. An explicit, non-empty scope list must be either entirely
%% privilege or entirely non-privilege — mixing them is rejected with
%% 400. Empty / omitted lists are the legacy unrestricted / deny-all
%% cases and pass through unchanged. Namespaced admins are exempt (RBAC
%% is their authoritative gate).
%%--------------------------------------------------------------------

-define(MUTEX_MSG, <<"Privilege scopes cannot be combined with other scopes">>).

%% Privilege-only scope lists are accepted for a global administrator.
t_user_privilege_only_lists_pass(_Config) ->
    add_admin(<<"admin">>),
    Token = jwt(<<"admin">>, test_password()),
    Lists = [
        [?SCOPE_SYSTEM],
        [?SCOPE_USER_MGMT],
        [?SCOPE_API_KEY_MGMT],
        [?SCOPE_SSO_MGMT],
        [?SCOPE_SYSTEM, ?SCOPE_USER_MGMT, ?SCOPE_API_KEY_MGMT, ?SCOPE_SSO_MGMT]
    ],
    lists:foreach(fun(Scopes) -> assert_create_user_ok(Token, Scopes) end, Lists).

%% Non-privilege-only scope lists are accepted.
t_user_nonprivilege_only_lists_pass(_Config) ->
    add_admin(<<"admin">>),
    Token = jwt(<<"admin">>, test_password()),
    Lists = [
        [?SCOPE_CONNECTIONS],
        [?SCOPE_CONNECTIONS, ?SCOPE_PUBLISH, ?SCOPE_MONITORING],
        [?SCOPE_AUDIT, ?SCOPE_LICENSE]
    ],
    lists:foreach(fun(Scopes) -> assert_create_user_ok(Token, Scopes) end, Lists).

%% Mixing any privilege scope with any non-privilege scope is rejected.
t_user_mixed_lists_rejected(_Config) ->
    add_admin(<<"admin">>),
    Token = jwt(<<"admin">>, test_password()),
    Lists = [
        [?SCOPE_SYSTEM, ?SCOPE_CONNECTIONS],
        [?SCOPE_USER_MGMT, ?SCOPE_PUBLISH],
        [?SCOPE_API_KEY_MGMT, ?SCOPE_MONITORING],
        [?SCOPE_SSO_MGMT, ?SCOPE_CONNECTIONS],
        [?SCOPE_SYSTEM, ?SCOPE_USER_MGMT, ?SCOPE_CONNECTIONS]
    ],
    lists:foreach(fun(Scopes) -> assert_create_user_mutex_400(Token, Scopes) end, Lists).

%% Omitted / empty scope lists are not "explicit mixed lists" — pass.
t_user_non_explicit_lists_pass(_Config) ->
    add_admin(<<"admin">>),
    Token = jwt(<<"admin">>, test_password()),
    %% scopes field omitted -> role-default fallback
    Body0 = #{
        <<"username">> => <<"u_omitted">>,
        <<"password">> => test_password(),
        <<"role">> => ?ROLE_SUPERUSER,
        <<"description">> => <<"test">>
    },
    ?assertMatch(
        {ok, 200, _},
        request_api(post, api_path(["users"]), auth_header(Token), Body0)
    ),
    %% explicit empty list -> deny-all, not mixed
    assert_create_user_ok(Token, []).

%% mfa_management is NOT a privilege scope: it may sit beside
%% non-privilege scopes, but not beside a privilege scope.
t_user_mfa_mgmt_not_privilege(_Config) ->
    add_admin(<<"admin">>),
    Token = jwt(<<"admin">>, test_password()),
    assert_create_user_ok(Token, [?SCOPE_MFA_MGMT, ?SCOPE_CONNECTIONS]),
    assert_create_user_mutex_400(Token, [?SCOPE_MFA_MGMT, ?SCOPE_SYSTEM]).

%% Namespaced admins are exempt from the mutex — RBAC gates the surface.
t_ns_admin_exempt_from_privilege_mutex(_Config) ->
    add_admin(<<"admin">>),
    Token = jwt(<<"admin">>, test_password()),
    Body = #{
        <<"username">> => <<"ns_mixed">>,
        <<"password">> => test_password(),
        <<"role">> => ?NS_ROLE,
        <<"description">> => <<"ns admin mixed">>,
        <<"scopes">> => [?SCOPE_SYSTEM, ?SCOPE_CONNECTIONS]
    },
    ?assertMatch(
        {ok, 200, _},
        request_api(post, api_path(["users"]), auth_header(Token), Body)
    ).

%% Regression: the namespaced-admin scope-compat check still runs
%% (a scope outside NS_ADMIN_ALLOWED_SCOPES is rejected) even though
%% the mutex is skipped for namespaced admins.
t_ns_admin_still_rejects_forbidden_scope(_Config) ->
    add_admin(<<"admin">>),
    Token = jwt(<<"admin">>, test_password()),
    Body = #{
        <<"username">> => <<"ns_forbidden">>,
        <<"password">> => test_password(),
        <<"role">> => ?NS_ROLE,
        <<"description">> => <<"ns admin forbidden">>,
        <<"scopes">> => [?SCOPE_GATEWAYS]
    },
    ?assertMatch(
        {ok, 400, _},
        request_api(post, api_path(["users"]), auth_header(Token), Body)
    ).

%% Update path: PUT rejects a mixed list and accepts a privilege-only
%% list for the same user.
t_user_update_privilege_mutex(_Config) ->
    add_admin(<<"admin">>),
    Token = jwt(<<"admin">>, test_password()),
    {ok, _} = emqx_dashboard_admin:add_user(
        <<"u_put">>, test_password(), ?ROLE_SUPERUSER, "test"
    ),
    MixedBody = #{
        <<"role">> => ?ROLE_SUPERUSER,
        <<"description">> => <<"mixed">>,
        <<"scopes">> => [?SCOPE_SYSTEM, ?SCOPE_CONNECTIONS]
    },
    ?assertMatch(
        {ok, 400, _},
        request_api(put, api_path(["users", "u_put"]), auth_header(Token), MixedBody)
    ),
    OkBody = MixedBody#{<<"scopes">> => [?SCOPE_SYSTEM]},
    ?assertMatch(
        {ok, 200, _},
        request_api(put, api_path(["users", "u_put"]), auth_header(Token), OkBody)
    ).

%% Legacy record with a mixed scope set (written before this rule
%% existed): reads are unaffected; the next update with the same mixed
%% list is rejected; an update that splits the list succeeds.
t_user_legacy_mixed_record(_Config) ->
    add_admin(<<"admin">>),
    Token = jwt(<<"admin">>, test_password()),
    {ok, _} = emqx_dashboard_admin:add_user(
        <<"u_legacy">>, test_password(), ?ROLE_SUPERUSER, "legacy"
    ),
    Mixed = [?SCOPE_SYSTEM, ?SCOPE_CONNECTIONS],
    {ok, _} = emqx_dashboard_admin:set_user_scopes(<<"u_legacy">>, Mixed),
    ?assertEqual(Mixed, emqx_dashboard_admin:scopes_of(<<"u_legacy">>)),
    %% Read is unaffected.
    ?assertMatch(
        {ok, 200, _},
        request_api(get, api_path(["users"]), auth_header(Token))
    ),
    %% Re-submitting the same mixed list on update is rejected.
    MixedBody = #{
        <<"role">> => ?ROLE_SUPERUSER,
        <<"description">> => <<"still mixed">>,
        <<"scopes">> => Mixed
    },
    ?assertMatch(
        {ok, 400, _},
        request_api(put, api_path(["users", "u_legacy"]), auth_header(Token), MixedBody)
    ),
    %% Splitting the list succeeds.
    SplitBody = MixedBody#{<<"scopes">> => [?SCOPE_CONNECTIONS]},
    ?assertMatch(
        {ok, 200, _},
        request_api(put, api_path(["users", "u_legacy"]), auth_header(Token), SplitBody)
    ),
    ?assertEqual([?SCOPE_CONNECTIONS], emqx_dashboard_admin:scopes_of(<<"u_legacy">>)).

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
    %% A generic scope is allowed for any role, so the persisted scope
    %% remains compatible after demotion.
    {ok, ok} = emqx_dashboard_admin:set_user_scopes(<<"u">>, [?SCOPE_CONNECTIONS]),
    PutBody = #{
        <<"role">> => ?ROLE_VIEWER,
        <<"description">> => <<"demoted">>
    },
    {ok, 200, _} =
        request_api(put, api_path(["users", "u"]), auth_header(Token), PutBody),
    [Admin] = emqx_dashboard_admin:lookup_user(<<"u">>),
    ?assertEqual(?ROLE_VIEWER, Admin#?ADMIN.role).

%%--------------------------------------------------------------------
%% Self-MFA policy (`/current_user/mfa')
%%
%% What used to be a seven-row matrix over {IsFirstSetup, IsSelf,
%% HasMfaMgmt, Locked, CallerRole} is now two rules, because the caller
%% and the subject are the same identity on these routes:
%%
%%   first-time setup          => allow  (deadlock prevention)
%%   admin_override = required => deny   (MFA_LOCKED)
%%   otherwise                 => allow
%%
%% `mfa_management' no longer acts as a self-exemption key: it is an
%% administrator-only scope meaning "manage OTHER users' MFA". The exits
%% from a locked account are an administrator exemption and the CLI.
%%--------------------------------------------------------------------

%% First-time setup is always allowed, regardless of the lock. Without
%% this a user under an active mandate could never enrol (deadlock).
t_first_time_setup_always_allowed(_Config) ->
    add_admin(<<"admin">>),
    {ok, _} = emqx_dashboard_admin:add_user(
        <<"u">>, test_password(), ?ROLE_VIEWER, "u"
    ),
    %% Force the lock state -- it would block a rotate, but mfa_state is
    %% absent (not_configured), so first-time setup short-circuits it.
    {ok, ok} = emqx_dashboard_admin:set_admin_override(<<"u">>, ?ADMIN_MFA_REQUIRED),
    Token = jwt(<<"u">>, test_password()),
    ?assertMatch({ok, 204, _}, setup_own_mfa(Token)).

%% Not locked: a user may rotate its own MFA.
t_self_can_rotate_when_not_locked(_Config) ->
    add_admin(<<"admin">>),
    {ok, _} = emqx_dashboard_admin:add_user(
        <<"u">>, test_password(), ?ROLE_VIEWER, "u"
    ),
    {ok, ok} = emqx_dashboard_admin:set_mfa_state(
        <<"u">>, #{mechanism => totp, secret => <<"S1">>, first_verify_ts => 1}
    ),
    Token = jwt(<<"u">>, test_password()),
    ?assertMatch({ok, 204, _}, setup_own_mfa(Token)).

%% Locked by an administrator decision: rotate is denied.
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
    {ok, 403, RespBody} = setup_own_mfa(Token),
    ?assertEqual(<<"MFA_LOCKED">>, error_code(RespBody)).

%% Locked by an administrator decision: self-disable is denied too.
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
    {ok, 403, RespBody} = delete_own_mfa(Token),
    ?assertEqual(<<"MFA_LOCKED">>, error_code(RespBody)).

%% `mfa_management' is no longer a self-exemption key. It is now an
%% administrator-only scope, so the holder must be an administrator --
%% and even then it does not unlock their own account. Replaces the two
%% former "self with mfa_management can rotate/delete under lock" rows.
t_self_with_mfa_mgmt_still_locked(_Config) ->
    add_admin(<<"admin">>),
    {ok, _} = emqx_dashboard_admin:add_user(
        <<"u">>, test_password(), ?ROLE_SUPERUSER, "u"
    ),
    {ok, ok} = emqx_dashboard_admin:set_user_scopes(<<"u">>, [?SCOPE_MFA_MGMT]),
    {ok, ok} = emqx_dashboard_admin:set_mfa_state(
        <<"u">>, #{mechanism => totp, secret => <<"S1">>, first_verify_ts => 1}
    ),
    {ok, ok} = emqx_dashboard_admin:set_admin_override(<<"u">>, ?ADMIN_MFA_REQUIRED),
    Token = jwt(<<"u">>, test_password()),
    {ok, 403, RotateBody} = setup_own_mfa(Token),
    ?assertEqual(<<"MFA_LOCKED">>, error_code(RotateBody)),
    {ok, 403, DeleteBody} = delete_own_mfa(Token),
    ?assertEqual(<<"MFA_LOCKED">>, error_code(DeleteBody)).

%% A viewer with an explicitly emptied scope list still reaches its own
%% MFA: self-service is not scope-gated.
t_self_mfa_not_gated_by_scopes(_Config) ->
    add_admin(<<"admin">>),
    {ok, _} = emqx_dashboard_admin:add_user(
        <<"u">>, test_password(), ?ROLE_VIEWER, "u"
    ),
    {ok, ok} = emqx_dashboard_admin:set_user_scopes(<<"u">>, []),
    ?assertMatch({ok, 204, _}, setup_own_mfa(jwt(<<"u">>, test_password()))),
    %% Re-keying MFA invalidates the account's sessions, so the disable
    %% needs a fresh token.
    ?assertMatch({ok, 204, _}, delete_own_mfa(jwt(<<"u">>, test_password()))).

%%--------------------------------------------------------------------
%% Administrator MFA routes (`/users/:username/mfa')
%%--------------------------------------------------------------------

%% An administrator resets another user's MFA. The role default gives
%% an administrator `mfa_management' implicitly.
t_admin_can_reset_others_mfa(_Config) ->
    add_admin(<<"admin">>),
    {ok, _} = emqx_dashboard_admin:add_user(
        <<"u">>, test_password(), ?ROLE_VIEWER, "u"
    ),
    {ok, ok} = emqx_dashboard_admin:set_mfa_state(
        <<"u">>, #{mechanism => totp, secret => <<"S1">>, first_verify_ts => 1}
    ),
    Token = jwt(<<"admin">>, test_password()),
    ?assertMatch({ok, 204, _}, admin_delete_mfa(Token, <<"u">>)).

%% The admin routes manage OTHER users only. An administrator aiming
%% them at its own account is refused and pointed at /current_user/mfa,
%% so a self-change cannot be laundered into an `admin_override' write.
t_admin_cannot_target_self_on_admin_route(_Config) ->
    add_admin(<<"admin">>),
    Token = jwt(<<"admin">>, test_password()),
    {ok, 400, PostBody} = admin_setup_mfa(Token, <<"admin">>),
    ?assertEqual(<<"NOT_ALLOWED">>, error_code(PostBody)),
    {ok, 400, DeleteBody} = admin_delete_mfa(Token, <<"admin">>),
    ?assertEqual(<<"NOT_ALLOWED">>, error_code(DeleteBody)),
    %% The refusal is a guard, not a side effect: no administrator
    %% decision was recorded against the account.
    ?assertEqual(undefined, emqx_dashboard_admin:admin_override_of(<<"admin">>)).

%% End-to-end: an administrator exemption is the way out of a locked
%% account. Admin disables (writes mfa_exempted), the user may then
%% enrol again through its own route.
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
    ?assertMatch({ok, 204, _}, admin_delete_mfa(AdminToken, <<"u">>)),
    ?assertEqual(?ADMIN_MFA_EXEMPTED, emqx_dashboard_admin:admin_override_of(<<"u">>)),
    UserToken = jwt(<<"u">>, test_password()),
    ?assertMatch({ok, 204, _}, setup_own_mfa(UserToken)).

%% End-to-end: an administrator reset locks the account against
%% self-disable, which is what `default_mfa' enforcement rests on.
t_admin_force_locks_user_against_self_disable(_Config) ->
    add_admin(<<"admin">>),
    {ok, _} = emqx_dashboard_admin:add_user(
        <<"u">>, test_password(), ?ROLE_VIEWER, "u"
    ),
    {ok, ok} = emqx_dashboard_admin:set_admin_override(<<"u">>, undefined),
    AdminToken = jwt(<<"admin">>, test_password()),
    ?assertMatch({ok, 204, _}, admin_setup_mfa(AdminToken, <<"u">>)),
    ?assertEqual(?ADMIN_MFA_REQUIRED, emqx_dashboard_admin:admin_override_of(<<"u">>)),
    %% Make MFA actually enabled (post-verify); set state directly to
    %% bypass the verify step.
    {ok, ok} = emqx_dashboard_admin:set_mfa_state(
        <<"u">>, #{mechanism => totp, secret => <<"S2">>, first_verify_ts => 1}
    ),
    UserToken = jwt(<<"u">>, test_password()),
    {ok, 403, RespBody} = delete_own_mfa(UserToken),
    ?assertEqual(<<"MFA_LOCKED">>, error_code(RespBody)).

%% A non-administrator cannot reach the admin MFA route at all, with or
%% without an explicit scope list. RBAC rejects it before any policy
%% check runs, so the assertion is on the status, not the error code.
t_viewer_cannot_reset_other_users_mfa(_Config) ->
    add_admin(<<"admin">>),
    {ok, _} = emqx_dashboard_admin:add_user(
        <<"v1">>, test_password(), ?ROLE_VIEWER, "v1"
    ),
    {ok, _} = emqx_dashboard_admin:add_user(
        <<"v2">>, test_password(), ?ROLE_VIEWER, "v2"
    ),
    {ok, ok} = emqx_dashboard_admin:set_mfa_state(
        <<"v2">>, #{mechanism => totp, secret => <<"S1">>, first_verify_ts => 1}
    ),
    Token = jwt(<<"v1">>, test_password()),
    ?assertMatch({ok, 403, _}, admin_delete_mfa(Token, <<"v2">>)),
    ?assertMatch({ok, 403, _}, admin_setup_mfa(Token, <<"v2">>)).

%% A namespaced administrator is denied on another user's MFA even
%% inside its own namespace -- the cross-user reset vector stays closed.
t_ns_admin_cannot_reset_other_users_mfa(_Config) ->
    add_admin(<<"admin">>),
    {ok, _} = emqx_dashboard_admin:add_user(
        <<"nsadmin">>, test_password(), <<"ns:ns1::", ?ROLE_SUPERUSER/binary>>, "ns"
    ),
    {ok, _} = emqx_dashboard_admin:add_user(
        <<"victim">>, test_password(), ?ROLE_VIEWER, "victim"
    ),
    {ok, ok} = emqx_dashboard_admin:set_mfa_state(
        <<"victim">>, #{mechanism => totp, secret => <<"S1">>, first_verify_ts => 1}
    ),
    Token = jwt(<<"nsadmin">>, test_password()),
    ?assertMatch({ok, 403, _}, admin_delete_mfa(Token, <<"victim">>)),
    ?assertMatch({ok, 403, _}, admin_setup_mfa(Token, <<"victim">>)),
    %% ... and its own account is reachable only through /current_user.
    ?assertMatch({ok, 403, _}, admin_delete_mfa(Token, <<"nsadmin">>)),
    ?assertMatch({ok, 204, _}, setup_own_mfa(Token)).

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

%% Create a fresh administrator user with the given scope list and
%% assert the request succeeds. A unique username is generated per call
%% so the same test can exercise several scope lists.
assert_create_user_ok(Token, Scopes) ->
    Body = create_user_body(Scopes),
    ?assertMatch(
        {ok, 200, _},
        request_api(post, api_path(["users"]), auth_header(Token), Body)
    ).

%% Like assert_create_user_ok/2 but asserts a 400 carrying the
%% privilege-scope mutex message.
assert_create_user_mutex_400(Token, Scopes) ->
    Body = create_user_body(Scopes),
    {ok, 400, RespBody} =
        request_api(post, api_path(["users"]), auth_header(Token), Body),
    ?assertMatch({_, _}, binary:match(RespBody, ?MUTEX_MSG)).

create_user_body(Scopes) ->
    N = erlang:integer_to_binary(erlang:unique_integer([positive, monotonic])),
    #{
        <<"username">> => <<"u_mutex_", N/binary>>,
        <<"password">> => test_password(),
        <<"role">> => ?ROLE_SUPERUSER,
        <<"description">> => <<"mutex test">>,
        <<"scopes">> => Scopes
    }.

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

%% Self-service requests: no username in the path, the bearer token is
%% the whole subject.
setup_own_mfa(Token) ->
    request_api(
        post,
        api_path(["current_user", "mfa"]),
        auth_header(Token),
        #{<<"mechanism">> => <<"totp">>}
    ).

delete_own_mfa(Token) ->
    request_api(delete, api_path(["current_user", "mfa"]), auth_header(Token), #{}).

%% Administrator requests against another user's account.
admin_setup_mfa(Token, TargetUsername) ->
    request_api(
        post,
        api_path(["users", binary_to_list(TargetUsername), "mfa"]),
        auth_header(Token),
        #{<<"mechanism">> => <<"totp">>}
    ).

admin_delete_mfa(Token, TargetUsername) ->
    request_api(
        delete,
        api_path(["users", binary_to_list(TargetUsername), "mfa"]),
        auth_header(Token),
        #{}
    ).

error_code(RespBody) ->
    maps:get(<<"code">>, emqx_utils_json:decode(RespBody)).
