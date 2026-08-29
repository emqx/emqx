%%--------------------------------------------------------------------
%% Copyright (c) 2026-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
%%
%% HTTP coverage for the self-service / administrator split:
%%
%%   /current_user            GET     own profile
%%   /current_user/change_pwd POST    own password
%%   /current_user/mfa        POST    own MFA setup / rotate
%%                            DELETE  own MFA disable
%%
%% Self endpoints are authorized by the authenticated identity alone --
%% no scope, no role, no `:username' in the path. The administrator
%% routes under `/users/:username/*' are the mirror image: scope plus
%% global-administrator role, target is always another user.
%%
%% The MFA lock policy itself (first-time setup, admin_override) lives
%% in emqx_dashboard_user_scopes_SUITE; this suite covers the routing,
%% the identity binding, the password rules and the SSO case.
%%--------------------------------------------------------------------

-module(emqx_dashboard_current_user_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("emqx_utils/include/emqx_api_key_scopes.hrl").
-include("emqx_dashboard.hrl").

-define(HOST, "http://127.0.0.1:18083").
-define(BASE_PATH, "/api/v5").

-define(PASSWORD, <<"P@ssw0rd_1">>).
-define(NEW_PASSWORD, <<"P@ssw0rd_2">>).

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
    ?EE_ONLY(emqx_cth_suite:stop(?config(apps, Config)), ok),
    ok.

end_per_testcase(_Case, _Config) ->
    lists:foreach(
        fun(User) -> emqx_dashboard_admin:remove_user(admin_key(User)) end,
        emqx_dashboard_admin:all_users()
    ).

%% `to_external_user/1' flattens an SSO key `{Backend, Name}' into a bare
%% `username' plus a `backend' field; `remove_user/1' wants the key back.
admin_key(#{username := Name, backend := ?BACKEND_LOCAL}) ->
    Name;
admin_key(#{username := Name, backend := Backend}) when is_atom(Backend) ->
    ?SSO_USERNAME(Backend, Name).

%%--------------------------------------------------------------------
%% GET /current_user
%%--------------------------------------------------------------------

%% The profile describes the caller, resolved from the bearer token --
%% there is no path parameter to point it at anybody else.
t_get_own_profile(_Config) ->
    ok = add_user(<<"viewer">>, ?ROLE_VIEWER),
    {ok, 200, Body} = get_current_user(token(<<"viewer">>)),
    ?assertMatch(
        #{
            <<"username">> := <<"viewer">>,
            <<"role">> := ?ROLE_VIEWER,
            <<"backend">> := <<"local">>,
            <<"mfa">> := <<"none">>,
            %% `null', not the string "global": the profile goes through
            %% `to_json_out/1' like every other emitter of this shape.
            <<"namespace">> := null
        },
        json(Body)
    ).

%% The profile is the same object `GET /users' reports for the same
%% account, so the two must not disagree field by field. Only `scopes'
%% differs by design: the self endpoint expands the role default.
t_own_profile_matches_admin_view(_Config) ->
    ok = add_user(<<"boss">>, ?ROLE_SUPERUSER),
    Token = token(<<"boss">>),
    {ok, 200, SelfBody} = get_current_user(Token),
    {ok, 200, ListBody} = request_api(get, api_path(["users"]), auth_header(Token)),
    Self = json(SelfBody),
    [Admin] = [U || U <- json(ListBody), maps:get(<<"username">>, U) =:= <<"boss">>],
    ?assertEqual(
        maps:remove(<<"scopes">>, Admin),
        maps:remove(<<"scopes">>, Self)
    ).

%% `scopes' is the EFFECTIVE list: the role default is expanded, so the
%% `unset' sentinel that `GET /users' may report never appears here.
%% A viewer that has never been given an explicit list still learns
%% what it holds.
t_get_own_profile_reports_effective_scopes(_Config) ->
    ok = add_user(<<"viewer">>, ?ROLE_VIEWER),
    ?assertEqual(undefined, emqx_dashboard_admin:scopes_of(<<"viewer">>)),
    {ok, 200, Body} = get_current_user(token(<<"viewer">>)),
    #{<<"scopes">> := Scopes} = json(Body),
    ?assert(is_list(Scopes), Scopes),
    ?assertEqual(
        lists:sort(emqx_dashboard_admin:effective_scopes_of(<<"viewer">>)),
        lists:sort(Scopes)
    ),
    %% An explicit list is reported verbatim.
    {ok, ok} = emqx_dashboard_admin:set_user_scopes(<<"viewer">>, [?SCOPE_MONITORING]),
    {ok, 200, Body1} = get_current_user(token(<<"viewer">>)),
    ?assertMatch(#{<<"scopes">> := [?SCOPE_MONITORING]}, json(Body1)).

%%--------------------------------------------------------------------
%% Self-service is not gated by role or scope
%%--------------------------------------------------------------------

%% Requirement: a viewer with no explicit scopes manages its own
%% account fully, and an explicitly emptied scope list does not lock it
%% out of its own password or MFA.
t_viewer_self_service_allowed(_Config) ->
    lists:foreach(
        fun(Scopes) ->
            ok = add_user(<<"viewer">>, ?ROLE_VIEWER),
            case Scopes of
                undefined -> ok;
                _ -> {ok, ok} = emqx_dashboard_admin:set_user_scopes(<<"viewer">>, Scopes)
            end,
            ?assertMatch({ok, 200, _}, get_current_user(token(<<"viewer">>)), Scopes),
            ?assertMatch({ok, 204, _}, setup_own_mfa(token(<<"viewer">>)), Scopes),
            %% A second POST is a rotate, not a first-time setup.
            ?assertMatch({ok, 204, _}, setup_own_mfa(token(<<"viewer">>)), Scopes),
            ?assertMatch({ok, 204, _}, delete_own_mfa(token(<<"viewer">>)), Scopes),
            ?assertMatch(
                {ok, 204, _},
                change_own_pwd(token(<<"viewer">>), ?PASSWORD, ?NEW_PASSWORD),
                Scopes
            ),
            {ok, _} = emqx_dashboard_admin:remove_user(<<"viewer">>)
        end,
        [undefined, []]
    ).

%% The mirror image: a viewer is refused on every administrator route,
%% for every target -- another user and itself alike. Deleting the
%% viewer-self RBAC clauses is what closes the "itself" half.
t_viewer_denied_on_admin_routes(_Config) ->
    ok = add_user(<<"viewer">>, ?ROLE_VIEWER),
    ok = add_user(<<"other">>, ?ROLE_VIEWER),
    Token = token(<<"viewer">>),
    lists:foreach(
        fun(Target) ->
            ?assertMatch({ok, 403, _}, admin_setup_mfa(Token, Target), Target),
            ?assertMatch({ok, 403, _}, admin_delete_mfa(Token, Target), Target),
            ?assertMatch({ok, 403, _}, update_user(Token, Target), Target),
            ?assertMatch({ok, 403, _}, delete_user(Token, Target), Target)
        end,
        [<<"other">>, <<"viewer">>]
    ).

%%--------------------------------------------------------------------
%% Administrator side
%%--------------------------------------------------------------------

%% An administrator manages other users through the admin routes and
%% its own account through the self routes -- and the admin routes
%% refuse a self target rather than quietly doing the wrong thing.
t_admin_manages_others_and_self_separately(_Config) ->
    ok = add_user(<<"boss">>, ?ROLE_SUPERUSER),
    ok = add_user(<<"other">>, ?ROLE_VIEWER),
    Token = token(<<"boss">>),
    ?assertMatch({ok, 204, _}, admin_setup_mfa(Token, <<"other">>)),
    ?assertMatch({ok, 204, _}, admin_delete_mfa(Token, <<"other">>)),
    ?assertMatch({ok, 200, _}, update_user(Token, <<"other">>)),
    %% Own account: refused on the admin route, served on the self route.
    ?assertMatch({ok, 400, _}, admin_setup_mfa(Token, <<"boss">>)),
    ?assertMatch({ok, 400, _}, admin_delete_mfa(Token, <<"boss">>)),
    %% A successful MFA write invalidates the caller's own sessions, so
    %% the second call needs a fresh token -- see t_own_mfa_write_ends_session.
    ?assertMatch({ok, 204, _}, setup_own_mfa(token(<<"boss">>))),
    ?assertMatch({ok, 204, _}, delete_own_mfa(token(<<"boss">>))).

%% The "cannot delete self" guard now compares the authenticated
%% identity with the target instead of re-parsing the Authorization
%% header. Deleting somebody else still works.
t_admin_cannot_delete_self(_Config) ->
    ok = add_user(<<"boss">>, ?ROLE_SUPERUSER),
    ok = add_user(<<"other">>, ?ROLE_VIEWER),
    Token = token(<<"boss">>),
    {ok, 400, Body} = delete_user(Token, <<"boss">>),
    ?assertEqual(<<"NOT_ALLOWED">>, error_code(Body)),
    ?assertMatch([_], emqx_dashboard_admin:lookup_user(<<"boss">>)),
    ?assertMatch({ok, 204, _}, delete_user(Token, <<"other">>)).

%% A namespaced administrator is confined to its own account: denied on
%% another user's MFA even inside its own namespace (the cross-user
%% reset vector), allowed on `/current_user/*'.
t_namespaced_admin_is_self_only(_Config) ->
    ok = add_user(<<"nsadmin">>, <<"ns:ns1::", ?ROLE_SUPERUSER/binary>>),
    ok = add_user(<<"victim">>, ?ROLE_VIEWER),
    Token = token(<<"nsadmin">>),
    ?assertMatch({ok, 403, _}, admin_setup_mfa(Token, <<"victim">>)),
    ?assertMatch({ok, 403, _}, admin_delete_mfa(Token, <<"victim">>)),
    ?assertMatch({ok, 200, _}, get_current_user(Token)),
    ?assertMatch({ok, 204, _}, setup_own_mfa(token(<<"nsadmin">>))),
    ?assertMatch({ok, 204, _}, delete_own_mfa(token(<<"nsadmin">>))),
    ?assertMatch(
        {ok, 204, _}, change_own_pwd(token(<<"nsadmin">>), ?PASSWORD, ?NEW_PASSWORD)
    ).

%%--------------------------------------------------------------------
%% POST /current_user/change_pwd
%%--------------------------------------------------------------------

%% `old_pwd' is always verified: there is no administrator HTTP path
%% that resets a password without it.
t_change_own_pwd(_Config) ->
    ok = add_user(<<"viewer">>, ?ROLE_VIEWER),
    Token = token(<<"viewer">>),
    {ok, 400, Wrong} = change_own_pwd(Token, <<"not-the-password">>, ?NEW_PASSWORD),
    ?assertEqual(<<"ERROR_PWD_NOT_MATCH">>, error_code(Wrong)),
    {ok, 400, EmptyOld} = change_own_pwd(Token, <<>>, ?NEW_PASSWORD),
    ?assertEqual(<<"BAD_REQUEST">>, error_code(EmptyOld)),
    {ok, 400, EmptyNew} = change_own_pwd(Token, ?PASSWORD, <<>>),
    ?assertEqual(<<"BAD_REQUEST">>, error_code(EmptyNew)),
    %% The failed attempts left the password alone.
    ?assertMatch({ok, 204, _}, change_own_pwd(Token, ?PASSWORD, ?NEW_PASSWORD)),
    ?assertMatch({ok, _}, emqx_dashboard_admin:check(<<"viewer">>, ?NEW_PASSWORD)).

%% An SSO account has no local password -- the identity provider owns
%% the credential. The endpoint must say so rather than crash:
%% `emqx_dashboard_admin:change_password/3' is guarded on a binary
%% username and would raise a function_clause on the SSO key.
t_change_own_pwd_sso_user_rejected(_Config) ->
    Backend = saml,
    SsoUser = <<"sso-user@example.com">>,
    {ok, _} = emqx_dashboard_admin:add_sso_user(Backend, SsoUser, ?ROLE_VIEWER, <<"d">>),
    SsoKey = ?SSO_USERNAME(Backend, SsoUser),
    {ok, 400, Body} = change_own_pwd(sso_token(SsoKey), ?PASSWORD, ?NEW_PASSWORD),
    ?assertEqual(<<"NOT_ALLOWED">>, error_code(Body)),
    %% The rest of the namespace still works for an SSO caller.
    {ok, 200, Profile} = get_current_user(sso_token(SsoKey)),
    ?assertMatch(
        #{<<"username">> := SsoUser, <<"backend">> := <<"saml">>},
        json(Profile)
    ),
    ?assertMatch({ok, 204, _}, setup_own_mfa(sso_token(SsoKey))),
    ?assertMatch({ok, 204, _}, delete_own_mfa(sso_token(SsoKey))).

%% Re-keying MFA invalidates the account's sessions
%% (`emqx_dashboard_admin:reinit_mfa/3' destroys its tokens), so the
%% bearer token that authorized the change is dead immediately after
%% it. Pinned here because every other self-MFA test has to work around
%% it, and a caller has to log in again with the new secret.
t_own_mfa_write_ends_session(_Config) ->
    ok = add_user(<<"viewer">>, ?ROLE_VIEWER),
    Token = token(<<"viewer">>),
    ?assertMatch({ok, 200, _}, get_current_user(Token)),
    ?assertMatch({ok, 204, _}, setup_own_mfa(Token)),
    {ok, 401, Body} = get_current_user(Token),
    ?assertEqual(<<"BAD_TOKEN">>, error_code(Body)),
    %% A fresh login is all it takes.
    ?assertMatch({ok, 200, _}, get_current_user(token(<<"viewer">>))).

%%--------------------------------------------------------------------
%% Routing
%%--------------------------------------------------------------------

%% `current_user', `self' and `me' are all legal usernames and there is
%% no reserved-word check on them. `/current_user' is a top-level path
%% outside `/users/', so such a user neither shadows nor is shadowed by
%% the self-service routes, and an administrator still manages it by
%% name.
t_username_colliding_with_self_route(_Config) ->
    ok = add_user(<<"boss">>, ?ROLE_SUPERUSER),
    AdminToken = token(<<"boss">>),
    lists:foreach(
        fun(Name) ->
            ok = add_user(Name, ?ROLE_VIEWER),
            Token = token(Name),
            %% The self route serves the caller, whatever it is called.
            {ok, 200, Body} = get_current_user(Token),
            ?assertMatch(#{<<"username">> := Name}, json(Body), Name),
            ?assertMatch({ok, 204, _}, setup_own_mfa(Token), Name),
            %% ... and the administrator still reaches the account by name.
            ?assertMatch({ok, 204, _}, admin_delete_mfa(AdminToken, Name), Name),
            ?assertMatch({ok, 200, _}, get_current_user(token(Name)), Name),
            ?assertMatch({ok, 200, _}, update_user(AdminToken, Name), Name),
            ?assertMatch({ok, 204, _}, delete_user(AdminToken, Name), Name)
        end,
        [<<"current_user">>, <<"self">>, <<"me">>]
    ).

%%--------------------------------------------------------------------
%% Deprecated shim: POST /users/:username/change_pwd
%%--------------------------------------------------------------------

%% The shim keeps working for the caller's own account, at any role and
%% with an explicitly emptied scope list -- dropping `user_management'
%% from the route is what lets a viewer keep its old call.
t_shim_changes_own_password(_Config) ->
    lists:foreach(
        fun(Role) ->
            ok = add_user(<<"u">>, Role),
            {ok, ok} = emqx_dashboard_admin:set_user_scopes(<<"u">>, []),
            ?assertMatch(
                {ok, 204, _},
                shim_change_pwd(token(<<"u">>), <<"u">>, ?PASSWORD, ?NEW_PASSWORD),
                Role
            ),
            ?assertMatch({ok, _}, emqx_dashboard_admin:check(<<"u">>, ?NEW_PASSWORD), Role),
            {ok, _} = emqx_dashboard_admin:remove_user(<<"u">>)
        end,
        [?ROLE_VIEWER, ?ROLE_SUPERUSER, <<"ns:ns1::", ?ROLE_SUPERUSER/binary>>]
    ).

%% Pointing the shim at somebody else is refused with 403, for every
%% role. Before the split this was allowed through RBAC for an
%% administrator and merely failed the `old_pwd' check, so a cross-user
%% call was blocked by accident; now it is blocked on purpose, and the
%% namespaced-administrator invariant holds by the same single check.
t_shim_rejects_other_user(_Config) ->
    ok = add_user(<<"victim">>, ?ROLE_VIEWER),
    lists:foreach(
        fun(Role) ->
            ok = add_user(<<"u">>, Role),
            {ok, 403, Body} = shim_change_pwd(
                token(<<"u">>), <<"victim">>, ?PASSWORD, ?NEW_PASSWORD
            ),
            ?assertEqual(<<"NOT_ALLOWED">>, error_code(Body), Role),
            %% The refusal is real: the target's password is untouched
            %% and so is the caller's.
            ?assertMatch({ok, _}, emqx_dashboard_admin:check(<<"victim">>, ?PASSWORD), Role),
            ?assertMatch({ok, _}, emqx_dashboard_admin:check(<<"u">>, ?PASSWORD), Role),
            {ok, _} = emqx_dashboard_admin:remove_user(<<"u">>)
        end,
        [?ROLE_VIEWER, ?ROLE_SUPERUSER, <<"ns:ns1::", ?ROLE_SUPERUSER/binary>>]
    ).

%% The shim runs the same checks as the canonical route, because it
%% delegates to it rather than reimplementing anything.
t_shim_verifies_old_password(_Config) ->
    ok = add_user(<<"u">>, ?ROLE_VIEWER),
    Token = token(<<"u">>),
    {ok, 400, Wrong} = shim_change_pwd(Token, <<"u">>, <<"not-the-password">>, ?NEW_PASSWORD),
    ?assertEqual(<<"ERROR_PWD_NOT_MATCH">>, error_code(Wrong)),
    {ok, 400, Empty} = shim_change_pwd(Token, <<"u">>, <<>>, ?NEW_PASSWORD),
    ?assertEqual(<<"BAD_REQUEST">>, error_code(Empty)),
    ?assertMatch({ok, _}, emqx_dashboard_admin:check(<<"u">>, ?PASSWORD)).

%% An SSO caller reaches the delegate and gets the "no local password"
%% answer. The route has no `?backend' parameter, so without matching on
%% the name part of the SSO key this would be a misleading 403 instead.
t_shim_sso_user_gets_no_local_password(_Config) ->
    Backend = saml,
    SsoUser = <<"sso-shim@example.com">>,
    {ok, _} = emqx_dashboard_admin:add_sso_user(Backend, SsoUser, ?ROLE_VIEWER, <<"d">>),
    SsoKey = ?SSO_USERNAME(Backend, SsoUser),
    {ok, 400, Body} = shim_change_pwd(sso_token(SsoKey), SsoUser, ?PASSWORD, ?NEW_PASSWORD),
    ?assertEqual(<<"NOT_ALLOWED">>, error_code(Body)).

%% The shim is advertised as deprecated so clients can see it is on the
%% way out, and the canonical route is not.
t_shim_is_marked_deprecated(_Config) ->
    ok = add_user(<<"boss">>, ?ROLE_SUPERUSER),
    Url = ?HOST ++ "/api-docs/swagger.json",
    {ok, {{_, 200, _}, _Headers, Body}} =
        httpc:request(
            get, {Url, [auth_header(token(<<"boss">>))]}, [], [{body_format, binary}]
        ),
    #{<<"paths">> := Paths} = json(Body),
    Shim = maps:get(<<"post">>, maps:get(<<"/users/{username}/change_pwd">>, Paths)),
    ?assertEqual(true, maps:get(<<"deprecated">>, Shim, false)),
    Canonical = maps:get(<<"post">>, maps:get(<<"/current_user/change_pwd">>, Paths)),
    ?assertEqual(false, maps:get(<<"deprecated">>, Canonical, false)).

%% The Dashboard SPA and emqx-docs are generated from the OpenAPI spec,
%% so the split is only really shipped once the spec carries it: the
%% three self routes present, the administrator password-reset route
%% gone.
t_openapi_spec_carries_the_split(_Config) ->
    ok = add_user(<<"boss">>, ?ROLE_SUPERUSER),
    Url = ?HOST ++ "/api-docs/swagger.json",
    {ok, {{_, 200, _}, _Headers, Body}} =
        httpc:request(
            get, {Url, [auth_header(token(<<"boss">>))]}, [], [{body_format, binary}]
        ),
    #{<<"paths">> := Paths} = json(Body),
    ?assertMatch(#{<<"get">> := _}, maps:get(<<"/current_user">>, Paths)),
    ?assertMatch(
        #{<<"post">> := _}, maps:get(<<"/current_user/change_pwd">>, Paths)
    ),
    ?assertMatch(
        #{<<"post">> := _, <<"delete">> := _}, maps:get(<<"/current_user/mfa">>, Paths)
    ),
    %% The administrator MFA route stays, and so does the deprecated
    %% change_pwd shim (see t_shim_is_marked_deprecated).
    ?assertMatch(
        #{<<"post">> := _, <<"delete">> := _}, maps:get(<<"/users/{username}/mfa">>, Paths)
    ),
    ?assertMatch(#{<<"post">> := _}, maps:get(<<"/users/{username}/change_pwd">>, Paths)).

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

sso_token(SsoKey) ->
    {ok, #{token := Token}} = emqx_dashboard_admin:sign_token(
        SsoKey, <<>>, ?TRUSTED_MFA_TOKEN
    ),
    Token.

add_user(Username, Role) ->
    {ok, _} = emqx_dashboard_admin:add_user(Username, ?PASSWORD, Role, <<"desc">>),
    ok.

token(Username) ->
    {ok, #{token := Token}} = emqx_dashboard_admin:sign_token(
        Username, ?PASSWORD, ?TRUSTED_MFA_TOKEN
    ),
    Token.

get_current_user(Token) ->
    request_api(get, api_path(["current_user"]), auth_header(Token)).

shim_change_pwd(Token, Target, OldPwd, NewPwd) ->
    request_api(
        post,
        api_path(["users", binary_to_list(Target), "change_pwd"]),
        auth_header(Token),
        #{<<"old_pwd">> => OldPwd, <<"new_pwd">> => NewPwd}
    ).

change_own_pwd(Token, OldPwd, NewPwd) ->
    request_api(
        post,
        api_path(["current_user", "change_pwd"]),
        auth_header(Token),
        #{<<"old_pwd">> => OldPwd, <<"new_pwd">> => NewPwd}
    ).

setup_own_mfa(Token) ->
    request_api(
        post,
        api_path(["current_user", "mfa"]),
        auth_header(Token),
        #{<<"mechanism">> => <<"totp">>}
    ).

delete_own_mfa(Token) ->
    request_api(delete, api_path(["current_user", "mfa"]), auth_header(Token), #{}).

admin_setup_mfa(Token, Target) ->
    request_api(
        post,
        api_path(["users", binary_to_list(Target), "mfa"]),
        auth_header(Token),
        #{<<"mechanism">> => <<"totp">>}
    ).

admin_delete_mfa(Token, Target) ->
    request_api(
        delete, api_path(["users", binary_to_list(Target), "mfa"]), auth_header(Token), #{}
    ).

update_user(Token, Target) ->
    request_api(
        put,
        api_path(["users", binary_to_list(Target)]),
        auth_header(Token),
        #{<<"description">> => <<"touched">>}
    ).

delete_user(Token, Target) ->
    request_api(
        delete, api_path(["users", binary_to_list(Target)]), auth_header(Token), #{}
    ).

auth_header(Token) ->
    {"Authorization", "Bearer " ++ binary_to_list(Token)}.

api_path(Parts) ->
    ?HOST ++ filename:join([?BASE_PATH | Parts]).

request_api(Method, Url, Auth) ->
    emqx_common_test_http:request_api(Method, Url, _QueryParams = [], Auth).

request_api(Method, Url, Auth, Body) ->
    emqx_common_test_http:request_api(Method, Url, _QueryParams = [], Auth, Body).

json(Body) ->
    emqx_utils_json:decode(Body).

error_code(Body) ->
    maps:get(<<"code">>, json(Body)).
