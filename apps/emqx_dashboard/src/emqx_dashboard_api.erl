%%--------------------------------------------------------------------
%% Copyright (c) 2020-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_dashboard_api).

-behaviour(minirest_api).

-include("emqx_dashboard.hrl").
-include("emqx_dashboard_rbac.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("typerefl/include/types.hrl").
-include_lib("emqx/include/emqx_config.hrl").
-include_lib("emqx_utils/include/emqx_api_key_scopes.hrl").

-export([
    api_spec/0,
    fields/1,
    paths/0,
    schema/1,
    namespace/0
]).

-export([
    login/2,
    logout/2,
    users/2,
    user_scopes/2,
    user/2,
    change_pwd/2,
    change_mfa/2
]).
-export([scopes/0]).

-define(EMPTY(V), (V == undefined orelse V == <<>>)).

-define(BAD_USERNAME_OR_PWD, 'BAD_USERNAME_OR_PWD').
-define(BAD_MFA_TOKEN, 'BAD_MFA_TOKEN').
-define(WRONG_TOKEN_OR_USERNAME, 'WRONG_TOKEN_OR_USERNAME').
-define(USER_NOT_FOUND, 'USER_NOT_FOUND').
-define(ERROR_PWD_NOT_MATCH, 'ERROR_PWD_NOT_MATCH').
-define(NOT_ALLOWED, 'NOT_ALLOWED').
-define(BAD_REQUEST, 'BAD_REQUEST').
-define(LOGIN_LOCKED, 'LOGIN_LOCKED').

namespace() -> "dashboard".

api_spec() ->
    emqx_dashboard_swagger:spec(?MODULE, #{check_schema => true, translate_body => true}).

%% API key auth is rejected at the minirest layer for these paths
%% (security => [#{bearerAuth => []}] excludes basic auth). The scope
%% map below applies to dashboard LOGIN users -- checked in
%% emqx_dashboard_rbac:check_login_user_scopes/2.
%%
%% ?SCOPE_PUBLIC marks paths that are intentionally unscoped:
%%   * /login -- pre-login (security => []).
%%   * /logout -- any authenticated role may log itself out.
%%   * /user_scopes -- static catalog endpoint, no tenant data.
scopes() ->
    #{
        <<"/login">> => ?SCOPE_PUBLIC,
        <<"/logout">> => ?SCOPE_PUBLIC,
        <<"/user_scopes">> => ?SCOPE_PUBLIC,
        <<"/users">> => ?SCOPE_USER_MGMT,
        <<"/users/:username">> => ?SCOPE_USER_MGMT,
        <<"/users/:username/change_pwd">> => ?SCOPE_USER_MGMT,
        <<"/users/:username/mfa">> => ?SCOPE_MFA_MGMT
    }.

paths() ->
    [
        "/login",
        "/logout",
        "/users",
        "/users/:username",
        "/users/:username/change_pwd",
        "/users/:username/mfa",
        "/user_scopes"
    ].

schema("/login") ->
    ErrorCodes = [?BAD_USERNAME_OR_PWD, ?BAD_MFA_TOKEN, ?LOGIN_LOCKED],
    #{
        'operationId' => login,
        post => #{
            tags => [<<"dashboard">>],
            desc => ?DESC(login_api),
            'requestBody' => fields([username, password, mfa_token]),
            responses => #{
                200 => fields([
                    role, token, version, license, password_expire_in_seconds
                ]),
                401 => emqx_dashboard_swagger:error_codes(ErrorCodes, ?DESC(login_failed401))
            },
            security => []
        }
    };
schema("/logout") ->
    #{
        'operationId' => logout,
        post => #{
            tags => [<<"dashboard">>],
            desc => ?DESC(logout_api),
            security => [#{'bearerAuth' => []}],
            parameters => sso_parameters(),
            'requestBody' => fields([username]),
            responses => #{
                204 => <<"Dashboard logout successfully">>,
                401 => response_schema(401)
            }
        }
    };
schema("/users") ->
    #{
        'operationId' => users,
        get => #{
            tags => [<<"dashboard">>],
            desc => ?DESC(list_users_api),
            security => [#{'bearerAuth' => []}],
            responses => #{
                200 => mk(
                    array(hoconsc:ref(user)),
                    #{desc => ?DESC(list_users_api)}
                )
            }
        },
        post => #{
            tags => [<<"dashboard">>],
            desc => ?DESC(create_user_api),
            security => [#{'bearerAuth' => []}],
            'requestBody' => fields([username, password, role, description, scopes_request]),
            responses => #{
                200 => user_fields()
            }
        }
    };
schema("/user_scopes") ->
    %% Public catalog endpoint — any authenticated dashboard login
    %% user (incl. viewer / SSO viewer) may list the available scope
    %% names. The path is intentionally absent from scopes/0 above so
    %% it falls through to the unmapped-path branch (fail-open).
    %%
    %% Top-level path (sibling to /action_types, /source_types) so it
    %% never collides with /users/:username wildcard routing.
    #{
        'operationId' => user_scopes,
        get => #{
            tags => [<<"Dashboard">>],
            desc => ?DESC(list_user_scopes_api),
            security => [#{'bearerAuth' => []}],
            responses => #{
                200 => mk(map(), #{desc => ?DESC(list_user_scopes_api)})
            }
        }
    };
schema("/users/:username") ->
    #{
        'operationId' => user,
        put => #{
            tags => [<<"dashboard">>],
            desc => ?DESC(update_user_api),
            parameters => sso_parameters(fields([username_in_path])),
            'requestBody' => fields([role, description, scopes_request]),
            responses => #{
                200 => user_fields(),
                400 => emqx_dashboard_swagger:error_codes(
                    [?BAD_REQUEST, ?NOT_ALLOWED], ?DESC(login_failed_response400)
                ),
                404 => response_schema(404)
            }
        },
        delete => #{
            tags => [<<"dashboard">>],
            desc => ?DESC(delete_user_api),
            parameters => sso_parameters(fields([username_in_path])),
            responses => #{
                204 => <<"Delete User successfully">>,
                400 => emqx_dashboard_swagger:error_codes(
                    [?BAD_REQUEST, ?NOT_ALLOWED], ?DESC(login_failed_response400)
                ),
                404 => response_schema(404)
            }
        }
    };
schema("/users/:username/change_pwd") ->
    #{
        'operationId' => change_pwd,
        post => #{
            tags => [<<"dashboard">>],
            desc => ?DESC(change_pwd_api),
            parameters => fields([username_in_path]),
            'requestBody' => fields([old_pwd, new_pwd]),
            responses => #{
                204 => <<"Update user password successfully">>,
                404 => response_schema(404),
                400 => emqx_dashboard_swagger:error_codes(
                    [?BAD_REQUEST, ?ERROR_PWD_NOT_MATCH],
                    ?DESC(login_failed_response400)
                )
            }
        }
    };
schema("/users/:username/mfa") ->
    #{
        'operationId' => change_mfa,
        post => #{
            tags => [<<"dashboard">>],
            desc => ?DESC(change_mfa),
            parameters => sso_parameters(fields([username_in_path])),
            'requestBody' => emqx_dashboard_schema:mfa_fields(),
            responses => #{
                204 => <<"MFA setting is updated">>,
                404 => response_schema(404)
            }
        },
        delete => #{
            tags => [<<"dashboard">>],
            desc => ?DESC(delete_mfa),
            parameters => sso_parameters(fields([username_in_path])),
            responses => #{
                204 => <<"MFA setting is disabled">>,
                404 => response_schema(404)
            }
        }
    }.

response_schema(401) ->
    emqx_dashboard_swagger:error_codes([?BAD_USERNAME_OR_PWD], ?DESC(login_failed401));
response_schema(404) ->
    emqx_dashboard_swagger:error_codes([?USER_NOT_FOUND], ?DESC(users_api404)).

fields(user) ->
    user_fields();
fields(List) ->
    [field(Key) || Key <- List, field_filter(Key)].

user_fields() ->
    fields([username, role, description, backend, scopes_response]) ++ ee_user_fields().

ee_user_fields() ->
    [
        {mfa,
            mk(
                enum([none, disabled] ++ emqx_dashboard_mfa:supported_mechanisms()),
                #{desc => ?DESC(mfa_status), example => totp}
            )}
    ].

field(username) ->
    {username,
        mk(binary(), #{desc => ?DESC(username), 'maxLength' => 100, example => <<"admin">>})};
field(username_in_path) ->
    {username,
        mk(binary(), #{
            desc => ?DESC(username),
            example => <<"admin">>,
            in => path,
            required => true
        })};
field(password) ->
    {password,
        mk(binary(), #{desc => ?DESC(password), 'maxLength' => 100, example => <<"public">>})};
field(mfa_token) ->
    {mfa_token,
        mk(binary(), #{
            desc => ?DESC(mfa_token),
            'maxLength' => 9,
            example => <<"023123">>,
            required => false
        })};
field(description) ->
    {description, mk(binary(), #{desc => ?DESC(user_description), example => <<"administrator">>})};
field(token) ->
    {token, mk(binary(), #{desc => ?DESC(token)})};
field(license) ->
    {license, [
        {edition,
            mk(
                enum([opensource, enterprise]),
                #{desc => ?DESC(license), example => opensource}
            )}
    ]};
field(version) ->
    {version, mk(string(), #{desc => ?DESC(version), example => <<"5.0.0">>})};
field(old_pwd) ->
    {old_pwd, mk(binary(), #{desc => ?DESC(old_pwd)})};
field(new_pwd) ->
    {new_pwd, mk(binary(), #{desc => ?DESC(new_pwd)})};
field(role) ->
    {role,
        mk(binary(), #{desc => ?DESC(role), default => ?ROLE_DEFAULT, example => ?ROLE_DEFAULT})};
field(scopes_request) ->
    {scopes,
        mk(hoconsc:array(binary()), #{
            desc => ?DESC(user_scopes_request),
            required => false,
            example => [?SCOPE_USER_MGMT, ?SCOPE_MFA_MGMT]
        })};
field(scopes_response) ->
    %% Response shape: `scopes' MAY be the binary sentinel <<"unset">> in
    %% addition to the array-of-binaries form. This sentinel surfaces only
    %% for legacy records that survived an upgrade from a release where
    %% the dashboard-user scopes feature did not exist (#17235). The POST
    %% and SSO-provisioning paths both materialize role-default scopes at
    %% creation time, so no fresh user will ever appear with
    %% `scopes => <<"unset">>'.
    {scopes,
        mk(hoconsc:union([unset, hoconsc:array(binary())]), #{
            desc => ?DESC(user_scopes_response),
            required => false,
            example => [?SCOPE_USER_MGMT, ?SCOPE_MFA_MGMT]
        })};
field(backend) ->
    {backend, mk(binary(), #{desc => ?DESC(backend), example => <<"local">>})};
field(password_expire_in_seconds) ->
    {password_expire_in_seconds,
        mk(integer(), #{desc => ?DESC(password_expire_in_seconds), example => 3600})}.

%% -------------------------------------------------------------------------------------------------
%% API

login(post, #{body := Params}) ->
    Username = maps:get(<<"username">>, Params),
    Password = maps:get(<<"password">>, Params),
    MfaToken = maps:get(<<"mfa_token">>, Params, ?NO_MFA_TOKEN),
    minirest_handler:update_log_meta(#{log_from => dashboard, log_source => Username}),
    case emqx_dashboard_admin:sign_token(Username, Password, MfaToken) of
        {ok, Result} ->
            ?SLOG(info, #{msg => "dashboard_login_successful", username => Username}),
            ok = emqx_dashboard_login_lock:reset(Username),
            Version = iolist_to_binary(proplists:get_value(version, emqx_sys:info())),
            {200,
                to_json_out(Result#{
                    version => Version,
                    license => #{edition => emqx_release:edition()}
                })};
        {error, R} ->
            ok = register_unsuccessful_login(Username, R),
            ?SLOG(info, #{msg => "dashboard_login_failed", username => Username, reason => R}),
            format_login_failed_error(R)
    end.

format_login_failed_error(<<"default_credentials_not_changed">>) ->
    {401, ?BAD_USERNAME_OR_PWD,
        ~b"""
    Default admin password must be changed before login is allowed.
    Run: emqx ctl admins passwd admin <a-strong-password>.
    Or configure: dashboard.default_password = "<a-strong-password>".
    """};
format_login_failed_error(Reason) ->
    maybe
        {is_mfa_error, false} ?= {is_mfa_error, emqx_dashboard_mfa:is_mfa_error(Reason)},
        {is_login_locked_error, false} ?=
            {is_login_locked_error, emqx_dashboard_login_lock:is_login_locked_error(Reason)},
        {401, ?BAD_USERNAME_OR_PWD, <<"Auth failed">>}
    else
        {is_mfa_error, true} ->
            {401, ?BAD_MFA_TOKEN, Reason};
        {is_login_locked_error, true} ->
            {401, ?LOGIN_LOCKED, <<"Login locked">>}
    end.

logout(_, #{
    body := #{<<"username">> := Username0} = Req,
    headers := #{<<"authorization">> := <<"Bearer ", Token/binary>>}
}) ->
    Username = username(Req, Username0),
    case emqx_dashboard_admin:destroy_token_by_username(Username, Token) of
        ok ->
            ?SLOG(info, #{msg => "dashboard_logout_successful", username => Username0}),
            204;
        _R ->
            ?SLOG(info, #{msg => "dashboard_logout_failed.", username => Username0}),
            {401, ?WRONG_TOKEN_OR_USERNAME, <<"Ensure your token & username">>}
    end.

user_scopes(get, _Request) ->
    Scopes = [resolve_scope_desc(S) || S <- emqx_scope_catalog:login_user_scope_catalog()],
    {200, #{scopes => Scopes}}.

resolve_scope_desc(#{desc := Desc} = Scope) ->
    Scope#{desc => emqx_dashboard_swagger:get_i18n(<<"desc">>, Desc, <<>>, #{})}.

users(get, _Request) ->
    {200, to_json_out(emqx_dashboard_admin:all_users())};
users(post, #{body := Params}) ->
    Desc = maps:get(<<"description">>, Params, <<"">>),
    Role = maps:get(<<"role">>, Params, ?ROLE_DEFAULT),
    Username = maps:get(<<"username">>, Params),
    Password = maps:get(<<"password">>, Params),
    %% Materialize role defaults when the client omitted `scopes' entirely.
    %% Explicit `[]' (deny-all) and explicit lists pass through unchanged.
    %% After this PR `<<"unset">>' in a GET response is reserved for legacy
    %% records that survived an upgrade; no creation path stores `undefined'.
    RawScopes = maps:get(<<"scopes">>, Params, undefined),
    Scopes = effective_scopes_on_create(Role, RawScopes),
    case ?EMPTY(Username) orelse ?EMPTY(Password) of
        true ->
            {400, ?BAD_REQUEST, <<"Username or password undefined">>};
        false ->
            create_user(Username, Password, Role, Desc, Scopes)
    end.

%% Run the validate → add_user → set_scopes pipeline. Each step short-
%% circuits to the appropriate HTTP response, keeping users(post,...)
%% within elvis's nesting cap.
create_user(Username, Password, Role, Desc, Scopes) ->
    case validate_login_user_scopes(Role, Scopes) of
        ok ->
            do_create_user(Username, Password, Role, Desc, Scopes);
        {error, Msg} ->
            {400, ?BAD_REQUEST, Msg}
    end.

do_create_user(Username, Password, Role, Desc, Scopes) ->
    case emqx_dashboard_admin:add_user(Username, Password, Role, Desc) of
        {ok, Result} ->
            finalise_create_user(Username, Scopes, Result);
        {error, Reason} ->
            ?SLOG(info, #{
                msg => "create_dashboard_user_failed",
                username => Username,
                reason => Reason
            }),
            {400, ?BAD_REQUEST, Reason}
    end.

finalise_create_user(Username, Scopes, Result) ->
    case maybe_set_user_scopes(Username, Scopes) of
        ok ->
            ?SLOG(info, #{
                msg => "create_dashboard_user_success",
                username => Username
            }),
            %% Re-read the persisted record so the response carries
            %% the final scopes / mfa state (the in-flight Result map
            %% from add_user/4 predates set_user_scopes and lacks
            %% these fields).
            {200, to_json_out(reload_external_user(Username, Result))};
        {error, <<"username_not_found">> = Reason} ->
            {404, ?USER_NOT_FOUND, Reason};
        {error, Reason} ->
            {400, ?BAD_REQUEST, Reason}
    end.

user(put, #{bindings := #{username := Username0}, body := Params} = Req) ->
    Role = maps:get(<<"role">>, Params, ?ROLE_DEFAULT),
    Desc = maps:get(<<"description">>, Params),
    Scopes = maps:get(<<"scopes">>, Params, undefined),
    Username = username(Req, Username0),
    case is_default_admin_modification(Username, Role, Scopes) of
        ok ->
            update_user(Username, Role, Desc, Scopes);
        {error, Msg} ->
            {400, ?NOT_ALLOWED, Msg}
    end;
user(delete, #{bindings := #{username := Username0}} = Req) ->
    %% Resolve the SSO target (e.g. `?backend=ldap' turns `Username0'
    %% into `{ldap, Username0}') before checking the break-glass
    %% protection — otherwise an SSO user that happens to share its
    %% name with `dashboard.default_username' would be wrongly rejected.
    Username = username(Req, Username0),
    case is_default_admin(Username) of
        true ->
            ?SLOG(info, #{
                msg => "dashboard_delete_admin_user_failed",
                username => Username,
                reason => "default admin user is protected"
            }),
            {400, ?NOT_ALLOWED, <<
                "The default administrator user cannot be deleted."
            >>};
        false ->
            handle_delete_user(Req)
    end.

%% The default administrator (configured via `dashboard.default_username')
%% is a break-glass account. Reject any modification that would weaken
%% it: role changes away from administrator, and explicit scope lists
%% (the role's implicit defaults must always apply). Empty
%% `dashboard.default_username' means no default user is in effect, so
%% the protection is a no-op.
is_default_admin_modification(Username, Role, Scopes) ->
    case is_default_admin(Username) of
        false ->
            ok;
        true ->
            case {Role, Scopes} of
                {?ROLE_SUPERUSER, undefined} ->
                    ok;
                {?ROLE_SUPERUSER, _} ->
                    {error, <<
                        "The default administrator cannot have an explicit "
                        "scope list; it always holds the full catalog."
                    >>};
                {_OtherRole, _} ->
                    {error, <<
                        "The default administrator role cannot be changed."
                    >>}
            end
    end.

is_default_admin(Username) when is_binary(Username) ->
    case emqx_dashboard_admin:default_username() of
        <<>> -> false;
        Default -> Username =:= Default
    end;
is_default_admin(_NonLocalTarget) ->
    %% SSO targets (e.g. `{ldap, Username}') are never the local
    %% break-glass account, even if their username happens to match
    %% `dashboard.default_username'.
    false.

handle_delete_user(#{bindings := #{username := Username0}, headers := Headers} = Req) ->
    Username = username(Req, Username0),
    case is_self_auth(Username0, Headers) of
        true ->
            {400, ?NOT_ALLOWED, <<"Cannot delete self">>};
        false ->
            case emqx_dashboard_admin:remove_user(Username) of
                {error, Reason} ->
                    {404, ?USER_NOT_FOUND, Reason};
                {ok, _} ->
                    ?SLOG(info, #{
                        msg => "dashboard_delete_admin_user", username => Username0
                    }),
                    {204}
            end
    end.

is_self_auth(?SSO_USERNAME(_, _), _) ->
    false;
is_self_auth(Username, #{<<"authorization">> := Token}) ->
    is_self_auth(Username, Token);
is_self_auth(Username, #{<<"Authorization">> := Token}) ->
    is_self_auth(Username, Token);
is_self_auth(Username, <<"basic ", Token/binary>>) ->
    is_self_auth_basic(Username, Token);
is_self_auth(Username, <<"Basic ", Token/binary>>) ->
    is_self_auth_basic(Username, Token);
is_self_auth(Username, <<"bearer ", Token/binary>>) ->
    is_self_auth_token(Username, Token);
is_self_auth(Username, <<"Bearer ", Token/binary>>) ->
    is_self_auth_token(Username, Token).

is_self_auth_basic(Username, Token) ->
    UP = base64:decode(Token),
    case binary:match(UP, Username) of
        {0, N} ->
            binary:part(UP, {N, 1}) == <<":">>;
        _ ->
            false
    end.

is_self_auth_token(Username, Token) ->
    case emqx_dashboard_token:owner(Token) of
        {ok, Owner} ->
            Owner == Username;
        {error, _NotFound} ->
            false
    end.

change_pwd(post, #{bindings := #{username := Username}, body := Params}) ->
    LogMeta = #{msg => "dashboard_change_password", username => binary_to_list(Username)},
    OldPwd = maps:get(<<"old_pwd">>, Params),
    NewPwd = maps:get(<<"new_pwd">>, Params),
    case ?EMPTY(OldPwd) orelse ?EMPTY(NewPwd) of
        true ->
            ?SLOG(error, LogMeta#{result => failed, reason => "password_undefined_or_empty"}),
            {400, ?BAD_REQUEST, <<"Old password or new password undefined">>};
        false ->
            case emqx_dashboard_admin:change_password(Username, OldPwd, NewPwd) of
                {ok, _} ->
                    ?SLOG(info, LogMeta#{result => success}),
                    {204};
                {error, <<"username_not_found">>} ->
                    ?SLOG(error, LogMeta#{result => failed, reason => "username not found"}),
                    {404, ?USER_NOT_FOUND, <<"User not found">>};
                {error, <<"password_error">>} ->
                    ?SLOG(error, LogMeta#{result => failed, reason => "error old pwd"}),
                    {400, ?ERROR_PWD_NOT_MATCH, <<"Old password not match">>};
                {error, Reason} ->
                    ?SLOG(error, LogMeta#{result => failed, reason => Reason}),
                    {400, ?BAD_REQUEST, Reason}
            end
    end.

change_mfa(delete, #{bindings := #{username := Username0}} = Req) ->
    Username = username(Req, Username0),
    LogMeta = #{msg => "dashboard_user_mfa_disable", username => Username},
    case authorize_mfa_change(Req, Username, disable) of
        ok ->
            ByAdmin = caller_is_admin_acting_on_other(Req, Username),
            case emqx_dashboard_admin:disable_mfa(Username, ByAdmin) of
                ok ->
                    ?SLOG(info, LogMeta#{result => success}),
                    {204};
                {error, <<"username_not_found">>} ->
                    ?SLOG(error, LogMeta#{result => failed, reason => "username not found"}),
                    {404, ?USER_NOT_FOUND, <<"User not found">>};
                {error, Reason} ->
                    ?SLOG(error, LogMeta#{result => failed, reason => Reason}),
                    {400, ?BAD_REQUEST, Reason}
            end;
        {deny, Code, ErrCode, Msg} ->
            ?SLOG(warning, LogMeta#{result => denied, reason => ErrCode}),
            {Code, ErrCode, Msg}
    end;
change_mfa(post, #{bindings := #{username := Username0}, body := Settings} = Req) ->
    Username = username(Req, Username0),
    Mechanism = maps:get(<<"mechanism">>, Settings),
    LogMeta = #{msg => "dashboard_user_mfa_setup", username => Username},
    case authorize_mfa_change(Req, Username, setup) of
        ok ->
            ByAdmin = caller_is_admin_acting_on_other(Req, Username),
            case emqx_dashboard_admin:reinit_mfa(Username, Mechanism, ByAdmin) of
                ok ->
                    ?SLOG(info, LogMeta#{result => success}),
                    {204};
                {error, <<"username_not_found">>} ->
                    ?SLOG(error, LogMeta#{result => failed, reason => "username not found"}),
                    {404, ?USER_NOT_FOUND, <<"User not found">>};
                {error, Reason} ->
                    ?SLOG(error, LogMeta#{result => failed, reason => Reason}),
                    {400, ?BAD_REQUEST, Reason}
            end;
        {deny, Code, ErrCode, Msg} ->
            ?SLOG(warning, LogMeta#{result => denied, reason => ErrCode}),
            {Code, ErrCode, Msg}
    end.

register_unsuccessful_login(Username, <<"password_error">>) ->
    emqx_dashboard_login_lock:register_unsuccessful_login(Username);
register_unsuccessful_login(_, _) ->
    ok.

%% --- login user scope schema validation ---
%%
%% Two-layer rule:
%%   * Any unknown scope name is rejected.
%%   * Non-administrator role users cannot hold any of the admin-only
%%     subset (user_management, sso_management, api_key_management).
%%     mfa_management is intentionally allowed for any role — non-
%%     admin holders can self-exempt their own MFA but cannot manage
%%     other users' MFA (handler-level enforcement).
%% @doc Resolve the role-default scopes that should be materialized into
%% the persisted record when the caller did not explicitly supply a
%% scope list (POST without `scopes', SSO auto-provisioning).
%%
%% After this PR, `undefined' is never written into mnesia by any
%% creation path; the `<<"unset">>' state in the GET response is only
%% possible for records that survived an upgrade from a release where
%% the dashboard-user scopes feature did not exist (#17235).
%%
%%   * administrator -> `?GENERIC_SCOPES ++ ?LOGIN_ONLY_SCOPES'
%%   * viewer        -> `?GENERIC_SCOPES'
%%
%% Explicit `[]' (deny-all) and explicit lists pass through unchanged.
effective_scopes_on_create(Role, undefined) ->
    emqx_dashboard_admin:role_default_scopes(Role);
effective_scopes_on_create(_Role, Scopes) when is_list(Scopes) ->
    Scopes;
effective_scopes_on_create(_Role, Other) ->
    %% Any non-list, non-undefined value is left as-is; downstream
    %% validation will reject it with the appropriate 400.
    Other.

validate_login_user_scopes(_Role, undefined) ->
    ok;
validate_login_user_scopes(_Role, Scopes) when not is_list(Scopes) ->
    {error, <<"scopes must be a list of strings">>};
validate_login_user_scopes(Role, Scopes) ->
    case validate_scope_names(Scopes) of
        ok ->
            validate_role_scope_compat(Role, Scopes);
        Error ->
            Error
    end.

%% Login users may hold ANY of the API key catalog scopes plus the
%% four login-only scopes. Any name outside this combined set is a
%% typo or an attempt to assign $denied — reject.
validate_scope_names(Scopes) ->
    Catalogue = [N || #{name := N} <- emqx_scope_catalog:scope_catalog()],
    Allowed = Catalogue ++ ?LOGIN_ONLY_SCOPES,
    case [S || S <- Scopes, not lists:member(S, Allowed)] of
        [] ->
            ok;
        Unknown ->
            Names = lists:join(<<", ">>, Unknown),
            {error, iolist_to_binary([<<"Unknown scope name(s): ">>, Names])}
    end.

validate_role_scope_compat(Role, Scopes) ->
    %% Parse the role to extract the base role name and namespace,
    %% so that namespaced administrator roles (e.g.
    %% "ns:test::administrator") are correctly recognised.
    case emqx_dashboard_rbac:parse_dashboard_role(Role) of
        {ok, #{?role := ?ROLE_SUPERUSER, ?namespace := ?global_ns}} ->
            ok;
        {ok, #{?role := ?ROLE_SUPERUSER, ?namespace := _}} ->
            %% Namespaced administrator: only the restricted subset
            %% is allowed.  RBAC is the primary gate — most mutating
            %% operations on non-whitelisted endpoints are already
            %% blocked by do_check_rbac/3 (catch-all returns false).
            %% Scope check is defense-in-depth.
            case [S || S <- Scopes, not lists:member(S, ?NS_ADMIN_ALLOWED_SCOPES)] of
                [] ->
                    ok;
                Forbidden ->
                    Names = lists:join(<<", ">>, Forbidden),
                    {error,
                        iolist_to_binary([
                            <<"Namespaced administrators cannot hold scopes: ">>, Names
                        ])}
            end;
        {ok, _} ->
            case [S || S <- Scopes, lists:member(S, ?ADMIN_ONLY_SCOPES)] of
                [] ->
                    ok;
                Conflicts ->
                    Names = lists:join(<<", ">>, Conflicts),
                    Msg = iolist_to_binary([
                        <<"Non-administrator users cannot hold admin-only scopes: ">>, Names
                    ]),
                    {error, Msg}
            end;
        {error, Msg} ->
            {error, Msg}
    end.

%% Run the validate → update_user → set_scopes pipeline. Mirrors
%% create_user/5 above, also kept as a helper to stay within elvis's
%% nesting cap.
%%
%% Validation runs against the *effective* scope list — the request
%% body's `scopes' field when present, otherwise the persisted scopes —
%% so a role demotion can never silently keep stale admin-only scopes
%% just because the client omitted the `scopes' field.
update_user(Username, Role, Desc, Scopes) ->
    EffectiveScopes = effective_request_scopes(Username, Scopes),
    case validate_login_user_scopes(Role, EffectiveScopes) of
        ok ->
            do_update_user(Username, Role, Desc, Scopes);
        {error, Msg} ->
            {400, ?BAD_REQUEST, Msg}
    end.

%% Fall back to persisted scopes only when the body did not supply a
%% `scopes' field. An explicit list (including `[]') is taken verbatim.
effective_request_scopes(_Username, Scopes) when is_list(Scopes) ->
    Scopes;
effective_request_scopes(Username, undefined) ->
    emqx_dashboard_admin:scopes_of(Username).

do_update_user(Username, Role, Desc, Scopes) ->
    case emqx_dashboard_admin:update_user(Username, Role, Desc) of
        {ok, Result} ->
            finalise_update_user(Username, Scopes, Result);
        {error, <<"username_not_found">> = Reason} ->
            {404, ?USER_NOT_FOUND, Reason};
        {error, Reason} ->
            {400, ?BAD_REQUEST, Reason}
    end.

finalise_update_user(Username, Scopes, Result) ->
    case maybe_set_user_scopes(Username, Scopes) of
        ok ->
            {200, to_json_out(reload_external_user(Username, Result))};
        {error, <<"username_not_found">> = Reason} ->
            {404, ?USER_NOT_FOUND, Reason};
        {error, Reason} ->
            {400, ?BAD_REQUEST, Reason}
    end.

%% Re-read the admin record after a write and project it via
%% to_external_user/1 so the response carries the canonical, persisted
%% shape (username, role, description, backend, mfa, scopes). Falls
%% back to the original in-flight map if the record vanished — that
%% means a concurrent delete won, and the caller has already returned
%% 200 OK so we still need a body.
reload_external_user(Username, Fallback) ->
    case emqx_dashboard_admin:lookup_user(Username) of
        [Admin] -> emqx_dashboard_admin:to_external_user(Admin);
        _ -> Fallback
    end.

%% Persist scopes to the admin record's extra map. Skip when scopes is
%% absent (no body field) — keeps the existing extra.scopes value.
%%
%% Returns {error, Reason} when the user record is missing (e.g. concurrent
%% deletion between add_user/update_user and this call). Callers must
%% translate to the proper HTTP status; never crash the handler.
maybe_set_user_scopes(_Username, undefined) ->
    ok;
maybe_set_user_scopes(Username, Scopes) when is_list(Scopes) ->
    case emqx_dashboard_admin:set_user_scopes(Username, Scopes) of
        {ok, ok} ->
            ok;
        {error, Reason} ->
            ?SLOG(warning, #{
                msg => "set_user_scopes_failed",
                username => Username,
                reason => Reason
            }),
            {error, Reason}
    end.

%% --- MFA self-lock authorization ---
%%
%% Decision matrix:
%%
%%   IsFirstSetup = (Op == setup) AND (target.mfa_state == not_configured)
%%   IsSelf       = (caller.username == target)
%%   HasMfaMgmt   = caller.scopes contains mfa_management
%%   Locked       = target.admin_override == mfa_required
%%
%%   IsFirstSetup => allow                                  (deadlock prevention)
%%   IsSelf       AND HasMfaMgmt           => allow         (self-exempt)
%%   IsSelf       AND NOT HasMfaMgmt AND NOT Locked => allow
%%   IsSelf       AND NOT HasMfaMgmt AND Locked     => deny mfa_locked
%%   NOT IsSelf   AND HasMfaMgmt AND administrator   => allow (admin reset)
%%   NOT IsSelf   AND HasMfaMgmt AND non-admin       => deny self_only
%%   NOT IsSelf   AND NOT HasMfaMgmt                 => deny missing_mfa_mgmt
%%
%% Returns:
%%   ok                                          allow
%%   {deny, HttpCode, ErrorCode, BinaryMessage}  deny with HTTP response
authorize_mfa_change(Req, TargetUsername, Op) ->
    Caller = caller_admin(Req),
    case Caller of
        undefined ->
            %% No bearer token caller (shouldn't happen — bearer-only
            %% endpoint, but guard anyway).
            {deny, 401, 'UNAUTHORIZED', <<"Bearer auth required">>};
        _ ->
            authorize_mfa_change_with_caller(Caller, TargetUsername, Op)
    end.

authorize_mfa_change_with_caller(Caller, TargetUsername, Op) ->
    IsSelf = (Caller#?ADMIN.username =:= TargetUsername),
    CallerRole = Caller#?ADMIN.role,
    HasMfaMgmt = caller_has_mfa_mgmt(Caller),
    IsFirstSetup = is_first_time_setup(TargetUsername, Op),
    Locked = target_self_locked(TargetUsername),
    case {IsFirstSetup, IsSelf, HasMfaMgmt, Locked, CallerRole} of
        {true, _, _, _, _} ->
            ok;
        {false, true, true, _, _} ->
            ok;
        {false, true, false, false, _} ->
            ok;
        {false, true, false, true, _} ->
            {deny, 403, 'MFA_LOCKED', <<
                "MFA changes for this account are restricted; "
                "the mfa_management scope is required to override."
            >>};
        {false, false, true, _, ?ROLE_SUPERUSER} ->
            ok;
        {false, false, true, _, _NonAdmin} ->
            {deny, 403, 'MFA_SELF_ONLY', <<
                "Non-administrator users with mfa_management scope can "
                "only manage their own MFA, not other users'."
            >>};
        {false, false, false, _, _} ->
            {deny, 403, 'MFA_MGMT_REQUIRED', <<
                "The mfa_management scope is required to manage other "
                "users' MFA."
            >>}
    end.

is_first_time_setup(_TargetUsername, disable) ->
    false;
is_first_time_setup(TargetUsername, setup) ->
    case emqx_dashboard_admin:get_mfa_state(TargetUsername) of
        {ok, _} -> false;
        _ -> true
    end.

%% Compute the "self locked" boolean used by authorize_mfa_change/3.
%% Only admin_override == mfa_required locks self-disable/rotate.
%% undefined means "no admin decision" — self may always disable an
%% already-configured MFA. The decision whether a user must SET UP
%% MFA in the first place lives in the login flow (consults backend
%% live force_mfa), not here.
target_self_locked(TargetUsername) ->
    emqx_dashboard_admin:admin_override_of(TargetUsername) =:= ?ADMIN_MFA_REQUIRED.

caller_has_mfa_mgmt(#?ADMIN{} = Caller) ->
    %% Use effective scopes so the role-default fallback (admin -> all
    %% 14, viewer -> common scopes only) applies. Viewer with no explicit
    %% scopes therefore does NOT carry mfa_management; admin always
    %% does.
    Scopes = emqx_dashboard_admin:effective_scopes_of_admin(Caller),
    lists:member(?SCOPE_MFA_MGMT, Scopes).

%% Look up the caller's #?ADMIN{} record using the bearer token's
%% `source' field (set by emqx_dashboard:authorize/2). Returns
%% undefined if not a bearer-token request or the user has been
%% deleted between login and this request.
caller_admin(#{auth_meta := #{auth_type := jwt_token, source := Username}}) ->
    %% auth_meta.source is the resolved admin record key — for SSO
    %% users this is `?SSO_USERNAME(Backend, Name)' (atom-tagged
    %% tuple), populated by emqx_dashboard:authorize/2 via
    %% emqx_dashboard_token:resolve_admin_key/1. Plain lookup is
    %% sufficient.
    case emqx_dashboard_admin:lookup_user(Username) of
        [Admin] -> Admin;
        _ -> undefined
    end;
caller_admin(_) ->
    undefined.

caller_is_admin_acting_on_other(Req, TargetUsername) ->
    case caller_admin(Req) of
        #?ADMIN{username = Caller} when Caller =/= TargetUsername -> true;
        _ -> false
    end.

mk(Type, Props) ->
    hoconsc:mk(Type, Props).

array(Type) ->
    hoconsc:array(Type).

enum(Symbols) ->
    hoconsc:enum(Symbols).

field_filter(_) ->
    true.

to_json_out(#{} = Result) ->
    maps:map(
        fun
            (_K, undefined) ->
                null;
            (_K, ?global_ns) ->
                null;
            (_K, V) ->
                V
        end,
        Result
    );
to_json_out(Results) when is_list(Results) ->
    lists:map(fun to_json_out/1, Results);
to_json_out(Result) ->
    Result.

sso_parameters() ->
    sso_parameters([]).

sso_parameters(Params) ->
    emqx_dashboard_sso_api:sso_parameters(Params).

username(#{query_string := #{<<"backend">> := ?BACKEND_LOCAL}}, Username) ->
    Username;
username(#{query_string := #{<<"backend">> := Backend}}, Username) ->
    ?SSO_USERNAME(Backend, Username);
username(_Req, Username) ->
    Username.
