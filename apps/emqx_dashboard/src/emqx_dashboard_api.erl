%%--------------------------------------------------------------------
%% Copyright (c) 2020-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_dashboard_api).

-behaviour(minirest_api).

-include("emqx_dashboard.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("typerefl/include/types.hrl").
-include_lib("emqx_utils/include/emqx_api_key_scopes.hrl").

-export([
    api_spec/0,
    fields/1,
    paths/0,
    schema/1,
    namespace/0
]).

-export([scopes/0]).

-export([
    login/2,
    logout/2,
    users/2,
    user_scopes/2,
    user/2,
    change_pwd/2,
    change_mfa/2
]).

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

%% API key auth is rejected at the minirest layer for these paths
%% (security => [#{bearerAuth => []}] excludes basic auth). The scope
%% map below applies to dashboard LOGIN users — checked in
%% emqx_dashboard_rbac:check_login_user_scopes/2.
%%
%% Public paths (/login, /logout) are intentionally absent from the map;
%% they fall through to the unmapped-path branch (fail-open) and are
%% guarded by their own security => [] / bearerAuth declarations.
scopes() ->
    #{
        <<"/users">> => ?SCOPE_USER_MGMT,
        <<"/users/:username">> => ?SCOPE_USER_MGMT,
        <<"/users/:username/change_pwd">> => ?SCOPE_USER_MGMT,
        <<"/users/:username/mfa">> => ?SCOPE_MFA_MGMT
    }.

api_spec() ->
    emqx_dashboard_swagger:spec(?MODULE, #{check_schema => true, translate_body => true}).

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
            tags => [<<"Dashboard">>],
            desc => ?DESC(login_api),
            summary => <<"Dashboard authentication">>,
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
            tags => [<<"Dashboard">>],
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
            tags => [<<"Dashboard">>],
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
            tags => [<<"Dashboard">>],
            desc => ?DESC(create_user_api),
            security => [#{'bearerAuth' => []}],
            'requestBody' => fields([username, password, role, description, scopes]),
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
            tags => [<<"Dashboard">>],
            desc => ?DESC(update_user_api),
            parameters => sso_parameters(fields([username_in_path])),
            'requestBody' => fields([role, description, scopes]),
            responses => #{
                200 => user_fields(),
                404 => response_schema(404)
            }
        },
        delete => #{
            tags => [<<"Dashboard">>],
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
            tags => [<<"Dashboard">>],
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
            tags => [<<"Dashboard">>],
            desc => ?DESC(change_mfa),
            parameters => sso_parameters(fields([username_in_path])),
            'requestBody' => emqx_dashboard_schema:mfa_fields(),
            responses => #{
                204 => <<"MFA setting is updated">>,
                404 => response_schema(404)
            }
        },
        delete => #{
            tags => [<<"Dashboard">>],
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
    fields([username, role, description, backend, scopes]) ++ ee_user_fields().

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
field(scopes) ->
    {scopes,
        mk(hoconsc:array(binary()), #{
            desc => ?DESC(user_scopes),
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
                filter_result(Result#{
                    version => Version,
                    license => #{edition => emqx_release:edition()}
                })};
        {error, R} ->
            ok = register_unsuccessful_login(Username, R),
            ?SLOG(info, #{msg => "dashboard_login_failed", username => Username, reason => R}),
            format_login_failed_error(R)
    end.

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
    {200, #{scopes => emqx_scope_catalog:login_user_scope_catalog()}}.

users(get, _Request) ->
    {200, filter_result(emqx_dashboard_admin:all_users())};
users(post, #{body := Params}) ->
    Desc = maps:get(<<"description">>, Params, <<"">>),
    Role = maps:get(<<"role">>, Params, ?ROLE_DEFAULT),
    Username = maps:get(<<"username">>, Params),
    Password = maps:get(<<"password">>, Params),
    Scopes = maps:get(<<"scopes">>, Params, undefined),
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
            {200, filter_result(reload_external_user(Username, Result))};
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
    update_user(Username, Role, Desc, Scopes);
user(delete, #{bindings := #{username := Username}} = Req) ->
    DefaultUsername = emqx_dashboard_admin:default_username(),
    case Username == DefaultUsername of
        true ->
            handle_delete_default_admin(Req);
        false ->
            handle_delete_user(Req)
    end.

handle_delete_default_admin(#{bindings := #{username := Username0}} = Req) ->
    AllAdminUsers = emqx_dashboard_admin:admin_users(),
    OtherAdminUsers = lists:filter(fun(#{username := U}) -> U /= Username0 end, AllAdminUsers),
    case OtherAdminUsers of
        [_ | _] ->
            %% There is at least one other admin user; we may delete the default user.
            handle_delete_user(Req);
        [] ->
            %% There's no other admin user.
            ?SLOG(info, #{msg => "dashboard_delete_admin_user_failed", username => Username0}),
            Message = list_to_binary(io_lib:format("Cannot delete user ~p", [Username0])),
            {400, ?NOT_ALLOWED, Message}
    end.

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

validate_role_scope_compat(?ROLE_SUPERUSER, _Scopes) ->
    ok;
validate_role_scope_compat(_NonAdminRole, Scopes) ->
    case [S || S <- Scopes, lists:member(S, ?ADMIN_ONLY_SCOPES)] of
        [] ->
            ok;
        Conflicts ->
            Names = lists:join(<<", ">>, Conflicts),
            Msg = iolist_to_binary([
                <<"Non-administrator users cannot hold admin-only scopes: ">>, Names
            ]),
            {error, Msg}
    end.

%% Run the validate → update_user → set_scopes pipeline. Mirrors
%% create_user/5 above, also kept as a helper to stay within elvis's
%% nesting cap.
update_user(Username, Role, Desc, Scopes) ->
    case validate_login_user_scopes(Role, Scopes) of
        ok ->
            do_update_user(Username, Role, Desc, Scopes);
        {error, Msg} ->
            {400, ?BAD_REQUEST, Msg}
    end.

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
            {200, filter_result(reload_external_user(Username, Result))};
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
%%
%% CE: scope enforcement is EE-only (emqx_dashboard_rbac is EE), so we
%% ignore any `scopes' input rather than persist a value that would
%% never be checked.
-if(?EMQX_RELEASE_EDITION == ee).
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
-else.
maybe_set_user_scopes(_Username, _Scopes) ->
    ok.
-endif.

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
    %% 14, viewer -> 10 generic) applies. Viewer with no explicit
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

-if(?EMQX_RELEASE_EDITION == ee).
field_filter(_) ->
    true.

filter_result(Result) ->
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

-else.

field_filter(role) ->
    false;
field_filter(scopes) ->
    %% Login-user scopes are an EE feature (enforcement lives in
    %% emqx_dashboard_rbac, which is EE-only). Strip the field from
    %% the schema so CE clients don't see a column that won't be
    %% checked.
    false;
field_filter(_) ->
    true.

filter_result(Result) when is_list(Result) ->
    lists:map(fun filter_result/1, Result);
filter_result(Result) ->
    maps:without([role, backend, scopes], Result).

sso_parameters() ->
    sso_parameters([]).

sso_parameters(Any) ->
    Any.

username(_Req, Username) ->
    Username.
-endif.
