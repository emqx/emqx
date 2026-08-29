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
    scram_challenge/2,
    scram_verify/2,
    logout/2,
    users/2,
    user_scopes/2,
    user/2,
    change_pwd/2,
    change_mfa/2,
    current_user/2,
    current_user_change_pwd/2,
    current_user_mfa/2
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
-define(PASSWORD_LOGIN_DISABLED, 'PASSWORD_LOGIN_DISABLED').
-define(SCRAM_CHALLENGE_INVALID, 'SCRAM_CHALLENGE_INVALID').
-define(SERVICE_UNAVAILABLE, 'SERVICE_UNAVAILABLE').
-define(MFA_ADMIN_REQUIRED, 'MFA_ADMIN_REQUIRED').

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
%%   * /current_user* -- self-service; the caller is the subject, so the
%%     authenticated identity alone authorizes the operation. A scope
%%     check here would let an explicit `scopes = []' lock a user out of
%%     their own password and MFA, which is what the scope layer exists
%%     to gate for OTHER users, not for the holder's own account. None of
%%     these operations can widen the caller's own privileges:
%%     change_pwd verifies `old_pwd', and the MFA routes only ever touch
%%     the caller's own record.
%%   * /users/:username/change_pwd -- the deprecated self-only shim. It
%%     carries no scope for the same reason as `/current_user/change_pwd':
%%     the handler asserts the target is the caller and then acts on the
%%     caller's own record, so there is nothing here to gate per-scope.
%%     Dropping `user_management' is what lets a viewer keep using its
%%     old password-change call.
scopes() ->
    #{
        <<"/login">> => ?SCOPE_PUBLIC,
        <<"/login/challenge">> => ?SCOPE_PUBLIC,
        <<"/login/verify">> => ?SCOPE_PUBLIC,
        <<"/logout">> => ?SCOPE_PUBLIC,
        <<"/user_scopes">> => ?SCOPE_PUBLIC,
        <<"/current_user">> => ?SCOPE_PUBLIC,
        <<"/current_user/change_pwd">> => ?SCOPE_PUBLIC,
        <<"/current_user/mfa">> => ?SCOPE_PUBLIC,
        <<"/users">> => ?SCOPE_USER_MGMT,
        <<"/users/:username">> => ?SCOPE_USER_MGMT,
        <<"/users/:username/change_pwd">> => ?SCOPE_PUBLIC,
        <<"/users/:username/mfa">> => ?SCOPE_MFA_MGMT
    }.

paths() ->
    [
        "/login",
        "/login/challenge",
        "/login/verify",
        "/logout",
        "/current_user",
        "/current_user/change_pwd",
        "/current_user/mfa",
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
                401 => emqx_dashboard_swagger:error_codes(ErrorCodes, ?DESC(login_failed401)),
                403 => emqx_dashboard_swagger:error_codes(
                    [?PASSWORD_LOGIN_DISABLED], ?DESC(login_failed_response400)
                )
            },
            security => []
        }
    };
schema("/login/challenge") ->
    #{
        'operationId' => scram_challenge,
        post => #{
            tags => [<<"dashboard">>],
            desc => ?DESC(login_api),
            'requestBody' => fields([username, client_nonce]),
            responses => #{
                200 => fields([mechanism, challenge_id, salt, iterations, server_nonce]),
                400 => response_schema(400),
                503 => response_schema(503)
            },
            security => []
        }
    };
schema("/login/verify") ->
    #{
        'operationId' => scram_verify,
        post => #{
            tags => [<<"dashboard">>],
            desc => ?DESC(login_api),
            'requestBody' => fields([challenge_id, combined_nonce, client_proof, mfa_token]),
            responses => #{
                200 => fields([
                    role,
                    token,
                    version,
                    license,
                    password_expire_in_seconds,
                    server_signature
                ]),
                400 => response_schema(400),
                401 => emqx_dashboard_swagger:error_codes(
                    [?BAD_USERNAME_OR_PWD, ?BAD_MFA_TOKEN, ?LOGIN_LOCKED, ?SCRAM_CHALLENGE_INVALID],
                    ?DESC(login_failed401)
                ),
                503 => response_schema(503)
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
%% Self-service routes. `/current_user' is a top-level path outside
%% `/users/', so it cannot be shadowed by -- or shadow -- a user whose
%% name happens to be `current_user', `self' or `me'; those are all
%% legal usernames and there is no reserved-word check on them.
%%
%% Every operationId is prefixed `current_user_' because minirest
%% requires globally unique operationIds and the admin routes already
%% own `change_pwd' / `change_mfa'.
schema("/current_user") ->
    #{
        'operationId' => current_user,
        get => #{
            tags => [<<"dashboard">>],
            desc => ?DESC(current_user_api),
            security => [#{'bearerAuth' => []}],
            responses => #{
                200 => current_user_fields(),
                404 => response_schema(404)
            }
        }
    };
schema("/current_user/change_pwd") ->
    #{
        'operationId' => current_user_change_pwd,
        post => #{
            tags => [<<"dashboard">>],
            desc => ?DESC(current_user_change_pwd_api),
            security => [#{'bearerAuth' => []}],
            'requestBody' => fields([old_pwd, new_pwd]),
            responses => #{
                204 => <<"Password is updated">>,
                404 => response_schema(404),
                400 => emqx_dashboard_swagger:error_codes(
                    [?BAD_REQUEST, ?ERROR_PWD_NOT_MATCH, ?NOT_ALLOWED],
                    ?DESC(login_failed_response400)
                )
            }
        }
    };
schema("/current_user/mfa") ->
    #{
        'operationId' => current_user_mfa,
        post => #{
            tags => [<<"dashboard">>],
            desc => ?DESC(current_user_mfa_api),
            security => [#{'bearerAuth' => []}],
            'requestBody' => emqx_dashboard_schema:mfa_fields(),
            responses => #{
                204 => <<"MFA setting is updated">>,
                404 => response_schema(404)
            }
        },
        delete => #{
            tags => [<<"dashboard">>],
            desc => ?DESC(delete_current_user_mfa_api),
            security => [#{'bearerAuth' => []}],
            responses => #{
                204 => <<"MFA setting is disabled">>,
                403 => emqx_dashboard_swagger:error_codes(
                    [?MFA_ADMIN_REQUIRED], ?DESC(current_user_mfa_admin_required)
                ),
                404 => response_schema(404)
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
%% Deprecated self-only shim for `/current_user/change_pwd'. Kept so the
%% heavily-integrated password-change call keeps working; scheduled for
%% removal a release later.
%%
%% This is not the old endpoint with a new name. The old one was reachable
%% for any target and merely failed on the `old_pwd' check, so a cross-user
%% call was refused by accident. The shim asserts the target is the caller
%% and answers 403 otherwise, which also makes the namespaced-administrator
%% invariant deliberate rather than incidental.
schema("/users/:username/change_pwd") ->
    #{
        'operationId' => change_pwd,
        post => #{
            tags => [<<"dashboard">>],
            desc => ?DESC(change_pwd_api),
            deprecated => true,
            security => [#{'bearerAuth' => []}],
            parameters => fields([username_in_path]),
            'requestBody' => fields([old_pwd, new_pwd]),
            responses => #{
                204 => <<"Update user password successfully">>,
                403 => emqx_dashboard_swagger:error_codes(
                    [?NOT_ALLOWED], ?DESC(change_pwd_self_only)
                ),
                404 => response_schema(404),
                400 => emqx_dashboard_swagger:error_codes(
                    [?BAD_REQUEST, ?ERROR_PWD_NOT_MATCH, ?NOT_ALLOWED],
                    ?DESC(login_failed_response400)
                )
            }
        }
    };
%% Administrator-only: the target is always another user. A caller
%% managing its own MFA uses `/current_user/mfa'.
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
                400 => emqx_dashboard_swagger:error_codes(
                    [?NOT_ALLOWED], ?DESC(login_failed_response400)
                ),
                404 => response_schema(404)
            }
        },
        delete => #{
            tags => [<<"dashboard">>],
            desc => ?DESC(delete_mfa),
            parameters => sso_parameters(fields([username_in_path])),
            responses => #{
                204 => <<"MFA setting is disabled">>,
                400 => emqx_dashboard_swagger:error_codes(
                    [?NOT_ALLOWED], ?DESC(login_failed_response400)
                ),
                404 => response_schema(404)
            }
        }
    }.

response_schema(401) ->
    emqx_dashboard_swagger:error_codes([?BAD_USERNAME_OR_PWD], ?DESC(login_failed401));
response_schema(400) ->
    emqx_dashboard_swagger:error_codes([?BAD_REQUEST], ?DESC(login_failed_response400));
response_schema(503) ->
    emqx_dashboard_swagger:error_codes([?SERVICE_UNAVAILABLE], ?DESC(login_failed_response400));
response_schema(404) ->
    emqx_dashboard_swagger:error_codes([?USER_NOT_FOUND], ?DESC(users_api404)).

fields(user) ->
    user_fields();
fields(List) ->
    [field(Key) || Key <- List, field_filter(Key)].

user_fields() ->
    fields([username, role, description, backend, scopes_response]) ++ ee_user_fields().

%% Same shape as `user_fields/0', but `scopes' carries the EFFECTIVE
%% list rather than the raw tri-state one. The admin routes report the
%% raw value so that a read-modify-write of `PUT /users/:username' does
%% not sediment the role default into an explicit list; this endpoint
%% has no write counterpart, and its caller is asking what it may
%% actually do, so the expanded list is the useful answer.
current_user_fields() ->
    fields([username, role, description, backend, effective_scopes_response]) ++ ee_user_fields().

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
    %% Accept the same shapes the response emits, so a read-modify-write can
    %% round-trip the value verbatim: an explicit array of scope names, or the
    %% `unset' sentinel (parsed to the atom `unset', also tolerated as the
    %% binary <<"unset">>). A list whose set equals the role default and the
    %% `unset' sentinel are both treated as "no explicit scopes" on write.
    {scopes,
        mk(hoconsc:union([unset, hoconsc:array(binary())]), #{
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
field(effective_scopes_response) ->
    %% Always a concrete list -- the role default is expanded, so the
    %% `unset' sentinel of `scopes_response' cannot appear here.
    {scopes,
        mk(hoconsc:array(binary()), #{
            desc => ?DESC(effective_user_scopes_response),
            required => true,
            example => [?SCOPE_MONITORING, ?SCOPE_CONNECTIONS]
        })};
field(backend) ->
    {backend, mk(binary(), #{desc => ?DESC(backend), example => <<"local">>})};
field(password_expire_in_seconds) ->
    {password_expire_in_seconds,
        mk(integer(), #{desc => ?DESC(password_expire_in_seconds), example => 3600})};
field(mechanism) ->
    {mechanism, mk(binary(), #{example => <<"SCRAM-SHA-256">>})};
field(challenge_id) ->
    {challenge_id,
        mk(binary(), #{
            required => true,
            'maxLength' => 128,
            example => <<"0123456789abcdefghijklmnopqrstuv">>
        })};
field(client_nonce) ->
    {client_nonce,
        mk(binary(), #{
            desc => ?DESC(client_nonce),
            required => true,
            'maxLength' => 128,
            example => <<"clientNonce1234567890">>
        })};
field(server_nonce) ->
    {server_nonce, mk(binary(), #{example => <<"server-nonce">>})};
field(combined_nonce) ->
    {combined_nonce,
        mk(binary(), #{
            desc => ?DESC(combined_nonce),
            required => true,
            'maxLength' => 160,
            example => <<"clientNonce1234567890serverNonce1234567890">>
        })};
field(client_proof) ->
    {client_proof, mk(binary(), #{required => true, example => <<"base64-proof">>})};
field(salt) ->
    {salt, mk(binary(), #{example => <<"base64-salt">>})};
field(iterations) ->
    {iterations, mk(pos_integer(), #{example => 600000})};
field(server_signature) ->
    {server_signature, mk(binary(), #{example => <<"base64-signature">>})}.

decode_base64(Bin) when is_binary(Bin) ->
    try
        {ok, base64:decode(Bin)}
    catch
        _:_ -> error
    end;
decode_base64(_) ->
    error.

%% -------------------------------------------------------------------------------------------------
%% API

login(post, #{body := Params}) ->
    case emqx_dashboard_login:password_login_enabled() of
        false ->
            ?SLOG(error, #{
                msg => "dashboard_plaintext_password_login_disabled",
                endpoint => <<"/api/v5/login">>,
                password_login_mode => scram_only,
                username => maps:get(<<"username">>, Params, undefined)
            }),
            {403, ?PASSWORD_LOGIN_DISABLED, <<"Plaintext password login is disabled.">>};
        true ->
            login_enabled(post, Params)
    end.

login_enabled(post, Params) ->
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
            %% During first-time MFA setup the reason map carries the TOTP
            %% `secret', which is intentionally returned in the 401 response
            %% body so the dashboard can render the authenticator QR code. The
            %% server log must not keep a copy of it, so redact before logging.
            ?SLOG(info, #{
                msg => "dashboard_login_failed",
                username => Username,
                reason => emqx_utils:redact(R)
            }),
            format_login_failed_error(R)
    end.

scram_challenge(post, #{body := Params}) ->
    Username = maps:get(<<"username">>, Params),
    ClientNonce = maps:get(<<"client_nonce">>, Params),
    minirest_handler:update_log_meta(#{log_from => dashboard, log_source => Username}),
    case emqx_dashboard_login:scram_challenge(Username, ClientNonce) of
        {ok, Result} ->
            {200, to_json_out(Result)};
        {error, bad_request} ->
            ?SLOG(info, #{msg => "dashboard_scram_challenge_failed", username => Username}),
            {400, ?BAD_REQUEST, <<"Invalid SCRAM challenge request">>};
        {error, capacity} ->
            {503, ?SERVICE_UNAVAILABLE, <<"SCRAM challenge capacity exhausted">>};
        {error, _Reason} ->
            {503, ?SERVICE_UNAVAILABLE, <<"SCRAM challenge is unavailable">>}
    end.

scram_verify(post, #{body := Params}) ->
    ChallengeId = maps:get(<<"challenge_id">>, Params),
    CombinedNonce = maps:get(<<"combined_nonce">>, Params),
    Proof0 = maps:get(<<"client_proof">>, Params),
    MfaToken = maps:get(<<"mfa_token">>, Params, ?NO_MFA_TOKEN),
    ScramUsername = scram_username(ChallengeId),
    maybe_update_scram_log_meta(ScramUsername),
    case decode_base64(Proof0) of
        {ok, ClientProof} ->
            case
                emqx_dashboard_login:scram_verify(
                    ChallengeId, CombinedNonce, ClientProof, MfaToken
                )
            of
                {ok, Result0} ->
                    ?SLOG(info, #{
                        msg => "dashboard_login_successful",
                        username => ScramUsername
                    }),
                    Version = iolist_to_binary(proplists:get_value(version, emqx_sys:info())),
                    {200,
                        to_json_out(Result0#{
                            version => Version,
                            license => #{edition => emqx_release:edition()}
                        })};
                {error, bad_request} ->
                    log_scram_failure(ScramUsername, bad_request),
                    {400, ?BAD_REQUEST, <<"Invalid SCRAM verification request">>};
                {error, invalid_challenge} ->
                    log_scram_failure(ScramUsername, invalid_challenge),
                    {401, ?SCRAM_CHALLENGE_INVALID, <<"SCRAM challenge is invalid or expired">>};
                {error, {storage_unavailable, _Reason}} ->
                    log_scram_failure(ScramUsername, storage_unavailable),
                    {503, ?SERVICE_UNAVAILABLE, <<"SCRAM challenge storage is unavailable">>};
                {error, password_error} ->
                    log_scram_failure(ScramUsername, password_error),
                    {401, ?BAD_USERNAME_OR_PWD, <<"Auth failed">>};
                {error, Reason} ->
                    log_scram_failure(ScramUsername, Reason),
                    format_login_failed_error(Reason)
            end;
        error ->
            log_scram_failure(ScramUsername, bad_proof_encoding),
            {400, ?BAD_REQUEST, <<"Invalid SCRAM proof encoding">>}
    end.

maybe_update_scram_log_meta(Username) ->
    case Username of
        Username0 when is_binary(Username0) ->
            minirest_handler:update_log_meta(#{log_from => dashboard, log_source => Username0});
        _ ->
            ok
    end.

scram_username(ChallengeId) ->
    case emqx_dashboard_login:owner(ChallengeId) of
        {ok, Username} -> Username;
        _ -> undefined
    end.

log_scram_failure(Username, Reason) ->
    ?SLOG(info, #{
        msg => "dashboard_login_failed",
        username => Username,
        reason => emqx_utils:redact(Reason)
    }).

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
    case ?EMPTY(Username) orelse ?EMPTY(Password) of
        true ->
            {400, ?BAD_REQUEST, <<"Username or password undefined">>};
        false ->
            create_user(Username, Password, Role, Desc, RawScopes)
    end.

%% Run the validate → add_user → set_scopes pipeline. Each step short-
%% circuits to the appropriate HTTP response, keeping users(post,...)
%% within elvis's nesting cap.
create_user(Username, Password, Role, Desc, RawScopes) ->
    case create_scope_intent(Role, RawScopes) of
        {ok, Intent} ->
            do_create_user(Username, Password, Role, Desc, Intent);
        {error, Msg} ->
            {400, ?BAD_REQUEST, Msg}
    end.

%% Resolve the storage intent for POST and run validation. Returns
%% `{ok, keep | unset | {set, [binary()]}}' or `{error, Msg}'.
%%
%%   * field omitted -> materialize the role default and store it (current
%%     behavior); validation runs with `RawScopes = undefined' so the
%%     privilege-scope mutex is not applied to the role-default mix.
%%   * `unset' sentinel or a list whose set equals the role default ->
%%     create the user with no explicit scopes (GET returns "unset").
%%   * any other list -> validate and store it verbatim.
create_scope_intent(Role, undefined) ->
    Scopes = emqx_dashboard_admin:role_default_scopes(Role),
    case validate_login_user_scopes(Role, undefined, Scopes) of
        ok -> {ok, {set, Scopes}};
        Error -> Error
    end;
create_scope_intent(Role, RawScopes) ->
    case write_scope_intent(Role, RawScopes) of
        unset ->
            {ok, unset};
        {set, Scopes} ->
            case validate_login_user_scopes(Role, Scopes, Scopes) of
                ok -> {ok, {set, Scopes}};
                Error -> Error
            end
    end.

do_create_user(Username, Password, Role, Desc, Intent) ->
    case emqx_dashboard_admin:add_user(Username, Password, Role, Desc) of
        {ok, Result} ->
            finalise_create_user(Username, Intent, Result);
        {error, Reason} ->
            ?SLOG(info, #{
                msg => "create_dashboard_user_failed",
                username => Username,
                reason => Reason
            }),
            {400, ?BAD_REQUEST, Reason}
    end.

finalise_create_user(Username, Intent, Result) ->
    case apply_scope_intent(Username, Intent) of
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
    RawScopes = maps:get(<<"scopes">>, Params, undefined),
    Username = username(Req, Username0),
    Intent = write_scope_intent(Role, RawScopes),
    case is_default_admin_modification(Username, Role, Intent) of
        ok ->
            update_user(Username, Role, Desc, Intent);
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
%% it: role changes away from administrator, and a genuinely different
%% explicit scope list (the role's implicit defaults must always apply).
%% `Intent' is the normalized write intent from `write_scope_intent/2':
%% `keep' (field omitted) and `unset' (the `unset' sentinel or a list
%% whose set equals the role default) are both "no explicit scopes" and
%% therefore accepted — this makes a read-modify-write of the admin (which
%% round-trips the expanded full catalog) succeed. Only `{set, _}' — a
%% list that differs from the role default — is rejected. Empty
%% `dashboard.default_username' means no default user is in effect, so
%% the protection is a no-op.
is_default_admin_modification(Username, Role, Intent) ->
    case is_default_admin(Username) of
        false ->
            ok;
        true ->
            case {Role, Intent} of
                {?ROLE_SUPERUSER, keep} ->
                    ok;
                {?ROLE_SUPERUSER, unset} ->
                    ok;
                {?ROLE_SUPERUSER, {set, _}} ->
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

handle_delete_user(#{bindings := #{username := Username0}} = Req) ->
    Username = username(Req, Username0),
    %% The caller is identified by the authenticated token, not by
    %% re-parsing the `Authorization' header: the header form varies
    %% (basic / bearer, either capitalisation) and only the token
    %% resolution knows the SSO key. `caller_key/1' returns the same
    %% admin-record key `username/2' produces, so the two are directly
    %% comparable for both local and SSO targets.
    case caller_key(Req) =:= Username of
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

%%--------------------------------------------------------------------
%% Self-service handlers (`/current_user/*')
%%
%% The subject is the authenticated identity itself. There is no
%% `:username' in the path, so there is nothing to spoof and nothing to
%% compare: `caller_key/1' IS the target. Authorization is therefore
%% complete once the request is authenticated -- RBAC lets any
%% authenticated dashboard user through, and the scope layer treats
%% these paths as public.
%%--------------------------------------------------------------------

current_user(get, Req) ->
    with_caller(Req, fun(#?ADMIN{username = Username} = Admin) ->
        Profile = emqx_dashboard_admin:to_external_user(Admin),
        %% `to_json_out/1' like every other emitter of this shape: it maps
        %% `?global_ns' to `null', and without it a global user reports
        %% `"namespace": "global"' here while `GET /users' reports `null'
        %% for the same account.
        {200, to_json_out(Profile#{scopes => emqx_dashboard_admin:effective_scopes_of(Username)})}
    end).

current_user_change_pwd(post, #{body := Params} = Req) ->
    with_caller(Req, fun(#?ADMIN{username = Username}) ->
        do_change_pwd(Username, Params)
    end).

%% Deprecated shim: assert the path target is the caller, then run the
%% canonical self operation. It always acts on `Username', the caller's
%% own resolved key, never on the path segment, so the path cannot steer
%% the write even if the match below were ever loosened.
change_pwd(post, #{bindings := #{username := Target}, body := Params} = Req) ->
    with_caller(Req, fun(#?ADMIN{username = Username}) ->
        case is_self_target(Username, Target) of
            true ->
                do_change_pwd(Username, Params);
            false ->
                ?SLOG(warning, #{
                    msg => "dashboard_change_password",
                    username => Target,
                    attempted_by => Username,
                    result => denied,
                    reason => "not_the_authenticated_user"
                }),
                {403, ?NOT_ALLOWED, <<
                    "This endpoint only changes your own password. "
                    "Use /current_user/change_pwd."
                >>}
        end
    end).

%% The route carries no `?backend' parameter, so an SSO caller's key
%% (`{Backend, Name}') never equals the bare path segment. Match on the
%% name part as well, so an SSO user reaches `do_change_pwd/2' and gets
%% the "no local password" answer rather than a misleading 403.
is_self_target(Username, Username) -> true;
is_self_target(?SSO_USERNAME(_Backend, Name), Name) -> true;
is_self_target(_Caller, _Target) -> false.

%% An SSO user has no local password -- the identity provider owns the
%% credential -- so there is nothing here to change. Reject explicitly:
%% `emqx_dashboard_admin:change_password/3' is guarded on a binary
%% username and would otherwise raise a function_clause on the
%% `?SSO_USERNAME' tuple, turning this into a 500.
do_change_pwd(?SSO_USERNAME(Backend, Name), _Params) ->
    ?SLOG(warning, #{
        msg => "dashboard_change_password",
        username => Name,
        backend => Backend,
        result => denied,
        reason => "sso_user_has_no_local_password"
    }),
    {400, ?NOT_ALLOWED, <<
        "This account signs in through an SSO backend and has no local "
        "password. Change it with the identity provider instead."
    >>};
do_change_pwd(Username, Params) when is_binary(Username) ->
    LogMeta = #{msg => "dashboard_change_password", username => Username},
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

current_user_mfa(post, #{body := Settings} = Req) ->
    Mechanism = maps:get(<<"mechanism">>, Settings),
    with_caller(Req, fun(#?ADMIN{username = Username}) ->
        LogMeta = #{msg => "dashboard_user_mfa_setup", username => Username},
        %% Never `ByAdmin': a self-initiated (re)init must not touch the
        %% admin_override decision.
        mfa_result(emqx_dashboard_admin:reinit_mfa(Username, Mechanism, false), LogMeta)
    end);
current_user_mfa(delete, Req) ->
    with_caller(Req, fun(#?ADMIN{username = Username}) ->
        LogMeta = #{msg => "dashboard_user_mfa_disable", username => Username},
        case authorize_self_mfa_disable(Username) of
            ok ->
                mfa_result(emqx_dashboard_admin:disable_mfa(Username, false), LogMeta);
            {deny, Code, ErrCode, Msg} ->
                ?SLOG(warning, LogMeta#{result => denied, reason => ErrCode}),
                {Code, ErrCode, Msg}
        end
    end).

mfa_result(ok, LogMeta) ->
    ?SLOG(info, LogMeta#{result => success}),
    {204};
mfa_result({error, <<"username_not_found">>}, LogMeta) ->
    ?SLOG(error, LogMeta#{result => failed, reason => "username not found"}),
    {404, ?USER_NOT_FOUND, <<"User not found">>};
mfa_result({error, Reason}, LogMeta) ->
    ?SLOG(error, LogMeta#{result => failed, reason => Reason}),
    {400, ?BAD_REQUEST, Reason}.

%% Self-MFA policy, in full:
%%
%%   setup / rotate             => always allowed. Rotation keeps MFA
%%                                 enabled, so it cannot weaken the
%%                                 requirement the override protects, and a
%%                                 first enrolment has to stay open or an
%%                                 account under a mandate could never
%%                                 comply. Hence no check on this route.
%%   disable, override=required => denied. An administrator requires MFA on
%%                                 this account; only another administrator
%%                                 or the CLI can lift it.
%%   disable, otherwise         => allowed.
%%
%% `admin_override' is only ever written when an administrator acts on
%% ANOTHER user (`emqx_dashboard_api:change_mfa/2' passes ByAdmin), so a
%% user cannot lock themselves out by rotating their own MFA.
authorize_self_mfa_disable(Username) ->
    case emqx_dashboard_admin:admin_override_of(Username) of
        ?ADMIN_MFA_REQUIRED ->
            {deny, 403, ?MFA_ADMIN_REQUIRED, <<
                "An administrator requires MFA on this account, so it "
                "cannot be turned off here. It can still be re-keyed."
            >>};
        _ ->
            ok
    end.

%% Resolve the caller's own admin record from the bearer token's
%% `source' -- the key `emqx_dashboard:authorize/2' already resolved
%% (a `?SSO_USERNAME' tuple for SSO users, a plain binary otherwise).
with_caller(Req, Fun) ->
    case caller_admin(Req) of
        #?ADMIN{} = Admin ->
            Fun(Admin);
        undefined ->
            %% Unreachable in practice: these routes are bearer-only and
            %% API keys are refused for them in `emqx_mgmt_auth:authorize/4'.
            %% Reachable only if the account is deleted between login and
            %% this request.
            {404, ?USER_NOT_FOUND, <<"User not found">>}
    end.

change_mfa(delete, #{bindings := #{username := Username0}} = Req) ->
    Username = username(Req, Username0),
    LogMeta = #{msg => "dashboard_user_mfa_disable", username => Username},
    case reject_self_target(Req, Username) of
        ok ->
            mfa_result(emqx_dashboard_admin:disable_mfa(Username, true), LogMeta);
        {deny, Code, ErrCode, Msg} ->
            ?SLOG(warning, LogMeta#{result => denied, reason => ErrCode}),
            {Code, ErrCode, Msg}
    end;
change_mfa(post, #{bindings := #{username := Username0}, body := Settings} = Req) ->
    Username = username(Req, Username0),
    Mechanism = maps:get(<<"mechanism">>, Settings),
    LogMeta = #{msg => "dashboard_user_mfa_setup", username => Username},
    case reject_self_target(Req, Username) of
        ok ->
            mfa_result(emqx_dashboard_admin:reinit_mfa(Username, Mechanism, true), LogMeta);
        {deny, Code, ErrCode, Msg} ->
            ?SLOG(warning, LogMeta#{result => denied, reason => ErrCode}),
            {Code, ErrCode, Msg}
    end.

%% The admin MFA routes act on other users only; the caller's own
%% account is served by `/current_user/mfa'. Without this guard an
%% administrator could route a self-change through the admin path,
%% where it would be recorded as an administrator decision
%% (`admin_override') and would skip the self policy entirely.
reject_self_target(Req, TargetUsername) ->
    case caller_key(Req) =:= TargetUsername of
        false ->
            ok;
        true ->
            {deny, 400, ?NOT_ALLOWED, <<
                "This endpoint manages other users' MFA. "
                "Use /current_user/mfa to manage your own."
            >>}
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
%%     subset, which is all four login-only scopes: user_management,
%%     mfa_management, sso_management, api_key_management.
%%     `mfa_management' means "manage OTHER users' MFA"; managing one's
%%     own MFA is identity-authorized on /current_user/mfa and needs no
%%     scope.
%% @doc Normalize a `scopes' request value to a storage intent:
%%   * `keep'       - field omitted (`undefined'): leave persisted scopes
%%                    unchanged (PUT read-modify-write of another field).
%%   * `unset'      - the `unset' sentinel (atom or binary <<"unset">>), or
%%                    a list whose set equals the role default: store NO
%%                    explicit `scopes' field, so `scopes_of/1' stays
%%                    `undefined' and GET keeps returning "unset".
%%   * `{set, L}'   - store the explicit list `L' verbatim.
%%
%% The role-default comparison is order-insensitive (set equality), so a
%% read-modify-write that round-trips GET's expanded full catalog never
%% sediments the role's implicit full set into a frozen explicit list.
%% A non-list, non-sentinel value falls through to `{set, Other}' so the
%% downstream validation rejects it with the appropriate 400.
write_scope_intent(_Role, undefined) ->
    keep;
write_scope_intent(_Role, unset) ->
    unset;
write_scope_intent(_Role, <<"unset">>) ->
    unset;
write_scope_intent(Role, Scopes) when is_list(Scopes) ->
    case is_role_default_scopes(Role, Scopes) of
        true -> unset;
        false -> {set, Scopes}
    end;
write_scope_intent(_Role, Other) ->
    {set, Other}.

%% Order-insensitive set comparison of a write scope list against the
%% role's implicit default (`role_default_scopes/1'). The dashboard may
%% send the scopes in any order.
is_role_default_scopes(Role, Scopes) ->
    Default = emqx_dashboard_admin:role_default_scopes(Role),
    lists:usort(Scopes) =:= lists:usort(Default).

%% Persist the resolved scope intent (`keep' | `unset' | `{set, L}').
%% Returns `ok' or `{error, Reason}'; the caller maps errors to the
%% proper HTTP status (never crashes the handler).
apply_scope_intent(_Username, keep) ->
    ok;
apply_scope_intent(Username, unset) ->
    handle_scope_write(Username, emqx_dashboard_admin:clear_user_scopes(Username));
apply_scope_intent(Username, {set, Scopes}) ->
    handle_scope_write(Username, emqx_dashboard_admin:set_user_scopes(Username, Scopes)).

handle_scope_write(_Username, {ok, ok}) ->
    ok;
handle_scope_write(Username, {error, Reason}) ->
    ?SLOG(warning, #{
        msg => "set_user_scopes_failed",
        username => Username,
        reason => Reason
    }),
    {error, Reason}.

%% `RawScopes' is the value the client supplied (before role-default /
%% persisted-scope materialisation); `EffectiveScopes' is the
%% materialised list actually validated and stored. The privilege-scope
%% mutex is applied to `RawScopes' so that an omitted scope list (which
%% materialises to the administrator role default — itself a mix of
%% privilege and non-privilege scopes) is treated as the unrestricted
%% case rather than an explicit mixed list.
validate_login_user_scopes(_Role, _RawScopes, undefined) ->
    ok;
validate_login_user_scopes(_Role, _RawScopes, Scopes) when not is_list(Scopes) ->
    {error, <<"scopes must be a list of strings">>};
validate_login_user_scopes(Role, RawScopes, Scopes) ->
    case validate_scope_names(Scopes) of
        ok ->
            case validate_role_scope_compat(Role, Scopes) of
                ok -> maybe_check_privilege_mutex(Role, RawScopes);
                Error -> Error
            end;
        Error ->
            Error
    end.

%% Privilege scopes are administrator-equivalent, so an explicit scope
%% list must not combine them with restricted scopes. This applies to
%% GLOBAL administrators only. For a namespaced administrator the RBAC
%% dispatch is the authoritative gate (do_check_rbac/3 blocks the
%% mutating surface those scopes would otherwise reach), and the
%% namespaced-role scope-compat check above already restricts the set,
%% so the mutex rule is skipped to keep the existing namespaced-admin
%% scope combinations valid. Only an explicit list (is_list) is checked;
%% an omitted `scopes' field (`undefined') is the unrestricted case.
maybe_check_privilege_mutex(_Role, RawScopes) when not is_list(RawScopes) ->
    ok;
maybe_check_privilege_mutex(Role, RawScopes) ->
    case emqx_dashboard_rbac:parse_dashboard_role(Role) of
        {ok, #{?namespace := ?global_ns}} ->
            emqx_scope_catalog:check_privilege_scope_mutex(RawScopes);
        _ ->
            ok
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
%% nesting cap. `Intent' is the normalized write intent from
%% `write_scope_intent/2'.
update_user(Username, Role, Desc, Intent) ->
    case validate_update_scopes(Role, Username, Intent) of
        ok ->
            do_update_user(Username, Role, Desc, Intent);
        {error, Msg} ->
            {400, ?BAD_REQUEST, Msg}
    end.

%% Validate the effective scope list for a PUT:
%%   * `keep'      - validate the *persisted* scopes against the (possibly
%%                   changed) role, so a role demotion can never silently
%%                   keep stale admin-only scopes when the client omitted
%%                   the `scopes' field.
%%   * `unset'     - clears to the role default, which is valid by
%%                   construction; nothing to check.
%%   * `{set, L}'  - validate the explicit list `L'.
validate_update_scopes(_Role, _Username, unset) ->
    ok;
validate_update_scopes(Role, Username, keep) ->
    Persisted = emqx_dashboard_admin:scopes_of(Username),
    validate_login_user_scopes(Role, undefined, Persisted);
validate_update_scopes(Role, _Username, {set, Scopes}) ->
    validate_login_user_scopes(Role, Scopes, Scopes).

do_update_user(Username, Role, Desc, Intent) ->
    case emqx_dashboard_admin:update_user(Username, Role, Desc) of
        {ok, Result} ->
            finalise_update_user(Username, Intent, Result);
        {error, <<"username_not_found">> = Reason} ->
            {404, ?USER_NOT_FOUND, Reason};
        {error, Reason} ->
            {400, ?BAD_REQUEST, Reason}
    end.

finalise_update_user(Username, Intent, Result) ->
    case apply_scope_intent(Username, Intent) of
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

%% The caller's own admin-record key, in the same shape `username/2'
%% builds for a path target, so the two compare directly. `undefined'
%% for a non-bearer caller or a deleted account; it never equals a real
%% target key, so a self-check against it is false, which is the safe
%% answer for both call sites (delete and the admin MFA routes).
caller_key(Req) ->
    case caller_admin(Req) of
        #?ADMIN{username = Username} -> Username;
        undefined -> undefined
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
