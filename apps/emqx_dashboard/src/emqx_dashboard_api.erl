%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_dashboard_api).

-behaviour(minirest_api).

-include("emqx_dashboard.hrl").
-include("emqx_dashboard_rbac.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("typerefl/include/types.hrl").
-include_lib("emqx/include/emqx_config.hrl").

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

api_spec() ->
    emqx_dashboard_swagger:spec(?MODULE, #{check_schema => true, translate_body => true}).

paths() ->
    [
        "/login",
        "/logout",
        "/users",
        "/users/:username",
        "/users/:username/change_pwd",
        "/users/:username/mfa"
    ].

schema("/login") ->
    ErrorCodes = [?BAD_USERNAME_OR_PWD, ?BAD_MFA_TOKEN, ?LOGIN_LOCKED],
    #{
        'operationId' => login,
        post => #{
            tags => [<<"dashboard">>],
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
            'requestBody' => fields([username, password, role, description]),
            responses => #{
                200 => fields([username, role, description, backend])
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
            'requestBody' => fields([role, description]),
            responses => #{
                200 => user_fields(),
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
            parameters => fields([username_in_path]),
            'requestBody' => emqx_dashboard_schema:mfa_fields(),
            responses => #{
                204 => <<"MFA setting is reset">>,
                404 => response_schema(404)
            }
        },
        delete => #{
            tags => [<<"dashboard">>],
            desc => ?DESC(delete_mfa),
            parameters => fields([username_in_path]),
            responses => #{
                204 => <<"MFA setting is deleted">>,
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
    fields([username, role, description, backend]) ++ ee_user_fields().

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

users(get, _Request) ->
    {200, filter_result(emqx_dashboard_admin:all_users())};
users(post, #{body := Params}) ->
    Desc = maps:get(<<"description">>, Params, <<"">>),
    Role = maps:get(<<"role">>, Params, ?ROLE_DEFAULT),
    Username = maps:get(<<"username">>, Params),
    Password = maps:get(<<"password">>, Params),
    case ?EMPTY(Username) orelse ?EMPTY(Password) of
        true ->
            {400, ?BAD_REQUEST, <<"Username or password undefined">>};
        false ->
            case emqx_dashboard_admin:add_user(Username, Password, Role, Desc) of
                {ok, Result} ->
                    ?SLOG(info, #{msg => "create_dashboard_user_success", username => Username}),
                    {200, filter_result(Result)};
                {error, Reason} ->
                    ?SLOG(info, #{
                        msg => "create_dashboard_user_failed",
                        username => Username,
                        reason => Reason
                    }),
                    {400, ?BAD_REQUEST, Reason}
            end
    end.

user(put, #{bindings := #{username := Username0}, body := Params} = Req) ->
    Role = maps:get(<<"role">>, Params, ?ROLE_DEFAULT),
    Desc = maps:get(<<"description">>, Params),
    Username = username(Req, Username0),
    case emqx_dashboard_admin:update_user(Username, Role, Desc) of
        {ok, Result} ->
            {200, filter_result(Result)};
        {error, <<"username_not_found">> = Reason} ->
            {404, ?USER_NOT_FOUND, Reason};
        {error, Reason} ->
            {400, ?BAD_REQUEST, Reason}
    end;
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

change_mfa(delete, #{bindings := #{username := Username}}) ->
    LogMeta = #{msg => "dashboard_user_mfa_delete", username => binary_to_list(Username)},
    case emqx_dashboard_admin:disable_mfa(Username) of
        {ok, ok} ->
            ?SLOG(info, LogMeta#{result => success}),
            {204};
        {error, <<"username_not_found">>} ->
            ?SLOG(error, LogMeta#{result => failed, reason => "username not found"}),
            {404, ?USER_NOT_FOUND, <<"User not found">>}
    end;
change_mfa(post, #{bindings := #{username := Username}, body := Settings}) ->
    Mechanism = maps:get(<<"mechanism">>, Settings),
    {ok, State} = emqx_dashboard_mfa:init(Mechanism),
    LogMeta = #{msg => "dashboard_user_mfa_setup", username => binary_to_list(Username)},
    case emqx_dashboard_admin:set_mfa_state(Username, State) of
        {ok, ok} ->
            ?SLOG(info, LogMeta#{result => success}),
            {204};
        {error, <<"username_not_found">>} ->
            ?SLOG(error, LogMeta#{result => failed, reason => "username not found"}),
            {404, ?USER_NOT_FOUND, <<"User not found">>}
    end.

register_unsuccessful_login(Username, <<"password_error">>) ->
    emqx_dashboard_login_lock:register_unsuccessful_login(Username);
register_unsuccessful_login(_, _) ->
    ok.

mk(Type, Props) ->
    hoconsc:mk(Type, Props).

array(Type) ->
    hoconsc:array(Type).

enum(Symbols) ->
    hoconsc:enum(Symbols).

field_filter(_) ->
    true.

filter_result(#{} = Result) ->
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
filter_result(Results) when is_list(Results) ->
    lists:map(fun filter_result/1, Results);
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
