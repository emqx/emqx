%%--------------------------------------------------------------------
%% Copyright (c) 2020-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(emqx_dashboard_api).

-behaviour(minirest_api).

-include("emqx_dashboard.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("typerefl/include/types.hrl").

-import(hoconsc, [
    mk/2,
    array/1,
    enum/1
]).

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
    change_pwd/2
]).

-define(EMPTY(V), (V == undefined orelse V == <<>>)).

-define(BAD_USERNAME_OR_PWD, 'BAD_USERNAME_OR_PWD').
-define(WRONG_TOKEN_OR_USERNAME, 'WRONG_TOKEN_OR_USERNAME').
-define(USER_NOT_FOUND, 'USER_NOT_FOUND').
-define(ERROR_PWD_NOT_MATCH, 'ERROR_PWD_NOT_MATCH').
-define(NOT_ALLOWED, 'NOT_ALLOWED').
-define(BAD_REQUEST, 'BAD_REQUEST').

namespace() -> "dashboard".

api_spec() ->
    emqx_dashboard_swagger:spec(?MODULE, #{check_schema => true, translate_body => true}).

paths() ->
    [
        "/login",
        "/logout",
        "/users",
        "/users/:username",
        "/users/:username/change_pwd"
    ].

schema("/login") ->
    #{
        'operationId' => login,
        post => #{
            tags => [<<"dashboard">>],
            desc => ?DESC(login_api),
            summary => <<"Dashboard authentication">>,
            'requestBody' => fields([username, password]),
            responses => #{
                200 => fields([
                    role, token, version, license, password_expire_in_seconds
                ]),
                401 => response_schema(401)
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
                200 => fields([username, role, description, backend]),
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
    }.

response_schema(401) ->
    emqx_dashboard_swagger:error_codes([?BAD_USERNAME_OR_PWD], ?DESC(login_failed401));
response_schema(404) ->
    emqx_dashboard_swagger:error_codes([?USER_NOT_FOUND], ?DESC(users_api404)).

fields(user) ->
    fields([username, role, description, backend]);
fields(List) ->
    [field(Key) || Key <- List, field_filter(Key)].

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
    minirest_handler:update_log_meta(#{log_from => dashboard, log_source => Username}),
    case emqx_dashboard_admin:sign_token(Username, Password) of
        {ok, Result} ->
            ?SLOG(info, #{msg => "dashboard_login_successful", username => Username}),
            Version = iolist_to_binary(proplists:get_value(version, emqx_sys:info())),
            {200,
                filter_result(Result#{
                    version => Version,
                    license => #{edition => emqx_release:edition()}
                })};
        {error, R} ->
            ?SLOG(info, #{msg => "dashboard_login_failed", username => Username, reason => R}),
            {401, ?BAD_USERNAME_OR_PWD, <<"Auth failed">>}
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
user(delete, #{bindings := #{username := Username0}, headers := Headers} = Req) ->
    case Username0 == emqx_dashboard_admin:default_username() of
        true ->
            ?SLOG(info, #{msg => "dashboard_delete_admin_user_failed", username => Username0}),
            Message = list_to_binary(io_lib:format("Cannot delete user ~p", [Username0])),
            {400, ?NOT_ALLOWED, Message};
        false ->
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
field_filter(_) ->
    true.

filter_result(Result) when is_list(Result) ->
    lists:map(fun filter_result/1, Result);
filter_result(Result) ->
    maps:without([role, backend], Result).

sso_parameters() ->
    sso_parameters([]).

sso_parameters(Any) ->
    Any.

username(_Req, Username) ->
    Username.
-endif.
