%%--------------------------------------------------------------------
%% Copyright (c) 2020-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-define(WRONG_USERNAME_OR_PWD, 'WRONG_USERNAME_OR_PWD').
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
            summary => <<"Dashboard Auth">>,
            'requestBody' => fields([username, password]),
            responses => #{
                200 => fields([token, version, license]),
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
            'requestBody' => fields([username, password, description]),
            responses => #{
                200 => fields([username, description])
            }
        }
    };
schema("/users/:username") ->
    #{
        'operationId' => user,
        put => #{
            tags => [<<"dashboard">>],
            desc => ?DESC(update_user_api),
            parameters => fields([username_in_path]),
            'requestBody' => fields([description]),
            responses => #{
                200 => fields([username, description]),
                404 => response_schema(404)
            }
        },
        delete => #{
            tags => [<<"dashboard">>],
            desc => ?DESC(delete_user_api),
            parameters => fields([username_in_path]),
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
        put => #{
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
    emqx_dashboard_swagger:error_codes([?WRONG_USERNAME_OR_PWD], ?DESC(login_failed401));
response_schema(404) ->
    emqx_dashboard_swagger:error_codes([?USER_NOT_FOUND], ?DESC(users_api404)).

fields(user) ->
    fields([username, description]);
fields(List) ->
    [field(Key) || Key <- List].

field(username) ->
    {username,
        mk(binary(), #{desc => ?DESC(username), 'maxLength' => 100, example => <<"admin">>})};
field(username_in_path) ->
    {username,
        mk(binary(), #{
            desc => ?DESC(username),
            'maxLength' => 100,
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
    {new_pwd, mk(binary(), #{desc => ?DESC(new_pwd)})}.

%% -------------------------------------------------------------------------------------------------
%% API

login(post, #{body := Params}) ->
    Username = maps:get(<<"username">>, Params),
    Password = maps:get(<<"password">>, Params),
    case emqx_dashboard_admin:sign_token(Username, Password) of
        {ok, Token} ->
            ?SLOG(info, #{msg => "Dashboard login successfully", username => Username}),
            Version = iolist_to_binary(proplists:get_value(version, emqx_sys:info())),
            {200, #{
                token => Token,
                version => Version,
                license => #{edition => emqx_release:edition()}
            }};
        {error, R} ->
            ?SLOG(info, #{msg => "Dashboard login failed", username => Username, reason => R}),
            {401, ?WRONG_USERNAME_OR_PWD, <<"Auth failed">>}
    end.

logout(_, #{
    body := #{<<"username">> := Username},
    headers := #{<<"authorization">> := <<"Bearer ", Token/binary>>}
}) ->
    case emqx_dashboard_admin:destroy_token_by_username(Username, Token) of
        ok ->
            ?SLOG(info, #{msg => "Dashboard logout successfully", username => Username}),
            204;
        _R ->
            ?SLOG(info, #{msg => "Dashboard logout failed.", username => Username}),
            {401, ?WRONG_TOKEN_OR_USERNAME, <<"Ensure your token & username">>}
    end.

users(get, _Request) ->
    {200, emqx_dashboard_admin:all_users()};
users(post, #{body := Params}) ->
    Desc = maps:get(<<"description">>, Params, <<"">>),
    Username = maps:get(<<"username">>, Params),
    Password = maps:get(<<"password">>, Params),
    case ?EMPTY(Username) orelse ?EMPTY(Password) of
        true ->
            {400, ?BAD_REQUEST, <<"Username or password undefined">>};
        false ->
            case emqx_dashboard_admin:add_user(Username, Password, Desc) of
                {ok, Result} ->
                    ?SLOG(info, #{msg => "Create dashboard success", username => Username}),
                    {200, Result};
                {error, Reason} ->
                    ?SLOG(info, #{
                        msg => "Create dashboard failed",
                        username => Username,
                        reason => Reason
                    }),
                    {400, ?BAD_REQUEST, Reason}
            end
    end.

user(put, #{bindings := #{username := Username}, body := Params}) ->
    Desc = maps:get(<<"description">>, Params),
    case emqx_dashboard_admin:update_user(Username, Desc) of
        {ok, Result} ->
            {200, Result};
        {error, Reason} ->
            {404, ?USER_NOT_FOUND, Reason}
    end;
user(delete, #{bindings := #{username := Username}, headers := Headers}) ->
    case Username == emqx_dashboard_admin:default_username() of
        true ->
            ?SLOG(info, #{msg => "Dashboard delete admin user failed", username => Username}),
            Message = list_to_binary(io_lib:format("Cannot delete user ~p", [Username])),
            {400, ?NOT_ALLOWED, Message};
        false ->
            case is_self_auth(Username, Headers) of
                true ->
                    {400, ?NOT_ALLOWED, <<"Cannot delete self">>};
                false ->
                    case emqx_dashboard_admin:remove_user(Username) of
                        {error, Reason} ->
                            {404, ?USER_NOT_FOUND, Reason};
                        {ok, _} ->
                            ?SLOG(info, #{
                                msg => "Dashboard delete admin user", username => Username
                            }),
                            {204}
                    end
            end
    end.

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

change_pwd(put, #{bindings := #{username := Username}, body := Params}) ->
    LogMeta = #{msg => "Dashboard change password", username => Username},
    OldPwd = maps:get(<<"old_pwd">>, Params),
    NewPwd = maps:get(<<"new_pwd">>, Params),
    case ?EMPTY(OldPwd) orelse ?EMPTY(NewPwd) of
        true ->
            ?SLOG(error, LogMeta#{result => failed, reason => "password undefined or empty"}),
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
