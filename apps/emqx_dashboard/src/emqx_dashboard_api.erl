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
-include_lib("typerefl/include/types.hrl").
-import(hoconsc, [mk/2, ref/2, array/1, enum/1]).

-export([api_spec/0, fields/1, paths/0, schema/1, namespace/0]).
-export([login/2, logout/2, users/2, user/2, change_pwd/2]).

-define(EMPTY(V), (V == undefined orelse V == <<>>)).
-define(ERROR_USERNAME_OR_PWD, 'ERROR_USERNAME_OR_PWD').
-define(USER_NOT_FOUND_BODY, #{ code => <<"USER_NOT_FOUND">>
                              , message => <<"User not found">>}).


namespace() -> "dashboard".

api_spec() ->
    emqx_dashboard_swagger:spec(?MODULE, #{check_schema => true, translate_body => true}).

paths() ->
    [ "/login"
    , "/logout"
    , "/users"
    , "/users/:username"
    , "/users/:username/change_pwd"].

schema("/login") ->
    #{
        'operationId' => login,
        post => #{
            tags => [<<"dashboard">>],
            desc => <<"Dashboard Auth">>,
            summary => <<"Dashboard Auth">>,
            'requestBody' => [
                {username, mk(binary(),
                    #{desc => <<"The User for which to create the token.">>,
                        'maxLength' => 100, example => <<"admin">>})},
                {password, mk(binary(),
                    #{desc => "password", example => "public"})}
            ],
            responses => #{
                200 => [
                    {token, mk(string(), #{desc => <<"JWT Token">>})},
                    {license, [{edition,
                        mk(enum([community, enterprise]), #{desc => <<"license">>,
                            example => "community"})}]},
                    {version, mk(string(), #{desc => <<"version">>, example => <<"5.0.0">>})}
                ],
                401 => [
                    {code, mk(string(), #{example => 'ERROR_USERNAME_OR_PWD'})},
                    {message, mk(string(), #{example => "Unauthorized"})}
                ]
            },
            security => []
        }};
schema("/logout") ->
    #{
        'operationId' => logout,
        post => #{
            tags => [<<"dashboard">>],
            desc => <<"Dashboard User logout">>,
            'requestBody' => [
                {username, mk(binary(),
                    #{desc => <<"The User for which to create the token.">>,
                        'maxLength' => 100, example => <<"admin">>})}
            ],
            responses => #{
                204 => <<"Dashboard logout successfully">>
            }
        }
    };
schema("/users") ->
    #{
        'operationId' => users,
        get => #{
            tags => [<<"dashboard">>],
            desc => <<"Get dashboard users list">>,
            responses => #{
                200 => mk( array(ref(?MODULE, user))
                         , #{desc => "User lists"})
            }
        },
        post => #{
            tags => [<<"dashboard">>],
            desc => <<"Create dashboard users">>,
            'requestBody' => fields(user_password),
            responses => #{
                200 => mk( ref(?MODULE, user)
                         , #{desc => <<"Create User successfully">>}),
                400 => [{code, mk(string(), #{example => 'CREATE_FAIL'})},
                    {message, mk(string(), #{example => "Create user failed"})}
                ]
            }
        }
    };

schema("/users/:username") ->
    #{
        'operationId' => user,
        put => #{
            tags => [<<"dashboard">>],
            desc => <<"Update dashboard users">>,
            parameters => [{username, mk(binary(),
                #{in => path, example => <<"admin">>})}],
            'requestBody' => [
                { description
                , mk(binary(),
                    #{desc => <<"User description">>, example => <<"administrator">>})}
            ],
            responses => #{
                200 => mk( ref(?MODULE, user)
                         , #{desc => <<"Update User successfully">>}),
                400 => [
                    {code, mk(string(), #{example => 'UPDATE_FAIL'})},
                    {message, mk(string(), #{example => "Update Failed unknown"})}
                ],
                404 => emqx_dashboard_swagger:error_codes(['USER_NOT_FOUND'], <<"User Not Found">>)
            }
        },
        delete => #{
            tags => [<<"dashboard">>],
            desc => <<"Delete dashboard users">>,
            parameters => [{username, mk(binary(),
                #{in => path, example => <<"admin">>})}],
            responses => #{
                204 => <<"Delete User successfully">>,
                400 => [
                    {code, mk(string(), #{example => 'CANNOT_DELETE_ADMIN'})},
                    {message, mk(string(), #{example => "CANNOT DELETE ADMIN"})}
                ],
                404 => emqx_dashboard_swagger:error_codes(['USER_NOT_FOUND'], <<"User Not Found">>)
            }
        }
    };
schema("/users/:username/change_pwd") ->
    #{
        'operationId' => change_pwd,
        put => #{
            tags => [<<"dashboard">>],
            desc => <<"Update dashboard users password">>,
            parameters => [{username, mk(binary(),
                #{in => path, required => true, example => <<"admin">>})}],
            'requestBody' => [
                {old_pwd, mk(binary(), #{required => true})},
                {new_pwd, mk(binary(), #{required => true})}
            ],
            responses => #{
                204 => <<"Update user password successfully">>,
                400 => [
                    {code, mk(string(), #{example => 'UPDATE_FAIL'})},
                    {message, mk(string(), #{example => "Failed Reason"})}
                ]
            }
        }
    }.

fields(user) ->
    [
        {description,
            mk(binary(),
                #{desc => <<"User description">>, example => "administrator"})},
        {username,
            mk(binary(),
                #{desc => <<"username">>, example => "emqx"})}
    ];
fields(user_password) ->
    fields(user) ++
        [{password, mk(binary(), #{desc => "Password", example => <<"public">>})}].

login(post, #{body := Params}) ->
    Username = maps:get(<<"username">>, Params),
    Password = maps:get(<<"password">>, Params),
    case emqx_dashboard_admin:sign_token(Username, Password) of
        {ok, Token} ->
            Version = iolist_to_binary(proplists:get_value(version, emqx_sys:info())),
            {200, #{token => Token,
                    version => Version,
                    license => #{edition => emqx_release:edition()}
                   }};
        {error, _} ->
            {401, #{code => ?ERROR_USERNAME_OR_PWD, message => <<"Auth filed">>}}
    end.

logout(_, #{body := #{<<"username">> := Username},
    headers := #{<<"authorization">> := <<"Bearer ", Token/binary>>}}) ->
    case emqx_dashboard_admin:destroy_token_by_username(Username, Token) of
        ok ->
            204;
        _R ->
            {401, 'BAD_TOKEN_OR_USERNAME', <<"Ensure your token & username">>}
    end.

users(get, _Request) ->
    {200, emqx_dashboard_admin:all_users()};

users(post, #{body := Params}) ->
    Desc = maps:get(<<"description">>, Params),
    Username = maps:get(<<"username">>, Params),
    Password = maps:get(<<"password">>, Params),
    case ?EMPTY(Username) orelse ?EMPTY(Password) of
        true ->
            {400, #{code => <<"CREATE_USER_FAIL">>,
                message => <<"Username or password undefined">>}};
        false ->
            case emqx_dashboard_admin:add_user(Username, Password, Desc) of
                {ok, Result} ->
                    {200, Result};
                {error, Reason} ->
                    {400, #{code => <<"CREATE_USER_FAIL">>, message => Reason}}
            end
    end.

user(put, #{bindings := #{username := Username}, body := Params}) ->
    Desc = maps:get(<<"description">>, Params),
    case emqx_dashboard_admin:update_user(Username, Desc) of
        {ok, Result} ->
            {200, Result};
        {error, _Reason} ->
            {404, ?USER_NOT_FOUND_BODY}
    end;

user(delete, #{bindings := #{username := Username}}) ->
    case Username == <<"admin">> of
        true ->
            {400, #{code => <<"ACTION_NOT_ALLOWED">>,
                    message => <<"Cannot delete admin">>}};
        false ->
            case emqx_dashboard_admin:remove_user(Username) of
                {error, _Reason} ->
                    {404, ?USER_NOT_FOUND_BODY};
                {ok, _} ->
                    {204}
            end
    end.

change_pwd(put, #{bindings := #{username := Username}, body := Params}) ->
    OldPwd = maps:get(<<"old_pwd">>, Params),
    NewPwd = maps:get(<<"new_pwd">>, Params),
    case emqx_dashboard_admin:change_password(Username, OldPwd, NewPwd) of
        {ok, _} ->
            {204};
        {error, Reason} ->
            {400, #{code => <<"CHANGE_PWD_FAIL">>, message => Reason}}
    end.
