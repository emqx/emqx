%%--------------------------------------------------------------------
%% Copyright (c) 2020-2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-ifndef(EMQX_ENTERPRISE).

-define(RELEASE, community).

-else.

-define(VERSION, enterprise).

-endif.

-behaviour(minirest_api).

-include("emqx_dashboard.hrl").

-import(emqx_mgmt_util, [ schema/1
                        , object_schema/1
                        , object_schema/2
                        , object_array_schema/1
                        , bad_request/0
                        , properties/1
                        ]).

-export([api_spec/0]).

-export([ login/2
        , logout/2
        , users/2
        , user/2
        , change_pwd/2
        ]).

-define(EMPTY(V), (V == undefined orelse V == <<>>)).

-define(ERROR_USERNAME_OR_PWD, 'ERROR_USERNAME_OR_PWD').

api_spec() ->
    {[ login_api()
     , logout_api()
     , users_api()
     , user_api()
     , change_pwd_api()
     ],
    []}.

login_api() ->
    AuthProps = properties([{username, string, <<"Username">>},
                            {password, string, <<"Password">>}]),

    TokenProps = properties([{token, string, <<"JWT Token">>},
                             {license, object, [{edition, string, <<"License">>, [community, enterprise]}]},
                             {version, string}]),
    Metadata = #{
        post => #{
            tags => [dashboard],
            description => <<"Dashboard Auth">>,
            'requestBody' => object_schema(AuthProps),
            responses => #{
                <<"200">> =>
                    object_schema(TokenProps, <<"Dashboard Auth successfully">>),
                <<"401">> => unauthorized_request()
            },
            security => []
        }
    },
    {"/login", Metadata, login}.

logout_api() ->
    LogoutProps = properties([{username, string, <<"Username">>}]),
    Metadata = #{
        post => #{
            tags => [dashboard],
            description => <<"Dashboard Auth">>,
            'requestBody' => object_schema(LogoutProps),
            responses => #{
                <<"200">> => schema(<<"Dashboard Auth successfully">>)
            }
        }
    },
    {"/logout", Metadata, logout}.

users_api() ->
    BaseProps = properties([{username, string, <<"Username">>},
                            {password, string, <<"Password">>},
                            {tag, string, <<"Tag">>}]),
    Metadata = #{
        get => #{
            tags => [dashboard],
            description => <<"Get dashboard users">>,
            responses => #{
                <<"200">> => object_array_schema(maps:without([password], BaseProps))
            }
        },
        post => #{
            tags => [dashboard],
            description => <<"Create dashboard users">>,
            'requestBody' => object_schema(BaseProps),
            responses => #{
                <<"200">> => schema(<<"Create Users successfully">>),
                <<"400">> => bad_request()
            }
        }
    },
    {"/users", Metadata, users}.

user_api() ->
    Metadata = #{
        delete => #{
            tags => [dashboard],
            description => <<"Delete dashboard users">>,
            parameters => parameters(),
            responses => #{
                <<"200">> => schema(<<"Delete User successfully">>),
                <<"400">> => bad_request()
            }
        },
        put => #{
            tags => [dashboard],
            description => <<"Update dashboard users">>,
            parameters => parameters(),
            'requestBody' => object_schema(properties([{tag, string, <<"Tag">>}])),
            responses => #{
                <<"200">> => schema(<<"Update Users successfully">>),
                <<"400">> => bad_request()
            }
        }
    },
    {"/users/:username", Metadata, user}.

change_pwd_api() ->
    Metadata = #{
        put => #{
            tags => [dashboard],
            description => <<"Update dashboard users password">>,
            parameters => parameters(),
            'requestBody' => object_schema(properties([old_pwd, new_pwd])),
            responses => #{
                <<"200">> => schema(<<"Update Users password successfully">>),
                <<"400">> => bad_request()
            }
        }
    },
    {"/users/:username/change_pwd", Metadata, change_pwd}.

login(post, #{body := Params}) ->
    Username = maps:get(<<"username">>, Params),
    Password = maps:get(<<"password">>, Params),
    case emqx_dashboard_admin:sign_token(Username, Password) of
        {ok, Token} ->
            Version = iolist_to_binary(proplists:get_value(version, emqx_sys:info())),
            {200, #{token => Token, version => Version, license => #{edition => ?RELEASE}}};
        {error, _} ->
            {401, #{code => ?ERROR_USERNAME_OR_PWD, message => <<"Auth filed">>}}
    end.

logout(_, #{body := Params}) ->
    Username = maps:get(<<"username">>, Params),
    emqx_dashboard_admin:destroy_token_by_username(Username),
    {200}.

users(get, _Request) ->
    {200, [row(User) || User <- emqx_dashboard_admin:all_users()]};

users(post, #{body := Params}) ->
    Tag = maps:get(<<"tag">>, Params),
    Username = maps:get(<<"username">>, Params),
    Password = maps:get(<<"password">>, Params),
    case ?EMPTY(Username) orelse ?EMPTY(Password) of
        true  ->
            {400, #{code => <<"CREATE_USER_FAIL">>,
                    message => <<"Username or password undefined">>}};
        false ->
            case emqx_dashboard_admin:add_user(Username, Password, Tag) of
                ok -> {200};
                {error, Reason} ->
                    {400, #{code => <<"CREATE_USER_FAIL">>, message => Reason}}
            end
    end.

user(put, #{bindings := #{username := Username}, body := Params}) ->
    Tag = maps:get(<<"tag">>, Params),
    case emqx_dashboard_admin:update_user(Username, Tag) of
        ok -> {200};
        {error, Reason} ->
            {400, #{code => <<"UPDATE_FAIL">>, message => Reason}}
    end;

user(delete, #{bindings := #{username := Username}}) ->
    case Username == <<"admin">> of
        true -> {400, #{code => <<"CONNOT_DELETE_ADMIN">>,
                        message => <<"Cannot delete admin">>}};
        false ->
            _ = emqx_dashboard_admin:remove_user(Username),
            {200}
    end.

change_pwd(put, #{bindings := #{username := Username}, body := Params}) ->
    OldPwd = maps:get(<<"old_pwd">>, Params),
    NewPwd = maps:get(<<"new_pwd">>, Params),
    case emqx_dashboard_admin:change_password(Username, OldPwd, NewPwd) of
        ok -> {200};
        {error, Reason} ->
            {400, #{code => <<"CHANGE_PWD_FAIL">>, message => Reason}}
    end.

row(#mqtt_admin{username = Username, tags = Tag}) ->
    #{username => Username, tag => Tag}.

parameters() ->
    [#{
        name => username,
        in => path,
        required => true,
        schema => #{type => string},
        example => <<"admin">>
    }].

unauthorized_request() ->
    object_schema(
        properties([{message, string},
                    {code, string, <<"Resp Code">>, [?ERROR_USERNAME_OR_PWD]}
                   ]),
        <<"Unauthorized">>
    ).
