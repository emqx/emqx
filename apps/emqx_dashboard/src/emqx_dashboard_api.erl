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

-behaviour(minirest_api).

-include("emqx_dashboard.hrl").

-import(emqx_mgmt_util, [ response_schema/1
                        , response_schema/2
                        , request_body_schema/1
                        , response_array_schema/2
                        ]).

-export([api_spec/0]).

-export([ auth/2
        , users/2
        , user/2
        , change_pwd/2
        ]).

api_spec() ->
    {[auth_api(), users_api(), user_api(), change_pwd_api()], schemas()}.

schemas() ->
    [#{auth => #{
        type => object,
        properties => #{
            username => #{
                type => string,
                description => <<"Username">>},
            password => #{
                type => string,
                description => <<"password">>}
        }
    }},
    #{show_user => #{
        type => object,
        properties => #{
            username => #{
                type => string,
                description => <<"Username">>},
            tag => #{
                type => string,
                description => <<"Tag">>}
        }
    }},
    #{create_user => #{
        type => object,
        properties => #{
            username => #{
                type => string,
                description => <<"Username">>},
            password => #{
                type => string,
                description => <<"Password">>},
            tag => #{
                type => string,
                description => <<"Tag">>}
        }
    }}].

auth_api() ->
    Metadata = #{
        post => #{
            description => <<"Dashboard Auth">>,
            'requestBody' => request_body_schema(auth),
            responses => #{
                <<"200">> =>
                    response_schema(<<"Dashboard Auth successfully">>),
                <<"400">> => bad_request()
            },
            security => []
        }
    },
    {"/auth", Metadata, auth}.

users_api() ->
    Metadata = #{
        get => #{
            description => <<"Get dashboard users">>,
            responses => #{
                <<"200">> => response_array_schema(<<"">>, show_user)
            }
        }
    },
    {"/users", Metadata, users}.

user_api() ->
    Metadata = #{
        delete => #{
            description => <<"Delete dashboard users">>,
            parameters => [path_param_username()],
            responses => #{
                <<"200">> => response_schema(<<"Delete User successfully">>),
                <<"400">> => bad_request()
            }
        },
        put => #{
            description => <<"Update dashboard users">>,
            parameters => [path_param_username()],
            'requestBody' => request_body_schema(#{
                type => object,
                properties => #{
                    tags => #{
                        type => string
                    }
                }
            }),
            responses => #{
                <<"200">> => response_schema(<<"Update Users successfully">>),
                <<"400">> => bad_request()
            }
        },
        post => #{
            description => <<"Create dashboard users">>,
            parameters => [path_param_username()],
            'requestBody' => request_body_schema(create_user),
            responses => #{
                <<"200">> => response_schema(<<"Create Users successfully">>),
                <<"400">> => bad_request()
            }
        }
    },
    {"/users/:username", Metadata, user}.

change_pwd_api() ->
    Metadata = #{
        put => #{
            description => <<"Update dashboard users password">>,
            parameters => [path_param_username()],
            'requestBody' => request_body_schema(#{
                type => object,
                properties => #{
                    old_pwd => #{
                        type => string
                    },
                    new_pwd => #{
                        type => string
                    }
                }
            }),
            responses => #{
                <<"200">> => response_schema(<<"Update Users password successfully">>),
                <<"400">> => bad_request()
            }
        }
    },
    {"/change_pwd/:username", Metadata, change_pwd}.

path_param_username() ->
    #{
        name => username,
        in => path,
        required => true,
        schema => #{type => string},
        example => <<"admin">>
    }.

-define(EMPTY(V), (V == undefined orelse V == <<>>)).

auth(post, Request) ->
    {ok, Body, _} = cowboy_req:read_body(Request),
    Params = emqx_json:decode(Body, [return_maps]),
    Username = maps:get(<<"username">>, Params),
    Password = maps:get(<<"password">>, Params),
    case emqx_dashboard_admin:check(Username, Password) of
        ok ->
            {200};
        {error, Reason} ->
            {400, #{code => <<"AUTH_FAIL">>, message => Reason}}
    end.

users(get, _Request) ->
    {200, [row(User) || User <- emqx_dashboard_admin:all_users()]}.

user(put, Request) ->
    Username = cowboy_req:binding(username, Request),
    {ok, Body, _} = cowboy_req:read_body(Request),
    Params = emqx_json:decode(Body, [return_maps]),
    Tags = maps:get(<<"tags">>, Params),
    case emqx_dashboard_admin:update_user(Username, Tags) of
        ok -> {200};
        {error, Reason} ->
            {400, #{code => <<"UPDATE_FAIL">>, message => Reason}}
    end;

user(delete, Request) ->
    Username = cowboy_req:binding(username, Request),
    case Username == <<"admin">> of
        true -> {400, #{code => <<"CONNOT_DELETE_ADMIN">>,
                        message => <<"Cannot delete admin">>}};
        false ->
            _ = emqx_dashboard_admin:remove_user(Username),
            {200}
    end;

user(post, Request) ->
    {ok, Body, _} = cowboy_req:read_body(Request),
    Params = emqx_json:decode(Body, [return_maps]),
    Tags = maps:get(<<"tags">>, Params),
    Username = maps:get(<<"username">>, Params),
    Password = maps:get(<<"password">>, Params),
    case ?EMPTY(Username) orelse ?EMPTY(Password) of
        true  ->
            {400, #{code => <<"CREATE_USER_FAIL">>,
                    message => <<"Username or password undefined">>}};
        false ->
            case emqx_dashboard_admin:add_user(Username, Password, Tags) of
                ok -> {200};
                {error, Reason} ->
                    {400, #{code => <<"CREATE_USER_FAIL">>, message => Reason}}
            end
    end.

change_pwd(put, Request) ->
    Username = cowboy_req:binding(username, Request),
    {ok, Body, _} = cowboy_req:read_body(Request),
    Params = emqx_json:decode(Body, [return_maps]),
    OldPwd = maps:get(<<"old_pwd">>, Params),
    NewPwd = maps:get(<<"new_pwd">>, Params),
    case emqx_dashboard_admin:change_password(Username, OldPwd, NewPwd) of
        ok -> {200};
        {error, Reason} ->
            {400, #{code => <<"CHANGE_PWD_FAIL">>, message => Reason}}
    end.

row(#mqtt_admin{username = Username, tags = Tags}) ->
    #{username => Username, tags => Tags}.

bad_request() ->
    response_schema(<<"Bad Request">>,
                    #{
                        type => object,
                        properties => #{
                            message => #{type => string},
                            code => #{type => string}
                        }
                    }).
