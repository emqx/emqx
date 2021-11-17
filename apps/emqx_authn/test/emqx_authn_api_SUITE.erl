%%--------------------------------------------------------------------
%% Copyright (c) 2021 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%% http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(emqx_authn_api_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include("emqx_authn.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").


-define(HOST, "http://127.0.0.1:18083/").
-define(API_VERSION, "v5").
-define(BASE_PATH, "api").

-define(TCP_DEFAULT, 'tcp:default').

-define(
    assertAuthenticatorsMatch(Guard, Path),
    (fun() ->
        {ok, 200, Response} = request(get, uri(Path)),
        ?assertMatch(Guard, jiffy:decode(Response, [return_maps]))
     end)()).

all() ->
    emqx_common_test_helpers:all(?MODULE).

groups() ->
    [].

init_per_testcase(_, Config) ->
    emqx_authn_test_lib:delete_authenticators(
      [authentication],
      ?GLOBAL),

    emqx_authn_test_lib:delete_authenticators(
      [listeners, tcp, default, authentication],
      ?TCP_DEFAULT),

    {atomic, ok} = mria:clear_table(emqx_authn_mnesia),
    Config.

init_per_suite(Config) ->
    ok = emqx_common_test_helpers:start_apps(
           [emqx_authn, emqx_dashboard],
           fun set_special_configs/1),

    ?AUTHN:delete_chain(?GLOBAL),
    {ok, Chains} = ?AUTHN:list_chains(),
    ?assertEqual(length(Chains), 0),
    Config.

end_per_suite(_Config) ->
    emqx_common_test_helpers:stop_apps([emqx_authn, emqx_dashboard]),
    ok.

set_special_configs(emqx_dashboard) ->
    Config = #{
        default_username => <<"admin">>,
        default_password => <<"public">>,
        listeners => [#{
            protocol => http,
            port => 18083
        }]
    },
    emqx_config:put([emqx_dashboard], Config),
    ok;
set_special_configs(_App) ->
    ok.

%%------------------------------------------------------------------------------
%% Tests
%%------------------------------------------------------------------------------

t_invalid_listener(_) ->
    {ok, 404, _} = request(get, uri(["listeners", "invalid", "authentication"])),
    {ok, 404, _} = request(get, uri(["listeners", "in:valid", "authentication"])).

t_authenticators(_) ->
    test_authenticators([]).

t_authenticator(_) ->
    test_authenticator([]).

t_authenticator_users(_) ->
    test_authenticator_users([]).

t_authenticator_user(_) ->
    test_authenticator_user([]).

t_authenticator_move(_) ->
    test_authenticator_move([]).

t_authenticator_import_users(_) ->
    test_authenticator_import_users([]).

t_listener_authenticators(_) ->
    test_authenticators(["listeners", ?TCP_DEFAULT]).

t_listener_authenticator(_) ->
    test_authenticator(["listeners", ?TCP_DEFAULT]).

t_listener_authenticator_users(_) ->
    test_authenticator_users(["listeners", ?TCP_DEFAULT]).

t_listener_authenticator_user(_) ->
    test_authenticator_user(["listeners", ?TCP_DEFAULT]).

t_listener_authenticator_move(_) ->
    test_authenticator_move(["listeners", ?TCP_DEFAULT]).

t_listener_authenticator_import_users(_) ->
    test_authenticator_import_users(["listeners", ?TCP_DEFAULT]).

test_authenticators(PathPrefix) ->

    ValidConfig = emqx_authn_test_lib:http_example(),
    {ok, 200, _} = request(
                     post,
                     uri(PathPrefix ++ ["authentication"]),
                     ValidConfig),

    InvalidConfig = ValidConfig#{method => <<"delete">>},
    {ok, 400, _} = request(
                     post,
                     uri(PathPrefix ++ ["authentication"]),
                     InvalidConfig),

    ?assertAuthenticatorsMatch(
       [#{<<"mechanism">> := <<"password-based">>, <<"backend">> := <<"http">>}],
       PathPrefix ++ ["authentication"]).

test_authenticator(PathPrefix) ->
    ValidConfig0 = emqx_authn_test_lib:http_example(),
    {ok, 200, _} = request(
                     post,
                     uri(PathPrefix ++ ["authentication"]),
                     ValidConfig0),
    {ok, 200, _} = request(
                     get,
                     uri(PathPrefix ++ ["authentication", "password-based:http"])),

    {ok, 404, _} = request(
                     get,
                     uri(PathPrefix ++ ["authentication", "password-based:redis"])),


    {ok, 404, _} = request(
                     put,
                     uri(PathPrefix ++ ["authentication", "password-based:built-in-database"]),
                     emqx_authn_test_lib:built_in_database_example()),

    InvalidConfig0 = ValidConfig0#{method => <<"delete">>},
    {ok, 400, _} = request(
                     put,
                     uri(PathPrefix ++ ["authentication", "password-based:http"]),
                     InvalidConfig0),

    ValidConfig1 = ValidConfig0#{pool_size => 9},
    {ok, 200, _} = request(
                     put,
                     uri(PathPrefix ++ ["authentication", "password-based:http"]),
                     ValidConfig1),

    {ok, 404, _} = request(
                     delete,
                     uri(PathPrefix ++ ["authentication", "password-based:redis"])),

    {ok, 204, _} = request(
                     delete,
                     uri(PathPrefix ++ ["authentication", "password-based:http"])),

    ?assertAuthenticatorsMatch([], PathPrefix ++ ["authentication"]).

test_authenticator_users(PathPrefix) ->
    UsersUri = uri(PathPrefix ++ ["authentication", "password-based:built-in-database", "users"]),

    {ok, 200, _} = request(
                     post,
                     uri(PathPrefix ++ ["authentication"]),
                     emqx_authn_test_lib:built_in_database_example()),

    InvalidUsers = [
                    #{clientid => <<"u1">>, password => <<"p1">>},
                    #{user_id => <<"u2">>},
                    #{user_id => <<"u3">>, password => <<"p3">>, foobar => <<"foobar">>}],

    lists:foreach(
      fun(User) -> {ok, 400, _} = request(post, UsersUri, User) end,
      InvalidUsers),


    ValidUsers = [
                  #{user_id => <<"u1">>, password => <<"p1">>},
                  #{user_id => <<"u2">>, password => <<"p2">>, is_superuser => true},
                  #{user_id => <<"u3">>, password => <<"p3">>}],

    lists:foreach(
      fun(User) ->
          {ok, 201, UserData} = request(post, UsersUri, User),
          CreatedUser = jiffy:decode(UserData, [return_maps]),
          ?assertMatch(#{<<"user_id">> := _}, CreatedUser)
      end,
      ValidUsers),

    {ok, 200, Page1Data} = request(get, UsersUri ++ "?page=1&limit=2"),

    #{<<"data">> := Page1Users,
      <<"meta">> :=
      #{<<"page">> := 1,
        <<"limit">> := 2,
        <<"count">> := 3}} =
    jiffy:decode(Page1Data, [return_maps]),

    {ok, 200, Page2Data} = request(get, UsersUri ++ "?page=2&limit=2"),

    #{<<"data">> := Page2Users,
      <<"meta">> :=
      #{<<"page">> := 2,
        <<"limit">> := 2,
        <<"count">> := 3}} = jiffy:decode(Page2Data, [return_maps]),

    ?assertEqual(2, length(Page1Users)),
    ?assertEqual(1, length(Page2Users)),

    ?assertEqual(
       [<<"u1">>, <<"u2">>, <<"u3">>],
       lists:usort([ UserId || #{<<"user_id">> := UserId} <- Page1Users ++ Page2Users])).

test_authenticator_user(PathPrefix) ->
    UsersUri = uri(PathPrefix ++ ["authentication", "password-based:built-in-database", "users"]),

    {ok, 200, _} = request(
                     post,
                     uri(PathPrefix ++ ["authentication"]),
                     emqx_authn_test_lib:built_in_database_example()),

    User = #{user_id => <<"u1">>, password => <<"p1">>},
    {ok, 201, _} = request(post, UsersUri, User),

    {ok, 404, _} = request(get, UsersUri ++ "/u123"),

    {ok, 409, _} = request(post, UsersUri, User),

    {ok, 200, UserData} = request(get, UsersUri ++ "/u1"),

    FetchedUser = jiffy:decode(UserData, [return_maps]),
    ?assertMatch(#{<<"user_id">> := <<"u1">>}, FetchedUser),
    ?assertNotMatch(#{<<"password">> := _}, FetchedUser),

    ValidUserUpdates = [
                        #{password => <<"p1">>},
                        #{password => <<"p1">>, is_superuser => true}],

    lists:foreach(
      fun(UserUpdate) -> {ok, 200, _} = request(put, UsersUri ++ "/u1", UserUpdate) end,
      ValidUserUpdates),

    InvalidUserUpdates = [#{user_id => <<"u1">>, password => <<"p1">>}],

    lists:foreach(
      fun(UserUpdate) -> {ok, 400, _} = request(put, UsersUri ++ "/u1", UserUpdate) end,
      InvalidUserUpdates),

    {ok, 404, _} = request(delete, UsersUri ++ "/u123"),
    {ok, 204, _} = request(delete, UsersUri ++ "/u1").

test_authenticator_move(PathPrefix) ->
    AuthenticatorConfs = [
                          emqx_authn_test_lib:http_example(),
                          emqx_authn_test_lib:jwt_example(),
                          emqx_authn_test_lib:built_in_database_example()
                         ],

    lists:foreach(
      fun(Conf) ->
              {ok, 200, _} = request(
                               post,
                               uri(PathPrefix ++ ["authentication"]),
                               Conf)
      end,
      AuthenticatorConfs),

    ?assertAuthenticatorsMatch(
       [
        #{<<"mechanism">> := <<"password-based">>, <<"backend">> := <<"http">>},
        #{<<"mechanism">> := <<"jwt">>},
        #{<<"mechanism">> := <<"password-based">>, <<"backend">> := <<"built-in-database">>}
       ],
       PathPrefix ++ ["authentication"]),

    % Invalid moves

    {ok, 400, _} = request(
                     post,
                     uri(PathPrefix ++ ["authentication", "jwt", "move"]),
                     #{position => <<"up">>}),

    {ok, 400, _} = request(
                     post,
                     uri(PathPrefix ++ ["authentication", "jwt", "move"]),
                     #{}),

    {ok, 404, _} = request(
                     post,
                     uri(PathPrefix ++ ["authentication", "jwt", "move"]),
                     #{position => <<"before:invalid">>}),

    {ok, 404, _} = request(
                     post,
                     uri(PathPrefix ++ ["authentication", "jwt", "move"]),
                     #{position => <<"before:password-based:redis">>}),

    {ok, 404, _} = request(
                     post,
                     uri(PathPrefix ++ ["authentication", "jwt", "move"]),
                     #{position => <<"before:password-based:redis">>}),

    % Valid moves

    {ok, 204, _} = request(
                     post,
                     uri(PathPrefix ++ ["authentication", "jwt", "move"]),
                     #{position => <<"top">>}),

    ?assertAuthenticatorsMatch(
       [
        #{<<"mechanism">> := <<"jwt">>},
        #{<<"mechanism">> := <<"password-based">>, <<"backend">> := <<"http">>},
        #{<<"mechanism">> := <<"password-based">>, <<"backend">> := <<"built-in-database">>}
       ],
       PathPrefix ++ ["authentication"]),

    {ok, 204, _} = request(
                     post,
                     uri(PathPrefix ++ ["authentication", "jwt", "move"]),
                     #{position => <<"bottom">>}),

    ?assertAuthenticatorsMatch(
       [
        #{<<"mechanism">> := <<"password-based">>, <<"backend">> := <<"http">>},
        #{<<"mechanism">> := <<"password-based">>, <<"backend">> := <<"built-in-database">>},
        #{<<"mechanism">> := <<"jwt">>}
       ],
       PathPrefix ++ ["authentication"]),

    {ok, 204, _} = request(
                     post,
                     uri(PathPrefix ++ ["authentication", "jwt", "move"]),
                     #{position => <<"before:password-based:built-in-database">>}),

    ?assertAuthenticatorsMatch(
       [
        #{<<"mechanism">> := <<"password-based">>, <<"backend">> := <<"http">>},
        #{<<"mechanism">> := <<"jwt">>},
        #{<<"mechanism">> := <<"password-based">>, <<"backend">> := <<"built-in-database">>}
       ],
       PathPrefix ++ ["authentication"]).

test_authenticator_import_users(PathPrefix) ->
    ImportUri = uri(
                  PathPrefix ++
                  ["authentication", "password-based:built-in-database", "import_users"]),


    {ok, 200, _} = request(
                     post,
                     uri(PathPrefix ++ ["authentication"]),
                     emqx_authn_test_lib:built_in_database_example()),

    {ok, 400, _} = request(post, ImportUri, #{}),

    {ok, 400, _} = request(post, ImportUri, #{filename => <<"/etc/passwd">>}),

    {ok, 400, _} = request(post, ImportUri, #{filename => <<"/not_exists.csv">>}),

    Dir = code:lib_dir(emqx_authn, test),
    JSONFileName = filename:join([Dir, <<"data/user-credentials.json">>]),
    CSVFileName = filename:join([Dir, <<"data/user-credentials.csv">>]),

    {ok, 204, _} = request(post, ImportUri, #{filename => JSONFileName}),

    {ok, 204, _} = request(post, ImportUri, #{filename => CSVFileName}).

%%------------------------------------------------------------------------------
%% Helpers
%%------------------------------------------------------------------------------

request(Method, Url) ->
    request(Method, Url, []).

request(Method, Url, Body) ->
    Request =
        case Body of
            [] ->
                {Url, [auth_header()]};
            _ ->
                {Url, [auth_header()], "application/json", to_json(Body)}
    end,
    ct:pal("Method: ~p, Request: ~p", [Method, Request]),
    case httpc:request(Method, Request, [], [{body_format, binary}]) of
        {error, socket_closed_remotely} ->
            {error, socket_closed_remotely};
        {ok, {{"HTTP/1.1", Code, _}, _Headers, Return} } ->
            {ok, Code, Return};
        {ok, {Reason, _, _}} ->
            {error, Reason}
    end.

uri() -> uri([]).
uri(Parts) when is_list(Parts) ->
    ?HOST ++ filename:join([?BASE_PATH, ?API_VERSION | Parts]).

auth_header() ->
    Username = <<"admin">>,
    Password = <<"public">>,
    {ok, Token} = emqx_dashboard_admin:sign_token(Username, Password),
    {"Authorization", "Bearer " ++ binary_to_list(Token)}.

to_json(Map) ->
    jiffy:encode(Map).
