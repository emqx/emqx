%%--------------------------------------------------------------------
%% Copyright (c) 2020 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_auth_mnesia_SUITE).

-compile(export_all).

-include("emqx_auth_mnesia.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-import(emqx_ct_http, [ request_api/3
                      , request_api/5
                      , get_http_data/1
                      , create_default_app/0
                      , default_auth_header/0
                      ]).

-define(HOST, "http://127.0.0.1:8081/").
-define(API_VERSION, "v4").
-define(BASE_PATH, "api").

all() ->
    emqx_ct:all(?MODULE).

groups() ->
    [].

init_per_suite(Config) ->
    ok = emqx_ct_helpers:start_apps([emqx_management, emqx_auth_mnesia], fun set_special_configs/1),
    create_default_app(),
    Config.

end_per_suite(_Config) ->
    emqx_ct_helpers:stop_apps([emqx_management, emqx_auth_mnesia]).

init_per_testcase(t_check_as_clientid, Config) ->
    Params = #{
            hash_type => application:get_env(emqx_auth_mnesia, hash_type, sha256),
            key_as => clientid
            },
    emqx:hook('client.authenticate', fun emqx_auth_mnesia:check/3, [Params]),
    Config;

init_per_testcase(_, Config) ->
    Params = #{
            hash_type => application:get_env(emqx_auth_mnesia, hash_type, sha256),
            key_as => username
            },
    emqx:hook('client.authenticate', fun emqx_auth_mnesia:check/3, [Params]),
    Config.

end_per_suite(_, Config) ->
    emqx:unhook('client.authenticate', fun emqx_auth_mnesia:check/3),
    Config.

set_special_configs(emqx) ->
    application:set_env(emqx, allow_anonymous, true),
    application:set_env(emqx, enable_acl_cache, false),
    LoadedPluginPath = filename:join(["test", "emqx_SUITE_data", "loaded_plugins"]),
    application:set_env(emqx, plugins_loaded_file,
                        emqx_ct_helpers:deps_path(emqx, LoadedPluginPath));

set_special_configs(_App) ->
    ok.

%%------------------------------------------------------------------------------
%% Testcases
%%------------------------------------------------------------------------------

t_check_as_username(_Config) ->
    clean_all_users(),

    ok = emqx_auth_mnesia_cli:add_user(<<"test_username">>, <<"password">>, true),
    {error, existed} = emqx_auth_mnesia_cli:add_user(<<"test_username">>, <<"password">>, true),

    ok = emqx_auth_mnesia_cli:update_user(<<"test_username">>, <<"new_password">>, false),
    {error,noexisted} = emqx_auth_mnesia_cli:update_user(<<"no_existed_user">>, <<"password">>, true),

    [<<"test_username">>] = emqx_auth_mnesia_cli:all_users(),
    [{emqx_user, <<"test_username">>, _HashedPass, false}] =
        emqx_auth_mnesia_cli:lookup_user(<<"test_username">>),

    User1 = #{username => <<"test_username">>,
              password => <<"new_password">>,
              zone     => external},

    {ok, #{is_superuser := false, 
           auth_result := success,
           anonymous := false}} = emqx_access_control:authenticate(User1),

    {error,password_error} = emqx_access_control:authenticate(User1#{password => <<"error_password">>}),

    ok = emqx_auth_mnesia_cli:remove_user(<<"test_username">>),
    {ok, #{auth_result := success,
           anonymous := true }} = emqx_access_control:authenticate(User1).

t_check_as_clientid(_Config) ->
    clean_all_users(),

    ok = emqx_auth_mnesia_cli:add_user(<<"test_clientid">>, <<"password">>, false),
    {error, existed} = emqx_auth_mnesia_cli:add_user(<<"test_clientid">>, <<"password">>, false),

    ok = emqx_auth_mnesia_cli:update_user(<<"test_clientid">>, <<"new_password">>, true),
    {error,noexisted} = emqx_auth_mnesia_cli:update_user(<<"no_existed_user">>, <<"password">>, true),

    [<<"test_clientid">>] = emqx_auth_mnesia_cli:all_users(),
    [{emqx_user, <<"test_clientid">>, _HashedPass, true}] =
    emqx_auth_mnesia_cli:lookup_user(<<"test_clientid">>),

    User1 = #{clientid => <<"test_clientid">>,
              password => <<"new_password">>,
              zone     => external},

    {ok, #{is_superuser := true, 
           auth_result := success,
           anonymous := false}} = emqx_access_control:authenticate(User1),

    {error,password_error} = emqx_access_control:authenticate(User1#{password => <<"error_password">>}),

    ok = emqx_auth_mnesia_cli:remove_user(<<"test_clientid">>),
    {ok, #{auth_result := success,
           anonymous := true }} = emqx_access_control:authenticate(User1).

t_rest_api(_Config) ->
    clean_all_users(),

    {ok, Result1} = request_http_rest_list(),
    [] = get_http_data(Result1),

    Params = #{<<"login">> => <<"test_username">>, <<"password">> => <<"password">>, <<"is_superuser">> => true},
    {ok, _} = request_http_rest_add(Params),

    Params1 = [
                #{<<"login">> => <<"test_username">>, <<"password">> => <<"password">>, <<"is_superuser">> => true},
                #{<<"login">> => <<"test_username/1">>, <<"password">> => <<"password">>, <<"is_superuser">> => error_format},
                #{<<"login">> => <<"test_username/2">>, <<"password">> => <<"password">>, <<"is_superuser">> => true}
                ],
    {ok, Result2} = request_http_rest_add(Params1),
    #{
        <<"test_username">> := <<"{error,existed}">>,
        <<"test_username/1">> := <<"{error,is_superuser}">>,
        <<"test_username/2">> := <<"ok">>
        } = get_http_data(Result2),

    {ok, Result3} = request_http_rest_lookup(<<"test_username">>),
    #{<<"login">> := <<"test_username">>, <<"is_superuser">> := true} = get_http_data(Result3),

    {ok, _} = request_http_rest_update(<<"test_username">>, <<"new_password">>, error_format),
    {ok, _} = request_http_rest_update(<<"error_username">>, <<"new_password">>, false),

    {ok, _} = request_http_rest_update(<<"test_username">>, <<"new_password">>, false),
    {ok, Result4} = request_http_rest_lookup(<<"test_username">>),
    #{<<"login">> := <<"test_username">>, <<"is_superuser">> := false} = get_http_data(Result4),

    User1 = #{username => <<"test_username">>,
        password => <<"new_password">>,
        zone     => external},

    {ok, #{is_superuser := false, 
        auth_result := success,
        anonymous := false}} = emqx_access_control:authenticate(User1),

    {ok, _} = request_http_rest_delete(<<"test_username">>),
    {ok, #{auth_result := success,
           anonymous := true }} = emqx_access_control:authenticate(User1).

t_run_command(_) ->
    clean_all_users(),
    ?assertEqual(ok, emqx_ctl:run_command(["mqtt-user", "add", "TestUser", "Password", false])),
    ?assertMatch([{emqx_user, <<"TestUser">>, _, false}], emqx_auth_mnesia_cli:lookup_user(<<"TestUser">>)),

    ?assertEqual(ok, emqx_ctl:run_command(["mqtt-user", "update", "TestUser", "NewPassword", true])),
    ?assertMatch([{emqx_user, <<"TestUser">>, _, true}], emqx_auth_mnesia_cli:lookup_user(<<"TestUser">>)),

    ?assertEqual(ok, emqx_ctl:run_command(["mqtt-user", "del", "TestUser"])),
    ?assertMatch([], emqx_auth_mnesia_cli:lookup_user(<<"TestUser">>)),

    ?assertEqual(ok, emqx_ctl:run_command(["mqtt-user", "show", "TestUser"])),
    ?assertEqual(ok, emqx_ctl:run_command(["mqtt-user", "list"])),
    ?assertEqual(ok, emqx_ctl:run_command(["mqtt-user"])).

t_cli(_) ->
    meck:new(emqx_ctl, [non_strict, passthrough]),
    meck:expect(emqx_ctl, print, fun(Arg) -> emqx_ctl:format(Arg) end),
    meck:expect(emqx_ctl, print, fun(Msg, Arg) -> emqx_ctl:format(Msg, Arg) end),
    meck:expect(emqx_ctl, usage, fun(Usages) -> emqx_ctl:format_usage(Usages) end),
    meck:expect(emqx_ctl, usage, fun(Cmd, Descr) -> emqx_ctl:format_usage(Cmd, Descr) end),

    clean_all_users(),

    ?assertMatch({match, _}, re:run(emqx_auth_mnesia_cli:auth_cli(["add", "TestUser", "Password", true]), "ok")),
    ?assertMatch({match, _}, re:run(emqx_auth_mnesia_cli:auth_cli(["add", "TestUser", "Password", true]), "Error")),

    ?assertMatch({match, _}, re:run(emqx_auth_mnesia_cli:auth_cli(["update", "NoExisted", "Password", false]), "Error")),
    ?assertMatch({match, _}, re:run(emqx_auth_mnesia_cli:auth_cli(["update", "TestUser", "Password", false]), "ok")),

    ?assertMatch(["User(login = <<\"TestUser\">> is_super = false)\n"], emqx_auth_mnesia_cli:auth_cli(["show", "TestUser"])),
    ?assertMatch(["User(login = <<\"TestUser\">>)\n"], emqx_auth_mnesia_cli:auth_cli(["list"])),

    ?assertMatch({match, _}, re:run(emqx_auth_mnesia_cli:auth_cli(["del", "TestUser"]), "ok")),
    ?assertMatch([], emqx_auth_mnesia_cli:auth_cli(["show", "TestUser"])),
    ?assertMatch([], emqx_auth_mnesia_cli:auth_cli(["list"])),

    ?assertMatch({match, _}, re:run(emqx_auth_mnesia_cli:auth_cli([]), "mqtt-user")),

    meck:unload(emqx_ctl).

%%------------------------------------------------------------------------------
%% Helpers
%%------------------------------------------------------------------------------

clean_all_users() ->
    [ mnesia:dirty_delete({emqx_user, Login})
      || Login <- mnesia:dirty_all_keys(emqx_user)].

%%--------------------------------------------------------------------
%% HTTP Request
%%--------------------------------------------------------------------

request_http_rest_list() ->
    request_api(get, uri(), default_auth_header()).

request_http_rest_lookup(Login) ->
    request_api(get, uri([Login]), default_auth_header()).

request_http_rest_add(Params) ->
    request_api(post, uri(), [], default_auth_header(), Params).

request_http_rest_update(Login, Password, IsSuperuser) ->
    Params = #{<<"password">> => Password, <<"is_superuser">> => IsSuperuser},
    request_api(put, uri([Login]), [], default_auth_header(), Params).

request_http_rest_delete(Login) ->
    request_api(delete, uri([Login]), default_auth_header()).

uri() -> uri([]).
uri(Parts) when is_list(Parts) ->
    NParts = [b2l(E) || E <- Parts],
    ?HOST ++ filename:join([?BASE_PATH, ?API_VERSION, "mqtt_user"| NParts]).

%% @private
b2l(B) when is_binary(B) ->
    binary_to_list(B);
b2l(L) when is_list(L) ->
    L.
