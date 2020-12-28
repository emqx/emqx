%%--------------------------------------------------------------------
%% Copyright (c) 2020 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_auth_mnesia_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include("emqx_auth_mnesia.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-import(emqx_ct_http, [ request_api/3
                      , request_api/5
                      , get_http_data/1
                      , create_default_app/0
                      , delete_default_app/0
                      , default_auth_header/0
                      ]).

-define(HOST, "http://127.0.0.1:8081/").
-define(API_VERSION, "v4").
-define(BASE_PATH, "api").

-define(TABLE, emqx_user).
-define(CLIENTID,  <<"clientid_for_ct">>).
-define(USERNAME,  <<"username_for_ct">>).
-define(PASSWORD,  <<"password">>).
-define(NPASSWORD, <<"new_password">>).

all() ->
    emqx_ct:all(?MODULE).

groups() ->
    [].

init_per_suite(Config) ->
    ok = emqx_ct_helpers:start_apps([emqx_management, emqx_auth_mnesia], fun set_special_configs/1),
    create_default_app(),
    Config.

end_per_suite(_Config) ->
    delete_default_app(),
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

t_management(_Config) ->
    clean_all_users(),

    ok = emqx_auth_mnesia_cli:add_user({username, ?USERNAME}, ?PASSWORD),
    {error, existed} = emqx_auth_mnesia_cli:add_user({username, ?USERNAME}, ?PASSWORD),
    ?assertMatch([{?TABLE, {username, ?USERNAME}, _, _}],
                 emqx_auth_mnesia_cli:all_users(username)
                ),

    ok = emqx_auth_mnesia_cli:add_user({clientid, ?CLIENTID}, ?PASSWORD),
    {error, existed} = emqx_auth_mnesia_cli:add_user({clientid, ?CLIENTID}, ?PASSWORD),
    ?assertMatch([{?TABLE, {clientid, ?CLIENTID}, _, _}],
                 emqx_auth_mnesia_cli:all_users(clientid)
                ),

    ?assertEqual(2, length(emqx_auth_mnesia_cli:all_users())),

    ok = emqx_auth_mnesia_cli:update_user({username, ?USERNAME}, ?NPASSWORD),
    {error, noexisted} = emqx_auth_mnesia_cli:update_user(
                          {username, <<"no_existed_user">>}, ?PASSWORD
                         ),

    ok = emqx_auth_mnesia_cli:update_user({clientid, ?CLIENTID}, ?NPASSWORD),
    {error, noexisted} = emqx_auth_mnesia_cli:update_user(
                          {clientid, <<"no_existed_user">>}, ?PASSWORD
                         ),

    ?assertMatch([{?TABLE, {username, ?USERNAME}, _, _}],
                 emqx_auth_mnesia_cli:lookup_user({username, ?USERNAME})
                ),
    ?assertMatch([{?TABLE, {clientid, ?CLIENTID}, _, _}],
                 emqx_auth_mnesia_cli:lookup_user({clientid, ?CLIENTID})
                ),

    User1 = #{username => ?USERNAME,
              clientid => undefined,
              password => ?NPASSWORD,
              zone     => external},

    {ok, #{auth_result := success,
           anonymous := false}} = emqx_access_control:authenticate(User1),

    {error, password_error} = emqx_access_control:authenticate(
                               User1#{password => <<"error_password">>}
                              ),

    ok = emqx_auth_mnesia_cli:remove_user({username, ?USERNAME}),
    {ok, #{auth_result := success,
           anonymous := true }} = emqx_access_control:authenticate(User1),

    User2 = #{clientid => ?CLIENTID,
              password => ?NPASSWORD,
              zone     => external},

    {ok, #{auth_result := success,
           anonymous := false}} = emqx_access_control:authenticate(User2),

    {error, password_error} = emqx_access_control:authenticate(
                               User2#{password => <<"error_password">>}
                              ),

    ok = emqx_auth_mnesia_cli:remove_user({clientid, ?CLIENTID}),
    {ok, #{auth_result := success,
           anonymous := true }} = emqx_access_control:authenticate(User2),

    [] = emqx_auth_mnesia_cli:all_users().

t_auth_clientid_cli(_) ->
    clean_all_users(),

    HashType = application:get_env(emqx_auth_mnesia, password_hash, sha256),

    emqx_auth_mnesia_cli:auth_clientid_cli(["add", ?CLIENTID, ?PASSWORD]),
    [{_, {clientid, ?CLIENTID},
      <<Salt:4/binary, Hash/binary>>,
      _}] = emqx_auth_mnesia_cli:lookup_user({clientid, ?CLIENTID}),
    ?assertEqual(Hash, emqx_passwd:hash(HashType, <<Salt/binary, ?PASSWORD/binary>>)),

    emqx_auth_mnesia_cli:auth_clientid_cli(["update", ?CLIENTID, ?NPASSWORD]),
    [{_, {clientid, ?CLIENTID},
      <<Salt1:4/binary, Hash1/binary>>,
      _}] = emqx_auth_mnesia_cli:lookup_user({clientid, ?CLIENTID}),
    ?assertEqual(Hash1, emqx_passwd:hash(HashType, <<Salt1/binary, ?NPASSWORD/binary>>)),

    emqx_auth_mnesia_cli:auth_clientid_cli(["del", ?CLIENTID]),
    ?assertEqual([], emqx_auth_mnesia_cli:lookup_user(?CLIENTID)),

    emqx_auth_mnesia_cli:auth_clientid_cli(["add", "user1", "pass1"]),
    emqx_auth_mnesia_cli:auth_clientid_cli(["add", "user2", "pass2"]),
    ?assertEqual(2, length(emqx_auth_mnesia_cli:auth_clientid_cli(["list"]))),

    emqx_auth_mnesia_cli:auth_clientid_cli(usage).

t_auth_username_cli(_) ->
    clean_all_users(),

    HashType = application:get_env(emqx_auth_mnesia, password_hash, sha256),

    emqx_auth_mnesia_cli:auth_username_cli(["add", ?USERNAME, ?PASSWORD]),
    [{_, {username, ?USERNAME},
      <<Salt:4/binary, Hash/binary>>,
      _}] = emqx_auth_mnesia_cli:lookup_user({username, ?USERNAME}),
    ?assertEqual(Hash, emqx_passwd:hash(HashType, <<Salt/binary, ?PASSWORD/binary>>)),

    emqx_auth_mnesia_cli:auth_username_cli(["update", ?USERNAME, ?NPASSWORD]),
    [{_, {username, ?USERNAME},
      <<Salt1:4/binary, Hash1/binary>>,
      _}] = emqx_auth_mnesia_cli:lookup_user({username, ?USERNAME}),
    ?assertEqual(Hash1, emqx_passwd:hash(HashType, <<Salt1/binary, ?NPASSWORD/binary>>)),

    emqx_auth_mnesia_cli:auth_username_cli(["del", ?USERNAME]),
    ?assertEqual([], emqx_auth_mnesia_cli:lookup_user(?USERNAME)),

    emqx_auth_mnesia_cli:auth_username_cli(["add", "user1", "pass1"]),
    emqx_auth_mnesia_cli:auth_username_cli(["add", "user2", "pass2"]),
    ?assertEqual(2, length(emqx_auth_mnesia_cli:auth_username_cli(["list"]))),

    emqx_auth_mnesia_cli:auth_username_cli(usage).


t_clientid_rest_api(_Config) ->
    clean_all_users(),

    {ok, Result1} = request_http_rest_list(["auth_clientid"]),
    [] = get_http_data(Result1),

    Params1 = #{<<"clientid">> => ?CLIENTID, <<"password">> => ?PASSWORD},
    {ok, _} = request_http_rest_add(["auth_clientid"], Params1),

    Path =  ["auth_clientid/" ++ binary_to_list(?CLIENTID)],
    Params2 = #{<<"clientid">> => ?CLIENTID, <<"password">> => ?NPASSWORD},
    {ok, _} = request_http_rest_update(Path, Params2),

    {ok, Result2} = request_http_rest_lookup(Path),
    ?assertMatch(#{<<"clientid">> := ?CLIENTID}, get_http_data(Result2)),

    Params3 = [ #{<<"clientid">> => ?CLIENTID, <<"password">> => ?PASSWORD}
              , #{<<"clientid">> => <<"clientid1">>, <<"password">> => ?PASSWORD}
              , #{<<"clientid">> => <<"clientid2">>, <<"password">> => ?PASSWORD}
              ],
    {ok, Result3} = request_http_rest_add(["auth_clientid"], Params3),
    ?assertMatch(#{ ?CLIENTID := <<"{error,existed}">>
                  , <<"clientid1">> := <<"ok">>
                  , <<"clientid2">> := <<"ok">>
                  }, get_http_data(Result3)),

    {ok, Result4} = request_http_rest_list(["auth_clientid"]),
    ?assertEqual(3, length(get_http_data(Result4))),

    {ok, _} = request_http_rest_delete(Path),
    {ok, Result5} = request_http_rest_lookup(Path),
    ?assertMatch(#{}, get_http_data(Result5)).

t_username_rest_api(_Config) ->
    clean_all_users(),

    {ok, Result1} = request_http_rest_list(["auth_username"]),
    [] = get_http_data(Result1),

    Params1 = #{<<"username">> => ?USERNAME, <<"password">> => ?PASSWORD},
    {ok, _} = request_http_rest_add(["auth_username"], Params1),

    Path =  ["auth_username/" ++ binary_to_list(?USERNAME)],
    Params2 = #{<<"username">> => ?USERNAME, <<"password">> => ?NPASSWORD},
    {ok, _} = request_http_rest_update(Path, Params2),

    {ok, Result2} = request_http_rest_lookup(Path),
    ?assertMatch(#{<<"username">> := ?USERNAME}, get_http_data(Result2)),

    Params3 = [ #{<<"username">> => ?USERNAME, <<"password">> => ?PASSWORD}
              , #{<<"username">> => <<"username1">>, <<"password">> => ?PASSWORD}
              , #{<<"username">> => <<"username2">>, <<"password">> => ?PASSWORD}
              ],
    {ok, Result3} = request_http_rest_add(["auth_username"], Params3),
    ?assertMatch(#{ ?USERNAME := <<"{error,existed}">>
                  , <<"username1">> := <<"ok">>
                  , <<"username2">> := <<"ok">>
                  }, get_http_data(Result3)),

    {ok, Result4} = request_http_rest_list(["auth_username"]),
    ?assertEqual(3, length(get_http_data(Result4))),

    {ok, _} = request_http_rest_delete(Path),
    {ok, Result5} = request_http_rest_lookup([Path]),
    ?assertMatch(#{}, get_http_data(Result5)).

%%------------------------------------------------------------------------------
%% Helpers
%%------------------------------------------------------------------------------

clean_all_users() ->
    [ mnesia:dirty_delete({emqx_user, Login})
      || Login <- mnesia:dirty_all_keys(emqx_user)].

%%--------------------------------------------------------------------
%% HTTP Request
%%--------------------------------------------------------------------

request_http_rest_list(Path) ->
    request_api(get, uri(Path), default_auth_header()).

request_http_rest_lookup(Path) ->
    request_api(get, uri([Path]), default_auth_header()).

request_http_rest_add(Path, Params) ->
    request_api(post, uri(Path), [], default_auth_header(), Params).

request_http_rest_update(Path, Params) ->
    request_api(put, uri([Path]), [], default_auth_header(), Params).

request_http_rest_delete(Login) ->
    request_api(delete, uri([Login]), default_auth_header()).

uri() -> uri([]).
uri(Parts) when is_list(Parts) ->
    NParts = [b2l(E) || E <- Parts],
    ?HOST ++ filename:join([?BASE_PATH, ?API_VERSION | NParts]).

%% @private
b2l(B) when is_binary(B) ->
    binary_to_list(B);
b2l(L) when is_list(L) ->
    L.
