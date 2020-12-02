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

-module(emqx_auth_username_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("emqx_libs/include/emqx.hrl").

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-define(TAB, emqx_auth_username).

all() ->
    emqx_ct:all(?MODULE).

init_per_suite(Config) ->
    emqx_ct_helpers:start_apps([emqx_auth_username], fun set_special_configs/1),
    Config.

end_per_suite(_Config) ->
    emqx_ct_helpers:stop_apps([emqx_auth_username]).

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

t_managing(_Config) ->
    clean_all_users(),

    ok = emqx_auth_username:add_user(<<"test_username">>, <<"password">>),
    [{?TAB, <<"test_username">>, _HashedPass}] =
        emqx_auth_username:lookup_user(<<"test_username">>),
    User1 = #{username => <<"test_username">>,
              password => <<"password">>,
              zone     => external},

    {ok, #{auth_result := success,
           anonymous := false}} = emqx_access_control:authenticate(User1),

    ok = emqx_auth_username:remove_user(<<"test_username">>),
    {ok, #{auth_result := success,
           anonymous := true }} = emqx_access_control:authenticate(User1).

t_rest_api(_Config) ->
    clean_all_users(),

    Username = <<"username">>,
    Password = <<"password">>,
    Password1 = <<"password1">>,
    User = #{username => Username, zone => external},

    ?assertEqual(return(),
                 emqx_auth_username_api:add(#{}, rest_params(Username, Password))),
    ?assertEqual(return({error, existed}),
                 emqx_auth_username_api:add(#{}, rest_params(Username, Password))),
    ?assertEqual(return([Username]),
                 emqx_auth_username_api:list(#{}, [])),

    {ok, #{code := 0, data := Data}} =
        emqx_auth_username_api:lookup(rest_binding(Username), []),
    ?assertEqual(true, match_password(maps:get(username, Data),  Password)),

    {ok, _} = emqx_access_control:authenticate(User#{password => Password}),

    ?assertEqual(return(),
                 emqx_auth_username_api:update(rest_binding(Username), rest_params(Password))),
    ?assertEqual(return({error, noexisted}),
                 emqx_auth_username_api:update(#{username => <<"another_user">>}, rest_params(<<"another_passwd">>))),

    {error, _} = emqx_access_control:authenticate(User#{password => Password1}),

    ?assertEqual(return(),
                 emqx_auth_username_api:delete(rest_binding(Username), [])),
    {ok, #{auth_result := success,
           anonymous := true}} = emqx_access_control:authenticate(User#{password => Password}).

t_cli(_Config) ->
    clean_all_users(),

    emqx_auth_username:cli(["add", "username", "password"]),
    ?assertEqual(true, match_password(<<"username">>, <<"password">>)),

    emqx_auth_username:cli(["update", "username", "newpassword"]),
    ?assertEqual(true, match_password(<<"username">>, <<"newpassword">>)),

    emqx_auth_username:cli(["del", "username"]),
    [] = emqx_auth_username:lookup_user(<<"username">>),
    emqx_auth_username:cli(["add", "user1", "pass1"]),
    emqx_auth_username:cli(["add", "user2", "pass2"]),
    UserList = emqx_auth_username:cli(["list"]),
    2 = length(UserList),
    emqx_auth_username:cli(usage).

t_conf_not_override_existed(_) ->
    clean_all_users(),

    Username = <<"username">>,
    Password = <<"password">>,
    NPassword = <<"password1">>,
    User = #{username => Username, zone => external},

    application:stop(emqx_auth_username),
    application:set_env(emqx_auth_username, userlist, [{Username, Password}]),
    application:ensure_all_started(emqx_auth_username),

    {ok, _} = emqx_access_control:authenticate(User#{password => Password}),
    emqx_auth_username:cli(["update", Username, NPassword]),

    {error, _} = emqx_access_control:authenticate(User#{password => Password}),
    {ok, _} = emqx_access_control:authenticate(User#{password => NPassword}),

    application:stop(emqx_auth_username),
    application:ensure_all_started(emqx_auth_username),
    {ok, _} = emqx_access_control:authenticate(User#{password => NPassword}),

    ?assertEqual(return(),
                 emqx_auth_username_api:update(rest_binding(Username), rest_params(Password))),
    application:stop(emqx_auth_username),
    application:ensure_all_started(emqx_auth_username),
    {ok, _} = emqx_access_control:authenticate(User#{password => Password}).

%%------------------------------------------------------------------------------
%% Helpers
%%------------------------------------------------------------------------------

clean_all_users() ->
    [ mnesia:dirty_delete({emqx_auth_username, Username})
      || Username <- mnesia:dirty_all_keys(emqx_auth_username)].

match_password(Username, PlainPassword) ->
    HashType = application:get_env(emqx_auth_username, password_hash, sha256),
    [{?TAB, Username, <<Salt:4/binary, Hash/binary>>}] =
        emqx_auth_username:lookup_user(Username),
    Hash =:= emqx_passwd:hash(HashType, <<Salt/binary, PlainPassword/binary>>).

rest_params(Passwd) ->
    [{<<"password">>, Passwd}].

rest_params(Username, Passwd) ->
    [{<<"username">>, Username},
     {<<"password">>, Passwd}].

rest_binding(Username) ->
    #{username => Username}.

return() ->
    {ok, #{code => 0}}.
return({error, Err}) ->
    {ok, #{message => Err}};
return(Data) ->
    {ok, #{code => 0, data => Data}}.

