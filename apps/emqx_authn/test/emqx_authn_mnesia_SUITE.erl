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

-module(emqx_authn_mnesia_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-include("emqx_authn.hrl").

-define(AUTH, emqx_authn).

all() ->
    emqx_ct:all(?MODULE).

init_per_suite(Config) ->
    emqx_ct_helpers:start_apps([emqx_authn], fun set_special_configs/1),
    Config.

end_per_suite(_) ->
    file:delete(filename:join(emqx:get_env(plugins_etc_dir), 'emqx_authn.conf')),
    emqx_ct_helpers:stop_apps([emqx_authn]),
    ok.

set_special_configs(emqx_authn) ->
    application:set_env(emqx, plugins_etc_dir,
                        emqx_ct_helpers:deps_path(emqx_authn, "test")),
    Conf = #{<<"emqx_authn">> => #{<<"authenticators">> => []}},
    ok = file:write_file(filename:join(emqx:get_env(plugins_etc_dir), 'emqx_authn.conf'), jsx:encode(Conf)),
    ok;
set_special_configs(_App) ->
    ok.

t_mnesia_authenticator(_) ->
    AuthenticatorName = <<"myauthenticator">>,
    AuthenticatorConfig = #{name => AuthenticatorName,
                            mechanism => 'password-based',
                            config => #{
                                server_type => 'built-in-database',
                                user_id_type => username,
                                password_hash_algorithm => #{
                                    name => sha256
                                }}},
    ?assertEqual({ok, AuthenticatorConfig}, ?AUTH:create_authenticator(?CHAIN, AuthenticatorConfig)),

    UserInfo = #{<<"user_id">> => <<"myuser">>,
                 <<"password">> => <<"mypass">>},
    ?assertEqual({ok, #{user_id => <<"myuser">>}}, ?AUTH:add_user(?CHAIN, AuthenticatorName, UserInfo)),
    ?assertEqual({ok, #{user_id => <<"myuser">>}}, ?AUTH:lookup_user(?CHAIN, AuthenticatorName, <<"myuser">>)),

    ?assertEqual(false, emqx_zone:get_env(external, bypass_auth_plugins, false)),

    ClientInfo = #{zone => external,
                   username => <<"myuser">>,
			       password => <<"mypass">>},
    ?assertEqual({stop, ok}, ?AUTH:authenticate(ClientInfo, ok)),
    ?AUTH:enable(),
    ?assertEqual(ok, emqx_access_control:authenticate(ClientInfo)),

    ClientInfo2 = ClientInfo#{username => <<"baduser">>},
    ?assertEqual({stop, {error, not_authorized}}, ?AUTH:authenticate(ClientInfo2, ok)),
    ?assertEqual({error, not_authorized}, emqx_access_control:authenticate(ClientInfo2)),

    ClientInfo3 = ClientInfo#{password => <<"badpass">>},
    ?assertEqual({stop, {error, bad_username_or_password}}, ?AUTH:authenticate(ClientInfo3, ok)),
    ?assertEqual({error, bad_username_or_password}, emqx_access_control:authenticate(ClientInfo3)),

    UserInfo2 = UserInfo#{<<"password">> => <<"mypass2">>},
    ?assertEqual({ok, #{user_id => <<"myuser">>}}, ?AUTH:update_user(?CHAIN, AuthenticatorName, <<"myuser">>, UserInfo2)),
    ClientInfo4 = ClientInfo#{password => <<"mypass2">>},
    ?assertEqual({stop, ok}, ?AUTH:authenticate(ClientInfo4, ok)),

    ?assertEqual(ok, ?AUTH:delete_user(?CHAIN, AuthenticatorName, <<"myuser">>)),
    ?assertEqual({error, not_found}, ?AUTH:lookup_user(?CHAIN, AuthenticatorName, <<"myuser">>)),

    ?assertEqual({ok, #{user_id => <<"myuser">>}}, ?AUTH:add_user(?CHAIN, AuthenticatorName, UserInfo)),
    ?assertMatch({ok, #{user_id := <<"myuser">>}}, ?AUTH:lookup_user(?CHAIN, AuthenticatorName, <<"myuser">>)),
    ?assertEqual(ok, ?AUTH:delete_authenticator(?CHAIN, AuthenticatorName)),

    ?assertEqual({ok, AuthenticatorConfig}, ?AUTH:create_authenticator(?CHAIN, AuthenticatorConfig)),
    ?assertMatch({error, not_found}, ?AUTH:lookup_user(?CHAIN, AuthenticatorName, <<"myuser">>)),
    ?assertEqual(ok, ?AUTH:delete_authenticator(?CHAIN, AuthenticatorName)),
    ok.

t_import(_) ->
    AuthenticatorName = <<"myauthenticator">>,
    AuthenticatorConfig = #{name => AuthenticatorName,
                            mechanism => 'password-based',
                            config => #{
                                server_type => 'built-in-database',
                                user_id_type => username,
                                password_hash_algorithm => #{
                                    name => sha256
                                }}},
    ?assertEqual({ok, AuthenticatorConfig}, ?AUTH:create_authenticator(?CHAIN, AuthenticatorConfig)),

    Dir = code:lib_dir(emqx_authn, test),
    ?assertEqual(ok, ?AUTH:import_users(?CHAIN, AuthenticatorName, filename:join([Dir, "data/user-credentials.json"]))),
    ?assertEqual(ok, ?AUTH:import_users(?CHAIN, AuthenticatorName, filename:join([Dir, "data/user-credentials.csv"]))),
    ?assertMatch({ok, #{user_id := <<"myuser1">>}}, ?AUTH:lookup_user(?CHAIN, AuthenticatorName, <<"myuser1">>)),
    ?assertMatch({ok, #{user_id := <<"myuser3">>}}, ?AUTH:lookup_user(?CHAIN, AuthenticatorName, <<"myuser3">>)),

    ClientInfo1 = #{username => <<"myuser1">>,
			        password => <<"mypassword1">>},
    ?assertEqual({stop, ok}, ?AUTH:authenticate(ClientInfo1, ok)),
    ClientInfo2 = ClientInfo1#{username => <<"myuser3">>,
                               password => <<"mypassword3">>},
    ?assertEqual({stop, ok}, ?AUTH:authenticate(ClientInfo2, ok)),
    ?assertEqual(ok, ?AUTH:delete_authenticator(?CHAIN, AuthenticatorName)),
    ok.

t_multi_mnesia_authenticator(_) ->
    AuthenticatorName1 = <<"myauthenticator1">>,
    AuthenticatorConfig1 = #{name => AuthenticatorName1,
                             mechanism => 'password-based',
                             config => #{
                                 server_type => 'built-in-database',
                                 user_id_type => username,
                                 password_hash_algorithm => #{
                                     name => sha256
                                 }}},
    AuthenticatorName2 = <<"myauthenticator2">>,
    AuthenticatorConfig2 = #{name => AuthenticatorName2,
                             mechanism => 'password-based',
                             config => #{
                                 server_type => 'built-in-database',
                                 user_id_type => clientid,
                                 password_hash_algorithm => #{
                                     name => sha256
                                 }}},
    ?assertEqual({ok, AuthenticatorConfig1}, ?AUTH:create_authenticator(?CHAIN, AuthenticatorConfig1)),
    ?assertEqual({ok, AuthenticatorConfig2}, ?AUTH:create_authenticator(?CHAIN, AuthenticatorConfig2)),

    ?assertEqual({ok, #{user_id => <<"myuser">>}},
                 ?AUTH:add_user(?CHAIN, AuthenticatorName1,
                                #{<<"user_id">> => <<"myuser">>,
                                  <<"password">> => <<"mypass1">>})),
    ?assertEqual({ok, #{user_id => <<"myclient">>}},
                 ?AUTH:add_user(?CHAIN, AuthenticatorName2,
                                #{<<"user_id">> => <<"myclient">>,
                                  <<"password">> => <<"mypass2">>})),

    ClientInfo1 = #{username => <<"myuser">>,
                    clientid => <<"myclient">>,
			        password => <<"mypass1">>},
    ?assertEqual({stop, ok}, ?AUTH:authenticate(ClientInfo1, ok)),
    ?assertEqual(ok, ?AUTH:move_authenticator_to_the_front(?CHAIN, AuthenticatorName2)),

    ?assertEqual({stop, {error, bad_username_or_password}}, ?AUTH:authenticate(ClientInfo1, ok)),
    ClientInfo2 = ClientInfo1#{password => <<"mypass2">>},
    ?assertEqual({stop, ok}, ?AUTH:authenticate(ClientInfo2, ok)),

    ?assertEqual(ok, ?AUTH:delete_authenticator(?CHAIN, AuthenticatorName1)),
    ?assertEqual(ok, ?AUTH:delete_authenticator(?CHAIN, AuthenticatorName2)),
    ok.
