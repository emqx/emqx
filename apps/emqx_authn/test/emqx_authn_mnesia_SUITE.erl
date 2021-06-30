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
    Conf = #{<<"authn">> => #{<<"chains">> => [], <<"bindings">> => []}},
    ok = file:write_file(filename:join(emqx:get_env(plugins_etc_dir), 'emqx_authn.conf'), jsx:encode(Conf)),
    ok;
set_special_configs(_App) ->
    ok.

t_mnesia_authenticator(_) ->
    ChainID = <<"mychain">>,
    Chain = #{id => ChainID,
              type => simple},
    ?assertMatch({ok, #{id := ChainID, authenticators := []}}, ?AUTH:create_chain(Chain)),

    AuthenticatorName = <<"myauthenticator">>,
    AuthenticatorConfig = #{name => AuthenticatorName,
                            type => 'built-in-database',
                            config => #{
                                user_id_type => username,
                                password_hash_algorithm => #{
                                    name => sha256
                                }}},
    ?assertEqual({ok, AuthenticatorConfig}, ?AUTH:create_authenticator(ChainID, AuthenticatorConfig)),

    UserInfo = #{<<"user_id">> => <<"myuser">>,
                 <<"password">> => <<"mypass">>},
    ?assertEqual({ok, #{user_id => <<"myuser">>}}, ?AUTH:add_user(ChainID, AuthenticatorName, UserInfo)),
    ?assertEqual({ok, #{user_id => <<"myuser">>}}, ?AUTH:lookup_user(ChainID, AuthenticatorName, <<"myuser">>)),

    ListenerID = <<"listener1">>,
    ?AUTH:bind(ChainID, [ListenerID]),

    ClientInfo = #{listener_id => ListenerID,
			       username => <<"myuser">>,
			       password => <<"mypass">>},
    ?assertEqual(ok, ?AUTH:authenticate(ClientInfo)),
    ClientInfo2 = ClientInfo#{username => <<"baduser">>},
    ?assertEqual({error, user_not_found}, ?AUTH:authenticate(ClientInfo2)),
    ClientInfo3 = ClientInfo#{password => <<"badpass">>},
    ?assertEqual({error, bad_password}, ?AUTH:authenticate(ClientInfo3)),

    UserInfo2 = UserInfo#{<<"password">> => <<"mypass2">>},
    ?assertEqual({ok, #{user_id => <<"myuser">>}}, ?AUTH:update_user(ChainID, AuthenticatorName, <<"myuser">>, UserInfo2)),
    ClientInfo4 = ClientInfo#{password => <<"mypass2">>},
    ?assertEqual(ok, ?AUTH:authenticate(ClientInfo4)),

    ?assertEqual(ok, ?AUTH:delete_user(ChainID, AuthenticatorName, <<"myuser">>)),
    ?assertEqual({error, not_found}, ?AUTH:lookup_user(ChainID, AuthenticatorName, <<"myuser">>)),

    ?assertEqual({ok, #{user_id => <<"myuser">>}}, ?AUTH:add_user(ChainID, AuthenticatorName, UserInfo)),
    ?assertMatch({ok, #{user_id := <<"myuser">>}}, ?AUTH:lookup_user(ChainID, AuthenticatorName, <<"myuser">>)),
    ?assertEqual(ok, ?AUTH:delete_authenticator(ChainID, AuthenticatorName)),
    ?assertEqual({ok, AuthenticatorConfig}, ?AUTH:create_authenticator(ChainID, AuthenticatorConfig)),
    ?assertMatch({error, not_found}, ?AUTH:lookup_user(ChainID, AuthenticatorName, <<"myuser">>)),

    ?AUTH:unbind([ListenerID], ChainID),
    ?assertEqual(ok, ?AUTH:delete_chain(ChainID)),
    ?assertEqual([], ets:tab2list(mnesia_basic_auth)),
    ok.

t_import(_) ->
    ChainID = <<"mychain">>,
    Chain = #{id => ChainID,
              type => simple},
    ?assertMatch({ok, #{id := ChainID, authenticators := []}}, ?AUTH:create_chain(Chain)),

    AuthenticatorName = <<"myauthenticator">>,
    AuthenticatorConfig = #{name => AuthenticatorName,
                            type => 'built-in-database',
                            config => #{
                                user_id_type => username,
                                password_hash_algorithm => #{
                                    name => sha256
                                }}},
    ?assertEqual({ok, AuthenticatorConfig}, ?AUTH:create_authenticator(ChainID, AuthenticatorConfig)),

    Dir = code:lib_dir(emqx_authn, test),
    ?assertEqual(ok, ?AUTH:import_users(ChainID, AuthenticatorName, filename:join([Dir, "data/user-credentials.json"]))),
    ?assertEqual(ok, ?AUTH:import_users(ChainID, AuthenticatorName, filename:join([Dir, "data/user-credentials.csv"]))),
    ?assertMatch({ok, #{user_id := <<"myuser1">>}}, ?AUTH:lookup_user(ChainID, AuthenticatorName, <<"myuser1">>)),
    ?assertMatch({ok, #{user_id := <<"myuser3">>}}, ?AUTH:lookup_user(ChainID, AuthenticatorName, <<"myuser3">>)),

    ListenerID = <<"listener1">>,
    ?AUTH:bind(ChainID, [ListenerID]),

    ClientInfo1 = #{listener_id => ListenerID,
			        username => <<"myuser1">>,
			        password => <<"mypassword1">>},
    ?assertEqual(ok, ?AUTH:authenticate(ClientInfo1)),
    ClientInfo2 = ClientInfo1#{username => <<"myuser3">>,
                               password => <<"mypassword3">>},
    ?assertEqual(ok, ?AUTH:authenticate(ClientInfo2)),

    ?AUTH:unbind([ListenerID], ChainID),
    ?assertEqual(ok, ?AUTH:delete_chain(ChainID)),
    ok.

t_multi_mnesia_authenticator(_) ->
    ChainID = <<"mychain">>,
    Chain = #{id => ChainID,
              type => simple},
    ?assertMatch({ok, #{id := ChainID, authenticators := []}}, ?AUTH:create_chain(Chain)),

    AuthenticatorName1 = <<"myauthenticator1">>,
    AuthenticatorConfig1 = #{name => AuthenticatorName1,
                             type => 'built-in-database',
                             config => #{
                                 user_id_type => username,
                                 password_hash_algorithm => #{
                                     name => sha256
                                 }}},
    AuthenticatorName2 = <<"myauthenticator2">>,
    AuthenticatorConfig2 = #{name => AuthenticatorName2,
                             type => 'built-in-database',
                             config => #{
                                 user_id_type => clientid,
                                 password_hash_algorithm => #{
                                     name => sha256
                                 }}},
    ?assertEqual({ok, AuthenticatorConfig1}, ?AUTH:create_authenticator(ChainID, AuthenticatorConfig1)),
    ?assertEqual({ok, AuthenticatorConfig2}, ?AUTH:create_authenticator(ChainID, AuthenticatorConfig2)),

    ?assertEqual({ok, #{user_id => <<"myuser">>}},
                 ?AUTH:add_user(ChainID, AuthenticatorName1,
                                #{<<"user_id">> => <<"myuser">>,
                                  <<"password">> => <<"mypass1">>})),
    ?assertEqual({ok, #{user_id => <<"myclient">>}},
                 ?AUTH:add_user(ChainID, AuthenticatorName2,
                                #{<<"user_id">> => <<"myclient">>,
                                  <<"password">> => <<"mypass2">>})),

    ListenerID = <<"listener1">>,
    ?AUTH:bind(ChainID, [ListenerID]),

    ClientInfo1 = #{listener_id => ListenerID,
			        username => <<"myuser">>,
                    clientid => <<"myclient">>,
			        password => <<"mypass1">>},
    ?assertEqual(ok, ?AUTH:authenticate(ClientInfo1)),
    ?assertEqual(ok, ?AUTH:move_authenticator_to_the_front(ChainID, AuthenticatorName2)),

    ?assertEqual({error, bad_password}, ?AUTH:authenticate(ClientInfo1)),
    ClientInfo2 = ClientInfo1#{password => <<"mypass2">>},
    ?assertEqual(ok, ?AUTH:authenticate(ClientInfo2)),

    ?AUTH:unbind([ListenerID], ChainID),
    ?assertEqual(ok, ?AUTH:delete_chain(ChainID)),
    ok.
