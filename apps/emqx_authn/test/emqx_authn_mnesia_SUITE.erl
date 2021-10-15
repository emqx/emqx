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

% -include_lib("common_test/include/ct.hrl").
% -include_lib("eunit/include/eunit.hrl").

% -include("emqx_authn.hrl").

% -define(AUTH, emqx_authn).

all() ->
    emqx_common_test_helpers:all(?MODULE).

% init_per_suite(Config) ->
%     emqx_common_test_helpers:start_apps([emqx_authn]),
%     Config.

% end_per_suite(_) ->
%     emqx_common_test_helpers:stop_apps([emqx_authn]),
%     ok.

% t_mnesia_authenticator(_) ->
%     AuthenticatorName = <<"myauthenticator">>,
%     AuthenticatorConfig = #{name => AuthenticatorName,
%                             mechanism => 'password-based',
%                             server_type => 'built-in-database',
%                             user_id_type => username,
%                             password_hash_algorithm => #{
%                                 name => sha256
%                             }},
%     {ok, #{name := AuthenticatorName, id := ID}} = ?AUTH:create_authenticator(?CHAIN, AuthenticatorConfig),

%     UserInfo = #{user_id => <<"myuser">>,
%                  password => <<"mypass">>},
%     ?assertMatch({ok, #{user_id := <<"myuser">>}}, ?AUTH:add_user(?CHAIN, ID, UserInfo)),
%     ?assertMatch({ok, #{user_id := <<"myuser">>}}, ?AUTH:lookup_user(?CHAIN, ID, <<"myuser">>)),

%     ClientInfo = #{zone => external,
%                    username => <<"myuser">>,
% 			       password => <<"mypass">>},
%     ?assertEqual({stop, {ok, #{is_superuser => false}}}, ?AUTH:authenticate(ClientInfo, ignored)),
%     ?AUTH:enable(),
%     ?assertEqual({ok, #{is_superuser => false}}, emqx_access_control:authenticate(ClientInfo)),

%     ClientInfo2 = ClientInfo#{username => <<"baduser">>},
%     ?assertEqual({stop, {error, not_authorized}}, ?AUTH:authenticate(ClientInfo2, ignored)),
%     ?assertEqual({error, not_authorized}, emqx_access_control:authenticate(ClientInfo2)),

%     ClientInfo3 = ClientInfo#{password => <<"badpass">>},
%     ?assertEqual({stop, {error, bad_username_or_password}}, ?AUTH:authenticate(ClientInfo3, ignored)),
%     ?assertEqual({error, bad_username_or_password}, emqx_access_control:authenticate(ClientInfo3)),

%     UserInfo2 = UserInfo#{password => <<"mypass2">>},
%     ?assertMatch({ok, #{user_id := <<"myuser">>}}, ?AUTH:update_user(?CHAIN, ID, <<"myuser">>, UserInfo2)),
%     ClientInfo4 = ClientInfo#{password => <<"mypass2">>},
%     ?assertEqual({stop, {ok, #{is_superuser => false}}}, ?AUTH:authenticate(ClientInfo4, ignored)),

%     ?assertMatch({ok, #{user_id := <<"myuser">>}}, ?AUTH:update_user(?CHAIN, ID, <<"myuser">>, #{is_superuser => true})),
%     ?assertEqual({stop, {ok, #{is_superuser => true}}}, ?AUTH:authenticate(ClientInfo4, ignored)),

%     ?assertEqual(ok, ?AUTH:delete_user(?CHAIN, ID, <<"myuser">>)),
%     ?assertEqual({error, not_found}, ?AUTH:lookup_user(?CHAIN, ID, <<"myuser">>)),

%     ?assertMatch({ok, #{user_id := <<"myuser">>}}, ?AUTH:add_user(?CHAIN, ID, UserInfo)),
%     ?assertMatch({ok, #{user_id := <<"myuser">>}}, ?AUTH:lookup_user(?CHAIN, ID, <<"myuser">>)),
%     ?assertEqual(ok, ?AUTH:delete_authenticator(?CHAIN, ID)),

%     {ok, #{name := AuthenticatorName, id := ID1}} = ?AUTH:create_authenticator(?CHAIN, AuthenticatorConfig),
%     ?assertMatch({error, not_found}, ?AUTH:lookup_user(?CHAIN, ID1, <<"myuser">>)),
%     ?assertEqual(ok, ?AUTH:delete_authenticator(?CHAIN, ID1)),
%     ok.

% t_import(_) ->
%     AuthenticatorName = <<"myauthenticator">>,
%     AuthenticatorConfig = #{name => AuthenticatorName,
%                             mechanism => 'password-based',
%                             server_type => 'built-in-database',
%                             user_id_type => username,
%                             password_hash_algorithm => #{
%                                 name => sha256
%                             }},
%     {ok, #{name := AuthenticatorName, id := ID}} = ?AUTH:create_authenticator(?CHAIN, AuthenticatorConfig),

%     Dir = code:lib_dir(emqx_authn, test),
%     ?assertEqual(ok, ?AUTH:import_users(?CHAIN, ID, filename:join([Dir, "data/user-credentials.json"]))),
%     ?assertEqual(ok, ?AUTH:import_users(?CHAIN, ID, filename:join([Dir, "data/user-credentials.csv"]))),
%     ?assertMatch({ok, #{user_id := <<"myuser1">>}}, ?AUTH:lookup_user(?CHAIN, ID, <<"myuser1">>)),
%     ?assertMatch({ok, #{user_id := <<"myuser3">>}}, ?AUTH:lookup_user(?CHAIN, ID, <<"myuser3">>)),

%     ClientInfo1 = #{username => <<"myuser1">>,
% 			        password => <<"mypassword1">>},
%     ?assertEqual({stop, {ok, #{is_superuser => true}}}, ?AUTH:authenticate(ClientInfo1, ignored)),

%     ClientInfo2 = ClientInfo1#{username => <<"myuser2">>,
%                                password => <<"mypassword2">>},
%     ?assertEqual({stop, {ok, #{is_superuser => false}}}, ?AUTH:authenticate(ClientInfo2, ignored)),

%     ClientInfo3 = ClientInfo1#{username => <<"myuser3">>,
%                                password => <<"mypassword3">>},
%     ?assertEqual({stop, {ok, #{is_superuser => true}}}, ?AUTH:authenticate(ClientInfo3, ignored)),

%     ?assertEqual(ok, ?AUTH:delete_authenticator(?CHAIN, ID)),
%     ok.

% t_multi_mnesia_authenticator(_) ->
%     AuthenticatorName1 = <<"myauthenticator1">>,
%     AuthenticatorConfig1 = #{name => AuthenticatorName1,
%                              mechanism => 'password-based',
%                              server_type => 'built-in-database',
%                              user_id_type => username,
%                              password_hash_algorithm => #{
%                                  name => sha256
%                              }},
%     AuthenticatorName2 = <<"myauthenticator2">>,
%     AuthenticatorConfig2 = #{name => AuthenticatorName2,
%                              mechanism => 'password-based',
%                              server_type => 'built-in-database',
%                              user_id_type => clientid,
%                              password_hash_algorithm => #{
%                                  name => sha256
%                              }},
%     {ok, #{name := AuthenticatorName1, id := ID1}} = ?AUTH:create_authenticator(?CHAIN, AuthenticatorConfig1),
%     {ok, #{name := AuthenticatorName2, id := ID2}} = ?AUTH:create_authenticator(?CHAIN, AuthenticatorConfig2),

%     ?assertMatch({ok, #{user_id := <<"myuser">>}},
%                  ?AUTH:add_user(?CHAIN, ID1,
%                                 #{user_id => <<"myuser">>,
%                                   password => <<"mypass1">>})),
%     ?assertMatch({ok, #{user_id := <<"myclient">>}},
%                  ?AUTH:add_user(?CHAIN, ID2,
%                                 #{user_id => <<"myclient">>,
%                                   password => <<"mypass2">>})),

%     ClientInfo1 = #{username => <<"myuser">>,
%                     clientid => <<"myclient">>,
% 			        password => <<"mypass1">>},
%     ?assertEqual({stop, {ok, #{is_superuser => false}}}, ?AUTH:authenticate(ClientInfo1, ignored)),
%     ?assertEqual(ok, ?AUTH:move_authenticator(?CHAIN, ID2, top)),

%     ?assertEqual({stop, {error, bad_username_or_password}}, ?AUTH:authenticate(ClientInfo1, ignored)),
%     ClientInfo2 = ClientInfo1#{password => <<"mypass2">>},
%     ?assertEqual({stop, {ok, #{is_superuser => false}}}, ?AUTH:authenticate(ClientInfo2, ignored)),

%     ?assertEqual(ok, ?AUTH:delete_authenticator(?CHAIN, ID1)),
%     ?assertEqual(ok, ?AUTH:delete_authenticator(?CHAIN, ID2)),
%     ok.
