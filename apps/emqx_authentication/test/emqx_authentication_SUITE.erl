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

-module(emqx_authentication_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(AUTH, emqx_authentication).

all() ->
    emqx_ct:all(?MODULE).

init_per_suite(Config) ->
    emqx_ct_helpers:start_apps([emqx_authentication]),
    Config.

end_per_suite(_) ->
    emqx_ct_helpers:stop_apps([emqx_authentication]),
    ok.

t_chain(_) ->
    ChainID = <<"mychain">>,
    ChainParams = #{chain_id => ChainID,
                    services => []},
    ?assertEqual({ok, ChainID}, ?AUTH:create_chain(ChainParams)),
    ?assertEqual({error, {already_exists, {chain, ChainID}}}, ?AUTH:create_chain(ChainParams)),
    ?assertMatch({ok, #{id := ChainID, services := []}}, ?AUTH:lookup_chain(ChainID)),
    ?assertEqual(ok, ?AUTH:delete_chain(ChainID)),
    ?assertMatch({error, {not_found, {chain, ChainID}}}, ?AUTH:lookup_chain(ChainID)),
    ok.

t_service(_) ->
    ChainID = <<"mychain">>,
    ServiceName1 = <<"myservice1">>,
    ServiceParams1 = #{name => ServiceName1,
                       type => mnesia,
                       params => #{
                           user_identity_type => <<"username">>,
                           password_hash_algorithm => <<"sha256">>}},
    ChainParams = #{chain_id => ChainID,
                    services => [ServiceParams1]},
    ?assertEqual({ok, ChainID}, ?AUTH:create_chain(ChainParams)),
    Service1 = ServiceParams1,
    ?assertMatch({ok, #{id := ChainID, services := [Service1]}}, ?AUTH:lookup_chain(ChainID)),
    ?assertEqual({ok, Service1}, ?AUTH:lookup_service(ChainID, ServiceName1)),
    ?assertEqual({ok, [Service1]}, ?AUTH:list_services(ChainID)),
    ?assertEqual({error, {already_exists, {service, ServiceName1}}}, ?AUTH:add_services(ChainID, [ServiceParams1])),
    ServiceName2 = <<"myservice2">>,
    ServiceParams2 = ServiceParams1#{name => ServiceName2},
    ?assertEqual(ok, ?AUTH:add_services(ChainID, [ServiceParams2])),
    Service2 = ServiceParams2,
    ?assertMatch({ok, #{id := ChainID, services := [Service1, Service2]}}, ?AUTH:lookup_chain(ChainID)),
    ?assertEqual({ok, Service2}, ?AUTH:lookup_service(ChainID, ServiceName2)),
    ?assertEqual({ok, [Service1, Service2]}, ?AUTH:list_services(ChainID)),

    ?assertEqual(ok, ?AUTH:move_service_to_the_front(ChainID, ServiceName2)),
    ?assertEqual({ok, [Service2, Service1]}, ?AUTH:list_services(ChainID)),
    ?assertEqual(ok, ?AUTH:move_service_to_the_end(ChainID, ServiceName2)),
    ?assertEqual({ok, [Service1, Service2]}, ?AUTH:list_services(ChainID)),
    ?assertEqual(ok, ?AUTH:move_service_to_the_nth(ChainID, ServiceName2, 1)),
    ?assertEqual({ok, [Service2, Service1]}, ?AUTH:list_services(ChainID)),
    ?assertEqual({error, out_of_range}, ?AUTH:move_service_to_the_nth(ChainID, ServiceName2, 3)),
    ?assertEqual({error, out_of_range}, ?AUTH:move_service_to_the_nth(ChainID, ServiceName2, 0)),
    ?assertEqual(ok, ?AUTH:delete_services(ChainID, [ServiceName1, ServiceName2])),
    ?assertEqual({ok, []}, ?AUTH:list_services(ChainID)),
    ?assertEqual(ok, ?AUTH:delete_chain(ChainID)),
    ok.

t_mnesia_service(_) ->
    ChainID = <<"mychain">>,
    ServiceName = <<"myservice">>,
    ServiceParams = #{name => ServiceName,
                      type => mnesia,
                      params => #{
                          user_identity_type => <<"username">>,
                          password_hash_algorithm => <<"sha256">>}},
    ChainParams = #{chain_id => ChainID,
                    services => [ServiceParams]},
    ?assertEqual({ok, ChainID}, ?AUTH:create_chain(ChainParams)),
    UserCredential = #{user_identity => <<"myuser">>,
                       password => <<"mypass">>},
    ?assertEqual(ok, ?AUTH:add_user_credential(ChainID, ServiceName, UserCredential)),
    ?assertMatch({ok, #{user_identity := <<"myuser">>, password_hash := _}},
                 ?AUTH:lookup_user_credential(ChainID, ServiceName, <<"myuser">>)),
    ClientInfo = #{chain_id => ChainID,
			       username => <<"myuser">>,
			       password => <<"mypass">>},
    ?assertEqual(ok, ?AUTH:authenticate(ClientInfo)),
    ClientInfo2 = ClientInfo#{username => <<"baduser">>},
    ?assertEqual({error, user_credential_not_found}, ?AUTH:authenticate(ClientInfo2)),
    ClientInfo3 = ClientInfo#{password => <<"badpass">>},
    ?assertEqual({error, bad_password}, ?AUTH:authenticate(ClientInfo3)),
    UserCredential2 = UserCredential#{password => <<"mypass2">>},
    ?assertEqual(ok, ?AUTH:update_user_credential(ChainID, ServiceName, UserCredential2)),
    ClientInfo4 = ClientInfo#{password => <<"mypass2">>},
    ?assertEqual(ok, ?AUTH:authenticate(ClientInfo4)),
    ?assertEqual(ok, ?AUTH:delete_user_credential(ChainID, ServiceName, <<"myuser">>)),
    ?assertEqual({error, not_found}, ?AUTH:lookup_user_credential(ChainID, ServiceName, <<"myuser">>)),

    ?assertEqual(ok, ?AUTH:add_user_credential(ChainID, ServiceName, UserCredential)),
    ?assertMatch({ok, #{user_identity := <<"myuser">>}}, ?AUTH:lookup_user_credential(ChainID, ServiceName, <<"myuser">>)),
    ?assertEqual(ok, ?AUTH:delete_services(ChainID, [ServiceName])),
    ?assertEqual(ok, ?AUTH:add_services(ChainID, [ServiceParams])),
    ?assertMatch({error, not_found}, ?AUTH:lookup_user_credential(ChainID, ServiceName, <<"myuser">>)),

    ?assertEqual(ok, ?AUTH:delete_chain(ChainID)),
    ?assertEqual([], ets:tab2list(mnesia_basic_auth)),
    ok.

t_import(_) ->
    ChainID = <<"mychain">>,
    ServiceName = <<"myservice">>,
    ServiceParams = #{name => ServiceName,
                      type => mnesia,
                      params => #{
                          user_identity_type => <<"username">>,
                          password_hash_algorithm => <<"sha256">>}},
    ChainParams = #{chain_id => ChainID,
                    services => [ServiceParams]},
    ?assertEqual({ok, ChainID}, ?AUTH:create_chain(ChainParams)),
    Dir = code:lib_dir(emqx_authentication, test),
    ?assertEqual(ok, ?AUTH:import_user_credentials(ChainID, ServiceName, filename:join([Dir, "data/user-credentials.json"]), json)),
    ?assertEqual(ok, ?AUTH:import_user_credentials(ChainID, ServiceName, filename:join([Dir, "data/user-credentials.csv"]), csv)),
    ?assertMatch({ok, #{user_identity := <<"myuser1">>}}, ?AUTH:lookup_user_credential(ChainID, ServiceName, <<"myuser1">>)),
    ?assertMatch({ok, #{user_identity := <<"myuser3">>}}, ?AUTH:lookup_user_credential(ChainID, ServiceName, <<"myuser3">>)),
    ClientInfo1 = #{chain_id => ChainID,
			        username => <<"myuser1">>,
			        password => <<"mypassword1">>},
    ?assertEqual(ok, ?AUTH:authenticate(ClientInfo1)),
    ClientInfo2 = ClientInfo1#{username => <<"myuser3">>,
                               password => <<"mypassword3">>},
    ?assertEqual(ok, ?AUTH:authenticate(ClientInfo2)),
    ?assertEqual(ok, ?AUTH:delete_chain(ChainID)),
    ok.

t_multi_mnesia_service(_) ->
    ChainID = <<"mychain">>,
    ServiceName1 = <<"myservice1">>,
    ServiceParams1 = #{name => ServiceName1,
                       type => mnesia,
                       params => #{
                           user_identity_type => <<"username">>,
                           password_hash_algorithm => <<"sha256">>}},
    ServiceName2 = <<"myservice2">>,
    ServiceParams2 = #{name => ServiceName2,
                       type => mnesia,
                       params => #{
                           user_identity_type => <<"clientid">>,
                           password_hash_algorithm => <<"sha256">>}},
    ChainParams = #{chain_id => ChainID,
                    services => [ServiceParams1, ServiceParams2]},
    ?assertEqual({ok, ChainID}, ?AUTH:create_chain(ChainParams)),

    ?assertEqual(ok, ?AUTH:add_user_credential(ChainID,
                                               ServiceName1,
                                               #{user_identity => <<"myuser">>,
                                                 password => <<"mypass1">>})),
    ?assertEqual(ok, ?AUTH:add_user_credential(ChainID,
                                               ServiceName2,
                                               #{user_identity => <<"myclient">>,
                                                 password => <<"mypass2">>})),
    ClientInfo1 = #{chain_id => ChainID,
			        username => <<"myuser">>,
                    clientid => <<"myclient">>,
			        password => <<"mypass1">>},
    ?assertEqual(ok, ?AUTH:authenticate(ClientInfo1)),
    ?assertEqual(ok, ?AUTH:move_service_to_the_front(ChainID, ServiceName2)),
    ?assertEqual({error, bad_password}, ?AUTH:authenticate(ClientInfo1)),
    ClientInfo2 = ClientInfo1#{password => <<"mypass2">>},
    ?assertEqual(ok, ?AUTH:authenticate(ClientInfo2)),
    ?assertEqual(ok, ?AUTH:delete_chain(ChainID)),
    ok.



