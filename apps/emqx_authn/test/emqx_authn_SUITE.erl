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

-module(emqx_authn_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(AUTH, emqx_authn).

all() ->
    emqx_ct:all(?MODULE).

init_per_suite(Config) ->
    application:set_env(ekka, strict_mode, true),
    emqx_ct_helpers:start_apps([emqx_authn]),
    Config.

end_per_suite(_) ->
    emqx_ct_helpers:stop_apps([emqx_authn]),
    ok.

t_chain(_) ->
    ChainID = <<"mychain">>,
    ?assertMatch({ok, #{id := ChainID, services := []}}, ?AUTH:create_chain(#{id => ChainID})),
    ?assertEqual({error, {already_exists, {chain, ChainID}}}, ?AUTH:create_chain(#{id => ChainID})),
    ?assertMatch({ok, #{id := ChainID, services := []}}, ?AUTH:lookup_chain(ChainID)),
    ?assertEqual(ok, ?AUTH:delete_chain(ChainID)),
    ?assertMatch({error, {not_found, {chain, ChainID}}}, ?AUTH:lookup_chain(ChainID)),
    ok.

t_service(_) ->
    ChainID = <<"mychain">>,
    ?assertMatch({ok, #{id := ChainID, services := []}}, ?AUTH:create_chain(#{id => ChainID})),
    ?assertMatch({ok, #{id := ChainID, services := []}}, ?AUTH:lookup_chain(ChainID)),

    ServiceName1 = <<"myservice1">>,
    ServiceParams1 = #{name => ServiceName1,
                       type => mnesia,
                       params => #{
                           user_id_type => <<"username">>,
                           password_hash_algorithm => <<"sha256">>}},
    ?assertEqual({ok, [ServiceParams1]}, ?AUTH:add_services(ChainID, [ServiceParams1])),
    ?assertEqual({ok, ServiceParams1}, ?AUTH:lookup_service(ChainID, ServiceName1)),
    ?assertEqual({ok, [ServiceParams1]}, ?AUTH:list_services(ChainID)),
    ?assertEqual({error, {already_exists, {service, ServiceName1}}}, ?AUTH:add_services(ChainID, [ServiceParams1])),

    ServiceName2 = <<"myservice2">>,
    ServiceParams2 = ServiceParams1#{name => ServiceName2},
    ?assertEqual({ok, [ServiceParams2]}, ?AUTH:add_services(ChainID, [ServiceParams2])),
    ?assertMatch({ok, #{id := ChainID, services := [ServiceParams1, ServiceParams2]}}, ?AUTH:lookup_chain(ChainID)),
    ?assertEqual({ok, ServiceParams2}, ?AUTH:lookup_service(ChainID, ServiceName2)),
    ?assertEqual({ok, [ServiceParams1, ServiceParams2]}, ?AUTH:list_services(ChainID)),

    ?assertEqual(ok, ?AUTH:move_service_to_the_front(ChainID, ServiceName2)),
    ?assertEqual({ok, [ServiceParams2, ServiceParams1]}, ?AUTH:list_services(ChainID)),
    ?assertEqual(ok, ?AUTH:move_service_to_the_end(ChainID, ServiceName2)),
    ?assertEqual({ok, [ServiceParams1, ServiceParams2]}, ?AUTH:list_services(ChainID)),
    ?assertEqual(ok, ?AUTH:move_service_to_the_nth(ChainID, ServiceName2, 1)),
    ?assertEqual({ok, [ServiceParams2, ServiceParams1]}, ?AUTH:list_services(ChainID)),
    ?assertEqual({error, out_of_range}, ?AUTH:move_service_to_the_nth(ChainID, ServiceName2, 3)),
    ?assertEqual({error, out_of_range}, ?AUTH:move_service_to_the_nth(ChainID, ServiceName2, 0)),
    ?assertEqual(ok, ?AUTH:delete_services(ChainID, [ServiceName1, ServiceName2])),
    ?assertEqual({ok, []}, ?AUTH:list_services(ChainID)),
    ?assertEqual(ok, ?AUTH:delete_chain(ChainID)),
    ok.

t_mnesia_service(_) ->
    ChainID = <<"mychain">>,
    ?assertMatch({ok, #{id := ChainID, services := []}}, ?AUTH:create_chain(#{id => ChainID})),

    ServiceName = <<"myservice">>,
    ServiceParams = #{name => ServiceName,
                      type => mnesia,
                      params => #{
                          user_id_type => <<"username">>,
                          password_hash_algorithm => <<"sha256">>}},
    ?assertEqual({ok, [ServiceParams]}, ?AUTH:add_services(ChainID, [ServiceParams])),

    UserInfo = #{<<"user_id">> => <<"myuser">>,
                 <<"password">> => <<"mypass">>},
    ?assertEqual({ok, #{user_id => <<"myuser">>}}, ?AUTH:add_user(ChainID, ServiceName, UserInfo)),
    ?assertEqual({ok, #{user_id => <<"myuser">>}}, ?AUTH:lookup_user(ChainID, ServiceName, <<"myuser">>)),
    ClientInfo = #{chain_id => ChainID,
			       username => <<"myuser">>,
			       password => <<"mypass">>},
    ?assertEqual(ok, ?AUTH:authenticate(ClientInfo)),
    ClientInfo2 = ClientInfo#{username => <<"baduser">>},
    ?assertEqual({error, user_not_found}, ?AUTH:authenticate(ClientInfo2)),
    ClientInfo3 = ClientInfo#{password => <<"badpass">>},
    ?assertEqual({error, bad_password}, ?AUTH:authenticate(ClientInfo3)),
    UserInfo2 = UserInfo#{<<"password">> => <<"mypass2">>},
    ?assertEqual({ok, #{user_id => <<"myuser">>}}, ?AUTH:update_user(ChainID, ServiceName, <<"myuser">>, UserInfo2)),
    ClientInfo4 = ClientInfo#{password => <<"mypass2">>},
    ?assertEqual(ok, ?AUTH:authenticate(ClientInfo4)),
    ?assertEqual(ok, ?AUTH:delete_user(ChainID, ServiceName, <<"myuser">>)),
    ?assertEqual({error, not_found}, ?AUTH:lookup_user(ChainID, ServiceName, <<"myuser">>)),

    ?assertEqual({ok, #{user_id => <<"myuser">>}}, ?AUTH:add_user(ChainID, ServiceName, UserInfo)),
    ?assertMatch({ok, #{user_id := <<"myuser">>}}, ?AUTH:lookup_user(ChainID, ServiceName, <<"myuser">>)),
    ?assertEqual(ok, ?AUTH:delete_services(ChainID, [ServiceName])),
    ?assertEqual({ok, [ServiceParams]}, ?AUTH:add_services(ChainID, [ServiceParams])),
    ?assertMatch({error, not_found}, ?AUTH:lookup_user(ChainID, ServiceName, <<"myuser">>)),

    ?assertEqual(ok, ?AUTH:delete_chain(ChainID)),
    ?assertEqual([], ets:tab2list(mnesia_basic_auth)),
    ok.

t_import(_) ->
    ChainID = <<"mychain">>,
    ?assertMatch({ok, #{id := ChainID, services := []}}, ?AUTH:create_chain(#{id => ChainID})),

    ServiceName = <<"myservice">>,
    ServiceParams = #{name => ServiceName,
                      type => mnesia,
                      params => #{
                          user_id_type => <<"username">>,
                          password_hash_algorithm => <<"sha256">>}},
    ?assertEqual({ok, [ServiceParams]}, ?AUTH:add_services(ChainID, [ServiceParams])),

    Dir = code:lib_dir(emqx_authn, test),
    ?assertEqual(ok, ?AUTH:import_users(ChainID, ServiceName, filename:join([Dir, "data/user-credentials.json"]))),
    ?assertEqual(ok, ?AUTH:import_users(ChainID, ServiceName, filename:join([Dir, "data/user-credentials.csv"]))),
    ?assertMatch({ok, #{user_id := <<"myuser1">>}}, ?AUTH:lookup_user(ChainID, ServiceName, <<"myuser1">>)),
    ?assertMatch({ok, #{user_id := <<"myuser3">>}}, ?AUTH:lookup_user(ChainID, ServiceName, <<"myuser3">>)),
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
    ?assertMatch({ok, #{id := ChainID, services := []}}, ?AUTH:create_chain(#{id => ChainID})),

    ServiceName1 = <<"myservice1">>,
    ServiceParams1 = #{name => ServiceName1,
                       type => mnesia,
                       params => #{
                           user_id_type => <<"username">>,
                           password_hash_algorithm => <<"sha256">>}},
    ServiceName2 = <<"myservice2">>,
    ServiceParams2 = #{name => ServiceName2,
                       type => mnesia,
                       params => #{
                           user_id_type => <<"clientid">>,
                           password_hash_algorithm => <<"sha256">>}},
    ?assertEqual({ok, [ServiceParams1]}, ?AUTH:add_services(ChainID, [ServiceParams1])),
    ?assertEqual({ok, [ServiceParams2]}, ?AUTH:add_services(ChainID, [ServiceParams2])),

    ?assertEqual({ok, #{user_id => <<"myuser">>}},
                 ?AUTH:add_user(ChainID, ServiceName1,
                                #{<<"user_id">> => <<"myuser">>,
                                  <<"password">> => <<"mypass1">>})),
    ?assertEqual({ok, #{user_id => <<"myclient">>}},
                 ?AUTH:add_user(ChainID, ServiceName2,
                                #{<<"user_id">> => <<"myclient">>,
                                  <<"password">> => <<"mypass2">>})),
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
