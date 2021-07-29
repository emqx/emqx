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

-include("emqx_authn.hrl").

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
    ?assertMatch({ok, #{id := ?CHAIN, authenticators := []}}, ?AUTH:lookup_chain(?CHAIN)),

    ChainID = <<"mychain">>,
    Chain = #{id => ChainID},
    ?assertMatch({ok, #{id := ChainID, authenticators := []}}, ?AUTH:create_chain(Chain)),
    ?assertEqual({error, {already_exists, {chain, ChainID}}}, ?AUTH:create_chain(Chain)),
    ?assertMatch({ok, #{id := ChainID, authenticators := []}}, ?AUTH:lookup_chain(ChainID)),
    ?assertEqual(ok, ?AUTH:delete_chain(ChainID)),
    ?assertMatch({error, {not_found, {chain, ChainID}}}, ?AUTH:lookup_chain(ChainID)),
    ok.

t_authenticator(_) ->
    AuthenticatorName1 = <<"myauthenticator1">>,
    AuthenticatorConfig1 = #{name => AuthenticatorName1,
                             mechanism => 'password-based',
                             server_type => 'built-in-database',
                             user_id_type => username,
                             password_hash_algorithm => #{
                                 name => sha256
                             }},
    {ok, #{name := AuthenticatorName1, id := ID1}} = ?AUTH:create_authenticator(?CHAIN, AuthenticatorConfig1),
    ?assertMatch({ok, #{name := AuthenticatorName1}}, ?AUTH:lookup_authenticator(?CHAIN, ID1)),
    ?assertMatch({ok, [#{name := AuthenticatorName1}]}, ?AUTH:list_authenticators(?CHAIN)),
    ?assertEqual({error, name_has_be_used}, ?AUTH:create_authenticator(?CHAIN, AuthenticatorConfig1)),

    AuthenticatorConfig2 = #{name => AuthenticatorName1,
                             mechanism => jwt,
                             use_jwks => false,
                             algorithm => 'hmac-based',
                             secret => <<"abcdef">>,
                             secret_base64_encoded => false,
                             verify_claims => []},
    {ok, #{name := AuthenticatorName1, id := ID1, mechanism := jwt}} = ?AUTH:update_authenticator(?CHAIN, ID1, AuthenticatorConfig2),

    ID2 = <<"random">>,
    ?assertEqual({error, {not_found, {authenticator, ID2}}}, ?AUTH:update_authenticator(?CHAIN, ID2, AuthenticatorConfig2)),
    ?assertEqual({error, name_has_be_used}, ?AUTH:update_or_create_authenticator(?CHAIN, ID2, AuthenticatorConfig2)),

    AuthenticatorName2 = <<"myauthenticator2">>,
    AuthenticatorConfig3 = AuthenticatorConfig2#{name => AuthenticatorName2},
    {ok, #{name := AuthenticatorName2, id := ID2, secret := <<"abcdef">>}} = ?AUTH:update_or_create_authenticator(?CHAIN, ID2, AuthenticatorConfig3),
    ?assertMatch({ok, #{name := AuthenticatorName2}}, ?AUTH:lookup_authenticator(?CHAIN, ID2)),
    {ok, #{name := AuthenticatorName2, id := ID2, secret := <<"fedcba">>}} = ?AUTH:update_or_create_authenticator(?CHAIN, ID2, AuthenticatorConfig3#{secret := <<"fedcba">>}),

    ?assertMatch({ok, #{id := ?CHAIN, authenticators := [#{name := AuthenticatorName1}, #{name := AuthenticatorName2}]}}, ?AUTH:lookup_chain(?CHAIN)),
    ?assertMatch({ok, [#{name := AuthenticatorName1}, #{name := AuthenticatorName2}]}, ?AUTH:list_authenticators(?CHAIN)),

    ?assertEqual(ok, ?AUTH:move_authenticator_to_the_nth(?CHAIN, ID2, 1)),
    ?assertMatch({ok, [#{name := AuthenticatorName2}, #{name := AuthenticatorName1}]}, ?AUTH:list_authenticators(?CHAIN)),
    ?assertEqual({error, out_of_range}, ?AUTH:move_authenticator_to_the_nth(?CHAIN, ID2, 3)),
    ?assertEqual({error, out_of_range}, ?AUTH:move_authenticator_to_the_nth(?CHAIN, ID2, 0)),
    ?assertEqual(ok, ?AUTH:delete_authenticator(?CHAIN, ID1)),
    ?assertEqual(ok, ?AUTH:delete_authenticator(?CHAIN, ID2)),
    ?assertEqual({ok, []}, ?AUTH:list_authenticators(?CHAIN)),
    ok.

t_authenticate(_) ->
    ClientInfo = #{zone => default,
                   listener => mqtt_tcp,
                   username => <<"myuser">>,
			       password => <<"mypass">>},
    ?assertEqual(ok, emqx_access_control:authenticate(ClientInfo)),
    emqx_authn:enable(),
    ?assertEqual({error, not_authorized}, emqx_access_control:authenticate(ClientInfo)).
