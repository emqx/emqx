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
    emqx_ct_helpers:start_apps([emqx_authn], fun set_special_configs/1),
    Config.

end_per_suite(_) ->
    file:delete(filename:join(emqx:get_env(plugins_etc_dir), 'emqx_authn.conf')),
    emqx_ct_helpers:stop_apps([emqx_authn]),
    ok.

set_special_configs(emqx_authn) ->
    application:set_env(emqx, plugins_etc_dir,
                        emqx_ct_helpers:deps_path(emqx_authn, "test")),
    Conf = #{<<"emqx_authn">> => #{<<"authenticators">> => [], <<"enable">> => false}},
    ok = file:write_file(filename:join(emqx:get_env(plugins_etc_dir), 'emqx_authn.conf'), jsx:encode(Conf)),
    ok;
set_special_configs(_App) ->
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
                             config => #{
                                 server_type => 'built-in-database',
                                 user_id_type => username,
                                 password_hash_algorithm => #{
                                     name => sha256
                                 }}},
    ?assertEqual({ok, AuthenticatorConfig1}, ?AUTH:create_authenticator(?CHAIN, AuthenticatorConfig1)),
    ?assertEqual({ok, AuthenticatorConfig1}, ?AUTH:lookup_authenticator(?CHAIN, AuthenticatorName1)),
    ?assertEqual({ok, [AuthenticatorConfig1]}, ?AUTH:list_authenticators(?CHAIN)),
    ?assertEqual({error, {already_exists, {authenticator, AuthenticatorName1}}}, ?AUTH:create_authenticator(?CHAIN, AuthenticatorConfig1)),

    AuthenticatorName2 = <<"myauthenticator2">>,
    AuthenticatorConfig2 = AuthenticatorConfig1#{name => AuthenticatorName2},
    ?assertEqual({ok, AuthenticatorConfig2}, ?AUTH:create_authenticator(?CHAIN, AuthenticatorConfig2)),
    ?assertMatch({ok, #{id := ?CHAIN, authenticators := [AuthenticatorConfig1, AuthenticatorConfig2]}}, ?AUTH:lookup_chain(?CHAIN)),
    ?assertEqual({ok, AuthenticatorConfig2}, ?AUTH:lookup_authenticator(?CHAIN, AuthenticatorName2)),
    ?assertEqual({ok, [AuthenticatorConfig1, AuthenticatorConfig2]}, ?AUTH:list_authenticators(?CHAIN)),

    ?assertEqual(ok, ?AUTH:move_authenticator_to_the_front(?CHAIN, AuthenticatorName2)),
    ?assertEqual({ok, [AuthenticatorConfig2, AuthenticatorConfig1]}, ?AUTH:list_authenticators(?CHAIN)),
    ?assertEqual(ok, ?AUTH:move_authenticator_to_the_end(?CHAIN, AuthenticatorName2)),
    ?assertEqual({ok, [AuthenticatorConfig1, AuthenticatorConfig2]}, ?AUTH:list_authenticators(?CHAIN)),
    ?assertEqual(ok, ?AUTH:move_authenticator_to_the_nth(?CHAIN, AuthenticatorName2, 1)),
    ?assertEqual({ok, [AuthenticatorConfig2, AuthenticatorConfig1]}, ?AUTH:list_authenticators(?CHAIN)),
    ?assertEqual({error, out_of_range}, ?AUTH:move_authenticator_to_the_nth(?CHAIN, AuthenticatorName2, 3)),
    ?assertEqual({error, out_of_range}, ?AUTH:move_authenticator_to_the_nth(?CHAIN, AuthenticatorName2, 0)),
    ?assertEqual(ok, ?AUTH:delete_authenticator(?CHAIN, AuthenticatorName1)),
    ?assertEqual(ok, ?AUTH:delete_authenticator(?CHAIN, AuthenticatorName2)),
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
