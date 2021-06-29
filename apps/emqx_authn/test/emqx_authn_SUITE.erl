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

t_chain(_) ->
    ChainID = <<"mychain">>,
    Chain = #{id => ChainID,
              type => simple},
    ?assertMatch({ok, #{id := ChainID, authenticators := []}}, ?AUTH:create_chain(Chain)),
    ?assertEqual({error, {already_exists, {chain, ChainID}}}, ?AUTH:create_chain(Chain)),
    ?assertMatch({ok, #{id := ChainID, authenticators := []}}, ?AUTH:lookup_chain(ChainID)),
    ?assertEqual(ok, ?AUTH:delete_chain(ChainID)),
    ?assertMatch({error, {not_found, {chain, ChainID}}}, ?AUTH:lookup_chain(ChainID)),
    ok.

t_binding(_) ->
    Listener1 = <<"listener1">>,
    Listener2 = <<"listener2">>,
    ChainID = <<"mychain">>,

    ?assertEqual({error, {not_found, {chain, ChainID}}}, ?AUTH:bind(ChainID, [Listener1])),

    Chain = #{id => ChainID,
              type => simple},
    ?assertMatch({ok, #{id := ChainID, authenticators := []}}, ?AUTH:create_chain(Chain)),

    ?assertEqual(ok, ?AUTH:bind(ChainID, [Listener1])),
    ?assertEqual(ok, ?AUTH:bind(ChainID, [Listener2])),
    ?assertEqual({error, {already_bound, [Listener1]}}, ?AUTH:bind(ChainID, [Listener1])),
    {ok, #{listeners := Listeners}} = ?AUTH:list_bindings(ChainID),
    ?assertEqual(2, length(Listeners)),
    ?assertMatch({ok, #{simple := ChainID}}, ?AUTH:list_bound_chains(Listener1)),

    ?assertEqual(ok, ?AUTH:unbind(ChainID, [Listener1])),
    ?assertEqual(ok, ?AUTH:unbind(ChainID, [Listener2])),
    ?assertEqual({error, {not_found, [Listener1]}}, ?AUTH:unbind(ChainID, [Listener1])),

    ?assertEqual(ok, ?AUTH:delete_chain(ChainID)),
    ok.

t_binding2(_) ->
    ChainID = <<"mychain">>,
    Chain = #{id => ChainID,
              type => simple},
    ?assertMatch({ok, #{id := ChainID, authenticators := []}}, ?AUTH:create_chain(Chain)),

    Listener1 = <<"listener1">>,
    Listener2 = <<"listener2">>,

    ?assertEqual(ok, ?AUTH:bind(ChainID, [Listener1, Listener2])),
    {ok, #{listeners := Listeners}} = ?AUTH:list_bindings(ChainID),
    ?assertEqual(2, length(Listeners)),
    ?assertEqual(ok, ?AUTH:unbind(ChainID, [Listener1, Listener2])),
    ?assertMatch({ok, #{listeners := []}}, ?AUTH:list_bindings(ChainID)),

    ?assertEqual(ok, ?AUTH:delete_chain(ChainID)),
    ok.

t_authenticator(_) ->
    ChainID = <<"mychain">>,
    Chain = #{id => ChainID,
              type => simple},
    ?assertMatch({ok, #{id := ChainID, authenticators := []}}, ?AUTH:create_chain(Chain)),
    ?assertMatch({ok, #{id := ChainID, authenticators := []}}, ?AUTH:lookup_chain(ChainID)),

    AuthenticatorName1 = <<"myauthenticator1">>,
    AuthenticatorConfig1 = #{name => AuthenticatorName1,
                             type => 'built-in-database',
                             config => #{
                                 user_id_type => username,
                                 password_hash_algorithm => #{
                                     name => sha256
                                 }}},
    ?assertEqual({ok, AuthenticatorConfig1}, ?AUTH:create_authenticator(ChainID, AuthenticatorConfig1)),
    ?assertEqual({ok, AuthenticatorConfig1}, ?AUTH:lookup_authenticator(ChainID, AuthenticatorName1)),
    ?assertEqual({ok, [AuthenticatorConfig1]}, ?AUTH:list_authenticators(ChainID)),
    ?assertEqual({error, {already_exists, {authenticator, AuthenticatorName1}}}, ?AUTH:create_authenticator(ChainID, AuthenticatorConfig1)),

    AuthenticatorName2 = <<"myauthenticator2">>,
    AuthenticatorConfig2 = AuthenticatorConfig1#{name => AuthenticatorName2},
    ?assertEqual({ok, AuthenticatorConfig2}, ?AUTH:create_authenticator(ChainID, AuthenticatorConfig2)),
    ?assertMatch({ok, #{id := ChainID, authenticators := [AuthenticatorConfig1, AuthenticatorConfig2]}}, ?AUTH:lookup_chain(ChainID)),
    ?assertEqual({ok, AuthenticatorConfig2}, ?AUTH:lookup_authenticator(ChainID, AuthenticatorName2)),
    ?assertEqual({ok, [AuthenticatorConfig1, AuthenticatorConfig2]}, ?AUTH:list_authenticators(ChainID)),

    ?assertEqual(ok, ?AUTH:move_authenticator_to_the_front(ChainID, AuthenticatorName2)),
    ?assertEqual({ok, [AuthenticatorConfig2, AuthenticatorConfig1]}, ?AUTH:list_authenticators(ChainID)),
    ?assertEqual(ok, ?AUTH:move_authenticator_to_the_end(ChainID, AuthenticatorName2)),
    ?assertEqual({ok, [AuthenticatorConfig1, AuthenticatorConfig2]}, ?AUTH:list_authenticators(ChainID)),
    ?assertEqual(ok, ?AUTH:move_authenticator_to_the_nth(ChainID, AuthenticatorName2, 1)),
    ?assertEqual({ok, [AuthenticatorConfig2, AuthenticatorConfig1]}, ?AUTH:list_authenticators(ChainID)),
    ?assertEqual({error, out_of_range}, ?AUTH:move_authenticator_to_the_nth(ChainID, AuthenticatorName2, 3)),
    ?assertEqual({error, out_of_range}, ?AUTH:move_authenticator_to_the_nth(ChainID, AuthenticatorName2, 0)),
    ?assertEqual(ok, ?AUTH:delete_authenticator(ChainID, AuthenticatorName1)),
    ?assertEqual(ok, ?AUTH:delete_authenticator(ChainID, AuthenticatorName2)),
    ?assertEqual({ok, []}, ?AUTH:list_authenticators(ChainID)),
    ?assertEqual(ok, ?AUTH:delete_chain(ChainID)),
    ok.
<<<<<<< HEAD
=======




>>>>>>> refactor(use hocon): rename to authn, support two types of chains and support bind listener to chain
