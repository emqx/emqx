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

-behaviour(hocon_schema).
-behaviour(emqx_authentication).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

% -include("emqx_authn.hrl").

-export([ fields/1 ]).

-export([ refs/0
        , create/1
        , update/2
        , authenticate/2
        , destroy/1
        ]).

-define(AUTHN, emqx_authentication).

%%------------------------------------------------------------------------------
%% Hocon Schema
%%------------------------------------------------------------------------------

fields(type1) ->
    [ {mechanism,               {enum, ['password-based']}}
    , {backend,                 {enum, ['built-in-database']}}
    ];

fields(type2) ->
    [ {mechanism,               {enum, ['password-based']}}
    , {backend,                 {enum, ['mysql']}}
    ].

%%------------------------------------------------------------------------------
%% Callbacks
%%------------------------------------------------------------------------------

refs() ->
    [ hoconsc:ref(?MODULE, type1)
    , hoconsc:ref(?MODULE, type2)
    ].

create(_Config) ->
    {ok, #{mark => 1}}.

update(_Config, _State) ->
    {ok, #{mark => 2}}.

authenticate(#{username := <<"good">>}, _State) ->
    {ok, #{superuser => true}};
authenticate(#{username := _}, _State) ->
    {error, bad_username_or_password}.

destroy(_State) ->
    ok.

all() ->
    emqx_ct:all(?MODULE).

init_per_suite(Config) ->
    application:set_env(ekka, strict_mode, true),
    emqx_ct_helpers:start_apps([]),
    Config.

end_per_suite(_) ->
    emqx_ct_helpers:stop_apps([]),
    ok.

t_chain(_) ->
    % CRUD of authentication chain
    ChainName = <<"test">>,
    ?assertMatch({ok, []}, ?AUTHN:list_chains()),
    ?assertMatch({ok, #{name := ChainName, authenticators := []}}, ?AUTHN:create_chain(ChainName)),
    ?assertEqual({error, {already_exists, {chain, ChainName}}}, ?AUTHN:create_chain(ChainName)),
    ?assertMatch({ok, #{name := ChainName, authenticators := []}}, ?AUTHN:lookup_chain(ChainName)),
    ?assertMatch({ok, [#{name := ChainName}]}, ?AUTHN:list_chains()),
    ?assertEqual(ok, ?AUTHN:delete_chain(ChainName)),
    ?assertMatch({error, {not_found, {chain, ChainName}}}, ?AUTHN:lookup_chain(ChainName)),
    ok.

t_authenticator(_) ->
    ChainName = <<"test">>,
    AuthenticatorConfig1 = #{mechanism => 'password-based',
                             backend => 'built-in-database'},

    % Create an authenticator when the authentication chain does not exist
    ?assertEqual({error, {not_found, {chain, ChainName}}}, ?AUTHN:create_authenticator(ChainName, AuthenticatorConfig1)),
    ?AUTHN:create_chain(ChainName),
    % Create an authenticator when the provider does not exist
    ?assertEqual({error, no_available_provider}, ?AUTHN:create_authenticator(ChainName, AuthenticatorConfig1)),

    AuthNType1 = {'password-based', 'built-in-database'},
    ?AUTHN:add_provider(AuthNType1, ?MODULE),
    ID1 = <<"password-based:built-in-database">>,

    % CRUD of authencaticator
    ?assertMatch({ok, #{id := ID1, state := #{mark := 1}}}, ?AUTHN:create_authenticator(ChainName, AuthenticatorConfig1)),
    ?assertMatch({ok, #{id := ID1}}, ?AUTHN:lookup_authenticator(ChainName, ID1)),
    ?assertMatch({ok, [#{id := ID1}]}, ?AUTHN:list_authenticators(ChainName)),
    ?assertEqual({error, {already_exists, {authenticator, ID1}}}, ?AUTHN:create_authenticator(ChainName, AuthenticatorConfig1)),
    ?assertMatch({ok, #{id := ID1, state := #{mark := 2}}}, ?AUTHN:update_authenticator(ChainName, ID1, AuthenticatorConfig1)),
    ?assertEqual(ok, ?AUTHN:delete_authenticator(ChainName, ID1)),
    ?assertEqual({error, {not_found, {authenticator, ID1}}}, ?AUTHN:update_authenticator(ChainName, ID1, AuthenticatorConfig1)),
    ?assertMatch({ok, []}, ?AUTHN:list_authenticators(ChainName)),

    % Multiple authenticators exist at the same time
    AuthNType2 = {'password-based', mysql},
    ?AUTHN:add_provider(AuthNType2, ?MODULE),
    ID2 = <<"password-based:mysql">>,
    AuthenticatorConfig2 = #{mechanism => 'password-based',
                             backend => mysql},
    ?assertMatch({ok, #{id := ID1}}, ?AUTHN:create_authenticator(ChainName, AuthenticatorConfig1)),
    ?assertMatch({ok, #{id := ID2}}, ?AUTHN:create_authenticator(ChainName, AuthenticatorConfig2)),

    % Move authenticator
    ?assertMatch({ok, [#{id := ID1}, #{id := ID2}]}, ?AUTHN:list_authenticators(ChainName)),
    ?assertEqual(ok, ?AUTHN:move_authenticator(ChainName, ID2, top)),
    ?assertMatch({ok, [#{id := ID2}, #{id := ID1}]}, ?AUTHN:list_authenticators(ChainName)),
    ?assertEqual(ok, ?AUTHN:move_authenticator(ChainName, ID2, bottom)),
    ?assertMatch({ok, [#{id := ID1}, #{id := ID2}]}, ?AUTHN:list_authenticators(ChainName)),
    ?assertEqual(ok, ?AUTHN:move_authenticator(ChainName, ID2, {before, ID1})),
    ?assertMatch({ok, [#{id := ID2}, #{id := ID1}]}, ?AUTHN:list_authenticators(ChainName)),

    ?AUTHN:delete_chain(ChainName),
    ?AUTHN:remove_provider(AuthNType1),
    ?AUTHN:remove_provider(AuthNType2),
    ok.

t_authenticate(_) ->
    ListenerID = <<"tcp:default">>,
    ClientInfo = #{zone => default,
                   listener => ListenerID,
                   protocol => mqtt,
                   username => <<"good">>,
			       password => <<"any">>},
    ?assertEqual({ok, #{superuser => false}}, emqx_access_control:authenticate(ClientInfo)),

    AuthNType = {'password-based', 'built-in-database'},
    ?AUTHN:add_provider(AuthNType, ?MODULE),

    AuthenticatorConfig = #{mechanism => 'password-based',
                            backend => 'built-in-database'},
    ?AUTHN:create_chain(ListenerID),
    ?assertMatch({ok, _}, ?AUTHN:create_authenticator(ListenerID, AuthenticatorConfig)),
    ?assertEqual({ok, #{superuser => true}}, emqx_access_control:authenticate(ClientInfo)),
    ?assertEqual({error, bad_username_or_password}, emqx_access_control:authenticate(ClientInfo#{username => <<"bad">>})),

    ?AUTHN:delete_chain(ListenerID),
    ?AUTHN:remove_provider(AuthNType),
    ok.

t_update_config(_) ->
    emqx_config_handler:add_handler([authentication], emqx_authentication),

    AuthNType1 = {'password-based', 'built-in-database'},
    AuthNType2 = {'password-based', mysql},
    ?AUTHN:add_provider(AuthNType1, ?MODULE),
    ?AUTHN:add_provider(AuthNType2, ?MODULE),

    ChainName = <<"mqtt:global">>,
    AuthenticatorConfig1 = #{mechanism => 'password-based',
                             backend => 'built-in-database'},
    AuthenticatorConfig2 = #{mechanism => 'password-based',
                             backend => mysql},
    ID1 = <<"password-based:built-in-database">>,
    ID2 = <<"password-based:mysql">>,
    
    ?assertMatch({ok, []}, ?AUTHN:list_chains()),
    ?assertMatch({ok, _}, update_config([authentication], {create_authenticator, ChainName, AuthenticatorConfig1})),
    ?assertMatch({ok, #{id := ID1, state := #{mark := 1}}}, ?AUTHN:lookup_authenticator(ChainName, ID1)),

    ?assertMatch({ok, _}, update_config([authentication], {create_authenticator, ChainName, AuthenticatorConfig2})),
    ?assertMatch({ok, #{id := ID2, state := #{mark := 1}}}, ?AUTHN:lookup_authenticator(ChainName, ID2)),

    ?assertMatch({ok, _}, update_config([authentication], {update_authenticator, ChainName, ID1, #{}})),
    ?assertMatch({ok, #{id := ID1, state := #{mark := 2}}}, ?AUTHN:lookup_authenticator(ChainName, ID1)),

    ?assertMatch({ok, _}, update_config([authentication], {move_authenticator, ChainName, ID2, <<"top">>})),
    ?assertMatch({ok, [#{id := ID2}, #{id := ID1}]}, ?AUTHN:list_authenticators(ChainName)),

    ?assertMatch({ok, _}, update_config([authentication], {delete_authenticator, ChainName, ID1})),
    ?assertEqual({error, {not_found, {authenticator, ID1}}}, ?AUTHN:lookup_authenticator(ChainName, ID1)),

    ?AUTHN:delete_chain(ChainName),
    ?AUTHN:remove_provider(AuthNType1),
    ?AUTHN:remove_provider(AuthNType2),
    ok.

update_config(Path, ConfigRequest) ->
    emqx:update_config(Path, ConfigRequest, #{rawconf_with_defaults => true}).
