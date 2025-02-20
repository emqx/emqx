%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_authn_chains_SUITE).

-behaviour(emqx_authn_provider).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("emqx/include/emqx_hooks.hrl").

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include("emqx_authn_chains.hrl").

-define(AUTHN, emqx_authn_chains).
-define(config(KEY),
    (fun() ->
        {KEY, _V_} = lists:keyfind(KEY, 1, Config),
        _V_
    end)()
).
-define(CONF_ROOT, ?EMQX_AUTHENTICATION_CONFIG_ROOT_NAME_ATOM).
-define(NOT_SUPERUSER, #{is_superuser => false}).

-define(assertAuthSuccessForUser(User),
    ?assertMatch(
        {ok, _},
        emqx_access_control:authenticate(ClientInfo#{username => atom_to_binary(User)})
    )
).
-define(assertAuthFailureForUser(User),
    ?assertMatch(
        {error, _},
        emqx_access_control:authenticate(ClientInfo#{username => atom_to_binary(User)})
    )
).

%%------------------------------------------------------------------------------
%% Callbacks
%%------------------------------------------------------------------------------

create(_AuthenticatorID, _Config) ->
    {ok, #{mark => 1}}.

update(_Config, _State) ->
    {ok, #{mark => 2}}.

authenticate(#{username := <<"good">>}, _State) ->
    {ok, #{is_superuser => true}};
authenticate(#{username := <<"ignore">>}, _State) ->
    ignore;
authenticate(#{username := <<"emqx_authn_ignore_for_hook_good">>}, _State) ->
    ignore;
authenticate(#{username := <<"emqx_authn_ignore_for_hook_bad">>}, _State) ->
    ignore;
authenticate(#{username := _}, _State) ->
    {error, bad_username_or_password}.

hook_authenticate(#{username := <<"hook_user_good">>}, _AuthResult) ->
    {ok, {ok, ?NOT_SUPERUSER}};
hook_authenticate(#{username := <<"hook_user_bad">>}, _AuthResult) ->
    {ok, {error, invalid_username}};
hook_authenticate(#{username := <<"hook_user_finally_good">>}, _AuthResult) ->
    {stop, {ok, ?NOT_SUPERUSER}};
hook_authenticate(#{username := <<"hook_user_finally_bad">>}, _AuthResult) ->
    {stop, {error, invalid_username}};
hook_authenticate(#{username := <<"emqx_authn_ignore_for_hook_good">>}, _AuthResult) ->
    {ok, {ok, ?NOT_SUPERUSER}};
hook_authenticate(#{username := <<"emqx_authn_ignore_for_hook_bad">>}, _AuthResult) ->
    {stop, {error, invalid_username}};
hook_authenticate(_ClientId, AuthResult) ->
    {ok, AuthResult}.

destroy(_State) ->
    ok.

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [
            emqx,
            emqx_conf,
            emqx_auth
        ],
        #{work_dir => ?config(priv_dir)}
    ),
    ok = emqx_access_control:set_default_authn_restrictive(),
    ok = deregister_providers(),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    ok = emqx_access_control:set_default_authn_permissive(),
    emqx_cth_suite:stop(?config(apps)),
    ok.

init_per_testcase(Case, Config) ->
    ?MODULE:Case({'init', Config}).

end_per_testcase(Case, Config) ->
    _ = ?MODULE:Case({'end', Config}),
    ok.

%%=================================================================================
%% Testcases
%%=================================================================================

t_chain({'init', Config}) ->
    Config;
t_chain(Config) when is_list(Config) ->
    % CRUD of authentication chain
    ChainName = 'test',
    ?assertMatch({ok, []}, ?AUTHN:list_chains()),
    ?assertMatch({ok, []}, ?AUTHN:list_chain_names()),

    %% to create a chain we need create an authenticator
    AuthenticatorConfig = #{
        mechanism => password_based,
        backend => built_in_database,
        enable => true
    },
    register_provider({password_based, built_in_database}, ?MODULE),
    ?AUTHN:create_authenticator(ChainName, AuthenticatorConfig),

    ?assertMatch({ok, #{name := ChainName, authenticators := [_]}}, ?AUTHN:lookup_chain(ChainName)),
    ?assertMatch({ok, [#{name := ChainName}]}, ?AUTHN:list_chains()),
    ?assertEqual({ok, [ChainName]}, ?AUTHN:list_chain_names()),
    ?assertEqual(ok, ?AUTHN:delete_chain(ChainName)),
    ?assertMatch({error, {not_found, {chain, ChainName}}}, ?AUTHN:lookup_chain(ChainName)),
    ok;
t_chain({'end', _Config}) ->
    ?AUTHN:delete_chain(test),
    ?AUTHN:deregister_providers([{password_based, built_in_database}]),
    ok.

t_authenticator({'init', Config}) ->
    [
        {"auth1", {password_based, built_in_database}},
        {"auth2", {password_based, mysql}}
        | Config
    ];
t_authenticator(Config) when is_list(Config) ->
    ChainName = 'test',
    AuthenticatorConfig1 = #{
        mechanism => password_based,
        backend => built_in_database,
        enable => true
    },

    % Create an authenticator when the provider does not exist

    ?assertEqual(
        {error, {no_available_provider_for, {password_based, built_in_database}}},
        ?AUTHN:create_authenticator(ChainName, AuthenticatorConfig1)
    ),

    AuthNType1 = ?config("auth1"),
    register_provider(AuthNType1, ?MODULE),
    ID1 = <<"password_based:built_in_database">>,

    % CRUD of authenticator
    ?assertMatch(
        {ok, #{id := ID1, state := #{mark := 1}}},
        ?AUTHN:create_authenticator(ChainName, AuthenticatorConfig1)
    ),

    ?assertMatch({ok, #{id := ID1}}, ?AUTHN:lookup_authenticator(ChainName, ID1)),
    ?assertMatch({ok, [#{id := ID1}]}, ?AUTHN:list_authenticators(ChainName)),

    ?assertEqual(
        {error, {already_exists, {authenticator, ID1}}},
        ?AUTHN:create_authenticator(ChainName, AuthenticatorConfig1)
    ),

    ?assertMatch(
        {ok, #{id := ID1, state := #{mark := 2}}},
        ?AUTHN:update_authenticator(ChainName, ID1, AuthenticatorConfig1)
    ),

    %% delete an unknown authenticator is allowed, do not epxect not_found
    ?assertEqual(ok, ?AUTHN:delete_authenticator(ChainName, <<"password_based:http">>)),
    %% the deletion of the last authenticator in the chain should result in
    %% an implict deletion of the chain
    ?assertEqual(ok, ?AUTHN:delete_authenticator(ChainName, ID1)),
    %% expected not_found for the chain
    ?assertEqual(
        {error, {not_found, {chain, test}}},
        ?AUTHN:update_authenticator(ChainName, ID1, AuthenticatorConfig1)
    ),

    ?assertEqual(
        {error, {not_found, {chain, test}}},
        ?AUTHN:update_authenticator(ChainName, ID1, AuthenticatorConfig1)
    ),

    ?assertMatch(
        {error, {not_found, {chain, ChainName}}},
        ?AUTHN:list_authenticators(ChainName)
    ),

    % Multiple authenticators exist at the same time
    AuthNType2 = ?config("auth2"),
    register_provider(AuthNType2, ?MODULE),
    ID2 = <<"password_based:mysql">>,
    AuthenticatorConfig2 = #{
        mechanism => 'password_based',
        backend => mysql,
        enable => true
    },

    ?assertMatch(
        {ok, #{id := ID1}},
        ?AUTHN:create_authenticator(ChainName, AuthenticatorConfig1)
    ),

    ?assertMatch(
        {ok, #{id := ID2}},
        ?AUTHN:create_authenticator(ChainName, AuthenticatorConfig2)
    ),

    % Move authenticator
    ?assertMatch({ok, [#{id := ID1}, #{id := ID2}]}, ?AUTHN:list_authenticators(ChainName)),

    ?assertEqual(ok, ?AUTHN:move_authenticator(ChainName, ID2, ?CMD_MOVE_FRONT)),
    ?assertMatch({ok, [#{id := ID2}, #{id := ID1}]}, ?AUTHN:list_authenticators(ChainName)),

    ?assertEqual(ok, ?AUTHN:move_authenticator(ChainName, ID2, ?CMD_MOVE_REAR)),
    ?assertMatch({ok, [#{id := ID1}, #{id := ID2}]}, ?AUTHN:list_authenticators(ChainName)),

    ?assertEqual(ok, ?AUTHN:move_authenticator(ChainName, ID2, ?CMD_MOVE_BEFORE(ID1))),
    ?assertMatch({ok, [#{id := ID2}, #{id := ID1}]}, ?AUTHN:list_authenticators(ChainName)),

    ?assertEqual(ok, ?AUTHN:move_authenticator(ChainName, ID2, ?CMD_MOVE_AFTER(ID1))),
    ?assertMatch({ok, [#{id := ID1}, #{id := ID2}]}, ?AUTHN:list_authenticators(ChainName));
t_authenticator({'end', Config}) ->
    ?AUTHN:delete_chain(test),
    ?AUTHN:deregister_providers([?config("auth1"), ?config("auth2")]),
    ok.

t_authenticate({init, Config}) ->
    [
        {listener_id, 'tcp:default'},
        {authn_type, {password_based, built_in_database}}
        | Config
    ];
t_authenticate(Config) when is_list(Config) ->
    ListenerID = ?config(listener_id),
    AuthNType = ?config(authn_type),
    ClientInfo = #{
        zone => default,
        listener => ListenerID,
        protocol => mqtt,
        username => <<"good">>,
        password => <<"any">>
    },
    ?assertEqual({ok, #{is_superuser => false}}, emqx_access_control:authenticate(ClientInfo)),

    register_provider(AuthNType, ?MODULE),

    AuthenticatorConfig = #{
        mechanism => password_based,
        backend => built_in_database,
        enable => true
    },
    ?assertMatch({ok, _}, ?AUTHN:create_authenticator(ListenerID, AuthenticatorConfig)),

    ?assertEqual(
        {ok, #{is_superuser => true}},
        emqx_access_control:authenticate(ClientInfo)
    ),

    ?assertEqual(
        {error, bad_username_or_password},
        emqx_access_control:authenticate(ClientInfo#{username => <<"bad">>})
    );
t_authenticate({'end', Config}) ->
    ?AUTHN:delete_chain(?config(listener_id)),
    ?AUTHN:deregister_provider(?config(authn_type)),
    ok.

t_update_config({init, Config}) ->
    Global = 'mqtt:global',
    AuthNType1 = {password_based, built_in_database},
    AuthNType2 = {password_based, mysql},
    [
        {global, Global},
        {"auth1", AuthNType1},
        {"auth2", AuthNType2}
        | Config
    ];
t_update_config(Config) when is_list(Config) ->
    emqx_config_handler:add_handler([?CONF_ROOT], emqx_authn_config),
    ok = emqx_config_handler:add_handler(
        [listeners, '?', '?', ?CONF_ROOT], emqx_authn_config
    ),
    ok = register_provider(?config("auth1"), ?MODULE),
    ok = register_provider(?config("auth2"), ?MODULE),
    Global = ?config(global),
    %% We mocked provider implementation, but did't mock the schema
    %% so we should provide full config
    AuthenticatorConfig1 = #{
        <<"mechanism">> => <<"password_based">>,
        <<"backend">> => <<"built_in_database">>,
        <<"enable">> => true
    },
    AuthenticatorConfig2 = #{
        <<"mechanism">> => <<"password_based">>,
        <<"backend">> => <<"mysql">>,
        <<"query">> => <<"SELECT password_hash, salt FROM users WHERE username = ?">>,
        <<"server">> => <<"127.0.0.1:5432">>,
        <<"database">> => <<"emqx">>,
        <<"enable">> => true
    },
    ID1 = <<"password_based:built_in_database">>,
    ID2 = <<"password_based:mysql">>,

    ?assertMatch({ok, []}, ?AUTHN:list_chains()),

    ?assertMatch(
        {ok, _},
        update_config([?CONF_ROOT], {create_authenticator, Global, AuthenticatorConfig1})
    ),

    ?assertMatch(
        {ok, #{id := ID1, state := #{mark := 1}}},
        ?AUTHN:lookup_authenticator(Global, ID1)
    ),

    ?assertMatch(
        {ok, _},
        update_config([?CONF_ROOT], {create_authenticator, Global, AuthenticatorConfig2})
    ),

    ?assertMatch(
        {ok, #{id := ID2, state := #{mark := 1}}},
        ?AUTHN:lookup_authenticator(Global, ID2)
    ),

    ?assertMatch(
        {ok, _},
        update_config(
            [?CONF_ROOT],
            {update_authenticator, Global, ID1, AuthenticatorConfig1#{<<"enable">> => false}}
        )
    ),

    ?assertMatch(
        {ok, #{id := ID1, state := #{mark := 2}}},
        ?AUTHN:lookup_authenticator(Global, ID1)
    ),

    ?assertMatch(
        {ok, _},
        update_config([?CONF_ROOT], {move_authenticator, Global, ID2, ?CMD_MOVE_FRONT})
    ),

    ?assertMatch({ok, [#{id := ID2}, #{id := ID1}]}, ?AUTHN:list_authenticators(Global)),

    [Raw2, Raw1] = emqx:get_raw_config([?CONF_ROOT]),
    ?assertMatch({ok, _}, update_config([?CONF_ROOT], [Raw1, Raw2])),
    ?assertMatch({ok, [#{id := ID1}, #{id := ID2}]}, ?AUTHN:list_authenticators(Global)),

    ?assertMatch({ok, _}, update_config([?CONF_ROOT], {delete_authenticator, Global, ID1})),
    ?assertEqual(
        {error, {not_found, {authenticator, ID1}}},
        ?AUTHN:lookup_authenticator(Global, ID1)
    ),

    ?assertMatch(
        {ok, _},
        update_config([?CONF_ROOT], {delete_authenticator, Global, ID2})
    ),

    ?assertEqual(
        {error, {not_found, {chain, Global}}},
        ?AUTHN:lookup_authenticator(Global, ID2)
    ),

    ListenerID = 'tcp:default',
    ConfKeyPath = [listeners, tcp, default, ?CONF_ROOT],

    ?assertMatch(
        {ok, _},
        update_config(
            ConfKeyPath,
            {create_authenticator, ListenerID, AuthenticatorConfig1}
        )
    ),

    ?assertMatch(
        {ok, #{id := ID1, state := #{mark := 1}}},
        ?AUTHN:lookup_authenticator(ListenerID, ID1)
    ),

    ?assertMatch(
        {ok, _},
        update_config(
            ConfKeyPath,
            {create_authenticator, ListenerID, AuthenticatorConfig2}
        )
    ),

    ?assertMatch(
        {ok, #{id := ID2, state := #{mark := 1}}},
        ?AUTHN:lookup_authenticator(ListenerID, ID2)
    ),

    ?assertMatch(
        {ok, _},
        update_config(
            ConfKeyPath,
            {update_authenticator, ListenerID, ID1, AuthenticatorConfig1#{<<"enable">> => false}}
        )
    ),

    ?assertMatch(
        {ok, #{id := ID1, state := #{mark := 2}}},
        ?AUTHN:lookup_authenticator(ListenerID, ID1)
    ),

    ?assertMatch(
        {ok, _},
        update_config(ConfKeyPath, {move_authenticator, ListenerID, ID2, ?CMD_MOVE_FRONT})
    ),
    ?assertMatch(
        {ok, [#{id := ID2}, #{id := ID1}]},
        ?AUTHN:list_authenticators(ListenerID)
    ),
    [LRaw2, LRaw1] = emqx:get_raw_config(ConfKeyPath),
    ?assertMatch({ok, _}, update_config(ConfKeyPath, [LRaw1, LRaw2])),
    ?assertMatch(
        {ok, [#{id := ID1}, #{id := ID2}]},
        ?AUTHN:list_authenticators(ListenerID)
    ),

    ?assertMatch(
        {ok, _},
        update_config(ConfKeyPath, {delete_authenticator, ListenerID, ID1})
    ),

    ?assertEqual(
        {error, {not_found, {authenticator, ID1}}},
        ?AUTHN:lookup_authenticator(ListenerID, ID1)
    );
t_update_config({'end', Config}) ->
    ?AUTHN:delete_chain(?config(global)),
    ?AUTHN:deregister_providers([?config("auth1"), ?config("auth2")]),
    ok.

t_restart({'init', Config}) ->
    Config;
t_restart(Config) when is_list(Config) ->
    ?assertEqual({ok, []}, ?AUTHN:list_chain_names()),

    %% to create a chain we need create an authenticator
    AuthenticatorConfig = #{
        mechanism => password_based,
        backend => built_in_database,
        enable => true
    },
    register_provider({password_based, built_in_database}, ?MODULE),
    ?AUTHN:create_authenticator(test_chain, AuthenticatorConfig),

    ?assertEqual({ok, [test_chain]}, ?AUTHN:list_chain_names()),

    ok = supervisor:terminate_child(emqx_authn_sup, ?AUTHN),
    {ok, _} = supervisor:restart_child(emqx_authn_sup, ?AUTHN),

    ?assertEqual({ok, [test_chain]}, ?AUTHN:list_chain_names());
t_restart({'end', _Config}) ->
    ?AUTHN:delete_chain(test_chain),
    ?AUTHN:deregister_providers([{password_based, built_in_database}]),
    ok.

t_combine_authn_and_callback({init, Config}) ->
    [
        {listener_id, 'tcp:default'},
        {authn_type, {password_based, built_in_database}}
        | Config
    ];
t_combine_authn_and_callback(Config) when is_list(Config) ->
    ListenerID = ?config(listener_id),
    ClientInfo = #{
        zone => default,
        listener => ListenerID,
        protocol => mqtt,
        password => <<"any">>
    },

    %% no emqx_authn_chains authenticators, anonymous is allowed
    ?assertAuthSuccessForUser(bad),

    AuthNType = ?config(authn_type),
    register_provider(AuthNType, ?MODULE),

    AuthenticatorConfig = #{
        mechanism => password_based,
        backend => built_in_database,
        enable => true
    },
    {ok, _} = ?AUTHN:create_authenticator(ListenerID, AuthenticatorConfig),

    %% emqx_authn_chains alone
    ?assertAuthSuccessForUser(good),
    ?assertAuthFailureForUser(ignore),
    ?assertAuthFailureForUser(bad),

    %% add hook with higher priority
    ok = hook(?HP_AUTHN + 1),

    %% for hook unrelataed users everything is the same
    ?assertAuthSuccessForUser(good),
    ?assertAuthFailureForUser(ignore),
    ?assertAuthFailureForUser(bad),

    %% higher-priority hook can permit access with {ok,...},
    %% then emqx_authn_chains overrides the result
    ?assertAuthFailureForUser(hook_user_good),
    ?assertAuthFailureForUser(hook_user_bad),

    %% higher-priority hook can permit and return {stop,...},
    %% then emqx_authn_chains cannot override the result
    ?assertAuthSuccessForUser(hook_user_finally_good),
    ?assertAuthFailureForUser(hook_user_finally_bad),

    ok = unhook(),

    %% add hook with lower priority
    ok = hook(?HP_AUTHN - 1),

    %% for hook unrelataed users
    ?assertAuthSuccessForUser(good),
    ?assertAuthFailureForUser(bad),
    ?assertAuthFailureForUser(ignore),

    %% lower-priority hook can overrride emqx_authn_chains result
    %% for ignored users
    ?assertAuthSuccessForUser(emqx_authn_ignore_for_hook_good),
    ?assertAuthFailureForUser(emqx_authn_ignore_for_hook_bad),

    %% lower-priority hook cannot overrride
    %% successful/unsuccessful emqx_authn_chains result
    ?assertAuthFailureForUser(hook_user_finally_good),
    ?assertAuthFailureForUser(hook_user_finally_bad),
    ?assertAuthFailureForUser(hook_user_good),
    ?assertAuthFailureForUser(hook_user_bad),

    ok = unhook();
t_combine_authn_and_callback({'end', Config}) ->
    ?AUTHN:delete_chain(?config(listener_id)),
    ?AUTHN:deregister_provider(?config(authn_type)),
    ok.

%%=================================================================================
%% Helpers fns
%%=================================================================================

hook(Priority) ->
    ok = emqx_hooks:put(
        'client.authenticate', {?MODULE, hook_authenticate, []}, Priority
    ).

unhook() ->
    ok = emqx_hooks:del('client.authenticate', {?MODULE, hook_authenticate}).

update_config(Path, ConfigRequest) ->
    emqx:update_config(Path, ConfigRequest, #{rawconf_with_defaults => true}).

certs(Certs) ->
    CertsPath = emqx_common_test_helpers:deps_path(emqx, "etc/certs"),
    lists:foldl(
        fun({Key, Filename}, Acc) ->
            {ok, Bin} = file:read_file(filename:join([CertsPath, Filename])),
            Acc#{Key => Bin}
        end,
        #{},
        Certs
    ).

register_provider(Type, Module) ->
    ok = ?AUTHN:register_providers([{Type, Module}]).

deregister_providers() ->
    lists:foreach(
        fun({Type, _Module}) ->
            ok = ?AUTHN:deregister_provider(Type)
        end,
        maps:to_list(?AUTHN:get_providers())
    ).
