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

-module(emqx_authn_mongodb_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include("../../emqx_connector/include/emqx_connector.hrl").
-include_lib("emqx_auth/include/emqx_authn.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-define(MONGO_HOST, "mongo").
-define(MONGO_CLIENT, 'emqx_authn_mongo_SUITE_client').

-define(PATH, [authentication]).

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_testcase(_TestCase, Config) ->
    emqx_authn_test_lib:delete_authenticators(
        [authentication],
        ?GLOBAL
    ),
    {ok, _} = mc_worker_api:connect(mongo_config()),
    ok = init_seeds(),
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok = emqx_authn_test_lib:enable_node_cache(false),
    ok = drop_seeds(),
    ok = mc_worker_api:disconnect(?MONGO_CLIENT).

init_per_suite(Config) ->
    case emqx_common_test_helpers:is_tcp_server_available(?MONGO_HOST, ?MONGO_DEFAULT_PORT) of
        true ->
            Apps = emqx_cth_suite:start([emqx, emqx_conf, emqx_auth, emqx_auth_mongodb], #{
                work_dir => ?config(priv_dir, Config)
            }),
            [{apps, Apps} | Config];
        false ->
            {skip, no_mongo}
    end.

end_per_suite(Config) ->
    emqx_authn_test_lib:delete_authenticators(
        [authentication],
        ?GLOBAL
    ),
    ok = emqx_cth_suite:stop(?config(apps, Config)),
    ok.

%%------------------------------------------------------------------------------
%% Tests
%%------------------------------------------------------------------------------

t_create(_Config) ->
    AuthConfig = raw_mongo_auth_config(),

    {ok, _} = emqx:update_config(
        ?PATH,
        {create_authenticator, ?GLOBAL, AuthConfig}
    ),

    {ok, [#{provider := emqx_authn_mongodb}]} = emqx_authn_chains:list_authenticators(?GLOBAL).

t_create_invalid(_Config) ->
    AuthConfig = raw_mongo_auth_config(),

    InvalidConfigs =
        [
            AuthConfig#{<<"mongo_type">> => <<"unknown">>},
            AuthConfig#{<<"filter">> => <<"{ \"username\": \"${username}\" }">>},
            AuthConfig#{<<"w_mode">> => <<"unknown">>}
        ],

    lists:foreach(
        fun(Config) ->
            {error, _} = emqx:update_config(
                ?PATH,
                {create_authenticator, ?GLOBAL, Config}
            ),

            ?assertEqual(
                {error, {not_found, {chain, ?GLOBAL}}},
                emqx_authn_chains:list_authenticators(?GLOBAL)
            )
        end,
        InvalidConfigs
    ).

t_authenticate(_Config) ->
    ok = lists:foreach(
        fun(Sample) ->
            ct:pal("test_user_auth sample: ~p", [Sample]),
            test_user_auth(Sample)
        end,
        user_seeds()
    ).

test_user_auth(#{
    credentials := Credentials0,
    config_params := SpecificConfigParams,
    result := Result
}) ->
    AuthConfig = maps:merge(raw_mongo_auth_config(), SpecificConfigParams),

    {ok, _} = emqx:update_config(
        ?PATH,
        {create_authenticator, ?GLOBAL, AuthConfig}
    ),

    Credentials = Credentials0#{
        listener => 'tcp:default',
        protocol => mqtt
    },
    ?assertEqual(Result, emqx_access_control:authenticate(Credentials)),

    emqx_authn_test_lib:delete_authenticators(
        [authentication],
        ?GLOBAL
    ).

t_destroy(_Config) ->
    ok = init_seeds(),
    AuthConfig = raw_mongo_auth_config(),

    {ok, _} = emqx:update_config(
        ?PATH,
        {create_authenticator, ?GLOBAL, AuthConfig}
    ),

    {ok, [#{provider := emqx_authn_mongodb, state := State}]} =
        emqx_authn_chains:list_authenticators(?GLOBAL),

    {ok, _} = emqx_authn_mongodb:authenticate(
        #{
            username => <<"plain">>,
            password => <<"plain">>
        },
        State
    ),

    emqx_authn_test_lib:delete_authenticators(
        [authentication],
        ?GLOBAL
    ),

    % Authenticator should not be usable anymore
    ?assertMatch(
        ignore,
        emqx_authn_mongodb:authenticate(
            #{
                username => <<"plain">>,
                password => <<"plain">>
            },
            State
        )
    ),

    ok = drop_seeds().

t_update(_Config) ->
    ok = init_seeds(),
    CorrectConfig = raw_mongo_auth_config(),
    IncorrectConfig =
        CorrectConfig#{<<"filter">> => #{<<"wrongfield">> => <<"wrongvalue">>}},

    {ok, _} = emqx:update_config(
        ?PATH,
        {create_authenticator, ?GLOBAL, IncorrectConfig}
    ),

    {error, not_authorized} = emqx_access_control:authenticate(
        #{
            username => <<"plain">>,
            password => <<"plain">>,
            listener => 'tcp:default',
            protocol => mqtt
        }
    ),

    % We update with config with correct filter, provider should update and work properly
    {ok, _} = emqx:update_config(
        ?PATH,
        {update_authenticator, ?GLOBAL, <<"password_based:mongodb">>, CorrectConfig}
    ),

    {ok, _} = emqx_access_control:authenticate(
        #{
            username => <<"plain">>,
            password => <<"plain">>,
            listener => 'tcp:default',
            protocol => mqtt
        }
    ),
    ok = drop_seeds().

t_is_superuser(_Config) ->
    Config = raw_mongo_auth_config(),
    {ok, _} = emqx:update_config(
        ?PATH,
        {create_authenticator, ?GLOBAL, Config}
    ),

    Checks = [
        {<<"0">>, false},
        {<<"">>, false},
        {null, false},
        {false, false},
        {0, false},
        {<<"val">>, false},

        {<<"1">>, true},
        {<<"123">>, true},
        {1, true},
        {123, true},
        {true, true}
    ],

    lists:foreach(fun test_is_superuser/1, Checks).

test_is_superuser({Value, ExpectedValue}) ->
    {true, _} = mc_worker_api:delete(?MONGO_CLIENT, <<"users">>, #{}),

    UserData = #{
        username => <<"user">>,
        password_hash => <<"plainsalt">>,
        salt => <<"salt">>,
        is_superuser => Value
    },

    ok = create_user(UserData),

    Credentials = #{
        listener => 'tcp:default',
        protocol => mqtt,
        username => <<"user">>,
        password => <<"plain">>
    },

    ?assertEqual(
        {ok, #{is_superuser => ExpectedValue}},
        emqx_access_control:authenticate(Credentials)
    ).

t_node_cache(_Config) ->
    ok = create_user(#{
        username => <<"node_cache_user">>, password_hash => <<"password">>, salt => <<"">>
    }),
    Config = raw_mongo_auth_config(),
    {ok, _} = emqx:update_config(
        ?PATH,
        {create_authenticator, ?GLOBAL, Config}
    ),
    ok = emqx_authn_test_lib:enable_node_cache(true),
    Credentials = #{
        listener => 'tcp:default',
        protocol => mqtt,
        username => <<"node_cache_user">>,
        password => <<"password">>
    },

    %% First time should be a miss, second time should be a hit
    ?assertMatch(
        {ok, #{is_superuser := false}},
        emqx_access_control:authenticate(Credentials)
    ),
    ?assertMatch(
        {ok, #{is_superuser := false}},
        emqx_access_control:authenticate(Credentials)
    ),
    ?assertMatch(
        #{hits := #{value := 1}, misses := #{value := 1}},
        emqx_auth_cache:metrics(?AUTHN_CACHE)
    ),

    %% Change a variable in the query, should be a miss
    _ = emqx_access_control:authenticate(Credentials#{username => <<"user2">>}),
    ?assertMatch(
        #{hits := #{value := 1}, misses := #{value := 2}},
        emqx_auth_cache:metrics(?AUTHN_CACHE)
    ).

%%------------------------------------------------------------------------------
%% Helpers
%%------------------------------------------------------------------------------

raw_mongo_auth_config() ->
    #{
        <<"mechanism">> => <<"password_based">>,
        <<"password_hash_algorithm">> => #{
            <<"name">> => <<"plain">>,
            <<"salt_position">> => <<"suffix">>
        },
        <<"enable">> => <<"true">>,

        <<"backend">> => <<"mongodb">>,
        <<"mongo_type">> => <<"single">>,
        <<"database">> => <<"mqtt">>,
        <<"collection">> => <<"users">>,
        <<"server">> => mongo_server(),
        <<"w_mode">> => <<"unsafe">>,

        <<"auth_source">> => mongo_authsource(),
        <<"username">> => mongo_username(),
        <<"password">> => mongo_password(),

        <<"filter">> => #{<<"username">> => <<"${username}">>},
        <<"password_hash_field">> => <<"password_hash">>,
        <<"salt_field">> => <<"salt">>,
        <<"is_superuser_field">> => <<"is_superuser">>,
        <<"use_legacy_protocol">> => <<"auto">>
    }.

user_seeds() ->
    PlainSeed =
        #{
            data => #{
                username => <<"plain">>,
                password_hash => <<"plainsalt">>,
                salt => <<"salt">>,
                is_superuser => <<"1">>
            },
            credentials => #{
                username => <<"plain">>,
                password => <<"plain">>
            },
            config_params => #{},
            result => {ok, #{is_superuser => true}}
        },
    [
        PlainSeed#{config_params => #{<<"use_legacy_protocol">> => <<"auto">>}},
        PlainSeed#{config_params => #{<<"use_legacy_protocol">> => <<"true">>}},
        PlainSeed#{config_params => #{<<"use_legacy_protocol">> => <<"false">>}},
        #{
            data => #{
                username => <<"md5">>,
                password_hash => <<"9b4d0c43d206d48279e69b9ad7132e22">>,
                salt => <<"salt">>,
                is_superuser => <<"0">>
            },
            credentials => #{
                username => <<"md5">>,
                password => <<"md5">>
            },
            config_params => #{
                <<"password_hash_algorithm">> => #{
                    <<"name">> => <<"md5">>,
                    <<"salt_position">> => <<"suffix">>
                }
            },
            result => {ok, #{is_superuser => false}}
        },

        #{
            data => #{
                username => <<"sha256">>,
                password_hash =>
                    <<"ac63a624e7074776d677dd61a003b8c803eb11db004d0ec6ae032a5d7c9c5caf">>,
                salt => <<"salt">>,
                is_superuser => 1
            },
            credentials => #{
                clientid => <<"sha256">>,
                password => <<"sha256">>
            },
            config_params => #{
                <<"filter">> => #{<<"username">> => <<"${clientid}">>},
                <<"password_hash_algorithm">> => #{
                    <<"name">> => <<"sha256">>,
                    <<"salt_position">> => <<"prefix">>
                }
            },
            result => {ok, #{is_superuser => true}}
        },

        #{
            data => #{
                cert_subject => <<"cert_subject_data">>,
                cert_common_name => <<"cert_common_name_data">>,
                password_hash =>
                    <<"ac63a624e7074776d677dd61a003b8c803eb11db004d0ec6ae032a5d7c9c5caf">>,
                salt => <<"salt">>,
                is_superuser => 1
            },
            credentials => #{
                cert_subject => <<"cert_subject_data">>,
                cert_common_name => <<"cert_common_name_data">>,
                password => <<"sha256">>
            },
            config_params => #{
                <<"filter">> => #{
                    <<"cert_subject">> => <<"${cert_subject}">>,
                    <<"cert_common_name">> => <<"${cert_common_name}">>
                },
                <<"password_hash_algorithm">> => #{
                    <<"name">> => <<"sha256">>,
                    <<"salt_position">> => <<"prefix">>
                }
            },
            result => {ok, #{is_superuser => true}}
        },

        #{
            data => #{
                username => <<"bcrypt">>,
                password_hash =>
                    <<"$2b$12$wtY3h20mUjjmeaClpqZVveDWGlHzCGsvuThMlneGHA7wVeFYyns2u">>,
                salt => <<"$2b$12$wtY3h20mUjjmeaClpqZVve">>,
                is_superuser => 0
            },
            credentials => #{
                username => <<"bcrypt">>,
                password => <<"bcrypt">>
            },
            config_params => #{
                <<"password_hash_algorithm">> => #{<<"name">> => <<"bcrypt">>}
            },
            result => {ok, #{is_superuser => false}}
        },

        #{
            data => #{
                username => <<"bcrypt0">>,
                password_hash =>
                    <<"$2b$12$wtY3h20mUjjmeaClpqZVveDWGlHzCGsvuThMlneGHA7wVeFYyns2u">>,
                salt => <<"$2b$12$wtY3h20mUjjmeaClpqZVve">>,
                is_superuser => <<"0">>
            },
            credentials => #{
                username => <<"bcrypt0">>,
                password => <<"bcrypt">>
            },
            config_params => #{
                % clientid variable & username credentials
                <<"filter">> => #{<<"username">> => <<"${clientid}">>},
                <<"password_hash_algorithm">> => #{<<"name">> => <<"bcrypt">>}
            },
            result => {error, not_authorized}
        },

        #{
            data => #{
                username => <<"bcrypt1">>,
                password_hash =>
                    <<"$2b$12$wtY3h20mUjjmeaClpqZVveDWGlHzCGsvuThMlneGHA7wVeFYyns2u">>,
                salt => <<"$2b$12$wtY3h20mUjjmeaClpqZVve">>,
                is_superuser => <<"0">>
            },
            credentials => #{
                username => <<"bcrypt1">>,
                password => <<"bcrypt">>
            },
            config_params => #{
                <<"filter">> => #{<<"userid">> => <<"${clientid}">>},
                <<"password_hash_algorithm">> => #{<<"name">> => <<"bcrypt">>}
            },
            result => {error, not_authorized}
        },

        #{
            data => #{
                username => <<"bcrypt2">>,
                password_hash =>
                    <<"$2b$12$wtY3h20mUjjmeaClpqZVveDWGlHzCGsvuThMlneGHA7wVeFYyns2u">>,
                salt => <<"$2b$12$wtY3h20mUjjmeaClpqZVve">>,
                is_superuser => <<"0">>
            },
            credentials => #{
                username => <<"bcrypt2">>,
                % Wrong password
                password => <<"wrongpass">>
            },
            config_params => #{
                <<"password_hash_algorithm">> => #{<<"name">> => <<"bcrypt">>}
            },
            result => {error, bad_username_or_password}
        }
    ].

init_seeds() ->
    Users = [Values || #{data := Values} <- user_seeds()],
    ok = lists:foreach(fun create_user/1, Users),
    ok.

create_user(User) ->
    {{true, _}, _} = mc_worker_api:insert(?MONGO_CLIENT, <<"users">>, [User]),
    ok.

drop_seeds() ->
    {true, _} = mc_worker_api:delete(?MONGO_CLIENT, <<"users">>, #{}),
    ok.

mongo_server() ->
    iolist_to_binary(io_lib:format("~s", [?MONGO_HOST])).

mongo_config() ->
    [
        {database, <<"mqtt">>},
        {host, ?MONGO_HOST},
        {port, ?MONGO_DEFAULT_PORT},
        {auth_source, mongo_authsource()},
        {login, mongo_username()},
        {password, mongo_password()},
        {register, ?MONGO_CLIENT}
    ].

mongo_authsource() ->
    iolist_to_binary(os:getenv("MONGO_AUTHSOURCE", "admin")).

mongo_username() ->
    iolist_to_binary(os:getenv("MONGO_USERNAME", "")).

mongo_password() ->
    iolist_to_binary(os:getenv("MONGO_PASSWORD", "")).

start_apps(Apps) ->
    lists:foreach(fun application:ensure_all_started/1, Apps).

stop_apps(Apps) ->
    lists:foreach(fun application:stop/1, Apps).
