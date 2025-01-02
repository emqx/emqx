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

-module(emqx_authn_postgresql_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("../../emqx_postgresql/include/emqx_postgresql.hrl").
-include_lib("emqx_auth/include/emqx_authn.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-define(PGSQL_HOST, "pgsql").
-define(PGSQL_RESOURCE, <<"emqx_authn_postgresql_SUITE">>).
-define(ResourceID, <<"password_based:postgresql">>).

-define(PATH, [authentication]).

-import(emqx_common_test_helpers, [on_exit/1]).

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_testcase(_, Config) ->
    emqx_authn_test_lib:delete_authenticators(
        [authentication],
        ?GLOBAL
    ),
    ok = init_seeds(),
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok = drop_seeds(),
    _ = emqx_auth_cache:reset(?AUTHN_CACHE),
    ok = emqx_authn_test_lib:enable_node_cache(false),
    emqx_authn_test_lib:delete_authenticators(
        [authentication],
        ?GLOBAL
    ),
    emqx_common_test_helpers:call_janitor(),
    ok.

init_per_suite(Config) ->
    case emqx_common_test_helpers:is_tcp_server_available(?PGSQL_HOST, ?PGSQL_DEFAULT_PORT) of
        true ->
            Apps = emqx_cth_suite:start([emqx, emqx_conf, emqx_auth, emqx_auth_postgresql], #{
                work_dir => ?config(priv_dir, Config)
            }),
            {ok, _} = emqx_resource:create_local(
                ?PGSQL_RESOURCE,
                ?AUTHN_RESOURCE_GROUP,
                emqx_postgresql,
                pgsql_config(),
                #{}
            ),
            [{apps, Apps} | Config];
        false ->
            case os:getenv("IS_CI") of
                "yes" ->
                    throw(no_postgres);
                _ ->
                    {skip, no_postgres}
            end
    end.

end_per_suite(Config) ->
    ok = emqx_resource:remove_local(?PGSQL_RESOURCE),
    ok = emqx_cth_suite:stop(?config(apps, Config)),
    ok.

%%------------------------------------------------------------------------------
%% Tests
%%------------------------------------------------------------------------------

t_create(_Config) ->
    AuthConfig = raw_pgsql_auth_config(),

    {ok, _} = emqx:update_config(
        ?PATH,
        {create_authenticator, ?GLOBAL, AuthConfig}
    ),

    {ok, [#{provider := emqx_authn_postgresql}]} = emqx_authn_chains:list_authenticators(?GLOBAL),
    emqx_authn_test_lib:delete_config(?ResourceID).

%% invalid config which does not pass the schema check should result in an error
t_update_with_invalid_config(_Config) ->
    AuthConfig = raw_pgsql_auth_config(),
    BadConfig = maps:without([<<"server">>], AuthConfig),
    ?assertMatch(
        {error, #{
            kind := validation_error,
            matched_type := "authn:postgresql",
            path := "authentication.1.server",
            reason := required_field
        }},
        emqx:update_config(
            ?PATH,
            {create_authenticator, ?GLOBAL, BadConfig}
        )
    ),
    ok.

%% bad config values may cause connection failure, but should still be able to update
t_update_with_bad_config_value(_Config) ->
    AuthConfig = raw_pgsql_auth_config(),

    InvalidConfigs =
        [
            AuthConfig#{<<"server">> => <<"unknownhost:3333">>},
            AuthConfig#{<<"password">> => <<"wrongpass">>},
            AuthConfig#{<<"database">> => <<"wrongdatabase">>}
        ],

    lists:foreach(
        fun(Config) ->
            {ok, _} = emqx:update_config(
                ?PATH,
                {create_authenticator, ?GLOBAL, Config}
            ),
            emqx_authn_test_lib:delete_config(?ResourceID),
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
    AuthConfig = maps:merge(raw_pgsql_auth_config(), SpecificConfigParams),

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

t_authenticate_disabled_prepared_statements(_Config) ->
    ResConfig = maps:merge(pgsql_config(), #{disable_prepared_statements => true}),
    {ok, _} = emqx_resource:recreate_local(?PGSQL_RESOURCE, emqx_postgresql, ResConfig, #{}),
    on_exit(fun() ->
        emqx_resource:recreate_local(?PGSQL_RESOURCE, emqx_postgresql, pgsql_config(), #{})
    end),
    ok = lists:foreach(
        fun(Sample0) ->
            Sample = maps:update_with(
                config_params,
                fun(Cfg) -> Cfg#{<<"disable_prepared_statements">> => true} end,
                Sample0
            ),
            ct:pal("test_user_auth sample: ~p", [Sample]),
            test_user_auth(Sample)
        end,
        user_seeds()
    ).

t_destroy(_Config) ->
    AuthConfig = raw_pgsql_auth_config(),

    {ok, _} = emqx:update_config(
        ?PATH,
        {create_authenticator, ?GLOBAL, AuthConfig}
    ),

    {ok, [#{provider := emqx_authn_postgresql, state := State}]} =
        emqx_authn_chains:list_authenticators(?GLOBAL),

    {ok, _} = emqx_authn_postgresql:authenticate(
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
        emqx_authn_postgresql:authenticate(
            #{
                username => <<"plain">>,
                password => <<"plain">>
            },
            State
        )
    ).

t_update(_Config) ->
    CorrectConfig = raw_pgsql_auth_config(),
    IncorrectConfig =
        CorrectConfig#{
            <<"query">> =>
                <<
                    "SELECT password_hash, salt, is_superuser_str as is_superuser\n"
                    "                          FROM users where username = ${username} LIMIT 0"
                >>
        },

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

    % We update with config with correct query, provider should update and work properly
    {ok, _} = emqx:update_config(
        ?PATH,
        {update_authenticator, ?GLOBAL, <<"password_based:postgresql">>, CorrectConfig}
    ),

    {ok, _} = emqx_access_control:authenticate(
        #{
            username => <<"plain">>,
            password => <<"plain">>,
            listener => 'tcp:default',
            protocol => mqtt
        }
    ).

t_is_superuser(_Config) ->
    Config = raw_pgsql_auth_config(),
    {ok, _} = emqx:update_config(
        ?PATH,
        {create_authenticator, ?GLOBAL, Config}
    ),

    Checks = [
        {is_superuser_str, "0", false},
        {is_superuser_str, "", false},
        {is_superuser_str, null, false},
        {is_superuser_str, "1", true},
        {is_superuser_str, "val", false},

        {is_superuser_int, 0, false},
        {is_superuser_int, null, false},
        {is_superuser_int, 1, true},
        {is_superuser_int, 123, true},

        {is_superuser_bool, false, false},
        {is_superuser_bool, null, false},
        {is_superuser_bool, true, true}
    ],

    lists:foreach(fun test_is_superuser/1, Checks).

test_is_superuser({Field, Value, ExpectedValue}) ->
    {ok, _} = q("DELETE FROM users"),

    UserData = #{
        username => "user",
        password_hash => "plainsalt",
        salt => "salt",
        Field => Value
    },

    ok = create_user(UserData),

    Query =
        "SELECT password_hash, salt, " ++ atom_to_list(Field) ++
            " as is_superuser "
            "FROM users where username = ${username} LIMIT 1",

    Config = maps:put(<<"query">>, Query, raw_pgsql_auth_config()),
    {ok, _} = emqx:update_config(
        ?PATH,
        {update_authenticator, ?GLOBAL, <<"password_based:postgresql">>, Config}
    ),

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
    Config = maps:merge(
        raw_pgsql_auth_config(),
        #{
            <<"query">> =>
                <<"SELECT password_hash, salt FROM users where username = ${username} LIMIT 1">>
        }
    ),
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

raw_pgsql_auth_config() ->
    #{
        <<"mechanism">> => <<"password_based">>,
        <<"password_hash_algorithm">> => #{
            <<"name">> => <<"plain">>,
            <<"salt_position">> => <<"suffix">>
        },
        <<"enable">> => <<"true">>,

        <<"backend">> => <<"postgresql">>,
        <<"database">> => <<"mqtt">>,
        <<"username">> => <<"root">>,
        <<"password">> => <<"public">>,

        <<"query">> =>
            <<
                "SELECT password_hash, salt, is_superuser_str as is_superuser\n"
                "                      FROM users where username = ${username} LIMIT 1"
            >>,
        <<"server">> => pgsql_server()
    }.

user_seeds() ->
    [
        #{
            data => #{
                username => "plain",
                password_hash => "plainsalt",
                salt => "salt",
                is_superuser_str => "1"
            },
            credentials => #{
                username => <<"plain">>,
                password => <<"plain">>
            },
            config_params => #{},
            result => {ok, #{is_superuser => true}}
        },

        #{
            data => #{
                username => "md5",
                password_hash => "9b4d0c43d206d48279e69b9ad7132e22",
                salt => "salt",
                is_superuser_str => "0"
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
                username => "sha256",
                password_hash => "ac63a624e7074776d677dd61a003b8c803eb11db004d0ec6ae032a5d7c9c5caf",
                salt => "salt",
                is_superuser_int => 1
            },
            credentials => #{
                clientid => <<"sha256">>,
                password => <<"sha256">>
            },
            config_params => #{
                <<"query">> =>
                    <<
                        "SELECT password_hash, salt, is_superuser_int as is_superuser\n"
                        "                            FROM users where username = ${clientid} LIMIT 1"
                    >>,
                <<"password_hash_algorithm">> => #{
                    <<"name">> => <<"sha256">>,
                    <<"salt_position">> => <<"prefix">>
                }
            },
            result => {ok, #{is_superuser => true}}
        },

        %% strip double quote support
        #{
            data => #{
                username => "sha256",
                password_hash => "ac63a624e7074776d677dd61a003b8c803eb11db004d0ec6ae032a5d7c9c5caf",
                salt => "salt",
                is_superuser_int => 1
            },
            credentials => #{
                username => <<"sha256">>,
                password => <<"sha256">>
            },
            config_params => #{
                <<"query">> =>
                    <<
                        "SELECT password_hash, salt, is_superuser_int as is_superuser\n"
                        "                            FROM users where username = \"${username}\" LIMIT 1"
                    >>,
                <<"password_hash_algorithm">> => #{
                    <<"name">> => <<"sha256">>,
                    <<"salt_position">> => <<"prefix">>
                }
            },
            result => {ok, #{is_superuser => true}}
        },

        #{
            data => #{
                username => "sha256",
                password_hash => "ac63a624e7074776d677dd61a003b8c803eb11db004d0ec6ae032a5d7c9c5caf",
                cert_subject => <<"cert_subject_data">>,
                cert_common_name => <<"cert_common_name_data">>,
                salt => "salt",
                is_superuser_int => 1
            },
            credentials => #{
                clientid => <<"sha256">>,
                password => <<"sha256">>,
                cert_subject => <<"cert_subject_data">>,
                cert_common_name => <<"cert_common_name_data">>
            },
            config_params => #{
                <<"query">> =>
                    <<
                        "SELECT password_hash, salt, is_superuser_int as is_superuser\n"
                        "      FROM users where cert_subject = ${cert_subject} AND \n"
                        "                       cert_common_name = ${cert_common_name} LIMIT 1"
                    >>,
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
                password_hash => "$2b$12$wtY3h20mUjjmeaClpqZVveDWGlHzCGsvuThMlneGHA7wVeFYyns2u",
                salt => "$2b$12$wtY3h20mUjjmeaClpqZVve",
                is_superuser_int => 0
            },
            credentials => #{
                username => <<"bcrypt">>,
                password => <<"bcrypt">>
            },
            config_params => #{
                <<"query">> =>
                    <<
                        "SELECT password_hash, salt, is_superuser_int as is_superuser\n"
                        "                            FROM users where username = ${username} LIMIT 1"
                    >>,
                <<"password_hash_algorithm">> => #{<<"name">> => <<"bcrypt">>}
            },
            result => {ok, #{is_superuser => false}}
        },

        #{
            data => #{
                username => <<"bcrypt0">>,
                password_hash => "$2b$12$wtY3h20mUjjmeaClpqZVveDWGlHzCGsvuThMlneGHA7wVeFYyns2u",
                salt => "$2b$12$wtY3h20mUjjmeaClpqZVve",
                is_superuser_str => "0"
            },
            credentials => #{
                username => <<"bcrypt0">>,
                password => <<"bcrypt">>
            },
            config_params => #{
                % clientid variable & username credentials
                <<"query">> =>
                    <<
                        "SELECT password_hash, salt, is_superuser_int as is_superuser\n"
                        "                            FROM users where username = ${clientid} LIMIT 1"
                    >>,
                <<"password_hash_algorithm">> => #{<<"name">> => <<"bcrypt">>}
            },
            result => {error, not_authorized}
        },

        #{
            data => #{
                username => <<"bcrypt1">>,
                password_hash => "$2b$12$wtY3h20mUjjmeaClpqZVveDWGlHzCGsvuThMlneGHA7wVeFYyns2u",
                salt => "$2b$12$wtY3h20mUjjmeaClpqZVve",
                is_superuser_str => "0"
            },
            credentials => #{
                username => <<"bcrypt1">>,
                password => <<"bcrypt">>
            },
            config_params => #{
                % Bad keys in query
                <<"query">> =>
                    <<
                        "SELECT 1 AS unknown_field\n"
                        "                            FROM users where username = ${username} LIMIT 1"
                    >>,
                <<"password_hash_algorithm">> => #{<<"name">> => <<"bcrypt">>}
            },
            result => {error, not_authorized}
        },

        #{
            data => #{
                username => <<"bcrypt2">>,
                password_hash => "$2b$12$wtY3h20mUjjmeaClpqZVveDWGlHzCGsvuThMlneGHA7wVeFYyns2u",
                salt => "$2b$12$wtY3h20mUjjmeaClpqZVve",
                is_superuser => "0"
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
    ok = drop_seeds(),
    {ok, _, _} = q(
        "CREATE TABLE users(\n"
        "                       username varchar(255),\n"
        "                       password_hash varchar(255),\n"
        "                       salt varchar(255),\n"
        "                       cert_subject varchar(255),\n"
        "                       cert_common_name varchar(255),\n"
        "                       is_superuser_str varchar(255),\n"
        "                       is_superuser_int smallint,\n"
        "                       is_superuser_bool boolean)"
    ),

    lists:foreach(
        fun(#{data := Values}) ->
            ok = create_user(Values)
        end,
        user_seeds()
    ).

create_user(Values) ->
    Fields = [
        username,
        password_hash,
        salt,
        cert_subject,
        cert_common_name,
        is_superuser_str,
        is_superuser_int,
        is_superuser_bool
    ],

    InsertQuery =
        "INSERT INTO users(username, password_hash, salt, cert_subject, cert_common_name, "
        "is_superuser_str, is_superuser_int, is_superuser_bool) "
        "VALUES($1, $2, $3, $4, $5, $6, $7, $8)",

    Params = [maps:get(F, Values, null) || F <- Fields],
    {ok, 1} = q(InsertQuery, Params),
    ok.

q(Sql) ->
    emqx_resource:simple_sync_query(
        ?PGSQL_RESOURCE,
        {query, Sql}
    ).

q(Sql, Params) ->
    emqx_resource:simple_sync_query(
        ?PGSQL_RESOURCE,
        {query, Sql, Params}
    ).

drop_seeds() ->
    {ok, _, _} = q("DROP TABLE IF EXISTS users"),
    ok.

pgsql_server() ->
    iolist_to_binary(io_lib:format("~s", [?PGSQL_HOST])).

pgsql_config() ->
    #{
        auto_reconnect => true,
        disable_prepared_statements => false,
        database => <<"mqtt">>,
        username => <<"root">>,
        password => <<"public">>,
        pool_size => 8,
        server => pgsql_server(),
        ssl => #{enable => false}
    }.

start_apps(Apps) ->
    lists:foreach(fun application:ensure_all_started/1, Apps).

stop_apps(Apps) ->
    lists:foreach(fun application:stop/1, Apps).
