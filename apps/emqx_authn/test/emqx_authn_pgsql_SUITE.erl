%%--------------------------------------------------------------------
%% Copyright (c) 2020-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_authn_pgsql_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("emqx_connector/include/emqx_connector.hrl").
-include_lib("emqx_authn/include/emqx_authn.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("emqx/include/emqx_placeholder.hrl").

-define(PGSQL_HOST, "pgsql").
-define(PGSQL_RESOURCE, <<"emqx_authn_pgsql_SUITE">>).
-define(ResourceID, <<"password_based:postgresql">>).

-define(PATH, [authentication]).

all() ->
    [{group, require_seeds}, t_create_invalid].

groups() ->
    [{require_seeds, [], [t_create, t_authenticate, t_update, t_destroy, t_is_superuser]}].

init_per_testcase(_, Config) ->
    {ok, _} = emqx_cluster_rpc:start_link(node(), emqx_cluster_rpc, 1000),
    emqx_authentication:initialize_authentication(?GLOBAL, []),
    emqx_authn_test_lib:delete_authenticators(
        [authentication],
        ?GLOBAL
    ),
    Config.

init_per_group(require_seeds, Config) ->
    ok = init_seeds(),
    Config.

end_per_group(require_seeds, Config) ->
    ok = drop_seeds(),
    Config.

init_per_suite(Config) ->
    _ = application:load(emqx_conf),
    case emqx_common_test_helpers:is_tcp_server_available(?PGSQL_HOST, ?PGSQL_DEFAULT_PORT) of
        true ->
            ok = emqx_common_test_helpers:start_apps([emqx_authn]),
            ok = start_apps([emqx_resource, emqx_connector]),
            {ok, _} = emqx_resource:create_local(
                ?PGSQL_RESOURCE,
                ?RESOURCE_GROUP,
                emqx_connector_pgsql,
                pgsql_config(),
                #{}
            ),
            Config;
        false ->
            {skip, no_pgsql}
    end.

end_per_suite(_Config) ->
    emqx_authn_test_lib:delete_authenticators(
        [authentication],
        ?GLOBAL
    ),
    ok = emqx_resource:remove_local(?PGSQL_RESOURCE),
    ok = stop_apps([emqx_resource, emqx_connector]),
    ok = emqx_common_test_helpers:stop_apps([emqx_authn]).

%%------------------------------------------------------------------------------
%% Tests
%%------------------------------------------------------------------------------

t_create(_Config) ->
    AuthConfig = raw_pgsql_auth_config(),

    {ok, _} = emqx:update_config(
        ?PATH,
        {create_authenticator, ?GLOBAL, AuthConfig}
    ),

    {ok, [#{provider := emqx_authn_pgsql}]} = emqx_authentication:list_authenticators(?GLOBAL),
    emqx_authn_test_lib:delete_config(?ResourceID).

t_create_invalid(_Config) ->
    AuthConfig = raw_pgsql_auth_config(),

    InvalidConfigs =
        [
            maps:without([<<"server">>], AuthConfig),
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
                emqx_authentication:list_authenticators(?GLOBAL)
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

t_destroy(_Config) ->
    AuthConfig = raw_pgsql_auth_config(),

    {ok, _} = emqx:update_config(
        ?PATH,
        {create_authenticator, ?GLOBAL, AuthConfig}
    ),

    {ok, [#{provider := emqx_authn_pgsql, state := State}]} =
        emqx_authentication:list_authenticators(?GLOBAL),

    {ok, _} = emqx_authn_pgsql:authenticate(
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
        emqx_authn_pgsql:authenticate(
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
    Fields = [username, password_hash, salt, is_superuser_str, is_superuser_int, is_superuser_bool],

    InsertQuery =
        "INSERT INTO users(username, password_hash, salt,"
        "is_superuser_str, is_superuser_int, is_superuser_bool) "
        "VALUES($1, $2, $3, $4, $5, $6)",

    Params = [maps:get(F, Values, null) || F <- Fields],
    {ok, 1} = q(InsertQuery, Params),
    ok.

q(Sql) ->
    emqx_resource:query(
        ?PGSQL_RESOURCE,
        {query, Sql}
    ).

q(Sql, Params) ->
    emqx_resource:query(
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
        database => <<"mqtt">>,
        username => <<"root">>,
        password => <<"public">>,
        pool_size => 8,
        server => {?PGSQL_HOST, ?PGSQL_DEFAULT_PORT},
        ssl => #{enable => false}
    }.

start_apps(Apps) ->
    lists:foreach(fun application:ensure_all_started/1, Apps).

stop_apps(Apps) ->
    lists:foreach(fun application:stop/1, Apps).
