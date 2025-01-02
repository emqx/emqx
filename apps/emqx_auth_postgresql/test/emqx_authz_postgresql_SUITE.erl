%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%% http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(emqx_authz_postgresql_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("../../emqx_postgresql/include/emqx_postgresql.hrl").
-include_lib("emqx_auth/include/emqx_authz.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-define(PGSQL_HOST, "pgsql").
-define(PGSQL_RESOURCE, <<"emqx_authz_pgsql_SUITE">>).

all() ->
    emqx_authz_test_lib:all_with_table_case(?MODULE, t_run_case, cases()).

groups() ->
    emqx_authz_test_lib:table_groups(t_run_case, cases()).

init_per_suite(Config) ->
    case emqx_common_test_helpers:is_tcp_server_available(?PGSQL_HOST, ?PGSQL_DEFAULT_PORT) of
        true ->
            Apps = emqx_cth_suite:start(
                [
                    emqx,
                    {emqx_conf,
                        "authorization.no_match = deny, authorization.cache.enable = false"},
                    emqx_auth,
                    emqx_auth_postgresql
                ],
                #{work_dir => ?config(priv_dir, Config)}
            ),
            {ok, _} = create_pgsql_resource(),
            [{suite_apps, Apps} | Config];
        false ->
            {skip, no_pgsql}
    end.

end_per_suite(Config) ->
    ok = emqx_authz_test_lib:restore_authorizers(),
    ok = emqx_resource:remove_local(?PGSQL_RESOURCE),
    ok = emqx_cth_suite:stop_apps(?config(suite_apps, Config)).

init_per_group(Group, Config) ->
    [{test_case, emqx_authz_test_lib:get_case(Group, cases())} | Config].
end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(_TestCase, Config) ->
    ok = emqx_authz_test_lib:reset_authorizers(),
    Config.
end_per_testcase(_TestCase, _Config) ->
    _ = emqx_authz:set_feature_available(rich_actions, true),
    _ = emqx_auth_cache:reset(?AUTHZ_CACHE),
    ok = emqx_authz_test_lib:enable_node_cache(false),
    ok = drop_table(),
    ok.

set_special_configs(emqx_auth) ->
    ok = emqx_authz_test_lib:reset_authorizers();
set_special_configs(_) ->
    ok.

%%------------------------------------------------------------------------------
%% Testcases
%%------------------------------------------------------------------------------

t_run_case(Config) ->
    Case = ?config(test_case, Config),
    ok = setup_source_data(Case),
    ok = setup_authz_source(Case),
    ok = emqx_authz_test_lib:run_checks(Case).

t_create_invalid(_Config) ->
    BadConfig = maps:merge(
        raw_pgsql_authz_config(),
        #{<<"server">> => <<"255.255.255.255:33333">>}
    ),
    {ok, _} = emqx_authz:update(?CMD_REPLACE, [BadConfig]),

    [_] = emqx_authz:lookup().

t_node_cache(_Config) ->
    Case = #{
        name => cache_publish,
        setup => [
            "CREATE TABLE acl(username VARCHAR(255), topic VARCHAR(255), "
            "permission VARCHAR(255), action VARCHAR(255))",
            "INSERT INTO acl(username, topic, permission, action) VALUES('username', 'a', 'allow', 'publish')"
        ],
        query => "SELECT permission, action, topic FROM acl WHERE username = ${username}",
        client_info => #{username => <<"username">>},
        checks => []
    },
    ok = setup_source_data(Case),
    ok = setup_authz_source(Case),
    ok = emqx_authz_test_lib:enable_node_cache(true),

    %% Subscribe to twice, should hit cache the second time
    emqx_authz_test_lib:run_checks(
        Case#{
            checks => [
                {allow, ?AUTHZ_PUBLISH, <<"a">>},
                {allow, ?AUTHZ_PUBLISH, <<"a">>}
            ]
        }
    ),
    ?assertMatch(
        #{hits := #{value := 1}, misses := #{value := 1}},
        emqx_auth_cache:metrics(?AUTHZ_CACHE)
    ),

    %% Change variable, should miss cache
    emqx_authz_test_lib:run_checks(
        Case#{
            checks => [{deny, ?AUTHZ_PUBLISH, <<"a">>}],
            client_info => #{username => <<"username2">>}
        }
    ),
    ?assertMatch(
        #{hits := #{value := 1}, misses := #{value := 2}},
        emqx_auth_cache:metrics(?AUTHZ_CACHE)
    ).

%%------------------------------------------------------------------------------
%% Cases
%%------------------------------------------------------------------------------

cases() ->
    [
        #{
            name => base_publish,
            setup => [
                "CREATE TABLE acl(username VARCHAR(255), topic VARCHAR(255), "
                "permission VARCHAR(255), action VARCHAR(255))",
                "INSERT INTO acl(username, topic, permission, action) VALUES('username', 'a', 'allow', 'publish')",
                "INSERT INTO acl(username, topic, permission, action) VALUES('username', 'b', 'allow', 'subscribe')"
            ],
            query => "SELECT permission, action, topic FROM acl WHERE username = ${username}",
            client_info => #{username => <<"username">>},
            checks => [
                {allow, ?AUTHZ_PUBLISH, <<"a">>},
                {deny, ?AUTHZ_PUBLISH, <<"b">>},
                {deny, ?AUTHZ_SUBSCRIBE, <<"a">>},
                {allow, ?AUTHZ_SUBSCRIBE, <<"b">>}
            ]
        },
        #{
            name => rule_by_clientid_cn_dn_peerhost,
            setup => [
                "CREATE TABLE acl(clientid VARCHAR(255), cn VARCHAR(255), dn VARCHAR(255),"
                " peerhost VARCHAR(255), topic VARCHAR(255),"
                " permission VARCHAR(255), action VARCHAR(255))",

                "INSERT INTO acl(clientid, cn, dn, peerhost, topic, permission, action)"
                " VALUES('clientid', 'cn', 'dn', '127.0.0.1', 'a', 'allow', 'publish')"
            ],
            query =>
                "SELECT permission, action, topic FROM acl WHERE"
                " clientid = ${clientid} AND cn = ${cert_common_name}"
                " AND dn = ${cert_subject} AND peerhost = ${peerhost}",
            client_info => #{
                clientid => <<"clientid">>,
                cn => <<"cn">>,
                dn => <<"dn">>,
                peerhost => {127, 0, 0, 1}
            },
            checks => [
                {allow, ?AUTHZ_PUBLISH, <<"a">>},
                {deny, ?AUTHZ_PUBLISH, <<"b">>}
            ]
        },
        #{
            name => topics_literal_wildcard_variable,
            setup => [
                "CREATE TABLE acl(username VARCHAR(255), topic VARCHAR(255), "
                "permission VARCHAR(255), action VARCHAR(255))",
                "INSERT INTO acl(username, topic, permission, action) "
                "VALUES('username', 't/${username}', 'allow', 'publish')",

                "INSERT INTO acl(username, topic, permission, action) "
                "VALUES('username', 't/${clientid}', 'allow', 'publish')",

                "INSERT INTO acl(username, topic, permission, action) "
                "VALUES('username', 'eq t/${username}', 'allow', 'publish')",

                "INSERT INTO acl(username, topic, permission, action) "
                "VALUES('username', 't/#', 'allow', 'publish')",

                "INSERT INTO acl(username, topic, permission, action) "
                "VALUES('username', 't1/+', 'allow', 'publish')"
            ],
            query => "SELECT permission, action, topic FROM acl WHERE username = ${username}",
            client_info => #{
                username => <<"username">>
            },
            checks => [
                {allow, ?AUTHZ_PUBLISH, <<"t/username">>},
                {allow, ?AUTHZ_PUBLISH, <<"t/clientid">>},
                {allow, ?AUTHZ_PUBLISH, <<"t/${username}">>},
                {allow, ?AUTHZ_PUBLISH, <<"t/1/2">>},
                {allow, ?AUTHZ_PUBLISH, <<"t1/1">>},
                {deny, ?AUTHZ_PUBLISH, <<"t1/1/2">>},
                {deny, ?AUTHZ_PUBLISH, <<"abc">>},
                {deny, ?AUTHZ_SUBSCRIBE, <<"t/username">>}
            ]
        },
        #{
            name => qos_retain_in_query_result,
            features => [rich_actions],
            setup => [
                "CREATE TABLE acl(username VARCHAR(255), topic VARCHAR(255), "
                "permission VARCHAR(255), action VARCHAR(255),"
                "qos_s VARCHAR(255), retain_s VARCHAR(255))",

                "INSERT INTO acl(username, topic, permission, action, qos_s, retain_s)"
                " VALUES('username', 't1', 'allow', 'publish', '1', 'true')",

                "INSERT INTO acl(username, topic, permission, action, qos_s, retain_s)"
                " VALUES('username', 't2', 'allow', 'publish', '2', 'false')",

                "INSERT INTO acl(username, topic, permission, action, qos_s, retain_s)"
                " VALUES('username', 't3', 'allow', 'publish', '0,1,2', 'all')",

                "INSERT INTO acl(username, topic, permission, action, qos_s, retain_s)"
                " VALUES('username', 't4', 'allow', 'subscribe', '1', null)",

                "INSERT INTO acl(username, topic, permission, action, qos_s, retain_s)"
                " VALUES('username', 't5', 'allow', 'subscribe', '0,1,2', null)"
            ],
            query =>
                "SELECT permission, action, topic, qos_s as qos, retain_s as retain"
                " FROM acl WHERE username = ${username}",
            client_info => #{
                username => <<"username">>
            },
            checks => [
                {allow, ?AUTHZ_PUBLISH(1, true), <<"t1">>},
                {deny, ?AUTHZ_PUBLISH(1, false), <<"t1">>},
                {deny, ?AUTHZ_PUBLISH(0, true), <<"t1">>},

                {allow, ?AUTHZ_PUBLISH(2, false), <<"t2">>},
                {deny, ?AUTHZ_PUBLISH(1, false), <<"t2">>},
                {deny, ?AUTHZ_PUBLISH(2, true), <<"t2">>},

                {allow, ?AUTHZ_PUBLISH(1, true), <<"t3">>},
                {allow, ?AUTHZ_PUBLISH(2, false), <<"t3">>},
                {allow, ?AUTHZ_PUBLISH(2, true), <<"t3">>},
                {allow, ?AUTHZ_PUBLISH(0, false), <<"t3">>},

                {allow, ?AUTHZ_SUBSCRIBE(1), <<"t4">>},
                {deny, ?AUTHZ_SUBSCRIBE(2), <<"t4">>},

                {allow, ?AUTHZ_SUBSCRIBE(1), <<"t5">>},
                {allow, ?AUTHZ_SUBSCRIBE(2), <<"t5">>},
                {allow, ?AUTHZ_SUBSCRIBE(0), <<"t5">>}
            ]
        },
        #{
            name => qos_retain_in_query_result_as_integer,
            features => [rich_actions],
            setup => [
                "CREATE TABLE acl(username VARCHAR(255), topic VARCHAR(255), "
                "permission VARCHAR(255), action VARCHAR(255),"
                "qos_i VARCHAR(255), retain_i VARCHAR(255))",

                "INSERT INTO acl(username, topic, permission, action, qos_i, retain_i)"
                " VALUES('username', 't1', 'allow', 'publish', 1, 1)"
            ],
            query =>
                "SELECT permission, action, topic, qos_i as qos, retain_i as retain"
                " FROM acl WHERE username = ${username}",
            client_info => #{
                username => <<"username">>
            },
            checks => [
                {allow, ?AUTHZ_PUBLISH(1, true), <<"t1">>},
                {deny, ?AUTHZ_PUBLISH(1, false), <<"t1">>},
                {deny, ?AUTHZ_PUBLISH(0, true), <<"t1">>}
            ]
        },
        #{
            name => retain_in_query_result_as_boolean,
            features => [rich_actions],
            setup => [
                "CREATE TABLE acl(username VARCHAR(255), topic VARCHAR(255), permission VARCHAR(255),"
                " action VARCHAR(255), retain_b BOOLEAN)",

                "INSERT INTO acl(username, topic, permission, action, retain_b)"
                " VALUES('username', 't1', 'allow', 'publish', true)",

                "INSERT INTO acl(username, topic, permission, action, retain_b)"
                " VALUES('username', 't2', 'allow', 'publish', false)"
            ],
            query =>
                "SELECT permission, action, topic, retain_b as retain"
                " FROM acl WHERE username = ${username}",
            client_info => #{
                username => <<"username">>
            },
            checks => [
                {allow, ?AUTHZ_PUBLISH(1, true), <<"t1">>},
                {deny, ?AUTHZ_PUBLISH(1, false), <<"t1">>},
                {allow, ?AUTHZ_PUBLISH(1, false), <<"t2">>},
                {deny, ?AUTHZ_PUBLISH(1, true), <<"t2">>}
            ]
        },
        #{
            name => nonbin_values_in_client_info,
            setup => [
                "CREATE TABLE acl(who VARCHAR(255), topic VARCHAR(255), permission VARCHAR(255),"
                " action VARCHAR(255))",

                "INSERT INTO acl(who, topic, permission, action)"
                " VALUES('username', 't/${username}', 'allow', 'publish')",

                "INSERT INTO acl(who, topic, permission, action)"
                " VALUES('clientid', 't/${clientid}', 'allow', 'publish')"
            ],
            query =>
                "SELECT permission, action, topic"
                " FROM acl WHERE who = ${username} OR who = ${clientid}",
            client_info => #{
                %% string, not a binary
                username => "username",
                %% atom, not a binary
                clientid => clientid
            },
            checks => [
                {allow, ?AUTHZ_PUBLISH, <<"t/username">>},
                {allow, ?AUTHZ_PUBLISH, <<"t/clientid">>},
                {deny, ?AUTHZ_PUBLISH, <<"t/foo">>}
            ]
        },
        #{
            name => array_null_qos,
            features => [rich_actions],
            setup => [
                "CREATE TABLE acl(qos INTEGER[], "
                " topic VARCHAR(255), permission VARCHAR(255), action VARCHAR(255))",

                "INSERT INTO acl(qos, topic, permission, action)"
                " VALUES('{1,2}', 'tp', 'allow', 'publish')",

                "INSERT INTO acl(qos, topic, permission, action)"
                " VALUES(NULL, 'ts', 'allow', 'subscribe')"
            ],
            query =>
                "SELECT permission, action, topic, qos FROM acl",
            checks => [
                {allow, ?AUTHZ_PUBLISH(1, false), <<"tp">>},
                {allow, ?AUTHZ_PUBLISH(2, false), <<"tp">>},
                {deny, ?AUTHZ_PUBLISH(3, false), <<"tp">>},

                {allow, ?AUTHZ_SUBSCRIBE(1), <<"ts">>},
                {allow, ?AUTHZ_SUBSCRIBE(2), <<"ts">>}
            ]
        },
        #{
            name => strip_double_quote,
            setup => [
                "CREATE TABLE acl(username VARCHAR(255), topic VARCHAR(255), "
                "permission VARCHAR(255), action VARCHAR(255))",
                "INSERT INTO acl(username, topic, permission, action) VALUES('username', 'a', 'allow', 'publish')"
            ],
            query => "SELECT permission, action, topic FROM acl WHERE username = \"${username}\"",
            checks => [
                {allow, ?AUTHZ_PUBLISH, <<"a">>}
            ]
        },
        #{
            name => invalid_query,
            setup => [],
            query => "SELECT permission, action, topic FROM acl WHER",
            checks => [
                {deny, ?AUTHZ_PUBLISH, <<"a">>}
            ]
        },
        #{
            name => pgsql_error,
            setup => [],
            query =>
                "SELECT permission, action, topic FROM table_not_exists WHERE username = ${username}",
            checks => [
                {deny, ?AUTHZ_PUBLISH, <<"t">>}
            ]
        },
        #{
            name => invalid_rule,
            setup => [
                "CREATE TABLE acl(username VARCHAR(255), topic VARCHAR(255), "
                "permission VARCHAR(255), action VARCHAR(255))",
                %% 'permit' is invalid value for action
                "INSERT INTO acl(username, topic, permission, action) VALUES('username', 'a', 'permit', 'publish')"
            ],
            query => "SELECT permission, action, topic FROM acl WHERE username = ${username}",
            checks => [
                {deny, ?AUTHZ_PUBLISH, <<"a">>}
            ]
        }
        %% TODO: add case for unknown variables after fixing EMQX-10400
    ].

%%------------------------------------------------------------------------------
%% Helpers
%%------------------------------------------------------------------------------

setup_source_data(#{setup := Queries}) ->
    lists:foreach(
        fun(Query) ->
            _ = q(Query)
        end,
        Queries
    ).

setup_authz_source(#{query := Query}) ->
    setup_config(
        #{
            <<"query">> => Query
        }
    ).

raw_pgsql_authz_config() ->
    #{
        <<"enable">> => <<"true">>,

        <<"type">> => <<"postgresql">>,
        <<"database">> => <<"mqtt">>,
        <<"username">> => <<"root">>,
        <<"password">> => <<"public">>,

        <<"query">> => <<
            "SELECT permission, action, topic "
            "FROM acl WHERE username = ${username}"
        >>,
        <<"server">> => <<?PGSQL_HOST>>
    }.

q(Sql) ->
    emqx_resource:simple_sync_query(
        ?PGSQL_RESOURCE,
        {query, Sql}
    ).

drop_table() ->
    {ok, _, _} = q("DROP TABLE IF EXISTS acl"),
    ok.

setup_config(SpecialParams) ->
    emqx_authz_test_lib:setup_config(
        raw_pgsql_authz_config(),
        SpecialParams
    ).

pgsql_config() ->
    #{
        auto_reconnect => true,
        disable_prepared_statements => false,
        database => <<"mqtt">>,
        username => <<"root">>,
        password => <<"public">>,
        pool_size => 8,
        server => <<?PGSQL_HOST>>,
        ssl => #{enable => false}
    }.

create_pgsql_resource() ->
    emqx_resource:create_local(
        ?PGSQL_RESOURCE,
        ?AUTHZ_RESOURCE_GROUP,
        emqx_postgresql,
        pgsql_config(),
        #{}
    ).

start_apps(Apps) ->
    lists:foreach(fun application:ensure_all_started/1, Apps).

stop_apps(Apps) ->
    lists:foreach(fun application:stop/1, Apps).
