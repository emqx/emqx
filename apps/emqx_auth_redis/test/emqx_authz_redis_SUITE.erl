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

-module(emqx_authz_redis_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include("../../emqx_connector/include/emqx_connector.hrl").
-include_lib("emqx_auth/include/emqx_authz.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-define(REDIS_HOST, "redis").
-define(REDIS_RESOURCE, <<"emqx_authz_redis_SUITE">>).

all() ->
    emqx_authz_test_lib:all_with_table_case(?MODULE, t_run_case, cases()).

groups() ->
    emqx_authz_test_lib:table_groups(t_run_case, cases()).

init_per_suite(Config) ->
    case emqx_common_test_helpers:is_tcp_server_available(?REDIS_HOST, ?REDIS_DEFAULT_PORT) of
        true ->
            Apps = emqx_cth_suite:start(
                [
                    emqx,
                    {emqx_conf,
                        "authorization.no_match = deny, authorization.cache.enable = false"},
                    emqx_auth,
                    emqx_auth_redis
                ],
                #{work_dir => ?config(priv_dir, Config)}
            ),
            ok = create_redis_resource(),
            [{suite_apps, Apps} | Config];
        false ->
            {skip, no_redis}
    end.

end_per_suite(Config) ->
    ok = emqx_authz_test_lib:restore_authorizers(),
    ok = emqx_resource:remove_local(?REDIS_RESOURCE),
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
    ok = emqx_authz_test_lib:enable_node_cache(false),
    _ = cleanup_redis(),
    ok.

%%------------------------------------------------------------------------------
%% Tests
%%------------------------------------------------------------------------------

t_run_case(Config) ->
    Case = ?config(test_case, Config),
    ok = setup_source_data(Case),
    ok = setup_authz_source(Case),
    ok = emqx_authz_test_lib:run_checks(Case).

%% should still succeed to create even if the config will not work,
%% because it's not a part of the schema check
t_create_with_config_values_wont_work(_Config) ->
    AuthzConfig = raw_redis_authz_config(),

    InvalidConfigs =
        [
            AuthzConfig#{<<"password">> => <<"wrongpass">>},
            AuthzConfig#{<<"database">> => <<"5678">>}
        ],

    lists:foreach(
        fun(Config) ->
            {ok, _} = emqx_authz:update(?CMD_REPLACE, [Config]),
            [_] = emqx_authz:lookup()
        end,
        InvalidConfigs
    ).

%% creating without a required field should return error
t_create_invalid_config(_Config) ->
    AuthzConfig = raw_redis_authz_config(),
    C = maps:without([<<"server">>], AuthzConfig),
    ?assertMatch(
        {error, #{
            kind := validation_error,
            path := "authorization.sources.1.server"
        }},
        emqx_authz:update(?CMD_REPLACE, [C])
    ).

t_redis_error(_Config) ->
    q([<<"SET">>, <<"notahash">>, <<"stringvalue">>]),

    ok = setup_config(#{<<"cmd">> => <<"HGETALL notahash">>}),

    ClientInfo = emqx_authz_test_lib:base_client_info(),

    ?assertEqual(
        deny,
        emqx_access_control:authorize(ClientInfo, ?AUTHZ_SUBSCRIBE, <<"a">>)
    ).

t_invalid_command(_Config) ->
    Config = raw_redis_authz_config(),

    ?assertMatch(
        {error, _},
        emqx_authz:update(?CMD_REPLACE, [Config#{<<"cmd">> => <<"HGET key">>}])
    ),

    ?assertMatch(
        {ok, _},
        emqx_authz:update(?CMD_REPLACE, [Config#{<<"cmd">> => <<"HGETALL key">>}])
    ),

    ?assertMatch(
        {error, _},
        emqx_authz:update({?CMD_REPLACE, redis}, Config#{<<"cmd">> => <<"HGET key">>})
    ).

t_node_cache(_Config) ->
    Case = #{
        name => cache_publish,
        setup => [["HMSET", "acl:node_cache_user", "a", "publish"]],
        cmd => "HGETALL acl:${username}",
        client_info => #{username => <<"node_cache_user">>},
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
                [
                    "HMSET",
                    "acl:username",
                    "a",
                    "publish",
                    "b",
                    "subscribe",
                    "d",
                    "all"
                ]
            ],
            cmd => "HGETALL acl:${username}",
            checks => [
                {allow, ?AUTHZ_PUBLISH, <<"a">>},
                {deny, ?AUTHZ_SUBSCRIBE, <<"a">>},

                {deny, ?AUTHZ_PUBLISH, <<"b">>},
                {allow, ?AUTHZ_SUBSCRIBE, <<"b">>},

                {allow, ?AUTHZ_PUBLISH, <<"d">>},
                {allow, ?AUTHZ_SUBSCRIBE, <<"d">>}
            ]
        },
        #{
            name => invalid_rule,
            setup => [
                [
                    "HMSET",
                    "acl:username",
                    "a",
                    "[]",
                    "b",
                    "{invalid:json}",
                    "c",
                    "pub",
                    "d",
                    emqx_utils_json:encode(#{qos => 1, retain => true})
                ]
            ],
            cmd => "HGETALL acl:${username}",
            checks => [
                {deny, ?AUTHZ_PUBLISH, <<"a">>},
                {deny, ?AUTHZ_PUBLISH, <<"b">>},
                {deny, ?AUTHZ_PUBLISH, <<"c">>},
                {deny, ?AUTHZ_PUBLISH(1, true), <<"d">>}
            ]
        },
        #{
            name => rule_by_clientid_cn_dn_peerhost,
            setup => [
                ["HMSET", "acl:clientid:cn:dn:127.0.0.1", "a", "publish"]
            ],
            cmd => "HGETALL acl:${clientid}:${cert_common_name}:${cert_subject}:${peerhost}",
            client_info => #{
                cn => <<"cn">>,
                dn => <<"dn">>
            },
            checks => [
                {allow, ?AUTHZ_PUBLISH, <<"a">>}
            ]
        },
        #{
            name => topics_literal_wildcard_variable,
            setup => [
                [
                    "HMSET",
                    "acl:username",
                    "t/${username}",
                    "publish",
                    "t/${clientid}",
                    "publish",
                    "t1/#",
                    "publish",
                    "t2/+",
                    "publish",
                    "eq t3/${username}",
                    "publish"
                ]
            ],
            cmd => "HGETALL acl:${username}",
            checks => [
                {allow, ?AUTHZ_PUBLISH, <<"t/username">>},
                {allow, ?AUTHZ_PUBLISH, <<"t/clientid">>},
                {allow, ?AUTHZ_PUBLISH, <<"t1/a/b">>},
                {allow, ?AUTHZ_PUBLISH, <<"t2/a">>},
                {allow, ?AUTHZ_PUBLISH, <<"t3/${username}">>},
                {deny, ?AUTHZ_PUBLISH, <<"t3/username">>}
            ]
        },
        #{
            name => qos_retain_in_query_result,
            features => [rich_actions],
            setup => [
                [
                    "HMSET",
                    "acl:username",
                    "a",
                    emqx_utils_json:encode(#{action => <<"publish">>, qos => 1, retain => true}),
                    "b",
                    emqx_utils_json:encode(#{
                        action => <<"publish">>, qos => <<"1">>, retain => <<"true">>
                    }),
                    "c",
                    emqx_utils_json:encode(#{action => <<"publish">>, qos => <<"1,2">>, retain => 1}),
                    "d",
                    emqx_utils_json:encode(#{
                        action => <<"publish">>, qos => [1, 2], retain => <<"1">>
                    }),
                    "e",
                    emqx_utils_json:encode(#{
                        action => <<"publish">>, qos => [1, 2], retain => <<"all">>
                    }),
                    "f",
                    emqx_utils_json:encode(#{action => <<"publish">>, qos => null, retain => null})
                ]
            ],
            cmd => "HGETALL acl:${username}",
            checks => [
                {allow, ?AUTHZ_PUBLISH(1, true), <<"a">>},
                {deny, ?AUTHZ_PUBLISH(1, false), <<"a">>},

                {allow, ?AUTHZ_PUBLISH(1, true), <<"b">>},
                {deny, ?AUTHZ_PUBLISH(1, false), <<"b">>},
                {deny, ?AUTHZ_PUBLISH(2, false), <<"b">>},

                {allow, ?AUTHZ_PUBLISH(2, true), <<"c">>},
                {deny, ?AUTHZ_PUBLISH(2, false), <<"c">>},
                {deny, ?AUTHZ_PUBLISH(0, true), <<"c">>},

                {allow, ?AUTHZ_PUBLISH(2, true), <<"d">>},
                {deny, ?AUTHZ_PUBLISH(0, true), <<"d">>},

                {allow, ?AUTHZ_PUBLISH(1, false), <<"e">>},
                {allow, ?AUTHZ_PUBLISH(1, true), <<"e">>},
                {deny, ?AUTHZ_PUBLISH(0, false), <<"e">>},

                {allow, ?AUTHZ_PUBLISH, <<"f">>},
                {deny, ?AUTHZ_SUBSCRIBE, <<"f">>}
            ]
        },
        #{
            name => nonbin_values_in_client_info,
            setup => [
                [
                    "HMSET",
                    "acl:username:clientid",
                    "a",
                    "publish"
                ]
            ],
            client_info => #{
                username => "username",
                clientid => clientid
            },
            cmd => "HGETALL acl:${username}:${clientid}",
            checks => [
                {allow, ?AUTHZ_PUBLISH, <<"a">>}
            ]
        },
        #{
            name => invalid_query,
            setup => [
                ["SET", "acl:username", 1]
            ],
            cmd => "HGETALL acl:${username}",
            checks => [
                {deny, ?AUTHZ_PUBLISH, <<"a">>}
            ]
        }
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

setup_authz_source(#{cmd := Cmd}) ->
    setup_config(
        #{
            <<"cmd">> => Cmd
        }
    ).

setup_config(SpecialParams) ->
    Config = maps:merge(raw_redis_authz_config(), SpecialParams),
    {ok, _} = emqx_authz:update(?CMD_REPLACE, [Config]),
    ok.

raw_redis_authz_config() ->
    #{
        <<"enable">> => <<"true">>,
        <<"type">> => <<"redis">>,
        <<"redis_type">> => <<"single">>,
        <<"cmd">> => <<"HGETALL mqtt_user:${username}">>,
        <<"database">> => <<"1">>,
        <<"password">> => <<"public">>,
        <<"server">> => <<?REDIS_HOST>>
    }.

cleanup_redis() ->
    q([<<"FLUSHALL">>]).

q(Command) ->
    emqx_resource:simple_sync_query(
        ?REDIS_RESOURCE,
        {cmd, Command}
    ).

redis_config() ->
    #{
        auto_reconnect => true,
        database => 1,
        pool_size => 8,
        redis_type => single,
        password => "public",
        server => <<?REDIS_HOST>>,
        ssl => #{enable => false}
    }.

start_apps(Apps) ->
    lists:foreach(fun application:ensure_all_started/1, Apps).

stop_apps(Apps) ->
    lists:foreach(fun application:stop/1, Apps).

create_redis_resource() ->
    {ok, _} = emqx_resource:create_local(
        ?REDIS_RESOURCE,
        ?AUTHZ_RESOURCE_GROUP,
        emqx_redis,
        redis_config(),
        #{}
    ),
    ok.
