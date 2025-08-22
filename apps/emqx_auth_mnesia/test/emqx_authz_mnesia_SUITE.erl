%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_authz_mnesia_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("emqx_auth/include/emqx_authz.hrl").
-include_lib("emqx/include/emqx_config.hrl").

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

all() ->
    emqx_common_test_helpers:all(?MODULE).

groups() ->
    [].

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [
            {emqx_conf, "authorization.no_match = deny, authorization.cache.enable = false"},
            emqx,
            emqx_auth,
            emqx_auth_mnesia
        ],
        #{work_dir => ?config(priv_dir, Config)}
    ),
    [{suite_apps, Apps} | Config].

end_per_suite(_Config) ->
    ok = emqx_authz_test_lib:restore_authorizers(),
    emqx_cth_suite:stop(?config(suite_apps, _Config)).

init_per_testcase(_TestCase, Config) ->
    ok = emqx_authz_test_lib:reset_authorizers(),
    ok = setup_config(),
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok = emqx_authz_mnesia:purge_rules().

%%------------------------------------------------------------------------------
%% Testcases
%%------------------------------------------------------------------------------

t_authz(_Config) ->
    ClientInfo = emqx_authz_test_lib:base_client_info(),

    test_authz(
        allow,
        {all, #{
            <<"permission">> => <<"allow">>, <<"action">> => <<"subscribe">>, <<"topic">> => <<"t">>
        }},
        {ClientInfo, ?AUTHZ_SUBSCRIBE, <<"t">>}
    ),
    test_authz(
        allow,
        {{username, <<"username">>}, #{
            <<"permission">> => <<"allow">>,
            <<"action">> => <<"subscribe">>,
            <<"topic">> => <<"t/${username}">>
        }},
        {ClientInfo, ?AUTHZ_SUBSCRIBE, <<"t/username">>}
    ),
    test_authz(
        allow,
        {{username, <<"username">>}, #{
            <<"permission">> => <<"allow">>,
            <<"action">> => <<"subscribe">>,
            <<"topic">> => <<"eq t/${username}">>
        }},
        {ClientInfo, ?AUTHZ_SUBSCRIBE, <<"t/${username}">>}
    ),
    test_authz(
        deny,
        {{username, <<"username">>}, #{
            <<"permission">> => <<"allow">>,
            <<"action">> => <<"subscribe">>,
            <<"topic">> => <<"eq t/${username}">>
        }},
        {ClientInfo, ?AUTHZ_SUBSCRIBE, <<"t/username">>}
    ),
    test_authz(
        allow,
        {{clientid, <<"clientid">>}, #{
            <<"permission">> => <<"allow">>,
            <<"action">> => <<"subscribe">>,
            <<"topic">> => <<"eq t/${username}">>
        }},
        {ClientInfo, ?AUTHZ_SUBSCRIBE, <<"t/${username}">>}
    ),
    test_authz(
        allow,
        {{clientid, <<"clientid">>}, #{
            <<"permission">> => <<"allow">>,
            <<"action">> => <<"subscribe">>,
            <<"topic">> => <<"t">>,
            <<"clientid_re">> => <<"ent+">>
        }},
        {ClientInfo, ?AUTHZ_SUBSCRIBE, <<"t">>}
    ),
    test_authz(
        deny,
        {{clientid, <<"clientid">>}, #{
            <<"permission">> => <<"allow">>,
            <<"action">> => <<"subscribe">>,
            <<"topic">> => <<"t">>,
            <<"clientid_re">> => <<"X+">>
        }},
        {ClientInfo, ?AUTHZ_SUBSCRIBE, <<"t">>}
    ),
    test_authz(
        allow,
        {{clientid, <<"clientid">>}, #{
            <<"permission">> => <<"allow">>,
            <<"action">> => <<"subscribe">>,
            <<"topic">> => <<"t">>,
            <<"clientid_re">> => <<"ent+">>,
            <<"username_re">> => <<"user+">>,
            <<"ipaddr">> => <<"127.0.0.0/24">>
        }},
        {ClientInfo, ?AUTHZ_SUBSCRIBE, <<"t">>}
    ),
    test_authz(
        deny,
        {{clientid, <<"clientid">>}, #{
            <<"permission">> => <<"allow">>,
            <<"action">> => <<"subscribe">>,
            <<"topic">> => <<"t">>,
            <<"ipaddr">> => <<"127.0.1.0/24">>
        }},
        {ClientInfo, ?AUTHZ_SUBSCRIBE, <<"t">>}
    ),
    test_authz(
        allow,
        {
            {clientid, <<"clientid">>},
            #{
                <<"permission">> => <<"allow">>,
                <<"action">> => <<"publish">>,
                <<"topic">> => <<"t">>,
                <<"qos">> => <<"1,2">>,
                <<"retain">> => <<"true">>
            }
        },
        {ClientInfo, ?AUTHZ_PUBLISH(1, true), <<"t">>}
    ),
    test_authz(
        deny,
        {
            {clientid, <<"clientid">>},
            #{
                <<"permission">> => <<"allow">>,
                <<"action">> => <<"publish">>,
                <<"topic">> => <<"t">>,
                <<"qos">> => <<"1,2">>,
                <<"retain">> => <<"true">>
            }
        },
        {ClientInfo, ?AUTHZ_PUBLISH(0, true), <<"t">>}
    ),
    test_authz(
        deny,
        {
            {clientid, <<"clientid">>},
            #{
                <<"permission">> => <<"allow">>,
                <<"action">> => <<"publish">>,
                <<"topic">> => <<"t">>,
                <<"qos">> => <<"1,2">>,
                <<"retain">> => <<"true">>
            }
        },
        {ClientInfo, ?AUTHZ_PUBLISH(1, false), <<"t">>}
    ),
    test_authz(
        allow,
        {
            {clientid, <<"clientid">>},
            #{
                <<"permission">> => <<"allow">>,
                <<"action">> => <<"publish">>,
                <<"topic">> => <<"t">>,
                <<"zone">> => <<"zone1">>
            }
        },
        {ClientInfo#{zone => zone1}, ?AUTHZ_PUBLISH, <<"t">>}
    ),
    test_authz(
        deny,
        {
            {clientid, <<"clientid">>},
            #{
                <<"permission">> => <<"allow">>,
                <<"action">> => <<"publish">>,
                <<"topic">> => <<"t">>,
                <<"zone">> => <<"zone1">>
            }
        },
        {ClientInfo#{zone => zone2}, ?AUTHZ_PUBLISH, <<"t">>}
    ),
    test_authz(
        allow,
        {
            {clientid, <<"clientid">>},
            #{
                <<"permission">> => <<"allow">>,
                <<"action">> => <<"publish">>,
                <<"topic">> => <<"t">>,
                <<"zone_re">> => <<"^zone\\d+">>
            }
        },
        {ClientInfo#{zone => zone1}, ?AUTHZ_PUBLISH, <<"t">>}
    ),
    test_authz(
        deny,
        {
            {clientid, <<"clientid">>},
            #{
                <<"permission">> => <<"allow">>,
                <<"action">> => <<"publish">>,
                <<"topic">> => <<"t">>,
                <<"zone_re">> => <<"^zone\\d+">>
            }
        },
        {ClientInfo#{zone => other}, ?AUTHZ_PUBLISH, <<"t">>}
    ),
    test_authz(
        allow,
        {
            {clientid, <<"clientid">>},
            #{
                <<"permission">> => <<"allow">>,
                <<"action">> => <<"publish">>,
                <<"topic">> => <<"t">>,
                <<"listener">> => <<"tcp:default">>
            }
        },
        {ClientInfo#{listener => 'tcp:default'}, ?AUTHZ_PUBLISH, <<"t">>}
    ),
    test_authz(
        deny,
        {
            {clientid, <<"clientid">>},
            #{
                <<"permission">> => <<"allow">>,
                <<"action">> => <<"publish">>,
                <<"topic">> => <<"t">>,
                <<"listener">> => <<"tcp:default">>
            }
        },
        {ClientInfo#{listener => 'ws:default'}, ?AUTHZ_PUBLISH, <<"t">>}
    ),
    test_authz(
        allow,
        {
            {clientid, <<"clientid">>},
            #{
                <<"permission">> => <<"allow">>,
                <<"action">> => <<"publish">>,
                <<"topic">> => <<"t">>,
                <<"listener_re">> => <<"^tcp:">>
            }
        },
        {ClientInfo#{listener => 'tcp:default'}, ?AUTHZ_PUBLISH, <<"t">>}
    ),
    test_authz(
        deny,
        {
            {clientid, <<"clientid">>},
            #{
                <<"permission">> => <<"allow">>,
                <<"action">> => <<"publish">>,
                <<"topic">> => <<"t">>,
                <<"listener_re">> => <<"^tcp:">>
            }
        },
        {ClientInfo#{listener => 'ws:default'}, ?AUTHZ_PUBLISH, <<"t">>}
    ),
    ok.

test_authz(Expected, {Who, Rule}, {ClientInfo, Action, Topic}) ->
    ct:pal("Test authz~nwho:~p~nrule:~p~nattempt:~p~nexpected ~p", [
        Who, Rule, {ClientInfo, Action, Topic}, Expected
    ]),
    try
        ok = emqx_authz_mnesia:store_rules(Who, [Rule]),
        ?assertEqual(Expected, emqx_access_control:authorize(ClientInfo, Action, Topic))
    after
        ok = emqx_authz_mnesia:purge_rules()
    end.

t_normalize_rules(_Config) ->
    ClientInfo = emqx_authz_test_lib:base_client_info(),

    ok = emqx_authz_mnesia:store_rules(
        {username, <<"username">>},
        [#{<<"permission">> => <<"allow">>, <<"action">> => <<"publish">>, <<"topic">> => <<"t">>}]
    ),

    ?assertEqual(
        allow,
        emqx_access_control:authorize(ClientInfo, ?AUTHZ_PUBLISH, <<"t">>)
    ),

    ?assertException(
        error,
        #{reason := invalid_rule},
        emqx_authz_mnesia:store_rules(
            {username, <<"username">>},
            [[<<"allow">>, <<"publish">>, <<"t">>]]
        )
    ),

    ?assertException(
        error,
        #{reason := invalid_action},
        emqx_authz_mnesia:store_rules(
            {username, <<"username">>},
            [
                #{
                    <<"permission">> => <<"allow">>,
                    <<"action">> => <<"badaction">>,
                    <<"topic">> => <<"t">>
                }
            ]
        )
    ),

    ?assertException(
        error,
        #{reason := invalid_permission},
        emqx_authz_mnesia:store_rules(
            {username, <<"username">>},
            [
                #{
                    <<"permission">> => <<"accept">>,
                    <<"action">> => <<"publish">>,
                    <<"topic">> => <<"t">>
                }
            ]
        )
    ).

t_legacy_rules(_Config) ->
    ClientInfo = emqx_authz_test_lib:base_client_info(),

    ok = emqx_authz_mnesia:do_store_rules(
        %% {?ACL_TABLE_USERNAME, <<"username">>}
        {1, <<"username">>},
        [
            %% Legacy 3-tuple format without `who' field
            {allow, {publish, [{qos, [0, 1, 2]}, {retain, all}]}, <<"t">>}
        ]
    ),

    ?assertEqual(
        allow,
        emqx_access_control:authorize(ClientInfo, ?AUTHZ_PUBLISH, <<"t">>)
    ).

t_destroy(_Config) ->
    ClientInfo = emqx_authz_test_lib:base_client_info(),

    ok = emqx_authz_mnesia:store_rules(
        {username, <<"username">>},
        [#{<<"permission">> => <<"allow">>, <<"action">> => <<"publish">>, <<"topic">> => <<"t">>}]
    ),

    ?assertEqual(
        allow,
        emqx_access_control:authorize(ClientInfo, ?AUTHZ_PUBLISH, <<"t">>)
    ),

    ok = emqx_authz_test_lib:reset_authorizers(),

    ?assertEqual(
        deny,
        emqx_access_control:authorize(ClientInfo, ?AUTHZ_PUBLISH, <<"t">>)
    ),

    ok = setup_config(),

    %% After destroy, the rules should be empty

    ?assertEqual(
        deny,
        emqx_access_control:authorize(ClientInfo, ?AUTHZ_PUBLISH, <<"t">>)
    ).

t_conf_cli_load(_Config) ->
    ClientInfo = emqx_authz_test_lib:base_client_info(),

    ok = emqx_authz_mnesia:store_rules(
        {username, <<"username">>},
        [#{<<"permission">> => <<"allow">>, <<"action">> => <<"publish">>, <<"topic">> => <<"t">>}]
    ),

    ?assertEqual(
        allow,
        emqx_access_control:authorize(ClientInfo, ?AUTHZ_PUBLISH, <<"t">>)
    ),
    PrevRules = ets:tab2list(emqx_acl),
    Hocon = emqx_conf_cli:get_config_namespaced(?global_ns, "authorization"),
    Bin = iolist_to_binary(hocon_pp:do(Hocon, #{})),
    ok = emqx_conf_cli:load_config(?global_ns, Bin, #{mode => merge}),
    %% ensure emqx_acl table not clear
    ?assertEqual(PrevRules, ets:tab2list(emqx_acl)),
    %% still working
    ?assertEqual(
        allow,
        emqx_access_control:authorize(ClientInfo, ?AUTHZ_PUBLISH, <<"t">>)
    ).

%%------------------------------------------------------------------------------
%% Helpers
%%------------------------------------------------------------------------------

raw_mnesia_authz_config() ->
    #{
        <<"enable">> => <<"true">>,
        <<"type">> => <<"built_in_database">>
    }.

setup_config() ->
    emqx_authz_test_lib:setup_config(raw_mnesia_authz_config(), #{}).
