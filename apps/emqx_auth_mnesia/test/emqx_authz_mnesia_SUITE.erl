%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_authz_mnesia_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("emqx_auth/include/emqx_authz.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_config.hrl").

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

all() ->
    emqx_common_test_helpers:all(?MODULE).

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
    ok = emqx_authz_mnesia:purge_rules(?global_ns).

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
    Namespace1 = <<"ns1">>,
    %% ns mismatch (global and specific)
    test_authz(
        deny,
        Namespace1,
        {all, #{
            <<"permission">> => <<"allow">>, <<"action">> => <<"subscribe">>, <<"topic">> => <<"t">>
        }},
        {ClientInfo, ?AUTHZ_SUBSCRIBE, <<"t">>}
    ),
    %% ns matches
    test_authz(
        allow,
        Namespace1,
        {all, #{
            <<"permission">> => <<"allow">>, <<"action">> => <<"subscribe">>, <<"topic">> => <<"t">>
        }},
        {with_ns(Namespace1, ClientInfo), ?AUTHZ_SUBSCRIBE, <<"t">>}
    ),
    %% ns mismatch (different specific namespaces)
    Namespace2 = <<"ns2">>,
    test_authz(
        deny,
        Namespace1,
        {all, #{
            <<"permission">> => <<"allow">>, <<"action">> => <<"subscribe">>, <<"topic">> => <<"t">>
        }},
        {with_ns(Namespace2, ClientInfo), ?AUTHZ_SUBSCRIBE, <<"t">>}
    ),
    %% user exists in global namespace, but credentials are namespace; falls back to
    %% global namespace for backwards compatibility.
    test_authz(
        allow,
        ?global_ns,
        {all, #{
            <<"permission">> => <<"allow">>, <<"action">> => <<"subscribe">>, <<"topic">> => <<"t">>
        }},
        {with_ns(Namespace1, ClientInfo), ?AUTHZ_SUBSCRIBE, <<"t">>}
    ),
    ok.

test_authz(Expected, {Who, Rule}, {ClientInfo, Action, Topic}) ->
    test_authz(Expected, ?global_ns, {Who, Rule}, {ClientInfo, Action, Topic}).

test_authz(Expected, Namespace, {Who, Rule}, {ClientInfo, Action, Topic}) ->
    ct:pal("Test authz~nns: ~p~nwho: ~p~nrule: ~p~nattempt: ~p~nexpected: ~p", [
        Namespace, Who, Rule, {ClientInfo, Action, Topic}, Expected
    ]),
    try
        ok = store_rules(Namespace, Who, [Rule]),
        ?assertEqual(Expected, emqx_access_control:authorize(ClientInfo, Action, Topic))
    after
        ok = emqx_authz_mnesia:purge_rules(Namespace)
    end.

t_normalize_rules(_Config) ->
    ClientInfo = emqx_authz_test_lib:base_client_info(),

    ok = store_rules(
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
        store_rules(
            {username, <<"username">>},
            [[<<"allow">>, <<"publish">>, <<"t">>]]
        )
    ),

    ?assertException(
        error,
        #{reason := invalid_action},
        store_rules(
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
        store_rules(
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
        ?global_ns,
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

    ok = store_rules(
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

    ok = store_rules(
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

store_rules(Who, Rules) ->
    emqx_authz_mnesia:store_rules(?global_ns, Who, Rules).

store_rules(Namespace, Who, Rules) ->
    emqx_authz_mnesia:store_rules(Namespace, Who, Rules).

with_ns(Namespace, ClientInfo) ->
    maps:update_with(
        client_attrs,
        fun(Attrs) -> Attrs#{?CLIENT_ATTR_NAME_TNS => Namespace} end,
        #{?CLIENT_ATTR_NAME_TNS => Namespace},
        ClientInfo
    ).
