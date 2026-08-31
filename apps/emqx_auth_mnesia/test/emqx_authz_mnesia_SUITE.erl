%%--------------------------------------------------------------------
%% Copyright (c) 2020-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
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
    [{group, legacy}, {group, hardened}].

groups() ->
    Tests = emqx_common_test_helpers:all(?MODULE),
    [{legacy, [], Tests}, {hardened, [], Tests}].

init_per_suite(Config) ->
    emqx_common_test_helpers:clear_security_profile(),
    Config.

end_per_suite(_Config) ->
    emqx_common_test_helpers:clear_security_profile().

init_per_group(Profile, Config) when Profile =:= legacy; Profile =:= hardened ->
    emqx_common_test_helpers:set_security_profile(Profile),
    Apps = emqx_cth_suite:start(
        [
            {emqx_conf,
                emqx_authz_test_lib:emqx_appspec(#{
                    config =>
                        "authorization.no_match = deny, authorization.cache.enable = false"
                })},
            emqx_auth,
            emqx_auth_mnesia
        ],
        #{work_dir => emqx_cth_suite:work_dir(Profile, Config)}
    ),
    [{suite_apps, Apps}, {security_profile, Profile} | Config].

end_per_group(_Profile, Config) ->
    ok = emqx_authz_test_lib:restore_authorizers(),
    emqx_cth_suite:stop(?config(suite_apps, Config)),
    emqx_common_test_helpers:clear_security_profile().

init_per_testcase(_TestCase, Config) ->
    emqx_common_test_helpers:set_security_profile(?config(security_profile, Config)),
    ok = emqx_authz_test_lib:reset_authorizers(),
    ok = setup_config(),
    Config.

end_per_testcase(_TestCase, _Config) ->
    emqx_common_test_helpers:clear_security_profile(),
    ok = emqx_authz_mnesia:purge_rules(?global_ns).

%%------------------------------------------------------------------------------
%% Testcases
%%------------------------------------------------------------------------------

t_authz(_Config) ->
    emqx_common_test_helpers:set_security_profile("hardened"),
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

t_namespace_fallback_to_global(Config) ->
    Namespace = <<"ns1">>,
    ClientInfo = with_ns(Namespace, emqx_authz_test_lib:base_client_info()),
    AllowRule = #{
        <<"permission">> => <<"allow">>,
        <<"action">> => <<"subscribe">>,
        <<"topic">> => <<"t">>
    },
    ok = store_rules(?global_ns, all, [AllowRule]),

    %% Empty namespaces keep the global fallback for backwards compatibility.
    ?assertEqual(
        allow,
        emqx_access_control:authorize(
            emqx_authz_context:make(ClientInfo), ?AUTHZ_SUBSCRIBE, <<"t">>
        )
    ),

    ok = store_rules(Namespace, {username, <<"other-user">>}, [AllowRule]),
    Expected =
        case ?config(security_profile, Config) of
            legacy ->
                ?assertMatch(
                    {ok, [_ | _]},
                    emqx_authz_mnesia:load_rules_for_authorize(
                        Namespace, <<"someclientid">>, <<"someusername">>
                    )
                ),
                allow;
            hardened ->
                ?assertEqual(
                    {ok, []},
                    emqx_authz_mnesia:load_rules_for_authorize(
                        Namespace, <<"someclientid">>, <<"someusername">>
                    )
                ),
                deny
        end,
    ?assertEqual(
        Expected,
        emqx_access_control:authorize(
            emqx_authz_context:make(ClientInfo), ?AUTHZ_SUBSCRIBE, <<"t">>
        )
    ),
    ok = emqx_authz_mnesia:purge_rules(Namespace).

t_explicit_global_rule_conflicts(Config) ->
    Namespace = <<"ns1">>,
    Username = <<"someusername">>,
    ClientId = <<"someclientid">>,
    Rule = #{
        <<"permission">> => <<"allow">>,
        <<"action">> => <<"subscribe">>,
        <<"topic">> => <<"t">>
    },
    ok = store_rules(?global_ns, {username, Username}, [Rule]),
    ok = store_rules(?global_ns, {clientid, ClientId}, [Rule]),

    case ?config(security_profile, Config) of
        legacy ->
            ok = store_rules(Namespace, {username, Username}, [Rule]),
            ok = store_rules(Namespace, {clientid, ClientId}, [Rule]);
        hardened ->
            {error, rules_shadowed} = store_rules(Namespace, {username, Username}, [Rule]),
            {error, rules_shadowed} = store_rules(Namespace, {clientid, ClientId}, [Rule])
    end,
    ok = emqx_authz_mnesia:purge_rules(Namespace),
    ok = emqx_authz_mnesia:purge_rules(?global_ns),

    %% Existing conflicts fail closed in strict mode, including conflicts created
    %% by adding global rules after namespace rules.
    ok = store_rules(Namespace, {username, Username}, [Rule]),
    ok = store_rules(?global_ns, {username, Username}, [Rule]),
    LoadResult = emqx_authz_mnesia:load_rules_for_authorize(Namespace, ClientId, Username),
    ClientInfo0 = emqx_authz_test_lib:base_client_info(),
    ClientInfo = with_ns(Namespace, ClientInfo0#{username => Username, clientid => ClientId}),
    case ?config(security_profile, Config) of
        legacy ->
            {ok, Rules} = LoadResult,
            ?assertNotEqual([], Rules),
            ?assertEqual(
                allow,
                emqx_access_control:authorize(
                    emqx_authz_context:make(ClientInfo), ?AUTHZ_SUBSCRIBE, <<"t">>
                )
            );
        hardened ->
            ?assertEqual(deny_for_conflict, LoadResult),
            ?assertEqual(
                deny,
                emqx_access_control:authorize(
                    emqx_authz_context:make(ClientInfo), ?AUTHZ_SUBSCRIBE, <<"t">>
                )
            )
    end,
    ok = emqx_authz_mnesia:purge_rules(Namespace).

test_authz(Expected, {Who, Rule}, {ClientInfo, Action, Topic}) ->
    test_authz(Expected, ?global_ns, {Who, Rule}, {ClientInfo, Action, Topic}).

test_authz(Expected, Namespace, {Who, Rule}, {ClientInfo, Action, Topic}) ->
    ct:pal("Test authz~nns: ~p~nwho: ~p~nrule: ~p~nattempt: ~p~nexpected: ~p", [
        Namespace, Who, Rule, {ClientInfo, Action, Topic}, Expected
    ]),
    try
        ok = store_rules(Namespace, Who, [Rule]),
        ?assertEqual(
            Expected,
            emqx_access_control:authorize(emqx_authz_context:make(ClientInfo), Action, Topic)
        )
    after
        ok = emqx_authz_mnesia:purge_rules(Namespace)
    end.

-doc """
Tests that `purge_rules/1` with a namespace deletes only that namespace's rules, leaving
global and other-namespace rules intact.
""".
t_purge_namespace_rules(_Config) ->
    Rule = #{
        <<"permission">> => <<"allow">>, <<"action">> => <<"publish">>, <<"topic">> => <<"t">>
    },
    Ns1 = <<"tns1">>,
    Ns2 = <<"tns2">>,
    ok = store_rules(?global_ns, {username, <<"global-username">>}, [Rule]),
    ok = store_rules(Ns1, {username, <<"username">>}, [Rule]),
    ok = store_rules(Ns1, {clientid, <<"clientid">>}, [Rule]),
    ok = store_rules(Ns1, all, [Rule]),
    ok = store_rules(Ns2, {username, <<"username">>}, [Rule]),

    ok = emqx_authz_mnesia:purge_rules(Ns1),

    not_found = emqx_authz_mnesia:get_rules(Ns1, {username, <<"username">>}),
    not_found = emqx_authz_mnesia:get_rules(Ns1, {clientid, <<"clientid">>}),
    not_found = emqx_authz_mnesia:get_rules(Ns1, all),
    {ok, _} = emqx_authz_mnesia:get_rules(?global_ns, {username, <<"global-username">>}),
    {ok, _} = emqx_authz_mnesia:get_rules(Ns2, {username, <<"username">>}),
    ?assertEqual(0, emqx_authz_mnesia:record_count(Ns1)),

    %% Idempotent
    ok = emqx_authz_mnesia:purge_rules(Ns1),
    ok = emqx_authz_mnesia:purge_rules(Ns2),
    ok.

t_normalize_rules(_Config) ->
    ClientInfo = emqx_authz_test_lib:base_client_info(),

    ok = store_rules(
        {username, <<"username">>},
        [#{<<"permission">> => <<"allow">>, <<"action">> => <<"publish">>, <<"topic">> => <<"t">>}]
    ),

    ?assertEqual(
        allow,
        emqx_access_control:authorize(emqx_authz_context:make(ClientInfo), ?AUTHZ_PUBLISH, <<"t">>)
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
        emqx_access_control:authorize(emqx_authz_context:make(ClientInfo), ?AUTHZ_PUBLISH, <<"t">>)
    ).

t_destroy(_Config) ->
    ClientInfo = emqx_authz_test_lib:base_client_info(),

    ok = store_rules(
        {username, <<"username">>},
        [#{<<"permission">> => <<"allow">>, <<"action">> => <<"publish">>, <<"topic">> => <<"t">>}]
    ),

    ?assertEqual(
        allow,
        emqx_access_control:authorize(emqx_authz_context:make(ClientInfo), ?AUTHZ_PUBLISH, <<"t">>)
    ),

    ok = emqx_authz_test_lib:reset_authorizers(),

    ?assertEqual(
        deny,
        emqx_access_control:authorize(emqx_authz_context:make(ClientInfo), ?AUTHZ_PUBLISH, <<"t">>)
    ),

    ok = setup_config(),

    %% After destroy, the rules should be empty

    ?assertEqual(
        deny,
        emqx_access_control:authorize(emqx_authz_context:make(ClientInfo), ?AUTHZ_PUBLISH, <<"t">>)
    ).

t_conf_cli_load(_Config) ->
    ClientInfo = emqx_authz_test_lib:base_client_info(),

    ok = store_rules(
        {username, <<"username">>},
        [#{<<"permission">> => <<"allow">>, <<"action">> => <<"publish">>, <<"topic">> => <<"t">>}]
    ),

    ?assertEqual(
        allow,
        emqx_access_control:authorize(emqx_authz_context:make(ClientInfo), ?AUTHZ_PUBLISH, <<"t">>)
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
        emqx_access_control:authorize(emqx_authz_context:make(ClientInfo), ?AUTHZ_PUBLISH, <<"t">>)
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
