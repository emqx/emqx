%%--------------------------------------------------------------------
%% Copyright (c) 2020-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_authz_mongodb_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("emqx_auth/include/emqx_authz.hrl").
-include("../../emqx_connector/include/emqx_connector.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("emqx/include/emqx_placeholder.hrl").

-define(MONGO_HOST, "mongo").

all() ->
    [
        {group, legacy_protocol_true},
        {group, legacy_protocol_false},
        {group, legacy_protocol_auto}
    ].

groups() ->
    All = emqx_authz_test_lib:all_with_table_case(?MODULE, t_run_case, cases()),
    [
        {legacy_protocol_true, [], All},
        {legacy_protocol_false, [], All},
        {legacy_protocol_auto, [], All}
    ] ++
        emqx_authz_test_lib:table_groups(t_run_case, cases()).

init_per_suite(Config) ->
    case emqx_common_test_helpers:is_tcp_server_available(?MONGO_HOST, ?MONGO_DEFAULT_PORT) of
        true ->
            Apps = emqx_cth_suite:start(
                [
                    emqx,
                    {emqx_conf,
                        "authorization.no_match = deny, authorization.cache.enable = false"},
                    emqx_auth,
                    emqx_auth_mongodb
                ],
                #{work_dir => ?config(priv_dir, Config)}
            ),
            [
                {suite_apps, Apps},
                {mongo_supports_legacy_protocol,
                    mongo_supports_legacy_protocol(?MONGO_HOST, ?MONGO_DEFAULT_PORT)}
                | Config
            ];
        false ->
            {skip, no_mongo}
    end.

end_per_suite(Config) ->
    ok = emqx_authz_test_lib:restore_authorizers(),
    emqx_cth_suite:stop(?config(suite_apps, Config)).

init_per_group(legacy_protocol_true, Config) ->
    [{use_legacy_protocol, true} | Config];
init_per_group(legacy_protocol_false, Config) ->
    [{use_legacy_protocol, false} | Config];
init_per_group(legacy_protocol_auto, Config) ->
    [{use_legacy_protocol, auto} | Config];
init_per_group(Group, Config) ->
    [{test_case, emqx_authz_test_lib:get_case(Group, cases())} | Config].
end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(_TestCase, Config) ->
    %% The forced-legacy cases only run against MongoDB servers that still
    %% support the legacy OP_QUERY opcodes (removed in MongoDB 5.1).
    case
        ?config(use_legacy_protocol, Config) =:= true andalso
            not proplists:get_value(mongo_supports_legacy_protocol, Config, true)
    of
        true ->
            {skip, mongo_server_dropped_legacy_protocol};
        false ->
            %% Address the seeding connection by pid: the driver keys the
            %% negotiated wire protocol by pid, so a registered name would
            %% resolve to the legacy-protocol default, which MongoDB 5.1+
            %% rejects. The pid auto-negotiates OP_MSG on every supported server.
            {ok, Client} = mc_worker_api:connect(mongo_config()),
            ok = emqx_authz_test_lib:reset_authorizers(),
            [{mongo_client, Client} | Config]
    end.

end_per_testcase(_TestCase, Config) ->
    Client = ?config(mongo_client, Config),
    ok = emqx_authz_test_lib:enable_node_cache(false),
    ok = reset_samples(Client),
    ok = mc_worker_api:disconnect(Client).

%%------------------------------------------------------------------------------
%% Testcases
%%------------------------------------------------------------------------------

t_run_case(Config) ->
    run_test(
        ?config(mongo_client, Config),
        ?config(test_case, Config),
        ?config(use_legacy_protocol, Config)
    ).

run_test(_Client, #{name := extended_query_with_order_skip_limit}, true) ->
    ok;
run_test(Client, Case, UseLegacyProtocol) ->
    ok = setup_source_data(Client, Case),
    ok = setup_authz_source(Case#{use_legacy_protocol => UseLegacyProtocol}),
    ok = emqx_authz_test_lib:run_checks(Case).

t_node_cache(Config) ->
    Client = ?config(mongo_client, Config),
    ok = emqx_authz_test_lib:reset_node_cache(),
    Case = #{
        name => cache_publish,
        records => [
            #{
                <<"username">> => <<"username">>,
                <<"action">> => <<"publish">>,
                <<"topic">> => <<"a">>,
                <<"permission">> => <<"allow">>
            }
        ],
        filter => #{<<"username">> => <<"${username}">>},
        client_info => #{username => <<"username">>},
        use_legacy_protocol => <<"auto">>,
        checks => []
    },
    ok = setup_source_data(Client, Case),
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
            records => [
                #{
                    <<"username">> => <<"username">>,
                    <<"action">> => <<"publish">>,
                    <<"topic">> => <<"a">>,
                    <<"permission">> => <<"allow">>
                },
                #{
                    <<"username">> => <<"username">>,
                    <<"action">> => <<"subscribe">>,
                    <<"topic">> => <<"b">>,
                    <<"permission">> => <<"allow">>
                },
                #{
                    <<"username">> => <<"username">>,
                    <<"action">> => <<"all">>,
                    <<"topics">> => [<<"c">>, <<"d">>],
                    <<"permission">> => <<"allow">>
                }
            ],
            filter => #{<<"username">> => <<"${username}">>},
            checks => [
                {allow, ?AUTHZ_PUBLISH, <<"a">>},
                {deny, ?AUTHZ_SUBSCRIBE, <<"a">>},

                {deny, ?AUTHZ_PUBLISH, <<"b">>},
                {allow, ?AUTHZ_SUBSCRIBE, <<"b">>},

                {allow, ?AUTHZ_PUBLISH, <<"c">>},
                {allow, ?AUTHZ_SUBSCRIBE, <<"c">>},
                {allow, ?AUTHZ_PUBLISH, <<"d">>},
                {allow, ?AUTHZ_SUBSCRIBE, <<"d">>}
            ]
        },
        #{
            name => filter_works,
            records => [
                #{
                    <<"action">> => <<"publish">>,
                    <<"topic">> => <<"a">>,
                    <<"permission">> => <<"allow">>
                }
            ],
            filter => #{<<"username">> => <<"${username}">>},
            checks => [
                {deny, ?AUTHZ_PUBLISH, <<"a">>}
            ]
        },
        #{
            name => extended_query_with_order_skip_limit,
            records => [
                #{
                    <<"username">> => <<"usernameWrong">>,
                    <<"action">> => <<"publish">>,
                    <<"topic">> => <<"a">>,
                    <<"permission">> => <<"allow">>,
                    <<"order">> => <<"0">>
                },
                #{
                    <<"username">> => <<"username">>,
                    <<"action">> => <<"publish">>,
                    <<"topic">> => <<"a">>,
                    <<"permission">> => <<"deny">>,
                    <<"order">> => <<"1">>
                },
                #{
                    <<"username">> => <<"username">>,
                    <<"action">> => <<"publish">>,
                    <<"topic">> => <<"a">>,
                    <<"permission">> => <<"allow">>,
                    <<"order">> => <<"2">>
                },
                #{
                    <<"username">> => <<"username">>,
                    <<"action">> => <<"publish">>,
                    <<"topic">> => <<"a">>,
                    <<"permission">> => <<"deny">>,
                    <<"order">> => <<"3">>
                }
            ],
            filter => #{
                <<"username">> => <<"${username}">>,
                <<"$orderby">> => #{<<"order">> => 1}
            },
            settings => #{skip => 1, limit => 1},
            %% We have 3 matching rules from 4.
            %% From the matching rules ordered by `order' field only the second one is allowing.
            %% We should reach it utilizing `$orderby' and `skip' options.
            checks => [
                {allow, ?AUTHZ_PUBLISH, <<"a">>}
            ]
        },
        #{
            name => invalid_rich_rules,
            records => [
                #{
                    <<"action">> => <<"publish">>,
                    <<"topic">> => <<"a">>,
                    <<"permission">> => <<"allow">>,
                    <<"qos">> => <<"1,2,3">>
                },
                #{
                    <<"action">> => <<"publish">>,
                    <<"topic">> => <<"a">>,
                    <<"permission">> => <<"allow">>,
                    <<"retain">> => <<"yes">>
                }
            ],
            filter => #{},
            checks => [
                {deny, ?AUTHZ_PUBLISH, <<"a">>}
            ]
        },
        #{
            name => invalid_rules,
            records => [
                #{
                    <<"action">> => <<"publis">>,
                    <<"topic">> => <<"a">>,
                    <<"permission">> => <<"allow">>
                }
            ],
            filter => #{},
            checks => [
                {deny, ?AUTHZ_PUBLISH, <<"a">>}
            ]
        },
        #{
            name => rule_by_clientid_cn_dn_peerhost,
            records => [
                #{
                    <<"cn">> => <<"cn">>,
                    <<"dn">> => <<"dn">>,
                    <<"clientid">> => <<"clientid">>,
                    <<"peerhost">> => <<"127.0.0.1">>,
                    <<"action">> => <<"publish">>,
                    <<"topic">> => <<"a">>,
                    <<"permission">> => <<"allow">>
                }
            ],
            client_info => #{
                cn => <<"cn">>,
                dn => <<"dn">>
            },
            filter => #{
                <<"cn">> => <<"${cert_common_name}">>,
                <<"dn">> => <<"${cert_subject}">>,
                <<"clientid">> => <<"${clientid}">>,
                <<"peerhost">> => <<"${peerhost}">>
            },
            checks => [
                {allow, ?AUTHZ_PUBLISH, <<"a">>}
            ]
        },
        #{
            name => topics_literal_wildcard_variable,
            records => [
                #{
                    <<"username">> => <<"username">>,
                    <<"action">> => <<"publish">>,
                    <<"permission">> => <<"allow">>,
                    <<"topics">> => [
                        <<"t/${username}">>,
                        <<"t/${clientid}">>,
                        <<"t1/#">>,
                        <<"t2/+">>,
                        <<"eq t3/${username}">>
                    ]
                }
            ],
            filter => #{<<"username">> => <<"${username}">>},
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
            records => [
                #{
                    <<"username">> => <<"username">>,
                    <<"action">> => <<"publish">>,
                    <<"permission">> => <<"allow">>,
                    <<"topic">> => <<"a">>,
                    <<"qos">> => 1,
                    <<"retain">> => true
                },
                #{
                    <<"username">> => <<"username">>,
                    <<"action">> => <<"publish">>,
                    <<"permission">> => <<"allow">>,
                    <<"topic">> => <<"b">>,
                    <<"qos">> => <<"1">>,
                    <<"retain">> => <<"true">>
                },
                #{
                    <<"username">> => <<"username">>,
                    <<"action">> => <<"publish">>,
                    <<"permission">> => <<"allow">>,
                    <<"topic">> => <<"c">>,
                    <<"qos">> => <<"1,2">>,
                    <<"retain">> => 1
                },
                #{
                    <<"username">> => <<"username">>,
                    <<"action">> => <<"publish">>,
                    <<"permission">> => <<"allow">>,
                    <<"topic">> => <<"d">>,
                    <<"qos">> => [1, 2],
                    <<"retain">> => <<"1">>
                },
                #{
                    <<"username">> => <<"username">>,
                    <<"action">> => <<"publish">>,
                    <<"permission">> => <<"allow">>,
                    <<"topic">> => <<"e">>,
                    <<"qos">> => [1, 2],
                    <<"retain">> => <<"all">>
                },
                #{
                    <<"username">> => <<"username">>,
                    <<"action">> => <<"publish">>,
                    <<"permission">> => <<"allow">>,
                    <<"topic">> => <<"f">>,
                    <<"qos">> => null,
                    <<"retain">> => null
                }
            ],
            filter => #{<<"username">> => <<"${username}">>},
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
            records => [
                #{
                    <<"username">> => <<"username">>,
                    <<"clientid">> => <<"clientid">>,
                    <<"action">> => <<"publish">>,
                    <<"topic">> => <<"a">>,
                    <<"permission">> => <<"allow">>
                }
            ],
            client_info => #{
                username => "username",
                clientid => clientid
            },
            filter => #{<<"username">> => <<"${username}">>, <<"clientid">> => <<"${clientid}">>},
            checks => [
                {allow, ?AUTHZ_PUBLISH, <<"a">>}
            ]
        },
        #{
            name => invalid_query,
            records => [
                #{
                    <<"action">> => <<"publish">>,
                    <<"topic">> => <<"a">>,
                    <<"permission">> => <<"allow">>
                }
            ],
            filter => #{<<"$in">> => #{<<"a">> => 1}},
            checks => [
                {deny, ?AUTHZ_PUBLISH, <<"a">>}
            ]
        },
        #{
            name => complex_query,
            records => [
                #{
                    <<"a">> => #{<<"u">> => <<"clientid">>, <<"c">> => [<<"cn">>, <<"dn">>]},
                    <<"action">> => <<"publish">>,
                    <<"topic">> => <<"a">>,
                    <<"permission">> => <<"allow">>
                }
            ],
            client_info => #{
                cn => <<"cn">>,
                dn => <<"dn">>
            },
            filter => #{
                <<"a">> => #{
                    <<"u">> => <<"${clientid}">>,
                    <<"c">> => [<<"${cert_common_name}">>, <<"${cert_subject}">>]
                }
            },
            checks => [
                {allow, ?AUTHZ_PUBLISH, <<"a">>}
            ]
        }
    ].

%%------------------------------------------------------------------------------
%% Helpers
%%------------------------------------------------------------------------------

reset_samples(Client) ->
    {true, _} = mc_worker_api:delete(Client, <<"acl">>, #{}),
    ok.

setup_source_data(Client, #{records := Records}) ->
    {{true, _}, _} = mc_worker_api:insert(Client, <<"acl">>, Records),
    ok.

setup_authz_source(#{filter := Filter, use_legacy_protocol := UseLegacyProtocol} = Case) ->
    AdditionalSettings = maps:get(settings, Case, #{}),
    setup_config(
        AdditionalSettings#{
            <<"filter">> => Filter,
            <<"use_legacy_protocol">> => UseLegacyProtocol
        }
    ).

setup_config(SpecialParams) ->
    emqx_authz_test_lib:setup_config(
        raw_mongo_authz_config(),
        SpecialParams
    ).

raw_mongo_authz_config() ->
    #{
        <<"type">> => <<"mongodb">>,
        <<"enable">> => <<"true">>,

        <<"mongo_type">> => <<"single">>,
        <<"database">> => <<"mqtt">>,
        <<"collection">> => <<"acl">>,
        <<"server">> => mongo_server(),

        <<"auth_source">> => mongo_authsource(),
        <<"username">> => mongo_username(),
        <<"password">> => mongo_password(),

        <<"filter">> => #{<<"username">> => <<"${username}">>},
        <<"use_legacy_protocol">> => <<"auto">>
    }.

mongo_server() ->
    iolist_to_binary(io_lib:format("~s", [?MONGO_HOST])).

mongo_config() ->
    [
        {database, <<"mqtt">>},
        {host, ?MONGO_HOST},
        {port, ?MONGO_DEFAULT_PORT},
        {auth_source, mongo_authsource()},
        {login, mongo_username()},
        {password, mongo_password()}
    ].

%% MongoDB 5.1 removed the legacy OP_QUERY opcodes (wire protocol version 14),
%% so the legacy-protocol cases only apply to servers reporting wire version 13
%% (MongoDB 5.0) or below. `isMaster` is answered before authentication, so no
%% credentials are required to probe the running server.
mongo_supports_legacy_protocol(Host, Port) ->
    %% Force the modern OP_MSG protocol for the probe itself: the legacy
    %% OP_QUERY opcodes the probe is checking for are exactly what newer servers
    %% reject, so a legacy probe connection would fail on them.
    {ok, Conn} = mc_worker_api:connect([
        {database, <<"admin">>},
        {host, Host},
        {port, Port},
        {use_legacy_protocol, false}
    ]),
    try
        {true, Info} = mc_worker_api:command(Conn, {<<"isMaster">>, 1}),
        maps:get(<<"maxWireVersion">>, Info, 0) =< 13
    after
        mc_worker_api:disconnect(Conn)
    end.

mongo_authsource() ->
    iolist_to_binary(os:getenv("MONGO_AUTHSOURCE", "admin")).

mongo_username() ->
    iolist_to_binary(os:getenv("MONGO_USERNAME", "")).

mongo_password() ->
    iolist_to_binary(os:getenv("MONGO_PASSWORD", "")).
