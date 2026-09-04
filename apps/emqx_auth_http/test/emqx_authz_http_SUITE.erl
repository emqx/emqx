%%--------------------------------------------------------------------
%% Copyright (c) 2020-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_authz_http_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("emqx_auth/include/emqx_authz.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("emqx/include/emqx_placeholder.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-define(HTTP_PATH, "/authz/[...]").
-define(AUTHZ_HTTP_RESP(Result, Req),
    cowboy_req:reply(
        200,
        #{<<"content-type">> => <<"application/json">>},
        "{\"result\": \"" ++ atom_to_list(Result) ++ "\"}",
        Req
    )
).

all() ->
    ProfileCases = profile_cases(),
    [{group, legacy}, {group, hardened}] ++
        (emqx_common_test_helpers:all(?MODULE) -- ProfileCases).

groups() ->
    ProfileCases = profile_cases(),
    [{legacy, [], ProfileCases}, {hardened, [], ProfileCases}].

init_per_group(Profile, Config) when Profile =:= legacy; Profile =:= hardened ->
    ok = emqx_common_test_helpers:set_security_profile(Profile),
    [{security_profile, Profile} | Config].

end_per_group(Profile, _Config) when Profile =:= legacy; Profile =:= hardened ->
    emqx_common_test_helpers:clear_security_profile().

init_per_suite(TCConfig) ->
    Apps = emqx_cth_suite:start(
        [
            {emqx_conf,
                emqx_authz_test_lib:emqx_appspec(#{
                    config => "authorization.no_match = deny, authorization.cache.enable = false"
                })},
            emqx_auth,
            emqx_auth_http
        ],
        #{work_dir => emqx_cth_suite:work_dir(TCConfig)}
    ),
    [{suite_apps, Apps} | TCConfig].

end_per_suite(_TCConfig) ->
    ok = emqx_authz_test_lib:restore_authorizers(),
    emqx_cth_suite:stop(?config(suite_apps, _TCConfig)).

init_per_testcase(t_bad_response = TestCase, TCConfig) ->
    TCApps = emqx_cth_suite:start_apps(
        [emqx_management, emqx_mgmt_api_test_util:emqx_dashboard()],
        #{work_dir => emqx_cth_suite:work_dir(TestCase, TCConfig)}
    ),
    init_per_testcase(common, [{tc_apps, TCApps} | TCConfig]);
init_per_testcase(_TestCase, TCConfig) ->
    ok = emqx_authz_test_lib:reset_authorizers(),
    ok = emqx_authz_test_lib:reset_node_cache(),
    HTTPPort = emqx_common_test_helpers:select_free_port(tcp),
    {ok, _} = emqx_utils_http_test_server:start_link(HTTPPort, ?HTTP_PATH),
    [{http_port, HTTPPort} | TCConfig].

end_per_testcase(t_bad_response, TCConfig) ->
    TCApps = ?config(tc_apps, TCConfig),
    emqx_cth_suite:stop_apps(TCApps),
    end_per_testcase(common, TCConfig);
end_per_testcase(_TestCase, _TCConfig) ->
    ok = emqx_authz_test_lib:enable_node_cache(false),
    try
        ok = emqx_utils_http_test_server:stop()
    catch
        exit:noproc ->
            ok
    end,
    snabbkaffe:stop(),
    ok.

%%------------------------------------------------------------------------------
%% Tests
%%------------------------------------------------------------------------------

t_response_handling(TCConfig) ->
    ClientInfo = #{
        clientid => <<"clientid">>,
        username => <<"username">>,
        peerhost => {127, 0, 0, 1},
        zone => default,
        listener => 'tcp:default'
    },

    %% OK, get, body & headers
    ok = setup_handler_and_config(
        TCConfig,
        fun(Req0, State) ->
            {ok, ?AUTHZ_HTTP_RESP(allow, Req0), State}
        end,
        #{}
    ),

    ?assertEqual(
        allow,
        emqx_access_control:authorize(emqx_authz_context:make(ClientInfo), ?AUTHZ_PUBLISH, <<"t">>)
    ),

    %% Not OK, get, no body
    ok = setup_handler_and_config(
        TCConfig,
        fun(Req0, State) ->
            Req = cowboy_req:reply(200, Req0),
            {ok, Req, State}
        end,
        #{}
    ),

    deny = emqx_access_control:authorize(
        emqx_authz_context:make(ClientInfo), ?AUTHZ_PUBLISH, <<"t">>
    ),

    %% OK, get, 204
    ok = setup_handler_and_config(
        TCConfig,
        fun(Req0, State) ->
            Req = cowboy_req:reply(204, Req0),
            {ok, Req, State}
        end,
        #{}
    ),

    ?assertEqual(
        allow,
        emqx_access_control:authorize(emqx_authz_context:make(ClientInfo), ?AUTHZ_PUBLISH, <<"t">>)
    ),

    %% Not OK, get, 400
    ok = setup_handler_and_config(
        TCConfig,
        fun(Req0, State) ->
            Req = cowboy_req:reply(400, Req0),
            {ok, Req, State}
        end,
        #{}
    ),

    ?assertEqual(
        deny,
        emqx_access_control:authorize(emqx_authz_context:make(ClientInfo), ?AUTHZ_PUBLISH, <<"t">>)
    ),

    %% Not OK, get, 400 + body & headers
    ok = setup_handler_and_config(
        TCConfig,
        fun(Req0, State) ->
            Req = cowboy_req:reply(
                400,
                #{<<"content-type">> => <<"text/plain">>},
                "Response body",
                Req0
            ),
            {ok, Req, State}
        end,
        #{}
    ),

    ?assertEqual(
        deny,
        emqx_access_control:authorize(emqx_authz_context:make(ClientInfo), ?AUTHZ_PUBLISH, <<"t">>)
    ),

    %% The server cannot be reached; hardened mode should deny authorization.
    ok = emqx_utils_http_test_server:stop(),

    ?check_trace(
        ?assertEqual(
            deny,
            emqx_access_control:authorize(
                emqx_authz_context:make(ClientInfo), ?AUTHZ_PUBLISH, <<"t">>
            )
        ),
        fun(Trace) ->
            ?assertMatch(
                [
                    #{
                        ?snk_kind := authz_http_request_failure,
                        error := {recoverable_error, econnrefused}
                    }
                ],
                ?of_kind(authz_http_request_failure, Trace)
            ),
            case ?config(security_profile, TCConfig) of
                legacy ->
                    ?assert(
                        ?strict_causality(
                            #{?snk_kind := authz_http_request_failure},
                            #{?snk_kind := authz_non_superuser, result := nomatch},
                            Trace
                        )
                    );
                hardened ->
                    ?assertEqual([], ?of_kind(authz_non_superuser, Trace))
            end,
            ok
        end
    ),

    ok.

t_query_params(TCConfig) ->
    ok = setup_handler_and_config(
        TCConfig,
        fun(Req0, State) ->
            #{
                username := <<"user name">>,
                clientid := <<"client id">>,
                peerhost := <<"127.0.0.1">>,
                peerport := <<"9876">>,
                proto_name := <<"MQTT">>,
                mountpoint := <<"MOUNTPOINT">>,
                topic := <<"t/1">>,
                action := <<"publish">>,
                access := <<"2">>,
                qos := <<"1">>,
                retain := <<"false">>
            } = cowboy_req:match_qs(
                [
                    username,
                    clientid,
                    peerhost,
                    peerport,
                    proto_name,
                    mountpoint,
                    topic,
                    action,
                    access,
                    qos,
                    retain
                ],
                Req0
            ),
            {ok, ?AUTHZ_HTTP_RESP(allow, Req0), State}
        end,
        #{
            <<"url">> => <<
                "http://127.0.0.1:",
                (http_port_bin(TCConfig))/binary,
                "/authz/users/?"
                "username=${username}&"
                "clientid=${clientid}&"
                "peerhost=${peerhost}&"
                "peerport=${peerport}&"
                "proto_name=${proto_name}&"
                "mountpoint=${mountpoint}&"
                "topic=${topic}&"
                "action=${action}&"
                "access=${access}&"
                "qos=${qos}&"
                "retain=${retain}"
            >>
        }
    ),

    ClientInfo = #{
        clientid => <<"client id">>,
        username => <<"user name">>,
        peerhost => {127, 0, 0, 1},
        peerport => 9876,
        protocol => <<"MQTT">>,
        mountpoint => <<"MOUNTPOINT">>,
        zone => default,
        listener => 'tcp:default'
    },

    ?assertEqual(
        allow,
        emqx_access_control:authorize(
            emqx_authz_context:make(ClientInfo), ?AUTHZ_PUBLISH(1, false), <<"t/1">>
        )
    ).

t_path(TCConfig) ->
    ok = setup_handler_and_config(
        TCConfig,
        fun(Req0, State) ->
            ?assertEqual(
                <<
                    "/authz/use+rs/"
                    "user+name/"
                    "client+id/"
                    "127.0.0.1/"
                    "MQTT/"
                    "MOUNTPOINT/"
                    "t%2F1/"
                    "publish/"
                    "2/"
                    "1/"
                    "false"
                >>,
                cowboy_req:path(Req0)
            ),
            {ok, ?AUTHZ_HTTP_RESP(allow, Req0), State}
        end,
        #{
            <<"url">> => <<
                "http://127.0.0.1:",
                (http_port_bin(TCConfig))/binary,
                "/authz/use+rs/"
                "${username}/"
                "${clientid}/"
                "${peerhost}/"
                "${proto_name}/"
                "${mountpoint}/"
                "${topic}/"
                "${action}/"
                "${access}/"
                "${qos}/"
                "${retain}"
            >>
        }
    ),

    ClientInfo = #{
        clientid => <<"client id">>,
        username => <<"user name">>,
        peerhost => {127, 0, 0, 1},
        protocol => <<"MQTT">>,
        mountpoint => <<"MOUNTPOINT">>,
        zone => default,
        listener => 'tcp:default'
    },

    ?assertEqual(
        allow,
        emqx_access_control:authorize(
            emqx_authz_context:make(ClientInfo), ?AUTHZ_PUBLISH(1, false), <<"t/1">>
        )
    ).

t_json_body(TCConfig) ->
    ok = setup_handler_and_config(
        TCConfig,
        fun(Req0, State) ->
            ?assertEqual(
                <<"/authz/users/">>,
                cowboy_req:path(Req0)
            ),

            {ok, RawBody, Req1} = cowboy_req:read_body(Req0),

            ?assertMatch(
                #{
                    <<"username">> := <<"user name">>,
                    <<"CLIENT">> := <<"client id">>,
                    <<"peerhost">> := <<"127.0.0.1">>,
                    <<"peerport">> := <<"9876">>,
                    <<"proto_name">> := <<"MQTT">>,
                    <<"mountpoint">> := <<"MOUNTPOINT">>,
                    <<"topic">> := <<"t">>,
                    <<"action">> := <<"publish">>,
                    <<"access">> := <<"2">>,
                    <<"qos">> := <<"1">>,
                    <<"retain">> := <<"false">>
                },
                emqx_utils_json:decode(RawBody)
            ),
            {ok, ?AUTHZ_HTTP_RESP(allow, Req1), State}
        end,
        #{
            <<"method">> => <<"post">>,
            <<"body">> => #{
                <<"username">> => <<"${username}">>,
                <<"CLIENT">> => <<"${clientid}">>,
                <<"peerhost">> => <<"${peerhost}">>,
                <<"peerport">> => <<"${peerport}">>,
                <<"proto_name">> => <<"${proto_name}">>,
                <<"mountpoint">> => <<"${mountpoint}">>,
                <<"topic">> => <<"${topic}">>,
                <<"action">> => <<"${action}">>,
                <<"access">> => <<"${access}">>,
                <<"qos">> => <<"${qos}">>,
                <<"retain">> => <<"${retain}">>
            }
        }
    ),

    ClientInfo = #{
        clientid => <<"client id">>,
        username => <<"user name">>,
        peerhost => {127, 0, 0, 1},
        peerport => 9876,
        protocol => <<"MQTT">>,
        mountpoint => <<"MOUNTPOINT">>,
        zone => default,
        listener => 'tcp:default'
    },

    ?assertEqual(
        allow,
        emqx_access_control:authorize(
            emqx_authz_context:make(ClientInfo), ?AUTHZ_PUBLISH(1, false), <<"t">>
        )
    ).

-doc "Verify that ${peerport} is rendered in HTTP authz body templates.".
t_peerport_rendered_in_body(TCConfig) ->
    ok = setup_handler_and_config(
        TCConfig,
        fun(Req0, State) ->
            {ok, RawBody, Req1} = cowboy_req:read_body(Req0),
            ?assertMatch(
                #{
                    <<"peerport">> := <<"9876">>
                },
                emqx_utils_json:decode(RawBody)
            ),
            {ok, ?AUTHZ_HTTP_RESP(allow, Req1), State}
        end,
        #{
            <<"method">> => <<"post">>,
            <<"body">> => #{
                <<"peerport">> => <<"${peerport}">>
            }
        }
    ),

    ClientInfo = #{
        clientid => <<"clientid">>,
        username => <<"username">>,
        peerhost => {127, 0, 0, 1},
        peerport => 9876,
        zone => default,
        listener => 'tcp:default'
    },

    ?assertEqual(
        allow,
        emqx_access_control:authorize(ClientInfo, ?AUTHZ_PUBLISH(1, false), <<"t">>)
    ).

-doc "Verify that ${peername} is rendered as \"IP:PORT\" in HTTP authz body templates.".
t_peername_rendered_in_body(TCConfig) ->
    ok = setup_handler_and_config(
        TCConfig,
        fun(Req0, State) ->
            {ok, RawBody, Req1} = cowboy_req:read_body(Req0),
            ?assertMatch(
                #{
                    <<"peername">> := <<"127.0.0.1:9876">>
                },
                emqx_utils_json:decode(RawBody)
            ),
            {ok, ?AUTHZ_HTTP_RESP(allow, Req1), State}
        end,
        #{
            <<"method">> => <<"post">>,
            <<"body">> => #{
                <<"peername">> => <<"${peername}">>
            }
        }
    ),

    ClientInfo = #{
        clientid => <<"clientid">>,
        username => <<"username">>,
        peerhost => {127, 0, 0, 1},
        peername => {{127, 0, 0, 1}, 9876},
        zone => default,
        listener => 'tcp:default'
    },

    ?assertEqual(
        allow,
        emqx_access_control:authorize(ClientInfo, ?AUTHZ_PUBLISH(1, false), <<"t">>)
    ).

t_placeholder_and_body(TCConfig) ->
    emqx_common_test_helpers:with_security_profile("hardened", fun() ->
        ok = setup_handler_and_config(
            TCConfig,
            fun(Req0, State) ->
                ?assertEqual(
                    <<"/authz/users/">>,
                    cowboy_req:path(Req0)
                ),

                <<"g1">> = cowboy_req:header(<<"the_group">>, Req0),
                {ok, PostVars, Req1} = cowboy_req:read_urlencoded_body(Req0),

                ?assertMatch(
                    #{
                        <<"username">> := <<"user name">>,
                        <<"clientid">> := <<"client id">>,
                        <<"peerhost">> := <<"127.0.0.1">>,
                        <<"peerport">> := <<"1883">>,
                        <<"proto_name">> := <<"MQTT">>,
                        <<"mountpoint">> := <<"MOUNTPOINT">>,
                        <<"topic">> := <<"t">>,
                        <<"action">> := <<"publish">>,
                        <<"access">> := <<"2">>,
                        <<"the_group">> := <<"g1">>,
                        <<"CN">> := ?PH_CERT_CN_NAME,
                        <<"CS">> := ?PH_CERT_SUBJECT,
                        <<"cert_pem">> := <<"Y2VydGlmaWNhdGU=">>,
                        <<"zone">> := <<"default">>,
                        <<"listener_id">> := <<"tcp:default">>
                    },
                    maps:from_list(PostVars)
                ),
                {ok, ?AUTHZ_HTTP_RESP(allow, Req1), State}
            end,
            #{
                <<"method">> => <<"post">>,
                <<"body">> => #{
                    <<"username">> => <<"${username}">>,
                    <<"clientid">> => <<"${clientid}">>,
                    <<"peerhost">> => <<"${peerhost}">>,
                    <<"peerport">> => <<"${peerport}">>,
                    <<"proto_name">> => <<"${proto_name}">>,
                    <<"mountpoint">> => <<"${mountpoint}">>,
                    <<"topic">> => <<"${topic}">>,
                    <<"action">> => <<"${action}">>,
                    <<"access">> => <<"${access}">>,
                    <<"the_group">> => <<"${client_attrs.group}">>,
                    <<"CN">> => ?PH_CERT_CN_NAME,
                    <<"CS">> => ?PH_CERT_SUBJECT,
                    <<"cert_pem">> => <<"${cert_pem}">>,
                    <<"zone">> => <<"${zone}">>,
                    <<"listener_id">> => <<"${listener}">>
                },
                <<"headers">> => #{
                    <<"content-type">> => <<"application/x-www-form-urlencoded">>,
                    <<"the_group">> => <<"${client_attrs.group}">>
                }
            }
        ),

        ClientInfo = #{
            clientid => <<"client id">>,
            username => <<"user name">>,
            peerhost => {127, 0, 0, 1},
            peername => {{127, 0, 0, 1}, 1883},
            protocol => <<"MQTT">>,
            mountpoint => <<"MOUNTPOINT">>,
            zone => default,
            listener => 'tcp:default',
            client_attrs => #{<<"group">> => <<"g1">>},
            cn => ?PH_CERT_CN_NAME,
            dn => ?PH_CERT_SUBJECT,
            cert_pem => <<"certificate">>
        },

        ?assertEqual(
            allow,
            emqx_access_control:authorize(
                emqx_authz_context:make(ClientInfo), ?AUTHZ_PUBLISH, <<"t">>
            )
        )
    end).

%% Checks that we don't crash when receiving an unsupported content-type back.
t_bad_response_content_type(TCConfig) ->
    ok = setup_handler_and_config(
        TCConfig,
        fun(Req0, State) ->
            ?assertEqual(
                <<"/authz/users/">>,
                cowboy_req:path(Req0)
            ),

            {ok, _PostVars, Req1} = cowboy_req:read_urlencoded_body(Req0),

            Req = cowboy_req:reply(
                200,
                #{<<"content-type">> => <<"text/csv">>},
                "hi",
                Req1
            ),
            {ok, Req, State}
        end,
        #{
            <<"method">> => <<"post">>,
            <<"body">> => #{
                <<"username">> => <<"${username}">>,
                <<"clientid">> => <<"${clientid}">>,
                <<"peerhost">> => <<"${peerhost}">>,
                <<"proto_name">> => <<"${proto_name}">>,
                <<"mountpoint">> => <<"${mountpoint}">>,
                <<"topic">> => <<"${topic}">>,
                <<"action">> => <<"${action}">>,
                <<"access">> => <<"${access}">>,
                <<"CN">> => ?PH_CERT_CN_NAME,
                <<"CS">> => ?PH_CERT_SUBJECT
            },
            <<"headers">> => #{
                <<"accept">> => <<"text/plain">>,
                <<"content-type">> => <<"application/json">>
            }
        }
    ),

    ClientInfo = #{
        clientid => <<"client id">>,
        username => <<"user name">>,
        peerhost => {127, 0, 0, 1},
        protocol => <<"MQTT">>,
        mountpoint => <<"MOUNTPOINT">>,
        zone => default,
        listener => 'tcp:default',
        cn => ?PH_CERT_CN_NAME,
        dn => ?PH_CERT_SUBJECT
    },

    ?check_trace(
        ?assertEqual(
            deny,
            emqx_access_control:authorize(
                emqx_authz_context:make(ClientInfo), ?AUTHZ_PUBLISH, <<"t">>
            )
        ),
        fun(Trace) ->
            ?assertMatch(
                [#{reason := <<"unsupported content-type", _/binary>>}],
                ?of_kind(bad_authz_http_response, Trace)
            ),
            ok
        end
    ).

t_bad_response_content_type_profile(TCConfig) ->
    ClientInfo = #{
        clientid => <<"client id">>,
        username => <<"user name">>,
        peerhost => {127, 0, 0, 1},
        protocol => <<"MQTT">>,
        mountpoint => <<"MOUNTPOINT">>,
        zone => default,
        listener => 'tcp:default',
        cn => ?PH_CERT_CN_NAME,
        dn => ?PH_CERT_SUBJECT
    },
    ok = setup_bad_response_content_type(TCConfig),
    {ok, _} = emqx:update_config([authorization, no_match], allow),
    Expected =
        case ?config(security_profile, TCConfig) of
            legacy -> allow;
            hardened -> deny
        end,
    ?assertEqual(
        Expected,
        emqx_access_control:authorize(
            emqx_authz_context:make(ClientInfo), ?AUTHZ_PUBLISH, <<"t">>
        )
    ).

%% Checks that we bump the correct metrics when we receive an error response
t_bad_response(TCConfig) ->
    ok = setup_handler_and_config(
        TCConfig,
        fun(Req0, State) ->
            ?assertEqual(
                <<"/authz/users/">>,
                cowboy_req:path(Req0)
            ),

            {ok, _PostVars, Req1} = cowboy_req:read_urlencoded_body(Req0),

            Req = cowboy_req:reply(
                400,
                #{<<"content-type">> => <<"application/json">>},
                "{\"error\":true}",
                Req1
            ),
            {ok, Req, State}
        end,
        #{
            <<"method">> => <<"post">>,
            <<"body">> => #{
                <<"username">> => <<"${username}">>
            },
            <<"headers">> => #{}
        }
    ),

    ClientInfo = #{
        clientid => <<"client id">>,
        username => <<"user name">>,
        peerhost => {127, 0, 0, 1},
        protocol => <<"MQTT">>,
        mountpoint => <<"MOUNTPOINT">>,
        zone => default,
        listener => 'tcp:default',
        cn => ?PH_CERT_CN_NAME,
        dn => ?PH_CERT_SUBJECT
    },

    MetricsBefore = get_metrics(),
    ?assertEqual(
        deny,
        emqx_access_control:authorize(emqx_authz_context:make(ClientInfo), ?AUTHZ_PUBLISH, <<"t">>)
    ),
    {ExpectedIgnore, ExpectedDeny, ExpectedGlobalIncrements} =
        case ?config(security_profile, TCConfig) of
            legacy ->
                {1, 0, #{
                    'authorization.superuser' => 0,
                    'authorization.matched.allow' => 0,
                    'authorization.matched.deny' => 0,
                    'authorization.nomatch' => 1
                }};
            hardened ->
                {0, 1, #{
                    'authorization.superuser' => 0,
                    'authorization.matched.allow' => 0,
                    'authorization.matched.deny' => 1,
                    'authorization.nomatch' => 0
                }}
        end,
    MetricsAfter = get_metrics(),
    ?assertMatch(
        #{
            counters := #{
                total := 1,
                ignore := ExpectedIgnore,
                nomatch := 0,
                allow := 0,
                deny := ExpectedDeny
            }
        },
        MetricsAfter
    ),
    ?assertEqual(
        ExpectedGlobalIncrements,
        maps:map(
            fun(Name, Value) -> Value - maps:get(Name, MetricsBefore) end,
            maps:with(maps:keys(ExpectedGlobalIncrements), MetricsAfter)
        )
    ),
    ?assertMatch(
        {200, #{
            <<"metrics">> := #{
                <<"ignore">> := ExpectedIgnore,
                <<"nomatch">> := 0,
                <<"allow">> := 0,
                <<"deny">> := ExpectedDeny,
                <<"total">> := 1
            },
            <<"node_metrics">> := [
                #{
                    <<"metrics">> := #{
                        <<"ignore">> := ExpectedIgnore,
                        <<"nomatch">> := 0,
                        <<"allow">> := 0,
                        <<"deny">> := ExpectedDeny,
                        <<"total">> := 1
                    }
                }
            ]
        }},
        get_status_api()
    ),
    ok.

t_no_value_for_placeholder(TCConfig) ->
    ok = setup_handler_and_config(
        TCConfig,
        fun(Req0, State) ->
            ?assertEqual(
                <<"/authz/users/">>,
                cowboy_req:path(Req0)
            ),

            {ok, RawBody, Req1} = cowboy_req:read_body(Req0),

            ?assertMatch(
                #{
                    <<"mountpoint">> := <<"[]">>
                },
                emqx_utils_json:decode(RawBody)
            ),
            {ok, ?AUTHZ_HTTP_RESP(allow, Req1), State}
        end,
        #{
            <<"method">> => <<"post">>,
            <<"body">> => #{
                <<"mountpoint">> => <<"[${mountpoint}]">>
            }
        }
    ),

    ClientInfo = #{
        clientid => <<"client id">>,
        username => <<"user name">>,
        peerhost => {127, 0, 0, 1},
        protocol => <<"MQTT">>,
        zone => default,
        listener => 'tcp:default'
    },

    ?assertEqual(
        allow,
        emqx_access_control:authorize(emqx_authz_context:make(ClientInfo), ?AUTHZ_PUBLISH, <<"t">>)
    ).

t_node_cache(TCConfig) ->
    ok = setup_handler_and_config(
        TCConfig,
        fun(#{path := Path} = Req, State) ->
            case {Path, cowboy_req:match_qs([username, cn], Req)} of
                {<<"/authz/clientid">>, #{username := <<"username">>, cn := <<"cn">>}} ->
                    {ok, ?AUTHZ_HTTP_RESP(allow, Req), State};
                _ ->
                    {ok, ?AUTHZ_HTTP_RESP(deny, Req), State}
            end
        end,
        #{
            <<"method">> => <<"get">>,
            <<"url">> =>
                <<"http://127.0.0.1:", (http_port_bin(TCConfig))/binary,
                    "/authz/${clientid}?username=${username}">>,
            <<"body">> => #{<<"cn">> => <<"${cert_common_name}">>}
        }
    ),
    ok = emqx_authz_test_lib:enable_node_cache(true),

    %% We authorize twice, the second time should be cached
    ClientInfo = #{
        clientid => <<"clientid">>,
        username => <<"username">>,
        peerhost => {127, 0, 0, 1},
        protocol => <<"MQTT">>,
        zone => default,
        listener => 'tcp:default',
        cn => <<"cn">>,
        dn => <<"dn">>
    },
    ?assertEqual(
        allow,
        emqx_access_control:authorize(emqx_authz_context:make(ClientInfo), ?AUTHZ_PUBLISH, <<"t">>)
    ),
    ?assertEqual(
        allow,
        emqx_access_control:authorize(emqx_authz_context:make(ClientInfo), ?AUTHZ_PUBLISH, <<"t">>)
    ),
    ?assertMatch(
        #{hits := #{value := 1}, misses := #{value := 1}},
        emqx_auth_cache:metrics(?AUTHZ_CACHE)
    ),
    %% Now change a var in each interpolated part, the cache should NOT be hit
    ?assertEqual(
        deny,
        emqx_access_control:authorize(
            emqx_authz_context:make(ClientInfo#{cn => <<"cn2">>}), ?AUTHZ_PUBLISH, <<"t">>
        )
    ),
    ?assertEqual(
        deny,
        emqx_access_control:authorize(
            emqx_authz_context:make(ClientInfo#{clientid => <<"clientid2">>}),
            ?AUTHZ_PUBLISH,
            <<"t">>
        )
    ),
    ?assertEqual(
        deny,
        emqx_access_control:authorize(
            emqx_authz_context:make(ClientInfo#{username => <<"username2">>}),
            ?AUTHZ_PUBLISH,
            <<"t">>
        )
    ),
    ?assertMatch(
        #{hits := #{value := 1}, misses := #{value := 4}},
        emqx_auth_cache:metrics(?AUTHZ_CACHE)
    ).

t_disallowed_placeholders_preserved(TCConfig) ->
    ok = setup_handler_and_config(
        TCConfig,
        fun(Req0, State) ->
            {ok, Body, Req1} = cowboy_req:read_body(Req0),
            ?assertMatch(
                #{
                    <<"cname">> := <<>>,
                    <<"usertypo">> := <<"${usertypo}">>
                },
                emqx_utils_json:decode(Body)
            ),
            {ok, ?AUTHZ_HTTP_RESP(allow, Req1), State}
        end,
        #{
            <<"method">> => <<"post">>,
            <<"body">> => #{
                <<"cname">> => ?PH_CERT_CN_NAME,
                <<"usertypo">> => <<"${usertypo}">>
            }
        }
    ),

    ClientInfo = #{
        clientid => <<"client id">>,
        username => <<"user name">>,
        peerhost => {127, 0, 0, 1},
        protocol => <<"MQTT">>,
        zone => default,
        listener => 'tcp:default'
    },

    ?assertEqual(
        allow,
        emqx_access_control:authorize(emqx_authz_context:make(ClientInfo), ?AUTHZ_PUBLISH, <<"t">>)
    ).

t_disallowed_placeholders_path(TCConfig) ->
    ok = setup_handler_and_config(
        TCConfig,
        fun(Req, State) ->
            {ok, ?AUTHZ_HTTP_RESP(allow, Req), State}
        end,
        #{
            <<"url">> =>
                <<"http://127.0.0.1:", (http_port_bin(TCConfig))/binary, "/authz/use+rs/${typo}">>
        }
    ),

    ClientInfo = #{
        clientid => <<"client id">>,
        username => <<"user name">>,
        peerhost => {127, 0, 0, 1},
        protocol => <<"MQTT">>,
        zone => default,
        listener => 'tcp:default'
    },

    % % NOTE: disallowed placeholder left intact, which makes the URL invalid
    ?assertEqual(
        deny,
        emqx_access_control:authorize(emqx_authz_context:make(ClientInfo), ?AUTHZ_PUBLISH, <<"t">>)
    ).

t_create_replace(TCConfig) ->
    ClientInfo = #{
        clientid => <<"clientid">>,
        username => <<"username">>,
        peerhost => {127, 0, 0, 1},
        zone => default,
        listener => 'tcp:default'
    },

    ValidConfig = raw_http_authz_config(TCConfig),

    %% Create with valid URL
    ok = setup_handler_and_config(
        TCConfig,
        fun(Req0, State) ->
            {ok, ?AUTHZ_HTTP_RESP(allow, Req0), State}
        end,
        #{
            <<"url">> =>
                <<"http://127.0.0.1:", (http_port_bin(TCConfig))/binary,
                    "/authz/users/?topic=${topic}&action=${action}">>
        }
    ),

    ?assertEqual(
        allow,
        emqx_access_control:authorize(emqx_authz_context:make(ClientInfo), ?AUTHZ_PUBLISH, <<"t">>)
    ),

    %% Changing to valid config
    OkConfig = ValidConfig#{
        <<"url">> =>
            <<"http://127.0.0.1:", (http_port_bin(TCConfig))/binary,
                "/authz/users/?topic=${topic}&action=${action}">>
    },

    ?assertMatch(
        {ok, _},
        emqx_authz:update({?CMD_REPLACE, http}, OkConfig)
    ),

    ?assertEqual(
        allow,
        emqx_access_control:authorize(emqx_authz_context:make(ClientInfo), ?AUTHZ_PUBLISH, <<"t">>)
    ),

    ?assertMatch(
        {error, _},
        emqx_authz:update({?CMD_REPLACE, http}, ValidConfig#{
            <<"url">> => <<"localhost">>
        })
    ),

    ?assertMatch(
        {error, _},
        emqx_authz:update({?CMD_REPLACE, http}, ValidConfig#{
            <<"url">> => <<"//foo.bar/x/y?q=z">>
        })
    ),

    ?assertMatch(
        {error, _},
        emqx_authz:update({?CMD_REPLACE, http}, ValidConfig#{
            <<"url">> =>
                <<"http://127.0.0.1:", (http_port_bin(TCConfig))/binary,
                    "/authz/users/?topic=${topic}&action=${action}#fragment">>
        })
    ).

t_resource_status(TCConfig) ->
    EnabledConfig = raw_http_authz_config(TCConfig),
    DisabledConfig =
        EnabledConfig#{<<"enable">> => false},

    %% Create enabled, update to disabled
    ok = emqx_authz_test_lib:setup_config(EnabledConfig, #{}),
    #{resource_id := ResourceId0} = emqx_authz:lookup_state(http),
    ?assertEqual({ok, connected}, emqx_resource:health_check(ResourceId0)),
    ?assertMatch(
        {ok, _},
        emqx_authz:update({?CMD_REPLACE, http}, DisabledConfig)
    ),
    ?assertEqual({error, resource_is_stopped}, emqx_resource:health_check(ResourceId0)),

    %% Cleanup
    emqx_authz_test_lib:reset_authorizers(),

    %% Now, create disabled, update to enabled
    ok = emqx_authz_test_lib:setup_config(DisabledConfig, #{}),
    #{resource_id := ResourceId1} = emqx_authz:lookup_state(http),
    ?assertEqual({error, resource_is_stopped}, emqx_resource:health_check(ResourceId1)),
    ?assertMatch(
        {ok, _},
        emqx_authz:update({?CMD_REPLACE, http}, EnabledConfig)
    ),
    ?assertEqual({ok, connected}, emqx_resource:health_check(ResourceId1)),

    %% Cleanup
    emqx_authz_test_lib:reset_authorizers().

t_uri_normalization(TCConfig) ->
    ok = emqx_authz_test_lib:setup_config(
        raw_http_authz_config(TCConfig),
        #{
            <<"url">> =>
                <<"http://127.0.0.1:", (http_port_bin(TCConfig))/binary,
                    "?topic=${topic}&action=${action}">>
        }
    ).

t_oauth2_client_credentials(TCConfig) ->
    Token = <<"authz-oauth2-token">>,
    BaseURL = <<"http://127.0.0.1:", (http_port_bin(TCConfig))/binary>>,
    Handler = fun(Req0, State) ->
        case cowboy_req:path(Req0) of
            <<"/authz/token">> ->
                {ok, Body, Req1} = cowboy_req:read_body(Req0),
                Form = uri_string:dissect_query(Body),
                ?assert(lists:member({<<"grant_type">>, <<"client_credentials">>}, Form)),
                Req = cowboy_req:reply(
                    200,
                    #{<<"content-type">> => <<"application/json">>},
                    emqx_utils_json:encode(#{
                        access_token => Token,
                        expires_in => 3600,
                        token_type => <<"Bearer">>
                    }),
                    Req1
                ),
                {ok, Req, State};
            <<"/authz/check">> ->
                Headers = cowboy_req:headers(Req0),
                ?assertEqual(<<"Bearer ", Token/binary>>, maps:get(<<"authorization">>, Headers)),
                {ok, ?AUTHZ_HTTP_RESP(allow, Req0), State}
        end
    end,
    ok = setup_handler_and_config(TCConfig, Handler, #{
        <<"url">> => <<BaseURL/binary, "/authz/check">>,
        <<"oauth2">> => oauth2_config(<<BaseURL/binary, "/authz/token">>)
    }),
    ClientInfo = #{
        clientid => <<"clientid">>,
        username => <<"username">>,
        peerhost => {127, 0, 0, 1},
        zone => default,
        listener => 'tcp:default'
    },
    ?assertEqual(allow, emqx_access_control:authorize(ClientInfo, ?AUTHZ_PUBLISH, <<"t">>)).

t_oauth2_ssl_certs_are_saved(TCConfig) ->
    BaseURL = <<"http://127.0.0.1:", (http_port_bin(TCConfig))/binary>>,
    ok = set_oauth2_token_handler(<<"/authz/token">>),
    SSL = inline_ssl_certs(),
    ok = emqx_authz_test_lib:setup_config(
        raw_http_authz_config(TCConfig),
        #{
            <<"oauth2">> =>
                (oauth2_config(<<BaseURL/binary, "/authz/token">>))#{<<"ssl">> => SSL}
        }
    ),
    [#{<<"oauth2">> := #{<<"ssl">> := SavedSSL}}] =
        emqx:get_raw_config([authorization, sources]),
    assert_ssl_certs_are_saved(SSL, SavedSSL).

t_oauth2_ssl_verify_none_allows_blank_certs(TCConfig) ->
    BaseURL = <<"http://127.0.0.1:", (http_port_bin(TCConfig))/binary>>,
    ok = set_oauth2_token_handler(<<"/authz/token">>),
    ok = emqx_authz_test_lib:setup_config(
        raw_http_authz_config(TCConfig),
        #{
            <<"oauth2">> =>
                (oauth2_config(<<BaseURL/binary, "/authz/token">>))#{
                    <<"ssl">> => #{
                        <<"enable">> => true,
                        <<"verify">> => <<"verify_none">>,
                        <<"cacertfile">> => <<>>,
                        <<"certfile">> => <<>>,
                        <<"keyfile">> => <<>>
                    }
                }
        }
    ).

t_oauth2_start_timeout_keeps_source(TCConfig) ->
    ok = block_oauth2_token_endpoint(<<"/authz/token">>),
    BaseURL = <<"http://127.0.0.1:", (http_port_bin(TCConfig))/binary>>,
    Oauth2 = (oauth2_config(<<BaseURL/binary, "/authz/token">>))#{
        <<"timeout">> => <<"30s">>
    },
    try
        ok = emqx_authz_test_lib:setup_config(
            raw_http_authz_config(TCConfig),
            #{<<"oauth2">> => Oauth2}
        ),
        ?assertMatch(
            [#{type := http}],
            emqx_conf:get([authorization, sources])
        ),
        #{resource_id := ResourceId} = emqx_authz:lookup_state(http),
        ?assert(lists:member(ResourceId, emqx_resource:list_group_instances(?AUTHZ_RESOURCE_GROUP)))
    after
        unblock_oauth2_token_endpoint()
    end.

t_oauth2_start_exception_removes_resource(TCConfig) ->
    BaseURL = <<"http://127.0.0.1:", (http_port_bin(TCConfig))/binary>>,
    Error = emqx_common_test_helpers:with_mock(
        emqx_resource,
        start,
        fun(_) -> error(start_failed) end,
        fun() ->
            emqx_authz_test_lib:setup_config(
                raw_http_authz_config(TCConfig),
                #{<<"oauth2">> => oauth2_config(<<BaseURL/binary, "/authz/token">>)}
            )
        end
    ),
    ?assertMatch({error, _}, Error),
    ?assert(contains_term(start_failed, Error)),
    ?assertEqual([], emqx_resource:list_group_instances(?AUTHZ_RESOURCE_GROUP)).

%%------------------------------------------------------------------------------
%% Helpers
%%------------------------------------------------------------------------------

%%------------------------------------------------------------------------------
%% Templated host (one-off request) tests
%%------------------------------------------------------------------------------

-doc """
A URL with a templated host renders the host from client attributes and sends a
one-off request to the rendered (allowed) host; query and header templates keep
working on this code path.
""".
t_templated_host_authorize(TCConfig) ->
    ok = setup_handler_and_config(
        TCConfig,
        fun(Req0, State) ->
            #{
                topic := <<"t">>,
                action := <<"publish">>
            } = cowboy_req:match_qs([topic, action], Req0),
            ?assertEqual(<<"localhost">>, maps:get(<<"x-tenant">>, cowboy_req:headers(Req0))),
            {ok, ?AUTHZ_HTTP_RESP(allow, Req0), State}
        end,
        templated_host_config_params(TCConfig)
    ),
    ?assertEqual(
        allow,
        emqx_access_control:authorize(templated_host_client_info(), ?AUTHZ_PUBLISH, <<"t">>)
    ).

-doc """
When the rendered host does not match any 'allowed_hosts' entry, the request is
not made and the authorization check fails closed (deny with no_match = deny).
""".
t_templated_host_not_allowed(TCConfig) ->
    ok = setup_handler_and_config(
        TCConfig,
        fun(Req0, State) ->
            {ok, ?AUTHZ_HTTP_RESP(allow, Req0), State}
        end,
        (templated_host_config_params(TCConfig))#{
            <<"allowed_hosts">> => [<<"*.example.com">>]
        }
    ),
    %% The server would allow: a deny result proves it was never contacted.
    ?assertEqual(
        deny,
        emqx_access_control:authorize(templated_host_client_info(), ?AUTHZ_PUBLISH, <<"t">>)
    ).

-doc """
When a host template placeholder has no value, the request is not made and the
authorization check fails closed.
""".
t_templated_host_missing_var(TCConfig) ->
    ok = setup_handler_and_config(
        TCConfig,
        fun(Req0, State) ->
            {ok, ?AUTHZ_HTTP_RESP(allow, Req0), State}
        end,
        templated_host_config_params(TCConfig)
    ),
    ClientInfo = maps:remove(client_attrs, templated_host_client_info()),
    ?assertEqual(
        deny,
        emqx_access_control:authorize(ClientInfo, ?AUTHZ_PUBLISH, <<"t">>)
    ).

-doc """
A templated host URL cannot be configured without a non-empty 'allowed_hosts'
list.
""".
t_templated_host_requires_allowed_hosts(TCConfig) ->
    Params = templated_host_config_params(TCConfig),
    lists:foreach(
        fun(ConfigParams) ->
            Result =
                try
                    setup_handler_and_config(
                        TCConfig,
                        fun(Req0, State) ->
                            {ok, ?AUTHZ_HTTP_RESP(allow, Req0), State}
                        end,
                        ConfigParams
                    )
                catch
                    _:Error ->
                        {error, Error}
                end,
            ?assertMatch({error, _}, Result, ConfigParams)
        end,
        [
            maps:remove(<<"allowed_hosts">>, Params),
            %% templated host requires explicit hostname_resolution = dynamic
            maps:remove(<<"hostname_resolution">>, Params),
            Params#{<<"allowed_hosts">> => []},
            Params#{<<"allowed_hosts">> => [<<"bad host">>]}
        ]
    ).

-doc """
'hostname_resolution = dynamic' forces per-request connections even when the
URL host is a literal hostname: no connector resource is created,
authorization works through the shared hackney pool sized by 'pool_size', and
'allowed_hosts' is not required.
""".
t_dynamic_resolution_static_host(TCConfig) ->
    ok = setup_handler_and_config(
        TCConfig,
        fun(Req0, State) ->
            #{
                topic := <<"t">>,
                action := <<"publish">>
            } = cowboy_req:match_qs([topic, action], Req0),
            {ok, ?AUTHZ_HTTP_RESP(allow, Req0), State}
        end,
        #{<<"hostname_resolution">> => <<"dynamic">>, <<"pool_size">> => 3}
    ),
    ?assertEqual([], emqx_resource:list_group_instances(?AUTHZ_RESOURCE_GROUP)),
    ?assertEqual(3, hackney_pool:max_connections(authz)),
    ?assertEqual(
        allow,
        emqx_access_control:authorize(templated_host_client_info(), ?AUTHZ_PUBLISH, <<"t">>)
    ).

templated_host_client_info() ->
    #{
        clientid => <<"clientid">>,
        username => <<"username">>,
        peerhost => {127, 0, 0, 1},
        zone => default,
        listener => 'tcp:default',
        client_attrs => #{<<"tns">> => <<"localhost">>}
    }.

templated_host_config_params(TCConfig) ->
    #{
        <<"url">> =>
            <<"http://${client_attrs.tns}:", (http_port_bin(TCConfig))/binary,
                "/authz/users/?topic=${topic}&action=${action}">>,
        <<"hostname_resolution">> => <<"dynamic">>,
        <<"allowed_hosts">> => [<<"localhost">>],
        <<"headers">> => #{<<"X-Tenant">> => <<"${client_attrs.tns}">>}
    }.

raw_http_authz_config(TCConfig) ->
    #{
        <<"enable">> => <<"true">>,
        <<"type">> => <<"http">>,
        <<"max_inactive">> => <<"10s">>,
        <<"method">> => <<"get">>,
        <<"url">> =>
            <<"http://127.0.0.1:", (http_port_bin(TCConfig))/binary,
                "/authz/users/?topic=${topic}&action=${action}">>,
        <<"headers">> => #{<<"X-Test-Header">> => <<"Test Value">>}
    }.

oauth2_config(TokenEndpoint) ->
    #{
        <<"enable">> => true,
        <<"grant_type">> => <<"client_credentials">>,
        <<"token_endpoint">> => TokenEndpoint,
        <<"client_id">> => <<"client-id">>,
        <<"client_secret">> => <<"client-secret">>
    }.

set_oauth2_token_handler(Path) ->
    emqx_utils_http_test_server:set_handler(fun(Req0, State) ->
        Path = cowboy_req:path(Req0),
        Req = cowboy_req:reply(
            200,
            #{<<"content-type">> => <<"application/json">>},
            emqx_utils_json:encode(#{
                access_token => <<"oauth2-token">>,
                expires_in => 3600,
                token_type => <<"Bearer">>
            }),
            Req0
        ),
        {ok, Req, State}
    end).

inline_ssl_certs() ->
    #{
        <<"enable">> => true,
        <<"verify">> => <<"verify_peer">>,
        <<"cacertfile">> => pem("cacert.pem"),
        <<"certfile">> => pem("client-cert.pem"),
        <<"keyfile">> => pem("client-key.pem")
    }.

pem(Name) ->
    Path = filename:join([code:lib_dir(emqx), etc, certs, Name]),
    {ok, Pem} = file:read_file(Path),
    Pem.

assert_ssl_certs_are_saved(SSL, SavedSSL) ->
    lists:foreach(
        fun(Key) ->
            SavedPath = maps:get(Key, SavedSSL),
            ?assertNotEqual(maps:get(Key, SSL), SavedPath),
            ?assert(filelib:is_regular(SavedPath))
        end,
        [<<"cacertfile">>, <<"certfile">>, <<"keyfile">>]
    ).

setup_handler_and_config(TCConfig, Handler, Config) ->
    ok = emqx_utils_http_test_server:set_handler(Handler),
    ok = emqx_authz_test_lib:setup_config(
        raw_http_authz_config(TCConfig),
        Config
    ).

block_oauth2_token_endpoint(Path) ->
    TestPid = self(),
    emqx_utils_http_test_server:set_handler(fun(Req0, State) ->
        Path = cowboy_req:path(Req0),
        TestPid ! {oauth2_token_request, self()},
        receive
            unblock_oauth2_token_endpoint -> ok
        end,
        Req = cowboy_req:reply(
            200,
            #{<<"content-type">> => <<"application/json">>},
            emqx_utils_json:encode(#{
                access_token => <<"oauth2-token">>,
                expires_in => 3600,
                token_type => <<"Bearer">>
            }),
            Req0
        ),
        {ok, Req, State}
    end).

unblock_oauth2_token_endpoint() ->
    receive
        {oauth2_token_request, Pid} ->
            Pid ! unblock_oauth2_token_endpoint,
            ok
    after 0 ->
        ok
    end.

contains_term(Term, Term) ->
    true;
contains_term(Needle, Term) when is_map(Term) ->
    contains_term(Needle, maps:to_list(Term));
contains_term(Needle, Term) when is_tuple(Term) ->
    contains_term(Needle, tuple_to_list(Term));
contains_term(Needle, Term) when is_list(Term) ->
    lists:any(fun(Element) -> contains_term(Needle, Element) end, Term);
contains_term(_Needle, _Term) ->
    false.

get_metrics() ->
    Metrics = emqx_metrics_worker:get_metrics(authz_metrics, http),
    lists:foldl(
        fun(Name, Acc) ->
            Acc#{Name => emqx_metrics:val_global(Name)}
        end,
        Metrics,
        [
            'authorization.superuser',
            'authorization.matched.allow',
            'authorization.matched.deny',
            'authorization.nomatch'
        ]
    ).

get_status_api() ->
    Path = emqx_mgmt_api_test_util:uri(["authorization", "sources", "http", "status"]),
    Auth = emqx_mgmt_api_test_util:auth_header_(),
    Opts = #{return_all => true},
    Res0 = emqx_mgmt_api_test_util:request_api(get, Path, _QParams = [], Auth, _Body = [], Opts),
    {Status, RawBody} = emqx_mgmt_api_test_util:simplify_result(Res0),
    {Status, emqx_utils_json:decode(RawBody)}.

http_port_bin(TCConfig) ->
    integer_to_binary(?config(http_port, TCConfig)).

setup_bad_response_content_type(TCConfig) ->
    setup_handler_and_config(
        TCConfig,
        fun(Req0, State) ->
            {ok, _PostVars, Req1} = cowboy_req:read_urlencoded_body(Req0),
            Req = cowboy_req:reply(
                200,
                #{<<"content-type">> => <<"text/csv">>},
                "hi",
                Req1
            ),
            {ok, Req, State}
        end,
        #{
            <<"method">> => <<"post">>,
            <<"body">> => #{<<"username">> => <<"${username}">>},
            <<"headers">> => #{
                <<"accept">> => <<"text/plain">>,
                <<"content-type">> => <<"application/json">>
            }
        }
    ).

profile_cases() ->
    [t_bad_response_content_type_profile, t_bad_response, t_response_handling].
